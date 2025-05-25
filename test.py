import streamlit as st
import polars as pl
import pandas as pd
from rapidfuzz import fuzz
from io import BytesIO
from collections import defaultdict
import re

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔍 Finacle vs Basis Fuzzy Matching")

st.markdown(
    "Upload your BASIS and FINACLE files below (CSV or Excel). "
    "Matching will be done using fuzzy logic for names, emails, dates of birth, and phone numbers."
)

# === 1. Upload Section ===
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📂 Upload BASIS file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="basis")
with col2:
    finacle_file = st.file_uploader("📂 Upload FINACLE file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="finacle")

# === 2. Helper Functions ===

def read_file(file, is_basis=True):
    if file.name.endswith('.csv'):
        if is_basis:
            return pl.read_csv(file, dtypes={"TEL_NUM": str, "TEL_NUM_2": str, "FAX_NUM": str})
        else:
            return pl.read_csv(file, dtypes={"PREFERREDPHONE": str, "SMSBANKINGMOBILENUMBER": str})
    else:
        if is_basis:
            return pl.read_excel(file, schema_overrides={"TEL_NUM": str, "TEL_NUM_2": str, "FAX_NUM": str})
        else:
            return pl.read_excel(file, schema_overrides={"PREFERREDPHONE": str, "SMSBANKINGMOBILENUMBER": str})

def clean_phone(phone: str) -> str:
    return re.sub(r'\D+', '', phone) if phone else ''

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email_Basis",
        "BIR_DATE": "Date_of_Birth_Basis",
        "TEL_NUM": "Phone_1_Basis",
        "TEL_NUM_2": "Phone_2_Basis",
        "FAX_NUM": "Phone_3_Basis"
    })
    return df.select([
        pl.col("Name").fill_null(""),
        pl.col("Email_Basis").fill_null(""),
        pl.col("Date_of_Birth_Basis").fill_null(""),
        pl.col("Phone_1_Basis").fill_null("").map_elements(clean_phone),
        pl.col("Phone_2_Basis").fill_null("").map_elements(clean_phone),
        pl.col("Phone_3_Basis").fill_null("").map_elements(clean_phone)
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email_Finacle",
        "CUST_DOB": "Date_of_Birth_Finacle",
        "PREFERREDPHONE": "Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER": "Phone_2_Finacle"
    })
    return df.select([
        pl.col("Name").fill_null(""),
        pl.col("Email_Finacle").fill_null(""),
        pl.col("Date_of_Birth_Finacle").fill_null(""),
        pl.col("Phone_1_Finacle").fill_null("").map_elements(clean_phone),
        pl.col("Phone_2_Finacle").fill_null("").map_elements(clean_phone),
        pl.lit("").alias("Phone_3_Finacle")
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.strip().str.to_lowercase())
    return df

def fuzzy_match_dates(d1, d2, threshold_days=30):
    try:
        date1 = pd.to_datetime(d1, errors='coerce')
        date2 = pd.to_datetime(d2, errors='coerce')
        if pd.isna(date1) or pd.isna(date2):
            return False, 0
        diff = abs((date1 - date2).days)
        return diff <= threshold_days, 100 - (diff / threshold_days * 100)
    except:
        return False, 0

def find_fuzzy_matches(basis_df: pl.DataFrame, finacle_df: pl.DataFrame, 
                      name_threshold=85, email_threshold=90, 
                      phone_threshold=85, dob_threshold_days=30):
    # Normalize and preprocess
    basis_df = normalize(basis_df)
    finacle_df = normalize(finacle_df)

    # Add indices for tracking
    basis_df = basis_df.with_columns(index=pl.int_range(0, basis_df.height))
    finacle_df = finacle_df.with_columns(index=pl.int_range(0, finacle_df.height))

    # Add blocking keys
    basis_df = basis_df.with_columns(
        pl.col("Name").str.slice(0, 3).alias("block_key"),
        pl.col("Date_of_Birth_Basis").str.slice(0, 4).alias("dob_year")
    )
    finacle_df = finacle_df.with_columns(
        pl.col("Name").str.slice(0, 3).alias("block_key"),
        pl.col("Date_of_Birth_Finacle").str.slice(0, 4).alias("dob_year")
    )

    # Build Finacle indices
    block_index = defaultdict(list)
    phone_index = defaultdict(list)
    
    for row in finacle_df.iter_rows(named=True):
        block_key = (row["block_key"], row["dob_year"])
        block_index[block_key].append(row["index"])
        for phone in [row["Phone_1_Finacle"], row["Phone_2_Finacle"], row["Phone_3_Finacle"]]:
            if phone:
                phone_index[phone].append(row["index"])

    matches = []
    mismatches = []
    matched_indices = set()

    # Process each Basis entry
    for basis_row in basis_df.iter_rows(named=True):
        candidates = set()

        # Block-based candidates
        block_key = (basis_row["block_key"], basis_row["dob_year"])
        candidates.update(block_index.get(block_key, []))

        # Phone-based candidates
        for phone in [basis_row["Phone_1_Basis"], basis_row["Phone_2_Basis"], basis_row["Phone_3_Basis"]]:
            if phone:
                candidates.update(phone_index.get(phone, []))

        candidates = [idx for idx in candidates if idx not in matched_indices]
        best_score = 0
        best_fin_idx = None
        best_data = None

        for fin_idx in candidates:
            fin_row = finacle_df.row(fin_idx, named=True)
            
            # Name similarity
            name_score = fuzz.ratio(basis_row["Name"], fin_row["Name"])
            if name_score < name_threshold:
                continue

            # Email similarity
            email_score = fuzz.ratio(basis_row["Email_Basis"], fin_row["Email_Finacle"]) if basis_row["Email_Basis"] and fin_row["Email_Finacle"] else 0

            # Phone similarity
            basis_phones = {basis_row["Phone_1_Basis"], basis_row["Phone_2_Basis"], basis_row["Phone_3_Basis"]}
            fin_phones = {fin_row["Phone_1_Finacle"], fin_row["Phone_2_Finacle"], fin_row["Phone_3_Finacle"]}
            common = len(basis_phones & fin_phones)
            total = len(basis_phones | fin_phones)
            phone_score = (common / total) * 100 if total else 100

            # Date similarity
            dob_match, dob_score = fuzzy_match_dates(
                basis_row["Date_of_Birth_Basis"], 
                fin_row["Date_of_Birth_Finacle"], 
                dob_threshold_days
            )

            total_score = (name_score * 0.4) + (email_score * 0.3) + (dob_score * 0.2) + (phone_score * 0.1)
            
            if total_score > best_score:
                best_score = total_score
                best_fin_idx = fin_idx
                best_data = {
                    "Finacle_Name": fin_row["Name"],
                    "Email_Finacle": fin_row["Email_Finacle"],
                    "DOB_Finacle": fin_row["Date_of_Birth_Finacle"],
                    "Phones_Finacle": list(fin_phones)
                }

        if best_score >= 70:  # Example threshold
            matched_indices.add(best_fin_idx)
            matches.append({
                "Basis_Name": basis_row["Name"],
                "Email_Basis": basis_row["Email_Basis"],
                "DOB_Basis": basis_row["Date_of_Birth_Basis"],
                "Phones_Basis": list(basis_phones),
                **best_data,
                "Score": round(best_score, 2)
            })
        else:
            mismatches.append({
                "Basis_Name": basis_row["Name"],
                "Email_Basis": basis_row["Email_Basis"],
                "DOB_Basis": basis_row["Date_of_Birth_Basis"],
                "Phones_Basis": list(basis_phones)
            })

    # Collect unmatched Finacle entries
    for fin_row in finacle_df.iter_rows(named=True):
        if fin_row["index"] not in matched_indices:
            mismatches.append({
                "Finacle_Name": fin_row["Name"],
                "Email_Finacle": fin_row["Email_Finacle"],
                "DOB_Finacle": fin_row["Date_of_Birth_Finacle"],
                "Phones_Finacle": [fin_row["Phone_1_Finacle"], fin_row["Phone_2_Finacle"], fin_row["Phone_3_Finacle"]]
            })

    return pd.DataFrame(matches), pd.DataFrame(mismatches)

def convert_df(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# === 3. Main Logic ===
if basis_file and finacle_file:
    with st.spinner("🔄 Matching records, please wait..."):
        basis_df = preprocess_basis(read_file(basis_file, is_basis=True))
        finacle_df = preprocess_finacle(read_file(finacle_file, is_basis=False))
        matches_df, mismatches_df = find_fuzzy_matches(basis_df, finacle_df)

    st.success("✅ Matching complete!")

    st.subheader("✅ Matches")
    st.dataframe(matches_df)

    st.subheader("❌ Mismatches")
    st.dataframe(mismatches_df)

    if not matches_df.empty:
        st.download_button(
            label="📥 Download Matches as Excel",
            data=convert_df(matches_df),
            file_name="matches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    if not mismatches_df.empty:
        st.download_button(
            label="📥 Download Mismatches as Excel",
            data=convert_df(mismatches_df),
            file_name="mismatches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
else:
    st.info("Please upload both BASIS and FINACLE files to start matching.")