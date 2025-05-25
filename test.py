import streamlit as st
import polars as pl
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from io import BytesIO

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔍 Finacle vs Basis Fuzzy Matching")

st.markdown(
    "Upload your BASIS and FINACLE files below (CSV or Excel). "
    "Matching will be done using fuzzy logic for names, emails, dates of birth, and phone numbers."
)

col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📂 Upload BASIS file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="basis")
with col2:
    finacle_file = st.file_uploader("📂 Upload FINACLE file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="finacle")

# === File Reading ===
def read_file(file, is_basis=True):
    if file.name.endswith('.csv'):
        return pl.read_csv(file, dtypes={"TEL_NUM": pl.Utf8, "TEL_NUM_2": pl.Utf8, "FAX_NUM": pl.Utf8} if is_basis else {
            "PREFERREDPHONE": pl.Utf8, "SMSBANKINGMOBILENUMBER": pl.Utf8})
    else:
        return pl.read_excel(file, schema_overrides={"TEL_NUM": pl.Utf8, "TEL_NUM_2": pl.Utf8, "FAX_NUM": pl.Utf8} if is_basis else {
            "PREFERREDPHONE": pl.Utf8, "SMSBANKINGMOBILENUMBER": pl.Utf8})

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
        "Date_of_Birth_Basis",
        pl.col("Phone_1_Basis").fill_null(""),
        pl.col("Phone_2_Basis").fill_null(""),
        pl.col("Phone_3_Basis").fill_null("")
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email_Finacle",
        "CUST_DOB": "Date_of_Birth_Finacle",
        "PREFERREDPHONE": "Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER": "Phone_2_Finacle"
    }).with_columns(pl.lit("").alias("Phone_3_Finacle"))
    return df.select([
        pl.col("Name").fill_null(""),
        pl.col("Email_Finacle").fill_null(""),
        "Date_of_Birth_Finacle",
        pl.col("Phone_1_Finacle").fill_null(""),
        pl.col("Phone_2_Finacle").fill_null(""),
        pl.col("Phone_3_Finacle").fill_null("")
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase().alias(col))
    return df

def fuzzy_match_string(s1, s2, threshold=85):
    if not s1 or not s2:
        return False, 0
    score = fuzz.ratio(s1, s2)
    return score >= threshold, score

def fuzzy_match_phones(list1, list2, threshold=85):
    set1 = set([p for p in list1 if p])
    set2 = set([p for p in list2 if p])
    if not set1 and not set2:
        return True, 100
    intersection = set1 & set2
    union = set1 | set2
    score = (len(intersection) / len(union)) * 100
    return score >= threshold, score

def fuzzy_match_dates(d1, d2, threshold_days=30):
    try:
        date1 = pd.to_datetime(d1, errors='coerce')
        date2 = pd.to_datetime(d2, errors='coerce')
        if pd.isna(date1) or pd.isna(date2):
            return False, 0
        diff = abs((date1 - date2).days)
        return diff <= threshold_days, max(0, 100 - (diff / threshold_days * 100))
    except:
        return False, 0

# === Optimized Matching ===
def find_fuzzy_matches(basis_df, finacle_df):
    basis_df = normalize(preprocess_basis(basis_df)).to_pandas()
    finacle_df = normalize(preprocess_finacle(finacle_df)).to_pandas()

    matches = []
    mismatches = []
    matched_indices = set()

    total = len(basis_df)
    progress = st.progress(0)

    for i, b_row in basis_df.iterrows():
        b_name = b_row["Name"]
        b_email = b_row["Email_Basis"]
        b_dob = b_row["Date_of_Birth_Basis"]
        b_phones = [b_row["Phone_1_Basis"], b_row["Phone_2_Basis"], b_row["Phone_3_Basis"]]

        best_score = 0
        best_match = None
        best_idx = None

        for j, f_row in finacle_df.iterrows():
            if j in matched_indices:
                continue
            f_name = f_row["Name"]
            f_email = f_row["Email_Finacle"]
            f_dob = f_row["Date_of_Birth_Finacle"]
            f_phones = [f_row["Phone_1_Finacle"], f_row["Phone_2_Finacle"], f_row["Phone_3_Finacle"]]

            name_match, name_score = fuzzy_match_string(b_name, f_name, 85)
            if not name_match:
                continue

            email_match, email_score = fuzzy_match_string(b_email, f_email, 90)
            phone_match, phone_score = fuzzy_match_phones(b_phones, f_phones, 85)
            dob_match, dob_score = fuzzy_match_dates(b_dob, f_dob, 30)

            total_score = name_score * 0.4 + email_score * 0.3 + dob_score * 0.2 + phone_score * 0.1

            if total_score > best_score:
                best_score = total_score
                best_match = f_row
                best_idx = j

        if best_match is not None:
            matches.append({
                "Basis_Name": b_name,
                "Finacle_Name": best_match["Name"],
                "Email_Basis": b_email,
                "Email_Finacle": best_match["Email_Finacle"],
                "DOB_Basis": b_dob,
                "DOB_Finacle": best_match["Date_of_Birth_Finacle"],
                "Phones_Basis": b_phones,
                "Phones_Finacle": [best_match["Phone_1_Finacle"], best_match["Phone_2_Finacle"], best_match["Phone_3_Finacle"]],
                "Score": round(best_score, 2)
            })
            matched_indices.add(best_idx)
        else:
            mismatches.append({
                "Unmatched_Basis_Name": b_name,
                "Email_Basis": b_email,
                "DOB_Basis": b_dob,
                "Phones_Basis": b_phones
            })

        progress.progress((i + 1) / total)

    # Unmatched Finacle entries
    for j, f_row in finacle_df.iterrows():
        if j not in matched_indices:
            mismatches.append({
                "Unmatched_Finacle_Name": f_row["Name"],
                "Email_Finacle": f_row["Email_Finacle"],
                "DOB_Finacle": f_row["Date_of_Birth_Finacle"],
                "Phones_Finacle": [f_row["Phone_1_Finacle"], f_row["Phone_2_Finacle"], f_row["Phone_3_Finacle"]]
            })

    return pd.DataFrame(matches), pd.DataFrame(mismatches)

# === Download Helper ===
def convert_df(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# === Run Matching ===
if basis_file and finacle_file:
    with st.spinner("🔄 Matching records, please wait..."):
        basis_df = read_file(basis_file, is_basis=True)
        finacle_df = read_file(finacle_file, is_basis=False)
        matches_df, mismatches_df = find_fuzzy_matches(basis_df, finacle_df)

    st.success("✅ Matching complete!")

    st.subheader("✅ Matches")
    st.dataframe(matches_df)

    st.subheader("❌ Mismatches")
    st.dataframe(mismatches_df)

    if not matches_df.empty:
        excel_data = convert_df(matches_df)
        st.download_button("📥 Download Matches", data=excel_data, file_name="matches.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if not mismatches_df.empty:
        excel_data = convert_df(mismatches_df)
        st.download_button("📥 Download Mismatches", data=excel_data, file_name="mismatches.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
else:
    st.info("Please upload both BASIS and FINACLE files to begin.")
