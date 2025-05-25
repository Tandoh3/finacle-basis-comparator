import streamlit as st
import polars as pl
import pandas as pd
from fuzzywuzzy import fuzz
from io import BytesIO

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

def read_file(file):
    if file.name.endswith('.csv'):
        return pl.read_csv(file)
    else:
        return pl.read_excel(file)

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
        pl.col("Phone_1_Basis").fill_null("").cast(pl.Utf8),
        pl.col("Phone_2_Basis").fill_null("").cast(pl.Utf8),
        pl.col("Phone_3_Basis").fill_null("").cast(pl.Utf8)
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
        pl.col("Phone_1_Finacle").fill_null("").cast(pl.Utf8),
        pl.col("Phone_2_Finacle").fill_null("").cast(pl.Utf8),
        pl.col("Phone_3_Finacle").fill_null("").cast(pl.Utf8)
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return df

def fuzzy_match_string(s1, s2, threshold=80):
    if not s1 or not s2:
        return False, 0
    score = fuzz.ratio(s1, s2)
    return score >= threshold, score

def fuzzy_match_phones(list1, list2, threshold=85):
    set1 = set([x for x in list1 if x])
    set2 = set([x for x in list2 if x])
    intersection = set1 & set2
    union = set1 | set2
    if not union:
        return True, 100
    score = (len(intersection) / len(union)) * 100
    return score >= threshold, score

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

def find_fuzzy_matches(
    basis_df: pl.DataFrame, finacle_df: pl.DataFrame,
    name_threshold=85, email_threshold=90,
    phone_threshold=85, dob_threshold_days=30
):
    basis_df = normalize(preprocess_basis(basis_df)).to_pandas()
    finacle_df = normalize(preprocess_finacle(finacle_df)).to_pandas()

    matches = []
    mismatches = []
    matched_indices = set()

    for i, b_row in basis_df.iterrows():
        b_phones = [b_row.get("Phone_1_Basis", ""), b_row.get("Phone_2_Basis", ""), b_row.get("Phone_3_Basis", "")]
        best_match = None
        best_score = 0
        best_idx = None

        for j, f_row in finacle_df.iterrows():
            if j in matched_indices:
                continue
            f_phones = [f_row.get("Phone_1_Finacle", ""), f_row.get("Phone_2_Finacle", ""), f_row.get("Phone_3_Finacle", "")]

            name_match, name_score = fuzzy_match_string(b_row["Name"], f_row["Name"], name_threshold)
            email_match, email_score = fuzzy_match_string(b_row["Email_Basis"], f_row["Email_Finacle"], email_threshold)
            phone_match, phone_score = fuzzy_match_phones(b_phones, f_phones, phone_threshold)
            dob_match, dob_score = fuzzy_match_dates(b_row["Date_of_Birth_Basis"], f_row["Date_of_Birth_Finacle"], dob_threshold_days)

            total_score = (name_score * 0.4 + email_score * 0.3 + dob_score * 0.2 + phone_score * 0.1) if name_match else 0

            if total_score > best_score and name_match:
                best_score = total_score
                best_match = f_row
                best_idx = j

        if best_match is not None:
            matches.append({
                "Basis_Name": b_row["Name"],
                "Finacle_Name": best_match["Name"],
                "Email_Basis": b_row["Email_Basis"],
                "Email_Finacle": best_match["Email_Finacle"],
                "DOB_Basis": b_row["Date_of_Birth_Basis"],
                "DOB_Finacle": best_match["Date_of_Birth_Finacle"],
                "Phones_Basis": ", ".join(filter(None, b_phones)),
                "Phones_Finacle": ", ".join(filter(None, [best_match["Phone_1_Finacle"], best_match["Phone_2_Finacle"], best_match["Phone_3_Finacle"]])),
                "Score": round(best_score, 2)
            })
            matched_indices.add(best_idx)
        else:
            mismatches.append({
                "Unmatched_Basis_Name": b_row["Name"],
                "Email_Basis": b_row["Email_Basis"],
                "DOB_Basis": b_row["Date_of_Birth_Basis"],
                "Phones_Basis": ", ".join(filter(None, b_phones))
            })

    for k, f_row in finacle_df.iterrows():
        if k not in matched_indices:
            mismatches.append({
                "Unmatched_Finacle_Name": f_row["Name"],
                "Email_Finacle": f_row["Email_Finacle"],
                "DOB_Finacle": f_row["Date_of_Birth_Finacle"],
                "Phones_Finacle": ", ".join(filter(None, [f_row["Phone_1_Finacle"], f_row["Phone_2_Finacle"], f_row["Phone_3_Finacle"]]))
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
        basis_df = read_file(basis_file)
        finacle_df = read_file(finacle_file)
        matches_df, mismatches_df = find_fuzzy_matches(basis_df, finacle_df)

    st.success("✅ Matching completed!")

    st.subheader("🎯 Matches")
    st.dataframe(matches_df)

    st.download_button(
        "⬇ Download Matches",
        convert_df(matches_df),
        file_name="matches.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.subheader("❌ Mismatches")
    st.dataframe(mismatches_df)

    st.download_button(
        "⬇ Download Mismatches",
        convert_df(mismatches_df),
        file_name="mismatches.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("⬆ Please upload both BASIS and FINACLE files to start matching.")
