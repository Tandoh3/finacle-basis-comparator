import streamlit as st
import polars as pl
import pandas as pd
from rapidfuzz import fuzz, process
from io import BytesIO
import time

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔍 Finacle vs Basis Fuzzy Matching (Optimized)")

st.markdown(
    "Upload your BASIS and FINACLE files below (CSV or Excel). "
    "Matching will be done using optimized fuzzy logic for names, emails, dates of birth, and phone numbers."
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
        dtypes = {}
        if is_basis:
            dtypes = {"TEL_NUM": pl.Utf8, "TEL_NUM_2": pl.Utf8, "FAX_NUM": pl.Utf8, "MOB_NUM": pl.Utf8}
        else:
            dtypes = {"PREFERREDPHONE": pl.Utf8, "SMSBANKINGMOBILENUMBER": pl.Utf8}
        return pl.read_csv(file, dtypes=dtypes)
    else:
        schema_overrides = {}
        if is_basis:
            schema_overrides = {"TEL_NUM": pl.Utf8, "TEL_NUM_2": pl.Utf8, "FAX_NUM": pl.Utf8, "MOB_NUM": pl.Utf8}
        else:
            schema_overrides = {"PREFERREDPHONE": pl.Utf8, "SMSBANKINGMOBILENUMBER": pl.Utf8}
        return pl.read_excel(file, schema_overrides=schema_overrides)

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email_Basis",
        "BIR_DATE": "Date_of_Birth_Basis",
        "TEL_NUM": "Phone_1_Basis",
        "TEL_NUM_2": "Phone_2_Basis",
        "FAX_NUM": "Phone_3_Basis",
        "MOB_NUM": "Phone_4_Basis"
    }).select([
        pl.col("Name").fill_null(""),
        pl.col("Email_Basis").fill_null(""),
        "Date_of_Birth_Basis",
        pl.col("Phone_1_Basis").fill_null("").cast(pl.Utf8),
        pl.col("Phone_2_Basis").fill_null("").cast(pl.Utf8),
        pl.col("Phone_3_Basis").fill_null("").cast(pl.Utf8),
        pl.col("Phone_4_Basis").fill_null("").cast(pl.Utf8)
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email_Finacle",
        "CUST_DOB": "Date_of_Birth_Finacle",
        "PREFERREDPHONE": "Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER": "Phone_2_Finacle"
    }).with_columns(pl.lit("").alias("Phone_3_Finacle")).select([
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
            df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase().alias(col))
    return df

def fuzzy_match_string_vectorized(s1: pl.Series, s2: pl.Series, threshold: int) -> pl.Series:
    return pl.Series([fuzz.ratio(a, b) >= threshold for a, b in zip(s1, s2)])

def fuzzy_score_string_vectorized(s1: pl.Series, s2: pl.Series) -> pl.Series:
    return pl.Series([fuzz.ratio(a, b) for a, b in zip(s1, s2)])

def fuzzy_match_phones_vectorized(phones1: pl.Series, phones2: pl.Series, threshold: int) -> pl.Series:
    def _match_phones(p1, p2):
        set1 = set([p for p in p1 if p])
        set2 = set([p for p in p2 if p])
        if not set1 or not set2:
            return True if not set1 and not set2 else False, 100 if not set1 and not set2 else 0
        intersection = set1 & set2
        union = set1 | set2
        score = (len(intersection) / len(union)) * 100 if union else 100
        return score >= threshold, score
    return phones1.zip_with(phones2, lambda a, b: _match_phones(list(a), list(b))[0])

def fuzzy_score_phones_vectorized(phones1: pl.Series, phones2: pl.Series) -> pl.Series:
    def _score_phones(p1, p2):
        set1 = set([p for p in p1 if p])
        set2 = set([p for p in p2 if p])
        if not set1 or not set2:
            return 100 if not set1 and not set2 else 0
        intersection = set1 & set2
        union = set1 | set2
        return (len(intersection) / len(union)) * 100 if union else 100
    return phones1.zip_with(phones2, lambda a, b: _score_phones(list(a), list(b)))

def fuzzy_match_dates_vectorized(d1: pl.Series, d2: pl.Series, threshold_days: int) -> pl.Series:
    def _match_dates(date_str1, date_str2):
        try:
            date1 = pd.to_datetime(date_str1, errors='coerce')
            date2 = pd.to_datetime(date_str2, errors='coerce')
            if pd.isna(date1) or pd.isna(date2):
                return False, 0
            diff = abs((date1 - date2).days)
            return diff <= threshold_days, 100 - (diff / threshold_days * 100) if threshold_days > 0 else 100
        except:
            return False, 0
    return d1.zip_with(d2, lambda a, b: _match_dates(a, b)[0])

def fuzzy_score_dates_vectorized(d1: pl.Series, d2: pl.Series) -> pl.Series:
    def _score_dates(date_str1, date_str2):
        try:
            date1 = pd.to_datetime(date_str1, errors='coerce')
            date2 = pd.to_datetime(date_str2, errors='coerce')
            if pd.isna(date1) or pd.isna(date2):
                return 0
            diff = abs((date1 - date2).days)
            return 100 - (diff / threshold_days * 100) if threshold_days > 0 else 100
        except:
            return 0
    return d1.zip_with(d2, lambda a, b: _score_dates(a, b))

def find_fuzzy_matches_optimized(basis_df: pl.DataFrame, finacle_df: pl.DataFrame, name_threshold=85, email_threshold=90, phone_threshold=85, dob_threshold_days=30):
    basis_df = normalize(preprocess_basis(basis_df))
    finacle_df = normalize(preprocess_finacle(finacle_df))

    basis_df = basis_df.with_columns(
        pl.concat_list(["Phone_1_Basis", "Phone_2_Basis", "Phone_3_Basis", "Phone_4_Basis"]).alias("Phones_Basis")
    )
    finacle_df = finacle_df.with_columns(
        pl.concat_list(["Phone_1_Finacle", "Phone_2_Finacle", "Phone_3_Finacle"]).alias("Phones_Finacle")
    )

    cross_join = basis_df.join(finacle_df, how="cross")

    matches = cross_join.filter(
        (fuzzy_match_string_vectorized(pl.col("Name"), pl.col("Name_right"), name_threshold)) &
        ((fuzzy_match_string_vectorized(pl.col("Email_Basis"), pl.col("Email_Finacle"), email_threshold)) | (pl.col("Email_Basis").is_null()) | (pl.col("Email_Finacle").is_null())) &
        ((fuzzy_match_dates_vectorized(pl.col("Date_of_Birth_Basis"), pl.col("Date_of_Birth_Finacle"), dob_threshold_days)) | (pl.col("Date_of_Birth_Basis").is_null()) | (pl.col("Date_of_Birth_Finacle").is_null())) &
        ((fuzzy_match_phones_vectorized(pl.col("Phones_Basis"), pl.col("Phones_Finacle"), phone_threshold)))
    ).with_columns(
        (
            fuzzy_score_string_vectorized(pl.col("Name"), pl.col("Name_right")) * 0.4 +
            fuzzy_score_string_vectorized(pl.col("Email_Basis"), pl.col("Email_Finacle")) * 0.3 +
            fuzzy_score_dates_vectorized(pl.col("Date_of_Birth_Basis"), pl.col("Date_of_Birth_Finacle")) * 0.2 +
            fuzzy_score_phones_vectorized(pl.col("Phones_Basis"), pl.col("Phones_Finacle")) * 0.1
        ).alias("Score")
    ).sort("Score", descending=True)

    # Handle mismatches (this will be less efficient with cross join for large data)
    # Consider alternative strategies for identifying mismatches with large data
    basis_matched_names = set(matches.select("Name").to_series())
    finacle_matched_names = set(matches.select("Name_right").to_series())

    mismatches_basis = basis_df.filter(~pl.col("Name").is_in(basis_matched_names)).to_pandas()
    mismatches_finacle = finacle_df.filter(~pl.col("Name").is_in(finacle_matched_names)).to_pandas()
    mismatches_df = pd.concat([mismatches_basis, mismatches_finacle], ignore_index=True)

    return matches.to_pandas(), mismatches_df

def convert_df(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# === 3. Main Logic ===
if basis_file and finacle_file:
    with st.spinner("🔄 Matching records, please wait... (Optimized)"):
        start_time = time.time()
        basis_df = read_file(basis_file, is_basis=True)
        finacle_df = read_file(finacle_file, is_basis=False)
        matches_df, mismatches_df = find_fuzzy_matches_optimized(basis_df, finacle_df)
        end_time = time.time()
        st.write(f"Processing time: {end_time - start_time:.2f} seconds")

    st.success("✅ Matching complete! (Optimized)")

    st.subheader("✅ Matches")
    st.dataframe(matches_df)

    st.subheader("❌ Mismatches")
    st.dataframe(mismatches_df)

    if not matches_df.empty:
        excel_data = convert_df(matches_df)
        st.download_button(
            label="📥 Download Matches as Excel",
            data=excel_data,
            file_name="matches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    if not mismatches_df.empty:
        excel_data = convert_df(mismatches_df)
        st.download_button(
            label="📥 Download Mismatches as Excel",
            data=excel_data,
            file_name="mismatches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
else:
    st.info("Please upload both BASIS and FINACLE files to start matching.")