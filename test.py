import streamlit as st
import polars as pl
import pandas as pd
import io
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔎 Finacle vs Basis Fuzzy Matching (Large Data Handling)")

# === 1. Preprocessing Functions ===

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
        "Name", "Email_Basis", "Date_of_Birth_Basis", "Phone_1_Basis", "Phone_2_Basis", "Phone_3_Basis"
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email_Finacle",
        "CUST_DOB": "Date_of_Birth_Finacle",
        "PREFERREDPHONE": "Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER": "Phone_2_Finacle"
    })
    df = df.with_columns(pl.lit("").alias("Phone_3_Finacle"))
    return df.with_columns([
        pl.col("Phone_1_Finacle").cast(pl.Utf8),
        pl.col("Phone_2_Finacle").cast(pl.Utf8),
        pl.col("Phone_3_Finacle").cast(pl.Utf8)
    ]).select([
        "Name", "Email_Finacle", "Date_of_Birth_Finacle", "Phone_1_Finacle", "Phone_2_Finacle", "Phone_3_Finacle"
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return df

def combine_phones(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
    df = df.with_columns([
        pl.col(f"Phone_1_{prefix}").fill_null("").cast(pl.Utf8),
        pl.col(f"Phone_2_{prefix}").fill_null("").cast(pl.Utf8),
        pl.col(f"Phone_3_{prefix}").fill_null("").cast(pl.Utf8)
    ]).with_columns(
        pl.concat_list([f"Phone_1_{prefix}", f"Phone_2_{prefix}", f"Phone_3_{prefix}"]).alias(f"Phones_{prefix}")
    ).drop([f"Phone_1_{prefix}", f"Phone_2_{prefix}", f"Phone_3_{prefix}"])
    return df

def fuzzy_match_string(s1, s2, threshold=80):
    if not s1 or not s2:
        return False, 0
    score = fuzz.ratio(s1, s2)
    return score >= threshold, score

def fuzzy_match_phones(list1, list2, threshold=85):
    if not list1 or not list2:
        return False, 0
    set1 = set(list1)
    set2 = set(list2)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    if not union:
        return True, 100
    score = (len(intersection) / len(union)) * 100
    return score >= threshold, score

def fuzzy_match_dates(d1, d2, threshold_days=30):
    try:
        if not d1 or not d2:
            return False, 0
        date1 = pd.to_datetime(d1, errors='coerce')
        date2 = pd.to_datetime(d2, errors='coerce')
        if pd.isna(date1) or pd.isna(date2):
            return False, 0
        diff = abs((date1 - date2).days)
        return diff <= threshold_days, 100 - (diff / threshold_days * 100) if threshold_days > 0 else 100
    except Exception:
        return False, 0

# === 2. Fuzzy Matching Function ===
def find_fuzzy_matches(basis_df: pl.DataFrame, finacle_df: pl.DataFrame, name_threshold=85, email_threshold=90, phone_threshold=85, dob_threshold_days=30):
    basis_normalized = normalize(basis_df).to_pandas()
    finacle_normalized = normalize(finacle_df).to_pandas()

    basis_with_phones = basis_normalized.apply(lambda row: {'Name': row['Name'], 'Email_Basis': row['Email_Basis'], 'Date_of_Birth_Basis': row['Date_of_Birth_Basis'], 'Phones_Basis': [p for p in [row['Phone_1_Basis'], row['Phone_2_Basis'], row['Phone_3_Basis']] if p]}, axis=1).tolist()
    finacle_with_phones = finacle_normalized.apply(lambda row: {'Name': row['Name'], 'Email_Finacle': row['Email_Finacle'], 'Date_of_Birth_Finacle': row['Date_of_Birth_Finacle'], 'Phones_Finacle': [p for p in [row['Phone_1_Finacle'], row['Phone_2_Finacle'], row['Phone_3_Finacle']] if p]}, axis=1).tolist()

    matches = []
    mismatches = []
    matched_indices_finacle = set()

    for basis_record in basis_with_phones:
        best_match = None
        best_score = 0
        best_index_finacle = -1

        for i, finacle_record in enumerate(finacle_with_phones):
            if i in matched_indices_finacle:
                continue

            name_match, name_score = fuzzy_match_string(basis_record['Name'], finacle_record['Name'], name_threshold)
            email_match, email_score = fuzzy_match_string(basis_record.get('Email_Basis'), finacle_record.get('Email_Finacle'), email_threshold)
            dob_match, dob_score = fuzzy_match_dates(basis_record.get('Date_of_Birth_Basis'), finacle_record.get('Date_of_Birth_Finacle'), dob_threshold_days)
            phone_match, phone_score = fuzzy_match_phones(basis_record.get('Phones_Basis'), finacle_record.get('Phones_Finacle'), phone_threshold)

            combined_score = (name_score * 0.4) + (email_score * 0.3) + (dob_score * 0.2) + (phone_score * 0.1) if name_match else 0

            if combined_score > best_score and name_match:
                best_score = combined_score
                best_match = finacle_record
                best_index_finacle = i

        if best_match:
            matches.append({
                "Basis_Name": basis_record['Name'],
                "Finacle_Name": best_match['Name'],
                "Email_Basis": basis_record.get('Email_Basis'),
                "Email_Finacle": best_match.get('Email_Finacle'),
                "DOB_Basis": basis_record.get('Date_of_Birth_Basis'),
                "DOB_Finacle": best_match.get('Date_of_Birth_Finacle'),
                "Phones_Basis": basis_record.get('Phones_Basis'),
                "Phones_Finacle": best_match.get('Phones_Finacle'),
                "Similarity_Score": best_score
            })
            matched_indices_finacle.add(best_index_finacle)
        else:
            mismatches.append({
                "Basis_Name": basis_record['Name'],
                "Email_Basis": basis_record.get('Email_Basis'),
                "DOB_Basis": basis_record.get('Date_of_Birth_Basis'),
                "Phones_Basis": basis_record.get('Phones_Basis')
            })

    for i, finacle_record in enumerate(finacle_with_phones):
        if i not in matched_indices_finacle:
            mismatches.append({
                "Finacle_Name": finacle_record['Name'],
                "Email_Finacle": finacle_record.get('Email_Finacle'),
                "DOB_Finacle": finacle_record.get('Date_of_Birth_Finacle'),
                "Phones_Finacle": finacle_record.get('Phones_Finacle')
            })

    return pd.DataFrame(matches), pd.DataFrame(mismatches)

# === 3. Upload Section ===

col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File (CSV/XLSX)", type=["csv", "xlsx"], key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File (CSV/XLSX)", type=["csv", "xlsx"], key="finacle")

# === 4. Processing Logic ===

if basis_file and finacle_file:
    try:
        # Read files using Polars
        basis_df_lazy = pl.scan_excel(basis_file) if basis_file.name.endswith("xlsx") else pl.scan_csv(basis_file)
        finacle_df_lazy = pl.scan_excel(finacle_file) if finacle_file.name.endswith("xlsx") else pl.scan_csv(
            finacle_file,
            schema_overrides={"PREFERREDPHONE": pl.Utf8}
        )

        st.subheader("⏳ Processing Data (Lazy Loading)")
        st.info("Data is being processed using lazy loading. This might take some time for large datasets.")

        basis_processed_lazy = preprocess_basis_lazy(basis_df_lazy)
        finacle_processed_lazy = preprocess_finacle_lazy(finacle_df_lazy)
        basis_normalized_lazy = normalize_lazy(basis_processed_lazy)
        finacle_normalized_lazy = normalize_lazy(finacle_processed_lazy)
        basis_with_phones_lazy = combine_phones_lazy(basis_normalized_lazy, "Basis")
        finacle_with_phones_lazy = combine_phones_lazy(finacle_normalized_lazy, "Finacle")

        st.subheader("⬇️ Collecting Data for Matching")
        basis_df = basis_with_phones_lazy.collect()
        finacle_df = finacle_with_phones_lazy.collect()

        st.subheader("✅ Performing Fuzzy Matching")
        matches_df, mismatches_df = find_fuzzy_matches(basis_df, finacle_df)

        st.subheader("✅ Fuzzy Matches (Potential Same Person)")
        if not matches_df.empty:
            st.dataframe(matches_df.head(100), use_container_width=True)
            output_matches = io.BytesIO()
            with pd.ExcelWriter(output_matches, engine="openpyxl") as writer:
                matches_df.to_excel(writer, index=False, sheet_name="Fuzzy_Matches")
            st.download_button(
                label="📥 Download Fuzzy Matches (Excel)",
                data=output_matches.getvalue(),
                file_name="fuzzy_matches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("No fuzzy matches found based on the defined thresholds.")

        st.subheader("💔 Mismatches (No Significant Fuzzy Match)")
        if not mismatches_df.empty:
            st.dataframe(mismatches_df.head(100), use_container_width=True)
            output_mismatches = io.BytesIO()
            with pd.ExcelWriter(output_mismatches, engine="openpyxl") as writer:
                mismatches_df.to_excel(writer, index=False, sheet_name="Mismatches")
            st.download_button(
                label="📥 Download Mismatches (Excel)",
                data=output_mismatches.getvalue(),
                file_name="fuzzy_mismatches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("No significant mismatches found.")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")

# Lazy preprocessing functions (for potentially large datasets)
def preprocess_basis_lazy(ldf: pl.LazyFrame) -> pl.LazyFrame:
    return ldf.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email_Basis",
        "BIR_DATE": "Date_of_Birth_Basis",
        "TEL_NUM": "Phone_1_Basis",
        "TEL_NUM_2": "Phone_2_Basis",
        "FAX_NUM": "Phone_3_Basis"
    }).select([
        "Name", "Email_Basis", "Date_of_Birth_Basis", "Phone_1_Basis", "Phone_2_Basis", "Phone_3_Basis"
    ])

def preprocess_finacle_lazy(ldf: pl.LazyFrame) -> pl.LazyFrame:
    ldf = ldf.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email_Finacle",
        "CUST_DOB": "Date_of_Birth_Finacle",
        "PREFERREDPHONE": "Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER": "Phone_2_Finacle"
    }).with_columns(pl.lit("").alias("Phone_3_Finacle"))
    return ldf.with_columns([
        pl.col("Phone_1_Finacle").cast(pl.Utf8),
        pl.col("Phone_2_Finacle").cast(pl.Utf8),
        pl.col("Phone_3_Finacle").cast(pl.Utf8)
    ]).select([
        "Name", "Email_Finacle", "Date_of_Birth_Finacle", "Phone_1_Finacle", "Phone_2_Finacle", "Phone_3_Finacle"
    ])

def normalize_lazy(ldf: pl.LazyFrame) -> pl.LazyFrame:
    for col in ldf.columns:
        if ldf.schema[col] == pl.Utf8:
            ldf = ldf.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return ldf

def combine_phones_lazy(ldf: pl.LazyFrame, prefix: str) -> pl.LazyFrame:
    ldf = ldf.with_columns([
        pl.col(f"Phone_1_{prefix}").fill_null("").cast(pl.Utf8),
        pl.col(f"Phone_2_{prefix}").fill_null("").cast(pl.Utf8),
        pl.col(f"Phone_3_{prefix}").fill_null("").cast(pl.Utf8)
    ]).with_columns(
        pl.concat_list([f"Phone_1_{prefix}", f"Phone_2_{prefix}", f"Phone_3_{prefix}"]).alias(f"Phones_{prefix}")
    ).drop([f"Phone_1_{prefix}", f"Phone_2_{prefix}", f"Phone_3_{prefix}"])
    return ldf