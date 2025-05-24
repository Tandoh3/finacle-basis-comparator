import streamlit as st
import polars as pl
import pandas as pd
import io
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔎 Finacle vs Basis Fuzzy Matching (Large Data Handling)")

# === 1. Preprocessing Functions (Adapt to Lazy Frames if needed) ===
# Most of your preprocessing can be applied to LazyFrames

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

# Fuzzy matching needs the actual data, so it will operate on collected DataFrames

# === 2. Upload Section ===
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File (CSV/XLSX)", type=["csv", "xlsx"], key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File (CSV/XLSX)", type=["csv", "xlsx"], key="finacle")

# === 3. Processing Logic ===
if basis_file and finacle_file:
    try:
        # Create LazyFrames
        if basis_file.name.endswith("xlsx"):
            basis_ldf = pl.scan_excel(basis_file)
        else:
            basis_ldf = pl.scan_csv(basis_file)

        if finacle_file.name.endswith("xlsx"):
            finacle_ldf = pl.scan_excel(finacle_file)
        else:
            finacle_ldf = pl.scan_csv(
                finacle_file,
                schema_overrides={"PREFERREDPHONE": pl.Utf8}
            )

        st.subheader("⏳ Processing Data (Lazy Loading)")
        st.info("Data is being processed using lazy loading. This might take some time for large datasets.")

        # Apply preprocessing (lazily)
        basis_processed_ldf = preprocess_basis_lazy(basis_ldf)
        finacle_processed_ldf = preprocess_finacle_lazy(finacle_ldf)
        basis_normalized_ldf = normalize_lazy(basis_processed_ldf)
        finacle_normalized_ldf = normalize_lazy(finacle_processed_ldf)
        basis_with_phones_ldf = combine_phones_lazy(basis_normalized_ldf, "Basis")
        finacle_with_phones_ldf = combine_phones_lazy(finacle_normalized_ldf, "Finacle")

        # Collect the LazyFrames into DataFrames (BE MINDFUL OF MEMORY HERE)
        st.subheader("⬇️ Collecting Data for Matching (This might be memory-intensive)")
        basis_df = basis_with_phones_ldf.collect()
        finacle_df = finacle_with_phones_ldf.collect()

        st.subheader("✅ Performing Fuzzy Matching")
        matches_df, mismatches_df = find_fuzzy_matches(basis_df, finacle_df)

        st.subheader("✅ Fuzzy Matches (Potential Same Person)")
        if not matches_df.empty:
            st.dataframe(matches_df.head(100), use_container_width=True) # Display a sample
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
            st.dataframe(mismatches_df.head(100), use_container_width=True) # Display a sample
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