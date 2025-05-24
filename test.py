import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Comparison Tool", layout="wide")
st.title("📊 Finacle vs Basis Comparator (Large Dataset Support)")

# === 1. Preprocessing Functions ===

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date_of_Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3"
    })
    return df.select([
        "BRA_CODE", "CUS_NUM", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    # ======= DEBUG: Print columns to check exact names =======
    st.write("Finacle Columns:", df.columns)

    # Adjust these names to match your exact Finacle file column names (case-sensitive)
    df = df.rename({
        "NAME": "Name",                   # <-- Change "NAME" if your column is different
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date_of_Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2"
    })

    # Add Phone_3 as empty string to align with Basis structure
    df = df.with_columns(pl.lit("").alias("Phone_3"))
    return df.select([
        "ORGKEY", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    # Lowercase and strip string columns for consistent comparison
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return df

# === 2. Upload Section ===

col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File (CSV/XLSX)", type=["csv", "xlsx"], key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File (CSV/XLSX)", type=["csv", "xlsx"], key="finacle")

# === 3. Processing Logic ===

if basis_file and finacle_file:
    try:
        # Read files with polars
        if basis_file.name.endswith("xlsx"):
            basis_df = pl.read_excel(basis_file)
        else:
            basis_df = pl.read_csv(basis_file)

        if finacle_file.name.endswith("xlsx"):
            finacle_df = pl.read_excel(finacle_file)
        else:
            finacle_df = pl.read_csv(finacle_file)

        st.subheader("📄 Uploaded Summary")
        st.write(f"🔹 BASIS Rows: {basis_df.height}")
        st.write(f"🔹 FINACLE Rows: {finacle_df.height}")

        # Preprocess and normalize
        basis = normalize(preprocess_basis(basis_df))
        finacle = normalize(preprocess_finacle(finacle_df))

        # Ensure phone columns are strings and fill nulls
        for df_name, df in {"Basis": basis, "Finacle": finacle}.items():
            df = df.with_columns([
                pl.col("Phone_1").fill_null("").cast(pl.Utf8),
                pl.col("Phone_2").fill_null("").cast(pl.Utf8),
                pl.col("Phone_3").fill_null("").cast(pl.Utf8),
            ])
            if df_name == "Basis":
                basis = df
            else:
                finacle = df

        # === Match logic ===
        # Create keys for matching
        # We'll merge Basis and Finacle on (Name, Email, Date_of_Birth) to compare phones

        basis_key = basis.select(["Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3", "BRA_CODE", "CUS_NUM"])
        finacle_key = finacle.select(["Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3", "ORGKEY"])

        # Merge on Name, Email, Date_of_Birth (inner join)
        merged = basis_key.join(
            finacle_key,
            on=["Name", "Email", "Date_of_Birth"],
            how="outer",
            suffix="_finacle"
        )

        # Fill nulls for phones after join
        merged = merged.with_columns([
            pl.col("Phone_1").fill_null("").cast(pl.Utf8),
            pl.col("Phone_2").fill_null("").cast(pl.Utf8),
            pl.col("Phone_3").fill_null("").cast(pl.Utf8),
            pl.col("Phone_1_finacle").fill_null("").cast(pl.Utf8),
            pl.col("Phone_2_finacle").fill_null("").cast(pl.Utf8),
            pl.col("Phone_3_finacle").fill_null("").cast(pl.Utf8),
        ])

        # Phone matching: check if any Basis phone appears in Finacle phones or vice versa
        def phone_match(row):
            basis_phones = {row["Phone_1"], row["Phone_2"], row["Phone_3"]}
            finacle_phones = {row["Phone_1_finacle"], row["Phone_2_finacle"], row["Phone_3_finacle"]}
            # Remove empty strings
            basis_phones.discard("")
            finacle_phones.discard("")
            # If intersection is empty, phones do not match
            return len(basis_phones.intersection(finacle_phones)) > 0

        # Convert merged to pandas for easy row-wise operation
        merged_pd = merged.to_pandas()
        merged_pd["Phone_Match"] = merged_pd.apply(phone_match, axis=1)

        # Records where any field mismatch:
        # Conditions:
        # - Name, Email, DOB matched in join (so present in both)
        # - But Phone_Match is False or missing values in key columns from one side

        # We'll flag records with any mismatch:
        # - Rows with nulls in Basis keys (BRA_CODE or CUS_NUM) => missing in Basis
        # - Rows with nulls in Finacle key (ORGKEY) => missing in Finacle
        # - Or Phone_Match == False

        mismatch_mask = (
            merged_pd["BRA_CODE"].isnull() | 
            merged_pd["ORGKEY"].isnull() |
            (merged_pd["Phone_Match"] == False)
        )

        mismatches = merged_pd[mismatch_mask]

        st.subheader("🔍 Mismatched Records")

        if not mismatches.empty:
            st.dataframe(mismatches.head(1000), use_container_width=True)

            # Prepare Excel download
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatches.to_excel(writer, index=False, sheet_name="Mismatches")

            st.download_button(
                label="📥 Download Mismatches (Excel)",
                data=output.getvalue(),
                file_name="finacle_basis_mismatches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.success("✅ All records match between Finacle and Basis.")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")
