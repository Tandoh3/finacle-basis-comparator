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
        "BIR_DATE": "Date of Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3",
        "CUS_NUM": "AccountNumber",
        "BRA_CODE": "BranchCode"
    })
    return df.select([
        "BranchCode", "AccountNumber", "Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date of Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2",
        "ORGKEY": "AccountNumber"
    })
    # Add Phone_3 as empty string to align columns
    df = df.with_columns(pl.lit("").alias("Phone_3"))
    return df.select([
        "AccountNumber", "Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    # Lowercase and strip string columns for uniformity
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
        # Read files with Polars
        basis_df = pl.read_excel(basis_file) if basis_file.name.endswith("xlsx") else pl.read_csv(basis_file)
        finacle_df = pl.read_excel(finacle_file) if finacle_file.name.endswith("xlsx") else pl.read_csv(finacle_file)

        st.subheader("📄 Uploaded Summary")
        st.write(f"🔹 BASIS Rows: {basis_df.height}")
        st.write(f"🔹 FINACLE Rows: {finacle_df.height}")

        # Preprocess & normalize
        basis = normalize(preprocess_basis(basis_df))
        finacle = normalize(preprocess_finacle(finacle_df))

        # Ensure phone columns are strings and fill nulls
        for df_name, df in [("basis", basis), ("finacle", finacle)]:
            df = df.with_columns([
                pl.col("Phone_1").fill_null("").cast(pl.Utf8),
                pl.col("Phone_2").fill_null("").cast(pl.Utf8),
                pl.col("Phone_3").fill_null("").cast(pl.Utf8),
            ])
            if df_name == "basis":
                basis = df
            else:
                finacle = df

        # Join on AccountNumber to align rows
        merged = basis.join(finacle, on="AccountNumber", how="inner", suffix="_finacle")

        # Compare columns
        merged = merged.with_columns([
            (pl.col("Name") == pl.col("Name_finacle")).alias("NameMatch"),
            (pl.col("Email") == pl.col("Email_finacle")).alias("EmailMatch"),
            (pl.col("Date of Birth") == pl.col("Date of Birth_finacle")).alias("DOBMatch"),
            # Phone match: check if any Basis phone in Finacle phones list
            (
                pl.col("Phone_1").is_in(
                    pl.concat_list([pl.col("Phone_1_finacle"), pl.col("Phone_2_finacle"), pl.col("Phone_3_finacle")])
                ) |
                pl.col("Phone_2").is_in(
                    pl.concat_list([pl.col("Phone_1_finacle"), pl.col("Phone_2_finacle"), pl.col("Phone_3_finacle")])
                ) |
                pl.col("Phone_3").is_in(
                    pl.concat_list([pl.col("Phone_1_finacle"), pl.col("Phone_2_finacle"), pl.col("Phone_3_finacle")])
                )
            ).alias("PhoneMatch")
        ])

        # Filter mismatches (where any of the four checks fail)
        mismatches = merged.filter(~(pl.col("NameMatch") & pl.col("EmailMatch") & pl.col("DOBMatch") & pl.col("PhoneMatch")))

        st.subheader("🔍 Mismatched Records")

        if mismatches.height > 0:
            # Show first 1000 mismatches in Streamlit
            st.dataframe(mismatches.select([
                "BranchCode",
                "AccountNumber",
                "Name", "Name_finacle",
                "Email", "Email_finacle",
                "Date of Birth", "Date of Birth_finacle",
                "Phone_1", "Phone_2", "Phone_3",
                "Phone_1_finacle", "Phone_2_finacle", "Phone_3_finacle"
            ]).head(1000).to_pandas(), use_container_width=True)

            # Prepare Excel for download
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatches.select([
                    "BranchCode",
                    "AccountNumber",
                    "Name", "Name_finacle",
                    "Email", "Email_finacle",
                    "Date of Birth", "Date of Birth_finacle",
                    "Phone_1", "Phone_2", "Phone_3",
                    "Phone_1_finacle", "Phone_2_finacle", "Phone_3_finacle"
                ]).to_pandas().to_excel(writer, index=False, sheet_name="Mismatches")

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
