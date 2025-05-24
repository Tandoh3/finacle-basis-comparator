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
        "FAX_NUM": "Phone_3"
    })
    return df.select([
        "BRA_CODE", "CUS_NUM", "Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date of Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2"
    })
    df = df.with_columns(pl.lit("").alias("Phone_3"))
    return df.select([
        "ORGKEY", "Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    # Lowercase and strip string columns
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
        # Read large files using Polars
        basis_df = pl.read_excel(basis_file) if basis_file.name.endswith("xlsx") else pl.read_csv(basis_file)
        finacle_df = pl.read_excel(finacle_file) if finacle_file.name.endswith("xlsx") else pl.read_csv(finacle_file)

        st.subheader("📄 Uploaded Summary")
        st.write(f"🔹 BASIS Rows: {basis_df.height}")
        st.write(f"🔹 FINACLE Rows: {finacle_df.height}")

        # Preprocess and normalize
        basis = normalize(preprocess_basis(basis_df))
        finacle = normalize(preprocess_finacle(finacle_df))

        # Ensure phone columns are strings and fill nulls
        basis = basis.with_columns([
            pl.col("Phone_1").fill_null("").cast(pl.Utf8),
            pl.col("Phone_2").fill_null("").cast(pl.Utf8),
            pl.col("Phone_3").fill_null("").cast(pl.Utf8),
        ])

        finacle = finacle.with_columns([
            pl.col("Phone_1").fill_null("").cast(pl.Utf8),
            pl.col("Phone_2").fill_null("").cast(pl.Utf8),
            pl.col("Phone_3").fill_null("").cast(pl.Utf8),
        ])

        # Compare Name, Email, Date of Birth
        name_match = basis["Name"] == finacle["Name"]
        email_match = basis["Email"] == finacle["Email"]
        dob_match = basis["Date of Birth"] == finacle["Date of Birth"]

        # Prepare unified Finacle phone list (unique & non-empty)
        finacle_phones = (
            finacle["Phone_1"].to_list() +
            finacle["Phone_2"].to_list() +
            finacle["Phone_3"].to_list()
        )
        finacle_phones = list(set([p for p in finacle_phones if p]))

        # Phone match if any basis phone is in finacle phones
        phone_match = (
            basis["Phone_1"].is_in(finacle_phones) |
            basis["Phone_2"].is_in(finacle_phones) |
            basis["Phone_3"].is_in(finacle_phones)
        )

        # Mismatch mask: if any of the 4 fields don't match, it's a mismatch
        mismatch_mask = ~(name_match & email_match & dob_match & phone_match)

        # Prepare output with mismatches only
        mismatches = pl.DataFrame({
            "BRA_CODE": basis.get_column("BRA_CODE") if "BRA_CODE" in basis.columns else pl.Series("BRA_CODE", ["N/A"] * basis.height),
            "ACCOUNT_NUMBER": basis.get_column("CUS_NUM") if "CUS_NUM" in basis.columns else pl.Series("ACCOUNT_NUMBER", ["N/A"] * basis.height),
            "ORGKEY": finacle.get_column("ORGKEY") if "ORGKEY" in finacle.columns else pl.Series("ORGKEY", ["N/A"] * finacle.height),
            "Name_Basis": basis["Name"],
            "Name_Finacle": finacle["Name"],
            "Email_Basis": basis["Email"],
            "Email_Finacle": finacle["Email"],
            "DOB_Basis": basis["Date of Birth"],
            "DOB_Finacle": finacle["Date of Birth"],
            "Phone_Basis": basis["Phone_1"] + ", " + basis["Phone_2"] + ", " + basis["Phone_3"],
            "Phone_Finacle": finacle["Phone_1"] + ", " + finacle["Phone_2"] + ", " + finacle["Phone_3"]
        }).filter(mismatch_mask)

        st.subheader("🔍 Mismatched Records")

        if mismatches.height > 0:
            df_out = mismatches.to_pandas()
            st.dataframe(df_out.head(1000), use_container_width=True)  # Show first 1000 rows

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_out.to_excel(writer, index=False, sheet_name="Mismatches")

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
