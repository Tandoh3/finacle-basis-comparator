import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Mismatch Comparator", layout="wide")
st.title("📊 Finacle vs Basis Comparator (Mismatch Finder)")

# --- Preprocessing Functions ---

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
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return df

# --- File Upload ---
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File (CSV/XLSX)", type=["csv", "xlsx"], key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File (CSV/XLSX)", type=["csv", "xlsx"], key="finacle")

# --- Main Logic ---
if basis_file and finacle_file:
    try:
        # Load data
        basis_df = pl.read_excel(basis_file) if basis_file.name.endswith("xlsx") else pl.read_csv(basis_file)
        finacle_df = pl.read_excel(finacle_file) if finacle_file.name.endswith("xlsx") else pl.read_csv(finacle_file)

        st.subheader("📄 Uploaded Summary")
        st.write(f"🔹 BASIS Rows: {basis_df.height}")
        st.write(f"🔹 FINACLE Rows: {finacle_df.height}")

        # Preprocess & normalize
        basis = normalize(preprocess_basis(basis_df))
        finacle = normalize(preprocess_finacle(finacle_df))

        # Fill nulls & cast phones as strings
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

        # Align by index to the shortest dataset
        min_len = min(basis.height, finacle.height)
        basis = basis.head(min_len)
        finacle = finacle.head(min_len)

        # Compare columns
        name_match = basis["Name"] == finacle["Name"]
        email_match = basis["Email"] == finacle["Email"]
        dob_match = basis["Date of Birth"] == finacle["Date of Birth"]

        # Phone match: check if any phone in basis row is in any phone in finacle row
        def phones_match(row_basis, row_finacle):
            basis_phones = {row_basis["Phone_1"], row_basis["Phone_2"], row_basis["Phone_3"]}
            finacle_phones = {row_finacle["Phone_1"], row_finacle["Phone_2"], row_finacle["Phone_3"]}
            # Remove empty strings
            basis_phones.discard("")
            finacle_phones.discard("")
            return len(basis_phones.intersection(finacle_phones)) > 0

        # Compute phone matches row-wise
        phone_matches = []
        for i in range(min_len):
            pb = basis[i]
            pf = finacle[i]
            phone_matches.append(phones_match(pb, pf))
        phone_match = pl.Series(phone_matches)

        # Mismatch mask if any field differs
        mismatch_mask = ~(name_match & email_match & dob_match & phone_match)

        # Build mismatch DataFrame
        mismatches = pl.DataFrame({
            "BRA_CODE": basis["BRA_CODE"],
            "ACCOUNT_NUMBER": basis["CUS_NUM"],
            "ORGKEY": finacle["ORGKEY"],
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
            st.dataframe(df_out.head(1000), use_container_width=True)

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
