import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Comparison Tool", layout="wide")
st.title("📊 Finacle vs Basis Comparator (Row-wise Comparison)")

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date_of_Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3"
    })
    # Keep needed columns and IDs
    return df.select([
        "BRA_CODE", "CUS_NUM", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date_of_Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2"
    })
    df = df.with_columns(pl.lit("").alias("Phone_3"))
    return df.select([
        "ORGKEY", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return df

def normalize_phones(df: pl.DataFrame) -> pl.DataFrame:
    for col in ["Phone_1", "Phone_2", "Phone_3"]:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).fill_null("").cast(pl.Utf8).str.strip_chars().alias(col)
            )
    return df

col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File (CSV/XLSX)", type=["csv", "xlsx"], key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File (CSV/XLSX)", type=["csv", "xlsx"], key="finacle")

if basis_file and finacle_file:
    try:
        # Read files
        basis_df = pl.read_excel(basis_file) if basis_file.name.endswith("xlsx") else pl.read_csv(basis_file)
        finacle_df = pl.read_excel(finacle_file) if finacle_file.name.endswith("xlsx") else pl.read_csv(finacle_file)

        st.subheader("📄 Uploaded Summary")
        st.write(f"🔹 BASIS Rows: {basis_df.height}")
        st.write(f"🔹 FINACLE Rows: {finacle_df.height}")

        # Preprocess & normalize
        basis = normalize_phones(normalize(preprocess_basis(basis_df)))
        finacle = normalize_phones(normalize(preprocess_finacle(finacle_df)))

        # Align lengths for row-wise compare by index
        min_len = min(basis.height, finacle.height)
        basis = basis.head(min_len)
        finacle = finacle.head(min_len)

        # Compare Name, Email, Date_of_Birth
        name_match = basis["Name"] == finacle["Name"]
        email_match = basis["Email"] == finacle["Email"]
        dob_match = basis["Date_of_Birth"] == finacle["Date_of_Birth"]

        # Phone match logic (any-to-any of 3 phones)
        # For each row, check if any phone in basis matches any phone in finacle
        phone_match = []
        for i in range(min_len):
            basis_phones = {basis[i, "Phone_1"], basis[i, "Phone_2"], basis[i, "Phone_3"]}
            finacle_phones = {finacle[i, "Phone_1"], finacle[i, "Phone_2"], finacle[i, "Phone_3"]}
            # Remove empty strings
            basis_phones.discard("")
            finacle_phones.discard("")
            phone_match.append(len(basis_phones.intersection(finacle_phones)) > 0)

        phone_match = pl.Series("phone_match", phone_match)

        # Mismatch mask: if any field mismatches, mark True
        mismatch_mask = ~(name_match & email_match & dob_match & phone_match)

        # Collect mismatches
        mismatches = pl.DataFrame({
            "BRA_CODE": basis["BRA_CODE"],
            "ACCOUNT_NUMBER": basis["CUS_NUM"],
            "ORGKEY": finacle["ORGKEY"],
            "Name_Basis": basis["Name"],
            "Name_Finacle": finacle["Name"],
            "Email_Basis": basis["Email"],
            "Email_Finacle": finacle["Email"],
            "DOB_Basis": basis["Date_of_Birth"],
            "DOB_Finacle": finacle["Date_of_Birth"],
            "Phone_Basis": basis["Phone_1"] + ", " + basis["Phone_2"] + ", " + basis["Phone_3"],
            "Phone_Finacle": finacle["Phone_1"] + ", " + finacle["Phone_2"] + ", " + finacle["Phone_3"]
        }).filter(mismatch_mask)

        st.subheader("🔍 Mismatched Records")

        if mismatches.height > 0:
            df_out = mismatches.to_pandas()
            st.dataframe(df_out.head(1000), use_container_width=True)  # show first 1000

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
            st.success("✅ All rows match on Name, Email, DOB and Phone.")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")
