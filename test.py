import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Comparison Tool", layout="wide")
st.title("📊 Finacle vs Basis Record Comparator")

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date of Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3",
        "CUS_NUM": "Account_Number",
        "BRA_CODE": "BRA_CODE"
    })
    return df.select([
        "BRA_CODE", "Account_Number", "Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date of Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2",
        "ORIGKEY": "ORIGKEY"
    })
    df = df.with_columns(pl.lit("").alias("Phone_3"))
    return df.select([
        "ORIGKEY", "Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        pl.col(col).cast(pl.Utf8).str.strip_chars().str.to_lowercase() for col in df.columns
    ])

# File upload section
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File (CSV/XLSX)", type=["csv", "xlsx"], key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File (CSV/XLSX)", type=["csv", "xlsx"], key="finacle")

if basis_file and finacle_file:
    try:
        # Load large files
        basis_df = pl.read_excel(basis_file) if basis_file.name.endswith("xlsx") else pl.read_csv(basis_file)
        finacle_df = pl.read_excel(finacle_file) if finacle_file.name.endswith("xlsx") else pl.read_csv(finacle_file)

        # Show total rows uploaded
        st.subheader("📄 Uploaded File Summary")
        st.write(f"🔹 BASIS Rows: {basis_df.height}")
        st.write(f"🔹 FINACLE Rows: {finacle_df.height}")

        # Preprocess and normalize
        raw_basis = preprocess_basis(basis_df)
        raw_finacle = preprocess_finacle(finacle_df)

        basis = normalize(raw_basis)
        finacle = normalize(raw_finacle)

        # Align both datasets by row index
        min_len = min(basis.height, finacle.height)
        basis = basis.head(min_len)
        finacle = finacle.head(min_len)
        raw_basis = raw_basis.head(min_len)
        raw_finacle = raw_finacle.head(min_len)

        # Compare Name, Email, DOB
        comparison = pl.DataFrame({
            "Name_match": basis["Name"] == finacle["Name"],
            "Email_match": basis["Email"] == finacle["Email"],
            "DOB_match": basis["Date of Birth"] == finacle["Date of Birth"]
        })

        # Compare Phones: any overlap
        phone_match = (
            basis["Phone_1"].is_in(finacle["Phone_1"]) |
            basis["Phone_1"].is_in(finacle["Phone_2"]) |
            basis["Phone_1"].is_in(finacle["Phone_3"]) |
            basis["Phone_2"].is_in(finacle["Phone_1"]) |
            basis["Phone_2"].is_in(finacle["Phone_2"]) |
            basis["Phone_2"].is_in(finacle["Phone_3"]) |
            basis["Phone_3"].is_in(finacle["Phone_1"]) |
            basis["Phone_3"].is_in(finacle["Phone_2"]) |
            basis["Phone_3"].is_in(finacle["Phone_3"])
        )

        comparison = comparison.with_columns(Phone_match=phone_match)

        # Identify mismatches
        mismatch_mask = ~(
            comparison["Name_match"] &
            comparison["Email_match"] &
            comparison["DOB_match"] &
            comparison["Phone_match"]
        )

        mismatches = pl.DataFrame({
            "BRA_CODE": raw_basis["BRA_CODE"],
            "Account_Number": raw_basis["Account_Number"],
            "ORIGKEY": raw_finacle["ORIGKEY"],
            "Name_Basis": raw_basis["Name"],
            "Name_Finacle": raw_finacle["Name"],
            "Email_Basis": raw_basis["Email"],
            "Email_Finacle": raw_finacle["Email"],
            "DOB_Basis": raw_basis["Date of Birth"],
            "DOB_Finacle": raw_finacle["Date of Birth"],
            "Phone_Basis": raw_basis["Phone_1"] + ", " + raw_basis["Phone_2"] + ", " + raw_basis["Phone_3"],
            "Phone_Finacle": raw_finacle["Phone_1"] + ", " + raw_finacle["Phone_2"] + ", " + raw_finacle["Phone_3"]
        }).filter(mismatch_mask)

        # Display results
        st.subheader("🔍 Mismatched Records")
        if mismatches.height > 0:
            df_out = mismatches.to_pandas()
            st.dataframe(df_out, use_container_width=True)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_out.to_excel(writer, index=False, sheet_name="Mismatches")

            st.download_button(
                label="📥 Download Mismatches as Excel",
                data=output.getvalue(),
                file_name="finacle_basis_mismatches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.success("✅ No mismatches found!")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")
