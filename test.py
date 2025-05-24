import streamlit as st
import dask.dataframe as dd
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Comparison Tool", layout="wide")
st.title("📊 Finacle vs Basis Record Comparator")

# Helper functions
def preprocess_basis(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "BRA_CODE": "BRA_CODE",
        "CUS_NUM": "ACCOUNT_NUMBER",
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date of Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3"
    })
    return df[["BRA_CODE", "ACCOUNT_NUMBER", "Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"]]

def preprocess_finacle(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "ORIGKEY": "ORIGKEY",
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date of Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2"
    })
    df["Phone_3"] = ""
    return df[["ORIGKEY", "Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"]]

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(lambda col: col.astype(str).str.strip().str.lower() if col.dtype == object else col)

def dask_read(file):
    if file.name.endswith("csv"):
        return dd.read_csv(file)
    else:
        raise ValueError("Dask currently doesn't support XLSX in-memory reading reliably. Convert to CSV.")

# Upload section
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File (CSV)", type=["csv"], key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File (CSV)", type=["csv"], key="finacle")

if basis_file and finacle_file:
    try:
        with st.spinner("🔄 Processing large files... please wait..."):
            basis_ddf = dask_read(basis_file)
            finacle_ddf = dask_read(finacle_file)

            basis_df = basis_ddf.compute()
            finacle_df = finacle_ddf.compute()

            st.subheader("📄 Uploaded File Summary")
            st.write(f"🔹 BASIS Rows: {len(basis_df)}")
            st.write(f"🔹 FINACLE Rows: {len(finacle_df)}")

            # Preprocess
            basis = normalize(preprocess_basis(basis_df))
            finacle = normalize(preprocess_finacle(finacle_df))

            # For demo purposes, match by row index
            min_len = min(len(basis), len(finacle))
            basis = basis.iloc[:min_len]
            finacle = finacle.iloc[:min_len]

            # Compare fields
            result = pd.DataFrame({
                "BRA_CODE": basis["BRA_CODE"],
                "ACCOUNT_NUMBER": basis["ACCOUNT_NUMBER"],
                "ORIGKEY": finacle["ORIGKEY"],
                "Name_Basis": basis["Name"],
                "Name_Finacle": finacle["Name"],
                "Email_Basis": basis["Email"],
                "Email_Finacle": finacle["Email"],
                "DOB_Basis": basis["Date of Birth"],
                "DOB_Finacle": finacle["Date of Birth"],
                "Phone_Basis": basis["Phone_1"] + ", " + basis["Phone_2"] + ", " + basis["Phone_3"],
                "Phone_Finacle": finacle["Phone_1"] + ", " + finacle["Phone_2"] + ", " + finacle["Phone_3"]
            })

            # Flag mismatches
            result["Name_Match"] = result["Name_Basis"] == result["Name_Finacle"]
            result["Email_Match"] = result["Email_Basis"] == result["Email_Finacle"]
            result["DOB_Match"] = result["DOB_Basis"] == result["DOB_Finacle"]
            result["Phone_Match"] = result.apply(
                lambda row: any(p in row["Phone_Finacle"] for p in row["Phone_Basis"].split(", ")), axis=1
            )

            mismatches = result[
                ~(result["Name_Match"] & result["Email_Match"] & result["DOB_Match"] & result["Phone_Match"])
            ]

        st.subheader("🔍 Mismatched Records")
        if not mismatches.empty:
            st.dataframe(mismatches.drop(columns=["Name_Match", "Email_Match", "DOB_Match", "Phone_Match"]), use_container_width=True)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatches.to_excel(writer, index=False, sheet_name="Mismatches")

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
