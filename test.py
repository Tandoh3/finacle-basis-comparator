import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Comparison Tool", layout="wide")
st.title("📊 Finacle vs Basis Record Comparator")

# Function to preprocess and standardize column names
def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({
        "Cus_sho_name": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date of Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3"
    }).select(["Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({
        "Name": "Name",
        "Preferredemail": "Email",
        "Cust_Dob": "Date of Birth",
        "PrefferedPhonenumber": "Phone_1",
        "SMSBankingMobileNumber": "Phone_2"
    }).with_columns([
        pl.lit(None).alias("Phone_3")
    ]).select(["Name", "Email", "Date of Birth", "Phone_1", "Phone_2", "Phone_3"])

# Function to normalize strings
def normalize_string(s):
    return s.strip().lower() if isinstance(s, str) else s

# Comparison logic
def compare_rows(row_basis, row_finacle):
    result = {}
    for col in ["Name", "Email", "Date of Birth"]:
        result[col] = row_basis[col] == row_finacle[col]

    # Compare phones (match if any phone matches)
    basis_phones = {row_basis["Phone_1"], row_basis["Phone_2"], row_basis["Phone_3"]}
    finacle_phones = {row_finacle["Phone_1"], row_finacle["Phone_2"], row_finacle["Phone_3"]}
    result["Phone"] = not basis_phones.isdisjoint(finacle_phones)

    return result

# Color cell background
def color_cell(val, match):
    color = "lightgreen" if match else "lightcoral"
    return f"background-color: {color}"

# Upload files
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File", type=["csv", "xlsx"], key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File", type=["csv", "xlsx"], key="finacle")

if basis_file and finacle_file:
    try:
        # Read files using pandas for compatibility
        if basis_file.name.endswith(".csv"):
            basis_raw = pl.read_csv(basis_file)
        else:
            basis_raw = pl.read_excel(basis_file)

        if finacle_file.name.endswith(".csv"):
            finacle_raw = pl.read_csv(finacle_file)
        else:
            finacle_raw = pl.read_excel(finacle_file)

        # Show total rows uploaded
        st.subheader("📄 Uploaded File Summary")
        st.write(f"🔹 BASIS Rows: {basis_raw.height}")
        st.write(f"🔹 FINACLE Rows: {finacle_raw.height}")

        # Preprocess both files
        basis = preprocess_basis(basis_raw).with_columns(
            [pl.col(col).cast(pl.Utf8).str.strip_chars().str.to_lowercase() for col in basis_raw.columns if col]
        )
        finacle = preprocess_finacle(finacle_raw).with_columns(
            [pl.col(col).cast(pl.Utf8).str.strip_chars().str.to_lowercase() for col in finacle_raw.columns if col]
        )

        # Ensure same length or align by index
        min_len = min(basis.height, finacle.height)
        basis = basis.slice(0, min_len)
        finacle = finacle.slice(0, min_len)

        # Compare each row
        comparison_results = []
        mismatch_rows = []

        for i in range(min_len):
            row_b = basis.row(i, named=True)
            row_f = finacle.row(i, named=True)
            match_result = compare_rows(row_b, row_f)
            comparison_results.append(match_result)
            if not all(match_result.values()):
                mismatch_rows.append({
                    "Name_Basis": row_b["Name"],
                    "Name_Finacle": row_f["Name"],
                    "Email_Basis": row_b["Email"],
                    "Email_Finacle": row_f["Email"],
                    "DOB_Basis": row_b["Date of Birth"],
                    "DOB_Finacle": row_f["Date of Birth"],
                    "Phone_Basis": ", ".join(filter(None, [row_b["Phone_1"], row_b["Phone_2"], row_b["Phone_3"]])),
                    "Phone_Finacle": ", ".join(filter(None, [row_f["Phone_1"], row_f["Phone_2"], row_f["Phone_3"]]))
                })

        st.subheader("🔍 Mismatched Records")
        if mismatch_rows:
            mismatch_df = pd.DataFrame(mismatch_rows)

            def highlight_mismatches(row):
                styles = []
                for b, f in [
                    ("Name_Basis", "Name_Finacle"),
                    ("Email_Basis", "Email_Finacle"),
                    ("DOB_Basis", "DOB_Finacle"),
                    ("Phone_Basis", "Phone_Finacle"),
                ]:
                    match = str(row[b]).strip().lower() == str(row[f]).strip().lower()
                    styles.append("background-color: lightgreen" if match else "background-color: lightcoral")
                    styles.append("background-color: lightgreen" if match else "background-color: lightcoral")
                return styles

            styled_df = mismatch_df.style.apply(highlight_mismatches, axis=1)
            st.dataframe(styled_df, use_container_width=True)

            # Download button
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatch_df.to_excel(writer, index=False, sheet_name="Mismatches")
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
