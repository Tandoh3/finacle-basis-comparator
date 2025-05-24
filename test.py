import streamlit as st
import duckdb
import pandas as pd
from thefuzz import fuzz
import io

st.set_page_config(page_title="DuckDB Fuzzy Matcher", layout="wide")
st.title("🔍 Fuzzy Match Finacle vs Basis Using DuckDB")

# Upload CSVs
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS CSV", type="csv", key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE CSV", type="csv", key="finacle")

if basis_file and finacle_file:
    with st.spinner("⏳ Loading files into DuckDB..."):

        # Load data into DuckDB from CSVs
        con = duckdb.connect()
        con.execute("INSTALL sqlite_scanner; LOAD sqlite_scanner;")
        con.sql("CREATE TABLE basis AS SELECT * FROM read_csv_auto($1)", [basis_file])
        con.sql("CREATE TABLE finacle AS SELECT * FROM read_csv_auto($1)", [finacle_file])

        # Clean column names (use standard ones)
        basis = con.execute("SELECT BRA_CODE, CUS_NUM AS ACCOUNT_NUMBER, CUS_SHO_NAME AS Name, EMAIL AS Email, BIR_DATE AS DOB FROM basis").df()
        finacle = con.execute("SELECT ORGKEY, NAME AS Name, PREFERREDEMAIL AS Email, CUST_DOB AS DOB FROM finacle").df()

    # Normalize
    def normalize(col):
        return col.astype(str).str.strip().str.lower()

    basis["Name_norm"] = normalize(basis["Name"])
    finacle["Name_norm"] = normalize(finacle["Name"])
    basis["DOB_norm"] = pd.to_datetime(basis["DOB"], errors='coerce').dt.date
    finacle["DOB_norm"] = pd.to_datetime(finacle["DOB"], errors='coerce').dt.date

    # Limit matches for performance during testing (remove in production)
    basis = basis.dropna(subset=["Name_norm", "DOB_norm"]).head(5000)
    finacle = finacle.dropna(subset=["Name_norm", "DOB_norm"]).head(5000)

    st.success(f"✅ Loaded {len(basis)} BASIS records and {len(finacle)} FINACLE records")

    # Match using Fuzzy + DOB
    st.info("🔍 Matching records using fuzzy name match and DOB...")

    matches = []
    for i, b_row in basis.iterrows():
        for j, f_row in finacle.iterrows():
            dob_match = b_row["DOB_norm"] == f_row["DOB_norm"]
            name_score = fuzz.token_sort_ratio(b_row["Name_norm"], f_row["Name_norm"])

            if dob_match and name_score > 85:
                matches.append({
                    "BRA_CODE": b_row["BRA_CODE"],
                    "ACCOUNT_NUMBER": b_row["ACCOUNT_NUMBER"],
                    "ORGKEY": f_row["ORGKEY"],
                    "Name_Basis": b_row["Name"],
                    "Name_Finacle": f_row["Name"],
                    "DOB_Basis": b_row["DOB"],
                    "DOB_Finacle": f_row["DOB"],
                    "Email_Basis": b_row["Email"],
                    "Email_Finacle": f_row["Email"],
                    "Match_Score": name_score
                })

    matched_df = pd.DataFrame(matches)

    if not matched_df.empty:
        st.subheader("✅ Matched Records")
        st.dataframe(matched_df, use_container_width=True)

        # Download
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            matched_df.to_excel(writer, index=False, sheet_name="Matches")

        st.download_button(
            label="📥 Download Matched Results",
            data=output.getvalue(),
            file_name="fuzzy_matches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("⚠️ No matches found based on DOB and fuzzy name similarity.")

