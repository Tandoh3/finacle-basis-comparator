import streamlit as st
import pandas as pd
from thefuzz import fuzz
from thefuzz import process

st.title("Customer BioData Comparison - BASIS vs FINACLE")

def clean_data(df):
    return df.drop_duplicates(subset=["Name", "DOB", "Email", "Phone1"])

def fuzzy_match(df1, df2, threshold=90):
    matched_rows = []

    for i, row in df1.iterrows():
        name1 = row["Name"]
        matches = process.extractOne(name1, df2["Name"], scorer=fuzz.token_sort_ratio)

        if matches and matches[1] >= threshold:
            matched_row = df2[df2["Name"] == matches[0]].iloc[0]
            result = {
                "Basis_Name": row["Name"],
                "Finacle_Name": matched_row["Name"],
                "Name_Score": matches[1],
                "DOB_Match": row["DOB"] == matched_row["DOB"],
                "Email_Match": row["Email"] == matched_row["Email"],
                "Phone_Match": row["Phone1"] == matched_row["Phone1"]
            }
            matched_rows.append(result)

    return pd.DataFrame(matched_rows)

uploaded_basis = st.file_uploader("Upload BASIS Excel File", type=["xlsx"])
uploaded_finacle = st.file_uploader("Upload FINACLE Excel File", type=["xlsx"])

if uploaded_basis and uploaded_finacle:
    basis_df = pd.read_excel(uploaded_basis)
    finacle_df = pd.read_excel(uploaded_finacle)

    st.subheader("Step 1: Clean & Deduplicate")
    basis_clean = clean_data(basis_df)
    finacle_clean = clean_data(finacle_df)
    st.success(f"Deduplicated BASIS: {len(basis_clean)} rows, FINACLE: {len(finacle_clean)} rows")

    st.subheader("Step 2: Fuzzy Matching...")
    threshold = st.slider("Select name match threshold", 80, 100, 90)
    matched_df = fuzzy_match(basis_clean, finacle_clean, threshold=threshold)

    st.write("🔍 Matching Results")
    st.dataframe(matched_df)

    st.download_button(
        "Download Matched Report",
        matched_df.to_csv(index=False),
        file_name="matched_biodata_report.csv",
        mime="text/csv"
    )
else:
    st.info("Upload both BASIS and FINACLE files to begin.")
