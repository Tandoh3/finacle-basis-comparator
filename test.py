import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Comparator", layout="wide")
st.title("🔍 Finacle vs Basis Comparison Tool")

# Uploads
col1, col2 = st.columns(2)
with col1:
    finacle_file = st.file_uploader("📂 Upload Finacle File", type=["csv", "xlsx"])
with col2:
    basis_file = st.file_uploader("📂 Upload Basis File", type=["csv", "xlsx"])

# Helper for reading Excel or CSV
def read_file(file):
    if file.name.endswith(".xlsx"):
        return pl.read_excel(file)
    return pl.read_csv(file)

# Normalize phone string
def normalize(phone):
    return str(phone).lower().replace(" ", "").replace("-", "").strip()

# Check if any phone matches
def phones_match(phone1, phone2):
    set1 = set(normalize(phone1).split("|"))
    set2 = set(normalize(phone2).split("|"))
    return not set1.isdisjoint(set2)

# Row-level mismatch check
def is_mismatch(row):
    if row["Name_Finacle"].strip().lower() != row["Name_Basis"].strip().lower():
        return True
    if row["Email_Finacle"].strip().lower() != row["Email_Basis"].strip().lower():
        return True
    if row["Date of Birth_Finacle"].strip().lower() != row["Date of Birth_Basis"].strip().lower():
        return True
    if not phones_match(row["Phone_Finacle"], row["Phone_Basis"]):
        return True
    return False

# Styling mismatches
def highlight_mismatches(row):
    styles = []
    for field in ["Name", "Email", "Date of Birth"]:
        val1 = row[f"{field}_Finacle"].strip().lower()
        val2 = row[f"{field}_Basis"].strip().lower()
        match = val1 == val2
        color = "lightgreen" if match else "salmon"
        styles.extend([f"background-color: {color}"] * 2)

    # Phone comparison
    match = phones_match(row["Phone_Finacle"], row["Phone_Basis"])
    color = "lightgreen" if match else "salmon"
    styles.extend([f"background-color: {color}"] * 2)

    return styles

# Process files
if finacle_file and basis_file:
    # Load
    finacle = read_file(finacle_file)
    basis = read_file(basis_file)

    st.success(f"✅ Finacle rows: {finacle.shape[0]}")
    st.success(f"✅ Basis rows: {basis.shape[0]}")

    # Rename columns explicitly
    finacle = finacle.with_columns([
        pl.col("NAME").cast(str).fill_null("").alias("Name_Finacle"),
        pl.col("PREFERREDEMAIL").cast(str).fill_null("").alias("Email_Finacle"),
        pl.col("CUST_DOB").cast(str).fill_null("").alias("Date of Birth_Finacle"),
        (pl.col("PREFERREDPHONE").cast(str).fill_null("") + "|" + pl.col("SMSBANKINGMOBILENUMBER").cast(str).fill_null("")).alias("Phone_Finacle")
    ])

    basis = basis.with_columns([
        pl.col("CUS_SHO_NAME").cast(str).fill_null("").alias("Name_Basis"),
        pl.col("EMAIL").cast(str).fill_null("").alias("Email_Basis"),
        pl.col("BIR_DATE").cast(str).fill_null("").alias("Date of Birth_Basis"),
        (pl.col("TEL_NUM").cast(str).fill_null("") + "|" +
         pl.col("TEL_NUM_2").cast(str).fill_null("") + "|" +
         pl.col("FAX_NUM").cast(str).fill_null("")).alias("Phone_Basis")
    ])

    # Select needed columns
    finacle_clean = finacle.select([col for col in finacle.columns if col.endswith("_Finacle")])
    basis_clean = basis.select([col for col in basis.columns if col.endswith("_Basis")])

    # Combine
    combined = pl.concat([basis_clean, finacle_clean], how="horizontal").to_pandas()

    # Mismatch filter
    combined["Mismatch"] = combined.apply(is_mismatch, axis=1)
    mismatches = combined[combined["Mismatch"]].drop(columns=["Mismatch"])

    st.subheader("❌ Mismatches Only")
    styled_df = mismatches.style.apply(highlight_mismatches, axis=1)
    st.dataframe(styled_df, use_container_width=True)

    # Excel download
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        mismatches.to_excel(writer, index=False, sheet_name="Mismatches")
        writer.save()
    st.download_button("📥 Download Mismatches as Excel", data=output.getvalue(), file_name="finacle_vs_basis_mismatches.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
