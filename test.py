import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Bio-Data Mismatch on Name", layout="wide")
st.title("⚠️ Finacle vs Basis Bio-Data Mismatch Finder (Same Name)")

# === 1. Preprocessing Functions ===

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email_Basis",
        "BIR_DATE": "Date_of_Birth_Basis",
        "TEL_NUM": "Phone_1_Basis",
        "TEL_NUM_2": "Phone_2_Basis",
        "FAX_NUM": "Phone_3_Basis"
    })
    return df.select([
        "Name", "Email_Basis", "Date_of_Birth_Basis", "Phone_1_Basis", "Phone_2_Basis", "Phone_3_Basis"
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email_Finacle",
        "CUST_DOB": "Date_of_Birth_Finacle",
        "PREFERREDPHONE": "Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER": "Phone_2_Finacle"
    })
    df = df.with_columns(pl.lit("").alias("Phone_3_Finacle"))
    return df.select([
        "Name", "Email_Finacle", "Date_of_Birth_Finacle", "Phone_1_Finacle", "Phone_2_Finacle", "Phone_3_Finacle"
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    # Lowercase and strip string columns
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return df

def combine_phones(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
    # Fill nulls and cast phones to string
    df = df.with_columns([
        pl.col(f"Phone_1_{prefix}").fill_null("").cast(pl.Utf8),
        pl.col(f"Phone_2_{prefix}").fill_null("").cast(pl.Utf8),
        pl.col(f"Phone_3_{prefix}").fill_null("").cast(pl.Utf8)
    ])
    # Create a list column of phones
    df = df.with_columns(
        pl.concat_list([f"Phone_1_{prefix}", f"Phone_2_{prefix}", f"Phone_3_{prefix}"]).alias(f"Phones_{prefix}")
    ).drop([f"Phone_1_{prefix}", f"Phone_2_{prefix}", f"Phone_3_{prefix}"])
    return df

def find_bio_data_mismatches_on_name(basis_df: pl.DataFrame, finacle_df: pl.DataFrame) -> pd.DataFrame:
    # Normalize both dataframes
    basis_normalized = normalize(basis_df)
    finacle_normalized = normalize(finacle_df)

    # Combine phone numbers into a list
    basis_with_phones = combine_phones(basis_normalized, "Basis")
    finacle_with_phones = combine_phones(finacle_normalized, "Finacle")

    # Inner join on Name
    merged_df = basis_with_phones.join(
        finacle_with_phones,
        on=["Name"],
        how="inner"
    )

    # Identify mismatches in other bio-data fields
    mismatched_df = merged_df.filter(
        (pl.col("Email_Basis") != pl.col("Email_Finacle")) |
        (pl.col("Date_of_Birth_Basis") != pl.col("Date_of_Birth_Finacle")) |
        (pl.col("Phones_Basis").list.sort() != pl.col("Phones_Finacle").list.sort())
    ).select([
        "Name",
        "Email_Basis",
        "Email_Finacle",
        "Date_of_Birth_Basis",
        "Date_of_Birth_Finacle",
        pl.col("Phones_Basis"),
        pl.col("Phones_Finacle")
    ]).to_pandas()

    return mismatched_df

# === 2. Upload Section ===

col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File (CSV/XLSX)", type=["csv", "xlsx"], key="basis")
with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File (CSV/XLSX)", type=["csv", "xlsx"], key="finacle")

# === 3. Processing Logic ===

if basis_file and finacle_file:
    try:
        # Read files using Polars
        basis_df = pl.read_excel(basis_file) if basis_file.name.endswith("xlsx") else pl.read_csv(basis_file)
        finacle_df = pl.read_excel(finacle_file) if finacle_file.name.endswith("xlsx") else pl.read_csv(finacle_file)

        st.subheader("📄 Uploaded Summary")
        st.write(f"🔹 BASIS Rows: {basis_df.height}")
        st.write(f"🔹 FINACLE Rows: {finacle_df.height}")

        # Preprocess the data
        basis_processed = preprocess_basis(basis_df)
        finacle_processed = preprocess_finacle(finacle_df)

        # Find bio-data mismatches based on matching names
        mismatched_bio_data_df = find_bio_data_mismatches_on_name(basis_processed, finacle_processed)

        st.subheader("🔥 Bio-Data Mismatches (Same Name, Different Other Info)")

        if not mismatched_bio_data_df.empty:
            st.dataframe(mismatched_bio_data_df, use_container_width=True)

            # Export mismatches to Excel for download
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatched_bio_data_df.to_excel(writer, index=False, sheet_name="Bio_Data_Mismatches")

            st.download_button(
                label="📥 Download Bio-Data Mismatches (Excel)",
                data=output.getvalue(),
                file_name="bio_data_mismatches_same_name.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.success("✅ No bio-data mismatches found for records with the same name between Finacle and Basis.")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")