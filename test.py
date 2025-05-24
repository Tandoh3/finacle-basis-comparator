import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Bio-Data Comparison", layout="wide")
st.title("🧬 Finacle vs Basis Bio-Data Comparator")

# === 1. Preprocessing Functions ===

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date_of_Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3"
    })
    return df.select([
        "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
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
        "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    # Lowercase and strip string columns
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return df

def combine_phones(df: pl.DataFrame) -> pl.DataFrame:
    # Fill nulls and cast phones to string
    df = df.with_columns([
        pl.col("Phone_1").fill_null("").cast(pl.Utf8),
        pl.col("Phone_2").fill_null("").cast(pl.Utf8),
        pl.col("Phone_3").fill_null("").cast(pl.Utf8)
    ])
    # Create a list column of phones
    df = df.with_columns(
        pl.concat_list(["Phone_1", "Phone_2", "Phone_3"]).alias("Phones")
    )
    return df.drop(["Phone_1", "Phone_2", "Phone_3"])

def compare_bio_data(basis_df: pl.DataFrame, finacle_df: pl.DataFrame) -> pd.DataFrame:
    # Normalize both dataframes
    basis_normalized = normalize(basis_df)
    finacle_normalized = normalize(finacle_df)

    # Combine phone numbers into a list for comparison
    basis_with_phones = combine_phones(basis_normalized)
    finacle_with_phones = combine_phones(finacle_normalized)

    # Outer join on Name, Email, and Date of Birth
    merged_df = basis_with_phones.join(
        finacle_with_phones,
        on=["Name", "Email", "Date_of_Birth"],
        how="outer",
        suffix="_finacle"
    )

    # Function to check if phone lists are the same (ignoring order)
    def are_phones_same(row):
        basis_phones = set(row.get("Phones") or [])
        finacle_phones = set(row.get("Phones_finacle") or [])
        return basis_phones == finacle_phones

    # Apply the phone comparison and flag mismatches
    mismatched_df = merged_df.filter(
        (pl.col("Name").is_not_null()) & (pl.col("Email").is_not_null()) & (pl.col("Date_of_Birth").is_not_null()) &
        (
            (pl.col("Phones").fill_null(pl.Series([[]])).list.sort() != pl.col("Phones_finacle").fill_null(pl.Series([[]])).list.sort()) |
            (pl.col("Phones").is_null() != pl.col("Phones_finacle").is_null()) # Handle cases where one side has no phones
        )
    ).select([
        "Name",
        "Email",
        "Date_of_Birth",
        pl.col("Phones").alias("Basis_Phones"),
        pl.col("Phones_finacle").alias("Finacle_Phones")
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

        # Compare bio-data
        mismatched_bio_data_df = compare_bio_data(basis_processed, finacle_processed)

        st.subheader("❌ Bio-Data Mismatches (Name, Email, DOB, Phone)")

        if not mismatched_bio_data_df.empty:
            st.dataframe(mismatched_bio_data_df, use_container_width=True)

            # Export mismatches to Excel for download
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatched_bio_data_df.to_excel(writer, index=False, sheet_name="Bio_Data_Mismatches")

            st.download_button(
                label="📥 Download Bio-Data Mismatches (Excel)",
                data=output.getvalue(),
                file_name="bio_data_mismatches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.success("✅ No bio-data mismatches found between Finacle and Basis based on Name, Email, Date of Birth, and Phone Number.")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")