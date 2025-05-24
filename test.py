import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Bio-Data Matching", layout="wide")
st.title("🔍 Finacle vs Basis Bio-Data Matching (Partial Match)")

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

def find_potential_matches(basis_df: pl.DataFrame, finacle_df: pl.DataFrame) -> pd.DataFrame:
    # Normalize both dataframes
    basis_normalized = normalize(basis_df)
    finacle_normalized = normalize(finacle_df)

    # Combine phone numbers
    basis_with_phones = combine_phones(basis_normalized, "Basis")
    finacle_with_phones = combine_phones(finacle_normalized, "Finacle")

    # Full outer join on Name
    merged_df = basis_with_phones.join(
        finacle_with_phones,
        on=["Name"],
        how="outer"
    )

    def check_partial_match(row):
        matches = 0
        if row["Email_Basis"] and row["Email_Finacle"] and row["Email_Basis"] == row["Email_Finacle"]:
            matches += 1
        if row["Date_of_Birth_Basis"] and row["Date_of_Birth_Finacle"] and row["Date_of_Birth_Basis"] == row["Date_of_Birth_Finacle"]:
            matches += 1
        basis_phones = set(row.get("Phones_Basis") or [])
        finacle_phones = set(row.get("Phones_Finacle") or [])
        if basis_phones and finacle_phones and basis_phones == finacle_phones:
            matches += 1
        return matches

    pdf = merged_df.to_pandas()
    pdf["Partial_Match_Count"] = pdf.apply(check_partial_match, axis=1)

    # Separate potential matches and mismatches
    potential_matches_df = pdf[pdf["Partial_Match_Count"] > 0]
    mismatches_df = pdf[pdf["Partial_Match_Count"] == 0]

    return potential_matches_df, mismatches_df

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

        # Find potential matches and mismatches
        potential_matches, mismatches = find_potential_matches(basis_processed, finacle_processed)

        st.subheader("🤝 Potential Matches (Same Name, Other Info Match)")
        if not potential_matches.empty:
            st.dataframe(potential_matches, use_container_width=True)
            output_potential = io.BytesIO()
            with pd.ExcelWriter(output_potential, engine="openpyxl") as writer:
                potential_matches.to_excel(writer, index=False, sheet_name="Potential_Matches")
            st.download_button(
                label="📥 Download Potential Matches (Excel)",
                data=output_potential.getvalue(),
                file_name="potential_matches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("No potential matches found based on the criteria.")

        st.subheader("💔 Mismatches (No Other Info Match for Same Name)")
        if not mismatches.empty:
            st.dataframe(mismatches, use_container_width=True)
            output_mismatches = io.BytesIO()
            with pd.ExcelWriter(output_mismatches, engine="openpyxl") as writer:
                mismatches.to_excel(writer, index=False, sheet_name="Mismatches")
            st.download_button(
                label="📥 Download Mismatches (Excel)",
                data=output_mismatches.getvalue(),
                file_name="bio_data_mismatches_no_other_match.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("No mismatches found where the name is the same but other info differs.")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")