import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="BASIS Migration Bio-Data Check", layout="wide")
st.title("🔍 BASIS Migration Bio-Data Verification")

# === 1. Data Loading and Preprocessing ===

def load_and_preprocess(uploaded_file: st.UploadedFile, file_key: str) -> pl.DataFrame:
    if uploaded_file is None:
        return None

    try:
        if uploaded_file.name.endswith("xlsx"):
            df = pl.read_excel(uploaded_file)
        elif uploaded_file.name.endswith("csv"):
            df = pl.read_csv(uploaded_file)
        else:
            st.error(f"❌ Unsupported file format for {file_key}. Please upload CSV or XLSX.")
            return None

        # Normalize string columns (lowercase and trim)
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
                )
        return df

    except Exception as e:
        st.error(f"❌ Error loading or preprocessing {file_key}: {e}")
        return None

def combine_phones(df: pl.DataFrame, phone_cols: list[str] = None, new_col_name: str = "Phones") -> pl.DataFrame:
    if not phone_cols or not all(col in df.columns for col in phone_cols):
        st.warning(f"⚠️ Phone columns not found in DataFrame. Skipping phone combination.")
        return df.with_columns(pl.lit(None).cast(pl.List(pl.Utf8)).alias(new_col_name))

    # Fill nulls with empty strings and cast to Utf8
    casted_phones = [pl.col(col).fill_null("").cast(pl.Utf8) for col in phone_cols]
    # Create a list column of phones, filtering out empty strings
    df = df.with_columns(
        pl.concat_list(casted_phones)
        .list.filter(pl.element() != "")
        .alias(new_col_name)
    )
    return df

def select_relevant_columns(df: pl.DataFrame) -> pl.DataFrame:
    cols_to_select = ["Name", "Email", "Date_of_Birth"]
    phone_like_cols = [col for col in df.columns if "phone" in col or "mobile" in col or "tel" in col]
    cols_to_select.extend(phone_like_cols)

    available_cols = [col for col in cols_to_select if col in df.columns]
    if not all(base_col in available_cols for base_col in ["Name", "Email", "Date_of_Birth"]):
        st.error("❌ 'Name', 'Email', and 'Date_of_Birth' columns are required for comparison.")
        return None

    return df.select(available_cols)

# === 2. Comparison Logic ===

def find_mismatched_bio_data(original_basis: pl.DataFrame, migrated_basis: pl.DataFrame) -> pd.DataFrame:
    if original_basis is None or migrated_basis is None:
        return pd.DataFrame()

    # Combine phone columns
    original_basis = combine_phones(original_basis)
    migrated_basis = combine_phones(migrated_basis)

    # Select relevant columns
    original_basis_compare = original_basis.select(["Name", "Email", "Date_of_Birth", "Phones"])
    migrated_basis_compare = migrated_basis.select(["Name", "Email", "Date_of_Birth", "Phones"])

    # Join the two DataFrames
    merged_df = original_basis_compare.join(
        migrated_basis_compare,
        on=["Name", "Email", "Date_of_Birth"],
        how="outer",
        suffix="_migrated"
    )

    # Function to check for bio-data mismatch
    def is_mismatch(row):
        name_match = row["Name"] == row["Name_migrated"]
        email_match = row["Email"] == row["Email_migrated"]
        dob_match = row["Date_of_Birth"] == row["Date_of_Birth_migrated"]

        phones_original = set(row.get("Phones") or [])
        phones_migrated = set(row.get("Phones_migrated") or [])
        phones_match = phones_original == phones_migrated

        return not (name_match and email_match and dob_match and phones_match)

    # Apply the mismatch check
    mismatched_df = merged_df.filter(
        pl.struct(pl.all()).apply(is_mismatch)
    ).to_pandas()

    return mismatched_df

# === 3. Streamlit UI ===

st.sidebar.header("📤 Upload Data Files")
original_basis_file = st.sidebar.file_uploader("Upload Original BASIS Data (CSV/XLSX)", type=["csv", "xlsx"], key="original_basis")
migrated_basis_file = st.sidebar.file_uploader("Upload Migrated BASIS Data (CSV/XLSX)", type=["csv", "xlsx"], key="migrated_basis")

if original_basis_file and migrated_basis_file:
    original_basis_df = load_and_preprocess(original_basis_file, "Original BASIS")
    migrated_basis_df = load_and_preprocess(migrated_basis_file, "Migrated BASIS")

    if original_basis_df is not None and migrated_basis_df is not None:
        st.subheader("🔍 Comparing Bio-Data")
        mismatches = find_mismatched_bio_data(original_basis_df, migrated_basis_df)

        if not mismatches.empty:
            st.warning("⚠️ Found Bio-Data Mismatches:")
            st.dataframe(mismatches, use_container_width=True)

            # Add download button for mismatches
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatches.to_excel(writer, index=False, sheet_name="Mismatches")
            st.download_button(
                label="⬇️ Download Mismatched Bio-Data (Excel)",
                data=output.getvalue(),
                file_name="basis_migration_bio_data_mismatches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.success("✅ No Bio-Data mismatches found between the Original and Migrated BASIS data based on Name, Email, Date of Birth, and Phone Number.")