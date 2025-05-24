import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Comparison Tool", layout="wide")
st.title("📊 Finacle vs Basis Comparator (Large Dataset Support)")

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
    # Create a unique key per person (assuming CUS_NUM is unique in BASIS)
    df = df.with_columns(pl.col("CUS_NUM").cast(pl.Utf8).alias("UniqueKey"))
    return df.select([
        "UniqueKey", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date_of_Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2"
    })
    df = df.with_columns([
        pl.lit("").alias("Phone_3"),
        pl.col("ORGKEY").cast(pl.Utf8).alias("UniqueKey")  # unique key in FINACLE
    ])
    return df.select([
        "UniqueKey", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
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
    return df

def aggregate_person(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    # Explode phones into rows and normalize each phone
    phones_exploded = (
        df.select(["Name", "Email", "Date_of_Birth", "Phones"])
        .explode("Phones")
        .with_columns(
            pl.col("Phones").str.strip_chars().str.to_lowercase().alias("Phone_Normalized")
        )
        .drop("Phones")
    )
    # Join normalized phones back to df for grouping
    df_with_phones = df.join(phones_exploded, on=["Name", "Email", "Date_of_Birth"], how="left")

    # Aggregate unique phones, unique keys, and record counts
    agg = df_with_phones.groupby(["Name", "Email", "Date_of_Birth"]).agg([
        pl.col("Phone_Normalized").unique().alias("Unique_Phones"),
        pl.col("UniqueKey").unique().alias(f"{dataset_name}_UniqueKeys"),
        pl.count().alias(f"{dataset_name}_RecordCount"),
    ])
    return agg

def compare_aggregated(basis_agg: pl.DataFrame, finacle_agg: pl.DataFrame) -> pd.DataFrame:
    # Outer join on Name, Email, Date_of_Birth
    joined = basis_agg.join(finacle_agg, on=["Name", "Email", "Date_of_Birth"], how="outer", suffix="_finacle")

    # Fill nulls with empty lists or empty strings
    joined = joined.with_columns([
        pl.col("Unique_Phones").fill_null(pl.Series([[]])),
        pl.col("Unique_Phones_finacle").fill_null(pl.Series([[]])),
        pl.col("Basis_UniqueKeys").fill_null(pl.Series([[]])),
        pl.col("Finacle_UniqueKeys").fill_null(pl.Series([[]])),
        pl.col("Basis_RecordCount").fill_null(0),
        pl.col("Finacle_RecordCount").fill_null(0),
        pl.col("Name").fill_null(""),
        pl.col("Email").fill_null(""),
        pl.col("Date_of_Birth").fill_null("")
    ])

    # Compare phones (set equality) using Python sets
    def phones_equal(row):
        set_basis = set(row["Unique_Phones"])
        set_finacle = set(row["Unique_Phones_finacle"])
        return set_basis == set_finacle

    # Apply the phone comparison
    mismatches = joined.filter(
        ~(pl.col("Unique_Phones").list.sort() == pl.col("Unique_Phones_finacle").list.sort()) |
        (pl.col("Basis_UniqueKeys").list.len() != pl.col("Finacle_UniqueKeys").list.len()) |
        (pl.col("Basis_UniqueKeys").list.len().fill_null(0) == 0) |
        (pl.col("Finacle_UniqueKeys").list.len().fill_null(0) == 0)
    ).to_pandas()

    return mismatches

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

        # Preprocess and normalize
        basis = normalize(preprocess_basis(basis_df))
        finacle = normalize(preprocess_finacle(finacle_df))

        # Combine phones into lists
        basis = combine_phones(basis)
        finacle = combine_phones(finacle)

        # Aggregate per person (Name, Email, DOB) with phones & keys
        basis_agg = aggregate_person(basis, "Basis")
        finacle_agg = aggregate_person(finacle, "Finacle")

        # Compare aggregated data for mismatches
        mismatches_df = compare_aggregated(basis_agg, finacle_agg)

        st.subheader("🔍 Mismatched Records")

        if not mismatches_df.empty:
            st.dataframe(mismatches_df, use_container_width=True)

            # Export to Excel for download
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatches_df.to_excel(writer, index=False, sheet_name="Mismatches")

            st.download_button(
                label="📥 Download Mismatches (Excel)",
                data=output.getvalue(),
                file_name="finacle_basis_mismatches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.success("✅ All records match between Finacle and Basis based on the defined criteria.")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")