import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Comparison Tool", layout="wide")
st.title("📊 Finacle vs Basis Comparator (Large Dataset Support)")

# === 1. Preprocessing Functions ===

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    # Rename relevant columns to unified names
    df = df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date_of_Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3",
        "CUS_NUM": "UniqueKey"  # Unique person key in BASIS
    })
    # Select needed columns and fill missing phones with empty string
    df = df.select([
        "UniqueKey", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ]).with_columns([
        pl.col("Phone_1").fill_null("").cast(pl.Utf8),
        pl.col("Phone_2").fill_null("").cast(pl.Utf8),
        pl.col("Phone_3").fill_null("").cast(pl.Utf8),
    ])
    return df

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    # Rename relevant columns to unified names
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date_of_Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2",
        "ORGKEY": "UniqueKey"  # Unique person key in FINACLE
    })
    df = df.with_columns([
        pl.col("Phone_1").fill_null("").cast(pl.Utf8),
        pl.col("Phone_2").fill_null("").cast(pl.Utf8),
        pl.lit("").alias("Phone_3")  # Add empty Phone_3 column for consistency
    ])
    df = df.select([
        "UniqueKey", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ])
    return df

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    # Lowercase, strip spaces on all string columns
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return df

def combine_phones(df: pl.DataFrame) -> pl.DataFrame:
    # Combine phones into a list column
    return df.with_columns(
        pl.concat_list(["Phone_1", "Phone_2", "Phone_3"]).alias("Phones")
    )

def aggregate_person(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """
    Group by Name, Email, Date_of_Birth, and aggregate phones as a list of unique phones
    Also collect all UniqueKeys per group (to know which records belong to each person)
    """
    df = df.with_columns([
        pl.col("Phones").arr.eval(pl.element().str.strip_chars()).alias("Phones"),  # strip spaces inside list elements
    ])

    agg = df.groupby(["Name", "Email", "Date_of_Birth"]).agg([
        pl.col("Phones").explode().unique().alias("Unique_Phones"),
        pl.col("UniqueKey").unique().alias(f"{dataset_name}_UniqueKeys"),
        pl.count().alias(f"{dataset_name}_RecordCount")
    ])
    return agg

def compare_groups(basis_grp: pl.DataFrame, finacle_grp: pl.DataFrame) -> pl.DataFrame:
    """
    Compare the two grouped dataframes on Name, Email, Date_of_Birth:
    - Merge on these keys with outer join to find matches and mismatches
    - Mark mismatches where Unique_Phones or UniqueKeys differ or missing on either side
    """
    joined = basis_grp.join(finacle_grp, on=["Name", "Email", "Date_of_Birth"], how="outer", suffix="_finacle")

    # Fill nulls to empty lists or strings so comparisons don't error
    joined = joined.with_columns([
        pl.col("Unique_Phones").list.to_set().alias("Unique_Phones"),
        pl.col("Unique_Phones_finacle").list.to_set().alias("Unique_Phones_finacle"),
        pl.col("basis_UniqueKeys").list.null_to([]),
        pl.col("finacle_UniqueKeys").list.null_to([]),
        pl.col("basis_RecordCount").fill_null(0),
        pl.col("finacle_RecordCount").fill_null(0),
    ])

    # Identify mismatches:
    mismatch = (
        (joined["Unique_Phones"] != joined["Unique_Phones_finacle"]) |
        (joined["basis_UniqueKeys"].arr.lengths() != joined["finacle_UniqueKeys"].arr.lengths())
    )

    joined = joined.with_columns(
        mismatch.alias("Mismatch")
    )

    # Filter mismatches only
    mismatches = joined.filter(pl.col("Mismatch") == True)

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
        # Read files with Polars
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

        # Aggregate by person group
        basis_grouped = aggregate_person(basis, "basis")
        finacle_grouped = aggregate_person(finacle, "finacle")

        # Compare groups and get mismatches
        mismatches = compare_groups(basis_grouped, finacle_grouped)

        st.subheader("🔍 Mismatched Person Groups")

        if mismatches.height == 0:
            st.success("✅ All person groups match between Finacle and Basis.")
        else:
            # Show in two columns
            colA, colB = st.columns(2)

            with colA:
                st.markdown("### BASIS Data")
                df_basis_show = mismatches.select([
                    "Name", "Email", "Date_of_Birth", "basis_UniqueKeys", "Unique_Phones", "basis_RecordCount"
                ]).to_pandas()
                df_basis_show["Unique_Phones"] = df_basis_show["Unique_Phones"].apply(lambda x: ", ".join(x))
                df_basis_show["basis_UniqueKeys"] = df_basis_show["basis_UniqueKeys"].apply(lambda x: ", ".join(map(str, x)))
                st.dataframe(df_basis_show)

            with colB:
                st.markdown("### FINACLE Data")
                df_finacle_show = mismatches.select([
                    "Name", "Email", "Date_of_Birth", "finacle_UniqueKeys", "Unique_Phones_finacle", "finacle_RecordCount"
                ]).to_pandas()
                df_finacle_show["Unique_Phones_finacle"] = df_finacle_show["Unique_Phones_finacle"].apply(lambda x: ", ".join(x))
                df_finacle_show["finacle_UniqueKeys"] = df_finacle_show["finacle_UniqueKeys"].apply(lambda x: ", ".join(map(str, x)))
                st.dataframe(df_finacle_show)

            # Prepare downloadable Excel of mismatches
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_basis_show.to_excel(writer, index=False, sheet_name="Basis_Mismatches")
                df_finacle_show.to_excel(writer, index=False, sheet_name="Finacle_Mismatches")

            st.download_button(
                label="📥 Download Mismatches (Excel)",
                data=output.getvalue(),
                file_name="finacle_basis_mismatches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")
