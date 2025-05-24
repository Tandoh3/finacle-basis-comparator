import streamlit as st
import polars as pl
import pandas as pd
import io

st.title("Person-level Data Validation by Composite Key")

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date_of_Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3"
    }).select(["Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"])

    # Normalize text columns
    for col in ["Name", "Email"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase().alias(col))

    # Normalize phone columns (remove spaces/dashes)
    for col in ["Phone_1", "Phone_2", "Phone_3"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.replace_all(r"\D", "").alias(col))

    # Format DOB as string to avoid type mismatch
    if "Date_of_Birth" in df.columns:
        df = df.with_columns(pl.col("Date_of_Birth").cast(pl.Utf8))

    # Create composite key by concatenating fields (empty if missing)
    df = df.with_columns(
        (pl.col("Name").fill_null("") + "|" +
         pl.col("Email").fill_null("") + "|" +
         pl.col("Date_of_Birth").fill_null("") + "|" +
         pl.col("Phone_1").fill_null("")).alias("Composite_Key")
    )

    return df

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date_of_Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2"
    })

    df = df.with_columns(pl.lit("").alias("Phone_3"))

    # Normalize like basis
    for col in ["Name", "Email"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase().alias(col))

    for col in ["Phone_1", "Phone_2", "Phone_3"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.replace_all(r"\D", "").alias(col))

    if "Date_of_Birth" in df.columns:
        df = df.with_columns(pl.col("Date_of_Birth").cast(pl.Utf8))

    df = df.with_columns(
        (pl.col("Name").fill_null("") + "|" +
         pl.col("Email").fill_null("") + "|" +
         pl.col("Date_of_Birth").fill_null("") + "|" +
         pl.col("Phone_1").fill_null("")).alias("Composite_Key")
    )
    return df.select(["Composite_Key", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"])

def aggregate_person_data(df: pl.DataFrame) -> pd.DataFrame:
    pdf = df.to_pandas()

    def unique_set(series):
        s = set(series.dropna().astype(str).str.strip())
        s.discard("")
        return s

    agg = pdf.groupby("Composite_Key").agg({
        "Name": unique_set,
        "Email": unique_set,
        "Date_of_Birth": unique_set,
        "Phone_1": unique_set,
        "Phone_2": unique_set,
        "Phone_3": unique_set,
    }).reset_index()

    # Combine phones
    agg["Phones"] = agg.apply(lambda r: r["Phone_1"].union(r["Phone_2"]).union(r["Phone_3"]), axis=1)
    agg = agg.drop(columns=["Phone_1", "Phone_2", "Phone_3"])

    return agg

col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("Upload BASIS File", type=["csv", "xlsx"])
with col2:
    finacle_file = st.file_uploader("Upload FINACLE File", type=["csv", "xlsx"])

if basis_file and finacle_file:
    try:
        basis_df = pl.read_excel(basis_file) if basis_file.name.endswith("xlsx") else pl.read_csv(basis_file)
        finacle_df = pl.read_excel(finacle_file) if finacle_file.name.endswith("xlsx") else pl.read_csv(finacle_file)

        basis = preprocess_basis(basis_df)
        finacle = preprocess_finacle(finacle_df)

        basis_agg = aggregate_person_data(basis)
        finacle_agg = aggregate_person_data(finacle)

        # Merge on Composite_Key
        merged = pd.merge(basis_agg, finacle_agg, on="Composite_Key", how="outer", suffixes=("_basis", "_finacle"))

        def sets_match(set1, set2):
            if pd.isna(set1) or pd.isna(set2):
                return False
            return set1 == set2

        def compare_row(row):
            mismatches = {}
            for field in ["Name", "Email", "Date_of_Birth", "Phones"]:
                val_basis = row.get(f"{field}_basis", set())
                val_finacle = row.get(f"{field}_finacle", set())
                if not sets_match(val_basis, val_finacle):
                    mismatches[field] = (val_basis, val_finacle)
            return mismatches

        merged["Mismatches"] = merged.apply(compare_row, axis=1)
        mismatched_rows = merged[merged["Mismatches"].map(bool)]

        if mismatched_rows.empty:
            st.success("✅ All composite keys match between BASIS and FINACLE!")
        else:
            st.subheader("Mismatched Persons by Composite Key")

            def format_mismatch(row):
                parts = []
                for field, (basis_val, finacle_val) in row["Mismatches"].items():
                    parts.append(f"**{field}**\nBASIS: {basis_val}\nFINACLE: {finacle_val}")
                return "\n\n".join(parts)

            mismatched_rows["Mismatch_Details"] = mismatched_rows.apply(format_mismatch, axis=1)
            st.dataframe(mismatched_rows[["Composite_Key", "Mismatch_Details"]])

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatched_rows[["Composite_Key", "Mismatch_Details"]].to_excel(writer, index=False)
            st.download_button(
                "Download mismatches as Excel",
                data=output.getvalue(),
                file_name="mismatched_composite_keys.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    except Exception as e:
        st.error(f"Error processing files: {e}")
