import streamlit as st
import polars as pl
import pandas as pd
import io

st.title("🔍 Show Only Mismatched Records: Finacle vs Basis")

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date_of_Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3"
    }).select(["BRA_CODE", "CUS_NUM", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    st.write("Finacle Columns:", df.columns)
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date_of_Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2"
    })
    df = df.with_columns(pl.lit("").alias("Phone_3"))
    return df.select(["ORGKEY", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase().alias(col))
    return df

basis_file = st.file_uploader("Upload BASIS File", type=["csv", "xlsx"])
finacle_file = st.file_uploader("Upload FINACLE File", type=["csv", "xlsx"])

if basis_file and finacle_file:
    try:
        basis_df = pl.read_excel(basis_file) if basis_file.name.endswith("xlsx") else pl.read_csv(basis_file)
        finacle_df = pl.read_excel(finacle_file) if finacle_file.name.endswith("xlsx") else pl.read_csv(finacle_file)

        basis = normalize(preprocess_basis(basis_df))
        finacle = normalize(preprocess_finacle(finacle_df))

        # Fill phone nulls
        basis = basis.with_columns([
            pl.col("Phone_1").fill_null(""),
            pl.col("Phone_2").fill_null(""),
            pl.col("Phone_3").fill_null("")
        ])
        finacle = finacle.with_columns([
            pl.col("Phone_1").fill_null(""),
            pl.col("Phone_2").fill_null(""),
            pl.col("Phone_3").fill_null("")
        ])

        # Join on Name, Email, DOB
        merged = basis.join(finacle, on=["Name", "Email", "Date_of_Birth"], how="outer", suffix="_finacle")

        merged = merged.with_columns([
            pl.col("Phone_1").fill_null(""),
            pl.col("Phone_2").fill_null(""),
            pl.col("Phone_3").fill_null(""),
            pl.col("Phone_1_finacle").fill_null(""),
            pl.col("Phone_2_finacle").fill_null(""),
            pl.col("Phone_3_finacle").fill_null("")
        ])

        # Define phone match: any phone overlap
        def phones_match(row):
            basis_phones = {row["Phone_1"], row["Phone_2"], row["Phone_3"]}
            finacle_phones = {row["Phone_1_finacle"], row["Phone_2_finacle"], row["Phone_3_finacle"]}
            basis_phones.discard("")
            finacle_phones.discard("")
            return len(basis_phones.intersection(finacle_phones)) > 0

        merged_pd = merged.to_pandas()
        merged_pd["Phone_Match"] = merged_pd.apply(phones_match, axis=1)

        # Mismatches = missing in either OR phones do not match
        mismatch = merged_pd[
            merged_pd["BRA_CODE"].isnull() |
            merged_pd["ORGKEY"].isnull() |
            (~merged_pd["Phone_Match"])
        ]

        if mismatch.empty:
            st.success("✅ No mismatched records found!")
        else:
            st.subheader("Mismatched Records")
            st.dataframe(mismatch)

            # Excel download
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatch.to_excel(writer, index=False, sheet_name="Mismatches")

            st.download_button(
                label="Download Mismatches Excel",
                data=output.getvalue(),
                file_name="mismatched_records.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"Error: {e}")
