import streamlit as st
import polars as pl
import pandas as pd
import io

st.title("📊 Person-Level Data Validation: BASIS vs FINACLE")

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({
        "CUS_NUM": "Person_ID",  # unique person key in BASIS
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date_of_Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3"
    }).select(["Person_ID", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "ORGKEY": "Person_ID",  # unique person key in FINACLE
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date_of_Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2"
    })
    df = df.with_columns(pl.lit("").alias("Phone_3"))
    return df.select(["Person_ID", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase().alias(col))
    return df

def aggregate_person_data(df: pl.DataFrame) -> pd.DataFrame:
    # Convert polars to pandas for ease
    pdf = df.to_pandas()

    def unique_set(series):
        s = set(series.dropna().astype(str).str.strip())
        s.discard("")
        return s

    # Aggregate to sets per Person_ID
    agg = pdf.groupby("Person_ID").agg({
        "Name": unique_set,
        "Email": unique_set,
        "Date_of_Birth": unique_set,
        "Phone_1": unique_set,
        "Phone_2": unique_set,
        "Phone_3": unique_set,
    }).reset_index()

    # Combine phones into one set
    agg["Phones"] = agg.apply(lambda r: r["Phone_1"].union(r["Phone_2"]).union(r["Phone_3"]), axis=1)

    # Drop separate phone columns now
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

        basis = normalize(preprocess_basis(basis_df))
        finacle = normalize(preprocess_finacle(finacle_df))

        basis_agg = aggregate_person_data(basis)
        finacle_agg = aggregate_person_data(finacle)

        # Merge on Person_ID
        merged = pd.merge(basis_agg, finacle_agg, on="Person_ID", how="outer", suffixes=("_basis", "_finacle"))

        def sets_match(set1, set2):
            if pd.isna(set1) or pd.isna(set2):
                return False
            return set1 == set2

        def compare_row(row):
            mismatches = {}
            for field in ["Name", "Email", "Date_of_Birth", "Phones"]:
                val_basis = row[f"{field}_basis"]
                val_finacle = row[f"{field}_finacle"]
                if not sets_match(val_basis, val_finacle):
                    mismatches[field] = (val_basis, val_finacle)
            return mismatches

        merged["Mismatches"] = merged.apply(compare_row, axis=1)

        # Filter only rows with mismatches
        mismatched_rows = merged[merged["Mismatches"].map(bool)]

        if mismatched_rows.empty:
            st.success("✅ All persons match between BASIS and FINACLE!")
        else:
            st.subheader("Persons with mismatched data")

            # Show mismatch details clearly
            def format_mismatch(row):
                parts = []
                for field, (basis_val, finacle_val) in row["Mismatches"].items():
                    parts.append(f"**{field}**\n BASIS: {basis_val}\n FINACLE: {finacle_val}")
                return "\n\n".join(parts)

            mismatched_rows["Mismatch_Details"] = mismatched_rows.apply(format_mismatch, axis=1)

            display_df = mismatched_rows[["Person_ID", "Mismatch_Details"]]
            st.dataframe(display_df)

            # Export option
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                display_df.to_excel(writer, index=False, sheet_name="Mismatched_Persons")
            st.download_button(
                "Download mismatches as Excel",
                data=output.getvalue(),
                file_name="mismatched_persons.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"Error processing files: {e}")
