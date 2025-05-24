import streamlit as st
import polars as pl
import pandas as pd  # For downloading Excel

def process_data(uploaded_file):
    """Reads an uploaded CSV/Excel file using Polars, normalizes strings."""
    if uploaded_file is not None:
        try:
            file_extension = uploaded_file.name.split(".")[-1].lower()
            if file_extension in ["xlsx", "xls"]:
                df = pl.read_excel(uploaded_file)
            elif file_extension == "csv":
                df = pl.read_csv(uploaded_file)
            else:
                st.error("Unsupported file format. Please upload a CSV or Excel file.")
                return None, None

            string_cols = df.select(pl.col(pl.Utf8)).columns
            for col in string_cols:
                df = df.with_columns(pl.col(col).str.to_lowercase().str.strip())

            phone_cols = [col for col in df.columns if "phone" in col.lower() or "mobile" in col.lower()]
            other_cols = [col for col in df.columns if col not in phone_cols]

            if phone_cols:
                df = df.with_columns(pl.concat_list(phone_cols).alias("phones")).drop(phone_cols)

            return df, other_cols
        except Exception as e:
            st.error(f"An error occurred while processing the file: {e}")
            return None, None
    return None, None

def group_and_aggregate(df: pl.DataFrame, key_cols: list[str]):
    """Groups by Name, Email, DOB and aggregates unique phones and keys."""
    if df is None:
        return None

    grouping_cols = ["name", "email", "date of birth"]
    grouping_cols = [col for col in grouping_cols if col in df.columns]

    if not grouping_cols:
        st.error("Error: 'Name', 'Email', or 'Date of Birth' columns not found for grouping.")
        return None

    agg_expr = []
    if "phones" in df.columns:
        agg_expr.append(pl.col("phones").list.unique().alias("unique_phones"))

    potential_key_cols = [col for col in df.columns if col not in grouping_cols + ["phones"]]
    if potential_key_cols:
        agg_expr.append(pl.concat_list(potential_key_cols).list.unique().alias("unique_keys"))

    if not agg_expr:
        return df.group_by(grouping_cols).count().rename({"count": "unique_keys_count"})

    return df.group_by(grouping_cols).agg(agg_expr)

def merge_and_compare(basis_grouped: pl.DataFrame, finacle_grouped: pl.DataFrame):
    """Merges grouped data and flags mismatches."""
    if basis_grouped is None or finacle_grouped is None:
        return None

    merged_df = basis_grouped.join(finacle_grouped, on=["name", "email", "date of birth"], how="outer", suffix="_finacle")

    def check_mismatch(row):
        basis_phones = set(row.get("unique_phones") or [])
        finacle_phones = set(row.get("unique_phones_finacle") or [])
        basis_keys = set(row.get("unique_keys") or [])
        finacle_keys = set(row.get("unique_keys_finacle") or [])

        phone_mismatch = basis_phones != finacle_phones
        key_mismatch = (len(basis_keys) if basis_keys is not None else 0) != (len(finacle_keys) if finacle_keys is not None else 0)

        if phone_mismatch and key_mismatch:
            return "Both Phone and Key Mismatch"
        elif phone_mismatch:
            return "Phone Mismatch"
        elif key_mismatch:
            return "Key Mismatch"
        else:
            return "No Mismatch"

    merged_df = merged_df.with_columns(
        pl.struct(
            [
                pl.col("unique_phones").fill_null(pl.Series("empty", [[]])),
                pl.col("unique_phones_finacle").fill_null(pl.Series("empty", [[]])),
                pl.col("unique_keys").fill_null(pl.Series("empty", [[]])),
                pl.col("unique_keys_finacle").fill_null(pl.Series("empty", [[]])),
            ]
        ).apply(check_mismatch).alias("Mismatch Flag")
    )

    mismatches_df = merged_df.filter(pl.col("Mismatch Flag") != "No Mismatch")
    return mismatches_df

def display_mismatches(mismatches_df: pl.DataFrame):
    """Displays the mismatches."""
    if mismatches_df is not None and not mismatches_df.is_empty():
        st.subheader("Mismatched Records")
        st.dataframe(mismatches_df.to_pandas())
        return mismatches_df.to_pandas()
    elif mismatches_df is not None:
        st.info("No mismatches found.")
    return None

def download_excel(df: pd.DataFrame):
    """Generates a download link for the given Pandas DataFrame as an Excel file."""
    if df is not None and not df.empty:
        output = df.to_excel(index=False)
        st.download_button(
            label="Download Mismatches as Excel",
            data=output,
            file_name="mismatches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

def main():
    st.title("Data Comparison Tool (BASIS vs. FINACLE)")

    col1, col2 = st.columns(2)

    with col1:
        basis_file = st.file_uploader("Upload BASIS Data (CSV or Excel)", type=["csv", "xlsx", "xls"])
    with col2:
        finacle_file = st.file_uploader("Upload FINACLE Data (CSV or Excel)", type=["csv", "xlsx", "xls"])

    if basis_file and finacle_file:
        basis_df, basis_other_cols = process_data(basis_file)
        finacle_df, finacle_other_cols = process_data(finacle_file)

        if basis_df is not None and finacle_df is not None:
            basis_grouped_df = group_and_aggregate(basis_df, basis_other_cols)
            finacle_grouped_df = group_and_aggregate(finacle_df, finacle_other_cols)

            if basis_grouped_df is not None and finacle_grouped_df is not None:
                mismatches_df_pl = merge_and_compare(basis_grouped_df, finacle_grouped_df)
                mismatches_df_pd = display_mismatches(mismatches_df_pl)

                if mismatches_df_pd is not None:
                    download_excel(mismatches_df_pd)

if __name__ == "__main__":
    main()