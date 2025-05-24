import streamlit as st
import polars as pl
import pandas as pd
import io

st.set_page_config(page_title="Finacle vs Basis Comparator", layout="wide")
st.title("📊 Finacle vs Basis Comparator")

# === 1. Preprocessing Functions ===

def preprocess_basis(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email",
        "BIR_DATE": "Date_of_Birth",
        "TEL_NUM": "Phone_1",
        "TEL_NUM_2": "Phone_2",
        "FAX_NUM": "Phone_3",
        "CUS_NUM": "Unique_Basis_Key",
        "BRA_CODE": "Branch_Code"
    })
    # Ensure all phone cols exist
    for col in ["Phone_1", "Phone_2", "Phone_3"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit("").alias(col))
    return df.select([
        "Unique_Basis_Key", "Branch_Code", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email",
        "CUST_DOB": "Date_of_Birth",
        "PREFERREDPHONE": "Phone_1",
        "SMSBANKINGMOBILENUMBER": "Phone_2",
        "ORGKEY": "Unique_Finacle_Key"
    })
    # Add Phone_3 if missing
    if "Phone_3" not in df.columns:
        df = df.with_columns(pl.lit("").alias("Phone_3"))
    return df.select([
        "Unique_Finacle_Key", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase().alias(col))
    return df

def read_file(file) -> pl.DataFrame:
    if file.name.endswith(".xlsx") or file.name.endswith(".xls"):
        return pl.read_excel(file)
    else:
        # Polars CSV reading
        return pl.read_csv(file)

# === 2. File Upload ===
col1, col2 = st.columns(2)

with col1:
    basis_file = st.file_uploader("📥 Upload BASIS File (CSV/XLSX)", type=["csv", "xlsx", "xls"], key="basis")

with col2:
    finacle_file = st.file_uploader("📥 Upload FINACLE File (CSV/XLSX)", type=["csv", "xlsx", "xls"], key="finacle")

# === 3. Process if both files are uploaded ===
if basis_file and finacle_file:
    try:
        # Read files
        basis_df = read_file(basis_file)
        finacle_df = read_file(finacle_file)

        st.subheader("📄 Uploaded File Summary")
        st.write(f"BASIS Rows: {basis_df.height}")
        st.write(f"FINACLE Rows: {finacle_df.height}")

        # Preprocess and normalize
        basis_df = normalize(preprocess_basis(basis_df))
        finacle_df = normalize(preprocess_finacle(finacle_df))

        # Convert DOB to string for safe comparison
        basis_df = basis_df.with_columns(pl.col("Date_of_Birth").cast(pl.Utf8))
        finacle_df = finacle_df.with_columns(pl.col("Date_of_Birth").cast(pl.Utf8))

        # Combine all phone columns into a list per row in BASIS and FINACLE
        def phones_to_list(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
            return df.with_columns(
                pl.concat_list(["Phone_1", "Phone_2", "Phone_3"]).alias(f"{prefix}_Phones")
            )

        basis_df = phones_to_list(basis_df, "Basis")
        finacle_df = phones_to_list(finacle_df, "Finacle")

        # Group by unique person key and aggregate sets of unique values
        basis_grouped = basis_df.groupby("Unique_Basis_Key").agg([
            pl.col("Name").unique().alias("Names"),
            pl.col("Email").unique().alias("Emails"),
            pl.col("Date_of_Birth").unique().alias("DOBs"),
            pl.col("Basis_Phones").explode().unique().alias("Phones"),
            pl.first("Branch_Code").alias("Branch_Code")
        ])

        finacle_grouped = finacle_df.groupby("Unique_Finacle_Key").agg([
            pl.col("Name").unique().alias("Names"),
            pl.col("Email").unique().alias("Emails"),
            pl.col("Date_of_Birth").unique().alias("DOBs"),
            pl.col("Finacle_Phones").explode().unique().alias("Phones"),
        ])

        # For matching, convert sets to python sets for easy comparison
        def pl_list_to_set(lst):
            if lst is None:
                return set()
            return set([x for x in lst if x and x.strip() != ""])

        # Prepare mismatches list
        mismatches = []

        # We assume the number of records can be large, but let's loop
        for b_row in basis_grouped.iter_rows(named=True):
            # Find matching finacle records (you can define a logic to find matching person)
            # Here: no common key, so we try to match by any overlap of Names, Emails, DOBs, or Phones
            basis_names = pl_list_to_set(b_row["Names"])
            basis_emails = pl_list_to_set(b_row["Emails"])
            basis_dobs = pl_list_to_set(b_row["DOBs"])
            basis_phones = pl_list_to_set(b_row["Phones"])

            # Find finacle rows that have any overlapping field with basis
            matched_finacle = []
            for f_row in finacle_grouped.iter_rows(named=True):
                finacle_names = pl_list_to_set(f_row["Names"])
                finacle_emails = pl_list_to_set(f_row["Emails"])
                finacle_dobs = pl_list_to_set(f_row["DOBs"])
                finacle_phones = pl_list_to_set(f_row["Phones"])

                # Check for overlap in any field
                name_overlap = len(basis_names.intersection(finacle_names)) > 0
                email_overlap = len(basis_emails.intersection(finacle_emails)) > 0
                dob_overlap = len(basis_dobs.intersection(finacle_dobs)) > 0
                phone_overlap = len(basis_phones.intersection(finacle_phones)) > 0

                # Consider match if any one field overlaps
                if name_overlap or email_overlap or dob_overlap or phone_overlap:
                    matched_finacle.append(f_row)

            # If no match at all
            if len(matched_finacle) == 0:
                mismatches.append({
                    "Basis_Key": b_row["Unique_Basis_Key"],
                    "Branch_Code": b_row["Branch_Code"],
                    "Basis_Names": list(basis_names),
                    "Basis_Emails": list(basis_emails),
                    "Basis_DOBs": list(basis_dobs),
                    "Basis_Phones": list(basis_phones),
                    "Finacle_Key": None,
                    "Finacle_Names": None,
                    "Finacle_Emails": None,
                    "Finacle_DOBs": None,
                    "Finacle_Phones": None,
                    "Mismatch_Reason": "No matching record in Finacle"
                })
            else:
                # For each matched finacle record, compare sets exactly (must all match)
                for f_match in matched_finacle:
                    # Check if all sets are exactly equal
                    all_match = (
                        basis_names == pl_list_to_set(f_match["Names"]) and
                        basis_emails == pl_list_to_set(f_match["Emails"]) and
                        basis_dobs == pl_list_to_set(f_match["DOBs"]) and
                        basis_phones == pl_list_to_set(f_match["Phones"])
                    )
                    if not all_match:
                        mismatches.append({
                            "Basis_Key": b_row["Unique_Basis_Key"],
                            "Branch_Code": b_row["Branch_Code"],
                            "Basis_Names": list(basis_names),
                            "Basis_Emails": list(basis_emails),
                            "Basis_DOBs": list(basis_dobs),
                            "Basis_Phones": list(basis_phones),
                            "Finacle_Key": f_match["Unique_Finacle_Key"],
                            "Finacle_Names": list(pl_list_to_set(f_match["Names"])),
                            "Finacle_Emails": list(pl_list_to_set(f_match["Emails"])),
                            "Finacle_DOBs": list(pl_list_to_set(f_match["DOBs"])),
                            "Finacle_Phones": list(pl_list_to_set(f_match["Phones"])),
                            "Mismatch_Reason": "Fields do not match exactly"
                        })

        if len(mismatches) == 0:
            st.success("✅ All Basis and Finacle records match based on the four key fields.")
        else:
            st.subheader(f"🔍 Found {len(mismatches)} mismatched records")

            # Display side-by-side columns with mismatches
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("### BASIS Records")
                basis_table = pd.DataFrame([
                    {
                        "Unique_Basis_Key": r["Basis_Key"],
                        "Branch_Code": r["Branch_Code"],
                        "Names": ", ".join(r["Basis_Names"]) if r["Basis_Names"] else "",
                        "Emails": ", ".join(r["Basis_Emails"]) if r["Basis_Emails"] else "",
                        "DOBs": ", ".join(r["Basis_DOBs"]) if r["Basis_DOBs"] else "",
                        "Phones": ", ".join(r["Basis_Phones"]) if r["Basis_Phones"] else "",
                        "Mismatch Reason": r["Mismatch_Reason"]
                    } for r in mismatches
                ])
                st.dataframe(basis_table)

            with col2:
                st.markdown("### FINACLE Records")
                finacle_table = pd.DataFrame([
                    {
                        "Unique_Finacle_Key": r["Finacle_Key"],
                        "Names": ", ".join(r["Finacle_Names"]) if r["Finacle_Names"] else "",
                        "Emails": ", ".join(r["Finacle_Emails"]) if r["Finacle_Emails"] else "",
                        "DOBs": ", ".join(r["Finacle_DOBs"]) if r["Finacle_DOBs"] else "",
                        "Phones": ", ".join(r["Finacle_Phones"]) if r["Finacle_Phones"] else "",
                        "Mismatch Reason": r["Mismatch_Reason"]
                    } for r in mismatches
                ])
                st.dataframe(finacle_table)

            # Provide download option for mismatches
            df_mismatches = pd.DataFrame(mismatches)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_mismatches.to_excel(writer, index=False, sheet_name="Mismatches")
            st.download_button(
                label="📥 Download Mismatches Excel",
                data=output.getvalue(),
                file_name="basis_finacle_mismatches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"Error processing files: {e}")

else:
    st.info("Please upload both Basis and Finacle files to start comparison.")
