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
    }).select(["Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"])

    # Cast to string first
    for col in ["Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

    # Normalize and clean
    for col in ["Name", "Email"]:
        df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase())
    for col in ["Phone_1", "Phone_2", "Phone_3"]:
        df = df.with_columns(pl.col(col).str.replace_all(r"\D", ""))

    # Composite key to identify the person
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

    # Cast to string first
    for col in ["Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

    # Normalize and clean
    for col in ["Name", "Email"]:
        df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase())
    for col in ["Phone_1", "Phone_2", "Phone_3"]:
        df = df.with_columns(pl.col(col).str.replace_all(r"\D", ""))

    # Composite key
    df = df.with_columns(
        (pl.col("Name").fill_null("") + "|" +
         pl.col("Email").fill_null("") + "|" +
         pl.col("Date_of_Birth").fill_null("") + "|" +
         pl.col("Phone_1").fill_null("")).alias("Composite_Key")
    )
    return df.select(["Composite_Key", "Name", "Email", "Date_of_Birth", "Phone_1", "Phone_2", "Phone_3"])

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
        if basis_file.name.endswith("xlsx"):
            basis_df = pl.read_excel(basis_file)
        else:
            basis_df = pl.read_csv(basis_file)

        if finacle_file.name.endswith("xlsx"):
            finacle_df = pl.read_excel(finacle_file)
        else:
            finacle_df = pl.read_csv(finacle_file)

        st.subheader("📄 Uploaded Summary")
        st.write(f"🔹 BASIS Rows: {basis_df.height}")
        st.write(f"🔹 FINACLE Rows: {finacle_df.height}")

        # Preprocess and normalize data
        basis = preprocess_basis(basis_df)
        finacle = preprocess_finacle(finacle_df)

        # Group by Composite_Key to aggregate data for each unique person
        basis_grouped = basis.groupby("Composite_Key").agg([
            pl.first("Name").alias("Name"),
            pl.first("Email").alias("Email"),
            pl.first("Date_of_Birth").alias("Date_of_Birth"),
            pl.unique(pl.concat_list(["Phone_1", "Phone_2", "Phone_3"])).alias("Phones")
        ])

        finacle_grouped = finacle.groupby("Composite_Key").agg([
            pl.first("Name").alias("Name"),
            pl.first("Email").alias("Email"),
            pl.first("Date_of_Birth").alias("Date_of_Birth"),
            pl.unique(pl.concat_list(["Phone_1", "Phone_2", "Phone_3"])).alias("Phones")
        ])

        # Join on Composite_Key with full outer join to find mismatches
        joined = basis_grouped.join(finacle_grouped, on="Composite_Key", how="outer", suffix="_finacle")

        # Function to compare phones lists (set equality)
        def phones_match(basis_phones, finacle_phones):
            basis_set = set(basis_phones) if basis_phones is not None else set()
            finacle_set = set(finacle_phones) if finacle_phones is not None else set()
            # Remove empty strings from sets
            basis_set.discard("")
            finacle_set.discard("")
            return basis_set == finacle_set

        # Build mismatch flags
        mismatch_flags = []

        for row in joined.iter_rows(named=True):
            # Extract fields safely
            basis_name = row.get("Name")
            finacle_name = row.get("Name_finacle")

            basis_email = row.get("Email")
            finacle_email = row.get("Email_finacle")

            basis_dob = row.get("Date_of_Birth")
            finacle_dob = row.get("Date_of_Birth_finacle")

            basis_phones = row.get("Phones") or []
            finacle_phones = row.get("Phones_finacle") or []

            name_match = (basis_name == finacle_name)
            email_match = (basis_email == finacle_email)
            dob_match = (basis_dob == finacle_dob)
            phone_match = phones_match(basis_phones, finacle_phones)

            mismatch_flags.append({
                "Composite_Key": row["Composite_Key"],
                "Name_Basis": basis_name,
                "Name_Finacle": finacle_name,
                "Email_Basis": basis_email,
                "Email_Finacle": finacle_email,
                "DOB_Basis": basis_dob,
                "DOB_Finacle": finacle_dob,
                "Phones_Basis": ", ".join(basis_phones),
                "Phones_Finacle": ", ".join(finacle_phones),
                "Name_Match": name_match,
                "Email_Match": email_match,
                "DOB_Match": dob_match,
                "Phone_Match": phone_match,
                "All_Match": name_match and email_match and dob_match and phone_match,
            })

        mismatch_df = pl.from_dicts(mismatch_flags)

        # Filter mismatches
        mismatches = mismatch_df.filter(pl.col("All_Match") == False)

        st.subheader("🔍 Mismatched Records Summary")

        if mismatches.height > 0:
            # Show mismatches in two columns
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Basis Data**")
                st.dataframe(
                    mismatches.select([
                        "Name_Basis", "Email_Basis", "DOB_Basis", "Phones_Basis",
                        "Name_Match", "Email_Match", "DOB_Match", "Phone_Match"
                    ]).to_pandas(),
                    use_container_width=True
                )
            with col2:
                st.write("**Finacle Data**")
                st.dataframe(
                    mismatches.select([
                        "Name_Finacle", "Email_Finacle", "DOB_Finacle", "Phones_Finacle",
                        "Name_Match", "Email_Match", "DOB_Match", "Phone_Match"
                    ]).to_pandas(),
                    use_container_width=True
                )

            # Download option for full mismatch report
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                mismatches.to_pandas().to_excel(writer, index=False, sheet_name="Mismatches")

            st.download_button(
                label="📥 Download Mismatch Report (Excel)",
                data=output.getvalue(),
                file_name="finacle_basis_mismatches.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.success("✅ All records match between Finacle and Basis!")

    except Exception as e:
        st.error(f"❌ Error processing files: {e}")
