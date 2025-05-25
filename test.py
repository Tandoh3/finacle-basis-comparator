import streamlit as st
import polars as pl
import pandas as pd
from fuzzywuzzy import fuzz
from io import BytesIO

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching (Batched)", layout="wide")
st.title("🔍 Finacle vs Basis Fuzzy Matching (Batched)")

st.markdown(
    "Upload your BASIS and FINACLE files below (CSV or Excel). "
    "Matching will be done in batches using fuzzy logic for names, emails, dates of birth, and phone numbers."
)

# === 1. Upload Section ===
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📂 Upload BASIS file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="basis")
with col2:
    finacle_file = st.file_uploader("📂 Upload FINACLE file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="finacle")

# === 2. Helper Functions ===

def read_file(file, is_basis=True):
    if file.name.endswith('.csv'):
        dtypes_basis = {"TEL_NUM": pl.Utf8, "TEL_NUM_2": pl.Utf8, "FAX_NUM": pl.Utf8}
        dtypes_finacle = {"PREFERREDPHONE": pl.Utf8, "SMSBANKINGMOBILENUMBER": pl.Utf8}
        return pl.read_csv(file, dtypes=dtypes_basis if is_basis else dtypes_finacle)
    else:
        schema_overrides_basis = {"TEL_NUM": pl.Utf8, "TEL_NUM_2": pl.Utf8, "FAX_NUM": pl.Utf8}
        schema_overrides_finacle = {"PREFERREDPHONE": pl.Utf8, "SMSBANKINGMOBILENUMBER": pl.Utf8}
        return pl.read_excel(file, schema_overrides=schema_overrides_basis if is_basis else schema_overrides_finacle)

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
        pl.col("Name").fill_null(""),
        pl.col("Email_Basis").fill_null(""),
        "Date_of_Birth_Basis",
        pl.col("Phone_1_Basis").fill_null("").cast(pl.Utf8),
        pl.col("Phone_2_Basis").fill_null("").cast(pl.Utf8),
        pl.col("Phone_3_Basis").fill_null("").cast(pl.Utf8)
    ])

def preprocess_finacle(df: pl.DataFrame) -> pl.DataFrame:
    df = df.rename({
        "NAME": "Name",
        "PREFERREDEMAIL": "Email_Finacle",
        "CUST_DOB": "Date_of_Birth_Finacle",
        "PREFERREDPHONE": "Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER": "Phone_2_Finacle"
    }).with_columns(pl.lit("").alias("Phone_3_Finacle"))
    return df.select([
        pl.col("Name").fill_null(""),
        pl.col("Email_Finacle").fill_null(""),
        "Date_of_Birth_Finacle",
        pl.col("Phone_1_Finacle").fill_null("").cast(pl.Utf8),
        pl.col("Phone_2_Finacle").fill_null("").cast(pl.Utf8),
        pl.col("Phone_3_Finacle").fill_null("").cast(pl.Utf8)
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.strip_chars().str.to_lowercase().alias(col)
            )
    return df

def fuzzy_match_string(s1, s2, threshold=80):
    if not s1 or not s2:
        return False, 0
    score = fuzz.ratio(s1, s2)
    return score >= threshold, score

def fuzzy_match_phones(list1, list2, threshold=85):
    set1 = set([p for p in list1 if p])
    set2 = set([p for p in list2 if p])
    intersection = set1 & set2
    union = set1 | set2
    if not union:
        return True, 100
    score = (len(intersection) / len(union)) * 100
    return score >= threshold, score

def fuzzy_match_dates(d1, d2, threshold_days=30):
    try:
        date1 = pd.to_datetime(d1, errors='coerce')
        date2 = pd.to_datetime(d2, errors='coerce')
        if pd.isna(date1) or pd.isna(date2):
            return False, 0
        diff = abs((date1 - date2).days)
        return diff <= threshold_days, 100 - (diff / threshold_days * 100)
    except:
        return False, 0

def find_fuzzy_matches_batched(basis_df: pl.DataFrame, finacle_df: pl.DataFrame, batch_size=1000, name_threshold=85, email_threshold=90, phone_threshold=85, dob_threshold_days=30):
    basis_df = normalize(preprocess_basis(basis_df)).to_pandas()
    finacle_df = normalize(preprocess_finacle(finacle_df)).to_pandas()

    matches = []
    mismatches_basis = []
    matched_finacle_indices = set()

    num_basis_rows = len(basis_df)
    for i in range(0, num_basis_rows, batch_size):
        basis_batch = basis_df.iloc[i:i + batch_size]
        st.info(f"Processing BASIS records {i + 1} to {min(i + batch_size, num_basis_rows)}")

        for _, b_row in basis_batch.iterrows():
            b_phones = [b_row.get("Phone_1_Basis", ""), b_row.get("Phone_2_Basis", ""), b_row.get("Phone_3_Basis", "")]
            best_match = None
            best_score = 0
            best_idx = None

            for j, f_row in finacle_df.iterrows():
                if j in matched_finacle_indices:
                    continue
                f_phones = [f_row.get("Phone_1_Finacle", ""), f_row.get("Phone_2_Finacle", ""), f_row.get("Phone_3_Finacle", "")]

                name_match, name_score = fuzzy_match_string(b_row["Name"], f_row["Name"], name_threshold)
                email_match, email_score = fuzzy_match_string(b_row["Email_Basis"], f_row["Email_Finacle"], email_threshold)
                phone_match, phone_score = fuzzy_match_phones(b_phones, f_phones, phone_threshold)
                dob_match, dob_score = fuzzy_match_dates(b_row["Date_of_Birth_Basis"], f_row["Date_of_Birth_Finacle"], dob_threshold_days)

                total_score = name_score * 0.4 + email_score * 0.3 + dob_score * 0.2 + phone_score * 0.1 if name_match else 0

                if total_score > best_score and name_match:
                    best_score = total_score
                    best_match = f_row
                    best_idx = j

            if best_match is not None:
                matches.append({
                    "Basis_Name": b_row["Name"],
                    "Finacle_Name": best_match["Name"],
                    "Email_Basis": b_row["Email_Basis"],
                    "Email_Finacle": best_match["Email_Finacle"],
                    "DOB_Basis": b_row["Date_of_Birth_Basis"],
                    "DOB_Finacle": best_match["Date_of_Birth_Finacle"],
                    "Phones_Basis": b_phones,
                    "Phones_Finacle": [best_match["Phone_1_Finacle"], best_match["Phone_2_Finacle"], best_match["Phone_3_Finacle"]],
                    "Score": round(best_score, 2)
                })
                matched_finacle_indices.add(best_idx)
            else:
                mismatches_basis.append({
                    "Unmatched_Basis_Name": b_row["Name"],
                    "Email_Basis": b_row["Email_Basis"],
                    "DOB_Basis": b_row["Date_of_Birth_Basis"],
                    "Phones_Basis": b_phones
                })
        st.progress((i + batch_size) / num_basis_rows)

    mismatches_finacle = []
    finacle_df_reset = finacle_df.reset_index()
    for _, f_row in finacle_df_reset.iterrows():
        if f_row['index'] not in matched_finacle_indices:
            mismatches_finacle.append({
                "Unmatched_Finacle_Name": f_row["Name"],
                "Email_Finacle": f_row["Email_Finacle"],
                "DOB_Finacle": f_row["Date_of_Birth_Finacle"],
                "Phones_Finacle": [f_row["Phone_1_Finacle"], f_row["Phone_2_Finacle"], f_row["Phone_3_Finacle"]]
            })

    return pd.DataFrame(matches), pd.DataFrame(mismatches_basis), pd.DataFrame(mismatches_finacle)

def convert_df(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# === 3. Main Logic ===
if basis_file and finacle_file:
    batch_size = st.slider("Batch Size for Processing", min_value=100, max_value=5000, value=1000, step=100)
    with st.spinner(f"🔄 Matching records in batches of {batch_size}, please wait..."):
        basis_df = read_file(basis_file, is_basis=True)
        finacle_df = read_file(finacle_file, is_basis=False)
        matches_df, mismatches_basis_df, mismatches_finacle_df = find_fuzzy_matches_batched(basis_df, finacle_df, batch_size=batch_size)

    st.success("✅ Matching complete!")

    st.subheader("✅ Matches")
    if not matches_df.empty:
        st.dataframe(matches_df)
        excel_data = convert_df(matches_df)
        st.download_button(
            label="📥 Download Matches as Excel",
            data=excel_data,
            file_name="matches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("No matches found.")

    st.subheader("❌ Unmatched Records from BASIS")
    if not mismatches_basis_df.empty:
        st.dataframe(mismatches_basis_df)
        excel_data = convert_df(mismatches_basis_df)
        st.download_button(
            label="📥 Download Unmatched BASIS Records as Excel",
            data=excel_data,
            file_name="unmatched_basis.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("All BASIS records were matched.")

    st.subheader("⚠️ Unmatched Records from FINACLE")
    if not mismatches_finacle_df.empty:
        st.dataframe(mismatches_finacle_df)
        excel_data = convert_df(mismatches_finacle_df)
        st.download_button(
            label="📥 Download Unmatched FINACLE Records as Excel",
            data=excel_data,
            file_name="unmatched_finacle.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("All FINACLE records were matched.")

else:
    st.info("Please upload both BASIS and FINACLE files to start matching.")