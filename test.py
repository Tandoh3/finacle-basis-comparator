import streamlit as st
import polars as pl
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from io import BytesIO

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔍 Finacle vs Basis Fuzzy Matching")

st.markdown(
    "Upload your BASIS and FINACLE files below (CSV or Excel). "
    "Matching will be done using fuzzy logic in batches for efficiency."
)

col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📂 Upload BASIS file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="basis")
with col2:
    finacle_file = st.file_uploader("📂 Upload FINACLE file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="finacle")

# === File Reading ===
def read_file(file, is_basis=True):
    schema = {"TEL_NUM": pl.Utf8, "TEL_NUM_2": pl.Utf8, "FAX_NUM": pl.Utf8} if is_basis else {
        "PREFERREDPHONE": pl.Utf8, "SMSBANKINGMOBILENUMBER": pl.Utf8
    }
    if file.name.endswith('.csv'):
        return pl.read_csv(file, dtypes=schema)
    else:
        return pl.read_excel(file, schema_overrides=schema)

# === Preprocessing ===
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
        pl.col("Phone_1_Basis").fill_null(""),
        pl.col("Phone_2_Basis").fill_null(""),
        pl.col("Phone_3_Basis").fill_null("")
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
        pl.col("Phone_1_Finacle").fill_null(""),
        pl.col("Phone_2_Finacle").fill_null(""),
        pl.col("Phone_3_Finacle").fill_null("")
    ])

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.strip_chars().str.to_lowercase().alias(col))
    return df

# === Fuzzy Matching Functions ===
def fuzzy_match_string(s1, s2, threshold=85):
    if not s1 or not s2:
        return False, 0
    score = fuzz.ratio(s1, s2)
    return score >= threshold, score

def fuzzy_match_phones(list1, list2, threshold=85):
    set1 = set(filter(None, list1))
    set2 = set(filter(None, list2))
    if not set1 and not set2:
        return True, 100
    intersection = set1 & set2
    union = set1 | set2
    score = (len(intersection) / len(union)) * 100 if union else 0
    return score >= threshold, score

def fuzzy_match_dates(d1, d2, threshold_days=30):
    try:
        date1 = pd.to_datetime(d1, errors='coerce')
        date2 = pd.to_datetime(d2, errors='coerce')
        if pd.isna(date1) or pd.isna(date2):
            return False, 0
        diff = abs((date1 - date2).days)
        return diff <= threshold_days, max(0, 100 - (diff / threshold_days * 100))
    except:
        return False, 0

# === Efficient Core Matching Logic with Batching ===
def efficient_find_fuzzy_matches(basis_df: pl.DataFrame, finacle_df: pl.DataFrame, batch_size=5000):
    basis_df = normalize(preprocess_basis(basis_df))
    finacle_df = normalize(preprocess_finacle(finacle_df))

    matches = []
    mismatches_basis = []
    matched_finacle_indices = set()

    basis_n_batches = (len(basis_df) + batch_size - 1) // batch_size
    for i in range(basis_n_batches):
        basis_batch = basis_df.slice(i * batch_size, batch_size).to_pandas().reset_index(names='basis_index')
        st.info(f"Processing BASIS batch {i + 1} of {basis_n_batches}")

        finacle_n_batches = (len(finacle_df) + batch_size - 1) // batch_size
        for j in range(finacle_n_batches):
            finacle_batch = finacle_df.slice(j * batch_size, batch_size).to_pandas().reset_index(names='finacle_index')
            st.info(f"  Comparing with FINACLE batch {j + 1} of {finacle_n_batches}")

            for b_idx, b_row in basis_batch.iterrows():
                if b_row['basis_index'] in [match['Basis_Index'] for match in matches if 'Basis_Index' in match]:
                    continue

                best_score = 0
                best_match = None
                best_f_idx = None

                for f_idx, f_row in finacle_batch.iterrows():
                    if f_row['finacle_index'] in matched_finacle_indices:
                        continue

                    name_match, name_score = fuzzy_match_string(b_row["Name"], f_row["Name"], 85)
                    if name_match:
                        email_match, email_score = fuzzy_match_string(b_row["Email_Basis"], f_row["Email_Finacle"], 90)
                        phone_match, phone_score = fuzzy_match_phones([b_row[f"Phone_{k}_Basis"] for k in range(1, 4)],
                                                                     [f_row[f"Phone_{k}_Finacle"] for k in range(1, 4)], 85)
                        dob_match, dob_score = fuzzy_match_dates(b_row["Date_of_Birth_Basis"], f_row["Date_of_Birth_Finacle"], 30)

                        total_score = name_score * 0.4 + email_score * 0.3 + dob_score * 0.2 + phone_score * 0.1

                        if total_score > best_score:
                            best_score = total_score
                            best_match = f_row
                            best_f_idx = f_row['finacle_index']

                if best_match is not None:
                    matches.append({
                        "Basis_Name": b_row["Name"],
                        "Finacle_Name": best_match["Name"],
                        "Email_Basis": b_row["Email_Basis"],
                        "Email_Finacle": best_match["Email_Finacle"],
                        "DOB_Basis": b_row["Date_of_Birth_Basis"],
                        "DOB_Finacle": best_match["Date_of_Birth_Finacle"],
                        "Phones_Basis": [b_row[f"Phone_{k}_Basis"] for k in range(1, 4)],
                        "Phones_Finacle": [best_match[f"Phone_{k}_Finacle"] for k in range(1, 4)],
                        "Score": round(best_score, 2),
                        "Basis_Index": b_row['basis_index'],
                        "Finacle_Index": best_f_idx
                    })
                    matched_finacle_indices.add(best_f_idx)

        st.progress((i + 1) / basis_n_batches)

    matched_basis_indices_final = set(match['Basis_Index'] for match in matches if 'Basis_Index' in match)
    mismatches_basis_df = basis_df.filter(~pl.Series(np.arange(len(basis_df))).is_in(list(matched_basis_indices_final))).to_pandas()

    matched_finacle_indices_final = set(match['Finacle_Index'] for match in matches if 'Finacle_Index' in match)
    mismatches_finacle_df = finacle_df.filter(~pl.Series(np.arange(len(finacle_df))).is_in(list(matched_finacle_indices_final))).to_pandas()

    return pd.DataFrame(matches), mismatches_basis_df, mismatches_finacle_df

# === Excel Export ===
def convert_df(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# === Trigger Matching ===
if basis_file and finacle_file:
    batch_size = st.slider("Batch Size for Processing", min_value=500, max_value=10000, value=5000, step=500)
    with st.spinner("🔄 Matching records in batches, please wait..."):
        basis_df = read_file(basis_file, is_basis=True)
        finacle_df = read_file(finacle_file, is_basis=False)
        matches_df, mismatches_basis_df, mismatches_finacle_df = efficient_find_fuzzy_matches(basis_df, finacle_df, batch_size=batch_size)

    st.success("✅ Matching complete!")

    st.subheader("✅ Matches")
    if not matches_df.empty:
        st.dataframe(matches_df)
        excel_data = convert_df(matches_df)
        st.download_button("📥 Download Matches", data=excel_data, file_name="matches.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("No matches found.")

    st.subheader("❌ Unmatched Records from BASIS")
    if not mismatches_basis_df.empty:
        st.dataframe(mismatches_basis_df)
        excel_data = convert_df(mismatches_basis_df)
        st.download_button("📥 Download Unmatched BASIS", data=excel_data, file_name="unmatched_basis.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("All BASIS records were matched.")

    st.subheader("⚠️ Unmatched Records from FINACLE")
    if not mismatches_finacle_df.empty:
        st.dataframe(mismatches_finacle_df)
        excel_data = convert_df(mismatches_finacle_df)
        st.download_button("📥 Download Unmatched FINACLE", data=excel_data, file_name="unmatched_finacle.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("All FINACLE records were matched.")

else:
    st.info("Please upload both BASIS and FINACLE files to begin.")