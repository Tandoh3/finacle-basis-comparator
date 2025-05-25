import streamlit as st
import pandas as pd
from rapidfuzz import fuzz
from io import BytesIO
import time

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔍 Finacle vs Basis Fuzzy Matching")

st.markdown(
    "Upload your BASIS and FINACLE files below (CSV or Excel). "
    "Matching will be done using fuzzy logic for names, emails, dates of birth, and phone numbers."
)

# === 1. Upload Section ===
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📂 Upload BASIS file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="basis")
with col2:
    finacle_file = st.file_uploader("📂 Upload FINACLE file (CSV or Excel)", type=["csv", "xlsx", "xls"], key="finacle")

# === 2. Helper Functions ===

def read_file_pd(file):
    if file.name.endswith('.csv'):
        return pd.read_csv(file, dtype=str)
    else:
        return pd.read_excel(file, dtype=str)

def preprocess_basis_pd(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email_Basis",
        "BIR_DATE": "Date_of_Birth_Basis",
        "TEL_NUM": "Phone_1_Basis",
        "TEL_NUM_2": "Phone_2_Basis",
        "FAX_NUM": "Phone_3_Basis",
        "MOB_NUM": "Phone_4_Basis"
    })
    # Ensure all expected columns exist, fill with empty string if not
    expected_cols = ["Name", "Email_Basis", "Date_of_Birth_Basis", "Phone_1_Basis", "Phone_2_Basis", "Phone_3_Basis", "Phone_4_Basis"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""
    return df[expected_cols].fillna('')

def preprocess_finacle_pd(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "NAME": "Name",
        "PREFERREDEMAIL": "Email_Finacle",
        "CUST_DOB": "Date_of_Birth_Finacle",
        "PREFERREDPHONE": "Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER": "Phone_2_Finacle"
    })
    # Ensure all expected columns exist, fill with empty string if not
    expected_cols = ["Name", "Email_Finacle", "Date_of_Birth_Finacle", "Phone_1_Finacle", "Phone_2_Finacle", "Phone_3_Finacle"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""
    df['Phone_3_Finacle'] = df.get('Phone_3_Finacle', '') # Ensure Phone_3_Finacle exists
    return df[expected_cols].fillna('')

def normalize_pd(series: pd.Series) -> pd.Series:
    return series.str.strip().str.lower()

def fuzzy_match_string_pd(s1: str, s2: str, threshold: int) -> bool:
    if not s1 or not s2:
        return False
    return fuzz.ratio(s1, s2) >= threshold

def fuzzy_score_string_pd(s1: str, s2: str) -> int:
    return fuzz.ratio(s1, s2) if s1 and s2 else 0

def fuzzy_match_phones_pd(phones1: list, phones2: list, threshold: int) -> bool:
    set1 = set([p for p in phones1 if p])
    set2 = set([p for p in phones2 if p])
    if not set1 or not set2:
        return True if not set1 and not set2 else False
    intersection = set1 & set2
    union = set1 | set2
    score = (len(intersection) / len(union)) * 100 if union else 100
    return score >= threshold

def fuzzy_score_phones_pd(phones1: list, phones2: list) -> float:
    set1 = set([p for p in phones1 if p])
    set2 = set([p for p in phones2 if p])
    if not set1 or not set2:
        return 100.0 if not set1 and not set2 else 0.0
    intersection = set1 & set2
    union = set1 | set2
    return (len(intersection) / len(union)) * 100 if union else 100.0

def fuzzy_match_dates_pd(d1_str: str, d2_str: str, threshold_days: int) -> bool:
    try:
        date1 = pd.to_datetime(d1_str, errors='coerce')
        date2 = pd.to_datetime(d2_str, errors='coerce')
        if pd.isna(date1) or pd.isna(date2):
            return False
        diff = abs((date1 - date2).days)
        return diff <= threshold_days
    except:
        return False

def fuzzy_score_dates_pd(d1_str: str, d2_str: str, threshold_days: int) -> float:
    try:
        date1 = pd.to_datetime(d1_str, errors='coerce')
        date2 = pd.to_datetime(d2_str, errors='coerce')
        if pd.isna(date1) or pd.isna(date2):
            return 0.0
        diff = abs((date1 - date2).days)
        return max(0, 100 - (diff / threshold_days * 100)) if threshold_days > 0 else 100.0
    except:
        return 0.0

def find_fuzzy_matches_pd(basis_df: pd.DataFrame, finacle_df: pd.DataFrame, name_threshold=85, email_threshold=90, phone_threshold=85, dob_threshold_days=30):
    basis_df_norm = basis_df.apply(normalize_pd)
    finacle_df_norm = finacle_df.apply(normalize_pd)

    matches = []
    mismatches_basis = []
    mismatches_finacle_indices = set()

    for idx_b, row_b in basis_df.iterrows():
        best_match = None
        best_score = 0
        best_idx_f = None
        phones_b = [row_b['Phone_1_Basis'], row_b['Phone_2_Basis'], row_b['Phone_3_Basis'], row_b.get('Phone_4_Basis', '')]
        row_b_norm = basis_df_norm.loc[idx_b]

        for idx_f, row_f in finacle_df.iterrows():
            if idx_f in mismatches_finacle_indices:
                continue
            phones_f = [row_f['Phone_1_Finacle'], row_f['Phone_2_Finacle'], row_f['Phone_3_Finacle']]
            row_f_norm = finacle_df_norm.loc[idx_f]

            name_match = fuzzy_match_string_pd(row_b_norm['Name'], row_f_norm['Name'], name_threshold)
            email_match = fuzzy_match_string_pd(row_b_norm['Email_Basis'], row_f_norm['Email_Finacle'], email_threshold)
            phone_match = fuzzy_match_phones_pd(phones_b, phones_f, phone_threshold)
            dob_match = fuzzy_match_dates_pd(row_b['Date_of_Birth_Basis'], row_f['Date_of_Birth_Finacle'], dob_threshold_days)

            name_score = fuzzy_score_string_pd(row_b_norm['Name'], row_f_norm['Name'])
            email_score = fuzzy_score_string_pd(row_b_norm['Email_Basis'], row_f_norm['Email_Finacle'])
            phone_score = fuzzy_score_phones_pd(phones_b, phones_f)
            dob_score = fuzzy_score_dates_pd(row_b['Date_of_Birth_Basis'], row_f['Date_of_Birth_Finacle'], dob_threshold_days)

            total_score = (name_score * 0.4) + (email_score * 0.3) + (dob_score * 0.2) + (phone_score * 0.1) if name_match else 0

            if total_score > best_score and name_match:
                best_score = total_score
                best_match = row_f
                best_idx_f = idx_f

        if best_match is not None:
            matches.append({
                "Basis_Name": row_b['Name'],
                "Finacle_Name": best_match['Name'],
                "Email_Basis": row_b['Email_Basis'],
                "Email_Finacle": best_match['Email_Finacle'],
                "DOB_Basis": row_b['Date_of_Birth_Basis'],
                "DOB_Finacle": best_match['Date_of_Birth_Finacle'],
                "Phones_Basis": phones_b,
                "Phones_Finacle": [best_match['Phone_1_Finacle'], best_match['Phone_2_Finacle'], best_match['Phone_3_Finacle']],
                "Score": round(best_score, 2)
            })
            mismatches_finacle_indices.add(best_idx_f)
        else:
            mismatches_basis.append(row_b.to_dict())

    mismatches_finacle = finacle_df.iloc[list(set(finacle_df.index) - mismatches_finacle_indices)].to_dict('records')

    return pd.DataFrame(matches), pd.DataFrame(mismatches_basis), pd.DataFrame(mismatches_finacle)

def convert_df_pd(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# === 3. Main Logic ===
if basis_file and finacle_file:
    with st.spinner("🔄 Matching records, please wait..."):
        start_time = time.time()
        basis_df = read_file_pd(basis_file)
        finacle_df = read_file_pd(finacle_file)

        basis_df_processed = preprocess_basis_pd(basis_df.copy())
        finacle_df_processed = preprocess_finacle_pd(finacle_df.copy())

        matches_df, mismatches_basis_df, mismatches_finacle_df = find_fuzzy_matches_pd(
            basis_df_processed, finacle_df_processed
        )
        end_time = time.time()
        st.write(f"Processing time: {end_time - start_time:.2f} seconds")

    st.success("✅ Matching complete!")

    st.subheader("✅ Matches")
    if not matches_df.empty:
        st.dataframe(matches_df)
        excel_data = convert_df_pd(matches_df)
        st.download_button(
            label="📥 Download Matches as Excel",
            data=excel_data,
            file_name="matches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("No matches found based on the defined thresholds.")

    st.subheader("💔 Unmatched Records (from BASIS)")
    if not mismatches_basis_df.empty:
        st.dataframe(mismatches_basis_df)
        excel_data = convert_df_pd(mismatches_basis_df)
        st.download_button(
            label="📥 Download Unmatched BASIS Records as Excel",
            data=excel_data,
            file_name="unmatched_basis.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("All BASIS records have a potential match.")

    st.subheader("💔 Unmatched Records (from FINACLE)")
    if not mismatches_finacle_df.empty:
        st.dataframe(mismatches_finacle_df)
        excel_data = convert_df_pd(mismatches_finacle_df)
        st.download_button(
            label="📥 Download Unmatched FINACLE Records as Excel",
            data=excel_data,
            file_name="unmatched_finacle.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("All FINACLE records have a potential match.")

else:
    st.info("Please upload both BASIS and FINACLE files to start matching.")