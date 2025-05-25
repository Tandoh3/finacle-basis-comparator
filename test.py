import streamlit as st
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
from io import BytesIO
import re
from datetime import datetime
from collections import defaultdict  # ✅ Added missing import

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

def read_file(file, is_basis=True):
    if file.name.endswith('.csv'):
        df = pd.read_csv(file, dtype='str', keep_default_na=False)
    else:
        df = pd.read_excel(file, dtype='str', keep_default_na=False)
    
    phone_cols = ['TEL_NUM', 'TEL_NUM_2', 'FAX_NUM'] if is_basis else ['PREFERREDPHONE', 'SMSBANKINGMOBILENUMBER']
    for col in phone_cols:
        if col in df.columns:
            df[col] = df[col].str.replace(r'\D+', '', regex=True)
    return df

def preprocess_basis(df):
    return df.rename(columns={
        "CUS_SHO_NAME": "Name",
        "EMAIL": "Email_Basis",
        "BIR_DATE": "DOB_Basis",
        "TEL_NUM": "Phone_1_Basis",
        "TEL_NUM_2": "Phone_2_Basis",
        "FAX_NUM": "Phone_3_Basis"
    })[['Name', 'Email_Basis', 'DOB_Basis', 'Phone_1_Basis', 'Phone_2_Basis', 'Phone_3_Basis']].fillna('')

def preprocess_finacle(df):
    df = df.rename(columns={
        "NAME": "Name",
        "PREFERREDEMAIL": "Email_Finacle",
        "CUST_DOB": "DOB_Finacle",
        "PREFERREDPHONE": "Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER": "Phone_2_Finacle"
    })
    df['Phone_3_Finacle'] = ''
    return df[['Name', 'Email_Finacle', 'DOB_Finacle', 'Phone_1_Finacle', 'Phone_2_Finacle', 'Phone_3_Finacle']].fillna('')

def normalize_text(s):
    return str(s).strip().lower()

def create_phone_index(df):
    phone_map = defaultdict(list)
    for idx, row in df.iterrows():
        for phone in [row['Phone_1_Finacle'], row['Phone_2_Finacle'], row['Phone_3_Finacle']]:
            if phone and len(phone) >= 6:
                phone_map[phone].append(idx)
    return phone_map

def date_similarity(d1, d2, threshold_days=30):
    try:
        date1 = pd.to_datetime(d1, errors='coerce')
        date2 = pd.to_datetime(d2, errors='coerce')
        if pd.isnull(date1) or pd.isnull(date2):
            return 0
        diff = abs((date1 - date2).days)
        return max(0, 100 - (diff / threshold_days * 100))
    except:
        return 0

def match_records(basis_df, finacle_df):
    basis_df = basis_df.applymap(normalize_text)
    finacle_df = finacle_df.applymap(normalize_text)

    phone_index = create_phone_index(finacle_df)
    finacle_df['block_key'] = finacle_df['Name'].str[:3] + finacle_df['DOB_Finacle'].str[:4]
    block_index = finacle_df.groupby('block_key').groups

    matches = []
    matched_indices = set()
    finacle_names = finacle_df['Name'].tolist()

    for basis_idx, basis_row in basis_df.iterrows():
        block_key = basis_row['Name'][:3] + basis_row['DOB_Basis'][:4]
        candidates = set()
        if block_key in block_index:
            candidates.update(block_index[block_key])

        for phone in [basis_row['Phone_1_Basis'], basis_row['Phone_2_Basis'], basis_row['Phone_3_Basis']]:
            if phone and len(phone) >= 6:
                candidates.update(phone_index.get(phone, []))

        if not candidates:
            name_matches = process.extractOne(
                basis_row['Name'], 
                finacle_names, 
                scorer=fuzz.token_sort_ratio,
                score_cutoff=70
            )
            if name_matches:
                candidates.add(finacle_names.index(name_matches[0]))

        best_score = 0
        best_match = None
        best_index = None

        for finacle_idx in candidates:
            if finacle_idx in matched_indices:
                continue

            finacle_row = finacle_df.iloc[finacle_idx]
            name_score = fuzz.token_sort_ratio(basis_row['Name'], finacle_row['Name'])
            email_score = fuzz.ratio(basis_row['Email_Basis'], finacle_row['Email_Finacle'])
            dob_score = date_similarity(basis_row['DOB_Basis'], finacle_row['DOB_Finacle'])

            basis_phones = {p for p in [basis_row['Phone_1_Basis'], basis_row['Phone_2_Basis'], basis_row['Phone_3_Basis']] if p}
            finacle_phones = {p for p in [finacle_row['Phone_1_Finacle'], finacle_row['Phone_2_Finacle'], finacle_row['Phone_3_Finacle']] if p}
            phone_score = 100 * len(basis_phones & finacle_phones) / len(basis_phones | finacle_phones) if basis_phones or finacle_phones else 0

            total_score = (name_score * 0.4 + email_score * 0.3 + dob_score * 0.2 + phone_score * 0.1)

            if total_score > best_score:
                best_score = total_score
                best_match = finacle_row
                best_index = finacle_idx

        if best_score >= 70 and best_match is not None:
            matched_indices.add(best_index)
            matches.append({
                'Basis_Name': basis_row['Name'],
                'Finacle_Name': best_match['Name'],
                'Email_Basis': basis_row['Email_Basis'],
                'Email_Finacle': best_match['Email_Finacle'],
                'DOB_Basis': basis_row['DOB_Basis'],
                'DOB_Finacle': best_match['DOB_Finacle'],
                'Phone_Basis': ', '.join(p for p in [basis_row['Phone_1_Basis'], basis_row['Phone_2_Basis'], basis_row['Phone_3_Basis']] if p),
                'Phone_Finacle': ', '.join(p for p in [best_match['Phone_1_Finacle'], best_match['Phone_2_Finacle'], best_match['Phone_3_Finacle']] if p),
                'Match_Score': round(best_score, 2)
            })

    all_finacle_indices = set(finacle_df.index)
    unmatched_finacle = all_finacle_indices - matched_indices

    mismatches = []
    matched_basis_names = {m['Basis_Name'] for m in matches}
    for idx, row in basis_df.iterrows():
        if row['Name'] not in matched_basis_names:
            mismatches.append({
                'Type': 'Basis',
                'Name': row['Name'],
                'Email': row['Email_Basis'],
                'DOB': row['DOB_Basis'],
                'Phones': ', '.join(p for p in [row['Phone_1_Basis'], row['Phone_2_Basis'], row['Phone_3_Basis']] if p)
            })

    for idx in unmatched_finacle:
        row = finacle_df.loc[idx]
        mismatches.append({
            'Type': 'Finacle',
            'Name': row['Name'],
            'Email': row['Email_Finacle'],
            'DOB': row['DOB_Finacle'],
            'Phones': ', '.join(p for p in [row['Phone_1_Finacle'], row['Phone_2_Finacle'], row['Phone_3_Finacle']] if p)
        })

    return pd.DataFrame(matches), pd.DataFrame(mismatches)

def convert_df(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# === 3. Main Logic ===
if basis_file and finacle_file:
    with st.spinner("🔄 Processing files and matching records..."):
        basis_df = preprocess_basis(read_file(basis_file, is_basis=True))
        finacle_df = preprocess_finacle(read_file(finacle_file, is_basis=False))
        matches_df, mismatches_df = match_records(basis_df, finacle_df)

    st.success(f"✅ Matching complete! Found {len(matches_df)} matches and {len(mismatches_df)} mismatches")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("✅ Matches")
        st.dataframe(matches_df.head(1000))
    with col2:
        st.subheader("❌ Mismatches")
        st.dataframe(mismatches_df.head(1000))

    if not matches_df.empty:
        st.download_button(
            label="📥 Download Matches",
            data=convert_df(matches_df),
            file_name="matches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    if not mismatches_df.empty:
        st.download_button(
            label="📥 Download Mismatches",
            data=convert_df(mismatches_df),
            file_name="mismatches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
else:
    st.info("ℹ️ Please upload both BASIS and FINACLE files to begin.")
