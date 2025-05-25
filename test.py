import streamlit as st
import pandas as pd
from rapidfuzz import fuzz
from io import BytesIO

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔍 Finacle vs Basis Fuzzy Matching Tool")

st.markdown("Upload your **BASIS** and **FINACLE** files to begin comparison.")

# File Uploads
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📂 Upload BASIS File", type=["csv", "xlsx", "xls"])
with col2:
    finacle_file = st.file_uploader("📂 Upload FINACLE File", type=["csv", "xlsx", "xls"])

@st.cache_data
def load_file(file):
    if file.name.endswith('.csv'):
        df = pd.read_csv(file, dtype=str)
    else:
        df = pd.read_excel(file, dtype=str)
    return df.fillna('').applymap(str.strip)

def prepare_basis(df):
    df = df.rename(columns={
        "cus_sho_name": "Name",
        "email": "Email_Basis",
        "bir_date": "DOB_Basis",
        "tel_num": "Phone_1_Basis",
        "tel_num_2": "Phone_2_Basis",
        "fax_num": "Phone_3_Basis"
    })
    for col in ["Name", "Email_Basis", "DOB_Basis", "Phone_1_Basis", "Phone_2_Basis", "Phone_3_Basis"]:
        if col not in df.columns:
            df[col] = ""
    return df

def prepare_finacle(df):
    df = df.rename(columns={
        "name": "Name",
        "preferredemail": "Email_Finacle",
        "cust_dob": "DOB_Finacle",
        "preferredphone": "Phone_1_Finacle",
        "smsbankingmobilenumber": "Phone_2_Finacle"
    })
    for col in ["Name", "Email_Finacle", "DOB_Finacle", "Phone_1_Finacle", "Phone_2_Finacle", "Phone_3_Finacle"]:
        if col not in df.columns:
            df[col] = ""
    return df

def fuzzy_compare(basis_df, finacle_df, threshold=70):
    matches = []
    mismatches = []
    n = min(len(basis_df), len(finacle_df))

    for i in range(n):
        b = basis_df.iloc[i]
        f = finacle_df.iloc[i]

        score_name = fuzz.token_sort_ratio(b["Name"], f["Name"])
        score_email = fuzz.ratio(b["Email_Basis"], f["Email_Finacle"])
        score_dob = 100 if b["DOB_Basis"] == f["DOB_Finacle"] and b["DOB_Basis"] != "" else 0

        phones_b = {b["Phone_1_Basis"], b["Phone_2_Basis"], b["Phone_3_Basis"]} - {""}
        phones_f = {f["Phone_1_Finacle"], f["Phone_2_Finacle"], f["Phone_3_Finacle"]} - {""}
        phone_overlap = len(phones_b & phones_f) / (len(phones_b | phones_f) or 1)
        score_phone = int(phone_overlap * 100)

        total_score = round(score_name * 0.4 + score_email * 0.3 + score_dob * 0.2 + score_phone * 0.1, 2)

        record = {
            "Name_Basis": b["Name"],
            "Name_Finacle": f["Name"],
            "Email_Basis": b["Email_Basis"],
            "Email_Finacle": f["Email_Finacle"],
            "DOB_Basis": b["DOB_Basis"],
            "DOB_Finacle": f["DOB_Finacle"],
            "Phones_Basis": ", ".join(phones_b),
            "Phones_Finacle": ", ".join(phones_f),
            "Score": total_score
        }

        if total_score >= threshold:
            matches.append(record)
        else:
            mismatches.append(record)

    # Extra rows
    for i in range(n, len(basis_df)):
        b = basis_df.iloc[i]
        mismatches.append({
            "Name_Basis": b["Name"], "Name_Finacle": "",
            "Email_Basis": b["Email_Basis"], "Email_Finacle": "",
            "DOB_Basis": b["DOB_Basis"], "DOB_Finacle": "",
            "Phones_Basis": ", ".join({b["Phone_1_Basis"], b["Phone_2_Basis"], b["Phone_3_Basis"]} - {""}),
            "Phones_Finacle": "", "Score": 0
        })

    for i in range(n, len(finacle_df)):
        f = finacle_df.iloc[i]
        mismatches.append({
            "Name_Basis": "", "Name_Finacle": f["Name"],
            "Email_Basis": "", "Email_Finacle": f["Email_Finacle"],
            "DOB_Basis": "", "DOB_Finacle": f["DOB_Finacle"],
            "Phones_Basis": "", "Phones_Finacle": ", ".join({f["Phone_1_Finacle"], f["Phone_2_Finacle"], f["Phone_3_Finacle"]} - {""}),
            "Score": 0
        })

    return pd.DataFrame(matches), pd.DataFrame(mismatches)

def download_button(df, filename, label):
    output = BytesIO()
    df.to_excel(output, index=False)
    st.download_button(label=label, data=output.getvalue(), file_name=filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Run comparison
if basis_file and finacle_file:
    with st.spinner("Processing..."):
        df_basis = prepare_basis(load_file(basis_file))
        df_finacle = prepare_finacle(load_file(finacle_file))
        matches, mismatches = fuzzy_compare(df_basis, df_finacle)

    st.success(f"✔ Found {len(matches)} matches and {len(mismatches)} mismatches")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("✅ Matches")
        st.dataframe(matches, use_container_width=True)
        if not matches.empty:
            download_button(matches, "matches.xlsx", "📥 Download Matches")

    with col2:
        st.subheader("❌ Mismatches")
        st.dataframe(mismatches, use_container_width=True)
        if not mismatches.empty:
            download_button(mismatches, "mismatches.xlsx", "📥 Download Mismatches")
else:
    st.info("📌 Please upload both files to proceed.")
