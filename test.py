import streamlit as st
import pandas as pd
from rapidfuzz import fuzz
from io import BytesIO

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔍 Finacle vs Basis Fuzzy Matching")

st.markdown("""
Upload your **BASIS** and **FINACLE** files below (CSV or Excel).  
We’ll compare them row-by-row using fuzzy logic on Name, Email, DOB, and Phones.
""")

# ── File upload ────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    basis_upload = st.file_uploader("📂 BASIS file", type=["csv","xlsx","xls"], key="basis")
with col2:
    finacle_upload = st.file_uploader("📂 FINACLE file", type=["csv","xlsx","xls"], key="finacle")

# ── Cached readers ─────────────────────────────────────────────────────────────
@st.cache_data
def load_df(uploaded, is_basis: bool) -> pd.DataFrame:
    if uploaded.name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(uploaded, dtype=str, keep_default_na=False)
    df = df.fillna("")  # no NaNs

    # normalize text columns
    for col in df.select_dtypes("object").columns:
        df[col] = df[col].str.strip().str.lower()

    return df

# ── Preprocessing ────────────────────────────────────────────────────────────
def prep_basis(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "cus_sho_name": "Name",
        "email": "Email_Basis",
        "bir_date": "DOB_Basis",
        "tel_num": "Phone_1_Basis",
        "tel_num_2": "Phone_2_Basis",
        "fax_num": "Phone_3_Basis",
    })
    # ensure all these exist
    for c in ["Name","Email_Basis","DOB_Basis","Phone_1_Basis","Phone_2_Basis","Phone_3_Basis"]:
        if c not in df.columns:
            df[c] = ""
    return df[["Name","Email_Basis","DOB_Basis","Phone_1_Basis","Phone_2_Basis","Phone_3_Basis"]]

def prep_finacle(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "name": "Name",
        "preferredemail": "Email_Finacle",
        "cust_dob": "DOB_Finacle",
        "preferredphone": "Phone_1_Finacle",
        "smsbankingmobilenumber": "Phone_2_Finacle",
    })
    # add missing columns
    df["Phone_3_Finacle"] = df.get("Phone_3_Finacle","")
    for c in ["Name","Email_Finacle","DOB_Finacle","Phone_1_Finacle","Phone_2_Finacle","Phone_3_Finacle"]:
        if c not in df.columns:
            df[c] = ""
    return df[["Name","Email_Finacle","DOB_Finacle","Phone_1_Finacle","Phone_2_Finacle","Phone_3_Finacle"]]

# ── Fuzzy compare row-by-row ──────────────────────────────────────────────────
def compare_dfs(basis: pd.DataFrame, fin: pd.DataFrame, threshold: float=70):
    n = min(len(basis), len(fin))
    matches, mismatches = [], []
    for i in range(n):
        b = basis.iloc[i]
        f = fin.iloc[i]
        # scores
        name_s = fuzz.token_sort_ratio(b.Name, f.Name)
        email_s = fuzz.ratio(b.Email_Basis, f.Email_Finacle)
        # simplistic DOB proximity (exact match = 100, else 0)
        dob_s = 100 if b.DOB_Basis == f.DOB_Finacle and b.DOB_Basis else 0
        # phone overlap
        setb = {b.Phone_1_Basis, b.Phone_2_Basis, b.Phone_3_Basis} - {""}
        setf = {f.Phone_1_Finacle, f.Phone_2_Finacle, f.Phone_3_Finacle} - {""}
        phone_s = int(len(setb & setf) / (len(setb | setf) or 1) * 100)

        total = name_s*0.4 + email_s*0.3 + dob_s*0.2 + phone_s*0.1

        rec = {
            "Name_Basis": b.Name,
            "Name_Finacle": f.Name,
            "Email_Basis": b.Email_Basis,
            "Email_Finacle": f.Email_Finacle,
            "DOB_Basis": b.DOB_Basis,
            "DOB_Finacle": f.DOB_Finacle,
            "Phones_Basis": ", ".join(sorted(setb)),
            "Phones_Finacle": ", ".join(sorted(setf)),
            "Score": round(total,2)
        }
        if total >= threshold:
            matches.append(rec)
        else:
            mismatches.append(rec)

    # any extra Basis rows?
    for i in range(n, len(basis)):
        b = basis.iloc[i]
        mismatches.append({**{
            "Name_Basis":b.Name, "Name_Finacle":"",
            "Email_Basis":b.Email_Basis,"Email_Finacle":"",
            "DOB_Basis":b.DOB_Basis,  "DOB_Finacle":"",
            "Phones_Basis":", ".join(sorted({b.Phone_1_Basis,b.Phone_2_Basis,b.Phone_3_Basis}-{""})),
            "Phones_Finacle":""}, "Score":0})
    # any extra Finacle rows?
    for i in range(n, len(fin)):
        f = fin.iloc[i]
        mismatches.append({**{
            "Name_Basis":"", "Name_Finacle":f.Name,
            "Email_Basis":"","Email_Finacle":f.Email_Finacle,
            "DOB_Basis":"","DOB_Finacle":f.DOB_Finacle,
            "Phones_Basis":"", "Phones_Finacle":", ".join(sorted({f.Phone_1_Finacle,f.Phone_2_Finacle,f.Phone_3_Finacle}-{""}))},
            "Score":0})

    return pd.DataFrame(matches), pd.DataFrame(mismatches)

# ── When both files are ready ────────────────────────────────────────────────
if basis_upload and finacle_upload:
    with st.spinner("🔄 Loading & comparing…"):
        raw_basis = load_df(basis_upload, is_basis=True)
        raw_fin    = load_df(finacle_upload, is_basis=False)
        basis = prep_basis(raw_basis)
        fin    = prep_finacle(raw_fin)
        matches, mismatches = compare_dfs(basis, fin)

    st.success(f"✅ Done: {len(matches)} matches, {len(mismatches)} mismatches")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("✅ Matches")
        st.dataframe(matches)
        if not matches.empty:
            buf = BytesIO(); matches.to_excel(buf,index=False); buf.seek(0)
            st.download_button("⬇ Download Matches", buf, "matches.xlsx")
    with c2:
        st.subheader("❌ Mismatches")
        st.dataframe(mismatches)
        if not mismatches.empty:
            buf2 = BytesIO(); mismatches.to_excel(buf2,index=False); buf2.seek(0)
            st.download_button("⬇ Download Mismatches", buf2, "mismatches.xlsx")

else:
    st.info("⬆ Please upload both your BASIS and FINACLE files to begin.")
