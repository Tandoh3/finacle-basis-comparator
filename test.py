import streamlit as st
import pandas as pd
from rapidfuzz import fuzz, process
from io import BytesIO
from collections import defaultdict

# === Streamlit Page Config ===
st.set_page_config(page_title="Streamed Finacle vs Basis Matching", layout="wide")
st.title("🔍 Streamed Finacle vs Basis Fuzzy Matching")

st.markdown(
    """
Upload **BASIS** (large) and **FINACLE** (reference) files below.
BASIS will be processed in **chunks** to keep the UI responsive.
Results appear incrementally and are shown side-by-side.
"""
)

# === Upload Section ===
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📂 BASIS file (CSV only)", type=["csv"], key="basis")
with col2:
    finacle_file = st.file_uploader("📂 FINACLE file (CSV/XLSX)", type=["csv","xlsx","xls"], key="finacle")

# === Helper Functions ===
@st.cache_data
def load_finacle(file):
    if file.name.endswith(".csv"):
        df = pd.read_csv(file, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(file, dtype=str, keep_default_na=False)
    df = df.fillna("")
    # Clean phones
    for c in ["PREFERREDPHONE","SMSBANKINGMOBILENUMBER"]:
        if c in df:
            df[c] = df[c].str.replace(r"\D+","",regex=True)
    # Rename to common schema
    df = df.rename(columns={
        "NAME":"Name",
        "PREFERREDEMAIL":"Email_Finacle",
        "CUST_DOB":"DOB_Finacle",
        "PREFERREDPHONE":"Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER":"Phone_2_Finacle"
    })
    df["Phone_3_Finacle"] = ""
    # Normalize text
    df = df.apply(lambda col: col.str.strip().str.lower() if col.dtype=="object" else col)
    # Block key
    df["block"] = df.Name.str[:3] + df.DOB_Finacle.str[:4]
    return df, df.groupby("block").groups


def score_pair(b, f):
    name_score = fuzz.token_sort_ratio(b.Name, f.Name)
    email_score = fuzz.ratio(b.Email_Basis, f.Email_Finacle)
    try:
        d1 = pd.to_datetime(b.DOB_Basis, errors="coerce")
        d2 = pd.to_datetime(f.DOB_Finacle, errors="coerce")
        diff = abs((d1 - d2).days) if pd.notna(d1) and pd.notna(d2) else 999
    except:
        diff = 999
    dob_score = max(0, 100 - diff/30*100) if diff<999 else 0
    setb = {p for p in [b.Phone_1_Basis, b.Phone_2_Basis, b.Phone_3_Basis] if p}
    setf = {p for p in [f.Phone_1_Finacle, f.Phone_2_Finacle, f.Phone_3_Finacle] if p}
    phone_score = len(setb & setf)/len(setb|setf)*100 if (setb|setf) else 0
    return name_score*0.4 + email_score*0.3 + dob_score*0.2 + phone_score*0.1

# === Main Logic ===
if basis_file and finacle_file:
    fin_df, block_map = load_finacle(finacle_file)

    matched = []
    mismatched = []
    matched_fin_idxs = set()

    progress = st.progress(0)
    placeholder = st.empty()

    # Read BASIS in chunks
    chunk_iter = pd.read_csv(basis_file, dtype=str, keep_default_na=False, chunksize=500)
    total_chunks = sum(1 for _ in pd.read_csv(basis_file, chunksize=500))
    chunk_iter = pd.read_csv(basis_file, dtype=str, keep_default_na=False, chunksize=500)

    for idx, chunk in enumerate(chunk_iter):
        # Preprocess chunk
        chunk = chunk.fillna("")
        for c in ["TEL_NUM","TEL_NUM_2","FAX_NUM"]:
            if c in chunk:
                chunk[c] = chunk[c].str.replace(r"\D+","",regex=True)
        chunk = chunk.rename(columns={
            "CUS_SHO_NAME":"Name",
            "EMAIL":"Email_Basis",
            "BIR_DATE":"DOB_Basis",
            "TEL_NUM":"Phone_1_Basis",
            "TEL_NUM_2":"Phone_2_Basis",
            "FAX_NUM":"Phone_3_Basis"
        })
        basis_df = chunk[["Name","Email_Basis","DOB_Basis","Phone_1_Basis","Phone_2_Basis","Phone_3_Basis"]]
        basis_df = basis_df.apply(lambda col: col.str.strip().str.lower() if col.dtype=="object" else col)

        # Match each row
        for _, b_row in basis_df.iterrows():
            blk = b_row.Name[:3] + b_row.DOB_Basis[:4]
            cands = list(block_map.get(blk, []))
            for p in (b_row.Phone_1_Basis, b_row.Phone_2_Basis, b_row.Phone_3_Basis):
                if p and len(p)>=6:
                    cands.extend(block_map.get(p, []))

            if not cands:
                nm = process.extractOne(b_row.Name, fin_df.Name.tolist(), scorer=fuzz.token_sort_ratio, score_cutoff=70)
                if nm:
                    cands.append(fin_df.Name.tolist().index(nm[0]))

            best_score, best_idx = 0, None
            for fidx in set(cands):
                if fidx in matched_fin_idxs: continue
                sc = score_pair(b_row, fin_df.iloc[fidx])
                if sc>best_score:
                    best_score, best_idx = sc, fidx

            if best_score>=70 and best_idx is not None:
                matched_fin_idxs.add(best_idx)
                frow = fin_df.iloc[best_idx]
                matched.append({
                    "Basis_Name":b_row.Name, "Finacle_Name":frow.Name, "Score":round(best_score,2)
                })
            else:
                mismatched.append({"Type":"Basis","Name":b_row.Name,"Score":round(best_score,2)})

        # update UI
        progress.progress((idx+1)/total_chunks)
        placeholder.dataframe(pd.DataFrame(matched).tail(5))

    # FINACLE unmatched
    for fidx in set(fin_df.index) - matched_fin_idxs:
        mismatched.append({"Type":"Finacle","Name":fin_df.loc[fidx,"Name"],"Score":0})

    # Display side by side
    colm1, colm2 = st.columns(2)
    with colm1:
        st.subheader("✅ Matches")
        st.dataframe(pd.DataFrame(matched))
    with colm2:
        st.subheader("❌ Mismatches")
        st.dataframe(pd.DataFrame(mismatched))

    # Download
    def to_xlsx(df):
        buf = BytesIO(); df.to_excel(buf,index=False); buf.seek(0); return buf

    colm1.download_button("⬇ Download Matches", to_xlsx(pd.DataFrame(matched)), "matches.xlsx")
    colm2.download_button("⬇ Download Mismatches", to_xlsx(pd.DataFrame(mismatched)), "mismatches.xlsx")
else:
    st.info("ℹ️ Please upload both BASIS and FINACLE files.")
