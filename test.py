import streamlit as st
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
from io import BytesIO
from collections import defaultdict

# 1) UI setup
st.set_page_config(page_title="Streamed Finacle vs Basis Matching", layout="wide")
st.title("🔍 Streamed Finacle vs Basis Fuzzy Matching")

st.markdown("""
Upload your BASIS (large) and FINACLE (reference) files.
We’ll read BASIS in **chunks** and match each chunk immediately, streaming results.
""")

basis_file = st.file_uploader("📂 BASIS (CSV only)", type=["csv"])
finacle_file = st.file_uploader("📂 FINACLE (CSV or Excel)", type=["csv","xlsx","xls"])

# 2) Helper functions (same as before)
def read_finacle(file):
    # read FINACLE all at once
    if file.name.endswith(".csv"):
        df = pd.read_csv(file, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(file, dtype=str, keep_default_na=False)
    df = df.fillna("")
    # clean phones
    for c in ["PREFERREDPHONE","SMSBANKINGMOBILENUMBER"]:
        if c in df:
            df[c] = df[c].str.replace(r"\D+","",regex=True)
    # rename
    df = df.rename(columns={
        "NAME":"Name",
        "PREFERREDEMAIL":"Email_Finacle",
        "CUST_DOB":"DOB_Finacle",
        "PREFERREDPHONE":"Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER":"Phone_2_Finacle"
    })
    df["Phone_3_Finacle"] = ""
    df = df[["Name","Email_Finacle","DOB_Finacle",
             "Phone_1_Finacle","Phone_2_Finacle","Phone_3_Finacle"]]
    df = df.applymap(lambda x: str(x).strip().lower())
    return df

def preprocess_basis_chunk(df):
    df = df.fillna("")
    # clean phones
    for c in ["TEL_NUM","TEL_NUM_2","FAX_NUM"]:
        if c in df:
            df[c] = df[c].str.replace(r"\D+","",regex=True)
    df = df.rename(columns={
        "CUS_SHO_NAME":"Name",
        "EMAIL":"Email_Basis",
        "BIR_DATE":"DOB_Basis",
        "TEL_NUM":"Phone_1_Basis",
        "TEL_NUM_2":"Phone_2_Basis",
        "FAX_NUM":"Phone_3_Basis"
    })
    df = df[["Name","Email_Basis","DOB_Basis",
             "Phone_1_Basis","Phone_2_Basis","Phone_3_Basis"]]
    return df.applymap(lambda x: str(x).strip().lower())

def score_pair(b, f):
    name_score = fuzz.token_sort_ratio(b.Name, f.Name)
    email_score = fuzz.ratio(b.Email_Basis, f.Email_Finacle)
    # date similarity 30-day tolerance
    try:
        d1 = pd.to_datetime(b.DOB_Basis, errors="coerce")
        d2 = pd.to_datetime(f.DOB_Finacle, errors="coerce")
        diff = abs((d1-d2).days) if pd.notna(d1) and pd.notna(d2) else 999
    except:
        diff = 999
    dob_score = max(0, 100 - diff/30*100) if diff<999 else 0
    # phone overlap
    setb = {p for p in (b.Phone_1_Basis,b.Phone_2_Basis,b.Phone_3_Basis) if p}
    setf = {p for p in (f.Phone_1_Finacle,f.Phone_2_Finacle,f.Phone_3_Finacle) if p}
    if setb or setf:
        phone_score = len(setb&setf)/len(setb|setf)*100
    else:
        phone_score = 0
    return name_score*0.4 + email_score*0.3 + dob_score*0.2 + phone_score*0.1

# 3) Kick off when both files are present
if basis_file and finacle_file:
    fin_df = read_finacle(finacle_file)
    # build simple block index on first 3 letters + year of dob
    fin_df["block"] = fin_df.Name.str[:3] + fin_df.DOB_Finacle.str[:4]
    block_map = fin_df.groupby("block").groups
    
    matched = []
    mismatched = []
    matched_fin_idxs = set()
    
    # placeholders
    progress = st.progress(0)
    table_holder = st.empty()
    
    # 4) Stream BASIS chunks
    chunk_iter = pd.read_csv(basis_file, dtype=str, keep_default_na=False, chunksize=500)
    total = 0
    for i, chunk in enumerate(chunk_iter):
        chunk = preprocess_basis_chunk(chunk)
        total += len(chunk)
        # for each row in chunk
        for idx, row in chunk.iterrows():
            blk = row.Name[:3] + row.DOB_Basis[:4]
            cands = block_map.get(blk, [])
            # phone overlap block
            for p in (row.Phone_1_Basis,row.Phone_2_Basis,row.Phone_3_Basis):
                cands += block_map.get(p, [])
            best_score, best_idx = 0, None
            for fidx in set(cands):
                if fidx in matched_fin_idxs: continue
                score = score_pair(row, fin_df.loc[fidx])
                if score>best_score:
                    best_score, best_idx = score, fidx
            if best_score>=70 and best_idx is not None:
                matched_fin_idxs.add(best_idx)
                frow = fin_df.loc[best_idx]
                matched.append({
                    "Basis_Name": row.Name, "Finacle_Name": frow.Name,
                    "Score":round(best_score,2)
                })
            else:
                mismatched.append({
                    "Type":"Basis","Name":row.Name,
                    "Score":round(best_score,2)
                })
        progress.progress((i+1)/len(chunk_iter))
        # show partial
        table_holder.dataframe(pd.DataFrame(matched).tail(10))
    # after all chunks
    unmatched_fin = set(fin_df.index) - matched_fin_idxs
    # add finacle‐side mismatches
    for fidx in unmatched_fin:
        frow = fin_df.loc[fidx]
        mismatched.append({"Type":"Finacle","Name":frow.Name,"Score":0})
    
    st.success(f"Done! {len(matched)} matches, {len(mismatched)} mismatches")
    st.subheader("Matches")
    st.dataframe(pd.DataFrame(matched))
    st.subheader("Mismatches")
    st.dataframe(pd.DataFrame(mismatched))
    
    # download
    def to_xlsx(df):
        buf = BytesIO()
        df.to_excel(buf,index=False)
        buf.seek(0)
        return buf
    st.download_button("⬇ Download Matches", to_xlsx(pd.DataFrame(matched)), "matches.xlsx")
    st.download_button("⬇ Download Mismatches", to_xlsx(pd.DataFrame(mismatched)), "mismatches.xlsx")
