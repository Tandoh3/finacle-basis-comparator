import streamlit as st
import pandas as pd
from rapidfuzz import fuzz
from io import BytesIO
from collections import defaultdict

st.set_page_config(page_title="Streamed Matching", layout="wide")
st.title("🔍 Streamed Finacle vs Basis Matcher")

st.markdown("""
Upload **BASIS** (huge) and **FINACLE** (reference) files.
BASIS will be read in chunks and matched on the fly.
""")

basis_file = st.file_uploader("📂 BASIS (CSV only)", type=["csv"])
finacle_file = st.file_uploader("📂 FINACLE (CSV/Excel)", type=["csv","xlsx","xls"])

# ———————————
# 1) Read FINACLE once & build indexes
# ———————————
@st.cache_data
def load_finacle(file):
    if file.name.endswith(".csv"):
        df = pd.read_csv(file, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(file, dtype=str, keep_default_na=False)
    df = df.fillna("")
    for c in ["PREFERREDPHONE","SMSBANKINGMOBILENUMBER"]:
        if c in df:
            df[c] = df[c].str.replace(r"\D+","",regex=True)
    df = df.rename(columns={
        "NAME":"Name",
        "PREFERREDEMAIL":"Email_Finacle",
        "CUST_DOB":"DOB_Finacle",
        "PREFERREDPHONE":"Phone_1_Finacle",
        "SMSBANKINGMOBILENUMBER":"Phone_2_Finacle"
    })
    df["Phone_3_Finacle"] = ""
    # normalize text columns
    df = df.apply(lambda col: col.str.strip().str.lower() if col.dtype == "object" else col)
    df["block"] = df.Name.str[:3] + df.DOB_Finacle.str[:4]
    return df, df.groupby("block").groups

# ———————————
# 2) Score function
# ———————————
def score_pair(b, f):
    name_score = fuzz.token_sort_ratio(b.Name, f.Name)
    email_score = fuzz.ratio(b.Email_Basis, f.Email_Finacle)
    try:
        d1 = pd.to_datetime(b.DOB_Basis, errors="coerce")
        d2 = pd.to_datetime(f.DOB_Finacle, errors="coerce")
        diff = abs((d1 - d2).days) if pd.notna(d1) and pd.notna(d2) else 365*100
    except:
        diff = 365*100
    dob_score = max(0, 100 - diff/30*100) if diff<365*100 else 0
    setb = {p for p in (b.Phone_1_Basis, b.Phone_2_Basis, b.Phone_3_Basis) if p}
    setf = {p for p in (f.Phone_1_Finacle, f.Phone_2_Finacle, f.Phone_3_Finacle) if p}
    phone_score = len(setb & setf)/len(setb|setf)*100 if (setb|setf) else 0
    return name_score*0.4 + email_score*0.3 + dob_score*0.2 + phone_score*0.1

# ———————————
# 3) Stream BASIS chunks
# ———————————
if basis_file and finacle_file:
    fin_df, block_map = load_finacle(finacle_file)

    matched = []
    mismatched = []
    matched_fin_idxs = set()

    progress = st.progress(0)
    table = st.empty()

    chunks = pd.read_csv(basis_file, dtype=str, keep_default_na=False, chunksize=500)
    total_chunks = sum(1 for _ in pd.read_csv(basis_file, chunksize=500))
    chunks = pd.read_csv(basis_file, dtype=str, keep_default_na=False, chunksize=500)

    for idx, chunk in enumerate(chunks):
        # preprocess chunk
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

        for _, b_row in basis_df.iterrows():
            blk = b_row.Name[:3] + b_row.DOB_Basis[:4]
            cands = list(block_map.get(blk, []))
            for p in (b_row.Phone_1_Basis, b_row.Phone_2_Basis, b_row.Phone_3_Basis):
                if p and len(p)>=6:
                    cands.extend(block_map.get(p, []))

            if not cands:
                # fallback fuzzy name
                nm = process.extractOne(b_row.Name, fin_df.Name.tolist(),
                                        scorer=fuzz.token_sort_ratio, score_cutoff=70)
                if nm:
                    cands.append(fin_df.Name.tolist().index(nm[0]))

            best_score, best_idx = 0, None
            for fidx in set(cands):
                if fidx in matched_fin_idxs:
                    continue
                sc = score_pair(b_row, fin_df.iloc[fidx])
                if sc>best_score:
                    best_score, best_idx = sc, fidx

            if best_score>=70 and best_idx is not None:
                matched_fin_idxs.add(best_idx)
                frow = fin_df.iloc[best_idx]
                matched.append({"Basis":b_row.Name, "Finacle":frow.Name, "Score":round(best_score,2)})
            else:
                mismatched.append({"Type":"Basis","Name":b_row.Name,"Score":round(best_score,2)})

        progress.progress((idx+1)/total_chunks)
        table.dataframe(pd.DataFrame(matched).tail(10))

    # any FINACLE left?
    for fidx in set(fin_df.index)-matched_fin_idxs:
        mismatched.append({"Type":"Finacle","Name":fin_df.loc[fidx,"Name"],"Score":0})

    st.success(f"Done: {len(matched)} matches, {len(mismatched)} mismatches")
    st.subheader("Matches")
    st.dataframe(pd.DataFrame(matched))
    st.subheader("Mismatches")
    st.dataframe(pd.DataFrame(mismatched))

    def to_xlsx(df):
        buf = BytesIO(); df.to_excel(buf,index=False); buf.seek(0); return buf
    st.download_button("⬇ Download Matches", to_xlsx(pd.DataFrame(matched)), "matches.xlsx")
    st.download_button("⬇ Download Mismatches", to_xlsx(pd.DataFrame(mismatched)), "mismatches.xlsx")
