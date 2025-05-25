import streamlit as st
import pandas as pd
from rapidfuzz import fuzz, process
from io import BytesIO
import os

st.set_page_config(page_title="Finacle vs Basis Fuzzy Matching", layout="wide")
st.title("🔍 Finacle vs Basis Fuzzy Matching (Batch Mode)")

st.markdown(
    "Upload your BASIS and FINACLE files below (CSV or Excel). "
    "They’ll be saved to disk, then compared in one batch."
)

# 1) Upload
col1, col2 = st.columns(2)
with col1:
    basis_file = st.file_uploader("📂 BASIS file (CSV or Excel)", type=["csv","xlsx","xls"], key="basis")
with col2:
    finacle_file = st.file_uploader("📂 FINACLE file (CSV or Excel)", type=["csv","xlsx","xls"], key="finacle")

# 2) Save to disk as soon as uploaded
BASIS_PATH = "/mnt/data/basis_input"
FINACLE_PATH = "/mnt/data/finacle_input"

def save_upload(uploaded, path):
    if uploaded is None:
        return False
    ext = os.path.splitext(uploaded.name)[1]
    out = path + ext
    with open(out, "wb") as f:
        f.write(uploaded.getbuffer())
    return out

basis_path = save_upload(basis_file, BASIS_PATH) 
finacle_path = save_upload(finacle_file, FINACLE_PATH)

# 3) Once both are saved, load, compare, display
if basis_path and finacle_path:
    with st.spinner("🔄 Loading files and performing batch comparison…"):
        # load with pandas
        if basis_path.endswith(".csv"):
            basis_df = pd.read_csv(basis_path, dtype=str, keep_default_na=False)
        else:
            basis_df = pd.read_excel(basis_path, dtype=str, keep_default_na=False)
        if finacle_path.endswith(".csv"):
            fin_df = pd.read_csv(finacle_path, dtype=str, keep_default_na=False)
        else:
            fin_df = pd.read_excel(finacle_path, dtype=str, keep_default_na=False)

        # small cleanup & normalize
        def clean_norm(df, colmap, phone_cols):
            df = df.fillna("")
            for c in phone_cols:
                if c in df: df[c] = df[c].str.replace(r"\D+","",regex=True)
            df = df.rename(columns=colmap)
            for col in df.select_dtypes("object"):
                df[col] = df[col].str.strip().str.lower()
            return df

        basis = clean_norm(
            basis_df,
            {"CUS_SHO_NAME":"Name","EMAIL":"Email_Basis","BIR_DATE":"DOB_Basis",
             "TEL_NUM":"Phone_1_Basis","TEL_NUM_2":"Phone_2_Basis","FAX_NUM":"Phone_3_Basis"},
            ["TEL_NUM","TEL_NUM_2","FAX_NUM"]
        )
        fin = clean_norm(
            fin_df,
            {"NAME":"Name","PREFERREDEMAIL":"Email_Finacle","CUST_DOB":"DOB_Finacle",
             "PREFERREDPHONE":"Phone_1_Finacle","SMSBANKINGMOBILENUMBER":"Phone_2_Finacle"},
            ["PREFERREDPHONE","SMSBANKINGMOBILENUMBER"]
        )
        fin["Phone_3_Finacle"] = ""
        
        # simple fuzzy compare one-to-one by index
        matches, mismatches = [], []
        n = min(len(basis), len(fin))
        for i in range(n):
            b, f = basis.iloc[i], fin.iloc[i]
            name_score = fuzz.token_sort_ratio(b.Name, f.Name)
            email_score = fuzz.ratio(b.Email_Basis, f.Email_Finacle)
            dob_score = 0
            try:
                d1 = pd.to_datetime(b.DOB_Basis, errors="coerce")
                d2 = pd.to_datetime(f.DOB_Finacle, errors="coerce")
                diff = abs((d1-d2).days)
                dob_score = max(0,100-diff/30*100) if pd.notna(d1) and pd.notna(d2) else 0
            except:
                pass
            setb = {b.Phone_1_Basis,b.Phone_2_Basis,b.Phone_3_Basis} - {""}
            setf = {f.Phone_1_Finacle,f.Phone_2_Finacle,f.Phone_3_Finacle} - {""}
            phone_score = (len(setb&setf)/len(setb|setf)*100) if (setb|setf) else 0

            total = name_score*0.4 + email_score*0.3 + dob_score*0.2 + phone_score*0.1
            row = {
                "Name_Basis":b.Name, "Name_Finacle":f.Name,
                "Email_Basis":b.Email_Basis, "Email_Finacle":f.Email_Finacle,
                "DOB_Basis":b.DOB_Basis, "DOB_Finacle":f.DOB_Finacle,
                "Phone_Basis":", ".join(setb), "Phone_Finacle":", ".join(setf),
                "Score":round(total,2)
            }
            if total >= 70:
                matches.append(row)
            else:
                mismatches.append(row)

    st.success("✅ Comparison complete!")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("✅ Matches")
        st.dataframe(pd.DataFrame(matches))
        if matches:
            buf = BytesIO(); pd.DataFrame(matches).to_excel(buf,index=False); buf.seek(0)
            st.download_button("⬇ Download Matches", buf, "matches.xlsx")
    with c2:
        st.subheader("❌ Mismatches")
        st.dataframe(pd.DataFrame(mismatches))
        if mismatches:
            buf2 = BytesIO(); pd.DataFrame(mismatches).to_excel(buf2,index=False); buf2.seek(0)
            st.download_button("⬇ Download Mismatches", buf2, "mismatches.xlsx")
else:
    st.info("⬆ Please upload both files to begin.")
