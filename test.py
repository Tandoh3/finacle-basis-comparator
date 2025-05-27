# batch_fuzzy_match_app.py

import streamlit as st
import polars as pl
from rapidfuzz import fuzz
import tempfile
import math

st.title("⚡️ Batch Bio Data Mismatch Detector (1.8M+ rows)")

# File upload
finacle_file = st.file_uploader("Upload Finacle CSV", type="csv")
basis_file = st.file_uploader("Upload Basis CSV", type="csv")

# Matching helper
def normalize(val):
    return str(val).strip().lower() if val and val != "null" else ""

def combine_phones(row, cols):
    return " ".join([normalize(row.get(col, "")) for col in cols])

def compare(f_row, b_row):
    scores = []

    if f_row.get("name") and b_row.get("name"):
        scores.append(fuzz.token_sort_ratio(normalize(f_row["name"]), normalize(b_row["name"])))

    if f_row.get("dob") and b_row.get("dob"):
        scores.append(fuzz.ratio(normalize(f_row["dob"]), normalize(b_row["dob"])))

    if f_row.get("email") and b_row.get("email"):
        scores.append(fuzz.token_sort_ratio(normalize(f_row["email"]), normalize(b_row["email"])))

    f_phone = combine_phones(f_row, ["preferredphone", "smsbankingnumber"])
    b_phone = combine_phones(b_row, ["tel_num", "tel_num_2", "fax_num", "mob_num"])
    if f_phone and b_phone:
        scores.append(fuzz.partial_ratio(f_phone, b_phone))

    return sum(scores) / len(scores) if scores else 0

if finacle_file and basis_file:
    threshold = st.slider("Match Score Threshold", 0, 100, 85)
    batch_size = st.number_input("Batch Size", value=10000, step=1000)

    with tempfile.NamedTemporaryFile(delete=False) as f_tmp, tempfile.NamedTemporaryFile(delete=False) as b_tmp:
        f_tmp.write(finacle_file.read())
        b_tmp.write(basis_file.read())

    st.info("Reading CSVs with Polars...")
    finacle = pl.read_csv(f_tmp.name).unique(subset=["name", "dob", "email"])
    basis = pl.read_csv(b_tmp.name).unique(subset=["name", "dob", "email"])

    st.success(f"Loaded Finacle: {len(finacle)} rows, Basis: {len(basis)} rows.")

    f_records = finacle.to_dicts()
    b_records = basis.to_dicts()

    # Index basis by DOB
    dob_index = {}
    for b_row in b_records:
        dob = normalize(b_row.get("dob", ""))
        if dob:
            dob_index.setdefault(dob, []).append(b_row)

    mismatches = []
    total_matches = 0

    total_batches = math.ceil(len(f_records) / batch_size)

    for i in range(total_batches):
        st.info(f"Processing batch {i+1}/{total_batches}...")
        start = i * batch_size
        end = start + batch_size
        batch = f_records[start:end]

        for f_row in batch:
            f_dob = normalize(f_row.get("dob", ""))
            pool = dob_index.get(f_dob, b_records)

            best_score = 0
            best_match = None

            for b_row in pool:
                score = compare(f_row, b_row)
                if score > best_score:
                    best_score = score
                    best_match = b_row

            if best_score < threshold:
                mismatches.append({
                    "finacle_name": f_row.get("name", ""),
                    "finacle_dob": f_row.get("dob", ""),
                    "finacle_email": f_row.get("email", ""),
                    "finacle_phones": combine_phones(f_row, ["preferredphone", "smsbankingnumber"]),
                    "basis_name": best_match.get("name", "") if best_match else "",
                    "basis_email": best_match.get("email", "") if best_match else "",
                    "basis_phones": combine_phones(best_match, ["tel_num", "tel_num_2", "fax_num", "mob_num"]) if best_match else "",
                    "match_score": best_score
                })
            else:
                total_matches += 1

        st.success(f"✅ Batch {i+1} complete — Matches: {total_matches}, Mismatches so far: {len(mismatches)}")

    st.success(f"✅ Done! Total Matches: {total_matches}, Mismatches: {len(mismatches)}")

    if mismatches:
        import pandas as pd
        mismatch_df = pd.DataFrame(mismatches)
        st.dataframe(mismatch_df)

        csv = mismatch_df.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download Mismatches CSV", csv, "mismatches.csv", "text/csv")
    else:
        st.info("🎉 No mismatches found!")
