# bio_mismatch_app.py
import streamlit as st
import pandas as pd
from rapidfuzz import fuzz
import tempfile

st.title("Bio Data Mismatch Detector")

st.markdown("""
Upload CSV files from **Finacle** and **Basis**, and the app will compare:
- `name`
- `dob`
- `email`
- All phone fields: `preferredphone`, `smsbankingnumber` vs `tel_num`, `tel_num_2`, `fax_num`, `mob_num`
""")

# File upload
finacle_file = st.file_uploader("Upload Finacle CSV", type="csv")
basis_file = st.file_uploader("Upload Basis CSV", type="csv")

# Matching function
def normalize(s):
    return str(s).lower().strip() if pd.notnull(s) else ""

def combine_phones(row, cols):
    return " ".join(normalize(row[col]) for col in cols if col in row)

def compare_records(f_row, b_row):
    name_score = fuzz.token_sort_ratio(normalize(f_row['name']), normalize(b_row['name']))
    dob_score = fuzz.ratio(normalize(f_row['dob']), normalize(b_row['dob']))
    email_score = fuzz.token_sort_ratio(normalize(f_row['email']), normalize(b_row['email']))

    f_phone = combine_phones(f_row, ['preferredphone', 'smsbankingnumber'])
    b_phone = combine_phones(b_row, ['tel_num', 'tel_num_2', 'fax_num', 'mob_num'])
    phone_score = fuzz.partial_ratio(f_phone, b_phone)

    avg_score = (name_score + dob_score + email_score + phone_score) / 4
    return avg_score

# Main logic
if finacle_file and basis_file:
    with st.spinner("Reading files..."):
        finacle = pd.read_csv(finacle_file)
        basis = pd.read_csv(basis_file)

    st.success("Files loaded successfully!")

    st.markdown("### Start Matching")

    threshold = st.slider("Match Score Threshold", 0, 100, 85)

    if st.button("Find Mismatches"):
        mismatches = []

        # Preprocess and block on DOB to reduce load
        basis_grouped = basis.groupby(basis['dob'])

        for _, f_row in finacle.iterrows():
            f_dob = normalize(f_row['dob'])
            candidate_pool = basis_grouped.get_group(f_dob) if f_dob in basis_grouped.groups else basis

            best_score = 0
            best_match = None

            for _, b_row in candidate_pool.iterrows():
                score = compare_records(f_row, b_row)
                if score > best_score:
                    best_score = score
                    best_match = b_row

            if best_score < threshold:
                mismatches.append({
                    'name': f_row['name'],
                    'dob': f_row['dob'],
                    'email': f_row['email'],
                    'finacle_phone': combine_phones(f_row, ['preferredphone', 'smsbankingnumber']),
                    'basis_match_name': best_match['name'] if best_match is not None else "",
                    'basis_match_email': best_match['email'] if best_match is not None else "",
                    'basis_phone': combine_phones(best_match, ['tel_num', 'tel_num_2', 'fax_num', 'mob_num']) if best_match is not None else "",
                    'match_score': best_score
                })

        mismatches_df = pd.DataFrame(mismatches)

        st.write("### Mismatched Records", mismatches_df)
        csv = mismatches_df.to_csv(index=False).encode('utf-8')
        st.download_button("Download Mismatches as CSV", csv, "bio_mismatches.csv", "text/csv")
