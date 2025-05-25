import streamlit as st
import polars as pl
from rapidfuzz import fuzz
import numpy as np
import pandas as pd  # For final result conversion

def efficient_find_fuzzy_matches(basis_df: pl.DataFrame, finacle_df: pl.DataFrame, batch_size=5000):
    basis_df = normalize(preprocess_basis(basis_df))
    finacle_df = normalize(preprocess_finacle(finacle_df))

    matches = []
    mismatches_basis = []
    matched_finacle_indices = set()

    basis_n_batches = (len(basis_df) + batch_size - 1) // batch_size
    for i in range(basis_n_batches):
        basis_batch = basis_df.slice(i * batch_size, batch_size)
        st.info(f"Processing BASIS batch {i + 1} of {basis_n_batches}")

        finacle_n_batches = (len(finacle_df) + batch_size - 1) // batch_size
        for j in range(finacle_n_batches):
            finacle_batch = finacle_df.slice(j * batch_size, batch_size)
            st.info(f"  Comparing with FINACLE batch {j + 1} of {finacle_n_batches}")

            # Convert batches to Pandas for easier (though less efficient) rapidfuzz application
            # For optimal performance, we'd ideally find a vectorized rapidfuzz solution within Polars
            basis_pd = basis_batch.to_pandas()
            finacle_pd = finacle_batch.to_pandas()

            for b_idx, b_row in basis_pd.iterrows():
                if b_idx in [match['Basis_Index'] for match in matches if 'Basis_Index' in match]: # Avoid re-matching
                    continue

                best_score = 0
                best_match = None
                best_f_idx = None

                for f_idx, f_row in finacle_pd.iterrows():
                    if f_idx in matched_finacle_indices:
                        continue

                    name_score = fuzz.ratio(b_row["Name"], f_row["Name"])
                    if name_score >= 85:
                        email_score = fuzz.ratio(b_row["Email_Basis"], f_row["Email_Finacle"])
                        phone_match, phone_score = fuzzy_match_phones([b_row[f"Phone_{k}_Basis"] for k in range(1, 4)],
                                                                     [f_row[f"Phone_{k}_Finacle"] for k in range(1, 4)], 85)
                        dob_match, dob_score = fuzzy_match_dates(b_row["Date_of_Birth_Basis"], f_row["Date_of_Birth_Finacle"], 30)

                        total_score = name_score * 0.4 + email_score * 0.3 + dob_score * 0.2 + phone_score * 0.1

                        if total_score > best_score:
                            best_score = total_score
                            best_match = f_row
                            best_f_idx = f_idx

                if best_match is not None:
                    matches.append({
                        "Basis_Name": b_row["Name"],
                        "Finacle_Name": best_match["Name"],
                        "Email_Basis": b_row["Email_Basis"],
                        "Email_Finacle": best_match["Email_Finacle"],
                        "DOB_Basis": b_row["Date_of_Birth_Basis"],
                        "DOB_Finacle": best_match["Date_of_Birth_Finacle"],
                        "Phones_Basis": [b_row[f"Phone_{k}_Basis"] for k in range(1, 4)],
                        "Phones_Finacle": [best_match[f"Phone_{k}_Finacle"] for k in range(1, 4)],
                        "Score": round(best_score, 2),
                        "Basis_Index": b_idx,
                        "Finacle_Index": best_f_idx
                    })
                    matched_finacle_indices.add(best_f_idx)

        st.progress((i + 1) / basis_n_batches)

    # Identify mismatches (records not in matches)
    matched_basis_indices = set(match['Basis_Index'] for match in matches if 'Basis_Index' in match)
    mismatches_basis_df = basis_df.filter(~pl.Series(np.arange(len(basis_df))).is_in(list(matched_basis_indices))).to_pandas()

    matched_finacle_indices_final = set(match['Finacle_Index'] for match in matches if 'Finacle_Index' in match)
    mismatches_finacle_df = finacle_df.filter(~pl.Series(np.arange(len(finacle_df))).is_in(list(matched_finacle_indices_final))).to_pandas()

    return pd.DataFrame(matches), mismatches_basis_df, mismatches_finacle_df

# === Main Logic ===
if basis_file and finacle_file:
    batch_size = st.slider("Batch Size for Processing", min_value=1000, max_value=10000, value=5000, step=1000)
    with st.spinner("🔄 Matching records in batches, please wait..."):
        basis_df = read_file(basis_file, is_basis=True)
        finacle_df = read_file(finacle_file, is_basis=False)
        matches_df, mismatches_basis_df, mismatches_finacle_df = efficient_find_fuzzy_matches(basis_df, finacle_df, batch_size=batch_size)

   # === 3. Main Logic ===
if basis_file and finacle_file:
    with st.spinner("🔄 Matching records, please wait..."):
        basis_df = read_file(basis_file, is_basis=True)
        finacle_df = read_file(finacle_file, is_basis=False)
        matches_df, mismatches_df = find_fuzzy_matches(basis_df, finacle_df)

    st.success("✅ Matching complete!")

    st.subheader("✅ Matches")
    st.dataframe(matches_df)

    st.subheader("❌ Mismatches")
    st.dataframe(mismatches_df)

    if not matches_df.empty:
        excel_data = convert_df(matches_df)
        st.download_button(
            label="📥 Download Matches as Excel",
            data=excel_data,
            file_name="matches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    if not mismatches_df.empty:
        excel_data = convert_df(mismatches_df)
        st.download_button(
            label="📥 Download Mismatches as Excel",
            data=excel_data,
            file_name="mismatches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
else:
    st.info("Please upload both BASIS and FINACLE files to start matching.")