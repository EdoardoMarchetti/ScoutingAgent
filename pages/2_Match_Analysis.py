import streamlit as st

st.title("Match Analysis")
st.write("Match analysis: filters, metrics, and visualizations.")

with st.form("match_filters"):
    st.subheader("Filters")
    match_id = st.text_input("Match ID")
    team = st.text_input("Team")
    submitted = st.form_submit_button("Apply")

if submitted:
    st.success(f"Filters applied - match_id: {match_id or 'N/A'}, team: {team or 'N/A'}")
    st.info("Connect your queries to analysis datasets/processes here.")
