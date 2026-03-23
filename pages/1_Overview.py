import streamlit as st

st.title("Overview")
st.write("Overview page for the Scouting Agent project.")

col1, col2, col3 = st.columns(3)
col1.metric("Analyzed Matches", "0")
col2.metric("Teams", "0")
col3.metric("Tracked Players", "0")

st.markdown("Update this page with real KPIs from your pipeline.")
