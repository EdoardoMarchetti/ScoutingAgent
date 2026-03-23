import streamlit as st

st.set_page_config(
    page_title="Scouting Agent",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Scouting Agent")
st.caption("Multipage dashboard for football analysis with Streamlit.")

st.markdown(
    """
Welcome to your multipage app.

Use the sidebar menu to navigate across pages.
"""
)

with st.sidebar:
    st.header("Navigation")
    st.info("Select a page from the top-left menu.")
