import streamlit as st

st.set_page_config(
    page_title="Scouting Agent",
    page_icon="⚽",
    layout="centered",
    initial_sidebar_state="expanded",
)

pages = [
    st.Page("pages/4_Chatbot.py", title="Chatbot", icon="💬", default=True),
    st.Page("pages/3_Player_Scouting.py", title="Player scouting", icon="⚽"),
    st.Page("pages/5_Player_Compare.py", title="Player comparison", icon="📊"),
    st.Page("pages/6_Report_RAG.py", title="Report RAG", icon="🔍"),
]

pg = st.navigation(pages)
pg.run()
