"""RAG page — Query across player scouting reports with chatbot interface."""

from __future__ import annotations

import json
from pathlib import Path

import streamlit as st

from services.llm_factory import create_graph_llm
from services.report_rag_service import (
    compute_reports_hash,
    create_vector_store_faiss,
    load_reports_from_directory,
    load_vector_store,
    save_vector_store,
)

# === Page config ===
st.set_page_config(layout="wide")
st.title("🔍 RAG — Query player reports")
st.caption("Ask questions across multiple player scouting reports using AI-powered semantic search.")

# === Session state ===
if "rag_vectorstore" not in st.session_state:
    st.session_state.rag_vectorstore = None
if "rag_reports_hash" not in st.session_state:
    st.session_state.rag_reports_hash = ""
if "rag_messages" not in st.session_state:
    st.session_state.rag_messages = []

# === Sidebar: Configuration ===
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Directory configuration
    reports_dir_input = st.text_input(
        "Reports directory",
        value="player_reports",
        help="Directory containing JSON scouting reports",
        key="rag_reports_dir",
    )
    
    reports_dir = Path(reports_dir_input)
    
    # Check directory
    if not reports_dir.exists():
        st.error(f"Directory `{reports_dir}` not found")
        st.info(
            "**How to generate reports:**\n\n"
            "1. Go to **Player Scouting** page\n"
            "2. Generate a report\n"
            "3. Click **Export PDF**\n"
            "4. JSON files auto-saved for RAG"
        )
        st.stop()
    
    # Count reports
    json_files = list(reports_dir.glob("*.json"))
    num_reports = len(json_files)
    
    if num_reports == 0:
        st.warning(f"No JSON reports in `{reports_dir}`")
        st.stop()
    
    st.success(f"📊 **{num_reports}** report(s) found")
    
    st.divider()
    
    # Retrieval settings
    st.subheader("🔎 Search settings")
    
    k_results = st.slider(
        "Documents to retrieve",
        min_value=1,
        max_value=20,
        value=5,
        key="rag_k",
    )
    
    score_threshold = st.slider(
        "Similarity threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.5,
        step=0.05,
        key="rag_score_threshold",
        help="Lower = more lenient, Higher = stricter",
    )
    
    st.divider()
    
    # Index management
    st.subheader("📚 Vector index")
    
    index_dir = reports_dir / ".vector_index"
    index_dir.mkdir(exist_ok=True)
    
    current_hash = compute_reports_hash(reports_dir)
    needs_reindex = (
        st.session_state.rag_reports_hash != current_hash
        or st.session_state.rag_vectorstore is None
    )
    
    if needs_reindex:
        index_status = "⚠️ Needs reindex"
        st.warning(index_status)
    else:
        index_status = "✅ Up to date"
        st.success(index_status)
    
    if st.button("🔄 Reindex now", use_container_width=True):
        with st.spinner("Indexing reports..."):
            try:
                documents = load_reports_from_directory(reports_dir)
                
                if not documents:
                    st.error("No valid documents loaded")
                    st.stop()
                
                vectorstore = create_vector_store_faiss(documents)
                save_vector_store(vectorstore, index_dir)
                
                st.session_state.rag_vectorstore = vectorstore
                st.session_state.rag_reports_hash = current_hash
                
                st.success(f"✅ Indexed {len(documents)} report(s)")
                st.rerun()
            
            except Exception as e:
                st.error(f"Indexing failed: {str(e)}")
    
    # Auto-load index if needed
    if st.session_state.rag_vectorstore is None and not needs_reindex:
        with st.spinner("Loading index..."):
            try:
                vectorstore = load_vector_store(index_dir)
                st.session_state.rag_vectorstore = vectorstore
            except Exception:
                pass
    
    st.divider()
    
    if st.button("🗑️ Clear chat", use_container_width=True):
        st.session_state.rag_messages = []
        st.rerun()

# === Main chat interface ===

# Check if index is ready
if st.session_state.rag_vectorstore is None:
    st.info("👈 Click **Reindex now** in the sidebar to build the vector index first.")
    st.stop()

# === Indexed matches display ===
st.subheader("📋 Indexed matches")

# Load all reports to extract match information
indexed_reports = []
for json_file in reports_dir.glob("*.json"):
    try:
        with open(json_file, encoding="utf-8") as f:
            report_data = json.load(f)
        
        match_header = report_data.get("match_header", {})
        indexed_reports.append({
            "player_name": match_header.get("player_name", "Unknown"),
            "team_name": match_header.get("team_name", "Unknown"),
            "match_label": match_header.get("match_label", "Unknown"),
            "competition_name": match_header.get("competition_name", "Unknown"),
            "match_date": match_header.get("match_date", "Unknown"),
        })
    except Exception:
        continue

if indexed_reports:
    # Group by match
    matches_by_label = {}
    for report in indexed_reports:
        match_key = f"{report['match_label']} ({report['match_date']})"
        if match_key not in matches_by_label:
            matches_by_label[match_key] = {
                "match_label": report["match_label"],
                "match_date": report["match_date"],
                "competition": report["competition_name"],
                "players": []
            }
        matches_by_label[match_key]["players"].append({
            "name": report["player_name"],
            "team": report["team_name"]
        })
    
    # Display in columns
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.caption(f"**{len(matches_by_label)}** unique match(es) with **{len(indexed_reports)}** player report(s)")
    
    with col2:
        show_details = st.toggle("Show details", value=False, key="rag_show_match_details")
    
    if show_details:
        for match_key, match_info in sorted(matches_by_label.items()):
            with st.expander(f"⚽ {match_info['match_label']}", expanded=False):
                st.caption(f"📅 **Date:** {match_info['match_date']}")
                st.caption(f"🏆 **Competition:** {match_info['competition']}")
                st.caption(f"👥 **Players ({len(match_info['players'])}):**")
                for player in match_info["players"]:
                    st.markdown(f"- {player['name']} ({player['team']})")
    else:
        # Compact display
        match_labels = [f"⚽ {info['match_label']}" for info in matches_by_label.values()]
        st.caption(" • ".join(match_labels[:5]))
        if len(match_labels) > 5:
            st.caption(f"... and {len(match_labels) - 5} more")
else:
    st.warning("No indexed reports found")

st.divider()

# Display chat messages
for message in st.session_state.rag_messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Show sources for assistant messages
        if message["role"] == "assistant" and "sources" in message:
            with st.expander("📄 Sources", expanded=False):
                for idx, source in enumerate(message["sources"]):
                    st.caption(
                        f"**{idx + 1}.** {source['player_name']} — "
                        f"{source['match_label']} ({source['match_date']})"
                    )

# Chat input
if prompt := st.chat_input("Ask a question about the reports...", key="rag_chat_input"):
    # Add user message
    st.session_state.rag_messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Generate response
    with st.chat_message("assistant"):
        with st.spinner("Searching reports..."):
            vectorstore = st.session_state.rag_vectorstore
            
            try:
                # Retrieve relevant documents
                retriever = vectorstore.as_retriever(
                    search_type="similarity_score_threshold",
                    search_kwargs={
                        "k": k_results,
                        "score_threshold": score_threshold,
                    },
                )
                
                relevant_docs = retriever.invoke(prompt)
                
                if not relevant_docs:
                    response_text = (
                        "I couldn't find relevant reports matching your query. "
                        "Try lowering the similarity threshold in the sidebar or rephrasing your question."
                    )
                    st.warning(response_text)
                    
                    st.session_state.rag_messages.append({
                        "role": "assistant",
                        "content": response_text,
                    })
                    st.stop()
                
                # Build context
                context_parts = []
                sources = []
                
                for idx, doc in enumerate(relevant_docs):
                    meta = doc.metadata
                    context_parts.append(
                        f"--- Report {idx + 1}: {meta.get('player_name', 'Unknown')} "
                        f"in {meta.get('match_label', 'Unknown')} ---\n"
                        f"{doc.page_content}\n"
                    )
                    sources.append({
                        "player_name": meta.get("player_name", "Unknown"),
                        "match_label": meta.get("match_label", "Unknown"),
                        "match_date": meta.get("match_date", "Unknown"),
                    })
                
                context = "\n\n".join(context_parts)
                
                # Generate answer with streaming
                llm = create_graph_llm()
                
                llm_prompt = f"""You are a football scouting analyst. Answer the user's question based on the provided player scouting reports.

Retrieved Reports:
{context}

User Question: {prompt}

Instructions:
- Provide a comprehensive answer citing specific reports and players when relevant
- Compare performances across different matches if the question asks for it
- If the reports don't contain enough information to answer fully, acknowledge this
- Use clear, professional language
- Structure your answer with headings or bullet points when appropriate

Answer:"""

                response = llm.invoke(llm_prompt)
                answer = (getattr(response, "content", "") or "").strip()
                
                if not answer:
                    answer = "I couldn't generate an answer. Please try rephrasing your question."
                
                # Display answer
                st.markdown(answer)
                
                # Show sources
                with st.expander(f"📄 Sources ({len(sources)} reports)", expanded=False):
                    for idx, source in enumerate(sources):
                        st.caption(
                            f"**{idx + 1}.** {source['player_name']} — "
                            f"{source['match_label']} ({source['match_date']})"
                        )
                
                # Add to chat history
                st.session_state.rag_messages.append({
                    "role": "assistant",
                    "content": answer,
                    "sources": sources,
                })
            
            except Exception as e:
                error_msg = f"❌ Error: {str(e)}"
                st.error(error_msg)
                st.session_state.rag_messages.append({
                    "role": "assistant",
                    "content": error_msg,
                })

# === Footer ===
if not st.session_state.rag_messages:
    st.markdown("---")
    st.markdown("### 💡 Example questions")
    st.markdown("""
    - *"How did Lamine Yamal perform in attacking phases across his matches?"*
    - *"Compare defensive actions between Player A and Player B"*
    - *"What were the key passing patterns in the last match?"*
    - *"Show me all high-intensity pressing situations"*
    - *"Summarize Player X's performances over multiple games"*
    """)
