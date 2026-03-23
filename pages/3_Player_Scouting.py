import streamlit as st

from langgraph_flow.my_scouting_report import get_graph
from services.match_repository import (
    build_match_labels,
    build_player_labels,
    get_available_matches,
    get_players_for_match,
)

st.title("Player Scouting")
st.write("Player scouting page.")

st.markdown("### Match Selection")
try:
    available_matches = get_available_matches(limit=300)
except Exception as exc:
    st.error(f"Unable to load matches from BigQuery: {exc}")
    available_matches = []

match_labels = build_match_labels(available_matches)
selected_match_id = st.selectbox(
    "Available matches (from BigQuery)",
    options=list(match_labels.keys()),
    index=None,
    placeholder="Select one match",
    format_func=lambda match_id: match_labels[match_id],
)
if not available_matches:
    st.info("No matches available in BigQuery for now.")

if selected_match_id is not None:
    try:
        available_players = get_players_for_match(match_id=selected_match_id, limit=300)
    except Exception as exc:
        st.error(f"Unable to load players for the selected match: {exc}")
        available_players = []
else:
    available_players = []

player_labels = build_player_labels(available_players)
selected_player_id = st.selectbox(
    "Available players (from selected match)",
    options=list(player_labels.keys()),
    index=None,
    placeholder="Select one player",
    format_func=lambda player_id: player_labels[player_id],
    disabled=selected_match_id is None,
)
if selected_match_id is not None and not available_players:
    st.info("No players available for this match in BigQuery.")

st.markdown("### Report Generation")
report_language = st.selectbox(
    "Report language",
    options=["English", "Italian", "Spanish"],
    index=0,
)
user_notes = st.text_area(
    "User notes (optional)",
    placeholder="Add tactical context or specific focus points...",
)

can_generate = selected_match_id is not None and selected_player_id is not None
generate_clicked = st.button("Generate scouting report", disabled=not can_generate)
if not can_generate:
    st.caption("Select one match and one player to enable report generation.")

if generate_clicked:
    try:
        graph = get_graph()
        graph_input = {
            "messages": [],
            "match_id": int(selected_match_id),
            "player_id": int(selected_player_id),
            "report_language": report_language,
            "user_notes": user_notes.strip(),
        }
        step_order = ["match_header", "possession_comments", "player_stats_summary", "scout"]
        step_labels = {
            "match_header": "Loading match and player header",
            "possession_comments": "Loading possession comments",
            "player_stats_summary": "Generating player statistical summary",
            "scout": "Generating final scouting report",
        }
        completed_steps: set[str] = set()
        progress = st.progress(0, text="Starting report workflow...")
        result = graph_input

        with st.status("Running report steps...", expanded=True) as status:
            for update in graph.stream(graph_input, stream_mode="updates"):
                if not isinstance(update, dict):
                    continue
                for step_name, payload in update.items():
                    if step_name not in step_labels:
                        continue
                    if step_name not in completed_steps:
                        completed_steps.add(step_name)
                        status.write(f"Completed: {step_labels[step_name]}")
                    if isinstance(payload, dict):
                        result.update(payload)
                    completed_count = len(completed_steps)
                    progress_value = int((completed_count / len(step_order)) * 100)
                    progress_text = (
                        f"Completed {completed_count}/{len(step_order)} steps"
                    )
                    progress.progress(progress_value, text=progress_text)

            status.update(label="Report workflow completed", state="complete")

        progress.progress(100, text="All steps completed")

        match_context = {
            "match_label": str(result.get("match_label") or "").strip(),
            "match_date": str(result.get("match_date") or "").strip(),
            "competition_name": str(result.get("competition_name") or "").strip(),
            "player_name": str(result.get("player_name") or "").strip(),
            "team_name": str(result.get("team_name") or "").strip(),
            "possession_comments_count": len(result.get("possession_comments") or []),
        }

        st.markdown("### Match context from flow")
        st.json(match_context)

        messages = result.get("messages") or []
        report_text = ""
        if messages:
            report_text = getattr(messages[-1], "content", "") or ""
        report_text = str(report_text).strip()

        st.markdown("### Generated report")
        report_header = (
            f"**Match:** {match_context['match_label']}  \n"
            f"**Competition:** {match_context['competition_name']}  \n"
            f"**Date:** {match_context['match_date']}  \n"
            f"**Player:** {match_context['player_name']}  \n"
            f"**Team:** {match_context['team_name']}"
        )
        st.markdown(report_header)
        st.markdown("---")
        if report_text:
            st.write(report_text)
        else:
            st.warning("The report was generated but returned empty content.")

        stats_summary = str(result.get("statistical_summary") or "").strip()
        st.markdown("### Statistical summary")
        if stats_summary:
            st.write(stats_summary)
        else:
            st.info("No statistical summary available.")
    except Exception as exc:
        st.error(f"Report generation failed: {exc}")
