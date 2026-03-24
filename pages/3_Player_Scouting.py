import base64
import json
import re
from pathlib import Path

import streamlit as st

from langgraph_flow.my_scouting_report import (
    VIZ_ONLY_PIPELINE_STEPS,
    get_graph,
    initial_scouting_state,
)
from services.match_repository import (
    build_player_labels,
    distinct_areas,
    distinct_competitions,
    distinct_seasons,
    get_available_matches_enriched,
    get_players_for_match,
    match_select_label,
    matches_after_filters,
)
from services.report_pdf import build_scouting_report_pdf


def _render_markdown_image(md_img: str) -> None:
    """Prefer st.image for base64 PNG (Streamlit markdown often skips data: URIs)."""
    m = re.match(r"^!\[\]\(data:image/png;base64,([^)]+)\)\s*$", md_img.strip())
    if m:
        try:
            st.image(base64.b64decode(m.group(1)), use_container_width=True)
            return
        except Exception:
            pass
    st.markdown(md_img, unsafe_allow_html=True)


def _render_entity_image(image_source: str, fallback_label: str) -> None:
    src = (image_source or "").strip()
    if not src:
        st.caption(fallback_label)
        st.info("No image")
        return
    m = re.match(r"^data:image/[^;]+;base64,(.+)$", src)
    if m:
        try:
            st.image(base64.b64decode(m.group(1)), use_container_width=True)
            return
        except Exception:
            st.caption(fallback_label)
            st.warning("Image unavailable")
            return
    st.image(src, use_container_width=True)


PHASE_VIZ: list[tuple[str, list[tuple[str, str]]]] = [
    (
        "Defensive phase",
        [
            ("duels_visualizations", "Duels"),
            ("recoveries_and_interceptions_visualization", "Recoveries & interceptions"),
        ],
    ),
    (
        "Build-up phase",
        [
            ("pass_sonar_visualization", "Touch density"),
            ("pass_start_network_visualization", "Pass start + network"),
            ("receiving_network_visualization", "Receiving + network"),
        ],
    ),
    (
        "Finalization phase",
        [
            ("shot_map_visualization", "Shot map"),
            ("crosses_map_visualization", "Crosses & key passes"),
        ],
    ),
]

STEP_LABELS = {
    "match_header": "Match & player header",
    "defensive_visualizations": "Defensive visualizations (duels, regains)",
    "build_up_visualizations": "Build-up visualizations (passes, touches)",
    "finalization_visualizations": "Finalization visualizations (shots, crosses)",
    "possession_comments": "Loading possession comments",
    "player_stats_summary": "Statistical summary (data agent)",
    "scout": "Final scouting report",
}

SESSION_RESULT_KEY = "player_scouting_last_result"
SESSION_CONTEXT_KEY = "player_scouting_last_context"


def _render_viz_block(block: dict, fallback_title: str) -> bool:
    """Render one visualization block. Returns True if anything rendered."""
    err = block.get("error")
    md_img = str(block.get("markdown_image") or "").strip()
    desc = str(block.get("description") or "").strip()
    caption = str(block.get("caption") or fallback_title).strip()
    passes_table = block.get("passes_table")
    has_passes_table = isinstance(passes_table, list)

    if err and not md_img and not desc and not has_passes_table:
        st.error(str(err))
        return True
    if not md_img and not desc and not has_passes_table:
        return False

    if caption and caption != fallback_title:
        st.caption(caption)
    if md_img:
        _render_markdown_image(md_img)
    if desc:
        st.markdown(desc)
    if has_passes_table:
        with st.expander("Passaggi — tabella eventi (stessi filtri del grafico)", expanded=False):
            if len(passes_table) == 0:
                st.caption(
                    "Nessuna riga: filtri attivi = recipient/passer id ≠ 0 e solo compagni "
                    "(team dominante su silver_match_event = team_id evento passaggio)."
                )
            else:
                st.dataframe(passes_table, use_container_width=True, hide_index=True)
    return True

st.title("Player Scouting")
st.write("Player scouting page.")

st.markdown("### Match Selection")
try:
    available_matches = get_available_matches_enriched(limit=800)
except Exception as exc:
    st.error(f"Unable to load matches from BigQuery: {exc}")
    available_matches = []

area_pairs = distinct_areas(available_matches)
area_ids = [a[0] for a in area_pairs]
area_labels = dict(area_pairs)

col_area, col_comp, col_season = st.columns(3)
with col_area:
    if not area_ids:
        st.selectbox("Area", options=["—"], disabled=True)
        selected_area_id = None
    else:
        selected_area_id = st.selectbox(
            "Area",
            options=area_ids,
            format_func=lambda i: area_labels[i],
            index=None,
            placeholder="Select area",
            key="ps_filter_area",
        )

with col_comp:
    comp_pairs = (
        distinct_competitions(available_matches, int(selected_area_id))
        if selected_area_id is not None
        else []
    )
    comp_ids = [c[0] for c in comp_pairs]
    comp_labels = dict(comp_pairs)
    if selected_area_id is None or not comp_ids:
        st.selectbox("Competition", options=["—"], disabled=True, key="ps_filter_comp_dis")
        selected_competition_id = None
    else:
        selected_competition_id = st.selectbox(
            "Competition",
            options=comp_ids,
            format_func=lambda i: comp_labels[i],
            index=None,
            placeholder="Select competition",
            key="ps_filter_comp",
        )

with col_season:
    season_pairs = (
        distinct_seasons(
            available_matches, int(selected_area_id), int(selected_competition_id)
        )
        if selected_area_id is not None and selected_competition_id is not None
        else []
    )
    season_ids = [s[0] for s in season_pairs]
    season_labels = dict(season_pairs)
    if (
        selected_area_id is None
        or selected_competition_id is None
        or not season_ids
    ):
        st.selectbox("Season", options=["—"], disabled=True, key="ps_filter_season_dis")
        selected_season_id = None
    else:
        selected_season_id = st.selectbox(
            "Season",
            options=season_ids,
            format_func=lambda i: season_labels[i],
            index=None,
            placeholder="Select season",
            key="ps_filter_season",
        )

match_rows = (
    matches_after_filters(
        available_matches,
        area_id=int(selected_area_id),
        competition_id=int(selected_competition_id),
        season_id=int(selected_season_id),
    )
    if (
        selected_area_id is not None
        and selected_competition_id is not None
        and selected_season_id is not None
    )
    else []
)
match_by_id = {int(r["match_id"]): r for r in match_rows}
match_id_options = list(match_by_id.keys())

if not available_matches:
    st.info("No matches available in BigQuery for now.")
elif (
    selected_area_id is not None
    and selected_competition_id is not None
    and selected_season_id is not None
    and not match_id_options
):
    st.warning("No matches for this area / competition / season.")

if match_id_options:
    selected_match_id = st.selectbox(
        "Match",
        options=match_id_options,
        index=None,
        placeholder="Select match",
        format_func=lambda mid: match_select_label(match_by_id[mid]),
        key="ps_filter_match",
    )
else:
    st.selectbox("Match", options=["—"], disabled=True, key="ps_filter_match_empty")
    selected_match_id = None

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
        graph_input = initial_scouting_state(
            match_id=int(selected_match_id),
            player_id=int(selected_player_id),
            report_language=report_language,
            user_notes=user_notes.strip(),
        )
        completed_steps: set[str] = set()
        progress = st.progress(0, text="Starting report workflow...")
        result = dict(graph_input)

        with st.status("Running report steps...", expanded=True) as status:
            for update in graph.stream(graph_input, stream_mode="updates"):
                if not isinstance(update, dict):
                    continue
                for step_name, payload in update.items():
                    label = STEP_LABELS.get(step_name, step_name.replace("_", " "))
                    if step_name not in completed_steps:
                        completed_steps.add(step_name)
                        status.write(f"Completed: {label}")
                    if isinstance(payload, dict):
                        result.update(payload)
                    n_done = len(completed_steps)
                    progress_value = min(
                        99,
                        int(100 * n_done / max(VIZ_ONLY_PIPELINE_STEPS, 1)),
                    )
                    progress.progress(
                        progress_value,
                        text=f"Steps completed: {n_done} (last: {label})",
                    )

            status.update(label="Report workflow completed", state="complete")

        progress.progress(100, text="All steps completed")

        match_context = {
            "match_label": str(result.get("match_label") or "").strip(),
            "match_date": str(result.get("match_date") or "").strip(),
            "competition_name": str(result.get("competition_name") or "").strip(),
            "player_name": str(result.get("player_name") or "").strip(),
            "team_name": str(result.get("team_name") or "").strip(),
            "report_language": str(result.get("report_language") or report_language).strip(),
            "player_image_data_url": str(result.get("player_image_data_url") or "").strip(),
            "team_image_data_url": str(result.get("team_image_data_url") or "").strip(),
            "possession_comments_count": len(result.get("possession_comments") or []),
        }
        st.session_state[SESSION_RESULT_KEY] = result
        st.session_state[SESSION_CONTEXT_KEY] = match_context
    except Exception as exc:
        st.error(f"Report generation failed: {exc}")

if SESSION_RESULT_KEY in st.session_state and SESSION_CONTEXT_KEY in st.session_state:
    result = st.session_state[SESSION_RESULT_KEY]
    match_context = st.session_state[SESSION_CONTEXT_KEY]

    col_pdf, col_json = st.columns(2)
    
    # PDF Export
    with col_pdf:
        try:
            messages = result.get("messages") or []
            report_text = str(getattr(messages[-1], "content", "") or "").strip() if messages else ""
            stats_summary = str(result.get("statistical_summary") or "").strip()
            pdf_bytes = build_scouting_report_pdf(
                logo_path="images/sport_data_campus.png",
                match_context=match_context,
                report_text=report_text,
                statistical_summary=stats_summary,
                phase_viz=PHASE_VIZ,
                result=result,
            )
            pdf_name = (
                f"scouting-report-{match_context.get('player_name','player')}"
                f"-{match_context.get('match_label','match')}.pdf"
            ).replace(" ", "_").replace("/", "-")
            
            # Save JSON alongside PDF for RAG
            json_name = pdf_name.replace(".pdf", ".json")
            reports_dir = Path("player_reports")
            reports_dir.mkdir(exist_ok=True)
            
            # Build JSON export (full state minus binary/heavy data)
            json_export = {
                "match_id": st.session_state.get("ps_selected_match_id"),
                "player_id": st.session_state.get("ps_selected_player_id"),
                "match_header": match_context,
                "scout": {"report_text": report_text},
                "player_stats_summary": {"summary": stats_summary},
                "possession_comments": {"comments": str(result.get("possession_comments") or "")},
                "duels_visualizations": {
                    "description": str(result.get("duels_visualizations", {}).get("description") or ""),
                    "caption": str(result.get("duels_visualizations", {}).get("caption") or ""),
                },
                "recoveries_and_interceptions_visualization": {
                    "description": str(result.get("recoveries_and_interceptions_visualization", {}).get("description") or ""),
                    "caption": str(result.get("recoveries_and_interceptions_visualization", {}).get("caption") or ""),
                },
                "pass_sonar_visualization": {
                    "description": str(result.get("pass_sonar_visualization", {}).get("description") or ""),
                    "caption": str(result.get("pass_sonar_visualization", {}).get("caption") or ""),
                },
                "pass_start_network_visualization": {
                    "description": str(result.get("pass_start_network_visualization", {}).get("description") or ""),
                    "caption": str(result.get("pass_start_network_visualization", {}).get("caption") or ""),
                },
                "receiving_network_visualization": {
                    "description": str(result.get("receiving_network_visualization", {}).get("description") or ""),
                    "caption": str(result.get("receiving_network_visualization", {}).get("caption") or ""),
                },
                "shot_map_visualization": {
                    "description": str(result.get("shot_map_visualization", {}).get("description") or ""),
                    "caption": str(result.get("shot_map_visualization", {}).get("caption") or ""),
                },
                "crosses_map_visualization": {
                    "description": str(result.get("crosses_map_visualization", {}).get("description") or ""),
                    "caption": str(result.get("crosses_map_visualization", {}).get("caption") or ""),
                },
            }
            
            # Write JSON to disk
            json_path = reports_dir / json_name
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(json_export, f, indent=2, ensure_ascii=False)
            
            st.download_button(
                "📄 Export PDF",
                data=pdf_bytes,
                file_name=pdf_name,
                mime="application/pdf",
                use_container_width=True,
            )
            st.caption(f"✅ JSON saved to `{json_path}` for RAG")
        except Exception as exc:
            st.warning(f"PDF export unavailable: {exc}")
    
    # JSON Download Button
    with col_json:
        try:
            json_export_data = json.dumps(json_export, indent=2, ensure_ascii=False)
            st.download_button(
                "📊 Download JSON",
                data=json_export_data,
                file_name=json_name,
                mime="application/json",
                use_container_width=True,
            )
            st.caption("For RAG and analysis")
        except Exception:
            pass

    try:
        st.markdown("### Match context from flow")
        ctx_col, player_col, team_col = st.columns([0.6, 0.2, 0.2])
        with ctx_col:
            st.markdown(
                f"**Match:** {match_context['match_label']}  \n"
                f"**Competition:** {match_context['competition_name']}  \n"
                f"**Date:** {match_context['match_date']}  \n"
                f"**Player:** {match_context['player_name']}  \n"
                f"**Team:** {match_context['team_name']}"
            )
        with player_col:
            _render_entity_image(match_context["player_image_data_url"], "Player")
        with team_col:
            _render_entity_image(match_context["team_image_data_url"], "Team")

        messages = result.get("messages") or []
        report_text = ""
        if messages:
            report_text = getattr(messages[-1], "content", "") or ""
        report_text = str(report_text).strip()

        st.markdown("### Report scout")
        if report_text:
            st.write(report_text)
        else:
            st.info(
                "No final report text in this run (expected when using the **viz-only** graph). "
                "Switch to `get_graph_full()` in code for the full scout node."
            )

        stats_summary = str(result.get("statistical_summary") or "").strip()
        st.markdown("### Report statistico")
        if stats_summary:
            st.write(stats_summary)
        else:
            st.info(
                "No statistical summary (data agent runs only in the **full** graph)."
            )

        st.markdown("### Visualizzazioni")
        any_viz = False
        for phase_name, viz_items in PHASE_VIZ:
            st.markdown(f"#### {phase_name}")
            cols = st.columns(len(viz_items))
            phase_has_any = False
            for col, (state_key, title) in zip(cols, viz_items):
                with col:
                    st.markdown(f"**{title}**")
                    block = result.get(state_key)
                    if isinstance(block, dict):
                        rendered = _render_viz_block(block, title)
                        phase_has_any = phase_has_any or rendered
                        any_viz = any_viz or rendered
                    else:
                        st.info("No data.")
            if phase_has_any:
                st.divider()
        if not any_viz:
            st.info("No chart payloads in state (empty match/player or pipeline without viz nodes).")
    except Exception as exc:
        st.error(f"Rendering failed: {exc}")
