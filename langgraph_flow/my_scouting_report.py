from __future__ import annotations

from typing import Any

from langchain_core.messages import AIMessage
from langgraph.graph import END, START, StateGraph

from langgraph_flow.scouting_state import ScoutingReportState
from langgraph_flow.viz_nodes import (
    node_build_up_visualizations,
    node_defensive_visualizations,
    node_finalization_visualizations,
)
from services.data_agent_client import query_data_agent
from services.llm_factory import create_graph_llm
from services.prompt_loader import load_prompt, render_prompt
from services.scouting_report_repository import (
    get_match_player_header,
    load_possession_comments,
)


def _enforce_output_language(prompt_text: str, language: str) -> str:
    lang = (language or "English").strip()
    return (
        f"IMPORTANT OUTPUT LANGUAGE RULE: Write the final answer only in {lang}. "
        f"Do not mix languages.\n\n{prompt_text}"
    )


def _build_viz_descriptions_for_report(state: ScoutingReportState) -> str:
    sections: list[str] = []
    viz_keys: list[tuple[str, str]] = [
        ("duels_visualizations", "Duels"),
        ("recoveries_and_interceptions_visualization", "Recoveries & Interceptions"),
        ("pass_sonar_visualization", "Touch density (build-up zones)"),
        ("pass_start_network_visualization", "Pass Start Heatmap with Network"),
        ("receiving_network_visualization", "Receiving Heatmap with Network"),
        ("shot_map_visualization", "Shot Map"),
        ("crosses_map_visualization", "Crosses & Key Passes Map"),
    ]
    for key, title in viz_keys:
        payload = state.get(key)
        if isinstance(payload, dict) and "error" not in payload:
            desc = (payload.get("description") or "").strip()
            if desc:
                sections.append(f"### {title}\n{desc}")

    possession_comments = state.get("possession_comments") or []
    if possession_comments:
        parts: list[str] = []
        for c in possession_comments[:100]:
            ts = c.get("temporal_moment_json") or c.get("ts") or ""
            comment = c.get("comment") or c.get("text") or c.get("description") or str(c)
            parts.append(f"- [{ts}] {comment}")
        sections.append("### Possession comments (timestamped)\n" + "\n".join(parts))

    return "\n\n".join(sections) if sections else "(No visualization or possession data available.)"


def _phase_overview_from_viz_descriptions(
    llm: Any,
    *,
    phase_name: str,
    player_name: str,
    descriptions: list[str],
    report_language: str,
) -> str:
    clean = [d.strip() for d in descriptions if (d or "").strip()]
    if not clean:
        return "_No phase overview available._"
    prompt = (
        "You are an expert football scout.\n"
        f"Player: {player_name}\n"
        f"Phase: {phase_name}\n\n"
        "Merge the following chart captions into ONE compact phase overview.\n"
        "Output rules:\n"
        "- One paragraph only, max 4 sentences.\n"
        "- Use zones/channels football language; avoid raw coordinates and exact numeric values.\n"
        "- Prefer qualitative intensity wording (isolated / occasional / recurrent / frequent).\n"
        "- Do not invent events.\n\n"
        "Captions:\n"
        + "\n\n".join(f"- {d}" for d in clean)
    )
    prompt = _enforce_output_language(prompt, report_language)
    try:
        resp = llm.invoke(prompt)
        return (getattr(resp, "content", "") or "").strip() or "_No phase overview available._"
    except Exception:
        return "_No phase overview available._"


def _build_rag_context(state: ScoutingReportState) -> str:
    viz_block = _build_viz_descriptions_for_report(state)
    defensive_phase = str(state.get("defensive_phase_overview") or "").strip()
    buildup_phase = str(state.get("buildup_phase_overview") or "").strip()
    finalization_phase = str(state.get("finalization_phase_overview") or "").strip()
    phase_block = (
        "## Phase overviews from visualizations\n\n"
        f"### Defensive\n{defensive_phase or '_No phase overview available._'}\n\n"
        f"### Build-up\n{buildup_phase or '_No phase overview available._'}\n\n"
        f"### Finalization\n{finalization_phase or '_No phase overview available._'}\n\n"
    )
    return phase_block + "## Detailed visualization & possession context\n\n" + viz_block


def node_match_header(state: ScoutingReportState):
    header = get_match_player_header(state["match_id"], state["player_id"])
    return {
        "match_label": header["match_label"],
        "match_date": header["match_date"],
        "competition_name": header["competition_name"],
        "player_name": header["player_name"],
        "team_name": header["team_name"],
        "player_image_data_url": header.get("player_image_data_url"),
        "team_image_data_url": header.get("team_image_data_url"),
    }


def node_possession_comments(state: ScoutingReportState):
    comments = load_possession_comments(state["match_id"], state["player_id"])
    return {"possession_comments": comments}


def node_player_stats_summary(state: ScoutingReportState):
    prompt_data = load_prompt("player_stats_summary", version="v1")
    template = str(prompt_data.get("prompt") or "").strip()
    if not template:
        return {"statistical_summary": "(Statistical summary prompt not found.)"}

    prompt = render_prompt(
        template,
        {
            "match_id": str(state["match_id"]),
            "player_id": str(state["player_id"]),
            "player_name": str(state.get("player_name") or f"Player {state['player_id']}"),
            "team_name": str(state.get("team_name") or ""),
            "user_query": str(state.get("user_notes") or "No specific focus."),
        },
    )
    prompt = _enforce_output_language(prompt, state.get("report_language", "English"))
    try:
        result = query_data_agent(user_message=prompt, use_conversation_context=False)
        answer = (result.get("answer_text") or "").strip() or "(No response from data agent.)"
        if result.get("error"):
            answer = f"{answer}\n\n[Data agent note: {result['error']}]"
        return {"statistical_summary": answer}
    except Exception as exc:  # pragma: no cover - defensive
        return {"statistical_summary": f"(Error fetching statistical summary: {exc})"}


def node_scout(state: ScoutingReportState):
    llm = create_graph_llm()
    report_language = state.get("report_language", "English")

    defensive_descs = []
    for k in ("duels_visualizations", "recoveries_and_interceptions_visualization"):
        p = state.get(k)
        if isinstance(p, dict) and not p.get("error"):
            d = (p.get("description") or "").strip()
            if d:
                defensive_descs.append(d)

    buildup_descs = []
    for k in ("pass_sonar_visualization", "pass_start_network_visualization", "receiving_network_visualization"):
        p = state.get(k)
        if isinstance(p, dict) and not p.get("error"):
            d = (p.get("description") or "").strip()
            if d:
                buildup_descs.append(d)

    finalization_descs = []
    for k in ("shot_map_visualization", "crosses_map_visualization"):
        p = state.get(k)
        if isinstance(p, dict) and not p.get("error"):
            d = (p.get("description") or "").strip()
            if d:
                finalization_descs.append(d)

    player_name = str(state.get("player_name") or "the player")
    defensive_phase = _phase_overview_from_viz_descriptions(
        llm,
        phase_name="Defensive",
        player_name=player_name,
        descriptions=defensive_descs,
        report_language=report_language,
    )
    buildup_phase = _phase_overview_from_viz_descriptions(
        llm,
        phase_name="Build-up",
        player_name=player_name,
        descriptions=buildup_descs,
        report_language=report_language,
    )
    finalization_phase = _phase_overview_from_viz_descriptions(
        llm,
        phase_name="Finalization",
        player_name=player_name,
        descriptions=finalization_descs,
        report_language=report_language,
    )

    prompt_data = load_prompt("scouting_report", version="v1")
    prompt_template = str(prompt_data.get("prompt") or "").strip()
    if not prompt_template:
        report_text = "Error: scouting_report_v1.yaml prompt is empty."
    else:
        rag_context = _build_rag_context(state)
        statistical_summary = (state.get("statistical_summary") or "").strip() or "Not available yet."
        prompt = render_prompt(
            prompt_template,
            {
                "team_name": str(state.get("team_name") or "Unknown"),
                "statistical_summary": statistical_summary,
                "user_notes": str(state.get("user_notes") or "(No user notes provided.)"),
                "rag_context": rag_context,
            },
        )
        prompt = _enforce_output_language(prompt, report_language)
        response = llm.invoke(prompt)
        report_text = (getattr(response, "content", "") or "").strip()

    return {
        "messages": [AIMessage(content=report_text)],
        "defensive_phase_overview": defensive_phase,
        "buildup_phase_overview": buildup_phase,
        "finalization_phase_overview": finalization_phase,
    }


def initial_scouting_state(
    *,
    match_id: int,
    player_id: int,
    report_language: str = "English",
    user_notes: str = "",
) -> dict[str, Any]:
    """Minimal input for `graph.invoke`; fills empty viz slots for TypedDict consumers."""
    return {
        "messages": [],
        "match_id": match_id,
        "player_id": player_id,
        "report_language": report_language,
        "user_notes": user_notes,
        "match_label": "",
        "match_date": "",
        "competition_name": "",
        "player_name": "",
        "team_name": "",
        "player_image_data_url": None,
        "team_image_data_url": None,
        "possession_comments": [],
        "statistical_summary": "",
        "duels_visualizations": {},
        "recoveries_and_interceptions_visualization": {},
        "pass_sonar_visualization": {},
        "pass_start_network_visualization": {},
        "receiving_network_visualization": {},
        "shot_map_visualization": {},
        "crosses_map_visualization": {},
        "defensive_phase_overview": "",
        "buildup_phase_overview": "",
        "finalization_phase_overview": "",
    }


# Steps that run in the default viz-only graph (for UI progress bars, etc.)
VIZ_ONLY_PIPELINE_STEPS = 4

def init_graph_viz_only():
    """
    Preview pipeline: match metadata + visualization nodes only (no BQ possessions,
    no data-agent stats, no final scout LLM).
    """
    graph_builder = StateGraph(ScoutingReportState)
    graph_builder.add_node("match_header", node_match_header)
    graph_builder.add_node("defensive_visualizations", node_defensive_visualizations)
    graph_builder.add_node("build_up_visualizations", node_build_up_visualizations)
    graph_builder.add_node("finalization_visualizations", node_finalization_visualizations)

    graph_builder.add_edge(START, "match_header")
    graph_builder.add_edge("match_header", "defensive_visualizations")
    graph_builder.add_edge("match_header", "build_up_visualizations")
    graph_builder.add_edge("match_header", "finalization_visualizations")
    graph_builder.add_edge("defensive_visualizations", END)
    graph_builder.add_edge("build_up_visualizations", END)
    graph_builder.add_edge("finalization_visualizations", END)
    return graph_builder.compile()


def init_graph_full():
    """
    Full report: possessions + data-agent summary + viz chain + scout.
    Use `get_graph_full()` or set `get_graph()` to return this when ready.
    """
    graph_builder = StateGraph(ScoutingReportState)
    graph_builder.add_node("match_header", node_match_header)
    graph_builder.add_node("possession_comments", node_possession_comments)
    graph_builder.add_node("player_stats_summary", node_player_stats_summary)
    graph_builder.add_node("defensive_visualizations", node_defensive_visualizations)
    graph_builder.add_node("build_up_visualizations", node_build_up_visualizations)
    graph_builder.add_node("finalization_visualizations", node_finalization_visualizations)
    graph_builder.add_node("scout", node_scout)

    graph_builder.add_edge(START, "match_header")
    graph_builder.add_edge("match_header", "possession_comments")
    graph_builder.add_edge("match_header", "player_stats_summary")
    graph_builder.add_edge("match_header", "defensive_visualizations")
    graph_builder.add_edge("match_header", "build_up_visualizations")
    graph_builder.add_edge("match_header", "finalization_visualizations")

    graph_builder.add_edge("possession_comments", "scout")
    graph_builder.add_edge("player_stats_summary", "scout")
    graph_builder.add_edge("defensive_visualizations", "scout")
    graph_builder.add_edge("build_up_visualizations", "scout")
    graph_builder.add_edge("finalization_visualizations", "scout")
    graph_builder.add_edge("scout", END)
    return graph_builder.compile()


def init_graph():
    """Default graph: full end-to-end scouting workflow."""
    return init_graph_full()


def get_graph():
    return init_graph()


def get_graph_full():
    return init_graph_full()
