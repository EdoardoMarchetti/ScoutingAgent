from __future__ import annotations

from typing import Annotated, Any

from langchain_core.messages import AIMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from services.data_agent_client import query_data_agent
from services.llm_factory import create_graph_llm
from services.prompt_loader import load_prompt, render_prompt
from services.scouting_report_repository import (
    get_match_player_header,
    load_possession_comments,
)


class State(TypedDict):
    messages: Annotated[list, add_messages]
    match_id: int
    player_id: int
    report_language: str
    user_notes: str
    match_label: str
    match_date: str
    competition_name: str
    player_name: str
    team_name: str
    possession_comments: list[dict[str, Any]]
    statistical_summary: str


def _enforce_output_language(prompt_text: str, language: str) -> str:
    lang = (language or "English").strip()
    return (
        f"IMPORTANT OUTPUT LANGUAGE RULE: Write the final answer only in {lang}. "
        f"Do not mix languages.\n\n{prompt_text}"
    )


def _build_rag_context(possessions: list[dict[str, Any]]) -> str:
    if not possessions:
        return "(No possession comments available.)"

    lines: list[str] = []
    for row in possessions:
        ts = row.get("temporal_moment_json") or ""
        description = (row.get("description") or "").strip()
        if description:
            lines.append(f"- [{ts}] {description}")
    return "\n".join(lines) if lines else "(No possession comments available.)"


def node_match_header(state: State):
    header = get_match_player_header(state["match_id"], state["player_id"])
    return {
        "match_label": header["match_label"],
        "match_date": header["match_date"],
        "competition_name": header["competition_name"],
        "player_name": header["player_name"],
        "team_name": header["team_name"],
    }


def node_possession_comments(state: State):
    comments = load_possession_comments(state["match_id"], state["player_id"])
    return {"possession_comments": comments}


def node_player_stats_summary(state: State):
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


def node_scout(state: State):
    llm = create_graph_llm()

    prompt_data = load_prompt("scouting_report", version="v1")
    prompt_template = str(prompt_data.get("prompt") or "").strip()
    if not prompt_template:
        report_text = "Error: scouting_report_v1.yaml prompt is empty."
    else:
        rag_context = _build_rag_context(state.get("possession_comments") or [])
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
        prompt = _enforce_output_language(prompt, state.get("report_language", "English"))
        response = llm.invoke(prompt)
        report_text = (getattr(response, "content", "") or "").strip()

    return {"messages": [AIMessage(content=report_text)]}


def init_graph():
    graph_builder = StateGraph(State)
    graph_builder.add_node("match_header", node_match_header)
    graph_builder.add_node("possession_comments", node_possession_comments)
    graph_builder.add_node("player_stats_summary", node_player_stats_summary)
    graph_builder.add_node("scout", node_scout)

    graph_builder.add_edge(START, "match_header")
    graph_builder.add_edge("match_header", "possession_comments")
    graph_builder.add_edge("match_header", "player_stats_summary")
    graph_builder.add_edge("possession_comments", "scout")
    graph_builder.add_edge("player_stats_summary", "scout")
    graph_builder.add_edge("scout", END)
    return graph_builder.compile()


def get_graph():
    return init_graph()



