from __future__ import annotations

from typing import Annotated, Any

from langgraph.graph.message import add_messages
from typing_extensions import TypedDict


class ScoutingReportState(TypedDict):
    """Shared LangGraph state for the scouting report pipeline."""

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
    player_image_data_url: str | None
    team_image_data_url: str | None
    possession_comments: list[dict[str, Any]]
    statistical_summary: str
    # Visualizations (each: markdown_image, description, caption, events, optional error)
    duels_visualizations: dict[str, Any]
    recoveries_and_interceptions_visualization: dict[str, Any]
    pass_sonar_visualization: dict[str, Any]
    pass_start_network_visualization: dict[str, Any]
    receiving_network_visualization: dict[str, Any]
    shot_map_visualization: dict[str, Any]
    crosses_map_visualization: dict[str, Any]
    # Phase overviews synthesized from per-viz captions
    defensive_phase_overview: str
    buildup_phase_overview: str
    finalization_phase_overview: str
