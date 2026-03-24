"""Streamlit-cached wrappers for player comparison repository (thin layer)."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from services import player_compare_repository as repo


@st.cache_data(ttl=600, show_spinner=False)
def search_players_cached(
    name_query: str,
    *,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Search players by name (cached)."""
    return repo.search_players(name_query, limit=limit)


@st.cache_data(ttl=600, show_spinner=False)
def fetch_cohort_player_ids_cached(
    target_player_id: int,
    start_date: str,
    end_date: str,
    *,
    team_ids: tuple[int, ...] | None = None,
    role_names: tuple[str, ...] | None = None,
    max_comparators: int = 30,
    min_events_threshold: int = 50,
) -> dict[str, Any]:
    """Fetch cohort (target + comparators) with filters (cached)."""
    team_list = list(team_ids) if team_ids else None
    role_list = list(role_names) if role_names else None
    
    return repo.fetch_cohort_player_ids(
        target_player_id=target_player_id,
        start_date=start_date,
        end_date=end_date,
        team_ids=team_list,
        role_names=role_list,
        max_comparators=max_comparators,
        min_events_threshold=min_events_threshold,
    )


@st.cache_data(ttl=600, show_spinner=False)
def fetch_player_metrics_aggregated_cached(
    player_ids: tuple[int, ...],
    metric_ids: tuple[str, ...],
    start_date: str,
    end_date: str,
    *,
    per_match_mode: bool = False,
) -> pd.DataFrame:
    """Fetch aggregated metrics (cached)."""
    return repo.fetch_player_metrics_aggregated(
        player_ids=list(player_ids),
        metric_ids=list(metric_ids),
        start_date=start_date,
        end_date=end_date,
        per_match_mode=per_match_mode,
    )


@st.cache_data(show_spinner=False)
def load_metrics_catalog_cached() -> dict[str, Any]:
    """Load metrics catalog YAML (cached)."""
    return repo.load_metrics_catalog()


@st.cache_data(ttl=600, show_spinner=False)
def get_available_teams_cached() -> list[dict[str, Any]]:
    """Get all teams for filter UI (cached)."""
    return repo.get_available_teams()
