from __future__ import annotations

from typing import Any

import streamlit as st
from google.cloud import bigquery

from services.bigquery_client import get_bigquery_client, get_bq_project_id


def _match_ordering_sql(column_expr: str) -> str:
    c = column_expr
    return (
        "COALESCE("
        f"SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM({c})), "
        f"SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', TRIM({c})), "
        f"TIMESTAMP(SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(TRIM({c}), 1, 10)))"
        ")"
    )


def short_match_date_label(value: Any) -> str:
    """Compact date for selectbox labels (YYYY-MM-DD when parseable)."""
    s = str(value or "").strip()
    if not s:
        return "—"
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s[:19] if len(s) > 19 else s


def match_select_label(row: dict[str, Any]) -> str:
    """Match line for UI: date — home vs away (no ids)."""
    d = short_match_date_label(row.get("match_date_utc"))
    home = (row.get("home_team_name") or "Home").strip()
    away = (row.get("away_team_name") or "Away").strip()
    return f"{d} — {home} vs {away}"


def _normalize_match_row_for_filters(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    aid = out.get("area_id")
    out["_area_id"] = int(aid) if aid is not None else -1
    out["_area_name"] = (str(out.get("area_name") or "").strip() or "Unknown area")
    cid = out.get("competition_id")
    out["_competition_id"] = int(cid) if cid is not None else -1
    out["_competition_name"] = (str(out.get("competition_name") or "").strip() or "Unknown competition")
    sid = out.get("season_id")
    out["_season_id"] = int(sid) if sid is not None else -1
    out["_season_name"] = (str(out.get("season_name") or "").strip() or "Unknown season")
    return out


@st.cache_data(ttl=300, show_spinner=False)
def get_available_matches_enriched(limit: int = 800) -> list[dict[str, Any]]:
    """Matches with area / competition / season dims for cascaded filters."""
    project = get_bq_project_id()
    client = get_bigquery_client()
    order_expr = _match_ordering_sql("m.match_date_utc")

    query = f"""
        SELECT
          m.match_id,
          m.match_date_utc,
          m.season_id,
          s.name AS season_name,
          m.competition_id,
          comp.name AS competition_name,
          comp.area_id,
          ar.name AS area_name,
          home_team.name AS home_team_name,
          away_team.name AS away_team_name
        FROM `{project}.scouting_agent.dim_match` AS m
        LEFT JOIN `{project}.scouting_agent.dim_season` AS s
          ON s.season_id = m.season_id
        LEFT JOIN `{project}.scouting_agent.dim_competition` AS comp
          ON comp.competition_id = m.competition_id
        LEFT JOIN `{project}.scouting_agent.dim_area` AS ar
          ON ar.area_id = comp.area_id
        LEFT JOIN `{project}.scouting_agent.dim_team` AS home_team
          ON home_team.team_id = m.home_team_id
        LEFT JOIN `{project}.scouting_agent.dim_team` AS away_team
          ON away_team.team_id = m.away_team_id
        ORDER BY {order_expr} DESC, m.match_id DESC
        LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", int(limit))]
    )
    rows = client.query(query, job_config=job_config).result()
    return [_normalize_match_row_for_filters(dict(r.items())) for r in rows]


def distinct_areas(matches: list[dict[str, Any]]) -> list[tuple[int, str]]:
    seen: dict[int, str] = {}
    for row in matches:
        aid = row["_area_id"]
        if aid not in seen:
            seen[aid] = row["_area_name"]
    return sorted(seen.items(), key=lambda x: (x[1].lower(), x[0]))


def distinct_competitions(matches: list[dict[str, Any]], area_id: int) -> list[tuple[int, str]]:
    seen: dict[int, str] = {}
    for row in matches:
        if row["_area_id"] != area_id:
            continue
        cid = row["_competition_id"]
        name = row["_competition_name"]
        seen[cid] = name
    return sorted(seen.items(), key=lambda x: (x[1].lower(), x[0]))


def distinct_seasons(
    matches: list[dict[str, Any]], area_id: int, competition_id: int
) -> list[tuple[int, str]]:
    seen: dict[int, str] = {}
    for row in matches:
        if row["_area_id"] != area_id or row["_competition_id"] != competition_id:
            continue
        sid = row["_season_id"]
        name = row["_season_name"]
        seen[sid] = name
    return sorted(seen.items(), key=lambda x: (x[1].lower(), x[0]))


def matches_after_filters(
    matches: list[dict[str, Any]],
    *,
    area_id: int,
    competition_id: int,
    season_id: int,
) -> list[dict[str, Any]]:
    return [
        r
        for r in matches
        if r["_area_id"] == area_id
        and r["_competition_id"] == competition_id
        and r["_season_id"] == season_id
    ]


@st.cache_data(ttl=300, show_spinner=False)
def get_available_matches(limit: int = 200) -> list[dict[str, Any]]:
    project = get_bq_project_id()
    client = get_bigquery_client()
    order_expr = _match_ordering_sql("m.match_date_utc")

    query = f"""
        SELECT
          m.match_id,
          m.match_date_utc,
          m.competition_id,
          m.season_id,
          home_team.name AS home_team_name,
          away_team.name AS away_team_name,
          comp.name AS competition_name
        FROM `{project}.scouting_agent.dim_match` AS m
        LEFT JOIN `{project}.scouting_agent.dim_team` AS home_team
          ON home_team.team_id = m.home_team_id
        LEFT JOIN `{project}.scouting_agent.dim_team` AS away_team
          ON away_team.team_id = m.away_team_id
        LEFT JOIN `{project}.scouting_agent.dim_competition` AS comp
          ON comp.competition_id = m.competition_id
        ORDER BY {order_expr} DESC, m.match_id DESC
        LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", int(limit))]
    )
    rows = client.query(query, job_config=job_config).result()
    return [dict(row.items()) for row in rows]


def build_match_labels(matches: list[dict[str, Any]]) -> dict[int, str]:
    labels: dict[int, str] = {}
    for row in matches:
        match_id = int(row["match_id"])
        date_label = (row.get("match_date_utc") or "Unknown date").strip()
        home = (row.get("home_team_name") or "Unknown home").strip()
        away = (row.get("away_team_name") or "Unknown away").strip()
        competition = (row.get("competition_name") or "Unknown competition").strip()
        labels[match_id] = f"{match_id} | {date_label} | {home} vs {away} | {competition}"
    return labels


@st.cache_data(ttl=300, show_spinner=False)
def get_players_for_match(match_id: int, limit: int = 200) -> list[dict[str, Any]]:
    project = get_bq_project_id()
    client = get_bigquery_client()

    query = f"""
        SELECT DISTINCT
          s.player_id,
          COALESCE(
            NULLIF(TRIM(p.short_name), ''),
            NULLIF(TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.last_name, ''))), '')
          ) AS player_name,
          p.role_name
        FROM `{project}.scouting_agent.silver_match_event` AS s
        LEFT JOIN `{project}.scouting_agent.dim_player` AS p
          ON p.player_id = s.player_id
        WHERE s.match_id = @match_id
          AND s.player_id IS NOT NULL
          AND s.player_id != 0
        ORDER BY player_name, s.player_id
        LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("match_id", "INT64", int(match_id)),
            bigquery.ScalarQueryParameter("limit", "INT64", int(limit)),
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    return [dict(row.items()) for row in rows]


def build_player_labels(players: list[dict[str, Any]]) -> dict[int, str]:
    labels: dict[int, str] = {}
    for row in players:
        player_id = int(row["player_id"])
        player_name = (row.get("player_name") or "Unknown player").strip()
        role_name = (row.get("role_name") or "Unknown role").strip()
        labels[player_id] = f"{player_name} ({role_name})"
    return labels
