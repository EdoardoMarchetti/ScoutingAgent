from __future__ import annotations

from typing import Any

from google.cloud import bigquery

from services.bigquery_client import get_bigquery_client, get_bq_project_id


def get_match_player_header(match_id: int, player_id: int) -> dict[str, str]:
    project = get_bq_project_id()
    client = get_bigquery_client()

    query = f"""
        SELECT
          m.match_id,
          CAST(m.match_date_utc AS STRING) AS match_date,
          comp.name AS competition_name,
          home.name AS home_team_name,
          away.name AS away_team_name,
          p.player_id,
          COALESCE(
            NULLIF(TRIM(p.short_name), ''),
            NULLIF(TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.last_name, ''))), '')
          ) AS player_name
        FROM `{project}.scouting_agent.dim_match` AS m
        LEFT JOIN `{project}.scouting_agent.dim_competition` AS comp
          ON comp.competition_id = m.competition_id
        LEFT JOIN `{project}.scouting_agent.dim_team` AS home
          ON home.team_id = m.home_team_id
        LEFT JOIN `{project}.scouting_agent.dim_team` AS away
          ON away.team_id = m.away_team_id
        LEFT JOIN `{project}.scouting_agent.dim_player` AS p
          ON p.player_id = @player_id
        WHERE m.match_id = @match_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("match_id", "INT64", int(match_id)),
            bigquery.ScalarQueryParameter("player_id", "INT64", int(player_id)),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return {
            "match_label": f"Match {match_id}",
            "match_date": "Unknown",
            "competition_name": "Unknown",
            "player_name": f"Player {player_id}",
            "team_name": "Unknown",
        }

    row = dict(rows[0].items())
    home = (row.get("home_team_name") or "Unknown home").strip()
    away = (row.get("away_team_name") or "Unknown away").strip()
    return {
        "match_label": f"{home} vs {away}",
        "match_date": (row.get("match_date") or "Unknown").strip(),
        "competition_name": (row.get("competition_name") or "Unknown").strip(),
        "player_name": (row.get("player_name") or f"Player {player_id}").strip(),
        "team_name": home,
    }


def load_possession_comments(match_id: int, player_id: int) -> list[dict[str, Any]]:
    project = get_bq_project_id()
    client = get_bigquery_client()

    query = f"""
        SELECT
          match_id,
          possession_id,
          player_id,
          player_name,
          team_in_possession_name,
          temporal_moment_json,
          description,
          generated_at
        FROM `{project}.scouting_agent.gold_match_possession_llm_general`
        WHERE match_id = @match_id
          AND player_id = @player_id
          AND COALESCE(TRIM(description), '') != ''
        ORDER BY possession_id
        
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("match_id", "INT64", int(match_id)),
            bigquery.ScalarQueryParameter("player_id", "INT64", int(player_id)),
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    return [dict(row.items()) for row in rows]
