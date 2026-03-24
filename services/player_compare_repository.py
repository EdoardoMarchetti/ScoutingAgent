"""Player comparison data repository (BQ queries, no Streamlit coupling)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from google.cloud import bigquery

from services.bigquery_client import get_bigquery_client, get_bq_project_id


def load_metrics_catalog() -> dict[str, Any]:
    """Parse config/player_compare_metrics.yaml."""
    config_path = Path(__file__).resolve().parents[1] / "config" / "player_compare_metrics.yaml"
    with open(config_path, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def search_players(
    name_query: str,
    *,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """
    Full-text search on dim_player (first_name + last_name + short_name).
    
    Returns list of {player_id, display_name, role_name, current_team_id, current_team_name, player_image_data_url, team_image_data_url}.
    """
    project = get_bq_project_id()
    client = get_bigquery_client()
    
    normalized = name_query.strip().lower()
    if not normalized:
        return []
    
    query = f"""
        SELECT
          p.player_id,
          TRIM(CONCAT(
            COALESCE(p.first_name, ''), ' ',
            COALESCE(p.last_name, '')
          )) AS display_name,
          COALESCE(p.short_name, '') AS short_name,
          p.role_name,
          p.current_team_id,
          t.name AS current_team_name,
          p.image_data_url AS player_image_data_url,
          t.image_data_url AS team_image_data_url
        FROM `{project}.scouting_agent.dim_player` AS p
        LEFT JOIN `{project}.scouting_agent.dim_team` AS t
          ON t.team_id = p.current_team_id
        WHERE
          LOWER(CONCAT(
            COALESCE(p.first_name, ''), ' ',
            COALESCE(p.last_name, ''), ' ',
            COALESCE(p.short_name, '')
          )) LIKE @pattern
        ORDER BY p.player_id
        LIMIT @limit
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pattern", "STRING", f"%{normalized}%"),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    
    rows = client.query(query, job_config=job_config).result()
    results = []
    for r in rows:
        row_dict = dict(r.items())
        dn = str(row_dict.get("display_name") or "").strip()
        sn = str(row_dict.get("short_name") or "").strip()
        final_name = dn if dn else sn if sn else "Unknown"
        results.append({
            "player_id": row_dict["player_id"],
            "display_name": final_name,
            "role_name": row_dict.get("role_name") or "",
            "current_team_id": row_dict.get("current_team_id"),
            "current_team_name": row_dict.get("current_team_name") or "",
            "player_image_data_url": row_dict.get("player_image_data_url") or "",
            "team_image_data_url": row_dict.get("team_image_data_url") or "",
        })
    return results


def get_available_teams() -> list[dict[str, Any]]:
    """
    Get all teams from dim_team for filter UI.
    
    Returns list of {team_id, team_name}.
    """
    project = get_bq_project_id()
    client = get_bigquery_client()
    
    query = f"""
        SELECT
          team_id,
          name AS team_name
        FROM `{project}.scouting_agent.dim_team`
        WHERE name IS NOT NULL
        ORDER BY name
    """
    
    rows = client.query(query).result()
    return [{"team_id": row.team_id, "team_name": row.team_name} for row in rows]


def fetch_cohort_player_ids(
    target_player_id: int,
    start_date: str,
    end_date: str,
    *,
    team_ids: list[int] | None = None,
    role_names: list[str] | None = None,
    max_comparators: int = 30,
    min_events_threshold: int = 50,
) -> dict[str, Any]:
    """
    Find comparable players (excluding target) within date range, optional filters.
    
    Returns:
      {
        "target": {player_id, display_name, role_name, current_team_id, current_team_name},
        "comparators": list of same schema,
      }
    
    Uses silver_match_event count as "activity threshold" proxy for minutes played.
    """
    project = get_bq_project_id()
    client = get_bigquery_client()
    
    # Parse date filter
    date_filter_sql = """
      COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(m.match_date_utc)),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', TRIM(m.match_date_utc)),
        TIMESTAMP(SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(TRIM(m.match_date_utc), 1, 10)))
      ) BETWEEN @start_ts AND @end_ts
    """
    
    # Team filter
    team_filter_sql = ""
    if team_ids:
        team_filter_sql = "AND p.current_team_id IN UNNEST(@team_ids)"
    
    # Role filter
    role_filter_sql = ""
    if role_names:
        role_filter_sql = "AND p.role_name IN UNNEST(@role_names)"
    
    query = f"""
        WITH player_activity AS (
          SELECT
            se.player_id,
            COUNT(*) AS event_count
          FROM `{project}.scouting_agent.silver_match_event` AS se
          INNER JOIN `{project}.scouting_agent.dim_match` AS m
            ON m.match_id = se.match_id
          WHERE
            se.player_id != 0
            AND {date_filter_sql}
          GROUP BY se.player_id
          HAVING COUNT(*) >= @min_events
        ),
        players_enriched AS (
          SELECT
            pa.player_id,
            pa.event_count,
            TRIM(CONCAT(
              COALESCE(p.first_name, ''), ' ',
              COALESCE(p.last_name, '')
            )) AS display_name,
            COALESCE(p.short_name, '') AS short_name,
            p.role_name,
            p.current_team_id,
            t.name AS current_team_name
          FROM player_activity AS pa
          INNER JOIN `{project}.scouting_agent.dim_player` AS p
            ON p.player_id = pa.player_id
          LEFT JOIN `{project}.scouting_agent.dim_team` AS t
            ON t.team_id = p.current_team_id
          WHERE TRUE
            {team_filter_sql}
            {role_filter_sql}
        ),
        target_player AS (
          SELECT
            player_id,
            display_name,
            short_name,
            role_name,
            current_team_id,
            current_team_name
          FROM players_enriched
          WHERE player_id = @target_player_id
        ),
        comparator_players AS (
          SELECT
            player_id,
            display_name,
            short_name,
            role_name,
            current_team_id,
            current_team_name,
            event_count
          FROM players_enriched
          WHERE player_id != @target_player_id
          ORDER BY event_count DESC
          LIMIT @max_comparators
        )
        SELECT 'target' AS group_label, * FROM target_player
        UNION ALL
        SELECT 'comparator' AS group_label, player_id, display_name, short_name, role_name, current_team_id, current_team_name FROM comparator_players
    """
    
    params = [
        bigquery.ScalarQueryParameter("start_ts", "TIMESTAMP", start_date),
        bigquery.ScalarQueryParameter("end_ts", "TIMESTAMP", end_date),
        bigquery.ScalarQueryParameter("target_player_id", "INT64", target_player_id),
        bigquery.ScalarQueryParameter("min_events", "INT64", min_events_threshold),
        bigquery.ScalarQueryParameter("max_comparators", "INT64", max_comparators),
    ]
    if team_ids:
        params.append(bigquery.ArrayQueryParameter("team_ids", "INT64", team_ids))
    if role_names:
        params.append(bigquery.ArrayQueryParameter("role_names", "STRING", role_names))
    
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = client.query(query, job_config=job_config).result()
    
    target = None
    comparators = []
    
    for r in rows:
        row_dict = dict(r.items())
        group = row_dict.get("group_label")
        dn = str(row_dict.get("display_name") or "").strip()
        sn = str(row_dict.get("short_name") or "").strip()
        final_name = dn if dn else sn if sn else "Unknown"
        
        player_info = {
            "player_id": row_dict["player_id"],
            "display_name": final_name,
            "role_name": row_dict.get("role_name") or "",
            "current_team_id": row_dict.get("current_team_id"),
            "current_team_name": row_dict.get("current_team_name") or "",
        }
        
        if group == "target":
            target = player_info
        else:
            comparators.append(player_info)
    
    return {
        "target": target,
        "comparators": comparators,
    }


def fetch_player_metrics_aggregated(
    player_ids: list[int],
    metric_ids: list[str],
    start_date: str,
    end_date: str,
    *,
    per_match_mode: bool = False,
) -> pd.DataFrame:
    """
    Fetch aggregated metrics for players in date range.
    
    Returns DataFrame with columns: player_id, display_name, metric_id, metric_label, value, matches_played.
    If per_match_mode=True, value represents total (caller divides by matches_played in UI).
    """
    catalog = load_metrics_catalog()
    metrics_dict = {m["id"]: m for m in catalog.get("metrics", [])}
    
    project = get_bq_project_id()
    client = get_bigquery_client()
    
    if not player_ids or not metric_ids:
        return pd.DataFrame(columns=["player_id", "display_name", "metric_id", "metric_label", "value", "matches_played"])
    
    # Filter requested metrics
    selected_metrics = [metrics_dict[mid] for mid in metric_ids if mid in metrics_dict]
    if not selected_metrics:
        return pd.DataFrame(columns=["player_id", "display_name", "metric_id", "metric_label", "value", "matches_played"])
    
    date_filter_sql = """
      COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(m.match_date_utc)),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', TRIM(m.match_date_utc)),
        TIMESTAMP(SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(TRIM(m.match_date_utc), 1, 10)))
      ) BETWEEN @start_ts AND @end_ts
    """
    
    # Build CTEs for each gold table used
    gold_tables_needed = list({m["source"] for m in selected_metrics})
    
    ctes = []
    for table in gold_tables_needed:
        cte_name = table.replace("gold_match_", "").replace("_event", "")
        ctes.append(f"""
        {cte_name}_events AS (
          SELECT
            ge.player_id,
            ge.match_id
          FROM `{project}.scouting_agent.{table}` AS ge
          INNER JOIN `{project}.scouting_agent.dim_match` AS m
            ON m.match_id = ge.match_id
          WHERE
            ge.player_id IN UNNEST(@player_ids)
            AND {date_filter_sql}
        )
        """)
    
    # Build metric queries
    metric_unions = []
    for metric in selected_metrics:
        table = metric["source"]
        cte_name = table.replace("gold_match_", "").replace("_event", "")
        agg = metric["aggregation"]
        value_col = metric["value_column"]
        filter_clause = metric.get("filter")
        
        filter_sql = f"AND {filter_clause}" if filter_clause else ""
        
        if agg == "count":
            agg_expr = f"COUNT(*)"
        elif agg == "sum":
            agg_expr = f"SUM({value_col})"
        elif agg == "avg":
            agg_expr = f"AVG({value_col})"
        else:
            agg_expr = f"COUNT(*)"
        
        metric_unions.append(f"""
        SELECT
          ge.player_id,
          '{metric["id"]}' AS metric_id,
          '{metric["label"]}' AS metric_label,
          {agg_expr} AS value
        FROM `{project}.scouting_agent.{table}` AS ge
        INNER JOIN `{project}.scouting_agent.dim_match` AS m
          ON m.match_id = ge.match_id
        WHERE
          ge.player_id IN UNNEST(@player_ids)
          AND {date_filter_sql}
          {filter_sql}
        GROUP BY ge.player_id
        """)
    
    # Matches played CTE
    matches_cte = f"""
    matches_played AS (
      SELECT
        se.player_id,
        COUNT(DISTINCT se.match_id) AS matches_played
      FROM `{project}.scouting_agent.silver_match_event` AS se
      INNER JOIN `{project}.scouting_agent.dim_match` AS m
        ON m.match_id = se.match_id
      WHERE
        se.player_id IN UNNEST(@player_ids)
        AND {date_filter_sql}
      GROUP BY se.player_id
    )
    """
    
    # Player names
    players_cte = f"""
    players_info AS (
      SELECT
        p.player_id,
        TRIM(CONCAT(
          COALESCE(p.first_name, ''), ' ',
          COALESCE(p.last_name, '')
        )) AS display_name,
        COALESCE(p.short_name, '') AS short_name
      FROM `{project}.scouting_agent.dim_player` AS p
      WHERE p.player_id IN UNNEST(@player_ids)
    )
    """
    
    # Full query
    query = f"""
        WITH
        {matches_cte},
        {players_cte},
        all_metrics AS (
          {" UNION ALL ".join(metric_unions)}
        )
        SELECT
          am.player_id,
          COALESCE(NULLIF(pi.display_name, ''), pi.short_name, 'Unknown') AS display_name,
          am.metric_id,
          am.metric_label,
          am.value,
          COALESCE(mp.matches_played, 0) AS matches_played
        FROM all_metrics AS am
        LEFT JOIN matches_played AS mp
          ON mp.player_id = am.player_id
        LEFT JOIN players_info AS pi
          ON pi.player_id = am.player_id
        ORDER BY am.player_id, am.metric_id
    """
    
    params = [
        bigquery.ArrayQueryParameter("player_ids", "INT64", player_ids),
        bigquery.ScalarQueryParameter("start_ts", "TIMESTAMP", start_date),
        bigquery.ScalarQueryParameter("end_ts", "TIMESTAMP", end_date),
    ]
    
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = client.query(query, job_config=job_config).result()
    
    df = pd.DataFrame([dict(r.items()) for r in rows])
    if df.empty:
        return pd.DataFrame(columns=["player_id", "display_name", "metric_id", "metric_label", "value", "matches_played"])
    
    return df
