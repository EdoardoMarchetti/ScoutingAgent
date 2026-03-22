"""@bruin
name: scouting_agent.gold_match_possession_llm_prompts
type: python
image: python:3.12
connection: gcp

depends:
  - scouting_agent.gold_match_possession
  - scouting_agent.silver_match_event
  - scouting_agent.dim_match
  - scouting_agent.dim_team
  - scouting_agent.dim_player

materialization:
  type: table
  strategy: merge

parameters:
  enforce_schema: true

columns:
  - name: match_id
    type: integer
    primary_key: true
  - name: possession_id
    type: integer
    primary_key: true
  - name: description_type
    type: string
    primary_key: true
  - name: season_id
    type: integer
  - name: competition_id
    type: integer
  - name: team_in_possession
    type: integer
  - name: team_in_possession_name
    type: string
  - name: opponent_team_id
    type: integer
  - name: opponent_team_name
    type: string
  - name: num_events
    type: integer
  - name: player_id
    type: integer
  - name: player_name
    type: string
  - name: temporal_moment_json
    type: string
  - name: prompt
    type: string
  - name: prompt_version
    type: string
  - name: bruin_run_id
    type: string
  - name: built_at
    type: timestamp
@bruin"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

_ROOT = Path(__file__).resolve().parents[3]
_BRUIN_PL = Path(__file__).resolve().parents[2]
for p in (_ROOT, _BRUIN_PL):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

load_dotenv(_ROOT / ".env")

from possession_analyzer import (  # noqa: E402
    analyze_possession,
    extract_possessions,
    parse_timestamp,
)
from possession_description import (  # noqa: E402
    render_general_possession_prompt,
    _analysis_row_metadata,
)
from possession_gold_overlay import overlay_gold_match_possession  # noqa: E402
from wyscout_dimension_scope import (  # noqa: E402
    analyzer_event_from_silver_match_event_row,
    bq_client,
)

_PROMPT_VERSION = "possession_description_general_v1"

_COLS = [
    "match_id",
    "possession_id",
    "description_type",
    "season_id",
    "competition_id",
    "team_in_possession",
    "team_in_possession_name",
    "opponent_team_id",
    "opponent_team_name",
    "num_events",
    "player_id",
    "player_name",
    "temporal_moment_json",
    "prompt",
    "prompt_version",
    "bruin_run_id",
    "built_at",
]


def _match_id_from_vars() -> int:
    raw = os.environ.get("BRUIN_VARS") or "{}"
    try:
        blob = json.loads(raw)
    except json.JSONDecodeError:
        return 0
    v = blob.get("match_id")
    if v is None or v == "":
        return 0
    if isinstance(v, bool):
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _row_dict(r: Any) -> dict[str, Any]:
    return {k: r[k] for k in r.keys()}


def _player_display_name(row: dict[str, Any]) -> str | None:
    sn = row.get("short_name")
    if isinstance(sn, str) and sn.strip():
        return sn.strip()
    fn = (row.get("first_name") or "").strip() if isinstance(row.get("first_name"), str) else ""
    ln = (row.get("last_name") or "").strip() if isinstance(row.get("last_name"), str) else ""
    s = f"{fn} {ln}".strip()
    return s or None


def _opponent_team(
    team_in_possession: Any,
    home_id: Any,
    away_id: Any,
    home_name: str | None,
    away_name: str | None,
) -> tuple[int | None, str | None]:
    if team_in_possession is None:
        return None, None
    ts, hs, aws = str(team_in_possession), str(home_id), str(away_id)
    if home_id is not None and ts == hs:
        oid = int(away_id) if away_id is not None else None
        return oid, away_name
    if away_id is not None and ts == aws:
        oid = int(home_id) if home_id is not None else None
        return oid, home_name
    return None, None


def _match_info_from_dims(
    home_id: Any,
    away_id: Any,
    home_name: str | None,
    away_name: str | None,
) -> dict[str, Any]:
    if home_id is None or away_id is None:
        return {}
    hi, ai = int(home_id), int(away_id)
    return {
        "teamsData": {
            str(hi): {"team": {"id": hi, "name": home_name}},
            str(ai): {"team": {"id": ai, "name": away_name}},
        }
    }


def materialize():
    match_id = _match_id_from_vars()
    if match_id <= 0:
        return pd.DataFrame(columns=_COLS)

    bq = bq_client(_ROOT)
    project = bq.project
    mid = match_id

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("mid", "INT64", mid)]
    )

    silver_job = bq.query(
        f"""
        SELECT *
        FROM `{project}.scouting_agent.silver_match_event`
        WHERE match_id = @mid
        ORDER BY minute, second, event_id
        """,
        job_config=job_config,
    )
    silver_rows = [_row_dict(r) for r in silver_job.result()]
    if not silver_rows:
        return pd.DataFrame(columns=_COLS)

    gold_job = bq.query(
        f"""
        SELECT *
        FROM `{project}.scouting_agent.gold_match_possession`
        WHERE match_id = @mid
        """,
        job_config=job_config,
    )
    gold_list = [_row_dict(r) for r in gold_job.result()]
    gold_by_pid = {int(g["possession_id"]): g for g in gold_list if g.get("possession_id") is not None}

    dm_job = bq.query(
        f"""
        SELECT match_id, season_id, competition_id, home_team_id, away_team_id
        FROM `{project}.scouting_agent.dim_match`
        WHERE match_id = @mid
        LIMIT 1
        """,
        job_config=job_config,
    )
    dm_rows = list(dm_job.result())
    if not dm_rows:
        raise RuntimeError(f"dim_match has no row for match_id={mid}")
    dm = _row_dict(dm_rows[0])
    home_id, away_id = dm.get("home_team_id"), dm.get("away_team_id")
    season_id, competition_id = dm.get("season_id"), dm.get("competition_id")

    team_ids = {x for x in (home_id, away_id) if x is not None}
    team_names: dict[int, str] = {}
    if team_ids:
        t_job = bq.query(
            f"""
            SELECT team_id, name
            FROM `{project}.scouting_agent.dim_team`
            WHERE team_id IN UNNEST(@tids)
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("tids", "INT64", [int(x) for x in team_ids]),
                ]
            ),
        )
        for r in t_job.result():
            team_names[int(r["team_id"])] = str(r["name"] or "")

    home_name = team_names.get(int(home_id)) if home_id is not None else None
    away_name = team_names.get(int(away_id)) if away_id is not None else None
    match_info = _match_info_from_dims(home_id, away_id, home_name, away_name)

    pids = {int(r["player_id"]) for r in silver_rows if r.get("player_id") is not None}
    player_names: dict[int, str] = {}
    if pids:
        p_job = bq.query(
            f"""
            SELECT player_id, short_name, first_name, last_name
            FROM `{project}.scouting_agent.dim_player`
            WHERE player_id IN UNNEST(@pids)
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("pids", "INT64", sorted(pids)),
                ]
            ),
        )
        for r in p_job.result():
            d = _row_dict(r)
            disp = _player_display_name(d)
            if disp:
                player_names[int(d["player_id"])] = disp

    events: list[dict[str, Any]] = []
    for r in silver_rows:
        pid = r.get("player_id")
        pname = player_names.get(int(pid)) if pid is not None else None
        ev = analyzer_event_from_silver_match_event_row(r, player_display_name=pname)
        if ev:
            events.append(ev)

    all_sorted = sorted(
        events,
        key=lambda e: parse_timestamp(e.get("matchTimestamp", "00:00:00.000")),
    )
    possessions = extract_possessions({"events": all_sorted})
    if not possessions:
        return pd.DataFrame(columns=_COLS)

    run_id = os.environ.get("BRUIN_RUN_ID") or ""
    now = datetime.now(timezone.utc)

    ordered_pids = sorted(
        possessions.keys(),
        key=lambda x: parse_timestamp(possessions[x][0].get("matchTimestamp", "00:00:00.000")),
    )

    out_rows: list[dict[str, Any]] = []
    for pid in ordered_pids:
        pe = possessions[pid]
        analysis = analyze_possession(pe, match_info, all_match_events=all_sorted)
        if not analysis:
            continue

        gold_row = gold_by_pid.get(int(pid))
        leading_name: str | None = None
        if gold_row is not None:
            lid = gold_row.get("team_leading_id")
            if lid not in (None, 0):
                leading_name = team_names.get(int(lid))
        analysis = overlay_gold_match_possession(
            analysis, gold_row, leading_team_name=leading_name
        )

        tip = analysis.get("team_in_possession")
        tip_name = analysis.get("team_in_possession_name")
        if tip is not None:
            from_dim = team_names.get(int(tip))
            if isinstance(from_dim, str) and from_dim.strip():
                tip_name = from_dim.strip()
        oid, oname = _opponent_team(tip, home_id, away_id, home_name, away_name)

        enriched = {
            **analysis,
            "match_id": mid,
            "season_id": int(season_id) if season_id is not None else None,
            "competition_id": int(competition_id) if competition_id is not None else None,
            "team_in_possession_name": tip_name,
            "opponent_team_id": oid,
            "opponent_team_name": oname,
        }

        prompt_text = render_general_possession_prompt(enriched)
        meta = _analysis_row_metadata(enriched)
        tm = meta.pop("temporal_moment", None)
        out_rows.append(
            {
                "match_id": mid,
                "possession_id": meta.get("possession_id"),
                "description_type": "general",
                "season_id": meta.get("season_id"),
                "competition_id": meta.get("competition_id"),
                "team_in_possession": meta.get("team_in_possession"),
                "team_in_possession_name": meta.get("team_in_possession_name"),
                "opponent_team_id": meta.get("opponent_team_id"),
                "opponent_team_name": meta.get("opponent_team_name"),
                "num_events": meta.get("num_events"),
                "player_id": None,
                "player_name": None,
                "temporal_moment_json": (
                    json.dumps(tm, ensure_ascii=False) if isinstance(tm, dict) else None
                ),
                "prompt": prompt_text,
                "prompt_version": _PROMPT_VERSION,
                "bruin_run_id": run_id,
                "built_at": now,
            }
        )

    if not out_rows:
        return pd.DataFrame(columns=_COLS)

    df = pd.DataFrame(out_rows)
    for c in (
        "match_id",
        "possession_id",
        "season_id",
        "competition_id",
        "team_in_possession",
        "opponent_team_id",
        "num_events",
        "player_id",
    ):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["built_at"] = pd.to_datetime(df["built_at"], utc=True)
    for c in _COLS:
        if c not in df.columns:
            df[c] = None
    return df[_COLS]
