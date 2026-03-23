"""@bruin
name: scouting_agent.gold_match_possession_llm_general
type: python
image: python:3.12
connection: gcp

depends:
  - scouting_agent.gold_match_possession_llm_prompts

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
  - name: entity_id
    type: integer
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
  - name: description
    type: string
  - name: model_name
    type: string
  - name: prompt_version
    type: string
  - name: bruin_run_id
    type: string
  - name: generated_at
    type: timestamp
@bruin"""

from __future__ import annotations

import json
import os
import re
import sys
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

from wyscout_dimension_scope import (  # noqa: E402
    bq_client,
    match_date_window_predicate_sql,
)

_COLS = [
    "match_id",
    "possession_id",
    "description_type",
    "entity_id",
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
    "description",
    "model_name",
    "prompt_version",
    "bruin_run_id",
    "generated_at",
]

_DEFAULT_MODEL = "sport-data-campus.scouting_agent.bqml_gemini_generate"
_MODEL_PATH_RE = re.compile(r"^[a-zA-Z0-9_.-]+$")


def _vars() -> dict[str, Any]:
    raw = os.environ.get("BRUIN_VARS") or "{}"
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _match_id() -> int:
    v = _vars().get("match_id")
    if v is None or v == "" or isinstance(v, bool):
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _sql_str_lit(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _bqml_model_path() -> str:
    m = _vars().get("bqml_generate_model") or _DEFAULT_MODEL
    if not isinstance(m, str) or not m.strip():
        m = _DEFAULT_MODEL
    m = m.strip()
    if not _MODEL_PATH_RE.match(m) or m.count(".") != 2:
        raise ValueError(
            "bqml_generate_model must look like project.dataset.model_name "
            f"(got {m!r}). Set via pipeline default or --var."
        )
    return m


def _selected_match_ids(bq: Any, project: str, match_id: int) -> list[int]:
    """
    - If match_id > 0: include it only if it is inside date scope (when date vars exist).
    - If match_id <= 0: include all matches in date scope; if no valid date scope, return [].
    """
    window_sql = match_date_window_predicate_sql("m.match_date_utc")
    if match_id > 0:
        q = f"""
        SELECT m.match_id
        FROM `{project}.scouting_agent.dim_match` AS m
        WHERE m.match_id = @mid
          {window_sql}
        LIMIT 1
        """
        rows = list(
            bq.query(
                q,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("mid", "INT64", match_id)]
                ),
            ).result()
        )
        return [int(rows[0]["match_id"])] if rows else []
    if not window_sql:
        return []
    q = f"""
    SELECT DISTINCT m.match_id
    FROM `{project}.scouting_agent.dim_match` AS m
    WHERE m.competition_id IS NOT NULL
      {window_sql}
    ORDER BY m.match_id
    """
    return [int(r["match_id"]) for r in bq.query(q).result()]


def materialize():
    mid = _match_id()

    model_path = _bqml_model_path()
    bq = bq_client(_ROOT)
    project = bq.project
    selected_mids = _selected_match_ids(bq, project, mid)
    if not selected_mids:
        print(
            f"[llm_general] No matches selected (match_id={mid}, "
            "date window may be empty or out of scope)."
        )
        return pd.DataFrame(columns=_COLS)
    print(
        f"[llm_general] Starting description generation for {len(selected_mids)} match(es): "
        f"{selected_mids}"
    )
    chunks: list[pd.DataFrame] = []
    for one_mid in selected_mids:
        print(f"[llm_general][match_id={one_mid}] start")
        sql = f"""
WITH prompts AS (
  SELECT *
  FROM `{project}.scouting_agent.gold_match_possession_llm_prompts`
  WHERE match_id = {int(one_mid)}
),
generated AS (
  SELECT *
  FROM
    ML.GENERATE_TEXT(
      MODEL `{model_path}`,
      (SELECT * FROM prompts),
      STRUCT(
        0.4 AS temperature,
        2048 AS max_output_tokens,
        TRUE AS flatten_json_output
      )
    )
)
SELECT
  p.match_id,
  p.possession_id,
  p.description_type,
  p.entity_id,
  p.season_id,
  p.competition_id,
  p.team_in_possession,
  p.team_in_possession_name,
  p.opponent_team_id,
  p.opponent_team_name,
  p.num_events,
  p.player_id,
  p.player_name,
  p.temporal_moment_json,
  g.ml_generate_text_llm_result AS description,
  CAST({_sql_str_lit(model_path)} AS STRING) AS model_name,
  p.prompt_version,
  p.bruin_run_id,
  CURRENT_TIMESTAMP() AS generated_at
FROM prompts AS p
INNER JOIN generated AS g
  ON p.match_id = g.match_id
  AND p.possession_id = g.possession_id
  AND p.description_type = g.description_type
  AND p.entity_id = g.entity_id
"""
        try:
            job = bq.query(sql)
            part = job.result().to_dataframe(create_bqstorage_client=False)
        except Exception as exc:
            print(f"[llm_general][match_id={one_mid}] failed: {exc}")
            raise
        if part.empty:
            print(f"[llm_general][match_id={one_mid}] done: rows=0")
            continue
        cnt = (
            part.groupby("description_type", dropna=False)
            .size()
            .to_dict()
        )
        print(
            f"[llm_general][match_id={one_mid}] done: rows={len(part)} "
            f"by_type={cnt}"
        )
        chunks.append(part)

    if not chunks:
        print("[llm_general] Completed with 0 generated rows.")
        return pd.DataFrame(columns=_COLS)
    df = pd.concat(chunks, ignore_index=True)
    by_match = (
        df.groupby(["match_id", "description_type"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["match_id", "description_type"])
    )
    for _, r in by_match.iterrows():
        print(
            f"[llm_general][match_id={int(r['match_id'])}] "
            f"type={r['description_type']} rows={int(r['rows'])}"
        )

    for c in (
        "match_id",
        "possession_id",
        "entity_id",
        "season_id",
        "competition_id",
        "team_in_possession",
        "opponent_team_id",
        "num_events",
        "player_id",
    ):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    if "generated_at" in df.columns:
        df["generated_at"] = pd.to_datetime(df["generated_at"], utc=True)
    for c in _COLS:
        if c not in df.columns:
            df[c] = None
    print(f"[llm_general] Completed. Total descriptions={len(df)}")
    return df[_COLS]
