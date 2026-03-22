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

_ROOT = Path(__file__).resolve().parents[3]
_BRUIN_PL = Path(__file__).resolve().parents[2]
for p in (_ROOT, _BRUIN_PL):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

load_dotenv(_ROOT / ".env")

from wyscout_dimension_scope import bq_client  # noqa: E402

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


def materialize():
    mid = _match_id()
    if mid <= 0:
        return pd.DataFrame(columns=_COLS)

    model_path = _bqml_model_path()
    bq = bq_client(_ROOT)
    project = bq.project

    sql = f"""
WITH prompts AS (
  SELECT *
  FROM `{project}.scouting_agent.gold_match_possession_llm_prompts`
  WHERE match_id = {int(mid)}
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
"""
    job = bq.query(sql)
    df = job.result().to_dataframe(create_bqstorage_client=False)

    if df.empty:
        return pd.DataFrame(columns=_COLS)

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
    if "generated_at" in df.columns:
        df["generated_at"] = pd.to_datetime(df["generated_at"], utc=True)
    for c in _COLS:
        if c not in df.columns:
            df[c] = None
    return df[_COLS]
