"""@bruin
name: scouting_agent.dim_team
type: python
image: python:3.12
connection: gcp

depends:
  - scouting_agent.dim_season

materialization:
  type: table
  strategy: merge

parameters:
  enforce_schema: true

columns:
  - name: team_id
    type: integer
    primary_key: true
  - name: name
    type: string
  - name: official_name
    type: string
  - name: type
    type: string
  - name: category
    type: string
  - name: gender
    type: string
  - name: city
    type: string
  - name: gsm_id
    type: integer
  - name: image_data_url
    type: string
  - name: area_id
    type: integer
  - name: area_name
    type: string
  - name: area_alpha2_code
    type: string
  - name: area_alpha3_code
    type: string
@bruin"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[3]
_BRUIN_PL = Path(__file__).resolve().parents[2]
for p in (_ROOT, _BRUIN_PL):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

load_dotenv(_ROOT / ".env")

import wyscout  # noqa: E402

from wyscout_dimension_scope import (  # noqa: E402
    season_ids_for_monitoring,
    team_row_from_api,
    teams_from_season_teams_payload,
)


def materialize():
    rows: list[dict] = []
    for season_id in season_ids_for_monitoring(_ROOT):
        payload = wyscout.get_teams_list_by_season(season_id, version="v3")
        if payload == -1:
            raise RuntimeError(
                f"get_teams_list_by_season failed for season_id={season_id}"
            )
        for t in teams_from_season_teams_payload(payload):
            row = team_row_from_api(t)
            if row:
                rows.append(row)

    if not rows:
        raise RuntimeError("No teams returned for the selected season(s).")

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["team_id"], keep="last")
    for col in ("gsm_id", "area_id"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df
