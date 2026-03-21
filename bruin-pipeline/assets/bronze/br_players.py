"""@bruin
name: scouting_agent.bronze_player
type: python
image: python:3.12
connection: gcp

depends:
  - scouting_agent.bronze_team

materialization:
  type: table
  strategy: merge

parameters:
  enforce_schema: true

columns:
  - name: player_id
    type: integer
    primary_key: true
  - name: first_name
    type: string
  - name: middle_name
    type: string
  - name: last_name
    type: string
  - name: short_name
    type: string
  - name: birth_date
    type: string
  - name: foot
    type: string
  - name: gender
    type: string
  - name: height
    type: integer
  - name: weight
    type: integer
  - name: status
    type: string
  - name: gsm_id
    type: integer
  - name: image_data_url
    type: string
  - name: current_team_id
    type: integer
  - name: current_national_team_id
    type: integer
  - name: role_code2
    type: string
  - name: role_code3
    type: string
  - name: role_name
    type: string
  - name: birth_area_id
    type: integer
  - name: passport_area_id
    type: integer
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

from wyscout_bronze_scope import player_row_from_api, season_ids_for_monitoring  # noqa: E402

_INT_COLS = (
    "height",
    "weight",
    "gsm_id",
    "current_team_id",
    "current_national_team_id",
    "birth_area_id",
    "passport_area_id",
)


def materialize():
    rows: list[dict] = []
    for season_id in season_ids_for_monitoring(_ROOT):
        pl = wyscout.get_players_list_by_season(season_id, version="v3")
        if pl == -1:
            raise RuntimeError(
                f"get_players_list_by_season failed for season_id={season_id}"
            )
        if not isinstance(pl, list):
            continue
        for p in pl:
            if not isinstance(p, dict):
                continue
            row = player_row_from_api(p)
            if row:
                rows.append(row)

    if not rows:
        raise RuntimeError("No players returned for the selected season(s).")

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["player_id"], keep="last")
    for col in _INT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df
