"""@bruin
name: scouting_agent.dim_match
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
  - name: match_id
    type: integer
    primary_key: true
  - name: season_id
    type: integer
  - name: competition_id
    type: integer
  - name: match_date_utc
    type: string
  - name: match_date_label
    type: string
  - name: round_id
    type: integer
  - name: status
    type: string
  - name: home_team_id
    type: integer
  - name: away_team_id
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

from wyscout_dimension_scope import (  # noqa: E402
    fixture_request_options,
    fixture_window_dates,
    match_row_from_wyscout,
    matches_from_season_fixtures_payload,
    season_ids_for_monitoring,
)


def materialize():
    season_ids = season_ids_for_monitoring(_ROOT)
    from_d, to_d = fixture_window_dates()
    details, fetch = fixture_request_options()

    rows: list[dict] = []
    for season_id in season_ids:
        payload = wyscout.get_season_fixtures(
            season_id,
            version="v3",
            from_date=from_d,
            to_date=to_d,
            fetch=fetch,
            details=details,
        )
        if payload == -1:
            raise RuntimeError(f"get_season_fixtures failed for season_id={season_id}")
        for m in matches_from_season_fixtures_payload(payload):
            row = match_row_from_wyscout(m, season_id)
            if row:
                rows.append(row)

    if not rows:
        raise RuntimeError(
            "No matches in fixtures response for the selected seasons and date window."
        )

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["match_id"], keep="first")
    for col in ("competition_id", "round_id", "home_team_id", "away_team_id"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df