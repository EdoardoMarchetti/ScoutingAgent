"""@bruin
name: scouting_agent.dim_area
type: python
image: python:3.12
connection: gcp

materialization:
  type: table
  strategy: merge

parameters:
  enforce_schema: true

columns:
  - name: area_id
    type: integer
    primary_key: true
  - name: name
    type: string
  - name: alpha2_code
    type: string
  - name: alpha3_code
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

from wyscout_dimension_scope import get_season_chain_cached, optional_season_id  # noqa: E402


def materialize():
    sid = optional_season_id()
    if sid is not None:
        area, _, _ = get_season_chain_cached(wyscout, sid)
        return pd.DataFrame([area])

    data = wyscout.get_areas(version="v3")
    print(data)
    if data == -1 or not data:
        msg = (
            "Wyscout get_areas failed or empty. Set WYSCOUT_USERNAME and "
            "WYSCOUT_PASSWORD in .env at repo root (or in the environment)."
        )
        raise RuntimeError(msg)

    df = pd.DataFrame(data)
    df = df.rename(
        columns={
            "id": "area_id",
            "alpha2code": "alpha2_code",
            "alpha3code": "alpha3_code",
        }
    )
    return df[["area_id", "name", "alpha2_code", "alpha3_code"]]
