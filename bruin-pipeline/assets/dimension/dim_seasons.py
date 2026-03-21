"""@bruin
name: scouting_agent.dim_season
type: python
image: python:3.12
connection: gcp

depends:
  - scouting_agent.dim_competition

materialization:
  type: table
  strategy: merge

parameters:
  enforce_schema: true

columns:
  - name: season_id
    type: integer
    primary_key: true
  - name: competition_id
    type: integer
  - name: name
    type: string
  - name: start_date
    type: string
  - name: end_date
    type: string
  - name: active
    type: boolean
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
    active_from_season,
    bq_client,
    get_season_chain_cached,
    optional_season_id,
)


def _as_item_list(payload: object, keys: tuple[str, ...]) -> list:
    if payload == -1 or payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in keys:
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []


def _as_str(v: object) -> str | None:
    if v is None:
        return None
    return str(v)


def materialize():
    sid = optional_season_id()
    if sid is not None:
        _, _, season_row = get_season_chain_cached(wyscout, sid)
        df = pd.DataFrame([season_row])
        df["active"] = df["active"].astype("boolean")
        return df

    client = bq_client(_ROOT)
    project = client.project
    query = f"""
        SELECT DISTINCT competition_id
        FROM `{project}.scouting_agent.dim_competition`
        ORDER BY competition_id
    """
    job = client.query(query)
    comp_ids = [int(r[0]) for r in job.result()]
    if not comp_ids:
        raise RuntimeError("dim_competition is empty; run dim_competition first.")

    rows: list[dict] = []
    for comp_id in comp_ids:
        payload = wyscout.get_seasons_list(comp_id, version="v3")
        if payload == -1:
            raise RuntimeError(f"get_seasons_list failed for competition_id={comp_id}")
        seasons = _as_item_list(payload, ("seasons", "items", "data"))
        for s in seasons:
            if not isinstance(s, dict):
                continue
            sw = s.get("wyId")
            if sw is None:
                continue
            rows.append(
                {
                    "season_id": int(sw),
                    "competition_id": comp_id,
                    "name": s.get("name"),
                    "start_date": _as_str(s.get("startDate") or s.get("start_date")),
                    "end_date": _as_str(s.get("endDate") or s.get("end_date")),
                    "active": active_from_season(s),
                }
            )

    if not rows:
        raise RuntimeError("No seasons returned from Wyscout for any competition.")

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["season_id"], keep="first")
    df["active"] = df["active"].astype("boolean")
    return df
