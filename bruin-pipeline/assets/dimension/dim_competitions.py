"""@bruin
name: scouting_agent.dim_competition
type: python
image: python:3.12
connection: gcp

depends:
  - scouting_agent.dim_area

materialization:
  type: table
  strategy: merge

parameters:
  enforce_schema: true

columns:
  - name: competition_id
    type: integer
    primary_key: true
  - name: area_id
    type: integer
  - name: name
    type: string
  - name: format
    type: string
  - name: competition_type
    type: string
  - name: category
    type: string
  - name: gender
    type: string
  - name: division_level
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

from wyscout_dimension_scope import get_season_chain_cached, optional_season_id  # noqa: E402


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


def materialize():
    sid = optional_season_id()
    if sid is not None:
        _, comp, _ = get_season_chain_cached(wyscout, sid)
        df = pd.DataFrame([comp])
        df["division_level"] = pd.to_numeric(
            df["division_level"], errors="coerce"
        ).astype("Int64")
        return df

    areas = wyscout.get_areas(version="v3")
    if areas == -1 or not areas:
        raise RuntimeError("get_areas failed; check Wyscout credentials.")

    rows: list[dict] = []
    for a in areas:
        area_id = a.get("id")
        alpha3 = a.get("alpha3code")
        if area_id is None or not alpha3:
            continue

        payload = wyscout.get_competitions_list(areaId=str(alpha3), version="v3")
        comps = _as_item_list(payload, ("competitions", "items", "data"))
        if payload == -1:
            raise RuntimeError(
                f"get_competitions_list failed for area_id={area_id} alpha3={alpha3}"
            )

        for c in comps:
            if not isinstance(c, dict):
                continue
            cid = c.get("wyId")
            if cid is None:
                continue
            area_obj = c.get("area") or {}
            rows.append(
                {
                    "competition_id": int(cid),
                    "area_id": int(area_obj.get("id", area_id)),
                    "name": c.get("name"),
                    "format": c.get("format"),
                    "competition_type": c.get("type"),
                    "category": c.get("category"),
                    "gender": c.get("gender"),
                    "division_level": c.get("divisionLevel"),
                }
            )

    if not rows:
        raise RuntimeError("No competitions returned from Wyscout for any area.")

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["competition_id"], keep="first")
    df["division_level"] = pd.to_numeric(df["division_level"], errors="coerce").astype(
        "Int64"
    )
    return df
