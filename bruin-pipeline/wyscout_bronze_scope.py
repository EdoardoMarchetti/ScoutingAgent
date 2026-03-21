"""
Helpers for Wyscout bronze Bruin assets. Not a Bruin asset (no @bruin block).

When pipeline variable ``season_id`` is set (via ``bruin run --var season_id=524``),
assets use a minimal API chain: season details → competition details → area.
"""

from __future__ import annotations

import json
import os
from typing import Any


def pipeline_vars() -> dict[str, Any]:
    raw = os.environ.get("BRUIN_VARS")
    if not raw:
        return {}
    return json.loads(raw)


def optional_season_id() -> int | None:
    """
    Pipeline variable ``season_id``: unset or ``0`` = full extract.
    Set to a Wyscout season wyId (e.g. ``bruin run --var season_id=524``) for targeted mode.
    """
    v = pipeline_vars().get("season_id")
    if v is None or v == "" or v == 0:
        return None
    return int(v)


def _unwrap_season(payload: Any) -> dict | None:
    if payload == -1 or payload is None:
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("wyId") is not None:
        return payload
    for k in ("season", "data", "result", "payload"):
        inner = payload.get(k)
        if isinstance(inner, dict) and inner.get("wyId") is not None:
            return inner
    return None


def _competition_id_from_season(season: dict) -> int | None:
    cid = season.get("competitionId")
    if cid is not None:
        return int(cid)
    comp = season.get("competition")
    if isinstance(comp, dict):
        w = comp.get("wyId") or comp.get("id")
        if w is not None:
            return int(w)
    return None


def _unwrap_competition(payload: Any) -> dict | None:
    if payload == -1 or payload is None:
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("wyId") is not None:
        return payload
    for k in ("competition", "data", "result", "payload"):
        inner = payload.get(k)
        if isinstance(inner, dict) and inner.get("wyId") is not None:
            return inner
    return None


def area_row_from_api(a: dict) -> dict:
    return {
        "area_id": int(a["id"]),
        "name": a.get("name"),
        "alpha2_code": a.get("alpha2code"),
        "alpha3_code": a.get("alpha3code"),
    }


def competition_row_from_api(c: dict) -> dict:
    area_obj = c.get("area") or {}
    return {
        "competition_id": int(c["wyId"]),
        "area_id": int(area_obj["id"]),
        "name": c.get("name"),
        "format": c.get("format"),
        "competition_type": c.get("type"),
        "category": c.get("category"),
        "gender": c.get("gender"),
        "division_level": c.get("divisionLevel"),
    }


def season_row_from_api(
    s: dict, competition_id: int, explicit_season_id: int | None = None
) -> dict:
    sid = s.get("wyId")
    if sid is None and explicit_season_id is not None:
        sid = explicit_season_id
    if sid is None:
        raise RuntimeError("Season payload missing wyId")
    return {
        "season_id": int(sid),
        "competition_id": competition_id,
        "name": s.get("name"),
        "start_date": _str_or_none(s.get("startDate") or s.get("start_date")),
        "end_date": _str_or_none(s.get("endDate") or s.get("end_date")),
        "active": active_from_season(s),
    }


def active_from_season(s: dict) -> bool | None:
    v = s.get("active")
    if v is None:
        v = s.get("isActive")
    return _as_bool(v)


def _as_bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        low = v.lower()
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no"):
            return False
    return None


def _str_or_none(v: Any) -> str | None:
    if v is None:
        return None
    return str(v)


_CHAIN_CACHE: tuple[int, tuple[dict, dict, dict]] | None = None


def get_season_chain_cached(wyscout_mod: Any, season_id: int) -> tuple[dict, dict, dict]:
    """Same process may run area → competition → season; avoid duplicate API calls."""
    global _CHAIN_CACHE
    if _CHAIN_CACHE is not None and _CHAIN_CACHE[0] == season_id:
        return _CHAIN_CACHE[1]
    triple = load_single_season_chain(wyscout_mod, season_id)
    _CHAIN_CACHE = (season_id, triple)
    return triple


def load_single_season_chain(wyscout_mod: Any, season_id: int) -> tuple[dict, dict, dict]:
    """
    GET /seasons/{id} then GET /competitions/{id}.
    Returns (area_row, competition_row, season_row).
    """
    sp = wyscout_mod.get_season_details(season_id, version="v3")
    season = _unwrap_season(sp)
    if not season:
        raise RuntimeError(
            f"get_season_details failed or empty for season_id={season_id}"
        )
    cid = _competition_id_from_season(season)
    if cid is None:
        raise RuntimeError(
            f"Could not resolve competition id from season {season_id}. "
            "Expected competitionId or competition.wyId in API JSON."
        )

    cp = wyscout_mod.get_competition_details(cid, version="v3")
    comp = _unwrap_competition(cp)
    if not comp:
        raise RuntimeError(
            f"get_competition_details failed or empty for competition_id={cid}"
        )

    area = comp.get("area") or {}
    if area.get("id") is None:
        raise RuntimeError(
            f"Competition {cid} has no area in API response; cannot build bronze_area."
        )

    comp_wyid = int(comp["wyId"])
    return (
        area_row_from_api(area),
        competition_row_from_api(comp),
        season_row_from_api(season, comp_wyid, explicit_season_id=int(season_id)),
    )
