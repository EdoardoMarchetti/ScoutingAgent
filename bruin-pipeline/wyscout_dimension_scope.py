"""
Helpers for Wyscout dim/fact Bruin assets. Not a Bruin asset (no @bruin block).

When pipeline variable ``season_id`` is set (via ``bruin run --var season_id=524``),
assets use a minimal API chain: season details → competition details → area.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any


def pipeline_vars() -> dict[str, Any]:
    raw = os.environ.get("BRUIN_VARS")
    print(raw)
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
            f"Competition {cid} has no area in API response; cannot build dim_area."
        )

    comp_wyid = int(comp["wyId"])
    return (
        area_row_from_api(area),
        competition_row_from_api(comp),
        season_row_from_api(season, comp_wyid, explicit_season_id=int(season_id)),
    )


# --- BigQuery (shared by dimension assets) ---


def read_gcp_project_id(repo_root: Path) -> str:
    text = (repo_root / ".bruin.yml").read_text(encoding="utf-8")
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("project_id:"):
            return s.split(":", 1)[1].strip().strip("\"'")
    raise RuntimeError("project_id not found in .bruin.yml")


def bq_client(repo_root: Path):
    from google.cloud import bigquery
    from google.oauth2 import service_account

    key_path = repo_root / ".secrets" / "bruin-wyscout-elt.json"
    if not key_path.is_file():
        raise RuntimeError(f"Service account file not found: {key_path}")
    creds = service_account.Credentials.from_service_account_file(str(key_path))
    project = read_gcp_project_id(repo_root)
    return bigquery.Client(credentials=creds, project=project)


def gcs_storage_client(repo_root: Path):
    from google.cloud import storage
    from google.oauth2 import service_account

    key_path = repo_root / ".secrets" / "bruin-wyscout-elt.json"
    if not key_path.is_file():
        raise RuntimeError(f"Service account file not found: {key_path}")
    creds = service_account.Credentials.from_service_account_file(str(key_path))
    project = read_gcp_project_id(repo_root)
    return storage.Client(credentials=creds, project=project)


def wyscout_gcs_bucket_name() -> str:
    """Override with env ``WYSCOUT_GCS_BUCKET``."""
    return os.environ.get("WYSCOUT_GCS_BUCKET", "sport-data-campus-bronze")


def wyscout_gcs_base_prefix() -> str:
    """
    Optional prefix inside the bucket (no leading/trailing slashes).
    Default empty: blobs are ``competitionId=…/season_id=…/match_id=…/events.json``
    at bucket root. Set ``WYSCOUT_GCS_BASE_PREFIX=bronze/wyscout`` to mirror
    ``path_to_file`` in ``.bruin.yml``.
    """
    v = os.environ.get("WYSCOUT_GCS_BASE_PREFIX")
    if v is not None:
        return v.strip().strip("/")
    return ""


def match_events_gcs_blob_path(
    competition_id: int, season_id: int, match_id: int, base_prefix: str
) -> str:
    rel = (
        f"competitionId={competition_id}/season_id={season_id}/"
        f"match_id={match_id}/events.json"
    )
    if base_prefix:
        return f"{base_prefix}/{rel}"
    return rel


def upload_json_bytes_to_gcs(
    storage_client: Any,
    bucket_name: str,
    blob_name: str,
    body: bytes,
    content_type: str = "application/json",
) -> str:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(body, content_type=content_type)
    return f"gs://{bucket_name}/{blob_name}"


# --- Fixtures / matches (season fixtures API) ---


def fixture_window_dates() -> tuple[str | None, str | None]:
    def _norm_iso_date(raw: Any, *, source: str, key: str) -> str | None:
        if raw is None:
            return None
        val = str(raw).strip()
        if not val:
            return None
        if not _ISO_DATE.match(val):

            raise ValueError(
                f"Invalid date format for {source}:{key}={raw!r}. Expected YYYY-MM-DD."
            )
        return val

    v = pipeline_vars()
    fd = _norm_iso_date(v.get("match_from_date"), source="BRUIN_VARS", key="match_from_date")
    td = _norm_iso_date(v.get("match_to_date"), source="BRUIN_VARS", key="match_to_date")
    if fd is None:
        fd = _norm_iso_date(
            os.environ.get("BRUIN_START_DATE"), source="ENV", key="BRUIN_START_DATE"
        )
    if td is None:
        td = _norm_iso_date(
            os.environ.get("BRUIN_END_DATE"), source="ENV", key="BRUIN_END_DATE"
        )
    return (fd, td)


_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def bq_match_date_from_utc_string(column_expr: str) -> str:
    """BigQuery: STRING ``match_date_utc`` → DATE (UTC calendar day, best-effort parse)."""
    c = column_expr
    return (
        "DATE(COALESCE("
        f"SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM({c})), "
        f"SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', TRIM({c})), "
        f"TIMESTAMP(SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(TRIM({c}), 1, 10)))"
        "))"
    )


def match_date_window_predicate_sql(match_date_column: str) -> str:
    """
    When ``match_from_date`` and ``match_to_date`` (or ``BRUIN_START_DATE`` /
    ``BRUIN_END_DATE``) are both ``YYYY-MM-DD``, return ``AND ...`` so only rows
    in that inclusive window remain. Otherwise return ``""`` (no date filter).
    """
    fd, td = fixture_window_dates()
    if not fd or not td or not _ISO_DATE.match(fd) or not _ISO_DATE.match(td):
        return ""
    dexpr = bq_match_date_from_utc_string(match_date_column)
    return (
        f" AND {match_date_column} IS NOT NULL AND TRIM({match_date_column}) != ''"
        f" AND {dexpr} BETWEEN DATE('{fd}') AND DATE('{td}')"
    )


def fetch_match_keys_for_events(
    client: Any, project: str, season_ids: list[int]
) -> list[tuple[int, int, int]]:
    """
    Distinct ``(competition_id, season_id, match_id)`` from ``dim_match`` for the
    given seasons. If pipeline vars set both match window dates, restricts to
    ``match_date_utc`` in that inclusive range (same semantics as ``dim_match`` ingest).
    """
    if not season_ids:
        return []
    sid_list = ",".join(str(int(s)) for s in season_ids)
    window_sql = match_date_window_predicate_sql("match_date_utc")
    q = f"""
        SELECT DISTINCT competition_id, season_id, match_id
        FROM `{project}.scouting_agent.dim_match`
        WHERE competition_id IS NOT NULL
          AND season_id IN ({sid_list})
          {window_sql}
        ORDER BY season_id, match_id
    """
    out: list[tuple[int, int, int]] = []
    for row in client.query(q).result():
        cid, sid, mid = int(row[0]), int(row[1]), int(row[2])
        out.append((cid, sid, mid))
    return out


def fixture_request_options() -> tuple[str, str | None]:
    v = pipeline_vars()
    details = (v.get("fixture_details") or "matches").strip() or "matches"
    fetch = (v.get("fixture_fetch") or "").strip() or None
    return details, fetch


def matches_from_season_fixtures_payload(payload: Any) -> list[dict]:
    """Flatten fixture response to list of raw items (may be {matchId, match:{...}} or flat match dicts)."""
    if payload == -1 or payload is None:
        return []
    out: list[dict] = []
    seen: set[int] = set()

    def add(raw: dict) -> None:
        if not isinstance(raw, dict):
            return
        mid = _fixture_match_id(raw)
        if mid is None:
            return
        k = int(mid)
        if k in seen:
            return
        seen.add(k)
        out.append(raw)

    if isinstance(payload, list):
        for x in payload:
            add(x if isinstance(x, dict) else {})
        return out

    if isinstance(payload, dict):
        for key in ("matches", "fixtures", "data", "items"):
            lst = payload.get(key)
            if isinstance(lst, list):
                for x in lst:
                    if isinstance(x, dict):
                        add(x)
        for rnd in payload.get("rounds") or []:
            if isinstance(rnd, dict):
                for m in rnd.get("matches") or []:
                    if isinstance(m, dict):
                        add(m)
    return out


def _fixture_match_id(raw: dict) -> int | None:
    inner = raw.get("match") if isinstance(raw.get("match"), dict) else None
    src = inner if inner is not None else raw
    return _maybe_int(src.get("wyId") or raw.get("matchId") or src.get("matchId"))


def _maybe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _maybe_int_coord(v: Any) -> int | None:
    """Pitch x/y: integers in docs; API may send floats."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(round(float(v)))
        except (TypeError, ValueError):
            return None


def _home_away_from_teams_data(src: dict) -> tuple[int | None, int | None]:
    td = src.get("teamsData")
    if not isinstance(td, dict):
        return None, None
    home = away = None
    for t in td.values():
        if not isinstance(t, dict):
            continue
        side = str(t.get("side") or "").lower()
        tid = _maybe_int(t.get("teamId"))
        if tid is None:
            continue
        if side == "home":
            home = tid
        elif side == "away":
            away = tid
    return home, away


def _home_away_team_ids(m: dict) -> tuple[int | None, int | None]:
    """Legacy list shape: teams[] with side + team.wyId."""
    teams = m.get("teams")
    if isinstance(teams, list):
        home = away = None
        for t in teams:
            if not isinstance(t, dict):
                continue
            side = str(t.get("side") or "").lower()
            team_obj = t.get("team")
            tid = None
            if isinstance(team_obj, dict):
                tid = _maybe_int(team_obj.get("wyId") or team_obj.get("id"))
            if tid is None:
                tid = _maybe_int(t.get("wyId") or t.get("teamId"))
            if tid is None:
                continue
            if side == "home":
                home = tid
            elif side == "away":
                away = tid
        if home is not None or away is not None:
            return home, away
        if len(teams) >= 2:
            t0, t1 = teams[0], teams[1]
            h = _maybe_int(
                (t0.get("team") or t0).get("wyId")
                if isinstance(t0.get("team"), dict)
                else t0.get("wyId")
            )
            a = _maybe_int(
                (t1.get("team") or t1).get("wyId")
                if isinstance(t1.get("team"), dict)
                else t1.get("wyId")
            )
            return h, a
    ht = m.get("homeTeam") if isinstance(m.get("homeTeam"), dict) else {}
    at = m.get("awayTeam") if isinstance(m.get("awayTeam"), dict) else {}
    return _maybe_int(ht.get("wyId")), _maybe_int(at.get("wyId"))


def match_row_from_wyscout(m: dict, season_id: int) -> dict | None:
    """
    Supports Wyscout fixtures shape: {matchId, goals, match: {wyId, label, dateutc,
    status, competitionId, seasonId, roundId, teamsData: {id: {teamId, side}}}}
    and flatter match dicts.
    """
    inner = m.get("match") if isinstance(m.get("match"), dict) else None
    src = inner if inner is not None else m

    mid = _maybe_int(src.get("wyId") or m.get("matchId") or src.get("matchId"))
    if mid is None:
        return None

    comp = src.get("competition") if isinstance(src.get("competition"), dict) else {}
    cid = _maybe_int(comp.get("wyId") or comp.get("id")) or _maybe_int(
        src.get("competitionId") or m.get("competitionId")
    )

    status = src.get("status")
    if isinstance(status, dict):
        status = status.get("type") or status.get("name")
    status_str = str(status) if status is not None else None

    rnd = src.get("roundId")
    if rnd is None and isinstance(src.get("round"), dict):
        rnd = src["round"].get("wyId")

    home_id, away_id = _home_away_from_teams_data(src)
    if home_id is None and away_id is None:
        home_id, away_id = _home_away_team_ids(src)

    return {
        "match_id": int(mid),
        "season_id": int(season_id),
        "competition_id": cid,
        "match_date_utc": _str_or_none(src.get("dateutc")),
        "match_date_label": _str_or_none(src.get("label")),
        "round_id": _maybe_int(rnd),
        "status": status_str,
        "home_team_id": home_id,
        "away_team_id": away_id,
    }


def season_ids_for_monitoring(repo_root: Path) -> list[int]:
    """Same rule as fixtures: optional ``season_id`` var, else active rows in dim_season."""
    sid = optional_season_id()
    if sid is not None:
        return [sid]
    client = bq_client(repo_root)
    project = client.project
    q = f"""
        SELECT DISTINCT season_id
        FROM `{project}.scouting_agent.dim_season`
        WHERE active = TRUE
        ORDER BY season_id
    """
    rows = list(client.query(q).result())
    if not rows:
        raise RuntimeError(
            "No active seasons in dim_season; load seasons or set season_id."
        )
    return [int(r[0]) for r in rows]


def teams_from_season_teams_payload(payload: Any) -> list[dict]:
    if payload == -1 or payload is None:
        return []
    if not isinstance(payload, dict):
        return []
    teams = payload.get("teams")
    if not isinstance(teams, list):
        return []
    return [t for t in teams if isinstance(t, dict)]


def team_row_from_api(t: dict) -> dict | None:
    tid = _maybe_int(t.get("wyId"))
    if tid is None:
        return None
    area = t.get("area") if isinstance(t.get("area"), dict) else {}
    return {
        "team_id": int(tid),
        "name": t.get("name"),
        "official_name": t.get("officialName"),
        "type": t.get("type"),
        "category": t.get("category"),
        "gender": t.get("gender"),
        "city": t.get("city"),
        "gsm_id": _maybe_int(t.get("gsmId")),
        "image_data_url": t.get("imageDataURL"),
        "area_id": _maybe_int(area.get("id")),
        "area_name": area.get("name"),
        "area_alpha2_code": area.get("alpha2code"),
        "area_alpha3_code": area.get("alpha3code"),
    }


def player_row_from_api(p: dict) -> dict | None:
    pid = _maybe_int(p.get("wyId"))
    if pid is None:
        return None
    ba = p.get("birthArea") if isinstance(p.get("birthArea"), dict) else {}
    pa = p.get("passportArea") if isinstance(p.get("passportArea"), dict) else {}
    role = p.get("role") if isinstance(p.get("role"), dict) else {}
    return {
        "player_id": int(pid),
        "first_name": p.get("firstName"),
        "middle_name": p.get("middleName"),
        "last_name": p.get("lastName"),
        "short_name": p.get("shortName"),
        "birth_date": _str_or_none(p.get("birthDate")),
        "foot": p.get("foot"),
        "gender": p.get("gender"),
        "height": _maybe_int(p.get("height")),
        "weight": _maybe_int(p.get("weight")),
        "status": p.get("status"),
        "gsm_id": _maybe_int(p.get("gsmId")),
        "image_data_url": p.get("imageDataURL"),
        "current_team_id": _maybe_int(p.get("currentTeamId")),
        "current_national_team_id": _maybe_int(p.get("currentNationalTeamId")),
        "role_code2": role.get("code2"),
        "role_code3": role.get("code3"),
        "role_name": role.get("name"),
        "birth_area_id": _maybe_int(ba.get("id")),
        "passport_area_id": _maybe_int(pa.get("id")),
    }


def download_json_from_gcs_uri(storage_client: Any, gcs_uri: str) -> dict:
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got {gcs_uri!r}")
    rest = gcs_uri[5:]
    if "/" not in rest:
        raise ValueError(f"Invalid GCS URI: {gcs_uri!r}")
    bucket_name, blob_path = rest.split("/", 1)
    blob = storage_client.bucket(bucket_name).blob(blob_path)
    raw = blob.download_as_bytes()
    return json.loads(raw.decode("utf-8"))


def unpack_bronze_match_events_document(doc: dict) -> dict:
    """Envelope written by bronze: ``{ payload: <Wyscout API response> }`` or raw API dict."""
    p = doc.get("payload")
    if isinstance(p, dict):
        return p
    return doc


def events_list_from_wyscout_match_events_api(payload: dict) -> list[dict]:
    """
    Wyscout v3 match events: top-level ``{ "events": [...], "meta": ... }``.
    Fallback: nested ``elements[0].events`` (older samples / other endpoints).
    """
    evs = payload.get("events")
    if isinstance(evs, list):
        return [x for x in evs if isinstance(x, dict)]
    els = payload.get("elements")
    if not isinstance(els, list) or not els:
        return []
    block = els[0]
    if not isinstance(block, dict):
        return []
    evs = block.get("events")
    if not isinstance(evs, list):
        return []
    return [x for x in evs if isinstance(x, dict)]


def _optional_json_subtree(obj: Any) -> str | None:
    if isinstance(obj, dict) and obj:
        return json.dumps(obj, ensure_ascii=False)
    return None


def silver_match_event_row(
    *,
    match_id: int,
    season_id: int | None,
    competition_id: int | None,
    source_gcs_uri: str,
    e: dict,
) -> dict | None:
    """
    Flat silver row: time, type, location, FK-style ids (team / opponent / player) for dim join,
    JSON payloads per subtree. No denormalized names or formations (use dim_team, dim_player).
    ``possession_team_id``: Wyscout ``possession.team.id`` when the possession object is present
    (needed for aerial duel offensive/defensive vs team in possession).
    """
    eid = _maybe_int(e.get("id"))
    if eid is None:
        return None
    t = e.get("type") if isinstance(e.get("type"), dict) else {}
    primary = t.get("primary") or t.get("name")
    sec = t.get("secondary")
    sec_json: str | None
    if isinstance(sec, list):
        sec_json = json.dumps(sec, ensure_ascii=False)
    else:
        sec_json = None

    loc = e.get("location") if isinstance(e.get("location"), dict) else {}
    team = e.get("team") if isinstance(e.get("team"), dict) else {}
    opp = e.get("opponentTeam") if isinstance(e.get("opponentTeam"), dict) else {}
    pl = e.get("player") if isinstance(e.get("player"), dict) else {}

    poss = e.get("possession") if isinstance(e.get("possession"), dict) else {}
    possession_id = _maybe_int(poss.get("id")) if poss else None
    poss_team = poss.get("team") if isinstance(poss.get("team"), dict) else {}
    possession_team_id = _maybe_int(poss_team.get("id")) if poss_team else None

    return {
        "match_id": int(match_id),
        "event_id": int(eid),
        "season_id": season_id,
        "competition_id": competition_id,
        "match_period": _str_or_none(e.get("matchPeriod")),
        "minute": _maybe_int(e.get("minute")),
        "second": _maybe_int(e.get("second")),
        "match_timestamp": _str_or_none(e.get("matchTimestamp")),
        "video_timestamp": _str_or_none(e.get("videoTimestamp")),
        "related_event_id": _maybe_int(e.get("relatedEventId")),
        "type_primary": _str_or_none(primary),
        "type_secondary_json": sec_json,
        "location_x": _maybe_int_coord(loc.get("x")),
        "location_y": _maybe_int_coord(loc.get("y")),
        "team_id": _maybe_int(team.get("id")),
        "opponent_team_id": _maybe_int(opp.get("id")),
        "player_id": _maybe_int(pl.get("id")),
        "possession_id": possession_id,
        "possession_team_id": possession_team_id,
        "pass_payload": _optional_json_subtree(e.get("pass")),
        "shot_payload": _optional_json_subtree(e.get("shot")),
        "ground_duel_payload": _optional_json_subtree(e.get("groundDuel")),
        "aerial_duel_payload": _optional_json_subtree(e.get("aerialDuel")),
        "infraction_payload": _optional_json_subtree(e.get("infraction")),
        "carry_payload": _optional_json_subtree(e.get("carry")),
        "source_gcs_uri": source_gcs_uri,
    }


def analyzer_event_from_silver_match_event_row(
    r: dict[str, Any],
    *,
    player_display_name: str | None = None,
) -> dict[str, Any] | None:
    """
    Rebuild a Wyscout-shaped event dict from a ``silver_match_event`` row for
    :func:`possession_analyzer.extract_possessions` / :func:`possession_analyzer.analyze_possession`.
    """
    eid = _maybe_int(r.get("event_id"))
    if eid is None:
        return None

    sec: list[Any] | None = None
    sec_raw = r.get("type_secondary_json")
    if isinstance(sec_raw, str) and sec_raw.strip():
        try:
            parsed = json.loads(sec_raw)
            if isinstance(parsed, list):
                sec = parsed
        except json.JSONDecodeError:
            sec = None

    typ: dict[str, Any] = {}
    primary = r.get("type_primary")
    if primary:
        typ["primary"] = primary
    if sec is not None:
        typ["secondary"] = sec

    possession: dict[str, Any] = {}
    pid = _maybe_int(r.get("possession_id"))
    if pid is not None:
        possession["id"] = int(pid)
    ptid = _maybe_int(r.get("possession_team_id"))
    if ptid is not None:
        possession["team"] = {"id": int(ptid)}

    team: dict[str, Any] = {}
    tid = _maybe_int(r.get("team_id"))
    if tid is not None:
        team["id"] = int(tid)

    player: dict[str, Any] = {}
    plid = _maybe_int(r.get("player_id"))
    if plid is not None:
        player["id"] = int(plid)
        if player_display_name:
            player["shortName"] = player_display_name
            player["name"] = player_display_name

    loc: dict[str, Any] | None = None
    lx = _maybe_int_coord(r.get("location_x"))
    ly = _maybe_int_coord(r.get("location_y"))
    if lx is not None and ly is not None:
        loc = {"x": lx, "y": ly}

    ts = _str_or_none(r.get("match_timestamp")) or "00:00:00.000"
    out: dict[str, Any] = {
        "id": int(eid),
        "minute": r.get("minute"),
        "second": r.get("second"),
        "matchTimestamp": ts,
        "matchPeriod": r.get("match_period"),
        "type": typ,
        "team": team,
        "player": player,
        "possession": possession,
    }
    if loc is not None:
        out["location"] = loc

    for col, api_key in (
        ("pass_payload", "pass"),
        ("shot_payload", "shot"),
        ("ground_duel_payload", "groundDuel"),
        ("aerial_duel_payload", "aerialDuel"),
        ("infraction_payload", "infraction"),
        ("carry_payload", "carry"),
    ):
        raw = r.get(col)
        if not isinstance(raw, str) or not raw.strip():
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj:
            out[api_key] = obj

    return out


def silver_match_possession_row(
    *,
    match_id: int,
    season_id: int | None,
    competition_id: int | None,
    source_gcs_uri: str,
    e: dict,
) -> dict | None:
    """
    One possession snapshot from a single event (Wyscout ``possession`` on the event).
    ``duration`` is Wyscout ``possession.duration`` (seconds) when parseable.
    ``types_json`` and ``attack_payload`` are stored; qualifiers from ``types`` and fields
    from ``attack`` are computed in BigQuery (``gold_match_possession``).
    Omits team name / formation; use ``dim_team`` via ``team_id``.
    """
    poss = e.get("possession") if isinstance(e.get("possession"), dict) else None
    if not poss:
        return None
    pid = _maybe_int(poss.get("id"))
    if pid is None:
        return None
    team = poss.get("team") if isinstance(poss.get("team"), dict) else {}
    types_raw = poss.get("types")
    if isinstance(types_raw, list):
        types_json = json.dumps(types_raw, ensure_ascii=False)
    else:
        types_json = "[]"
    duration_sec: float | None = None
    raw_dur = poss.get("duration")
    if raw_dur is not None:
        try:
            duration_sec = float(raw_dur)
        except (TypeError, ValueError):
            pass
    return {
        "match_id": int(match_id),
        "possession_id": int(pid),
        "season_id": season_id,
        "competition_id": competition_id,
        "team_id": _maybe_int(team.get("id")),
        "events_number": _maybe_int(poss.get("eventsNumber")),
        "event_index": _maybe_int(poss.get("eventIndex")),
        "duration": duration_sec,
        "types_json": types_json,
        "attack_payload": _optional_json_subtree(poss.get("attack")),
        "source_gcs_uri": source_gcs_uri,
    }
