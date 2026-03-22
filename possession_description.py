"""
Build LLM prompts for natural-language possession descriptions from ``analyze_possession`` output.

Event lines in the prompt use a slim header (time, type, team, player, location,
``pitch_zone`` / ``pass_end_pitch_zone`` from the 3×3 grid) plus **only non-null Wyscout subtrees**
— nested objects (API) or parsed silver ``*_payload`` JSON strings — not flattened gold columns.
Coordinate flip into the possessing-team view is applied here (from raw ``possession_events`` /
``preceding_events`` on the analysis dict), not in ``analyze_possession``.
Textual ``pitch_zone`` / ``pass_end_pitch_zone`` follow the 3×3 grid: for the **possessing** team
they match that frame; for **opponent** actors they use that team’s attack perspective (inverted
thirds/laterals relative to the same numeric coordinates).

Templates live under ``prompts/`` (YAML with a ``prompt`` field using Jinja2 ``{{ ... }}`` placeholders).
Custom YAML files must include ``{{ output_format_constraints }}`` (or pass a compatible placeholder) because renders inject ``POSSESSION_LLM_OUTPUT_FORMAT_CONSTRAINTS``.
The player-section template also expects ``{{ metrics_text }}``, ``{{ preceding_events_section }}``, and ``{{ events_text }}`` like the general template.
Section 1 (general) and Section 2 (target player) use separate YAML files; use
``render_player_section_possession_prompt`` / ``generate_possession_player_section`` for the latter.
``generate_possession_descriptions_pipeline`` runs general + all involved players and returns row dicts
(``description_type`` ``general`` / ``player``) for warehousing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from jinja2 import Environment, BaseLoader, StrictUndefined

from possession_analyzer import _same_team_id, flip_coordinates_for_defensive_team

_DEFAULT_PROMPT_REL = Path("prompts") / "possession_description_general_v1.yaml"
_DEFAULT_PLAYER_SECTION_PROMPT_REL = (
    Path("prompts") / "possession_description_player_section_v1.yaml"
)

# Injected into all possession LLM prompts so Section 1/2 stay one plain paragraph every time.
POSSESSION_LLM_OUTPUT_FORMAT_CONSTRAINTS = """\
OUTPUT FORMAT (mandatory — use this structure every time):
- Line 1: only the required section header, exactly as specified (no text before it).
- Lines after that: exactly one continuous paragraph of plain prose. No blank lines inside the answer body.
- Do not use bullet lists, numbered lists, subheadings, markdown (e.g. **bold**, # headings, tables), or code fences.
- Cover all requested analytical points inside that single paragraph using connected sentences."""


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def load_prompt_yaml(
    path: Optional[Path] = None,
) -> tuple[str, Dict[str, Any]]:
    """
    Load ``prompt`` string and full YAML document from a possession prompt file.
    """
    p = path or (_repo_root() / _DEFAULT_PROMPT_REL)
    with open(p, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict) or "prompt" not in doc:
        raise ValueError(f"YAML at {p} must be a dict with a 'prompt' key")
    prompt = doc["prompt"]
    if not isinstance(prompt, str):
        raise TypeError("'prompt' must be a string")
    return prompt, doc


# Silver / pipeline string columns → Wyscout-like subtree keys in the prompt JSON
_PAYLOAD_STRING_FIELDS: tuple[tuple[str, str], ...] = (
    ("pass_payload", "pass"),
    ("shot_payload", "shot"),
    ("ground_duel_payload", "groundDuel"),
    ("aerial_duel_payload", "aerialDuel"),
    ("infraction_payload", "infraction"),
    ("carry_payload", "carry"),
)

_WYSCOUT_PAYLOAD_KEYS = ("pass", "shot", "groundDuel", "aerialDuel", "infraction", "carry")


def _events_in_possessing_view_for_prompt(
    possession_analysis: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Possession + preceding event lists with locations in the team-in-possession frame.

    New analyses expose raw ``possession_events`` / ``preceding_events``; we flip here.
    Legacy analyses (only ``flipped_events`` / ``flipped_preceding_events``) are returned as-is.
    """
    opp = possession_analysis.get("opponent_team_id")
    if "possession_events" in possession_analysis:
        poss = possession_analysis.get("possession_events") or []
        prev = possession_analysis.get("preceding_events") or []
        if opp is None:
            return list(poss), list(prev)
        fe = (
            flip_coordinates_for_defensive_team(poss, opp)
            if poss
            else []
        )
        pe = (
            flip_coordinates_for_defensive_team(prev, opp)
            if prev
            else []
        )
        return fe, pe
    poss = possession_analysis.get("flipped_events") or []
    prev = (
        possession_analysis.get("flipped_preceding_events")
        or possession_analysis.get("preceding_events")
        or []
    )
    return list(poss), list(prev)


def _event_team_id(e: Dict[str, Any]) -> Any:
    team = e.get("team") if isinstance(e.get("team"), dict) else {}
    if team.get("id") is not None:
        return team.get("id")
    if e.get("teamId") is not None:
        return e.get("teamId")
    return e.get("team_id")


def _event_player_id(e: Dict[str, Any]) -> Any:
    pl = e.get("player") if isinstance(e.get("player"), dict) else {}
    if pl.get("id") is not None:
        return pl.get("id")
    return e.get("playerId")


def _same_player_id(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return False
    if a in (0, "0") or b in (0, "0"):
        return False
    return str(a) == str(b)


def pitch_zone_label_from_xy(x: float, y: float) -> str:
    """
    Text zone for the 3×3 juego-de-posición grid from the **team-in-possession** perspective
    (same thresholds as ``prompts/possession_description_general_v1.yaml``).
    """
    if x < 33:
        third = "Defensive third"
    elif x <= 66:
        third = "Middle third"
    else:
        third = "Attacking third"
    if y < 33:
        lateral = "left"
    elif y <= 66:
        lateral = "center"
    else:
        lateral = "right"
    return f"{third}, {lateral}"


def _xy_for_pitch_zone_text(
    x: Any,
    y: Any,
    *,
    event_team_id: Any,
    possession_team_id: Optional[Any],
    coordinates_in_possessing_view: bool,
) -> tuple[Optional[float], Optional[float]]:
    """
    (x, y) used only to build the textual 3×3 zone.

    Event ``location`` / pass end in the prompt is in **team-in-possession** space (after
    ``flip_coordinates_for_defensive_team``). For the **possessing** actor, zone text matches
    that frame. For the **opponent** actor, zone text uses **their** attack perspective
    (invert x and y in that same frame: their attacking third ↔ our defensive third).
    """
    if x is None or y is None:
        return None, None
    try:
        fx = float(x)
        fy = float(y)
    except (TypeError, ValueError):
        return None, None

    opp_actor = (
        possession_team_id is not None
        and event_team_id is not None
        and not _same_team_id(event_team_id, possession_team_id)
    )

    if coordinates_in_possessing_view:
        if opp_actor:
            return 100.0 - fx, 100.0 - fy
        return fx, fy

    # Raw Wyscout coordinates (rare): map opponent once into possessing space for zone text
    if opp_actor:
        fx = 100.0 - fx
        fy = 100.0 - fy
    return fx, fy


def _attach_pitch_zone_fields(
    out: Dict[str, Any],
    e: Dict[str, Any],
    *,
    possession_team_id: Optional[Any],
    coordinates_in_possessing_view: bool,
) -> None:
    tid = _event_team_id(e)
    loc = out.get("location")
    if isinstance(loc, dict):
        xr, yr = _xy_for_pitch_zone_text(
            loc.get("x"),
            loc.get("y"),
            event_team_id=tid,
            possession_team_id=possession_team_id,
            coordinates_in_possessing_view=coordinates_in_possessing_view,
        )
        if xr is not None and yr is not None:
            out["pitch_zone"] = pitch_zone_label_from_xy(xr, yr)
    pwd = out.get("pass")
    if isinstance(pwd, dict):
        end = pwd.get("endLocation")
        if isinstance(end, dict):
            xe, ye = _xy_for_pitch_zone_text(
                end.get("x"),
                end.get("y"),
                event_team_id=tid,
                possession_team_id=possession_team_id,
                coordinates_in_possessing_view=coordinates_in_possessing_view,
            )
            if xe is not None and ye is not None:
                out["pass_end_pitch_zone"] = pitch_zone_label_from_xy(xe, ye)


def _parsed_non_null_payloads(e: Dict[str, Any]) -> Dict[str, Any]:
    """Subtree dicts from nested Wyscout objects or from non-empty JSON ``*_payload`` strings."""
    out: Dict[str, Any] = {}
    for k in _WYSCOUT_PAYLOAD_KEYS:
        v = e.get(k)
        if isinstance(v, dict) and v:
            out[k] = v
    for src, dst in _PAYLOAD_STRING_FIELDS:
        if dst in out:
            continue
        raw = e.get(src)
        if not raw or not isinstance(raw, str) or len(raw.strip()) <= 2:
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj:
            out[dst] = obj
    return out


def slim_event_for_possession_prompt(
    e: Dict[str, Any],
    *,
    possession_team_id: Optional[Any] = None,
    coordinates_in_possessing_view: bool = True,
) -> Dict[str, Any]:
    """
    Minimal event line for the LLM: clock, type, actors, location, ``pitch_zone`` /
    ``pass_end_pitch_zone`` (3×3 grid, possessing-team wording), plus **only non-null**
    subtrees (``pass`` / ``shot`` / duels / …).

    With ``coordinates_in_possessing_view=True`` (default when building from
    ``render_general_possession_prompt``), opponent-actor zone strings use their own
    attacking perspective; numeric ``location`` stays in the possessing-team frame.
    Set ``False`` only for raw Wyscout coordinates without that flip.
    """
    t = e.get("type") if isinstance(e.get("type"), dict) else {}
    pl = e.get("player") if isinstance(e.get("player"), dict) else {}
    team = e.get("team") if isinstance(e.get("team"), dict) else {}
    type_primary = e.get("type_primary") if e.get("type_primary") is not None else t.get("primary")
    type_secondary = e.get("type_secondary")
    if type_secondary is None:
        ts = e.get("type_secondary_json")
        if isinstance(ts, str) and ts.strip():
            try:
                type_secondary = json.loads(ts)
            except json.JSONDecodeError:
                type_secondary = None
    if type_secondary is None:
        type_secondary = t.get("secondary")
    loc = e.get("location")
    if (not isinstance(loc, dict)) and e.get("location_x") is not None:
        loc = {"x": e.get("location_x"), "y": e.get("location_y")}
    out: Dict[str, Any] = {
        "id": e.get("id") if e.get("id") is not None else e.get("event_id"),
        "minute": e.get("minute"),
        "second": e.get("second"),
        "matchTimestamp": e.get("matchTimestamp")
        if e.get("matchTimestamp") is not None
        else e.get("match_timestamp"),
        "matchPeriod": e.get("matchPeriod")
        if e.get("matchPeriod") is not None
        else e.get("match_period"),
        "type_primary": type_primary,
        "type_secondary": type_secondary,
        "teamId": team.get("id") if team else (e.get("teamId") if e.get("teamId") is not None else e.get("team_id")),
        "playerId": pl.get("id") if pl else (e.get("playerId") if e.get("playerId") is not None else e.get("player_id")),
        "playerName": (pl.get("shortName") or pl.get("name")) if pl else e.get("playerName"),
        "location": loc if isinstance(loc, dict) else None,
    }
    out.update(_parsed_non_null_payloads(e))
    _attach_pitch_zone_fields(
        out,
        e,
        possession_team_id=possession_team_id,
        coordinates_in_possessing_view=coordinates_in_possessing_view,
    )
    return {k: v for k, v in out.items() if v is not None}


def _slim_wyscout_event(e: Dict[str, Any]) -> Dict[str, Any]:
    """Backward-compatible name for :func:`slim_event_for_possession_prompt` (default zone options)."""
    return slim_event_for_possession_prompt(e)


def build_allowed_entities_section(possession_analysis: Dict[str, Any]) -> str:
    """ALLOWED_ENTITIES block: teams + players grouped by team (from analysis)."""
    lines = [
        "ALLOWED_ENTITIES:",
        "Teams:",
    ]
    tid = possession_analysis.get("team_in_possession")
    tname = possession_analysis.get("team_in_possession_name") or f"team_{tid}"
    lines.append(f"  - {tname} (team id: {tid})")
    oid = possession_analysis.get("opponent_team_id")
    oname = possession_analysis.get("opponent_team_name")
    if oid is not None or oname:
        lines.append(
            f"  - {oname or f'team_{oid}'} (team id: {oid})"
        )

    players = [
        p for p in (possession_analysis.get("players_involved") or []) if isinstance(p, dict)
    ]
    lines.append("Players (by team):")
    if not players:
        lines.append("  (none)")
        return "\n".join(lines)

    possessing: List[Dict[str, Any]] = []
    opponent: List[Dict[str, Any]] = []
    unknown: List[Dict[str, Any]] = []
    for p in players:
        p_tid = p.get("team_id")
        if p_tid is None:
            unknown.append(p)
        elif tid is not None and _same_team_id(p_tid, tid):
            possessing.append(p)
        elif oid is not None and _same_team_id(p_tid, oid):
            opponent.append(p)
        else:
            unknown.append(p)

    def _emit_player_subsection(header: str, plist: List[Dict[str, Any]]) -> None:
        if not plist:
            return
        lines.append(f"  {header}:")
        for p in plist:
            pid = p.get("id")
            pname = p.get("name") or f"player_{pid}"
            lines.append(f"    - {pname} (player id: {pid})")

    _emit_player_subsection(f"{tname} (team id: {tid})", possessing)
    if oid is not None or oname:
        _emit_player_subsection(f"{oname or f'team_{oid}'} (team id: {oid})", opponent)
    _emit_player_subsection("Other / team not recorded", unknown)

    return "\n".join(lines)


def build_metrics_text(possession_analysis: Dict[str, Any]) -> str:
    """JSON block of scalar / structured metrics (no raw event lists)."""
    skip = {
        "possession_events",
        "preceding_events",
        "flipped_events",
        "flipped_preceding_events",
    }
    payload = {k: v for k, v in possession_analysis.items() if k not in skip}
    return json.dumps(payload, indent=2, ensure_ascii=False)


def build_preceding_events_section(possession_analysis: Dict[str, Any]) -> str:
    _, prev = _events_in_possessing_view_for_prompt(possession_analysis)
    if not prev:
        return ""
    # Analyzer stores preceding as most-recent-first; prompt lists chronological (oldest first)
    prev_chrono = list(reversed(prev))
    tid = possession_analysis.get("team_in_possession")
    slim = [
        slim_event_for_possession_prompt(
            e,
            possession_team_id=tid,
            coordinates_in_possessing_view=True,
        )
        if isinstance(e, dict)
        else e
        for e in prev_chrono
    ]
    body = json.dumps(slim, indent=2, ensure_ascii=False)
    return (
        "\n\nPRECEDING EVENTS (before possession start, oldest first; coordinates in possessing-team view):\n"
        f"{body}\n"
    )


def build_events_text(possession_analysis: Dict[str, Any]) -> str:
    fe, _ = _events_in_possessing_view_for_prompt(possession_analysis)
    tid = possession_analysis.get("team_in_possession")
    slim = [
        slim_event_for_possession_prompt(
            e,
            possession_team_id=tid,
            coordinates_in_possessing_view=True,
        )
        if isinstance(e, dict)
        else e
        for e in fe
    ]
    return json.dumps(slim, indent=2, ensure_ascii=False)


def _analysis_row_metadata(possession_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Common fields for pipeline output rows (BigQuery-friendly scalars where possible)."""
    meta: Dict[str, Any] = {}
    for key in (
        "possession_id",
        "team_in_possession",
        "team_in_possession_name",
        "opponent_team_id",
        "opponent_team_name",
        "num_events",
        "match_id",
        "season_id",
        "competition_id",
    ):
        if key in possession_analysis and possession_analysis[key] is not None:
            meta[key] = possession_analysis[key]
    tm = possession_analysis.get("temporal_moment")
    if isinstance(tm, dict):
        meta["temporal_moment"] = tm
    return meta


def unique_players_involved(
    possession_analysis: Dict[str, Any],
    *,
    player_ids: Optional[List[Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Distinct human players from ``players_involved`` (excludes Wyscout ball id ``0``).

    If ``player_ids`` is set, keep only those ids (string-normalized match).
    """
    allow = {str(x) for x in player_ids} if player_ids is not None else None
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for p in possession_analysis.get("players_involved") or []:
        if not isinstance(p, dict):
            continue
        pid = p.get("id")
        if pid is None or pid in (0, "0"):
            continue
        sk = str(pid)
        if sk in seen:
            continue
        if allow is not None and sk not in allow:
            continue
        seen.add(sk)
        out.append({"id": pid, "name": p.get("name")})
    return out


def _target_player_display_name(
    possession_analysis: Dict[str, Any],
    target_player_id: Any,
    *,
    override: Optional[str] = None,
) -> str:
    if override:
        return override
    for p in possession_analysis.get("players_involved") or []:
        if isinstance(p, dict) and _same_player_id(p.get("id"), target_player_id):
            return str(p.get("name") or f"player_{target_player_id}")
    return f"player_{target_player_id}"


def build_target_player_events_text(
    possession_analysis: Dict[str, Any],
    target_player_id: Any,
) -> str:
    """
    JSON list of slim events where ``target_player_id`` is the actor, in time order:
    preceding (oldest first) then possession events. Each object has ``phase`` =
    ``\"preceding\"`` | ``\"possession\"`` plus the usual slim fields.
    """
    fe, prev = _events_in_possessing_view_for_prompt(possession_analysis)
    prev_chrono = list(reversed(prev))
    tid = possession_analysis.get("team_in_possession")
    rows: List[Dict[str, Any]] = []

    for e in prev_chrono:
        if not isinstance(e, dict):
            continue
        if not _same_player_id(_event_player_id(e), target_player_id):
            continue
        slim = slim_event_for_possession_prompt(
            e,
            possession_team_id=tid,
            coordinates_in_possessing_view=True,
        )
        rows.append({"phase": "preceding", **slim})

    for e in fe:
        if not isinstance(e, dict):
            continue
        if not _same_player_id(_event_player_id(e), target_player_id):
            continue
        slim = slim_event_for_possession_prompt(
            e,
            possession_team_id=tid,
            coordinates_in_possessing_view=True,
        )
        rows.append({"phase": "possession", **slim})

    return json.dumps(rows, indent=2, ensure_ascii=False)


def render_general_possession_prompt(
    possession_analysis: Dict[str, Any],
    *,
    prompt_yaml_path: Optional[Path] = None,
) -> str:
    """Fill ``possession_description_general_v1.yaml`` (or custom path) with analysis context."""
    template_str, _ = load_prompt_yaml(prompt_yaml_path)
    env = Environment(loader=BaseLoader(), undefined=StrictUndefined, autoescape=False)
    tpl = env.from_string(template_str)
    return tpl.render(
        allowed_entities_section=build_allowed_entities_section(possession_analysis),
        metrics_text=build_metrics_text(possession_analysis),
        preceding_events_section=build_preceding_events_section(possession_analysis),
        events_text=build_events_text(possession_analysis),
        output_format_constraints=POSSESSION_LLM_OUTPUT_FORMAT_CONSTRAINTS,
    )


def render_player_section_possession_prompt(
    possession_analysis: Dict[str, Any],
    *,
    general_description: str,
    target_player_id: Any,
    target_player_name: Optional[str] = None,
    prompt_yaml_path: Optional[Path] = None,
) -> str:
    """
    Fill ``possession_description_player_section_v1.yaml`` (Section 2: target player).

    ``general_description`` is the model output from Section 1 (general possession analysis).
    """
    p = prompt_yaml_path or (_repo_root() / _DEFAULT_PLAYER_SECTION_PROMPT_REL)
    template_str, _ = load_prompt_yaml(p)
    env = Environment(loader=BaseLoader(), undefined=StrictUndefined, autoescape=False)
    tpl = env.from_string(template_str)
    return tpl.render(
        allowed_entities_section=build_allowed_entities_section(possession_analysis),
        metrics_text=build_metrics_text(possession_analysis),
        preceding_events_section=build_preceding_events_section(possession_analysis),
        events_text=build_events_text(possession_analysis),
        general_description=general_description,
        target_player_name=_target_player_display_name(
            possession_analysis,
            target_player_id,
            override=target_player_name,
        ),
        target_player_events_text=build_target_player_events_text(
            possession_analysis,
            target_player_id,
        ),
        output_format_constraints=POSSESSION_LLM_OUTPUT_FORMAT_CONSTRAINTS,
    )


def _vertex_generative_model_text(
    model: Any,
    prompt: str,
    *,
    generation_config: Optional[Any] = None,
) -> str:
    kwargs: Dict[str, Any] = {"contents": prompt}
    if generation_config is not None:
        kwargs["generation_config"] = generation_config
    resp = model.generate_content(**kwargs)
    if hasattr(resp, "text") and resp.text:
        return resp.text
    parts = []
    for c in getattr(resp, "candidates", []) or []:
        content = getattr(c, "content", None)
        if content is None:
            continue
        for p in getattr(content, "parts", []) or []:
            t = getattr(p, "text", None)
            if t:
                parts.append(t)
    return "\n".join(parts) if parts else ""


def generate_possession_description(
    model: Any,
    possession_analysis: Dict[str, Any],
    *,
    prompt_yaml_path: Optional[Path] = None,
    generation_config: Optional[Any] = None,
) -> str:
    """
    Render the general prompt and call a Vertex ``GenerativeModel``.

    ``model`` must be a ``vertexai.generative_models.GenerativeModel`` instance.
    ``generation_config`` optional ``GenerationConfig`` from the same module.
    """
    prompt = render_general_possession_prompt(
        possession_analysis,
        prompt_yaml_path=prompt_yaml_path,
    )
    return _vertex_generative_model_text(
        model, prompt, generation_config=generation_config
    )


def generate_possession_player_section(
    model: Any,
    possession_analysis: Dict[str, Any],
    general_description: str,
    target_player_id: Any,
    *,
    target_player_name: Optional[str] = None,
    prompt_yaml_path: Optional[Path] = None,
    generation_config: Optional[Any] = None,
) -> str:
    """
    Section 2 — target player impact. Pass ``general_description`` from Section 1 output.

    ``model`` must be a ``vertexai.generative_models.GenerativeModel`` instance.
    """
    prompt = render_player_section_possession_prompt(
        possession_analysis,
        general_description=general_description,
        target_player_id=target_player_id,
        target_player_name=target_player_name,
        prompt_yaml_path=prompt_yaml_path,
    )
    return _vertex_generative_model_text(
        model, prompt, generation_config=generation_config
    )


def generate_possession_descriptions_pipeline(
    model: Any,
    possession_analysis: Dict[str, Any],
    *,
    generation_config: Optional[Any] = None,
    general_prompt_yaml_path: Optional[Path] = None,
    player_prompt_yaml_path: Optional[Path] = None,
    general_description: Optional[str] = None,
    include_general_row: bool = True,
    player_ids: Optional[List[Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Full LLM pipeline: optional general (Section 1), then Section 2 for each involved player.

    Returns a list of dicts, each suitable as one warehouse row:

    - ``description_type``: ``\"general\"`` | ``\"player\"``
    - ``player_id`` / ``player_name``: ``null`` for general
    - ``description``: model text
    - plus metadata from the analysis (``possession_id``, teams, ``num_events``, …)

    If ``general_description`` is already known, pass it to skip the first Vertex call.
    If ``include_general_row`` is False, no general row is emitted (then ``general_description``
    must be provided for player prompts). ``player_ids`` optionally restricts which players
    get a Section 2 call (default: all in ``players_involved`` except id ``0``).
    """
    meta = _analysis_row_metadata(possession_analysis)
    rows: List[Dict[str, Any]] = []

    g_text: Optional[str] = general_description
    if include_general_row:
        if g_text is None:
            g_text = generate_possession_description(
                model,
                possession_analysis,
                prompt_yaml_path=general_prompt_yaml_path,
                generation_config=generation_config,
            )
        rows.append(
            {
                **meta,
                "description_type": "general",
                "player_id": None,
                "player_name": None,
                "description": g_text,
            }
        )
    else:
        if g_text is None:
            raise ValueError(
                "include_general_row=False requires general_description for player prompts"
            )

    players = unique_players_involved(possession_analysis, player_ids=player_ids)
    for pl in players:
        pid = pl["id"]
        pname = pl.get("name")
        p_text = generate_possession_player_section(
            model,
            possession_analysis,
            g_text,
            pid,
            target_player_name=pname if isinstance(pname, str) else None,
            prompt_yaml_path=player_prompt_yaml_path,
            generation_config=generation_config,
        )
        rows.append(
            {
                **meta,
                "description_type": "player",
                "player_id": pid,
                "player_name": pname,
                "description": p_text,
            }
        )

    return rows
