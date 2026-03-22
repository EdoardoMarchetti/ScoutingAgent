"""
Overlay ``gold_match_possession`` scalars onto ``analyze_possession`` output so LLM
``metrics_text`` matches warehouse gold while event lists stay from the analyzer.
"""

from __future__ import annotations

from typing import Any, Mapping


def _f(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _i(x: Any) -> int | None:
    if x is None:
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def overlay_gold_match_possession(
    analysis: Mapping[str, Any],
    gold: Mapping[str, Any] | None,
    *,
    leading_team_name: str | None,
) -> dict[str, Any]:
    """
    Return a copy of ``analysis`` with pass/duration/thirds/match_state aligned to gold when present.
    """
    out = dict(analysis)
    if not gold:
        return out

    pc = _i(gold.get("pass_count"))
    if pc is not None:
        out["pass_count"] = pc
    aps = _f(gold.get("avg_pass_speed"))
    if aps is not None:
        out["avg_pass_speed"] = aps
    bc = _i(gold.get("ball_circulation_count"))
    if bc is not None:
        out["ball_circulation_count"] = bc
    dur = _f(gold.get("duration"))
    if dur is not None:
        out["duration"] = dur

    d = _f(gold.get("pct_time_defensive_third"))
    m = _f(gold.get("pct_time_middle_third"))
    a = _f(gold.get("pct_time_attacking_third"))
    if d is not None or m is not None or a is not None:
        out["time_in_thirds"] = {
            "defensive": d if d is not None else 0.0,
            "middle": m if m is not None else 0.0,
            "attacking": a if a is not None else 0.0,
        }

    hs = _i(gold.get("possession_start_home_score"))
    aws = _i(gold.get("possession_start_away_score"))
    if hs is not None or aws is not None:
        out["match_state"] = {
            "home_score": hs if hs is not None else 0,
            "away_score": aws if aws is not None else 0,
            "leading_team": leading_team_name,
        }

    out["gold_possession"] = {
        "is_attack": gold.get("is_attack"),
        "is_counterattack": gold.get("is_counterattack"),
        "transition": gold.get("transition"),
        "set_piece": gold.get("set_piece"),
        "attack_with_shot": gold.get("attack_with_shot"),
        "attack_with_shot_on_goal": gold.get("attack_with_shot_on_goal"),
        "attack_with_goal": gold.get("attack_with_goal"),
        "attack_flank": gold.get("attack_flank"),
        "attack_xg": _f(gold.get("attack_xg")),
        "third_start": gold.get("third_start"),
        "third_end": gold.get("third_end"),
        "time_defensive_third_sec": _f(gold.get("time_defensive_third_sec")),
        "time_middle_third_sec": _f(gold.get("time_middle_third_sec")),
        "time_attacking_third_sec": _f(gold.get("time_attacking_third_sec")),
    }

    return out
