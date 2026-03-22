"""
Possession qualifier helpers aligned with ``gold_match_possession`` (BigQuery).

Use this module for notebooks, tests, or LLM prep without importing ``possession_analyzer``.
Constants and flipped-x / third labels match the SQL definitions.

Notes:
- **pass_count**, **avg_pass_speed**, **time in thirds** (seconds + %), **third_start/end** use only
  rows where ``team_id = possession_team_id``. **ball_circulation_count** already did.
- **avg_pass_speed** in SQL uses Wyscout 0–100 grid distance per second (same unit as
  ``possession_analyzer.calculate_distance``), time = next event clock delta (minute/second).
- **ball_circulation_count** in SQL uses **raw** ``location_y`` (Wyscout), same as the analyzer:
  adjacent wide-lane transitions (y<33 vs y>66) on consecutive in-possession-team events;
  ``possession_analyzer.count_ball_circulation`` uses a state machine that can differ when the
  ball transits via the middle third.
- **team_leading_id** in SQL: ``home_team_id`` if home ahead, ``away_team_id`` if away ahead, ``0`` if
  draw; NULL if scores or dim unavailable. **possession_start_goal_diff** = home − away at possession
  start.
"""

from __future__ import annotations

import math
from typing import Literal

# Wyscout x on pitch, flipped to possession-team attacking perspective (see SQL).
THIRD_DEFENSIVE_MAX_X = 33.0
THIRD_ATTACKING_MIN_X = 66.0

ThirdLabel = Literal["defensive", "middle", "attacking"]


def flipped_x_for_third(
    possession_team_id: int | None,
    event_team_id: int | None,
    location_x: float | None,
) -> float | None:
    """
    x in possession-team frame: opponent events use 100 - x (same idea as flipping defensive
    team coords in ``possession_analyzer`` for thirds).
    """
    if location_x is None or possession_team_id is None or event_team_id is None:
        return float(location_x) if location_x is not None else None
    if int(event_team_id) == int(possession_team_id):
        return float(location_x)
    return float(100.0 - float(location_x))


def third_from_flipped_x(fx: float | None) -> ThirdLabel | None:
    if fx is None:
        return None
    if fx < THIRD_DEFENSIVE_MAX_X:
        return "defensive"
    if fx <= THIRD_ATTACKING_MIN_X:
        return "middle"
    return "attacking"


def pass_grid_distance_meters_approx(
    x0: float, y0: float, x1: float, y1: float, pitch_length_m: float = 105.0
) -> float:
    """Map 0–100 deltas to ~meters along axes (same scaling on x and y as typical hack)."""
    dx = (float(x1) - float(x0)) / 100.0 * pitch_length_m
    dy = (float(y1) - float(y0)) / 100.0 * 68.0
    return math.hypot(dx, dy)


def ball_circulation_state_machine(ys: list[float]) -> int:
    """
    Port of ``possession_analyzer.count_ball_circulation`` for a pre-filtered y sequence
    (e.g. only team-in-possession events, in time order).
    """
    circulation_count = 0
    in_left = False
    in_right = False
    for y in ys:
        if y < 33:
            if in_right:
                circulation_count += 1
                in_right = False
            in_left = True
        elif y > 66:
            if in_left:
                circulation_count += 1
                in_left = False
            in_right = True
    return circulation_count
