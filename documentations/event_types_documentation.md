# Wyscout Event Types and Subtypes Documentation

This document contains all unique combinations of event types and subtypes found in Wyscout API events.

**Source:** Analysis of 2 matches (5712262, 5712263) from season 191623  
**Total unique combinations:** 112  
**Total events analyzed:** 3,486

## Structure

Events have the following structure:
```json
{
  "type": {
    "primary": "pass",
    "secondary": ["lateral_pass", "loss", "short_or_medium_pass"]
  }
}
```

- `type.primary`: The main event type (string)
- `type.secondary`: List of subtypes (array of strings)

## All Type/Subtype Combinations

### acceleration
- `acceleration` → (no subtype)
- `acceleration` → `carry`
- `acceleration` → `progressive_run`

### clearance
- `clearance` → (no subtype)
- `clearance` → `carry`
- `clearance` → `head_pass`
- `clearance` → `loss`
- `clearance` → `recovery`
- `clearance` → `under_pressure`

### corner
- `corner` → (no subtype)
- `corner` → `loss`
- `corner` → `shot_assist`

### duel
- `duel` → (no subtype)
- `duel` → `aerial_duel`
- `duel` → `carry`
- `duel` → `counterpressing_recovery`
- `duel` → `defensive_duel`
- `duel` → `dribble`
- `duel` → `dribbled_past_attempt`
- `duel` → `foul_suffered`
- `duel` → `ground_duel`
- `duel` → `interception`
- `duel` → `linkup_play`
- `duel` → `loose_ball_duel`
- `duel` → `loss`
- `duel` → `offensive_duel`
- `duel` → `progressive_run`
- `duel` → `recovery`
- `duel` → `sliding_tackle`

### free_kick
- `free_kick` → (no subtype)
- `free_kick` → `free_kick_cross`
- `free_kick` → `shot_assist`

### game_interruption
- `game_interruption` → (no subtype)
- `game_interruption` → `ball_out`
- `game_interruption` → `whistle`

### goal_kick
- `goal_kick` → (no subtype)
- `goal_kick` → `loss`

### goalkeeper_exit
- `goalkeeper_exit` → (no subtype)

### infraction
- `infraction` → (no subtype)
- `infraction` → `foul`
- `infraction` → `yellow_card`

### interception
- `interception` → (no subtype)
- `interception` → `acceleration`
- `interception` → `back_pass`
- `interception` → `carry`
- `interception` → `counterpressing_recovery`
- `interception` → `forward_pass`
- `interception` → `hand_pass`
- `interception` → `head_pass`
- `interception` → `lateral_pass`
- `interception` → `long_pass`
- `interception` → `loss`
- `interception` → `pass`
- `interception` → `pass_to_final_third`
- `interception` → `progressive_pass`
- `interception` → `progressive_run`
- `interception` → `recovery`
- `interception` → `short_or_medium_pass`
- `interception` → `shot_block`
- `interception` → `touch_in_box`

### offside
- `offside` → (no subtype)

### pass
- `pass` → (no subtype)
- `pass` → `assist`
- `pass` → `back_pass`
- `pass` → `carry`
- `pass` → `counterpressing_recovery`
- `pass` → `cross`
- `pass` → `cross_blocked`
- `pass` → `deep_completed_cross`
- `pass` → `deep_completion`
- `pass` → `forward_pass`
- `pass` → `hand_pass`
- `pass` → `head_pass`
- `pass` → `key_pass`
- `pass` → `lateral_pass`
- `pass` → `linkup_play`
- `pass` → `long_pass`
- `pass` → `loss`
- `pass` → `pass_to_final_third`
- `pass` → `pass_to_penalty_area`
- `pass` → `progressive_pass`
- `pass` → `progressive_run`
- `pass` → `recovery`
- `pass` → `second_assist`
- `pass` → `short_or_medium_pass`
- `pass` → `shot_assist`
- `pass` → `smart_pass`
- `pass` → `third_assist`
- `pass` → `through_pass`
- `pass` → `touch_in_box`
- `pass` → `under_pressure`

### shot
- `shot` → (no subtype)
- `shot` → `goal`
- `shot` → `head_shot`
- `shot` → `interception`
- `shot` → `opportunity`
- `shot` → `shot_after_corner`
- `shot` → `shot_after_free_kick`
- `shot` → `shot_after_throw_in`
- `shot` → `touch_in_box`

### shot_against
- `shot_against` → (no subtype)
- `shot_against` → `conceded_goal`
- `shot_against` → `save`
- `shot_against` → `save_with_reflex`

### throw_in
- `throw_in` → (no subtype)
- `throw_in` → `loss`

### touch
- `touch` → (no subtype)
- `touch` → `carry`
- `touch` → `loss`
- `touch` → `opportunity`
- `touch` → `progressive_run`
- `touch` → `touch_in_box`

## Usage Examples

### Identifying a Goal
```python
event_type = event.get('type', {})
type_primary = event_type.get('primary', '')
type_secondary = event_type.get('secondary', [])

is_goal = type_primary == 'shot' and 'goal' in type_secondary
```

### Identifying a Pass
```python
event_type = event.get('type', {})
type_primary = event_type.get('primary', '')

is_pass = type_primary == 'pass'
```

### Checking Pass Subtypes
```python
event_type = event.get('type', {})
type_primary = event_type.get('primary', '')
type_secondary = event_type.get('secondary', [])

if type_primary == 'pass':
    is_assist = 'assist' in type_secondary
    is_key_pass = 'key_pass' in type_secondary
    is_progressive = 'progressive_pass' in type_secondary
```

## Notes

- Some events may have no subtypes (empty `secondary` list)
- Multiple subtypes can be present in the same event
- The `secondary` field is always a list, even if empty
- Goal detection: `type.primary == "shot"` AND `"goal" in type.secondary`

### Offensive penalty box logic (Wyscout 0–100)

In addition to relying on the explicit `touch_in_box` subtype, we define the **offensive penalty box** geometrically in the Wyscout 0–100 coordinate system.

- **Assumptions**
  - Coordinates are Wyscout-normalized: `x` from 0 (own goal line) to 100 (opponent goal line), `y` from 0 (bottom touchline) to 100 (top touchline), always oriented in the **attacking direction of the team in possession**.
  - The real pitch is 105m × 68m and the penalty area is 16.5m deep and 40.3m wide, centered on the goal.
  - We approximate these dimensions on the 0–100 grid:
    - Depth: \(16.5 / 105 \approx 0.157\) ⇒ **x in \[84.0, 100.0\]** for the offensive box.
    - Width: \((68 - 40.3) / 2 \approx 13.8\)m from each sideline ⇒ \(13.8 / 68 \approx 0.203\) ⇒ **y in \[20.0, 80.0\]**.

- **Geometric definition**
  - A point \((x, y)\) is inside the **offensive penalty box** iff:
    - \(x \ge 84.0\)
    - \(20.0 \le y \le 80.0\)

- **Canonical boolean for touches in offensive box (Python, on raw Wyscout events)**

```python
PEN_BOX_X_MIN = 84.0
PEN_BOX_Y_MIN = 20.0
PEN_BOX_Y_MAX = 80.0


def _in_offensive_box(x: float, y: float) -> bool:
    return (
        x is not None
        and y is not None
        and x >= PEN_BOX_X_MIN
        and PEN_BOX_Y_MIN <= y <= PEN_BOX_Y_MAX
    )


def is_offensive_touch_in_box(event: dict) -> bool:
    """
    Returns True if the event is a touch in the offensive penalty area
    for the team in possession, using both the 'touch_in_box' subtype
    and geometric conditions on Wyscout 0–100 coordinates.
    """
    ev_type = (event.get("type") or {})
    primary = ev_type.get("primary") or ""
    secondary = ev_type.get("secondary") or []

    # 1) Trust Wyscout tagging when available
    if "touch_in_box" in secondary:
        return True

    # 2) Fallback: derive from coordinates
    loc = (event.get("location") or {})  # Wyscout top-level location
    x_start = loc.get("x")
    y_start = loc.get("y")

    # Prefer endLocation when present (receptions, passes, carries)
    end_loc = None
    if primary in {"pass", "clearance"}:
        end_loc = (event.get("pass") or {}).get("endLocation") or {}
    elif primary == "shot":
        # Some feeds use shot.location/endLocation
        end_loc = (event.get("shot") or {}).get("endLocation") or {}
    elif primary in {"duel", "touch", "interception", "ball_recovery"}:
        end_loc = (event.get("carry") or {}).get("endLocation") or {}

    x_end = (end_loc or {}).get("x")
    y_end = (end_loc or {}).get("y")

    # Use end location when available, otherwise start location
    if x_end is not None and y_end is not None:
        return _in_offensive_box(x_end, y_end)

    if x_start is not None and y_start is not None:
        return _in_offensive_box(x_start, y_start)

    return False
```

- **Canonical boolean for touches in offensive box (BigQuery SQL, on `fact_event`)**

When building `fact_event`, we recommend computing `is_touch_in_box` with:

```sql
EXISTS (
  SELECT 1
  FROM UNNEST(event_type_secondary) AS s
  WHERE s = 'touch_in_box'
)
OR (
  event_type_primary IN (
    'pass',
    'shot',
    'duel',
    'interception',
    'ball_recovery',
    'touch',
    'clearance',
    'free_kick',
    'corner',
    'throw_in',
    'goal_kick',
    'goalkeeper_exit'
  )
  AND (
    -- Prefer end location when available (receptions, carries, passes)
    (
      end_location_x IS NOT NULL
      AND end_location_y IS NOT NULL
      AND end_location_x >= 84.0
      AND end_location_y BETWEEN 20.0 AND 80.0
    )
    OR (
      (end_location_x IS NULL OR end_location_y IS NULL)
      AND location_x IS NOT NULL
      AND location_y IS NOT NULL
      AND location_x >= 84.0
      AND location_y BETWEEN 20.0 AND 80.0
    )
  )
)
AS is_touch_in_box
```

This definition combines Wyscout’s `touch_in_box` subtype with a robust geometric fallback on (x, y) in the Wyscout 0–100 system to identify offensive penalty-area touches.
