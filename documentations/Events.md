# Wyscout Events Documentation

```
{
      "id": 533182244,
      "matchId": 2050345,
      "matchPeriod": "P",
      "minute": 125,
      "second": 45,
      "matchTimestamp": "02:05:45.456",
      "videoTimestamp": "8760.456735",
      "relatedEventId": null,
      "type": {
        "primary": "postmatch_penalty",
        "secondary": [

        ]
      },
      "location": {
        "x": 90,
        "y": 50
      },
      "team": {
        "id": 1841,
        "name": "Zenit",
        "formation": "4-1-3-2"
      },
      "opponentTeam": {
        "id": 1843,
        "name": "Lokomotiv Moskva",
        "formation": "4-4-1-1"
      },
      "player": {
        "id": 17686,
        "name": "D. Criscito",
        "position": "LB"
      },
      "pass": null,
      "shot": {
        "bodyPart": "left_foot",
        "isGoal": true,
        "onTarget": true,
        "goalZone": "gbr",
        "xg": null,
        "postShotXg": null,
        "goalkeeperActionId": 533182178,
        "goalkeeper": {
          "id": 8992,
          "name": "Guilherme",
          "reflexSave": false
        }
      },
      "groundDuel": null,
      "aerialDuel": null,
      "infraction": null,
      "carry": null,
      "possession": null
    },
    {
      "id": 533182178,
      "matchId": 2050345,
      "matchPeriod": "P",
      "minute": 125,
      "second": 45,
      "matchTimestamp": "02:05:45.970",
      "videoTimestamp": "8760.97048",
      "relatedEventId": 533182244,
      "type": {
        "primary": "postmatch_penalty_faced",
        "secondary": [
          "conceded_postmatch_penalty"
        ]
      },
      "location": {
        "x": 100,
        "y": 100
      },
      "team": {
        "id": 1843,
        "name": "Lokomotiv Moskva",
        "formation": "4-4-1-1"
      },
      "opponentTeam": {
        "id": 1841,
        "name": "Zenit",
        "formation": "4-1-3-2"
      },
      "player": {
        "id": 8992,
        "name": "Guilherme",
        "position": "GK"
      },
      "pass": null,
      "shot": null,
      "groundDuel": null,
      "aerialDuel": null,
      "infraction": null,
      "carry": null,
      "possession": null
    }
```

## Possessions

```python
{
	'possession': {'id': 2937950745,
   'duration': '35.777015',
   'types': [],
   'eventsNumber': 18,
   'eventIndex': 0,
   'startLocation': {'x': 50, 'y': 50},
   'endLocation': {'x': 96, 'y': 34},
   'team': {'id': 3166, 'name': 'Bologna', 'formation': '4-3-3'},
   'attack': None}},
}
```

Possessions types:

* *corner (A corner that is immediately followed by a goalkeeper save or a direct goal is considered a [Shot](https://dataglossary.wyscout.com/shot).),*
* *counterattack (A transition of the possession from the opponent team, where the team is transitioning quickly from defensive to attacking phase, trying to catch the opponent out of their defensive shape),*
* *set_piece_attack,*
* *attack,*
* *free_kick,*
* *direct_free_kick,*
* free_kick_cross,
* *penalty, throw_in,*
* *transition_low, startingLocatio x < 33*
* transition_medium, startingLocation 33 < x < 66
* transition_high, startingLocation x > 66

If possession type is attack then you have:

```
{
    "possession": {
      "attack": {
        "withShot": false,
        "withShotOnGoal": false,
        "withGoal": false,
        "flank": "center",
        "xg": 0
      }
    }
}
```

with flank that could be left, right, center.

The set piece hierarchy is the following:

```
set_piece_attack
	|
	|-free_kick
	|	|-stop
	|	|-direct_free_kick
	|	|-free_kick_cross
	|-penalty
	|-corner
```

## 

Pass

When an event is a shot there are additional qualifiers within the pass key

```json
{
    "shot": {
      "bodyPart": "left_foot",
      "onTarget": true,
      "isGoal": false,
      "goalZone": "br",
      "xg": 0.235,
      "postShotXg": 0.425,
      "goalkeeperActionId": 423860521,
      "goalkeeper": {
        "id": 21816,
        "name": "D. De Gea"
      }
    }
}
```

```json
{
    "pass": {
      "accurate": true,
      "recipient": {
        "id": 265366,
        "name": "W. Ndidi",
        "position": "DMF"
      },
      "endLocation": {
        "x": 44,
        "y": 51
      },
      "length": 6.34,
      "angle": 172,
      "height": "low"
    }
}
```

Possible values for height: low, high, blocked, null. Only present for crosses, free kick crosses, and long passes.

Only crosses can be blocked.

 Length is specified in meters, taking into account standard field dimensions.

For angle, 0° represents a perfect forward pass (straight line towards the goal). Passes to the right will have positive values (90° pass is a pass strictly to the right), to the left, negative (-90° pass is a pass strictly to the left). Straight back passes will have the angle of 180°. Angle is specified in degrees, taking into account standard field dimensions.

## Shot

When an event is a shot there are additional qualifiers within the shot key

```json
{
    "shot": {
      "bodyPart": "left_foot",
      "onTarget": true,
      "isGoal": false,
      "goalZone": "br",
      "xg": 0.235,
      "postShotXg": 0.425,
      "goalkeeperActionId": 423860521,
      "goalkeeper": {
        "id": 21816,
        "name": "D. De Gea"
      }
    }
}
```

Possible values for bodyPart:  *left_foot, right_foot, head_or_other* .

If xg > 0.2 is a big chance

goalZone:

* otl: out top left
* ot: out top
* otr: out top right
* ol: out left
* or: out right
* olb: out left bottom
* orb: out right bottom
* gtl: goal top left
* gt: goal top
* gtr: goal top roght
* gl: goal left
* gc: goal center
* gr: goal right
* glb: goal left bottom
* gb: goal bottom
* gbr: goal bottom right
* bc: blocked
* p*: post

![1774003901779](image/Events/1774003901779.png)

## Ground Duel

When an event is a ground duel there are additional qualifiers in the ground duel key:

```
{
    "groundDuel": {
      "opponent": {
        "id": 265366,
        "name": "W. Ndidi",
        "position": "DMF"
      },
      "duelType": "defensive_duel",
      "keptPossession": null,
      "progressedWithBall": null,
      "recoveredPossession": false,
      "stoppedProgress": true,
      "takeOn": false,
      "side": "right",
      "relatedDuelId": 601919939
    }
}
```

Possible duelType values: defensive_duel, dribble, offensive_duel.

### Offensive duel

* When the attacking player uses their ability and skill in an attempt to pass an opponent, this is also a [Dribble](https://dataglossary.wyscout.com/dribble).
* However, when the player in possession is required to protect the ball with his body, although this is an offensive duel, it is not a Dribble.
* Offensive duels can happen anywhere on the pitch, including inside a player’s own penalty area.
* An offensive duel is always paired to an [Defensive duel](https://dataglossary.wyscout.com/defensive_duel) from the player of another team
* When a player suffers a foul in a duel, the duel is always considered won.

#### Dribble

side: The side that the attacking player attempts to dribble past an opponent.

takeon:

* true: The **Take-on** dribble is a stricter attempt to dribble past the opposite player. The dribble definition includes both these types of dribbles.
* false (Space): The **Space** type is used when the attacking player dribbles past an opponent to create space for his next action.

### Defensive Duel

If the defensive player stopped the progression of the attacking player with the ball and didn't commit a foul, the defensive duel is considered won (and the linked offensive duel is considered lost)

Examples of a won defensive duel:

1. defending player dispossesses the attacker (recoveredPossession true)
2. defending player kicks the ball out
3. the attacker stays with the ball, but the defender forces him to go back (stoppedProgress true)

If a defending player lets the attacking player progress with the ball, it's considered a lost defensive duel (and a won **Offensive duel** for the player with the ball, see **Offensive duel** for more details).

#### `gold_match_duel_event` (pipeline)

Wyscout can tag duels with secondary types such as **`sliding_tackle`** and **`dribbled_past_attempt`** (see event type combinations in `event_types_documentation.md`). In the gold table:

- **`is_sliding_tackle`**: `true` when the secondary list contains `sliding_tackle`.
- **`is_dribbled_past_attempt`**: `true` when either the secondary list contains **`dribbled_past_attempt`**, or **`groundDuel.duelType` is `defensive_duel` and `takeOn` is `true`**. In the latter case, treat the row as the defender’s side of a duel where the ball carrier attempted a **take-on** (strict dribble past the defender); Wyscout does not always emit the `dribbled_past_attempt` secondary together with that payload, so the defensive + take-on rule aligns the flag with the same idea.

## Recovery & interception (gold layer)

`gold_match_recovery_interception_event` uses an **inclusive** rule (same `event_id` may also appear in pass/duel/clearance/shot gold): a row is kept if any of the following holds:

- **`is_interception`**: `type.primary` is `interception`, or the secondary list contains the tag `interception` (e.g. some shot events).
- **`is_recovery`**: `type.primary` is `ball_recovery`, or the secondary list contains the tag **`recovery`** (exact match, not `counterpressing_recovery`).
- **`is_counterpressing`**: secondary contains **`counterpressing_recovery`**.

`regain_signal_type` concatenates which signals fired (`interception`, `recovery`, `counterpressing_recovery` joined with `+`). **`is_opponent_half`** is `TRUE` when `location_x >= 50` on the Wyscout 0–100 pitch (same orientation assumption as elsewhere); if `location_x` is null, `FALSE`.

No Wyscout JSON payloads are expanded beyond `type_primary` / `type_secondary_json` and coordinates.

## Clearance (gold layer)

`gold_match_clearance_event` keeps rows where **`type.primary` is `clearance`** or the secondary list contains the tag **`clearance`** (defensive coding). Wyscout clearance secondaries documented in `event_types_documentation.md` include `carry`, `head_pass`, `loss`, `recovery`, `under_pressure`; the gold table exposes them as boolean columns **`is_secondary_*`** only on clearance-classified rows (so `under_pressure` on a normal pass does not appear here).

Many clearances carry a **`pass`** subtree (`endLocation`, `length`, `angle`, `accurate`, `recipient`, …); those fields are parsed when `pass_payload` is present. **`carry.progression`** is read from `carry_payload` when the clearance secondary includes `carry`.

## Infraction (gold layer)

`gold_match_infraction_event` keeps rows where **`type.primary` is `infraction`** or a non-empty **`infraction_payload`** is present on silver (defensive inclusion if Wyscout ever misaligns primary vs subtree).

- **`infraction_payload`**: raw JSON string from silver (Wyscout `infraction` object, camelCase keys).
- **`is_foul`**: `type.secondary` contains `foul`.
- **`yellow_card` / `red_card`**: from payload `yellowCard` / `redCard` (no duplicate secondary flags for cards).
- **`foul_type`**: payload `type` (e.g. `regular_foul`, `hand_foul`, `violent_foul`, … — see Wyscout glossary).
- **`opponent_player_id`**: payload `opponent.id` (player fouled or involved, per Wyscout).

## Touch in box (gold layer)

`gold_match_touch_in_box_event` lists silver rows whose **`type.secondary`** contains the tag **`touch_in_box`** only (Wyscout’s explicit tagging). **`is_touch_in_box`** is always `TRUE` on this table. No geometric fallback: for `x ≥ 84` / `y ∈ [20, 80]` on start or end location, see `event_types_documentation.md` (offensive penalty box). Payloads stay on `silver_match_event`—join on `(match_id, event_id)` if needed.

## Shot against (gold layer)

`gold_match_shot_against_event` keeps rows with **`type.primary` = `shot_against`** (portiere / squadra che subisce il tiro). Usa solo **`type.secondary`**: **`is_save`** (tag `save`), **`is_save_with_reflex`**, **`is_conceded_goal`**, e **`shot_against_signal_type`** (concatenazione non vuota). **Non** si legge **`shot_payload`**: il subtree `shot` su silver è per il tiro dal lato attaccante (`gold_match_shot_event`).

## Goalkeeper exit (gold layer)

`gold_match_goalkeeper_exit_event` keeps **`type.primary` = `goalkeeper_exit`**. Contesto evento e coordinate Wyscout (`location_x` / `location_y`) per altezza uscita; lo silver attuale non espone un payload dedicato per questo tipo—se Wyscout aggiunge un subtree in bronze, si può estendere silver e questa gold.

## Set piece (gold layer)

`gold_match_set_piece_event` unifica ripartenze piazzate con **`type.primary` IN (`free_kick`, `corner`, `throw_in`, `goal_kick`)**. Il tipo preciso resta in **`type_primary`**. Tag su `type.secondary` (vedi `event_types_documentation.md`): **`is_loss`**, **`is_shot_assist`**, **`is_free_kick_cross`** (tipico su `free_kick`). Pass/cross/shot restano su silver o altre gold; join su `(match_id, event_id)` se serve.

## Possession (silver + gold)

- **`silver_match_possession`**: una riga per **`(match_id, possession_id)`** da eventi match; ultimo evento che cita il possesso vince. Colonne: `team_id`, `events_number`, `event_index`, **`types_json`** (`possession.types`), **`attack_payload`** (JSON grezzo di `possession.attack` se presente). Nessun nome squadra/formazione (usa `dim_team`).
- **`gold_match_possession`**: stesso grain; **`opponent_team_id`** da `dim_match` (l’altra squadra rispetto a `team_id`); **`is_attack`** (tag `attack` in `types`, non l’oggetto), **`is_counterattack`**, **`transition`**, **`set_piece`** da `types_json`; **`attack_payload`** pass-through; da `attack` (camelCase Wyscout): **`attack_with_shot`**, **`attack_with_shot_on_goal`**, **`attack_with_goal`**, **`attack_flank`**, **`attack_xg`**. Qualificatori da eventi: pass, terzi, circolazione, **`possession_start_*_score`**, **`possession_start_goal_diff`** (casa − trasferta), **`team_leading_id`** (id squadra in vantaggio, `0` se pareggio). Join eventi: `silver_match_event.possession_id` = `gold_match_possession.possession_id` e stesso `match_id`. **Prima materializzazione:** `bruin run --full-refresh` su questo asset (strategia `delete+insert`: serve tabella esistente).

## Aerial Duel

When event is aerial duel.

```json
{ "aerialDuel": {
    "opponent": {
      "id": 441396,
      "name": "A. Davies",
      "position": "LB",
      "height": 181
    },
    "firstTouch": false,
    "height": 175,
    "relatedDuelId": 601919956
  }
}
```

* If there are more then two players competing for the ball at the same time, an aerial duel will be recorded for all opposing players.
* is offensive for the players who belong to the team in possession (see possession key)
* is defensive for the players who belong to the team without the possession (see possesison key)
* if the possession team is not set, the duel is neither defensive nor offensive
* An aerial duel is considered won in favour of the player who touches the ball first, no matter what happens next. An aerial duel that results in a foul is considered won in favour of the player who suffered a foul.

## Infraction

When event is foul, yellow_card or red_card.

```json
{
    "infraction": {
      "yellowCard": false,
      "redCard": false,
      "type": "regular_foul",
      "opponent": {
        "id": 419254,
        "name": "A. Hakimi",
        "position": "RWB"
      }
    }
}
```

Possible type values: [hand_foul, regular_foul, violent_foul, out_of_play_foul, protest_foul, time_lost_foul, simulation_foul, late_card_foul].

## Carry

When event is a carry with the ball.

```json
{
    "carry": {
      "progression": 21.896,
      "endLocation": {
        "x": 68,
        "y": 31
      }
    }
}
```

progression is the meters along the x axis of the pitch

### Carry (gold layer)

`gold_match_carry_event` includes rows where **`type.primary` is `carry`** or **`carry`** appears in **`type.secondary`** (e.g. pass/clearance/interception/duel with carry, or `acceleration` with secondary `carry`). Wyscout should attach a **`carry`** subtree in those cases; if `carry_payload` is missing, parsed metrics stay null.

- **`is_primary_carry` / `is_secondary_carry`**, **`carry_signal_type`**: how the row qualified (`primary_carry`, `secondary_carry`, or both joined with `+`).
- **`carry_payload`**: raw JSON from silver; **`carry_progression_m`**, **`carry_end_x` / `carry_end_y`** from `progression` and `endLocation` (Wyscout 0–100).
- **`is_acceleration`**: `type.primary` is `acceleration` or secondary contains **`acceleration`**.
- **`is_progressive_run`**: secondary contains **`progressive_run`**.
- **`is_under_pressure`**: secondary contains **`under_pressure`**.

Same `event_id` may appear in other gold tables (pass, clearance, …).

## Touch

An action (a **Pass** or a  **Touch** ) that happens in the opponent penalty area. Duels are excluded from this definition.

Two touches in box in the same attack will count as two separate actions.

No ground duels, aerial duels or fouls are considered touches in box.
