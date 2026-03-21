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

## Touch

An action (a **Pass** or a  **Touch** ) that happens in the opponent penalty area. Duels are excluded from this definition.

Two touches in box in the same attack will count as two separate actions.

No ground duels, aerial duels or fouls are considered touches in box.
