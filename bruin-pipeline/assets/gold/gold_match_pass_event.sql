/* @bruin

name: scouting_agent.gold_match_pass_event
type: bq.sql
connection: gcp

depends:
  - scouting_agent.silver_match_event

materialization:
  type: table
  strategy: create+replace

columns:
  - name: match_id
    type: integer
    primary_key: true
  - name: event_id
    type: integer
    primary_key: true
  - name: season_id
    type: integer
  - name: competition_id
    type: integer
  - name: match_period
    type: string
  - name: minute
    type: integer
  - name: second
    type: integer
  - name: match_timestamp
    type: string
  - name: video_timestamp
    type: string
  - name: related_event_id
    type: integer
  - name: type_primary
    type: string
  - name: type_secondary_json
    type: string
  - name: location_x
    type: integer
  - name: location_y
    type: integer
  - name: team_id
    type: integer
  - name: opponent_team_id
    type: integer
  - name: player_id
    type: integer
  - name: possession_id
    type: integer
  - name: source_gcs_uri
    type: string
  - name: pass_signal_primary
    type: boolean
  - name: pass_signal_secondary
    type: boolean
  - name: pass_signal_payload
    type: boolean
  - name: pass_signal_payload_only
    type: boolean
  - name: pass_signal_type
    type: string
  - name: is_free_kick
    type: boolean
  - name: is_corner
    type: boolean
  - name: is_throw_in
    type: boolean
  - name: is_goal_kick
    type: boolean
  - name: is_free_kick_cross
    type: boolean
  - name: is_assist
    type: boolean
  - name: is_second_assist
    type: boolean
  - name: is_third_assist
    type: boolean
  - name: is_key_pass
    type: boolean
  - name: is_shot_assist
    type: boolean
  - name: is_progressive_pass
    type: boolean
  - name: is_through_pass
    type: boolean
  - name: is_pass_to_final_third
    type: boolean
  - name: is_pass_to_penalty_area
    type: boolean
  - name: is_forward_pass
    type: boolean
  - name: is_deep_completion
    type: boolean
  - name: is_deep_completed_cross
    type: boolean
  - name: is_cross
    type: boolean
  - name: is_cross_blocked
    type: boolean
  - name: is_long_pass
    type: boolean
  - name: is_short_or_medium_pass
    type: boolean
  - name: is_lateral_pass
    type: boolean
  - name: is_back_pass
    type: boolean
  - name: is_head_pass
    type: boolean
  - name: is_hand_pass
    type: boolean
  - name: is_smart_pass
    type: boolean
  - name: is_under_pressure
    type: boolean
  - name: is_linkup_play
    type: boolean
  - name: pass_accurate
    type: boolean
  - name: pass_length_m
    type: float
  - name: pass_angle_deg
    type: float
  - name: pass_height
    type: string
  - name: pass_end_x
    type: float
  - name: pass_end_y
    type: float
  - name: recipient_player_id
    type: integer

@bruin */

WITH silver AS (
  SELECT
    match_id,
    event_id,
    season_id,
    competition_id,
    match_period,
    minute,
    second,
    match_timestamp,
    video_timestamp,
    related_event_id,
    type_primary,
    type_secondary_json,
    location_x,
    location_y,
    team_id,
    opponent_team_id,
    player_id,
    possession_id,
    pass_payload,
    source_gcs_uri
  FROM `scouting_agent.silver_match_event`
),
parsed AS (
  SELECT
    s.*,
    IFNULL(
      JSON_VALUE_ARRAY(SAFE.PARSE_JSON(NULLIF(TRIM(s.type_secondary_json), ''))),
      ARRAY<STRING>[]
    ) AS sec,
    SAFE.PARSE_JSON(NULLIF(TRIM(s.pass_payload), '')) AS pass_json
  FROM silver AS s
),
flags AS (
  SELECT
    p.*,
    (p.type_primary = 'pass') AS pass_signal_primary,
    (
      EXISTS (
        SELECT 1
        FROM UNNEST(p.sec) AS tag
        WHERE tag = 'pass' OR ENDS_WITH(tag, '_pass')
      )
    ) AS pass_signal_secondary,
    (
      p.pass_payload IS NOT NULL
      AND LENGTH(TRIM(p.pass_payload)) > 2
    ) AS pass_signal_payload
  FROM parsed AS p
),
classified AS (
  SELECT
    f.*,
    (f.pass_signal_primary OR f.pass_signal_secondary OR f.pass_signal_payload) AS is_pass_event,
    (
      f.pass_signal_payload
      AND NOT (f.pass_signal_primary OR f.pass_signal_secondary)
    ) AS pass_signal_payload_only,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT part
        FROM UNNEST(
          [
            IF(f.pass_signal_primary, 'primary', NULL),
            IF(f.pass_signal_secondary, 'secondary', NULL),
            IF(f.pass_signal_payload, 'payload', NULL)
          ]
        ) AS part
        WHERE part IS NOT NULL
      ),
      '+'
    ) AS pass_signal_type
  FROM flags AS f
),
geom AS (
  SELECT
    c.*,
    SAFE_CAST(JSON_VALUE(c.pass_json, '$.endLocation.x') AS FLOAT64) AS pass_end_x,
    SAFE_CAST(JSON_VALUE(c.pass_json, '$.endLocation.y') AS FLOAT64) AS pass_end_y
  FROM classified AS c
)
SELECT
  g.match_id,
  g.event_id,
  g.season_id,
  g.competition_id,
  g.match_period,
  g.minute,
  g.second,
  g.match_timestamp,
  g.video_timestamp,
  g.related_event_id,
  g.type_primary,
  g.type_secondary_json,
  g.location_x,
  g.location_y,
  g.team_id,
  g.opponent_team_id,
  g.player_id,
  g.possession_id,
  g.source_gcs_uri,
  g.pass_signal_primary,
  g.pass_signal_secondary,
  g.pass_signal_payload,
  g.pass_signal_payload_only,
  g.pass_signal_type,
  (g.type_primary = 'free_kick') AS is_free_kick,
  (g.type_primary = 'corner') AS is_corner,
  (g.type_primary = 'throw_in') AS is_throw_in,
  (g.type_primary = 'goal_kick') AS is_goal_kick,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'free_kick_cross') AS is_free_kick_cross,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'assist') AS is_assist,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'second_assist') AS is_second_assist,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'third_assist') AS is_third_assist,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'key_pass') AS is_key_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'shot_assist') AS is_shot_assist,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'progressive_pass') AS is_progressive_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'through_pass') AS is_through_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'pass_to_final_third') AS is_pass_to_final_third,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'pass_to_penalty_area') AS is_pass_to_penalty_area,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'forward_pass') AS is_forward_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'deep_completion') AS is_deep_completion,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'deep_completed_cross') AS is_deep_completed_cross,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'cross') AS is_cross,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'cross_blocked') AS is_cross_blocked,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'long_pass') AS is_long_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'short_or_medium_pass') AS is_short_or_medium_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'lateral_pass') AS is_lateral_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'back_pass') AS is_back_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'head_pass') AS is_head_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'hand_pass') AS is_hand_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'smart_pass') AS is_smart_pass,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'under_pressure') AS is_under_pressure,
  EXISTS (SELECT 1 FROM UNNEST(g.sec) AS t WHERE t = 'linkup_play') AS is_linkup_play,
  SAFE_CAST(JSON_VALUE(g.pass_json, '$.accurate') AS BOOL) AS pass_accurate,
  SAFE_CAST(JSON_VALUE(g.pass_json, '$.length') AS FLOAT64) AS pass_length_m,
  SAFE_CAST(JSON_VALUE(g.pass_json, '$.angle') AS FLOAT64) AS pass_angle_deg,
  JSON_VALUE(g.pass_json, '$.height') AS pass_height,
  g.pass_end_x,
  g.pass_end_y,
  SAFE_CAST(JSON_VALUE(g.pass_json, '$.recipient.id') AS INT64) AS recipient_player_id
FROM geom AS g
WHERE g.is_pass_event
