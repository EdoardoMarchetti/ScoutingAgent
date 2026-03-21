/* @bruin

name: scouting_agent.gold_match_duel_event
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
  - name: possession_team_id
    type: integer
  - name: source_gcs_uri
    type: string
  - name: duel_type
    type: string
  - name: duel_phase
    type: string
  - name: is_dribble
    type: boolean
  - name: ground_duel_type
    type: string
  - name: side
    type: string
  - name: take_on
    type: boolean
  - name: foul_suffered
    type: boolean
  - name: is_sliding_tackle
    type: boolean
  - name: is_dribbled_past_attempt
    type: boolean
  - name: is_duel_won
    type: boolean
  - name: opponent_player_id
    type: integer
  - name: player_height
    type: integer
  - name: opponent_player_height
    type: integer
  - name: related_duel_event_id
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
    possession_team_id,
    ground_duel_payload,
    aerial_duel_payload,
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
    SAFE.PARSE_JSON(NULLIF(TRIM(s.ground_duel_payload), '')) AS ground_json,
    SAFE.PARSE_JSON(NULLIF(TRIM(s.aerial_duel_payload), '')) AS aerial_json
  FROM silver AS s
),
typed AS (
  SELECT
    p.*,
    JSON_VALUE(p.ground_json, '$.duelType') AS ground_duel_type,
    JSON_VALUE(p.ground_json, '$.side') AS ground_duel_side,
    SAFE_CAST(JSON_VALUE(p.ground_json, '$.takeOn') AS BOOL) AS ground_duel_take_on,
    EXISTS (
      SELECT 1
      FROM UNNEST(p.sec) AS t
      WHERE t = 'foul_suffered'
    ) AS foul_suffered,
    EXISTS (
      SELECT 1
      FROM UNNEST(p.sec) AS t
      WHERE t = 'sliding_tackle'
    ) AS is_sliding_tackle,
    EXISTS (
      SELECT 1
      FROM UNNEST(p.sec) AS t
      WHERE t = 'dribbled_past_attempt'
    ) AS sec_dribbled_past_attempt,
    SAFE_CAST(JSON_VALUE(p.ground_json, '$.recoveredPossession') AS BOOL) AS gd_recovered_possession,
    SAFE_CAST(JSON_VALUE(p.ground_json, '$.stoppedProgress') AS BOOL) AS gd_stopped_progress,
    SAFE_CAST(JSON_VALUE(p.ground_json, '$.keptPossession') AS BOOL) AS gd_kept_possession,
    SAFE_CAST(JSON_VALUE(p.ground_json, '$.progressedWithBall') AS BOOL) AS gd_progressed_with_ball,
    SAFE_CAST(JSON_VALUE(p.ground_json, '$.relatedDuelId') AS INT64) AS gd_related_duel_id,
    SAFE_CAST(JSON_VALUE(p.ground_json, '$.opponent.id') AS INT64) AS gd_opponent_player_id,
    SAFE_CAST(JSON_VALUE(p.aerial_json, '$.relatedDuelId') AS INT64) AS ad_related_duel_id,
    SAFE_CAST(JSON_VALUE(p.aerial_json, '$.opponent.id') AS INT64) AS ad_opponent_player_id,
    SAFE_CAST(JSON_VALUE(p.aerial_json, '$.firstTouch') AS BOOL) AS ad_first_touch,
    SAFE_CAST(JSON_VALUE(p.aerial_json, '$.height') AS INT64) AS ad_player_height,
    SAFE_CAST(JSON_VALUE(p.aerial_json, '$.opponent.height') AS INT64) AS ad_opponent_height,
    (
      p.type_primary = 'duel'
      OR EXISTS (
        SELECT 1
        FROM UNNEST(p.sec) AS t
        WHERE t IN (
          'aerial_duel',
          'ground_duel',
          'loose_ball_duel',
          'defensive_duel',
          'offensive_duel',
          'dribble',
          'sliding_tackle',
          'dribbled_past_attempt'
        )
      )
      OR (
        p.ground_duel_payload IS NOT NULL
        AND LENGTH(TRIM(p.ground_duel_payload)) > 2
      )
      OR (
        p.aerial_duel_payload IS NOT NULL
        AND LENGTH(TRIM(p.aerial_duel_payload)) > 2
      )
    ) AS is_duel_event,
    CASE
      WHEN EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'aerial_duel')
        OR p.aerial_json IS NOT NULL
        THEN 'aerial_duel'
      WHEN EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'ground_duel')
        OR p.ground_json IS NOT NULL
        THEN 'ground_duel'
      WHEN EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'loose_ball_duel')
        THEN 'loose_ball_duel'
      WHEN EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'sliding_tackle')
        THEN 'ground_duel'
      WHEN EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'dribbled_past_attempt')
        THEN 'ground_duel'
      ELSE NULL
    END AS duel_type_raw
  FROM parsed AS p
),
computed AS (
  SELECT
    t.*,
    COALESCE(
      t.duel_type_raw,
      IF(t.is_duel_event AND t.ground_json IS NOT NULL, 'ground_duel', NULL),
      IF(t.foul_suffered, 'ground_duel', NULL),
      IF(t.is_sliding_tackle, 'ground_duel', NULL),
      IF(t.sec_dribbled_past_attempt, 'ground_duel', NULL)
    ) AS duel_type,
    (
      t.sec_dribbled_past_attempt
      OR (
        JSON_VALUE(t.ground_json, '$.duelType') = 'defensive_duel'
        AND t.ground_duel_take_on IS TRUE
      )
    ) AS is_dribbled_past_attempt,
    (
      EXISTS (SELECT 1 FROM UNNEST(t.sec) AS x WHERE x = 'dribble')
      OR JSON_VALUE(t.ground_json, '$.duelType') = 'dribble'
    ) AS is_dribble,
    CASE
      WHEN COALESCE(
        t.duel_type_raw,
        IF(t.is_duel_event AND t.ground_json IS NOT NULL, 'ground_duel', NULL)
      ) = 'aerial_duel'
        THEN
          CASE
            WHEN t.possession_team_id IS NULL THEN NULL
            WHEN t.possession_team_id = t.team_id THEN 'offensive'
            WHEN t.possession_team_id = t.opponent_team_id THEN 'defensive'
            ELSE NULL
          END
      WHEN COALESCE(
        t.duel_type_raw,
        IF(t.is_duel_event AND t.ground_json IS NOT NULL, 'ground_duel', NULL)
      ) IN ('ground_duel', 'loose_ball_duel')
        THEN
          CASE JSON_VALUE(t.ground_json, '$.duelType')
            WHEN 'defensive_duel' THEN 'defensive'
            WHEN 'offensive_duel' THEN 'offensive'
            WHEN 'dribble' THEN 'offensive'
            ELSE NULL
          END
      ELSE NULL
    END AS duel_phase,
    CASE
      WHEN t.foul_suffered THEN TRUE
      WHEN COALESCE(
        t.duel_type_raw,
        IF(t.is_duel_event AND t.ground_json IS NOT NULL, 'ground_duel', NULL)
      ) = 'aerial_duel' THEN t.ad_first_touch
      WHEN JSON_VALUE(t.ground_json, '$.duelType') = 'defensive_duel' THEN (
        COALESCE(t.gd_recovered_possession, FALSE)
        OR COALESCE(t.gd_stopped_progress, FALSE)
      )
      WHEN JSON_VALUE(t.ground_json, '$.duelType') IN ('offensive_duel', 'dribble') THEN (
        IF(
          t.gd_kept_possession IS NULL
          AND t.gd_progressed_with_ball IS NULL,
          NULL,
          COALESCE(t.gd_kept_possession, FALSE)
          OR COALESCE(t.gd_progressed_with_ball, FALSE)
        )
      )
      ELSE NULL
    END AS is_duel_won
  FROM typed AS t
)
SELECT
  c.match_id,
  c.event_id,
  c.season_id,
  c.competition_id,
  c.match_period,
  c.minute,
  c.second,
  c.match_timestamp,
  c.video_timestamp,
  c.related_event_id,
  c.type_primary,
  c.type_secondary_json,
  c.location_x,
  c.location_y,
  c.team_id,
  c.opponent_team_id,
  c.player_id,
  c.possession_id,
  c.possession_team_id,
  c.source_gcs_uri,
  c.duel_type,
  c.duel_phase,
  c.is_dribble,
  c.ground_duel_type,
  c.ground_duel_side AS side,
  c.ground_duel_take_on AS take_on,
  c.foul_suffered,
  c.is_sliding_tackle,
  c.is_dribbled_past_attempt,
  c.is_duel_won,
  COALESCE(c.gd_opponent_player_id, c.ad_opponent_player_id) AS opponent_player_id,
  c.ad_player_height AS player_height,
  c.ad_opponent_height AS opponent_player_height,
  COALESCE(c.gd_related_duel_id, c.ad_related_duel_id) AS related_duel_event_id
FROM computed AS c
WHERE c.is_duel_event
  AND c.duel_type IS NOT NULL
