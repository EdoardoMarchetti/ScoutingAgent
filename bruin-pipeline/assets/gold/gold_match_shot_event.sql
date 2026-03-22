/* @bruin

name: scouting_agent.gold_match_shot_event
type: bq.sql
connection: gcp

depends:
  - scouting_agent.silver_match_event
  - scouting_agent.dim_match
  - scouting_agent.dim_season

materialization:
  type: table
  strategy: delete+insert
  incremental_key: match_id

columns:
  - name: match_id
    type: int64
    primary_key: true
  - name: event_id
    type: int64
    primary_key: true
  - name: season_id
    type: int64
  - name: competition_id
    type: int64
  - name: match_period
    type: string
  - name: minute
    type: int64
  - name: second
    type: int64
  - name: match_timestamp
    type: string
  - name: video_timestamp
    type: string
  - name: related_event_id
    type: int64
  - name: type_primary
    type: string
  - name: type_secondary_json
    type: string
  - name: location_x
    type: int64
  - name: location_y
    type: int64
  - name: team_id
    type: int64
  - name: opponent_team_id
    type: int64
  - name: player_id
    type: int64
  - name: possession_id
    type: int64
  - name: source_gcs_uri
    type: string
  - name: shot_signal_primary
    type: bool
  - name: shot_signal_secondary
    type: bool
  - name: shot_signal_payload
    type: bool
  - name: shot_signal_payload_only
    type: bool
  - name: shot_signal_type
    type: string
  - name: is_head_shot
    type: bool
  - name: is_opportunity
    type: bool
  - name: is_shot_after_corner
    type: bool
  - name: is_shot_after_free_kick
    type: bool
  - name: is_shot_after_throw_in
    type: bool
  - name: is_free_kick
    type: bool
  - name: is_corner
    type: bool
  - name: is_throw_in
    type: bool
  - name: is_goal_kick
    type: bool
  - name: shot_body_part
    type: string
  - name: shot_is_goal
    type: bool
  - name: shot_goal_zone
    type: string
  - name: shot_goal_zone_label
    type: string
  - name: shot_outcome
    type: string
  - name: shot_xg
    type: float64
  - name: shot_post_shot_xg
    type: float64
  - name: is_big_chance_xg
    type: bool
  - name: goalkeeper_action_event_id
    type: int64
  - name: goalkeeper_player_id
    type: int64
  - name: goalkeeper_reflex_save
    type: bool

@bruin */

WITH silver AS (
  SELECT
    se.match_id,
    se.event_id,
    se.season_id,
    se.competition_id,
    se.match_period,
    se.minute,
    se.second,
    se.match_timestamp,
    se.video_timestamp,
    se.related_event_id,
    se.type_primary,
    se.type_secondary_json,
    se.location_x,
    se.location_y,
    se.team_id,
    se.opponent_team_id,
    se.player_id,
    se.possession_id,
    se.shot_payload,
    se.source_gcs_uri
  FROM `scouting_agent.silver_match_event` AS se
  {% if not full_refresh %}
  INNER JOIN (
    SELECT DISTINCT m.match_id
    FROM `scouting_agent.dim_match` AS m
    WHERE m.competition_id IS NOT NULL
    {% if var.season_id|default(0)|int %}
    AND m.season_id = {{ var.season_id|int }}
    {% else %}
    AND m.season_id IN (
      SELECT season_id
      FROM `scouting_agent.dim_season`
      WHERE active IS TRUE
    )
    {% endif %}
    {% if var.match_from_date and var.match_to_date %}
    AND m.match_date_utc IS NOT NULL
    AND TRIM(m.match_date_utc) != ''
    AND DATE(
      COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(m.match_date_utc)),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', TRIM(m.match_date_utc)),
        TIMESTAMP(SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(TRIM(m.match_date_utc), 1, 10)))
      )
    ) BETWEEN DATE('{{ var.match_from_date }}') AND DATE('{{ var.match_to_date }}')
    {% endif %}
  ) AS _gold_scope
    ON se.match_id = _gold_scope.match_id
  {% endif %}
),
parsed AS (
  SELECT
    s.*,
    IFNULL(
      JSON_VALUE_ARRAY(SAFE.PARSE_JSON(NULLIF(TRIM(s.type_secondary_json), ''))),
      ARRAY<STRING>[]
    ) AS sec,
    SAFE.PARSE_JSON(NULLIF(TRIM(s.shot_payload), '')) AS shot_json
  FROM silver AS s
),
flags AS (
  SELECT
    p.*,
    (p.type_primary = 'shot') AS shot_signal_primary,
    (
      EXISTS (
        SELECT 1
        FROM UNNEST(p.sec) AS tag
        WHERE
          tag = 'shot'
          OR ENDS_WITH(tag, '_shot')
          OR tag IN (
            'shot_after_corner',
            'shot_after_free_kick',
            'shot_after_throw_in'
          )
      )
    ) AS shot_signal_secondary,
    (
      p.shot_payload IS NOT NULL
      AND LENGTH(TRIM(p.shot_payload)) > 2
    ) AS shot_signal_payload
  FROM parsed AS p
),
classified AS (
  SELECT
    f.*,
    (
      f.type_primary != 'shot_against'
      AND (
        f.shot_signal_primary
        OR f.shot_signal_secondary
        OR f.shot_signal_payload
      )
    ) AS is_shot_event,
    (
      f.shot_signal_payload
      AND NOT (f.shot_signal_primary OR f.shot_signal_secondary)
    ) AS shot_signal_payload_only,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT part
        FROM UNNEST(
          [
            IF(f.shot_signal_primary, 'primary', NULL),
            IF(f.shot_signal_secondary, 'secondary', NULL),
            IF(f.shot_signal_payload, 'payload', NULL)
          ]
        ) AS part
        WHERE part IS NOT NULL
      ),
      '+'
    ) AS shot_signal_type
  FROM flags AS f
),
/* Wyscout goalZone codes — documentations/Events.md */
enriched AS (
  SELECT
    c.*,
    JSON_VALUE(c.shot_json, '$.goalZone') AS _gz_raw
  FROM classified AS c
)
SELECT
  e.match_id,
  e.event_id,
  e.season_id,
  e.competition_id,
  e.match_period,
  e.minute,
  e.second,
  e.match_timestamp,
  e.video_timestamp,
  e.related_event_id,
  e.type_primary,
  e.type_secondary_json,
  e.location_x,
  e.location_y,
  e.team_id,
  e.opponent_team_id,
  e.player_id,
  e.possession_id,
  e.source_gcs_uri,
  e.shot_signal_primary,
  e.shot_signal_secondary,
  e.shot_signal_payload,
  e.shot_signal_payload_only,
  e.shot_signal_type,
  EXISTS (SELECT 1 FROM UNNEST(e.sec) AS t WHERE t = 'head_shot') AS is_head_shot,
  EXISTS (SELECT 1 FROM UNNEST(e.sec) AS t WHERE t = 'opportunity') AS is_opportunity,
  EXISTS (SELECT 1 FROM UNNEST(e.sec) AS t WHERE t = 'shot_after_corner') AS is_shot_after_corner,
  EXISTS (SELECT 1 FROM UNNEST(e.sec) AS t WHERE t = 'shot_after_free_kick') AS is_shot_after_free_kick,
  EXISTS (SELECT 1 FROM UNNEST(e.sec) AS t WHERE t = 'shot_after_throw_in') AS is_shot_after_throw_in,
  (e.type_primary = 'free_kick') AS is_free_kick,
  (e.type_primary = 'corner') AS is_corner,
  (e.type_primary = 'throw_in') AS is_throw_in,
  (e.type_primary = 'goal_kick') AS is_goal_kick,
  JSON_VALUE(e.shot_json, '$.bodyPart') AS shot_body_part,
  SAFE_CAST(JSON_VALUE(e.shot_json, '$.isGoal') AS BOOL) AS shot_is_goal,
  e._gz_raw AS shot_goal_zone,
  CASE
    WHEN e._gz_raw IS NULL OR TRIM(e._gz_raw) = '' THEN NULL
    WHEN e._gz_raw = 'otl' THEN 'out top left'
    WHEN e._gz_raw = 'ot' THEN 'out top centre'
    WHEN e._gz_raw = 'otr' THEN 'out top right'
    WHEN e._gz_raw = 'ol' THEN 'out left'
    WHEN e._gz_raw = 'or' THEN 'out right'
    WHEN e._gz_raw = 'olb' THEN 'out left bottom'
    WHEN e._gz_raw = 'orb' THEN 'out right bottom'
    WHEN e._gz_raw = 'gtl' THEN 'on target - top left corner'
    WHEN e._gz_raw = 'gt' THEN 'on target - top centre'
    WHEN e._gz_raw = 'gtr' THEN 'on target - top right corner'
    WHEN e._gz_raw = 'gl' THEN 'on target - left (mid height)'
    WHEN e._gz_raw = 'gc' THEN 'on target - centre'
    WHEN e._gz_raw = 'gr' THEN 'on target - right (mid height)'
    WHEN e._gz_raw = 'glb' THEN 'On target — inside goal: bottom left'
    WHEN e._gz_raw = 'gb' THEN 'on target - bottom centre'
    WHEN e._gz_raw = 'gbr' THEN 'on target - bottom right'
    WHEN e._gz_raw = 'bc' THEN 'blocked before goal line'
    WHEN STARTS_WITH(e._gz_raw, 'p') THEN CONCAT(
        'post or woodwork ',
        e._gz_raw
      )
    ELSE CONCAT('Unmapped Wyscout goalZone code (inspect raw): ', e._gz_raw)
  END AS shot_goal_zone_label,
  CASE
    WHEN e._gz_raw IS NULL OR TRIM(e._gz_raw) = '' THEN NULL
    WHEN e._gz_raw IN (
        'gtl',
        'gt',
        'gtr',
        'gl',
        'gc',
        'gr',
        'glb',
        'gb',
        'gbr'
      )
      THEN 'on_target'
    WHEN e._gz_raw IN ('otl', 'ot', 'otr', 'ol', 'or', 'olb', 'orb')
      THEN 'miss_off_target'
    WHEN e._gz_raw = 'bc' THEN 'blocked'
    WHEN STARTS_WITH(e._gz_raw, 'p') THEN 'post_or_woodwork'
    ELSE 'unknown_code'
  END AS shot_outcome,
  SAFE_CAST(JSON_VALUE(e.shot_json, '$.xg') AS FLOAT64) AS shot_xg,
  SAFE_CAST(JSON_VALUE(e.shot_json, '$.postShotXg') AS FLOAT64) AS shot_post_shot_xg,
  (
    SAFE_CAST(JSON_VALUE(e.shot_json, '$.xg') AS FLOAT64) IS NOT NULL
    AND SAFE_CAST(JSON_VALUE(e.shot_json, '$.xg') AS FLOAT64) > 0.2
  ) AS is_big_chance_xg,
  SAFE_CAST(JSON_VALUE(e.shot_json, '$.goalkeeperActionId') AS INT64) AS goalkeeper_action_event_id,
  SAFE_CAST(JSON_VALUE(e.shot_json, '$.goalkeeper.id') AS INT64) AS goalkeeper_player_id,
  SAFE_CAST(JSON_VALUE(e.shot_json, '$.goalkeeper.reflexSave') AS BOOL) AS goalkeeper_reflex_save
FROM enriched AS e
WHERE e.is_shot_event
