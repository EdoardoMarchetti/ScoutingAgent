/* @bruin

name: scouting_agent.gold_match_possession
type: bq.sql
connection: gcp

depends:
  - scouting_agent.silver_match_possession
  - scouting_agent.dim_match
  - scouting_agent.dim_season

materialization:
  type: table
  strategy: delete+insert
  incremental_key: match_id

# First deploy: incremental runs DELETE from this table before INSERT; create it once with:
#   bruin run --full-refresh bruin-pipeline/assets/gold/gold_match_possession.sql

columns:
  - name: match_id
    type: int64
    primary_key: true
  - name: possession_id
    type: int64
    primary_key: true
  - name: season_id
    type: int64
  - name: competition_id
    type: int64
  - name: team_id
    type: int64
  - name: events_number
    type: int64
  - name: event_index
    type: int64
  - name: source_gcs_uri
    type: string
  - name: types_json
    type: string
  - name: is_attack
    type: bool
  - name: is_counterattack
    type: bool
  - name: transition
    type: string
  - name: set_piece
    type: string
  - name: attack_payload
    type: string
  - name: attack_with_shot
    type: bool
  - name: attack_with_shot_on_goal
    type: bool
  - name: attack_with_goal
    type: bool
  - name: attack_flank
    type: string
  - name: attack_xg
    type: float64

@bruin */

WITH silver AS (
  SELECT
    sp.match_id,
    sp.possession_id,
    sp.season_id,
    sp.competition_id,
    sp.team_id,
    sp.events_number,
    sp.event_index,
    sp.types_json,
    sp.attack_payload,
    sp.source_gcs_uri
  FROM `scouting_agent.silver_match_possession` AS sp
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
    ON sp.match_id = _gold_scope.match_id
  {% endif %}
),
parsed AS (
  SELECT
    s.*,
    IFNULL(
      JSON_VALUE_ARRAY(SAFE.PARSE_JSON(NULLIF(TRIM(s.types_json), ''))),
      ARRAY<STRING>[]
    ) AS types_arr,
    SAFE.PARSE_JSON(NULLIF(TRIM(s.attack_payload), '')) AS attack_json
  FROM silver AS s
)
SELECT
  p.match_id,
  p.possession_id,
  p.season_id,
  p.competition_id,
  p.team_id,
  p.events_number,
  p.event_index,
  p.source_gcs_uri,
  p.types_json,
  EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'attack') AS is_attack,
  EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'counterattack') AS is_counterattack,
  CASE
    WHEN EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'transition_low') THEN 'low'
    WHEN EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'transition_medium') THEN 'medium'
    WHEN EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'transition_high') THEN 'high'
    ELSE CAST(NULL AS STRING)
  END AS transition,
  CASE
    WHEN EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'direct_free_kick') THEN 'direct_free_kick'
    WHEN EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'free_kick_cross') THEN 'free_kick_cross'
    WHEN EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'free_kick') THEN 'free_kick'
    WHEN EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'corner') THEN 'corner'
    WHEN EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'penalty') THEN 'penalty'
    WHEN EXISTS (SELECT 1 FROM UNNEST(p.types_arr) AS t WHERE t = 'throw_in') THEN 'throw_in'
    ELSE CAST(NULL AS STRING)
  END AS set_piece,
  p.attack_payload,
  SAFE_CAST(JSON_VALUE(p.attack_json, '$.withShot') AS BOOL) AS attack_with_shot,
  SAFE_CAST(JSON_VALUE(p.attack_json, '$.withShotOnGoal') AS BOOL) AS attack_with_shot_on_goal,
  SAFE_CAST(JSON_VALUE(p.attack_json, '$.withGoal') AS BOOL) AS attack_with_goal,
  JSON_VALUE(p.attack_json, '$.flank') AS attack_flank,
  SAFE_CAST(JSON_VALUE(p.attack_json, '$.xg') AS FLOAT64) AS attack_xg
FROM parsed AS p
