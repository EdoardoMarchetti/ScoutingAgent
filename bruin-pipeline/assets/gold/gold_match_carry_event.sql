/* @bruin

name: scouting_agent.gold_match_carry_event
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
  - name: possession_team_id
    type: int64
  - name: source_gcs_uri
    type: string
  - name: is_primary_carry
    type: bool
  - name: is_secondary_carry
    type: bool
  - name: carry_signal_type
    type: string
  - name: carry_payload
    type: string
  - name: carry_progression_m
    type: float64
  - name: carry_end_x
    type: float64
  - name: carry_end_y
    type: float64
  - name: is_acceleration
    type: bool
  - name: is_progressive_run
    type: bool
  - name: is_under_pressure
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
    se.possession_team_id,
    se.carry_payload,
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
    SAFE.PARSE_JSON(NULLIF(TRIM(s.carry_payload), '')) AS carry_json
  FROM silver AS s
),
flags AS (
  SELECT
    p.*,
    (p.type_primary = 'carry') AS is_primary_carry,
    EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'carry') AS is_secondary_carry,
    (
      p.type_primary = 'acceleration'
      OR EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'acceleration')
    ) AS is_acceleration,
    EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'progressive_run') AS is_progressive_run,
    EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'under_pressure') AS is_under_pressure
  FROM parsed AS p
),
classified AS (
  SELECT
    f.*,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT part
        FROM UNNEST(
          [
            IF(f.is_primary_carry, 'primary_carry', NULL),
            IF(f.is_secondary_carry, 'secondary_carry', NULL)
          ]
        ) AS part
        WHERE part IS NOT NULL
      ),
      '+'
    ) AS carry_signal_type
  FROM flags AS f
),
enriched AS (
  SELECT
    c.*,
    SAFE_CAST(JSON_VALUE(c.carry_json, '$.progression') AS FLOAT64) AS carry_progression_m,
    SAFE_CAST(JSON_VALUE(c.carry_json, '$.endLocation.x') AS FLOAT64) AS carry_end_x,
    SAFE_CAST(JSON_VALUE(c.carry_json, '$.endLocation.y') AS FLOAT64) AS carry_end_y
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
  e.possession_team_id,
  e.source_gcs_uri,
  e.is_primary_carry,
  e.is_secondary_carry,
  e.carry_signal_type,
  e.carry_payload,
  e.carry_progression_m,
  e.carry_end_x,
  e.carry_end_y,
  e.is_acceleration,
  e.is_progressive_run,
  e.is_under_pressure
FROM enriched AS e
WHERE e.is_primary_carry OR e.is_secondary_carry
