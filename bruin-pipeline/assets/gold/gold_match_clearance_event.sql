/* @bruin

name: scouting_agent.gold_match_clearance_event
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
  - name: clearance_signal_primary
    type: bool
  - name: clearance_signal_secondary
    type: bool
  - name: clearance_signal_type
    type: string
  - name: is_secondary_carry
    type: bool
  - name: is_secondary_head_pass
    type: bool
  - name: is_secondary_loss
    type: bool
  - name: is_secondary_recovery
    type: bool
  - name: is_secondary_under_pressure
    type: bool
  - name: pass_accurate
    type: bool
  - name: pass_length_m
    type: float64
  - name: pass_angle_deg
    type: float64
  - name: pass_height
    type: string
  - name: pass_end_x
    type: float64
  - name: pass_end_y
    type: float64
  - name: recipient_player_id
    type: int64
  - name: carry_progression_m
    type: float64

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
    se.pass_payload,
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
    SAFE.PARSE_JSON(NULLIF(TRIM(s.pass_payload), '')) AS pass_json,
    SAFE.PARSE_JSON(NULLIF(TRIM(s.carry_payload), '')) AS carry_json
  FROM silver AS s
),
flags AS (
  SELECT
    p.*,
    (p.type_primary = 'clearance') AS clearance_signal_primary,
    (
      EXISTS (
        SELECT 1
        FROM UNNEST(p.sec) AS tag
        WHERE tag = 'clearance'
      )
    ) AS clearance_signal_secondary,
    EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'carry') AS is_secondary_carry,
    EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'head_pass') AS is_secondary_head_pass,
    EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'loss') AS is_secondary_loss,
    EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'recovery') AS is_secondary_recovery,
    EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'under_pressure') AS is_secondary_under_pressure
  FROM parsed AS p
),
classified AS (
  SELECT
    f.*,
    (
      f.clearance_signal_primary
      OR f.clearance_signal_secondary
    ) AS is_clearance_event,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT part
        FROM UNNEST(
          [
            IF(f.clearance_signal_primary, 'primary', NULL),
            IF(f.clearance_signal_secondary, 'secondary_tag_clearance', NULL)
          ]
        ) AS part
        WHERE part IS NOT NULL
      ),
      '+'
    ) AS clearance_signal_type
  FROM flags AS f
),
enriched AS (
  SELECT
    c.*,
    SAFE_CAST(JSON_VALUE(c.pass_json, '$.accurate') AS BOOL) AS pass_accurate,
    SAFE_CAST(JSON_VALUE(c.pass_json, '$.length') AS FLOAT64) AS pass_length_m,
    SAFE_CAST(JSON_VALUE(c.pass_json, '$.angle') AS FLOAT64) AS pass_angle_deg,
    JSON_VALUE(c.pass_json, '$.height') AS pass_height,
    SAFE_CAST(JSON_VALUE(c.pass_json, '$.endLocation.x') AS FLOAT64) AS pass_end_x,
    SAFE_CAST(JSON_VALUE(c.pass_json, '$.endLocation.y') AS FLOAT64) AS pass_end_y,
    SAFE_CAST(JSON_VALUE(c.pass_json, '$.recipient.id') AS INT64) AS recipient_player_id,
    SAFE_CAST(JSON_VALUE(c.carry_json, '$.progression') AS FLOAT64) AS carry_progression_m
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
  e.clearance_signal_primary,
  e.clearance_signal_secondary,
  e.clearance_signal_type,
  e.is_secondary_carry,
  e.is_secondary_head_pass,
  e.is_secondary_loss,
  e.is_secondary_recovery,
  e.is_secondary_under_pressure,
  e.pass_accurate,
  e.pass_length_m,
  e.pass_angle_deg,
  e.pass_height,
  e.pass_end_x,
  e.pass_end_y,
  e.recipient_player_id,
  e.carry_progression_m
FROM enriched AS e
WHERE e.is_clearance_event
