/* @bruin

name: scouting_agent.gold_match_recovery_interception_event
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
  - name: is_opponent_half
    type: bool
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
  - name: is_interception
    type: bool
  - name: is_recovery
    type: bool
  - name: is_counterpressing
    type: bool
  - name: regain_signal_type
    type: string

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
    ) AS sec
  FROM silver AS s
),
flags AS (
  SELECT
    p.*,
    (
      p.type_primary = 'interception'
      OR EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'interception')
    ) AS is_interception,
    (
      p.type_primary = 'ball_recovery'
      OR EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'recovery')
    ) AS is_recovery,
    EXISTS (
      SELECT 1
      FROM UNNEST(p.sec) AS t
      WHERE t = 'counterpressing_recovery'
    ) AS is_counterpressing
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
            IF(f.is_interception, 'interception', NULL),
            IF(f.is_recovery, 'recovery', NULL),
            IF(f.is_counterpressing, 'counterpressing_recovery', NULL)
          ]
        ) AS part
        WHERE part IS NOT NULL
      ),
      '+'
    ) AS regain_signal_type
  FROM flags AS f
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
  (c.location_x IS NOT NULL AND c.location_x >= 50) AS is_opponent_half,
  c.team_id,
  c.opponent_team_id,
  c.player_id,
  c.possession_id,
  c.possession_team_id,
  c.source_gcs_uri,
  c.is_interception,
  c.is_recovery,
  c.is_counterpressing,
  c.regain_signal_type
FROM classified AS c
WHERE
  c.is_interception
  OR c.is_recovery
  OR c.is_counterpressing
