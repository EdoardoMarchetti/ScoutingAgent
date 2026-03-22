/* @bruin

name: scouting_agent.gold_match_goalkeeper_exit_event
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
)
SELECT
  s.match_id,
  s.event_id,
  s.season_id,
  s.competition_id,
  s.match_period,
  s.minute,
  s.second,
  s.match_timestamp,
  s.video_timestamp,
  s.related_event_id,
  s.type_primary,
  s.type_secondary_json,
  s.location_x,
  s.location_y,
  s.team_id,
  s.opponent_team_id,
  s.player_id,
  s.possession_id,
  s.possession_team_id,
  s.source_gcs_uri
FROM silver AS s
WHERE s.type_primary = 'goalkeeper_exit'
