/* @bruin

name: scouting_agent.gold_match_infraction_event
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
  - name: infraction_payload
    type: string
  - name: is_foul
    type: bool
  - name: yellow_card
    type: bool
  - name: red_card
    type: bool
  - name: foul_type
    type: string
  - name: opponent_player_id
    type: int64

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
    se.infraction_payload,
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
    SAFE.PARSE_JSON(NULLIF(TRIM(s.infraction_payload), '')) AS inf_json
  FROM silver AS s
),
flags AS (
  SELECT
    p.*,
    EXISTS (SELECT 1 FROM UNNEST(p.sec) AS t WHERE t = 'foul') AS is_foul
  FROM parsed AS p
)
SELECT
  f.match_id,
  f.event_id,
  f.season_id,
  f.competition_id,
  f.match_period,
  f.minute,
  f.second,
  f.match_timestamp,
  f.video_timestamp,
  f.related_event_id,
  f.type_primary,
  f.type_secondary_json,
  f.location_x,
  f.location_y,
  f.team_id,
  f.opponent_team_id,
  f.player_id,
  f.possession_id,
  f.possession_team_id,
  f.source_gcs_uri,
  f.infraction_payload,
  f.is_foul,
  SAFE_CAST(JSON_VALUE(f.inf_json, '$.yellowCard') AS BOOL) AS yellow_card,
  SAFE_CAST(JSON_VALUE(f.inf_json, '$.redCard') AS BOOL) AS red_card,
  JSON_VALUE(f.inf_json, '$.type') AS foul_type,
  SAFE_CAST(JSON_VALUE(f.inf_json, '$.opponent.id') AS INT64) AS opponent_player_id
FROM flags AS f
WHERE
  f.type_primary = 'infraction'
  OR (
    f.infraction_payload IS NOT NULL
    AND LENGTH(TRIM(f.infraction_payload)) > 2
  )
