/* @bruin

name: scouting_agent.gold_match_possession
type: bq.sql
connection: gcp

depends:
  - scouting_agent.silver_match_possession
  - scouting_agent.silver_match_event
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
  - name: opponent_team_id
    type: int64
  - name: events_number
    type: int64
  - name: event_index
    type: int64
  - name: duration
    type: float64
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
  - name: pass_count
    type: int64
  - name: avg_pass_speed
    type: float64
  - name: time_defensive_third_sec
    type: float64
  - name: time_middle_third_sec
    type: float64
  - name: time_attacking_third_sec
    type: float64
  - name: pct_time_defensive_third
    type: float64
  - name: pct_time_middle_third
    type: float64
  - name: pct_time_attacking_third
    type: float64
  - name: third_start
    type: string
  - name: third_end
    type: string
  - name: ball_circulation_count
    type: int64
  - name: possession_start_home_score
    type: int64
  - name: possession_start_away_score
    type: int64
  - name: possession_start_goal_diff
    type: int64
  - name: team_leading_id
    type: int64

@bruin */

WITH scoped_matches AS (
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
),
silver AS (
  SELECT
    sp.match_id,
    sp.possession_id,
    sp.season_id,
    sp.competition_id,
    sp.team_id,
    sp.events_number,
    sp.event_index,
    sp.types_json,
    sp.duration,
    sp.attack_payload,
    sp.source_gcs_uri
  FROM `scouting_agent.silver_match_possession` AS sp
  {% if not full_refresh %}
  INNER JOIN scoped_matches AS sm
    ON sp.match_id = sm.match_id
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
),
match_ev AS (
  SELECT
    se.match_id,
    se.event_id,
    se.minute,
    se.second,
    se.type_primary,
    se.type_secondary_json,
    se.location_x,
    se.location_y,
    se.team_id,
    se.possession_id,
    se.possession_team_id,
    se.pass_payload,
    ROW_NUMBER() OVER (
      PARTITION BY se.match_id
      ORDER BY se.minute, se.second, se.event_id
    ) AS ev_ord
  FROM `scouting_agent.silver_match_event` AS se
  {% if not full_refresh %}
  INNER JOIN scoped_matches AS sm2
    ON se.match_id = sm2.match_id
  {% endif %}
  WHERE se.possession_id IS NOT NULL
),
ev_enriched AS (
  SELECT
    e.*,
    SAFE.PARSE_JSON(NULLIF(TRIM(e.pass_payload), '')) AS pass_json,
    CASE
      WHEN e.location_x IS NULL OR e.possession_team_id IS NULL OR e.team_id IS NULL THEN CAST(NULL AS FLOAT64)
      WHEN e.team_id = e.possession_team_id THEN CAST(e.location_x AS FLOAT64)
      ELSE 100.0 - CAST(e.location_x AS FLOAT64)
    END AS fx,
    CASE
      WHEN e.location_y IS NULL OR e.possession_team_id IS NULL OR e.team_id IS NULL THEN CAST(NULL AS FLOAT64)
      WHEN e.team_id = e.possession_team_id THEN CAST(e.location_y AS FLOAT64)
      ELSE 100.0 - CAST(e.location_y AS FLOAT64)
    END AS fy
  FROM match_ev AS e
),
ev_third AS (
  SELECT
    x.*,
    CASE
      WHEN x.fx IS NULL THEN CAST(NULL AS STRING)
      WHEN x.fx < 33.0 THEN 'defensive'
      WHEN x.fx <= 66.0 THEN 'middle'
      ELSE 'attacking'
    END AS x_third,
    LEAD(x.minute) OVER (
      PARTITION BY x.match_id, x.possession_id
      ORDER BY x.minute, x.second, x.event_id
    ) AS n_min,
    LEAD(x.second) OVER (
      PARTITION BY x.match_id, x.possession_id
      ORDER BY x.minute, x.second, x.event_id
    ) AS n_sec
  FROM ev_enriched AS x
),
ev_seg AS (
  SELECT
    t.*,
    CASE
      WHEN t.n_min IS NULL THEN 0.0
      ELSE GREATEST(
        0.0,
        CAST((t.n_min - t.minute) * 60 + (t.n_sec - t.second) AS FLOAT64)
      )
    END AS seg_sec
  FROM ev_third AS t
),
third_bounds AS (
  SELECT
    z.match_id,
    z.possession_id,
    MAX(
      IF(
        z.rn_start = 1,
        z.x_third,
        CAST(NULL AS STRING)
      )
    ) AS third_start,
    MAX(
      IF(
        z.rn_end = 1,
        z.x_third,
        CAST(NULL AS STRING)
      )
    ) AS third_end
  FROM (
    SELECT
      s.match_id,
      s.possession_id,
      s.x_third,
      ROW_NUMBER() OVER (
        PARTITION BY s.match_id, s.possession_id
        ORDER BY s.minute, s.second, s.event_id
      ) AS rn_start,
      ROW_NUMBER() OVER (
        PARTITION BY s.match_id, s.possession_id
        ORDER BY s.minute DESC, s.second DESC, s.event_id DESC
      ) AS rn_end
    FROM ev_seg AS s
    WHERE
      s.team_id = s.possession_team_id
      AND s.x_third IS NOT NULL
  ) AS z
  GROUP BY 1, 2
),
poss_agg AS (
  SELECT
    s.match_id,
    s.possession_id,
    COUNTIF(s.type_primary = 'pass' AND s.team_id = s.possession_team_id) AS pass_count,
    SUM(
      IF(
        s.team_id = s.possession_team_id AND s.x_third = 'defensive',
        s.seg_sec,
        0.0
      )
    ) AS time_defensive_third_sec,
    SUM(
      IF(s.team_id = s.possession_team_id AND s.x_third = 'middle', s.seg_sec, 0.0)
    ) AS time_middle_third_sec,
    SUM(
      IF(
        s.team_id = s.possession_team_id AND s.x_third = 'attacking',
        s.seg_sec,
        0.0
      )
    ) AS time_attacking_third_sec,
    SUM(IF(s.team_id = s.possession_team_id, s.seg_sec, 0.0)) AS total_seg_sec,
    tb.third_start,
    tb.third_end
  FROM ev_seg AS s
  LEFT JOIN third_bounds AS tb
    ON s.match_id = tb.match_id
    AND s.possession_id = tb.possession_id
  GROUP BY 1, 2
  , tb.third_start
  , tb.third_end
),
poss_pct AS (
  SELECT
    a.*,
    100.0 * SAFE_DIVIDE(a.time_defensive_third_sec, NULLIF(a.total_seg_sec, 0)) AS pct_time_defensive_third,
    100.0 * SAFE_DIVIDE(a.time_middle_third_sec, NULLIF(a.total_seg_sec, 0)) AS pct_time_middle_third,
    100.0 * SAFE_DIVIDE(a.time_attacking_third_sec, NULLIF(a.total_seg_sec, 0)) AS pct_time_attacking_third
  FROM poss_agg AS a
),
pass_speed AS (
  SELECT
    s.match_id,
    s.possession_id,
    AVG(
      SAFE_DIVIDE(
        SQRT(
          POW(
            SAFE_CAST(JSON_VALUE(s.pass_json, '$.endLocation.x') AS FLOAT64) - CAST(s.location_x AS FLOAT64),
            2
          )
          + POW(
            SAFE_CAST(JSON_VALUE(s.pass_json, '$.endLocation.y') AS FLOAT64) - CAST(s.location_y AS FLOAT64),
            2
          )
        ),
        GREATEST(
          1e-6,
          CAST((s.n_min - s.minute) * 60 + (s.n_sec - s.second) AS FLOAT64)
        )
      )
    ) AS avg_pass_speed
  FROM ev_seg AS s
  WHERE
    s.team_id = s.possession_team_id
    AND s.type_primary = 'pass'
    AND s.pass_json IS NOT NULL
    AND JSON_VALUE(s.pass_json, '$.endLocation.x') IS NOT NULL
    AND JSON_VALUE(s.pass_json, '$.endLocation.y') IS NOT NULL
    AND s.location_x IS NOT NULL
    AND s.location_y IS NOT NULL
    AND s.n_min IS NOT NULL
  GROUP BY 1, 2
),
circ_events AS (
  SELECT
    e.match_id,
    e.possession_id,
    CAST(e.location_y AS FLOAT64) AS ly,
    LAG(CAST(e.location_y AS FLOAT64)) OVER (
      PARTITION BY e.match_id, e.possession_id
      ORDER BY e.minute, e.second, e.event_id
    ) AS prev_ly
  FROM ev_enriched AS e
  WHERE
    e.team_id = e.possession_team_id
    AND e.location_y IS NOT NULL
),
circ_agg AS (
  SELECT
    c.match_id,
    c.possession_id,
    COUNTIF(
      c.prev_ly IS NOT NULL
      AND (
        (c.prev_ly > 66.0 AND c.ly < 33.0)
        OR (c.prev_ly < 33.0 AND c.ly > 66.0)
      )
    ) AS ball_circulation_count
  FROM circ_events AS c
  GROUP BY 1, 2
),
goals AS (
  SELECT
    e.match_id,
    e.team_id,
    e.ev_ord
  FROM match_ev AS e
  WHERE
    e.type_primary = 'shot'
    AND EXISTS (
      SELECT 1
      FROM UNNEST(
        IFNULL(
          JSON_VALUE_ARRAY(SAFE.PARSE_JSON(NULLIF(TRIM(e.type_secondary_json), ''))),
          ARRAY<STRING>[]
        )
      ) AS g
      WHERE g = 'goal'
    )
),
poss_start AS (
  SELECT
    e.match_id,
    e.possession_id,
    MIN(e.ev_ord) AS start_ev_ord
  FROM match_ev AS e
  GROUP BY 1, 2
),
match_state_scores AS (
  SELECT
    ps.match_id,
    ps.possession_id,
    (
      SELECT COUNT(*)
      FROM goals AS g
      WHERE
        g.match_id = ps.match_id
        AND g.ev_ord < ps.start_ev_ord
        AND dm.home_team_id IS NOT NULL
        AND g.team_id = dm.home_team_id
    ) AS possession_start_home_score,
    (
      SELECT COUNT(*)
      FROM goals AS g
      WHERE
        g.match_id = ps.match_id
        AND g.ev_ord < ps.start_ev_ord
        AND dm.away_team_id IS NOT NULL
        AND g.team_id = dm.away_team_id
    ) AS possession_start_away_score
  FROM poss_start AS ps
  LEFT JOIN `scouting_agent.dim_match` AS dm
    ON ps.match_id = dm.match_id
)
SELECT
  p.match_id,
  p.possession_id,
  p.season_id,
  p.competition_id,
  p.team_id,
  p.events_number,
  p.event_index,
  CAST(p.duration AS FLOAT64) AS duration,
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
  SAFE_CAST(JSON_VALUE(p.attack_json, '$.xg') AS FLOAT64) AS attack_xg,
  COALESCE(pp.pass_count, 0) AS pass_count,
  ps.avg_pass_speed AS avg_pass_speed,
  COALESCE(pp.time_defensive_third_sec, 0.0) AS time_defensive_third_sec,
  COALESCE(pp.time_middle_third_sec, 0.0) AS time_middle_third_sec,
  COALESCE(pp.time_attacking_third_sec, 0.0) AS time_attacking_third_sec,
  pp.pct_time_defensive_third AS pct_time_defensive_third,
  pp.pct_time_middle_third AS pct_time_middle_third,
  pp.pct_time_attacking_third AS pct_time_attacking_third,
  pp.third_start AS third_start,
  pp.third_end AS third_end,
  COALESCE(ca.ball_circulation_count, 0) AS ball_circulation_count,
  ms.possession_start_home_score AS possession_start_home_score,
  ms.possession_start_away_score AS possession_start_away_score,
  CASE
    WHEN ms.match_id IS NULL THEN CAST(NULL AS INT64)
    ELSE COALESCE(ms.possession_start_home_score, 0) - COALESCE(ms.possession_start_away_score, 0)
  END AS possession_start_goal_diff,
  CASE
    WHEN ms.match_id IS NULL OR dm.match_id IS NULL THEN CAST(NULL AS INT64)
    WHEN COALESCE(ms.possession_start_home_score, 0) > COALESCE(ms.possession_start_away_score, 0) THEN dm.home_team_id
    WHEN COALESCE(ms.possession_start_away_score, 0) > COALESCE(ms.possession_start_home_score, 0) THEN dm.away_team_id
    ELSE 0
  END AS team_leading_id
FROM parsed AS p
LEFT JOIN `scouting_agent.dim_match` AS dm
  ON p.match_id = dm.match_id
LEFT JOIN poss_pct AS pp
  ON p.match_id = pp.match_id
  AND p.possession_id = pp.possession_id
LEFT JOIN pass_speed AS ps
  ON p.match_id = ps.match_id
  AND p.possession_id = ps.possession_id
LEFT JOIN circ_agg AS ca
  ON p.match_id = ca.match_id
  AND p.possession_id = ca.possession_id
LEFT JOIN match_state_scores AS ms
  ON p.match_id = ms.match_id
  AND p.possession_id = ms.possession_id
