"""@bruin
name: scouting_agent.silver_match_event
type: python
image: python:3.12
connection: gcp

depends:
  - scouting_agent.bronze_match_events
  - scouting_agent.dim_match

materialization:
  type: table
  strategy: merge

parameters:
  enforce_schema: true

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
  - name: pass_payload
    type: string
  - name: shot_payload
    type: string
  - name: ground_duel_payload
    type: string
  - name: aerial_duel_payload
    type: string
  - name: infraction_payload
    type: string
  - name: carry_payload
    type: string
  - name: source_gcs_uri
    type: string
@bruin"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[3]
_BRUIN_PL = Path(__file__).resolve().parents[2]
for p in (_ROOT, _BRUIN_PL):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

load_dotenv(_ROOT / ".env")

from wyscout_dimension_scope import (  # noqa: E402
    bq_client,
    download_json_from_gcs_uri,
    events_list_from_wyscout_match_events_api,
    gcs_storage_client,
    match_date_window_predicate_sql,
    silver_match_event_row,
    unpack_bronze_match_events_document,
)

_INT_COLS = (
    "season_id",
    "competition_id",
    "minute",
    "second",
    "related_event_id",
    "location_x",
    "location_y",
    "team_id",
    "opponent_team_id",
    "player_id",
    "possession_id",
    "possession_team_id",
)


def materialize():
    bq = bq_client(_ROOT)
    project = bq.project
    window_sql = match_date_window_predicate_sql("m.match_date_utc")
    if window_sql:
        purge_q = f"""
        DELETE FROM `{project}.scouting_agent.silver_match_event` AS s
        WHERE NOT EXISTS (
          SELECT 1
          FROM `{project}.scouting_agent.dim_match` AS m
          WHERE m.match_id = s.match_id
            AND m.season_id = s.season_id
            AND m.competition_id IS NOT NULL
            {window_sql}
        )
        """
        bq.query(purge_q).result()
        q = f"""
        SELECT b.match_id, b.season_id, b.competition_id, b.gcs_uri
        FROM `{project}.scouting_agent.bronze_match_events` b
        INNER JOIN `{project}.scouting_agent.dim_match` m
          ON b.match_id = m.match_id AND b.season_id = m.season_id
        WHERE b.ok IS TRUE AND b.gcs_uri IS NOT NULL
        {window_sql}
        ORDER BY b.match_id
        """
    else:
        q = f"""
        SELECT match_id, season_id, competition_id, gcs_uri
        FROM `{project}.scouting_agent.bronze_match_events`
        WHERE ok IS TRUE AND gcs_uri IS NOT NULL
        ORDER BY match_id
        """
    job = bq.query(q)
    storage = gcs_storage_client(_ROOT)

    out: list[dict] = []
    for row in job.result():
        match_id = int(row[0])
        season_id = int(row[1]) if row[1] is not None else None
        competition_id = int(row[2]) if row[2] is not None else None
        gcs_uri = str(row[3])
        doc = download_json_from_gcs_uri(storage, gcs_uri)
        if not isinstance(doc, dict):
            raise RuntimeError(f"Invalid JSON object for match_id={match_id} uri={gcs_uri}")
        payload = unpack_bronze_match_events_document(doc)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Invalid payload for match_id={match_id} uri={gcs_uri}")
        for e in events_list_from_wyscout_match_events_api(payload):
            r = silver_match_event_row(
                match_id=match_id,
                season_id=season_id,
                competition_id=competition_id,
                source_gcs_uri=gcs_uri,
                e=e,
            )
            if r:
                out.append(r)

    if not out:
        return pd.DataFrame(
            columns=[
                "match_id",
                "event_id",
                "season_id",
                "competition_id",
                "match_period",
                "minute",
                "second",
                "match_timestamp",
                "video_timestamp",
                "related_event_id",
                "type_primary",
                "type_secondary_json",
                "location_x",
                "location_y",
                "team_id",
                "opponent_team_id",
                "player_id",
                "possession_id",
                "possession_team_id",
                "pass_payload",
                "shot_payload",
                "ground_duel_payload",
                "aerial_duel_payload",
                "infraction_payload",
                "carry_payload",
                "source_gcs_uri",
            ]
        )

    df = pd.DataFrame(out)
    df = df.drop_duplicates(subset=["match_id", "event_id"], keep="last")
    for col in _INT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["match_id"] = pd.to_numeric(df["match_id"], errors="coerce").astype("Int64")
    df["event_id"] = pd.to_numeric(df["event_id"], errors="coerce").astype("Int64")
    return df
