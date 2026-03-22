"""@bruin
name: scouting_agent.silver_match_possession
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
  - name: possession_id
    type: integer
    primary_key: true
  - name: season_id
    type: integer
  - name: competition_id
    type: integer
  - name: team_id
    type: integer
  - name: events_number
    type: integer
  - name: event_index
    type: integer
  - name: types_json
    type: string
  - name: attack_payload
    type: string
  - name: source_gcs_uri
    type: string
@bruin"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound

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
    silver_match_possession_row,
    unpack_bronze_match_events_document,
)

_INT_COLS = (
    "season_id",
    "competition_id",
    "team_id",
    "events_number",
    "event_index",
)


def materialize():
    bq = bq_client(_ROOT)
    project = bq.project
    window_sql = match_date_window_predicate_sql("m.match_date_utc")
    if window_sql:
        purge_q = f"""
        DELETE FROM `{project}.scouting_agent.silver_match_possession` AS s
        WHERE NOT EXISTS (
          SELECT 1
          FROM `{project}.scouting_agent.dim_match` AS m
          WHERE m.match_id = s.match_id
            AND m.season_id = s.season_id
            AND m.competition_id IS NOT NULL
            {window_sql}
        )
        """
        try:
            bq.get_table(f"{project}.scouting_agent.silver_match_possession")
        except NotFound:
            pass
        else:
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

    # Last event per (match_id, possession_id) wins (Wyscout order ≈ chronological).
    by_key: dict[tuple[int, int], dict] = {}
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
            r = silver_match_possession_row(
                match_id=match_id,
                season_id=season_id,
                competition_id=competition_id,
                source_gcs_uri=gcs_uri,
                e=e,
            )
            if r:
                by_key[(match_id, int(r["possession_id"]))] = r

    out = list(by_key.values())
    if not out:
        return pd.DataFrame(
            columns=[
                "match_id",
                "possession_id",
                "season_id",
                "competition_id",
                "team_id",
                "events_number",
                "event_index",
                "types_json",
                "attack_payload",
                "source_gcs_uri",
            ]
        )

    df = pd.DataFrame(out)
    for col in _INT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["match_id"] = pd.to_numeric(df["match_id"], errors="coerce").astype("Int64")
    df["possession_id"] = pd.to_numeric(df["possession_id"], errors="coerce").astype("Int64")
    return df
