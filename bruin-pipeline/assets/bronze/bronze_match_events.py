"""@bruin
name: scouting_agent.bronze_match_events
type: python
image: python:3.12
connection: gcp

depends:
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
  - name: season_id
    type: integer
  - name: competition_id
    type: integer
  - name: gcs_uri
    type: string
  - name: fetched_at
    type: string
  - name: ok
    type: boolean
  - name: error_message
    type: string
@bruin"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[3]
_BRUIN_PL = Path(__file__).resolve().parents[2]
for p in (_ROOT, _BRUIN_PL):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

load_dotenv(_ROOT / ".env")

import wyscout  # noqa: E402

from wyscout_dimension_scope import (  # noqa: E402
    bq_client,
    fetch_match_keys_for_events,
    gcs_storage_client,
    match_events_gcs_blob_path,
    season_ids_for_monitoring,
    upload_json_bytes_to_gcs,
    wyscout_gcs_base_prefix,
    wyscout_gcs_bucket_name,
)


def materialize():
    full_refresh = os.environ.get("BRUIN_FULL_REFRESH") == "1"
    bq = bq_client(_ROOT)
    project = bq.project
    season_ids = season_ids_for_monitoring(_ROOT)
    keys = fetch_match_keys_for_events(bq, project, season_ids)
    if not keys:
        raise RuntimeError(
            "No rows in dim_match for the selected seasons (with competition_id set)."
        )

    bucket_name = wyscout_gcs_bucket_name()
    base_prefix = wyscout_gcs_base_prefix()
    storage = gcs_storage_client(_ROOT)
    bucket = storage.bucket(bucket_name)

    manifest: list[dict] = []
    for competition_id, season_id, match_id in keys:
        blob_name = match_events_gcs_blob_path(
            competition_id, season_id, match_id, base_prefix
        )
        blob = bucket.blob(blob_name)
        if not full_refresh and blob.exists():
            continue

        payload = wyscout.get_match_events(match_id, version="v3")
        if payload == -1:
            manifest.append(
                {
                    "match_id": match_id,
                    "season_id": season_id,
                    "competition_id": competition_id,
                    "gcs_uri": None,
                    "fetched_at": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "ok": False,
                    "error_message": "Wyscout get_match_events returned -1",
                }
            )
            continue

        envelope = {
            "source": "wyscout",
            "api_version": "v3",
            "match_id": match_id,
            "season_id": season_id,
            "competition_id": competition_id,
            "fetched_at": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "payload": payload,
        }
        body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        uri = upload_json_bytes_to_gcs(storage, bucket_name, blob_name, body)
        manifest.append(
            {
                "match_id": match_id,
                "season_id": season_id,
                "competition_id": competition_id,
                "gcs_uri": uri,
                "fetched_at": envelope["fetched_at"],
                "ok": True,
                "error_message": None,
            }
        )

    if not manifest:
        return pd.DataFrame(
            columns=[
                "match_id",
                "season_id",
                "competition_id",
                "gcs_uri",
                "fetched_at",
                "ok",
                "error_message",
            ]
        )

    df = pd.DataFrame(manifest)
    for col in ("match_id", "season_id", "competition_id"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df
