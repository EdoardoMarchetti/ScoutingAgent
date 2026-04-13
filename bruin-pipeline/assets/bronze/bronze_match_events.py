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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm.auto import tqdm

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

# First attempt + 5 retries = 6 GET per match.
_API_RETRIES = 5
_API_BACKOFF_BASE_S = 0.6
_MAX_DOWNLOAD_WORKERS = 16


def _fetch_match_events_with_retry(match_id: int) -> tuple[object, str | None]:
    """
    Fino a ``1 + _API_RETRIES`` chiamate Wyscout; ritorna ``(payload, None)`` se ok,
    altrimenti ``(-1, error_message)``.
    """
    last_err: str | None = None
    max_attempts = _API_RETRIES + 1
    for attempt in range(1, max_attempts + 1):
        try:
            payload = wyscout.get_match_events(match_id, version="v3")
            if payload != -1:
                return payload, None
            last_err = "Wyscout get_match_events returned -1"
        except Exception as exc:  # noqa: BLE001 — surface last error in bronze row
            last_err = f"{type(exc).__name__}: {exc}"
        if attempt < max_attempts:
            time.sleep(_API_BACKOFF_BASE_S * (2 ** (attempt - 1)))
    return -1, last_err


def _download_and_upload_one(
    competition_id: int,
    season_id: int,
    match_id: int,
    *,
    storage: object,
    bucket_name: str,
    base_prefix: str,
) -> dict:
    blob_name = match_events_gcs_blob_path(
        competition_id, season_id, match_id, base_prefix
    )
    fetched_at = (
        datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    payload, err = _fetch_match_events_with_retry(match_id)
    if payload == -1:
        return {
            "match_id": match_id,
            "season_id": season_id,
            "competition_id": competition_id,
            "gcs_uri": None,
            "fetched_at": fetched_at,
            "ok": False,
            "error_message": err or "Wyscout get_match_events failed after retries",
        }

    envelope = {
        "source": "wyscout",
        "api_version": "v3",
        "match_id": match_id,
        "season_id": season_id,
        "competition_id": competition_id,
        "fetched_at": fetched_at,
        "payload": payload,
    }
    body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    uri = upload_json_bytes_to_gcs(storage, bucket_name, blob_name, body)
    return {
        "match_id": match_id,
        "season_id": season_id,
        "competition_id": competition_id,
        "gcs_uri": uri,
        "fetched_at": fetched_at,
        "ok": True,
        "error_message": None,
    }


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

    to_fetch: list[tuple[int, int, int]] = []
    skipped_existing_blob = 0
    for competition_id, season_id, match_id in keys:
        blob_name = match_events_gcs_blob_path(
            competition_id, season_id, match_id, base_prefix
        )
        blob = bucket.blob(blob_name)
        if not full_refresh and blob.exists():
            skipped_existing_blob += 1
            continue
        to_fetch.append((competition_id, season_id, match_id))

    manifest: list[dict] = []
    match_tentati = 0
    riusciti = 0
    falliti = 0

    if to_fetch:
        match_tentati = len(to_fetch)
        workers = min(_MAX_DOWNLOAD_WORKERS, max(1, len(to_fetch)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = [
                pool.submit(
                    _download_and_upload_one,
                    cid,
                    sid,
                    mid,
                    storage=storage,
                    bucket_name=bucket_name,
                    base_prefix=base_prefix,
                )
                for cid, sid, mid in to_fetch
            ]
            for fut in tqdm(
                as_completed(futs),
                total=len(futs),
                desc="bronze_match_events",
                unit="match",
            ):
                row = fut.result()
                manifest.append(row)
                if row.get("ok"):
                    riusciti += 1
                else:
                    falliti += 1

    print(
        "[bronze_match_events] Riepilogo: "
        f"match_tentati={match_tentati} riusciti={riusciti} falliti={falliti} "
        f"saltati_blob_esistente={skipped_existing_blob} "
        f"(partite_in_scope={len(keys)})"
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
