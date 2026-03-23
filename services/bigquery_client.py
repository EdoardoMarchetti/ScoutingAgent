from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def _load_gcp_config_from_env() -> tuple[str, Path | None]:
    repo_root = _repo_root()
    load_dotenv(repo_root / ".env")

    project_id = (
        os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCP_PROJECT_ID")
        or os.getenv("BQ_PROJECT_ID")
    )
    if not project_id:
        raise RuntimeError(
            "Missing GCP project. Set GOOGLE_CLOUD_PROJECT (or GCP_PROJECT_ID/BQ_PROJECT_ID) in .env."
        )

    credentials_file = (
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or os.getenv("GCP_SERVICE_ACCOUNT_FILE")
        or os.getenv("BQ_SERVICE_ACCOUNT_FILE")
    )
    credentials_path = None
    if credentials_file:
        credentials_path = Path(credentials_file)
        if not credentials_path.is_absolute():
            credentials_path = (repo_root / credentials_path).resolve()

    return str(project_id), credentials_path


@lru_cache(maxsize=1)
def get_bigquery_client() -> bigquery.Client:
    project_id, credentials_path = _load_gcp_config_from_env()
    if credentials_path is None:
        return bigquery.Client(project=project_id)
    if not credentials_path.is_file():
        raise RuntimeError(f"Service account file not found: {credentials_path}")

    credentials = service_account.Credentials.from_service_account_file(
        str(credentials_path)
    )
    return bigquery.Client(credentials=credentials, project=project_id)


@lru_cache(maxsize=1)
def get_bq_project_id() -> str:
    project_id, _ = _load_gcp_config_from_env()
    return project_id
