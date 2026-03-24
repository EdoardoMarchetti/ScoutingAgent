from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

from services.runtime_secrets import (
    get_secret,
    get_secret_value,
    parse_service_account_info,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def _load_gcp_config_from_env() -> tuple[str, Any]:
    repo_root = _repo_root()
    load_dotenv(repo_root / ".env")

    project_id = (
        get_secret("GOOGLE_CLOUD_PROJECT")
        or get_secret("GCP_PROJECT_ID")
        or get_secret("BQ_PROJECT_ID")
    )
    if not project_id:
        raise RuntimeError(
            "Missing GCP project. Set GOOGLE_CLOUD_PROJECT (or GCP_PROJECT_ID/BQ_PROJECT_ID) in .env."
        )

    credentials_value = (
        get_secret_value("GOOGLE_APPLICATION_CREDENTIALS")
        or get_secret_value("GCP_SERVICE_ACCOUNT_FILE")
        or get_secret_value("BQ_SERVICE_ACCOUNT_FILE")
    )
    return str(project_id), credentials_value


@lru_cache(maxsize=1)
def get_bigquery_client() -> bigquery.Client:
    project_id, credentials_value = _load_gcp_config_from_env()
    if not credentials_value:
        return bigquery.Client(project=project_id)

    credentials_raw = str(credentials_value).strip()
    if credentials_raw.startswith("{"):
        info = parse_service_account_info(credentials_value)
        credentials = service_account.Credentials.from_service_account_info(
            info
        )
        return bigquery.Client(credentials=credentials, project=project_id)

    repo_root = _repo_root()
    credentials_path = Path(credentials_raw)
    if not credentials_path.is_absolute():
        credentials_path = (repo_root / credentials_path).resolve()
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
