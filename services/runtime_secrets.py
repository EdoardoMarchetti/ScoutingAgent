from __future__ import annotations

import atexit
import json
import os
import re
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

_materialized_adc_path: str | None = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_google_application_credentials_file() -> None:
    """
    Vertex AI and google.auth.default() expect GOOGLE_APPLICATION_CREDENTIALS to be a
    filesystem path. Streamlit secrets often hold inline JSON (not in git): write it
    to a temp file once per process and point the env var there.
    """
    global _materialized_adc_path
    if _materialized_adc_path and Path(_materialized_adc_path).is_file():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _materialized_adc_path
        return

    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if raw in (None, ""):
        raw = get_secret_value("GOOGLE_APPLICATION_CREDENTIALS", None)
    if raw in (None, ""):
        return

    s = str(raw).strip()
    if s.startswith("{"):
        info = parse_service_account_info(s)
        fd, path_str = tempfile.mkstemp(prefix="gcp-adc-", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(info, handle)
        except Exception:
            Path(path_str).unlink(missing_ok=True)
            raise
        _materialized_adc_path = path_str
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_str

        def _cleanup(p: str = path_str) -> None:
            try:
                Path(p).unlink(missing_ok=True)
            except OSError:
                pass

        atexit.register(_cleanup)
        return

    p = Path(s).expanduser()
    if not p.is_absolute():
        p = (_repo_root() / p).resolve()
    if not p.is_file():
        raise RuntimeError(
            f"GOOGLE_APPLICATION_CREDENTIALS file not found: {p}. "
            "Use a repo-relative path or paste JSON in Streamlit secrets."
        )
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p)


def resolve_gcp_project_id() -> str:
    """
    Project id for Vertex / ADC. Streamlit Cloud often has JSON creds but no
    GOOGLE_CLOUD_PROJECT env var — align with bigquery_client secret names.
    """
    for env_key in ("GOOGLE_CLOUD_PROJECT", "GCP_PROJECT", "GCLOUD_PROJECT"):
        v = os.getenv(env_key, "").strip()
        if v:
            return v
    for secret_key in ("GOOGLE_CLOUD_PROJECT", "GCP_PROJECT_ID", "BQ_PROJECT_ID"):
        v = str(get_secret_value(secret_key, "") or "").strip()
        if v:
            return v
    adc_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if adc_path and not adc_path.startswith("{") and Path(adc_path).is_file():
        try:
            with open(adc_path, encoding="utf-8") as handle:
                info = json.load(handle)
            pid = str(info.get("project_id") or "").strip()
            if pid:
                return pid
        except (OSError, json.JSONDecodeError, TypeError):
            pass
    return ""


def ensure_google_cloud_project_env() -> None:
    """Set GOOGLE_CLOUD_PROJECT so vertexai / aiplatform find the project on Cloud."""
    if os.getenv("GOOGLE_CLOUD_PROJECT", "").strip():
        return
    pid = resolve_gcp_project_id()
    if pid:
        os.environ["GOOGLE_CLOUD_PROJECT"] = pid
        os.environ.setdefault("GCP_PROJECT", pid)


def get_secret_value(name: str, default: Any = "") -> Any:
    """Read a config value from env vars, then Streamlit secrets."""
    value = os.getenv(name)
    if value not in (None, ""):
        return value

    try:
        import streamlit as st

        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass

    return default


def get_secret(name: str, default: str = "") -> str:
    """String wrapper for secret values."""
    raw: Any = get_secret_value(name, default)
    return str(raw)


def parse_service_account_info(raw_value: Any) -> dict[str, Any]:
    """
    Parse service account credentials from:
    - mapping/table (best for st.secrets)
    - JSON string
    - JSON-like string where private_key contains literal newlines
    """
    if isinstance(raw_value, Mapping):
        return dict(raw_value)

    raw = str(raw_value or "").strip()
    if not raw:
        raise ValueError("Empty service account credentials.")

    try:
        return dict(json.loads(raw))
    except json.JSONDecodeError:
        pass

    # Recover from multiline TOML basic strings that turned \n into literal newlines.
    fixed = re.sub(
        r'("private_key"\s*:\s*")(.*?)(")',
        lambda m: m.group(1) + m.group(2).replace("\r", "\\r").replace("\n", "\\n") + m.group(3),
        raw,
        flags=re.DOTALL,
    )
    return dict(json.loads(fixed))
