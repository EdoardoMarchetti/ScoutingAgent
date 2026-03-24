from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from typing import Any


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
