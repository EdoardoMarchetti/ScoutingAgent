from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from services.bigquery_client import get_bq_project_id

_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_API_BASE = "https://geminidataanalytics.googleapis.com/v1alpha"


def _get_access_token() -> str:
    raw = os.getenv("DATA_AGENT_CREDENTIALS", "").strip()
    if not raw:
        raise RuntimeError("Missing DATA_AGENT_CREDENTIALS in environment.")

    if raw.startswith("{"):
        info = json.loads(raw)
        creds = service_account.Credentials.from_service_account_info(info, scopes=[_SCOPE])
    else:
        credentials_path = Path(raw).expanduser()
        if not credentials_path.is_absolute():
            credentials_path = (Path(__file__).resolve().parents[1] / credentials_path).resolve()
        if not credentials_path.is_file():
            raise RuntimeError(f"Service account file not found: {credentials_path}")
        creds = service_account.Credentials.from_service_account_file(
            str(credentials_path), scopes=[_SCOPE]
        )
    creds.refresh(Request())
    token = creds.token
    if not token:
        raise RuntimeError("Unable to refresh service account access token.")
    return token


def _extract_answer(messages: list[dict[str, Any]]) -> str:
    final_parts: list[str] = []
    generic_parts: list[str] = []
    for msg in messages:
        system = msg.get("systemMessage") or {}
        text = system.get("text") or {}
        parts = text.get("parts") or []
        text_type = str(text.get("textType") or "").strip()
        if not isinstance(parts, list):
            continue

        clean_parts = [str(p).strip() for p in parts if str(p).strip()]
        if not clean_parts:
            continue

        if text_type == "FINAL_RESPONSE":
            final_parts.extend(clean_parts)
        elif text_type in {"THOUGHT", "PROGRESS"}:
            # Explicitly ignore reasoning/progress messages.
            continue
        else:
            generic_parts.extend(clean_parts)

    if final_parts:
        return "\n".join(final_parts).strip()
    if generic_parts:
        return "\n".join(generic_parts).strip()
    return ""


def query_data_agent(user_message: str, use_conversation_context: bool = False) -> dict[str, Any]:
    del use_conversation_context  # Reserved for future multi-turn support.

    project_id = get_bq_project_id()
    location = (os.getenv("DATA_AGENT_LOCATION") or "").strip()
    agent_id = (os.getenv("SCOUTING_AGENT_ID") or "").strip()
    if not location:
        raise RuntimeError("Missing DATA_AGENT_LOCATION in environment.")
    if not agent_id:
        raise RuntimeError("Missing SCOUTING_AGENT_ID in environment.")

    token = _get_access_token()
    parent = f"projects/{project_id}/locations/{location}"
    data_agent_name = f"{parent}/dataAgents/{agent_id}"
    url = f"{_API_BASE}/{parent}:chat"

    body = {
        "messages": [{"userMessage": {"text": user_message}}],
        "dataAgentContext": {"dataAgent": data_agent_name},
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = requests.post(url, json=body, headers=headers, timeout=120)
    response.raise_for_status()

    answer_text = ""
    payload: Any
    try:
        payload = response.json()
        if isinstance(payload, list):
            answer_text = _extract_answer([p for p in payload if isinstance(p, dict)])
        elif isinstance(payload, dict):
            answer_text = _extract_answer([payload])
    except ValueError:
        # Fallback for streamed newline-delimited JSON.
        chunks: list[dict[str, Any]] = []
        for line in response.text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                chunks.append(obj)
        answer_text = _extract_answer(chunks)
        payload = chunks

    return {
        "answer_text": answer_text,
        "raw_response": payload,
        "error": "",
    }
