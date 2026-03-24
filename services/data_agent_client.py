from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from services.bigquery_client import get_bq_project_id
from services.runtime_secrets import (
    get_secret,
    get_secret_value,
    parse_service_account_info,
)

_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_API_BASE = "https://geminidataanalytics.googleapis.com/v1alpha"


def _get_access_token() -> str:
    credentials_value = get_secret_value("DATA_AGENT_CREDENTIALS", "")
    if not credentials_value:
        raise RuntimeError("Missing DATA_AGENT_CREDENTIALS in environment.")

    raw = str(credentials_value).strip()
    if raw.startswith("{"):
        info = parse_service_account_info(credentials_value)
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


def _extract_content_blocks(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract an ordered list of content blocks preserving original message order."""
    blocks: list[dict[str, Any]] = []
    final_text_blocks: list[dict[str, Any]] = []
    generic_text_blocks: list[dict[str, Any]] = []
    sql_queries: list[str] = []
    has_final = False

    for msg in messages:
        system = msg.get("systemMessage") or {}
        group_id = msg.get("groupId") or system.get("groupId")

        text_msg = system.get("text") or {}
        parts = text_msg.get("parts") or []
        text_type = str(text_msg.get("textType") or "").strip()
        if isinstance(parts, list):
            clean = "\n".join(str(p).strip() for p in parts if str(p).strip())
            if clean:
                block = {"type": "text", "content": clean, "group_id": group_id}
                if text_type == "FINAL_RESPONSE":
                    has_final = True
                    final_text_blocks.append(block)
                elif text_type not in {"THOUGHT", "PROGRESS"}:
                    generic_text_blocks.append(block)

        chart_msg = system.get("chart")
        if isinstance(chart_msg, dict):
            result = chart_msg.get("result") or {}
            vega_config = result.get("vegaConfig")
            image_blob = result.get("image") or {}
            image_data = image_blob.get("data")
            image_mime = image_blob.get("mimeType", "image/png")
            if vega_config or image_data:
                blocks.append({
                    "type": "chart",
                    "vega_config": vega_config,
                    "image_base64": image_data,
                    "image_mime": image_mime,
                    "group_id": group_id,
                })

        data_msg = system.get("data")
        if isinstance(data_msg, dict):
            generated_sql = data_msg.get("generatedSql")
            if generated_sql and isinstance(generated_sql, str) and generated_sql.strip():
                sql_queries.append(generated_sql.strip())

            result = data_msg.get("result") or {}
            rows = result.get("data")
            schema = result.get("schema")
            name = result.get("name", "")
            if isinstance(rows, list) and rows:
                blocks.append({
                    "type": "table",
                    "name": name,
                    "schema": schema,
                    "data": rows,
                    "group_id": group_id,
                })

        analysis_msg = system.get("analysis")
        if isinstance(analysis_msg, dict):
            event = analysis_msg.get("progressEvent") or {}

            vega_json_str = event.get("resultVegaChartJson")
            if isinstance(vega_json_str, str) and vega_json_str.strip():
                try:
                    vega_spec = json.loads(vega_json_str)
                    blocks.append({
                        "type": "chart",
                        "vega_config": vega_spec,
                        "image_base64": None,
                        "image_mime": None,
                        "group_id": group_id,
                    })
                except json.JSONDecodeError:
                    pass

            csv_data = event.get("resultCsvData")
            if isinstance(csv_data, str) and csv_data.strip():
                blocks.append({
                    "type": "table",
                    "name": "analysis_result",
                    "schema": None,
                    "csv": csv_data,
                    "data": None,
                    "group_id": group_id,
                })

    text_blocks = final_text_blocks if has_final else generic_text_blocks

    ordered = _interleave_text_with_visuals(text_blocks, blocks)
    return ordered, sql_queries


def _interleave_text_with_visuals(
    text_blocks: list[dict[str, Any]],
    visual_blocks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Interleave text and visual blocks by group_id when possible, else by order."""
    group_visuals: dict[Any, list[dict[str, Any]]] = {}
    ungrouped_visuals: list[dict[str, Any]] = []
    for vb in visual_blocks:
        gid = vb.get("group_id")
        if gid is not None:
            group_visuals.setdefault(gid, []).append(vb)
        else:
            ungrouped_visuals.append(vb)

    emitted_groups: set[Any] = set()
    ordered: list[dict[str, Any]] = []

    for tb in text_blocks:
        ordered.append(tb)
        gid = tb.get("group_id")
        if gid is not None and gid in group_visuals and gid not in emitted_groups:
            ordered.extend(group_visuals[gid])
            emitted_groups.add(gid)

    for gid, vbs in group_visuals.items():
        if gid not in emitted_groups:
            ordered.extend(vbs)

    ordered.extend(ungrouped_visuals)
    return ordered


def query_data_agent(user_message: str, use_conversation_context: bool = False) -> dict[str, Any]:
    del use_conversation_context  # Reserved for future multi-turn support.

    project_id = get_bq_project_id()
    location = get_secret("DATA_AGENT_LOCATION", "").strip()
    agent_id = get_secret("SCOUTING_AGENT_ID", "").strip()
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

    payload: Any
    try:
        payload = response.json()
        if isinstance(payload, list):
            msgs = [p for p in payload if isinstance(p, dict)]
        elif isinstance(payload, dict):
            msgs = [payload]
        else:
            msgs = []
    except ValueError:
        msgs = []
        payload = []
        for line in response.text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                msgs.append(obj)
        payload = msgs

    content_blocks, sql_queries = _extract_content_blocks(msgs)

    text_parts = [b["content"] for b in content_blocks if b["type"] == "text"]
    answer_text = "\n".join(text_parts).strip()

    return {
        "answer_text": answer_text,
        "content_blocks": content_blocks,
        "sql_queries": sql_queries,
        "raw_response": payload,
        "error": "",
    }
