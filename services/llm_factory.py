from __future__ import annotations

import os

from langchain_google_vertexai import ChatVertexAI

from services.runtime_secrets import (
    ensure_google_application_credentials_file,
    ensure_google_cloud_project_env,
    get_secret,
    resolve_gcp_project_id,
)


def create_graph_llm():
    """Vertex AI only (LangGraph scouting / viz descriptions)."""
    model_name = get_secret("VERTEX_MODEL_NAME", "gemini-2.5-flash-lite").strip()
    temperature = 0.2

    ensure_google_application_credentials_file()
    ensure_google_cloud_project_env()
    project_id = (
        get_secret("GCP_PROJECT_ID", "").strip()
        or os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
        or resolve_gcp_project_id()
    )
    if not project_id:
        raise RuntimeError(
            "Vertex AI needs a GCP project id. Set GOOGLE_CLOUD_PROJECT or GCP_PROJECT_ID "
            "(or BQ_PROJECT_ID) in Streamlit Cloud secrets / environment, or ensure the "
            "service account JSON includes project_id."
        )
    os.environ["GOOGLE_CLOUD_PROJECT"] = project_id

    location = get_secret("GOOGLE_CLOUD_LOCATION", "europe-west1").strip()
    return ChatVertexAI(
        model=model_name,
        project=project_id,
        location=location,
        temperature=temperature,
    )
