from __future__ import annotations

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_google_vertexai import ChatVertexAI

from services.runtime_secrets import get_secret


def create_graph_llm():
    """
    Build an LLM instance for LangGraph report generation.

    Strategy:
    - If GRAPH_LLM_BACKEND=google_genai, use ChatGoogleGenerativeAI.
    - Otherwise default to Vertex AI (ChatVertexAI).
    """
    backend = get_secret("GRAPH_LLM_BACKEND", "vertex").strip().lower()
    model_name = get_secret("GRAPH_LLM_MODEL", "gemini-2.5-pro").strip()
    temperature = float(get_secret("GRAPH_LLM_TEMPERATURE", "0.2"))

    if backend == "google_genai":
        return ChatGoogleGenerativeAI(
            model=model_name,
            temperature=temperature,
        )

    location = get_secret("GOOGLE_CLOUD_LOCATION", "europe-west1").strip()
    return ChatVertexAI(
        model=model_name,
        location=location,
        temperature=temperature,
    )
