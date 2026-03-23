from __future__ import annotations

import os

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_google_vertexai import ChatVertexAI


def create_graph_llm():
    """
    Build an LLM instance for LangGraph report generation.

    Strategy:
    - If GRAPH_LLM_BACKEND=google_genai, use ChatGoogleGenerativeAI.
    - Otherwise default to Vertex AI (ChatVertexAI).
    """
    backend = (os.getenv("GRAPH_LLM_BACKEND") or "vertex").strip().lower()
    model_name = (os.getenv("GRAPH_LLM_MODEL") or "gemini-2.5-pro").strip()
    temperature = float(os.getenv("GRAPH_LLM_TEMPERATURE") or "0.2")

    if backend == "google_genai":
        return ChatGoogleGenerativeAI(
            model=model_name,
            temperature=temperature,
        )

    location = (os.getenv("GOOGLE_CLOUD_LOCATION") or "europe-west1").strip()
    return ChatVertexAI(
        model=model_name,
        location=location,
        temperature=temperature,
    )
