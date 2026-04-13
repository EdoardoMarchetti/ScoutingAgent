"""RAG service for player scouting reports — document indexing and retrieval."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from langchain_core.documents import Document
from langchain_google_vertexai import VertexAIEmbeddings

from services.runtime_secrets import (
    ensure_google_application_credentials_file,
    ensure_google_cloud_project_env,
)


def _ensure_vertex_env() -> None:
    """Ensure Vertex AI environment is configured."""
    ensure_google_application_credentials_file()
    ensure_google_cloud_project_env()


def get_embeddings_model() -> VertexAIEmbeddings:
    """Create Vertex AI embeddings model."""
    _ensure_vertex_env()
    return VertexAIEmbeddings(
        model_name="text-embedding-004",
        project=None,  # Auto-resolved from env
    )


def load_reports_from_directory(reports_dir: Path) -> list[Document]:
    """
    Load all JSON reports from a directory into LangChain Documents.
    
    Expected structure: each .json file contains the full scouting state with:
    - match_header: {match_label, player_name, team_name, competition_name, match_date}
    - scout: {report_text}
    - player_stats_summary: {summary}
    - possession_comments: {comments}
    - visualizations: {various viz blocks with descriptions}
    """
    documents = []
    
    if not reports_dir.exists():
        return documents
    
    for json_file in reports_dir.glob("*.json"):
        try:
            with open(json_file, encoding="utf-8") as f:
                report_data = json.load(f)
            
            # Extract metadata
            match_header = report_data.get("match_header", {})
            match_id = report_data.get("match_id", "unknown")
            player_id = report_data.get("player_id", "unknown")
            
            metadata = {
                "source": str(json_file),
                "match_id": str(match_id),
                "player_id": str(player_id),
                "player_name": match_header.get("player_name", "Unknown"),
                "team_name": match_header.get("team_name", "Unknown"),
                "match_label": match_header.get("match_label", "Unknown"),
                "competition_name": match_header.get("competition_name", "Unknown"),
                "match_date": match_header.get("match_date", "Unknown"),
            }
            
            # Combine text content
            text_parts = []
            
            # Match context
            text_parts.append(f"Match: {metadata['match_label']}")
            text_parts.append(f"Player: {metadata['player_name']} ({metadata['team_name']})")
            text_parts.append(f"Competition: {metadata['competition_name']}")
            text_parts.append(f"Date: {metadata['match_date']}")
            text_parts.append("")
            
            # Scout report
            scout_text = report_data.get("scout", {}).get("report_text", "").strip()
            if scout_text:
                text_parts.append("## Scouting Report")
                text_parts.append(scout_text)
                text_parts.append("")
            
            # Statistical summary
            stats_text = report_data.get("player_stats_summary", {}).get("summary", "").strip()
            if stats_text:
                text_parts.append("## Statistical Summary")
                text_parts.append(stats_text)
                text_parts.append("")
            
            # Possession comments
            poss_text = report_data.get("possession_comments", {}).get("comments", "").strip()
            if poss_text:
                text_parts.append("## Possession Analysis")
                text_parts.append(poss_text)
                text_parts.append("")
            
            # Visualization descriptions
            viz_descriptions = []
            for key in ["duels_visualizations", "recoveries_and_interceptions_visualization",
                        "player_heatmap", "pass_start_network_visualization",
                        "receiving_network_visualization", "shot_map_visualization",
                        "crosses_map_visualization"]:
                viz_block = report_data.get(key, {})
                if isinstance(viz_block, dict):
                    desc = viz_block.get("description", "").strip()
                    caption = viz_block.get("caption", "").strip()
                    if desc:
                        viz_descriptions.append(f"{caption}: {desc}" if caption else desc)
            
            if viz_descriptions:
                text_parts.append("## Visualization Insights")
                text_parts.extend(viz_descriptions)
            
            full_text = "\n".join(text_parts).strip()
            
            if full_text:
                doc = Document(
                    page_content=full_text,
                    metadata=metadata,
                )
                documents.append(doc)
        
        except Exception as e:
            # Skip invalid files
            print(f"Warning: Failed to load {json_file}: {e}")
            continue
    
    return documents


def create_vector_store_faiss(documents: list[Document]) -> Any:
    """
    Create a FAISS vector store from documents.
    
    Returns the FAISS index (requires langchain-community).
    """
    try:
        from langchain_community.vectorstores import FAISS
    except ImportError:
        raise RuntimeError(
            "FAISS vector store requires 'langchain-community' and 'faiss-cpu'. "
            "Install with: pip install langchain-community faiss-cpu"
        )
    
    if not documents:
        raise ValueError("No documents to index")
    
    embeddings = get_embeddings_model()
    vectorstore = FAISS.from_documents(documents, embeddings)
    return vectorstore


def save_vector_store(vectorstore: Any, save_path: Path) -> None:
    """Save FAISS vector store to disk."""
    vectorstore.save_local(str(save_path))


def load_vector_store(load_path: Path) -> Any:
    """Load FAISS vector store from disk."""
    try:
        from langchain_community.vectorstores import FAISS
    except ImportError:
        raise RuntimeError(
            "FAISS vector store requires 'langchain-community' and 'faiss-cpu'. "
            "Install with: pip install langchain-community faiss-cpu"
        )
    
    embeddings = get_embeddings_model()
    vectorstore = FAISS.load_local(
        str(load_path),
        embeddings,
        allow_dangerous_deserialization=True,  # We control the source
    )
    return vectorstore


def compute_reports_hash(reports_dir: Path) -> str:
    """Compute hash of all report files for cache invalidation."""
    if not reports_dir.exists():
        return ""
    
    file_hashes = []
    for json_file in sorted(reports_dir.glob("*.json")):
        try:
            file_hashes.append(hashlib.md5(json_file.read_bytes()).hexdigest())
        except Exception:
            continue
    
    combined = "".join(file_hashes)
    return hashlib.md5(combined.encode()).hexdigest()
