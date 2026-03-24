# Report RAG Feature

## Overview

The Report RAG (Retrieval-Augmented Generation) page allows you to query across multiple player scouting reports using semantic search and AI-powered analysis.

## How It Works

1. **Document Collection**: Generate player scouting reports from the "Player Scouting" page
2. **Automatic Indexing**: Reports are automatically saved as JSON files and indexed with vector embeddings
3. **Semantic Search**: Ask natural language questions that are matched against all indexed reports
4. **AI Analysis**: An LLM synthesizes answers from relevant reports, citing specific matches and players

## Setup

### 1. Generate Reports

1. Go to **Player Scouting** page
2. Select a match and player
3. Generate a scouting report
4. Click **Export PDF** — this automatically saves a JSON file to `player_reports/`

Repeat for multiple players/matches to build your knowledge base.

### 2. Use RAG Page

1. Navigate to **Report RAG** page
2. The system will auto-index all JSON files in `player_reports/`
3. Ask questions in natural language

## Example Queries

- "How did Lamine Yamal perform in attacking phases across his matches?"
- "Compare defensive actions between Player A and Player B"
- "What were the key insights from Match X?"
- "Show me all high-intensity pressing situations across the reports"
- "Summarize passing patterns for Player Y in his last 3 games"

## Technical Details

### Architecture

- **Embeddings**: Vertex AI `text-embedding-004` model
- **Vector Store**: FAISS (local, persisted to disk)
- **Retrieval**: Similarity search with configurable threshold
- **LLM**: Same Vertex AI model as the rest of the app (gemini-2.5-flash-lite)

### Document Structure

Each JSON report contains:
- Match metadata (player, team, competition, date)
- Scout report text
- Statistical summary
- Possession analysis
- Visualization descriptions (duels, passes, shots, etc.)

### Index Management

- Index is automatically created on first use
- Cached in session state for performance
- Persisted to `player_reports/.vector_index/`
- Auto-reindex when new reports are added (hash-based detection)
- Manual reindex via "🔄 Reindex" button

## Configuration

### Retrieval Settings

- **Number of documents**: How many reports to retrieve (default: 5)
- **Similarity threshold**: Minimum relevance score 0-1 (default: 0.5)
  - Lower = more lenient, retrieves more documents
  - Higher = stricter, only highly relevant documents

### Directory Path

By default, reports are stored in `player_reports/` relative to the project root. You can change this in the RAG page settings.

## Tips

1. **Build a diverse corpus**: Generate reports for different players, teams, and competitions
2. **Use specific questions**: More specific queries = better retrieval
3. **Iterate on threshold**: If you get too few/many results, adjust the similarity threshold
4. **Comparative analysis**: Ask questions that compare multiple players or matches
5. **Context matters**: The LLM has access to full report text, so detailed questions work well

## Limitations

- Embeddings require network access to Vertex AI (online only)
- Index size grows with number of reports (~1-2MB per 100 reports)
- First-time indexing can take 1-2 minutes for 50+ reports
- Retrieval quality depends on report content richness

## Dependencies

- `langchain-community` — Document loaders and vector stores
- `faiss-cpu` — Fast similarity search
- `langchain-google-vertexai` — Vertex AI embeddings and LLM

All dependencies are included in `pyproject.toml` and auto-installed with `uv sync`.
