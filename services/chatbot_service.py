from __future__ import annotations

from typing import Any

from services.data_agent_client import query_data_agent

SYSTEM_PREAMBLE = (
    "You are a football scouting assistant. Follow these rules strictly:\n"
    "1. Always use human-readable labels for entities (team names, player names, "
    "competition names, etc.). Never expose internal IDs to the user.\n"
    "2. Do NOT show your reasoning, thinking process, or intermediate steps.\n"
    "3. Do NOT include SQL code in your answer unless the user explicitly asks for it.\n"
    "4. When returning tabular data, format it as a clean markdown table.\n"
    "5. Be concise and direct.\n"
    "6. For complex analyses, break the work into smaller steps: first retrieve and "
    "verify the raw data, then compute metrics, then draw conclusions. Never combine "
    "multiple unrelated aggregations in a single query if they can be computed separately.\n"
    "7. After computing a metric, sanity-check it against the raw data before presenting "
    "it (e.g. total goals must equal the sum of per-match goals; xG must be non-negative "
    "and less than the number of shots).\n"
    "8. Prefer CTEs over nested subqueries for readability and debuggability.\n"
    "9. When comparing players or teams, compute each entity's metrics independently "
    "first, then join/compare. Do not attempt a single massive query that does everything.\n\n"
)


def chat(user_message: str, history: list[dict[str, str]]) -> dict[str, Any]:
    context_parts: list[str] = []
    for msg in history[-10:]:
        role = msg.get("role", "user")
        content = (msg.get("content") or "").strip()
        if content:
            context_parts.append(f"[{role}]: {content}")

    full_prompt = SYSTEM_PREAMBLE
    if context_parts:
        full_prompt += "Conversation so far:\n" + "\n".join(context_parts) + "\n\n"
    full_prompt += f"[user]: {user_message}"

    result = query_data_agent(user_message=full_prompt, use_conversation_context=False)
    return result
