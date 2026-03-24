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
    "5. Be concise and direct.\n\n"
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
