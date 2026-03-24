from __future__ import annotations

import base64
import re
from io import StringIO
from typing import Any

import pandas as pd
import streamlit as st

from services.chatbot_service import chat

_MD_TABLE_RE = re.compile(r"((?:^\|.+\|$\n?){2,})", re.MULTILINE)


def _try_parse_md_table(block: str) -> pd.DataFrame | None:
    try:
        lines = [l for l in block.strip().splitlines() if l.strip()]
        if len(lines) < 2:
            return None
        if all(
            set(c) <= {"-", "|", ":", " "}
            for c in lines[1].strip().split("|")
            if c.strip()
        ):
            lines.pop(1)
        csv_lines: list[str] = []
        for line in lines:
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            csv_lines.append(",".join(cells))
        df = pd.read_csv(StringIO("\n".join(csv_lines)))
        if df.empty or len(df.columns) < 2:
            return None
        return df
    except Exception:
        return None


def _render_text_block(text: str) -> None:
    parts = _MD_TABLE_RE.split(text)
    for part in parts:
        stripped = part.strip()
        if not stripped:
            continue
        if stripped.startswith("|"):
            df = _try_parse_md_table(stripped)
            if df is not None:
                st.dataframe(df, use_container_width=True)
                continue
        st.markdown(stripped)


def _render_chart_block(block: dict[str, Any]) -> None:
    vega_config = block.get("vega_config")
    image_b64 = block.get("image_base64")

    if vega_config and isinstance(vega_config, dict):
        try:
            st.vega_lite_chart(vega_config, use_container_width=True)
            return
        except Exception:
            pass

    if image_b64:
        try:
            img_bytes = base64.b64decode(image_b64)
            st.image(img_bytes, use_container_width=True)
            return
        except Exception:
            pass


def _render_table_block(block: dict[str, Any]) -> None:
    rows = block.get("data")
    csv_text = block.get("csv")
    df: pd.DataFrame | None = None

    if isinstance(rows, list) and rows:
        try:
            df = pd.DataFrame(rows)
        except Exception:
            pass
    elif isinstance(csv_text, str) and csv_text.strip():
        try:
            df = pd.read_csv(StringIO(csv_text))
        except Exception:
            pass

    if df is not None and not df.empty:
        st.dataframe(df, use_container_width=True)


def _render_content_blocks(blocks: list[dict[str, Any]]) -> None:
    if not blocks:
        st.info("The agent returned an empty response. Try rephrasing your question.")
        return

    for block in blocks:
        block_type = block.get("type")
        if block_type == "text":
            _render_text_block(block.get("content", ""))
        elif block_type == "chart":
            _render_chart_block(block)
        elif block_type == "table":
            _render_table_block(block)


def _render_history_message(msg: dict[str, Any]) -> None:
    if msg["role"] == "user":
        st.markdown(msg["content"])
    else:
        blocks = msg.get("content_blocks") or []
        if blocks:
            _render_content_blocks(blocks)
        else:
            st.markdown(msg.get("content", ""))


st.title("Scouting Chatbot")
st.caption(
    "Ask questions about matches, players and stats. "
    "The agent uses human-readable labels by default."
)

if "chat_history" not in st.session_state:
    st.session_state.chat_history: list[dict[str, Any]] = []

for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        _render_history_message(msg)

user_input = st.chat_input("Ask something about a match, player or competition...")

if user_input:
    st.session_state.chat_history.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                result = chat(user_input, st.session_state.chat_history)
                content_blocks = result.get("content_blocks") or []
                answer_text = (result.get("answer_text") or "").strip()
                if not content_blocks:
                    content_blocks = [{"type": "text", "content": answer_text or "I couldn't generate an answer. Please try rephrasing."}]
            except Exception as exc:
                content_blocks = [{"type": "text", "content": f"Error: {exc}"}]
                answer_text = content_blocks[0]["content"]

        _render_content_blocks(content_blocks)
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": answer_text,
            "content_blocks": content_blocks,
        })

if st.session_state.chat_history:
    if st.button("Clear conversation", type="secondary"):
        st.session_state.chat_history = []
        st.rerun()
