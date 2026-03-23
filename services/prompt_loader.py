from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_prompt(prompt_name: str, version: str = "v1") -> dict[str, Any]:
    file_name = f"{prompt_name}_{version}.yaml"
    prompt_path = _repo_root() / "prompts" / file_name
    if not prompt_path.is_file():
        return {}
    payload = yaml.safe_load(prompt_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def render_prompt(template: str, values: dict[str, str]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace(f"{{{{ {key} }}}}", value)
    return rendered
