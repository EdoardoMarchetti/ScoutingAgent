from __future__ import annotations

import base64
import io
import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]


def branding_logo_path() -> str:
    p = REPO_ROOT / "images" / "sport_data_campus.png"
    return str(p) if p.is_file() else "images/sport_data_campus.png"


def fig_to_markdown_png(fig: plt.Figure, *, dpi: int = 110) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"![](data:image/png;base64,{b64})"


def _json_default(o: Any) -> Any:
    if isinstance(o, (np.integer, np.floating)):
        return float(o) if isinstance(o, np.floating) else int(o)
    if isinstance(o, np.bool_):
        return bool(o)
    if pd.isna(o):
        return None
    return str(o)


def structured_json_for_llm(data: dict[str, Any], max_chars: int = 14_000) -> str:
    s = json.dumps(data, indent=2, default=_json_default)
    if len(s) > max_chars:
        return s[: max_chars - 20] + "\n... (truncated)"
    return s


def df_sample_records(df: pd.DataFrame, n: int = 30, cols: list[str] | None = None) -> list[dict[str, Any]]:
    if df.empty:
        return []
    d = df if cols is None else df[[c for c in cols if c in df.columns]]
    d = d.head(n).replace({np.nan: None})
    return d.to_dict(orient="records")


def passes_table_records(df: pd.DataFrame, *, max_rows: int = 800) -> list[dict[str, Any]]:
    """JSON-safe rows for Streamlit / session state (no numpy scalars)."""
    if df.empty:
        return []
    return json.loads(df.head(int(max_rows)).to_json(orient="records", date_format="iso"))


def pitch_zone_label_from_xy(x: Any, y: Any) -> str | None:
    """3x3 juego de posicion label from Wyscout x/y in possessing-team frame."""
    try:
        xf = float(x)
        yf = float(y)
    except (TypeError, ValueError):
        return None
    if pd.isna(xf) or pd.isna(yf):
        return None
    if xf < 33:
        third = "Defensive third"
    elif xf <= 66:
        third = "Middle third"
    else:
        third = "Attacking third"

    if yf < 33:
        channel = "left channel"
    elif yf <= 66:
        channel = "central channel"
    else:
        channel = "right channel"
    return f"{third}, {channel}"


def add_pitch_zone_columns(
    df: pd.DataFrame,
    *,
    x_col: str = "location_x",
    y_col: str = "location_y",
    out_col: str = "pitch_zone",
) -> pd.DataFrame:
    out = df.copy()
    if x_col in out.columns and y_col in out.columns:
        out[out_col] = out.apply(
            lambda r: pitch_zone_label_from_xy(r.get(x_col), r.get(y_col)),
            axis=1,
        )
    return out


def summarize_duels_df(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"n_events": 0}
    d = add_pitch_zone_columns(df, x_col="location_x", y_col="location_y", out_col="pitch_zone")
    out: dict[str, Any] = {"n_events": int(len(d))}
    if "is_duel_won" in df.columns:
        s = d["is_duel_won"].dropna()
        if len(s):
            try:
                won = s.astype(bool) | (s.astype(int) == 1)
                out["n_won"] = int(won.sum())
                out["n_lost"] = int((~won).sum())
            except (ValueError, TypeError):
                pass
    if "duel_type" in d.columns:
        out["by_duel_type"] = d.groupby("duel_type").size().astype(int).to_dict()
    if "pitch_zone" in d.columns:
        out["by_pitch_zone"] = (
            d["pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    out["sample_events"] = df_sample_records(
        d,
        18,
        [
            "minute",
            "second",
            "pitch_zone",
            "duel_type",
            "is_duel_won",
            "is_sliding_tackle",
        ],
    )
    return out


def summarize_regains_df(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"n_events": 0}
    d = add_pitch_zone_columns(df, x_col="location_x", y_col="location_y", out_col="pitch_zone")
    out: dict[str, Any] = {"n_events": int(len(d))}
    if "is_interception" in df.columns:
        out["n_interception_flag"] = int(d["is_interception"].fillna(0).astype(int).sum())
    if "is_recovery" in df.columns:
        out["n_recovery_flag"] = int(d["is_recovery"].fillna(0).astype(int).sum())
    if "is_counterpressing" in df.columns:
        out["n_counterpressing"] = int(d["is_counterpressing"].fillna(False).astype(bool).sum())
    if "pitch_zone" in d.columns:
        out["by_pitch_zone"] = (
            d["pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    out["sample_events"] = df_sample_records(
        d,
        18,
        [
            "minute",
            "second",
            "pitch_zone",
            "is_interception",
            "is_recovery",
            "is_counterpressing",
            "regain_signal_type",
        ],
    )
    return out


def summarize_passes_out_df(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"n_passes": 0}
    d = df.dropna(subset=["start_x", "start_y", "end_x", "end_y"], how="any")
    out: dict[str, Any] = {"n_passes": int(len(d))}
    if d.empty:
        return out
    d = add_pitch_zone_columns(d, x_col="start_x", y_col="start_y", out_col="start_pitch_zone")
    d = add_pitch_zone_columns(d, x_col="end_x", y_col="end_y", out_col="end_pitch_zone")
    if "start_pitch_zone" in d.columns:
        out["start_zone_profile"] = (
            d["start_pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    if "end_pitch_zone" in d.columns:
        out["end_zone_profile"] = (
            d["end_pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    if "recipient_id" in d.columns:
        top = (
            d.groupby(["recipient_id", "recipient_name"], dropna=False)
            .size()
            .reset_index(name="n")
            .sort_values("n", ascending=False)
            .head(8)
        )
        out["top_recipients"] = top.to_dict(orient="records")
    out["sample_passes"] = df_sample_records(
        d,
        18,
        ["minute", "second", "start_pitch_zone", "end_pitch_zone", "recipient_name"],
    )
    return out


def summarize_passes_in_df(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"n_passes": 0}
    d = df.dropna(subset=["start_x", "start_y", "end_x", "end_y"], how="any")
    out: dict[str, Any] = {"n_passes": int(len(d))}
    if d.empty:
        return out
    d = add_pitch_zone_columns(d, x_col="start_x", y_col="start_y", out_col="start_pitch_zone")
    d = add_pitch_zone_columns(d, x_col="end_x", y_col="end_y", out_col="end_pitch_zone")
    if "start_pitch_zone" in d.columns:
        out["origin_zone_profile"] = (
            d["start_pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    if "end_pitch_zone" in d.columns:
        out["reception_zone_profile"] = (
            d["end_pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    if "passer_id" in d.columns:
        top = (
            d.groupby(["passer_id", "passer_name"], dropna=False)
            .size()
            .reset_index(name="n")
            .sort_values("n", ascending=False)
            .head(8)
        )
        out["top_passers"] = top.to_dict(orient="records")
    out["sample_passes"] = df_sample_records(
        d,
        18,
        ["minute", "second", "start_pitch_zone", "end_pitch_zone", "passer_name"],
    )
    return out


def summarize_touches_df(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"n_touches": 0}
    d = df.dropna(subset=["location_x", "location_y"], how="any")
    out: dict[str, Any] = {"n_touches": int(len(d))}
    if d.empty:
        return out
    d = add_pitch_zone_columns(d, x_col="location_x", y_col="location_y", out_col="pitch_zone")
    if "pitch_zone" in d.columns:
        out["zone_profile"] = (
            d["pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    if "type_primary" in d.columns:
        out["by_event_type"] = d["type_primary"].value_counts().head(12).astype(int).to_dict()
    out["sample_events"] = df_sample_records(
        d,
        16,
        ["minute", "second", "pitch_zone", "type_primary"],
    )
    return out


def summarize_shots_df(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"n_shots": 0}
    out: dict[str, Any] = {"n_shots": int(len(df))}
    xg = pd.to_numeric(df.get("shot_xg", df.get("xg", 0)), errors="coerce").fillna(0)
    out["total_xg"] = float(xg.sum())
    if "shot_is_goal" in df.columns:
        out["n_goals"] = int(df["shot_is_goal"].fillna(False).astype(bool).sum())
    elif "shot_outcome" in df.columns:
        out["n_goals"] = int(df["shot_outcome"].astype(str).str.contains("goal", case=False, na=False).sum())
    d = add_pitch_zone_columns(df, x_col="location_x", y_col="location_y", out_col="pitch_zone")
    if "pitch_zone" in d.columns:
        out["shot_zone_profile"] = (
            d["pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    out["sample_shots"] = df_sample_records(
        d,
        16,
        ["minute", "second", "pitch_zone", "shot_xg", "shot_outcome"],
    )
    return out


def summarize_crosses_df(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"n_events": 0}
    out: dict[str, Any] = {"n_events": int(len(df))}
    if "is_cross" in df.columns:
        out["n_cross"] = int(df["is_cross"].fillna(False).astype(bool).sum())
    if "is_key_pass" in df.columns:
        out["n_key_pass"] = int(df["is_key_pass"].fillna(False).astype(bool).sum())
    if "pass_accurate" in df.columns:
        pa = df["pass_accurate"].dropna()
        if len(pa):
            out["accurate_rate"] = float(pa.astype(bool).mean())
    d = add_pitch_zone_columns(df, x_col="location_x", y_col="location_y", out_col="start_pitch_zone")
    d = add_pitch_zone_columns(d, x_col="pass_end_x", y_col="pass_end_y", out_col="end_pitch_zone")
    if "start_pitch_zone" in d.columns:
        out["start_zone_profile"] = (
            d["start_pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    if "end_pitch_zone" in d.columns:
        out["end_zone_profile"] = (
            d["end_pitch_zone"].dropna().value_counts().head(9).astype(int).to_dict()
        )
    out["sample_events"] = df_sample_records(
        d,
        16,
        [
            "minute",
            "second",
            "start_pitch_zone",
            "end_pitch_zone",
            "is_cross",
            "is_key_pass",
            "pass_accurate",
        ],
    )
    return out


def build_viz_description_prompt(
    *,
    viz_title: str,
    viz_kind_note: str,
    player_name: str,
    match_label: str,
    structured_json: str,
) -> str:
    return (
        "You are an expert football scout writing a concise analytic caption for a chart.\n\n"
        f"Player: {player_name}\n"
        f"Match: {match_label}\n"
        f"Visualization: {viz_title}\n"
        f"Note: {viz_kind_note}\n\n"
        "Pitch orientation: vertical, attacking direction toward the top.\n"
        "Use zones/channels language (defensive/middle/attacking third; left/central/right channel).\n"
        "Do not refer to raw coordinates or exact medians.\n"
        "Avoid exact counts unless essential; prefer qualitative intensity words (few, several, frequent, isolated).\n\n"
        "Structured data (JSON) derived from the same events used to build the chart:\n"
        f"{structured_json}\n\n"
        "Write exactly 2 short paragraphs (max 4 sentences total).\n"
        "Reference what the chart shows (density, clusters, links, outcomes) using only this JSON.\n"
        "Do not invent events. Neutral scouting tone.\n"
    )
