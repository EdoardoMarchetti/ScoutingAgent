"""
Vertical Wyscout pitch visualizations + BigQuery loaders for ScoutingAgent / Streamlit.

Canonical plot names:
  plot_shot_map_vertical, plot_cross_key_pass_map_vertical, plot_player_touch_kde_vertical,
  plot_pass_link_out_kde_vertical, plot_pass_link_in_kde_vertical,
  plot_defensive_duel_map_vertical, plot_regain_map_vertical

Legacy aliases (notebook names) are defined at the bottom of this module.
"""

from __future__ import annotations

import base64
import io
import os
from typing import Any, Callable, Literal

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from google.cloud import bigquery
from matplotlib import cm
from matplotlib.axes import Axes
from matplotlib.colors import LinearSegmentedColormap, Normalize
from matplotlib.contour import QuadContourSet
from mplsoccer import FontManager, VerticalPitch
from PIL import Image

# --- Brand colormaps ----------------------------------------------------------

SDC_CMAP = LinearSegmentedColormap.from_list(
    "sdc",
    ["#2d1b4e", "#7c2d6b", "#e85d8a", "#ff7a33", "#ffd666"],
    N=256,
)
SDC_CMAP_WHITE0 = LinearSegmentedColormap.from_list(
    "sdc_w0",
    ["#ffffff", "#2d1b4e", "#7c2d6b", "#e85d8a", "#ff7a33", "#ffd666"],
    N=256,
)

_ROBOTO_REGULAR_URL = (
    "https://raw.githubusercontent.com/googlefonts/roboto/main/src/hinted/Roboto-Regular.ttf"
)
_ROBOTO_SLAB_URL = (
    "https://raw.githubusercontent.com/google/fonts/main/apache/robotoslab/RobotoSlab%5Bwght%5D.ttf"
)

try:
    FONT_NORMAL = FontManager(_ROBOTO_REGULAR_URL)
except Exception:
    FONT_NORMAL = None

try:
    FONT_BOLD = FontManager(_ROBOTO_SLAB_URL)
except Exception:
    FONT_BOLD = None

# --- Pass-link viz (shared out / in) -----------------------------------------

PASS_VIZ_STAR_SIZE = 200
PASS_VIZ_NODE_SIZE = 200
PASS_VIZ_COUNT_FONTSIZE = 8
PASS_VIZ_MARKERS = ["o", "s", "D", "^", "v"]
PASS_VIZ_COLORS = ["#059669", "#1d4ed8", "#7c3aed", "#db2777", "#ea580c"]
PITCH_LINE_SOFT = "#A8A9AD"
PITCH_LINEWIDTH_SOFT = 1.6


def _img_from_any(source: str | None) -> np.ndarray | None:
    if not source:
        return None
    source = source.strip()
    try:
        if source.startswith("data:image"):
            b64 = source.split(",", 1)[1]
            raw = base64.b64decode(b64)
            return np.array(Image.open(io.BytesIO(raw)).convert("RGBA"))
        if source.startswith("http://") or source.startswith("https://"):
            r = requests.get(source, timeout=15)
            r.raise_for_status()
            return np.array(Image.open(io.BytesIO(r.content)).convert("RGBA"))
        return np.array(Image.open(source).convert("RGBA"))
    except Exception:
        return None


def add_header_branding(
    fig: plt.Figure,
    title: str,
    *,
    subtitle: str | None = None,
    left_logo_path: str = "images/sport_data_campus.png",
    team_img_source: str | None = None,
    player_img_source: str | None = None,
    font_prop: Any = None,
    header_center_y: float = 1.0,
    logo_w: float = 0.16,
    logo_h: float = 0.11,
    right_h: float = 0.10,
    right_w_single: float = 0.08,
    right_w_double: float = 0.07,
) -> None:
    fig.subplots_adjust(top=0.84)
    has_subtitle = bool((subtitle or "").strip())
    title_y = header_center_y + (0.013 if has_subtitle else 0.0)
    fig.text(
        0.5,
        title_y,
        title,
        ha="center",
        va="center",
        fontsize=28,
        fontweight="bold",
        color="#A8A9AD",
        fontproperties=(FONT_BOLD.prop if FONT_BOLD is not None else None),
    )
    if has_subtitle:
        fig.text(
            0.5,
            header_center_y - 0.028,
            str(subtitle).strip(),
            ha="center",
            va="center",
            fontsize=16,
            fontweight="normal",
            color="#A8A9AD",
            alpha=0.9,
            fontproperties=(FONT_NORMAL.prop if FONT_NORMAL is not None else None),
        )

    def _axes_centered(x0: float, w: float, h: float) -> plt.Axes:
        bottom = header_center_y - h / 2
        return fig.add_axes([x0, bottom, w, h])

    left_img = _img_from_any(left_logo_path)
    if left_img is not None:
        ax_left = _axes_centered(0.03, logo_w, logo_h)
        ax_left.imshow(left_img)
        ax_left.axis("off")

    team_img = _img_from_any(team_img_source)
    player_img = _img_from_any(player_img_source)

    if player_img is not None and team_img is not None:
        ax_p = _axes_centered(0.83, right_w_double, right_h)
        ax_p.imshow(player_img)
        ax_p.axis("off")
        ax_t = _axes_centered(0.91, right_w_double, right_h)
        ax_t.imshow(team_img)
        ax_t.axis("off")
    elif player_img is not None:
        ax_p = _axes_centered(0.90, right_w_single, right_h)
        ax_p.imshow(player_img)
        ax_p.axis("off")
    elif team_img is not None:
        ax_t = _axes_centered(0.90, right_w_single, right_h)
        ax_t.imshow(team_img)
        ax_t.axis("off")




def _pitch_link_line(
    pitch: VerticalPitch,
    ax: plt.Axes,
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    color: str,
    z_white: int = 18,
    z_col: int = 19,
) -> None:
    xa = np.asarray([float(x0)], dtype=float)
    ya = np.asarray([float(y0)], dtype=float)
    xb = np.asarray([float(x1)], dtype=float)
    yb = np.asarray([float(y1)], dtype=float)
    pitch.lines(
        xa, ya, xb, yb,
        ax=ax,
        color="white",
        linewidth=2.2,
        zorder=z_white,
        alpha=0.95,
        comet=False,
    )
    pitch.lines(
        xa, ya, xb, yb,
        ax=ax,
        color=color,
        linewidth=1.1,
        zorder=z_col,
        alpha=1.0,
        comet=False,
    )


def _bq_project_id(project_id: str | None) -> str:
    pid = project_id or os.environ.get("GCP_PROJECT_ID")
    if not pid:
        raise ValueError("Set GCP_PROJECT_ID or pass project_id=")
    return pid


def _table_fq(project: str, dataset: str, table: str) -> str:
    return f"`{project}.{dataset}.{table}`"


# --- Data prep (shots) --------------------------------------------------------

def prepare_shot_events_for_plot(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split shot rows into (non_goal, goal) with columns x, y, xg."""
    out = df.copy()
    if "x" not in out.columns:
        out["x"] = pd.to_numeric(out["location_x"], errors="coerce")
    if "y" not in out.columns:
        out["y"] = pd.to_numeric(out["location_y"], errors="coerce")
    if "xg" not in out.columns:
        out["xg"] = pd.to_numeric(out.get("shot_xg"), errors="coerce").fillna(0.0)
    else:
        out["xg"] = pd.to_numeric(out["xg"], errors="coerce").fillna(0.0)
    if "shot_is_goal" in out.columns:
        is_goal = out["shot_is_goal"].astype(bool)
    else:
        is_goal = out.get("shot_outcome", pd.Series("", index=out.index)).astype(str).str.contains(
            "goal", case=False, na=False
        )
    out = out.dropna(subset=["x", "y"])
    out["x"] = out["x"].clip(0, 100)
    out["y"] = out["y"].clip(0, 100)
    return out[~is_goal].copy(), out[is_goal].copy()


# --- BigQuery: loads matching the plots --------------------------------------

def fetch_match_shots_for_player(
    player_id: int,
    match_id: int,
    *,
    project_id: str | None = None,
    dataset: str = "scouting_agent",
) -> dict[str, Any]:
    """Shots + dim image URLs for one player/match."""
    pid = _bq_project_id(project_id)
    client = bigquery.Client(project=pid)
    fq_shots = _table_fq(pid, dataset, "gold_match_shot_event")
    fq_player = _table_fq(pid, dataset, "dim_player")
    fq_team = _table_fq(pid, dataset, "dim_team")
    sql = f"""
    WITH base AS (
      SELECT s.*, p.image_data_url AS player_image_data_url,
             t.image_data_url AS team_image_data_url
      FROM {fq_shots} AS s
      LEFT JOIN {fq_player} AS p ON s.player_id = p.player_id
      LEFT JOIN {fq_team} AS t ON s.team_id = t.team_id
      WHERE s.player_id = @player_id AND s.match_id = @match_id
    )
    SELECT * FROM base ORDER BY minute, second, event_id
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
                bigquery.ScalarQueryParameter("match_id", "INT64", match_id),
            ]
        ),
    )
    df = job.to_dataframe()
    if df.empty:
        return {"shots": df, "player_image_data_url": None, "team_image_data_url": None, "team_id": None}
    pu = df["player_image_data_url"].dropna().iloc[0] if "player_image_data_url" in df.columns else None
    tu = df["team_image_data_url"].dropna().iloc[0] if "team_image_data_url" in df.columns else None
    tid = int(df["team_id"].iloc[0]) if pd.notna(df["team_id"].iloc[0]) else None
    cols = [c for c in df.columns if c not in ("player_image_data_url", "team_image_data_url")]
    return {
        "shots": df[cols].copy(),
        "player_image_data_url": str(pu) if pu is not None and str(pu).strip() else None,
        "team_image_data_url": str(tu) if tu is not None and str(tu).strip() else None,
        "team_id": tid,
    }


def fetch_match_passes_from_player(
    match_id: int,
    player_id: int,
    *,
    project_id: str | None = None,
    dataset: str = "scouting_agent",
    limit: int = 50000,
) -> pd.DataFrame:
    """Passes made by player_id with recipient + coordinates.

    Excludes Wyscout placeholder recipient id 0 and non-teammate recipients
    (dominant match team from silver_match_event must match pass event team_id).
    """
    pid = _bq_project_id(project_id)
    lim = max(1, min(int(limit), 100_000))
    fq = _table_fq(pid, dataset, "gold_match_pass_event")
    se = _table_fq(pid, dataset, "silver_match_event")
    dp = _table_fq(pid, dataset, "dim_player")
    sql = f"""
    WITH player_team_votes AS (
      SELECT
        s.match_id,
        s.player_id,
        s.team_id,
        COUNT(*) AS c
      FROM {se} AS s
      WHERE s.match_id = @match_id
        AND s.player_id IS NOT NULL AND s.player_id != 0
        AND s.team_id IS NOT NULL
      GROUP BY s.match_id, s.player_id, s.team_id
    ),
    player_dominant_team AS (
      SELECT match_id, player_id, team_id AS dominant_team_id
      FROM player_team_votes
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY match_id, player_id ORDER BY c DESC, team_id
      ) = 1
    )
    SELECT p.match_id, p.event_id, p.minute, p.second,
      p.location_x AS start_x, p.location_y AS start_y,
      p.pass_end_x AS end_x, p.pass_end_y AS end_y,
      p.player_id AS passer_id, pas.short_name AS passer_name,
      p.recipient_player_id AS recipient_id, rec.short_name AS recipient_name,
      p.team_id AS passer_team_id
    FROM {fq} AS p
    LEFT JOIN {dp} AS pas ON p.player_id = pas.player_id
    LEFT JOIN {dp} AS rec ON p.recipient_player_id = rec.player_id
    INNER JOIN player_dominant_team AS rdt
      ON rdt.match_id = p.match_id AND rdt.player_id = p.recipient_player_id
    WHERE p.match_id = @match_id AND p.player_id = @player_id
      AND p.recipient_player_id IS NOT NULL AND p.recipient_player_id != 0
      AND p.pass_end_x IS NOT NULL AND p.pass_end_y IS NOT NULL
      AND p.team_id IS NOT NULL
      AND rdt.dominant_team_id = p.team_id
    ORDER BY p.minute, p.second, p.event_id LIMIT @limit
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("match_id", "INT64", int(match_id)),
            bigquery.ScalarQueryParameter("player_id", "INT64", int(player_id)),
            bigquery.ScalarQueryParameter("limit", "INT64", lim),
        ]
    )
    return bigquery.Client(project=pid).query(sql, job_config=cfg).to_dataframe()


def fetch_match_passes_to_player(
    match_id: int,
    target_player_id: int,
    *,
    project_id: str | None = None,
    dataset: str = "scouting_agent",
    limit: int = 50000,
) -> pd.DataFrame:
    """Passes received by target_player_id (recipient).

    Excludes passer id 0 and passes where the passer is not on the same match
    team as the recipient (teammate passes only).
    """
    pid = _bq_project_id(project_id)
    lim = max(1, min(int(limit), 100_000))
    fq = _table_fq(pid, dataset, "gold_match_pass_event")
    se = _table_fq(pid, dataset, "silver_match_event")
    dp = _table_fq(pid, dataset, "dim_player")
    sql = f"""
    WITH player_team_votes AS (
      SELECT
        s.match_id,
        s.player_id,
        s.team_id,
        COUNT(*) AS c
      FROM {se} AS s
      WHERE s.match_id = @match_id
        AND s.player_id IS NOT NULL AND s.player_id != 0
        AND s.team_id IS NOT NULL
      GROUP BY s.match_id, s.player_id, s.team_id
    ),
    player_dominant_team AS (
      SELECT match_id, player_id, team_id AS dominant_team_id
      FROM player_team_votes
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY match_id, player_id ORDER BY c DESC, team_id
      ) = 1
    )
    SELECT p.match_id, p.event_id, p.minute, p.second,
      p.location_x AS start_x, p.location_y AS start_y,
      p.pass_end_x AS end_x, p.pass_end_y AS end_y,
      p.player_id AS passer_id, pas.short_name AS passer_name,
      p.recipient_player_id AS target_id, tar.short_name AS target_name,
      p.team_id AS passer_team_id
    FROM {fq} AS p
    LEFT JOIN {dp} AS pas ON p.player_id = pas.player_id
    LEFT JOIN {dp} AS tar ON p.recipient_player_id = tar.player_id
    INNER JOIN player_dominant_team AS fdt
      ON fdt.match_id = p.match_id AND fdt.player_id = @target_player_id
    WHERE p.match_id = @match_id AND p.recipient_player_id = @target_player_id
      AND p.player_id IS NOT NULL AND p.player_id != 0
      AND p.pass_end_x IS NOT NULL AND p.pass_end_y IS NOT NULL
      AND p.team_id IS NOT NULL
      AND p.team_id = fdt.dominant_team_id
    ORDER BY p.minute, p.second, p.event_id LIMIT @limit
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("match_id", "INT64", int(match_id)),
            bigquery.ScalarQueryParameter("target_player_id", "INT64", int(target_player_id)),
            bigquery.ScalarQueryParameter("limit", "INT64", lim),
        ]
    )
    return bigquery.Client(project=pid).query(sql, job_config=cfg).to_dataframe()


def fetch_match_key_pass_crosses(
    *,
    match_id: int | None = None,
    player_id: int | None = None,
    team_id: int | None = None,
    season_id: int | None = None,
    competition_id: int | None = None,
    event_type: Literal["both", "key_pass", "cross"] = "both",
    include_free_kick_cross: bool = True,
    include_shot_assist_as_key: bool = True,
    only_accurate: bool = False,
    limit: int = 5000,
    project_id: str | None = None,
    dataset: str = "scouting_agent",
) -> pd.DataFrame:
    """Key passes / crosses from gold_match_pass_event."""
    pid = _bq_project_id(project_id)
    if match_id is None and player_id is None and team_id is None and season_id is None and competition_id is None:
        raise ValueError("Provide at least one filter (match_id, team_id, player_id, ...).")
    lim = max(1, min(int(limit), 20_000))
    table = _table_fq(pid, dataset, "gold_match_pass_event")
    if event_type == "key_pass":
        key_expr = "is_key_pass OR is_shot_assist" if include_shot_assist_as_key else "is_key_pass"
        event_filter = f"({key_expr})"
    elif event_type == "cross":
        cross_expr = "is_cross OR is_free_kick_cross" if include_free_kick_cross else "is_cross"
        event_filter = f"({cross_expr})"
    else:
        key_expr = "is_key_pass OR is_shot_assist" if include_shot_assist_as_key else "is_key_pass"
        cross_expr = "is_cross OR is_free_kick_cross" if include_free_kick_cross else "is_cross"
        event_filter = f"(({key_expr}) OR ({cross_expr}))"
    sql = f"""
    SELECT match_id, event_id, season_id, competition_id, match_period, minute, second,
      team_id, opponent_team_id, player_id, recipient_player_id,
      location_x, location_y, pass_end_x, pass_end_y, pass_accurate,
      pass_length_m, pass_angle_deg, pass_height,
      is_key_pass, is_shot_assist, is_cross, is_free_kick_cross,
      is_progressive_pass, is_through_pass, is_pass_to_final_third, is_pass_to_penalty_area
    FROM {table}
    WHERE {event_filter}
      {"AND match_id = @match_id" if match_id is not None else ""}
      {"AND player_id = @player_id" if player_id is not None else ""}
      {"AND team_id = @team_id" if team_id is not None else ""}
      {"AND season_id = @season_id" if season_id is not None else ""}
      {"AND competition_id = @competition_id" if competition_id is not None else ""}
      {"AND pass_accurate IS TRUE" if only_accurate else ""}
    ORDER BY match_id, minute, second, event_id LIMIT @limit
    """
    params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("limit", "INT64", int(lim))
    ]
    if match_id is not None:
        params.append(bigquery.ScalarQueryParameter("match_id", "INT64", int(match_id)))
    if player_id is not None:
        params.append(bigquery.ScalarQueryParameter("player_id", "INT64", int(player_id)))
    if team_id is not None:
        params.append(bigquery.ScalarQueryParameter("team_id", "INT64", int(team_id)))
    if season_id is not None:
        params.append(bigquery.ScalarQueryParameter("season_id", "INT64", int(season_id)))
    if competition_id is not None:
        params.append(bigquery.ScalarQueryParameter("competition_id", "INT64", int(competition_id)))
    return bigquery.Client(project=pid).query(
        sql, job_config=bigquery.QueryJobConfig(query_parameters=params)
    ).to_dataframe()


def fetch_match_touch_events(
    match_id: int,
    player_id: int,
    *,
    project_id: str | None = None,
    dataset: str = "scouting_agent",
    limit: int = 20000,
) -> pd.DataFrame:
    """silver_match_event positions for player touch map / KDE."""
    pid = _bq_project_id(project_id)
    lim = max(1, min(int(limit), 50_000))
    table = _table_fq(pid, dataset, "silver_match_event")
    sql = f"""
    SELECT match_id, event_id, season_id, competition_id, match_period, minute, second,
      match_timestamp, type_primary, type_secondary_json,
      location_x, location_y, team_id, opponent_team_id, player_id,
      possession_id, possession_team_id
    FROM {table}
    WHERE match_id = @match_id AND player_id = @player_id
      AND location_x IS NOT NULL AND location_y IS NOT NULL
    ORDER BY minute, second, event_id LIMIT @limit
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("match_id", "INT64", int(match_id)),
            bigquery.ScalarQueryParameter("player_id", "INT64", int(player_id)),
            bigquery.ScalarQueryParameter("limit", "INT64", lim),
        ]
    )
    return bigquery.Client(project=pid).query(sql, job_config=cfg).to_dataframe()


def fetch_match_defensive_duels(
    match_id: int,
    *,
    team_id: int | None = None,
    player_id: int | None = None,
    project_id: str | None = None,
    dataset: str = "scouting_agent",
    limit: int = 20000,
) -> pd.DataFrame:
    pid = _bq_project_id(project_id)
    lim = max(1, min(int(limit), 100_000))
    t = _table_fq(pid, dataset, "gold_match_duel_event")
    sql = f"""
    SELECT match_id, event_id, minute, second, location_x, location_y, team_id, player_id,
      duel_type, duel_phase, is_duel_won, is_sliding_tackle, opponent_player_id
    FROM {t}
    WHERE match_id = @match_id AND duel_phase = 'defensive'
      AND location_x IS NOT NULL AND location_y IS NOT NULL
      {"AND team_id = @team_id" if team_id is not None else ""}
      {"AND player_id = @player_id" if player_id is not None else ""}
    ORDER BY minute, second, event_id LIMIT @limit
    """
    params: list = [
        bigquery.ScalarQueryParameter("match_id", "INT64", int(match_id)),
        bigquery.ScalarQueryParameter("limit", "INT64", lim),
    ]
    if team_id is not None:
        params.append(bigquery.ScalarQueryParameter("team_id", "INT64", int(team_id)))
    if player_id is not None:
        params.append(bigquery.ScalarQueryParameter("player_id", "INT64", int(player_id)))
    return bigquery.Client(project=pid).query(
        sql, job_config=bigquery.QueryJobConfig(query_parameters=params)
    ).to_dataframe()


def fetch_match_regains(
    match_id: int,
    *,
    team_id: int | None = None,
    player_id: int | None = None,
    project_id: str | None = None,
    dataset: str = "scouting_agent",
    limit: int = 20000,
) -> pd.DataFrame:
    """Interceptions / recoveries from gold_match_recovery_interception_event."""
    pid = _bq_project_id(project_id)
    lim = max(1, min(int(limit), 100_000))
    t = _table_fq(pid, dataset, "gold_match_recovery_interception_event")
    sql = f"""
    SELECT match_id, event_id, minute, second, location_x, location_y, team_id, player_id,
      is_interception, is_recovery, is_counterpressing, regain_signal_type, type_primary
    FROM {t}
    WHERE match_id = @match_id
      AND location_x IS NOT NULL AND location_y IS NOT NULL
      {"AND team_id = @team_id" if team_id is not None else ""}
      {"AND player_id = @player_id" if player_id is not None else ""}
    ORDER BY minute, second, event_id LIMIT @limit
    """
    params: list = [
        bigquery.ScalarQueryParameter("match_id", "INT64", int(match_id)),
        bigquery.ScalarQueryParameter("limit", "INT64", lim),
    ]
    if team_id is not None:
        params.append(bigquery.ScalarQueryParameter("team_id", "INT64", int(team_id)))
    if player_id is not None:
        params.append(bigquery.ScalarQueryParameter("player_id", "INT64", int(player_id)))
    return bigquery.Client(project=pid).query(
        sql, job_config=bigquery.QueryJobConfig(query_parameters=params)
    ).to_dataframe()


# --- Duel / regain pitch helpers ----------------------------------------------



def _event_pitch_draw(
    *,
    pitch_half: bool,
    figsize: tuple[float, float],
) -> tuple[plt.Figure, plt.Axes, VerticalPitch]:
    pitch = VerticalPitch(
        pitch_type="wyscout",
        pitch_length=100,
        pitch_width=100,
        half=pitch_half,
        pad_bottom=0.5,
        goal_type="box",
        goal_alpha=0.8,
        pitch_color="white",
        line_color=PITCH_LINE_SOFT,
        linewidth=PITCH_LINEWIDTH_SOFT,
        line_zorder=4,
    )
    fig, ax = pitch.draw(figsize=figsize)
    fig.patch.set_facecolor("white")
    fig.subplots_adjust(left=0.07, right=0.94, top=0.86, bottom=0.18)
    return fig, ax, pitch


DUEL_TYPE_MARKERS: dict[str, str] = {
    "aerial_duel": "^",
    "ground_duel": "o",
    "loose_ball_duel": "s",
}


def _as_bool_col(x: Any) -> bool:
    if x is None or pd.isna(x):
        return False
    if isinstance(x, (bool, np.bool_)):
        return bool(x)
    try:
        return bool(int(x))
    except (TypeError, ValueError):
        return False


def _recovery_kind(row: pd.Series) -> str:
    ir = _as_bool_col(row.get("is_interception"))
    rc = _as_bool_col(row.get("is_recovery"))
    if ir and rc:
        return "interception+recovery"
    if ir:
        return "interception"
    if rc:
        return "recovery"
    return "other"


RECOVERY_KIND_STYLE: dict[str, tuple[str, str]] = {
    "interception": ("^", "#1d4ed8"),
    "recovery": ("o", "#ea580c"),
    "interception+recovery": ("D", "#7c3aed"),
}
RECOVERY_KIND_LABEL: dict[str, str] = {
    "interception": "Interception",
    "recovery": "Recovery",
    "interception+recovery": "Interception + recovery",
}


# --- Plots --------------------------------------------------------------------

def plot_shot_map_vertical(
    df_shots: pd.DataFrame,
    title: str,
    *,
    subtitle: str | None = None,
    left_logo_path: str = "images/sport_data_campus.png",
    team_img_source: str | None = None,
    player_img_source: str | None = None,
    font_prop: Any = None,
    figsize: tuple[float, float] = (12, 10),
    pitch: VerticalPitch | None = None,
    edgecolors: str = "#b94b75",
    xg_scale: float = 1900,
    xg_offset: float = 100,
    branding_header: Callable[..., None] | None = None,
) -> tuple[plt.Figure, plt.Axes]:
    df_non, df_goals = prepare_shot_events_for_plot(df_shots)
    if pitch is None:
        pitch = VerticalPitch(
            pitch_type="wyscout",
            pitch_length=100,
            pitch_width=100,
            half=True,
            pad_bottom=0.5,
            goal_type="box",
            goal_alpha=0.8,
        )
    fig, ax = pitch.draw(figsize=figsize)
    if not df_non.empty:
        pitch.scatter(
            df_non["x"],
            df_non["y"],
            s=(df_non["xg"] * xg_scale) + xg_offset,
            edgecolors=edgecolors,
            c="None",
            hatch="///",
            marker="o",
            ax=ax,
        )
    if not df_goals.empty:
        pitch.scatter(
            df_goals["x"],
            df_goals["y"],
            s=(df_goals["xg"] * xg_scale) + xg_offset,
            edgecolors=edgecolors,
            linewidths=0.6,
            c="white",
            marker="football",
            ax=ax,
        )
    if branding_header is not None:
        branding_header(
            fig,
            title=title,
            subtitle=subtitle,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
        )
    else:
        add_header_branding(
            fig,
            title=title,
            subtitle=subtitle,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            font_prop=font_prop,
        )
    return fig, ax


def plot_cross_key_pass_map_vertical(
    df_cross: pd.DataFrame,
    title: str,
    *,
    subtitle: str | None = None,
    left_logo_path: str = "images/sport_data_campus.png",
    team_img_source: str | None = None,
    player_img_source: str | None = None,
    legend_ncol: int = 3,
    line_lw: float = 6,
    key_pass_color: str = "#5ce1ff",
    key_pass_lw_scale: float = 0.55,
) -> tuple[plt.Figure, plt.Axes]:
    d = df_cross.copy()
    for c in ["location_x", "location_y", "pass_end_x", "pass_end_y"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    x1 = d["pass_end_x"].fillna(0.0)
    y1 = d["pass_end_y"].fillna(0.0)
    blocked = (x1 == 0) & (y1 == 0)
    pitch = VerticalPitch(
        pitch_type="wyscout",
        pitch_length=100,
        pitch_width=100,
        half=True,
        pad_bottom=0.5,
        goal_type="box",
        goal_alpha=0.8,
    )
    fig, ax_pitch = pitch.draw(figsize=(12, 10))
    add_header_branding(
        fig,
        title=title,
        subtitle=subtitle,
        left_logo_path=left_logo_path,
        team_img_source=team_img_source,
        player_img_source=player_img_source,
    )
    ks = (
        d["is_key_pass"].fillna(False).astype(bool)
        if "is_key_pass" in d.columns
        else pd.Series(False, index=d.index)
    )
    sa = (
        d["is_shot_assist"].fillna(False).astype(bool)
        if "is_shot_assist" in d.columns
        else pd.Series(False, index=d.index)
    )
    both, only_sa, only_kp = ks & sa, sa & ~ks, ks & ~sa
    base = ~blocked

    def _lines_comet(sub: pd.Series, label: str, cmap: str, lw: float, z: int) -> None:
        sub = sub & base
        dd = d[sub].dropna(subset=["location_x", "location_y", "pass_end_x", "pass_end_y"])
        if dd.empty:
            return
        pitch.lines(
            dd["location_x"],
            dd["location_y"],
            dd["pass_end_x"],
            dd["pass_end_y"],
            lw=lw,
            transparent=True,
            comet=True,
            cmap=cmap,
            label=label,
            ax=ax_pitch,
            zorder=z,
        )

    def _lines_kp_dashed(sub: pd.Series, label: str, lw: float, z: int) -> None:
        sub = sub & base
        dd = d[sub].dropna(subset=["location_x", "location_y", "pass_end_x", "pass_end_y"])
        if dd.empty:
            return
        pitch.lines(
            dd["location_x"],
            dd["location_y"],
            dd["pass_end_x"],
            dd["pass_end_y"],
            lw=lw,
            color=key_pass_color,
            linestyle=(0, (6, 4)),
            comet=False,
            alpha=0.95,
            label=label,
            ax=ax_pitch,
            zorder=z,
        )

    _lines_comet(both, "key pass + shot assist", "spring", line_lw + 4, z=4)
    _lines_comet(only_sa, "shot assist", "Greens", line_lw + 2, z=3)
    _lines_kp_dashed(only_kp, "key pass", max(1.5, line_lw * key_pass_lw_scale), z=2)
    d_blk = d[blocked].dropna(subset=["location_x", "location_y"])
    if not d_blk.empty:
        pitch.scatter(
            d_blk["location_x"],
            d_blk["location_y"],
            marker="x",
            s=280,
            linewidths=2.5,
            c="#ff6b6b",
            edgecolors="white",
            zorder=6,
            label="Cross Blocked",
            ax=ax_pitch,
        )
    ax_pitch.legend(loc="lower center", ncol=legend_ncol, handlelength=4, framealpha=0.92)
    return fig, ax_pitch


def plot_player_touch_kde_vertical(
    df_events: pd.DataFrame,
    title: str,
    *,
    subtitle: str | None = None,
    branding_header: Callable[..., None] | None = None,
    left_logo_path: str = "images/sport_data_campus.png",
    team_img_source: str | None = None,
    player_img_source: str | None = None,
    x_col: str = "location_x",
    y_col: str = "location_y",
    figsize: tuple[float, float] = (6.0, 8.0),
    cmap: Any | None = None,
    pitch_half: bool = False,
    fill: bool = True,
    levels: int = 100,
    thresh: float = 0.03,
    cut: int = 4,
    colorbar: bool = True,
    cbar_label: str = "Density (KDE)",
    header_center_y: float = 1.03,
) -> tuple[plt.Figure, plt.Axes]:
    if cmap is None:
        cmap = SDC_CMAP
    d = df_events.copy()
    d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
    d[y_col] = pd.to_numeric(d[y_col], errors="coerce")
    d = d.dropna(subset=[x_col, y_col])
    if len(d) < 3:
        raise ValueError("Need at least ~3 points with coordinates for a stable KDE.")
    pitch = VerticalPitch(
        pitch_type="wyscout",
        pitch_length=100,
        pitch_width=100,
        half=pitch_half,
        pad_bottom=0.5,
        goal_type="box",
        goal_alpha=0.8,
        pitch_color="white",
        line_color=PITCH_LINE_SOFT,
        linewidth=PITCH_LINEWIDTH_SOFT,
        line_zorder=3,
    )
    fig, ax = pitch.draw(figsize=figsize)
    fig.patch.set_facecolor("white")
    hdr = branding_header or add_header_branding
    try:
        hdr(
            fig,
            title=title,
            subtitle=subtitle,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    except TypeError:
        hdr(
            fig,
            title=title,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    kde_return = pitch.kdeplot(
        d[x_col],
        d[y_col],
        ax=ax,
        fill=fill,
        levels=levels,
        thresh=thresh,
        cut=cut,
        cmap=cmap,
        zorder=1,
        alpha=0.92,
    )
    
    return fig, ax


def plot_pass_link_out_kde_vertical(
    df_passes: pd.DataFrame,
    title: str,
    *,
    subtitle: str | None = None,
    branding_header: Callable[..., None] | None = None,
    left_logo_path: str = "images/sport_data_campus.png",
    team_img_source: str | None = None,
    player_img_source: str | None = None,
    pitch_half: bool = False,
    top_k: int = 3,
    figsize: tuple[float, float] = (6.0, 8.0),
    kde_cmap: Any = "Blues",
    kde_levels: int = 100,
    kde_thresh: float = 0.035,
    kde_cut: int = 4,
    kde_bw_adjust: float = 1.5,
    kde_alpha: float = 0.5,
    show_kde_colorbar: bool = False,
    header_center_y: float = 1.03,
) -> tuple[plt.Figure, plt.Axes]:
    d = df_passes.copy()
    for c in ["start_x", "start_y", "end_x", "end_y"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["start_x", "start_y", "end_x", "end_y", "recipient_id"])
    d = d[~((d["end_x"] == 0) & (d["end_y"] == 0))]
    if len(d) < 3:
        raise ValueError("Not enough passes with coordinates.")
    passer_name = (
        d["passer_name"].dropna().astype(str).iloc[0]
        if "passer_name" in d.columns
        else "Target"
    )
    sx, sy = float(d["start_x"].median()), float(d["start_y"].median())
    gb = d.groupby(["recipient_id", "recipient_name"], dropna=False)
    g = (
        gb.agg(rx=("end_x", "mean"), ry=("end_y", "mean"))
        .assign(n_pass=gb.size())
        .reset_index()
        .sort_values("n_pass", ascending=False)
        .head(top_k)
        .reset_index(drop=True)
    )
    pitch = VerticalPitch(
        pitch_type="wyscout",
        pitch_length=100,
        pitch_width=100,
        half=pitch_half,
        pad_bottom=0.5,
        goal_type="box",
        goal_alpha=0.8,
        pitch_color="white",
        line_color=PITCH_LINE_SOFT,
        linewidth=PITCH_LINEWIDTH_SOFT,
        line_zorder=4,
    )
    fig, ax = pitch.draw(figsize=figsize)
    fig.patch.set_facecolor("white")
    fig.subplots_adjust(left=0.07, right=0.94, top=0.86, bottom=0.20)
    cmap_kde = plt.colormaps.get_cmap(kde_cmap).copy()
    cmap_kde.set_under("#ffffff")
    kde_ret = pitch.kdeplot(
        d["start_x"],
        d["start_y"],
        ax=ax,
        fill=True,
        levels=kde_levels,
        thresh=kde_thresh,
        cut=kde_cut,
        cmap=cmap_kde,
        bw_adjust=kde_bw_adjust,
        zorder=1,
        alpha=kde_alpha,
    )
    for coll in ax.collections:
        if coll.get_zorder() < 10:
            coll.set_zorder(2)
    
    pitch.scatter(
        sx,
        sy,
        s=PASS_VIZ_STAR_SIZE,
        marker="*",
        c="#e63946",
        edgecolors="#000009",
        linewidths=1.3,
        zorder=30,
        ax=ax,
        label=f"{passer_name} (target)",
    )
    for j, (_, row) in enumerate(g.iterrows()):
        rx, ry = float(row["rx"]), float(row["ry"])
        n = int(row["n_pass"])
        rname = (
            str(row["recipient_name"])
            if pd.notna(row["recipient_name"])
            else f"id {int(row['recipient_id'])}"
        )
        mc = PASS_VIZ_COLORS[j % len(PASS_VIZ_COLORS)]
        mk = PASS_VIZ_MARKERS[j % len(PASS_VIZ_MARKERS)]
        _pitch_link_line(pitch, ax, sx, sy, rx, ry, mc)
        pitch.scatter(
            rx,
            ry,
            s=PASS_VIZ_NODE_SIZE,
            marker=mk,
            facecolors=mc,
            edgecolors="#000009",
            linewidths=1.4,
            zorder=28,
            ax=ax,
            label=f"{rname} · {n} pass",
        )
        pitch.text(
            rx,
            ry,
            str(n),
            ax=ax,
            ha="center",
            va="center",
            fontsize=PASS_VIZ_COUNT_FONTSIZE,
            color="white",
            weight="bold",
            zorder=35,
        )
    leg = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.07),
        ncol=min(3, top_k + 1),
        frameon=True,
        fancybox=True,
        framealpha=0.96,
        edgecolor="#cccccc",
        facecolor="white",
        fontsize=8.5,
    )
    for t in leg.get_texts():
        t.set_color("#222222")
    hdr = branding_header or add_header_branding
    try:
        hdr(
            fig,
            title=title,
            subtitle=subtitle,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    except TypeError:
        hdr(
            fig,
            title=title,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    return fig, ax


def plot_pass_link_in_kde_vertical(
    df_passes: pd.DataFrame,
    title: str,
    *,
    subtitle: str | None = None,
    branding_header: Callable[..., None] | None = None,
    left_logo_path: str = "images/sport_data_campus.png",
    team_img_source: str | None = None,
    player_img_source: str | None = None,
    pitch_half: bool = False,
    top_k: int = 3,
    figsize: tuple[float, float] = (6.0, 8.0),
    kde_cmap: Any = "Blues",
    kde_levels: int = 100,
    kde_thresh: float = 0.035,
    kde_cut: int = 4,
    kde_bw_adjust: float = 1.5,
    kde_alpha: float = 0.5,
    show_kde_colorbar: bool = False,
    header_center_y: float = 1.03,
    converge_lines_to_target: bool = True,
) -> tuple[plt.Figure, plt.Axes]:
    d = df_passes.copy()
    for c in ["start_x", "start_y", "end_x", "end_y"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["start_x", "start_y", "end_x", "end_y", "passer_id"])
    d = d[~((d["end_x"] == 0) & (d["end_y"] == 0))]
    if len(d) < 3:
        raise ValueError("Not enough received passes with coordinates.")
    target_name = (
        d["target_name"].dropna().astype(str).iloc[0]
        if "target_name" in d.columns
        else "Target"
    )
    tx, ty = float(d["end_x"].median()), float(d["end_y"].median())
    gb = d.groupby(["passer_id", "passer_name"], dropna=False)
    g = (
        gb.agg(
            sx=("start_x", "mean"),
            sy=("start_y", "mean"),
            ex=("end_x", "mean"),
            ey=("end_y", "mean"),
        )
        .assign(n_pass=gb.size())
        .reset_index()
        .sort_values("n_pass", ascending=False)
        .head(top_k)
        .reset_index(drop=True)
    )
    pitch = VerticalPitch(
        pitch_type="wyscout",
        pitch_length=100,
        pitch_width=100,
        half=pitch_half,
        pad_bottom=0.5,
        goal_type="box",
        goal_alpha=0.8,
        pitch_color="white",
        line_color=PITCH_LINE_SOFT,
        linewidth=PITCH_LINEWIDTH_SOFT,
        line_zorder=4,
    )
    fig, ax = pitch.draw(figsize=figsize)
    fig.patch.set_facecolor("white")
    fig.subplots_adjust(left=0.07, right=0.94, top=0.86, bottom=0.20)
    cmap_kde = plt.colormaps.get_cmap(kde_cmap).copy()
    cmap_kde.set_under("#ffffff")
    kde_ret = pitch.kdeplot(
        d["end_x"],
        d["end_y"],
        ax=ax,
        fill=True,
        levels=kde_levels,
        thresh=kde_thresh,
        cut=kde_cut,
        cmap=cmap_kde,
        bw_adjust=kde_bw_adjust,
        zorder=1,
        alpha=kde_alpha,
    )
    for coll in ax.collections:
        if coll.get_zorder() < 10:
            coll.set_zorder(2)
    
    pitch.scatter(
        tx,
        ty,
        s=PASS_VIZ_STAR_SIZE,
        marker="*",
        c="#e63946",
        edgecolors="#000009",
        linewidths=1.3,
        zorder=30,
        ax=ax,
        label=f"{target_name} · mean reception",
    )
    for j, (_, row) in enumerate(g.iterrows()):
        sx, sy = float(row["sx"]), float(row["sy"])
        ex, ey = float(row["ex"]), float(row["ey"])
        n = int(row["n_pass"])
        pname = (
            str(row["passer_name"])
            if pd.notna(row["passer_name"])
            else f"id {int(row['passer_id'])}"
        )
        mc = PASS_VIZ_COLORS[j % len(PASS_VIZ_COLORS)]
        mk = PASS_VIZ_MARKERS[j % len(PASS_VIZ_MARKERS)]
        x1, y1 = (tx, ty) if converge_lines_to_target else (ex, ey)
        _pitch_link_line(pitch, ax, sx, sy, x1, y1, mc)
        pitch.scatter(
            sx,
            sy,
            s=PASS_VIZ_NODE_SIZE,
            marker=mk,
            facecolors=mc,
            edgecolors="#000009",
            linewidths=1.4,
            zorder=28,
            ax=ax,
            label=f"{pname} · {n} pass",
        )
        pitch.text(
            sx,
            sy,
            str(n),
            ax=ax,
            ha="center",
            va="center",
            fontsize=PASS_VIZ_COUNT_FONTSIZE,
            color="white",
            weight="bold",
            zorder=35,
        )
    leg = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.07),
        ncol=min(3, top_k + 1),
        frameon=True,
        fancybox=True,
        framealpha=0.96,
        edgecolor="#cccccc",
        facecolor="white",
        fontsize=8.5,
    )
    for t in leg.get_texts():
        t.set_color("#222222")
    hdr = branding_header or add_header_branding
    try:
        hdr(
            fig,
            title=title,
            subtitle=subtitle,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    except TypeError:
        hdr(
            fig,
            title=title,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    return fig, ax


def plot_defensive_duel_map_vertical(
    df_duels: pd.DataFrame,
    title: str,
    *,
    subtitle: str | None = None,
    branding_header: Callable[..., None] | None = None,
    left_logo_path: str = "images/sport_data_campus.png",
    team_img_source: str | None = None,
    player_img_source: str | None = None,
    pitch_half: bool = False,
    figsize: tuple[float, float] = (6.0, 8.0),
    header_center_y: float = 1.03,
    scatter_size: int = 120,
    won_color: str = "#16a34a",
    lost_color: str = "#dc2626",
    unknown_color: str = "#6b7280",
) -> tuple[plt.Figure, plt.Axes]:
    d = df_duels.copy()
    d["location_x"] = pd.to_numeric(d["location_x"], errors="coerce")
    d["location_y"] = pd.to_numeric(d["location_y"], errors="coerce")
    d = d.dropna(subset=["location_x", "location_y"])
    if d.empty:
        raise ValueError("No duels with valid coordinates.")
    fig, ax, pitch = _event_pitch_draw(pitch_half=pitch_half, figsize=figsize)
    

    def _to_won(x: Any) -> object:
        if x is None or pd.isna(x):
            return None
        if isinstance(x, (bool, np.bool_)):
            return bool(x)
        try:
            return bool(int(x))
        except (TypeError, ValueError):
            return None

    d["_won"] = d["is_duel_won"].map(_to_won) if "is_duel_won" in d.columns else np.nan
    d["duel_type"] = d["duel_type"].fillna("ground_duel").astype(str)
    legend_keys: set[str] = set()
    for duel_type, g in d.groupby("duel_type"):
        mk = DUEL_TYPE_MARKERS.get(duel_type, "D")
        for outcome_label, mask in [
            ("won", g["_won"] == True),  # noqa: E712
            ("lost", g["_won"] == False),  # noqa: E712
            ("n/a", g["_won"].isna()),
        ]:
            sub = g[mask]
            if sub.empty:
                continue
            c = (
                won_color
                if outcome_label == "won"
                else lost_color
                if outcome_label == "lost"
                else unknown_color
            )
            leg_key = f"{duel_type}|{outcome_label}"
            show_leg = leg_key not in legend_keys
            if show_leg:
                legend_keys.add(leg_key)
            lab = f"{duel_type} · {outcome_label}"
            pitch.scatter(
                sub["location_x"],
                sub["location_y"],
                s=scatter_size,
                marker=mk,
                c=c,
                edgecolors="#000009",
                linewidths=0.9,
                alpha=0.9,
                zorder=10,
                ax=ax,
                label=lab if show_leg else "_nolegend_",
            )
    leg = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=3,
        frameon=True,
        fancybox=True,
        framealpha=0.96,
        edgecolor="#cccccc",
        facecolor="white",
        fontsize=7.5,
    )
    for t in leg.get_texts():
        t.set_color("#222222")
    hdr = branding_header or add_header_branding
    try:
        hdr(
            fig,
            title=title,
            subtitle=subtitle,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    except TypeError:
        hdr(
            fig,
            title=title,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    return fig, ax


def plot_regain_map_vertical(
    df_events: pd.DataFrame,
    title: str,
    *,
    subtitle: str | None = None,
    branding_header: Callable[..., None] | None = None,
    left_logo_path: str = "images/sport_data_campus.png",
    team_img_source: str | None = None,
    player_img_source: str | None = None,
    pitch_half: bool = False,
    figsize: tuple[float, float] = (6.0, 8.0),
    header_center_y: float = 1.03,
    scatter_size: int = 130,
    counterpress_edge: str = "#ca8a04",
    counterpress_lw: float = 2.2,
) -> tuple[plt.Figure, plt.Axes]:
    d = df_events.copy()
    d["location_x"] = pd.to_numeric(d["location_x"], errors="coerce")
    d["location_y"] = pd.to_numeric(d["location_y"], errors="coerce")
    d = d.dropna(subset=["location_x", "location_y"])
    if d.empty:
        raise ValueError("No events with valid coordinates.")
    d["_kind"] = d.apply(_recovery_kind, axis=1)
    d = d[d["_kind"] != "other"]
    if d.empty:
        raise ValueError("No rows flagged as interception or recovery.")
    d["_cp"] = (
        d["is_counterpressing"].fillna(False).astype(bool)
        if "is_counterpressing" in d.columns
        else False
    )
    fig, ax, pitch = _event_pitch_draw(pitch_half=pitch_half, figsize=figsize)
    used_labels: set[str] = set()
    for kind, g in d.groupby("_kind"):
        mk, fc = RECOVERY_KIND_STYLE.get(kind, ("o", "#374151"))
        base = RECOVERY_KIND_LABEL.get(kind, kind)
        for is_cp, sub in g.groupby("_cp"):
            ec = counterpress_edge if is_cp else "#000009"
            lw = counterpress_lw if is_cp else 0.9
            suffix = " · counterpressing" if is_cp else ""
            lab = f"{base}{suffix}"
            show = lab not in used_labels
            if show:
                used_labels.add(lab)
            pitch.scatter(
                sub["location_x"],
                sub["location_y"],
                s=scatter_size,
                marker=mk,
                c=fc,
                edgecolors=ec,
                linewidths=lw,
                alpha=0.9,
                zorder=10,
                ax=ax,
                label=lab if show else "_nolegend_",
            )
    leg = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=3,
        frameon=True,
        fancybox=True,
        framealpha=0.96,
        edgecolor="#cccccc",
        facecolor="white",
        fontsize=8.0,
    )
    for t in leg.get_texts():
        t.set_color("#222222")
    hdr = branding_header or add_header_branding
    try:
        hdr(
            fig,
            title=title,
            subtitle=subtitle,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    except TypeError:
        hdr(
            fig,
            title=title,
            left_logo_path=left_logo_path,
            team_img_source=team_img_source,
            player_img_source=player_img_source,
            header_center_y=header_center_y,
        )
    return fig, ax


# --- Legacy names (notebook / older imports) ----------------------------------

plot_gold_shots_mpl_style = plot_shot_map_vertical
plot_crosses_same_pitch_as_pass_leading = plot_cross_key_pass_map_vertical
plot_player_match_kde_heatmap = plot_player_touch_kde_vertical
plot_passing_links_kde_vertical = plot_pass_link_out_kde_vertical
plot_received_passing_links_kde_vertical = plot_pass_link_in_kde_vertical
plot_defensive_duels_map_vertical = plot_defensive_duel_map_vertical
plot_recovery_interception_map_vertical = plot_regain_map_vertical

prepare_gold_shots_for_plot = prepare_shot_events_for_plot
fetch_player_shots_single_match = fetch_match_shots_for_player
fetch_passer_recipient_passes = fetch_match_passes_from_player
fetch_target_received_passes = fetch_match_passes_to_player
fetch_key_passes_and_crosses = fetch_match_key_pass_crosses
fetch_player_events_single_match = fetch_match_touch_events
fetch_gold_match_defensive_duels = fetch_match_defensive_duels
fetch_gold_match_recovery_interception = fetch_match_regains

