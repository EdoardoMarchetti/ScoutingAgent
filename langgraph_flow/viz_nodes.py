from __future__ import annotations

from typing import Any

import pandas as pd

from langgraph_flow.scouting_state import ScoutingReportState
from langgraph_flow.viz_helpers import (
    branding_logo_path,
    build_viz_description_prompt,
    fig_to_markdown_png,
    structured_json_for_llm,
    summarize_crosses_df,
    summarize_duels_df,
    summarize_passes_in_df,
    summarize_passes_out_df,
    summarize_regains_df,
    summarize_shots_df,
    summarize_touches_df,
)
from services.bigquery_client import get_bq_project_id
from services.llm_factory import create_graph_llm
from visualization.scouting_viz import (
    SDC_CMAP_WHITE0,
    fetch_match_defensive_duels,
    fetch_match_key_pass_crosses,
    fetch_match_passes_from_player,
    fetch_match_passes_to_player,
    fetch_match_regains,
    fetch_match_shots_for_player,
    fetch_match_touch_events,
    plot_cross_key_pass_map_vertical,
    plot_defensive_duel_map_vertical,
    plot_pass_link_in_kde_vertical,
    plot_pass_link_out_kde_vertical,
    plot_player_touch_kde_vertical,
    plot_regain_map_vertical,
    plot_shot_map_vertical,
)


def _lang(report_language: str) -> str:
    l = (report_language or "English").strip().lower()
    if l.startswith("it"):
        return "it"
    if l.startswith("es"):
        return "es"
    return "en"


def _tr(report_language: str, key: str) -> str:
    txt = {
        "duels_caption": {
            "en": "Defensive duel locations and outcomes across the pitch.",
            "it": "Posizioni ed esiti dei duelli difensivi su tutto il campo.",
            "es": "Ubicaciones y resultados de los duelos defensivos en todo el campo.",
        },
        "regains_caption": {
            "en": "Ball regains by type, including counterpressing actions.",
            "it": "Riconquiste palla per tipologia, incluse le azioni di contropressing.",
            "es": "Recuperaciones de balón por tipo, incluyendo acciones de contrapresión.",
        },
        "pass_out_caption": {
            "en": "Pass origin density and main passing links from the player.",
            "it": "Densita delle origini dei passaggi e principali connessioni in uscita dal giocatore.",
            "es": "Densidad de origen de pases y principales conexiones de pase del jugador.",
        },
        "pass_in_caption": {
            "en": "Reception density and key teammates supplying passes.",
            "it": "Densita delle ricezioni e compagni chiave che servono il giocatore.",
            "es": "Densidad de recepciones y companeros clave que asisten al jugador.",
        },
        "touch_caption": {
            "en": "On-ball touch density map showing involvement zones.",
            "it": "Mappa di densita dei tocchi che mostra le zone di coinvolgimento.",
            "es": "Mapa de densidad de contactos que muestra zonas de participacion.",
        },
        "shots_caption": {
            "en": "Shot locations and chance quality profile in this match.",
            "it": "Posizioni di tiro e profilo della qualita delle occasioni nel match.",
            "es": "Ubicaciones de tiro y perfil de calidad de ocasiones en este partido.",
        },
        "crosses_caption": {
            "en": "Cross and key-pass trajectories, including blocked deliveries.",
            "it": "Traiettorie di cross e passaggi chiave, incluse le giocate bloccate.",
            "es": "Trayectorias de centros y pases clave, incluyendo envios bloqueados.",
        },
    }
    return txt[key][_lang(report_language)]


def _enforce_output_language(prompt_text: str, language: str) -> str:
    lang = (language or "English").strip()
    return (
        f"IMPORTANT OUTPUT LANGUAGE RULE: Write the final answer only in {lang}. "
        f"Do not mix languages.\n\n{prompt_text}"
    )


def _llm_describe(llm: Any, prompt: str, report_language: str) -> str:
    prompt = _enforce_output_language(prompt, report_language)
    try:
        resp = llm.invoke(prompt)
        return (getattr(resp, "content", "") or "").strip()
    except Exception:
        return ""


def _empty_viz(error: str) -> dict[str, Any]:
    return {
        "error": error,
        "markdown_image": "",
        "description": "",
        "caption": "",
        "events": [],
    }


def node_defensive_visualizations(state: ScoutingReportState) -> dict[str, Any]:
    match_id = int(state["match_id"])
    player_id = int(state["player_id"])
    project_id = get_bq_project_id()
    report_language = state.get("report_language", "English")
    title_main = str(state.get("player_name") or "Player")
    subtitle = str(state.get("match_label") or "Match")
    logo = branding_logo_path()
    player_img = state.get("player_image_data_url")
    team_img = state.get("team_image_data_url")
    llm = create_graph_llm()

    # --- Duels ---
    duels_payload: dict[str, Any]
    try:
        df_d = fetch_match_defensive_duels(
            match_id, player_id=player_id, project_id=project_id, limit=15_000
        )
        summary = summarize_duels_df(df_d)
        structured = structured_json_for_llm(summary)
        if df_d.empty:
            duels_payload = {
                "markdown_image": "",
                "description": _llm_describe(
                    llm,
                    build_viz_description_prompt(
                        viz_title="Defensive duels map",
                        viz_kind_note="No defensive duel events with coordinates for this player.",
                        player_name=str(state.get("player_name") or ""),
                        match_label=str(state.get("match_label") or ""),
                        structured_json=structured,
                    ),
                    report_language,
                )
                or _tr(report_language, "duels_caption"),
                "caption": _tr(report_language, "duels_caption"),
                "events": [],
            }
        else:
            fig, _ = plot_defensive_duel_map_vertical(
                df_d,
                title_main,
                subtitle=subtitle,
                left_logo_path=logo,
                team_img_source=team_img,
                player_img_source=player_img,
                pitch_half=False,
            )
            md = fig_to_markdown_png(fig)
            desc = _llm_describe(
                llm,
                build_viz_description_prompt(
                    viz_title="Defensive duels map",
                    viz_kind_note="Scatter map of defensive duels (markers by duel type, color by won/lost).",
                    player_name=str(state.get("player_name") or ""),
                    match_label=str(state.get("match_label") or ""),
                    structured_json=structured,
                ),
                report_language,
            )
            duels_payload = {
                "markdown_image": md,
                "description": desc,
                "caption": _tr(report_language, "duels_caption"),
                "events": summary.get("sample_events", []),
            }
    except Exception as exc:
        duels_payload = _empty_viz(str(exc))

    # --- Recoveries / interceptions ---
    rec_payload: dict[str, Any]
    try:
        df_r = fetch_match_regains(match_id, player_id=player_id, project_id=project_id, limit=15_000)
        summary = summarize_regains_df(df_r)
        structured = structured_json_for_llm(summary)
        if df_r.empty:
            rec_payload = {
                "markdown_image": "",
                "description": _llm_describe(
                    llm,
                    build_viz_description_prompt(
                        viz_title="Recoveries & interceptions map",
                        viz_kind_note="No recovery/interception events for this player.",
                        player_name=str(state.get("player_name") or ""),
                        match_label=str(state.get("match_label") or ""),
                        structured_json=structured,
                    ),
                    report_language,
                )
                or _tr(report_language, "regains_caption"),
                "caption": _tr(report_language, "regains_caption"),
                "events": [],
            }
        else:
            fig, _ = plot_regain_map_vertical(
                df_r,
                title_main,
                subtitle=subtitle,
                left_logo_path=logo,
                team_img_source=team_img,
                player_img_source=player_img,
                pitch_half=False,
            )
            md = fig_to_markdown_png(fig)
            desc = _llm_describe(
                llm,
                build_viz_description_prompt(
                    viz_title="Recoveries & interceptions map",
                    viz_kind_note="Map of regains: interception vs recovery; counterpressing highlighted.",
                    player_name=str(state.get("player_name") or ""),
                    match_label=str(state.get("match_label") or ""),
                    structured_json=structured,
                ),
                report_language,
            )
            rec_payload = {
                "markdown_image": md,
                "description": desc,
                "caption": _tr(report_language, "regains_caption"),
                "events": summary.get("sample_events", []),
            }
    except Exception as exc:
        rec_payload = _empty_viz(str(exc))

    return {
        "duels_visualizations": duels_payload,
        "recoveries_and_interceptions_visualization": rec_payload,
    }


def node_build_up_visualizations(state: ScoutingReportState) -> dict[str, Any]:
    match_id = int(state["match_id"])
    player_id = int(state["player_id"])
    project_id = get_bq_project_id()
    report_language = state.get("report_language", "English")
    title_main = str(state.get("player_name") or "Player")
    subtitle = str(state.get("match_label") or "Match")
    logo = branding_logo_path()
    player_img = state.get("player_image_data_url")
    team_img = state.get("team_image_data_url")
    llm = create_graph_llm()

    out: dict[str, Any] = {}

    # Pass start network (out)
    try:
        df_out = fetch_match_passes_from_player(match_id, player_id, project_id=project_id, limit=25_000)
        summary = summarize_passes_out_df(df_out)
        structured = structured_json_for_llm(summary)
        if len(df_out.dropna(subset=["start_x", "start_y", "end_x", "end_y"])) < 3:
            out["pass_start_network_visualization"] = {
                "markdown_image": "",
                "description": _llm_describe(
                    llm,
                    build_viz_description_prompt(
                        viz_title="Pass start heatmap with network",
                        viz_kind_note="Too few passes with full coordinates to render KDE+network.",
                        player_name=str(state.get("player_name") or ""),
                        match_label=str(state.get("match_label") or ""),
                        structured_json=structured,
                    ),
                    report_language,
                )
                or _tr(report_language, "pass_out_caption"),
                "caption": _tr(report_language, "pass_out_caption"),
                "events": summary.get("sample_passes", []),
            }
        else:
            fig, _ = plot_pass_link_out_kde_vertical(
                df_out,
                title_main,
                subtitle=subtitle,
                left_logo_path=logo,
                team_img_source=team_img,
                player_img_source=player_img,
                pitch_half=False,
                kde_cmap=SDC_CMAP_WHITE0,
            )
            md = fig_to_markdown_png(fig)
            desc = _llm_describe(
                llm,
                build_viz_description_prompt(
                    viz_title="Pass start heatmap with network",
                    viz_kind_note="KDE of pass start locations + top recipient link lines.",
                    player_name=str(state.get("player_name") or ""),
                    match_label=str(state.get("match_label") or ""),
                    structured_json=structured,
                ),
                report_language,
            )
            out["pass_start_network_visualization"] = {
                "markdown_image": md,
                "description": desc,
                "caption": _tr(report_language, "pass_out_caption"),
                "events": summary.get("sample_passes", []),
            }
    except Exception as exc:
        out["pass_start_network_visualization"] = _empty_viz(str(exc))

    # Receiving network (in)
    try:
        df_in = fetch_match_passes_to_player(match_id, player_id, project_id=project_id, limit=25_000)
        summary = summarize_passes_in_df(df_in)
        structured = structured_json_for_llm(summary)
        if len(df_in.dropna(subset=["start_x", "start_y", "end_x", "end_y"])) < 3:
            out["receiving_network_visualization"] = {
                "markdown_image": "",
                "description": _llm_describe(
                    llm,
                    build_viz_description_prompt(
                        viz_title="Receiving heatmap with network",
                        viz_kind_note="Too few received passes with coordinates.",
                        player_name=str(state.get("player_name") or ""),
                        match_label=str(state.get("match_label") or ""),
                        structured_json=structured,
                    ),
                    report_language,
                )
                or _tr(report_language, "pass_in_caption"),
                "caption": _tr(report_language, "pass_in_caption"),
                "events": summary.get("sample_passes", []),
            }
        else:
            fig, _ = plot_pass_link_in_kde_vertical(
                df_in,
                title_main,
                subtitle=subtitle,
                left_logo_path=logo,
                team_img_source=team_img,
                player_img_source=player_img,
                pitch_half=False,
                kde_cmap=SDC_CMAP_WHITE0,
            )
            md = fig_to_markdown_png(fig)
            desc = _llm_describe(
                llm,
                build_viz_description_prompt(
                    viz_title="Receiving heatmap with network",
                    viz_kind_note="KDE of reception locations + top passer link lines.",
                    player_name=str(state.get("player_name") or ""),
                    match_label=str(state.get("match_label") or ""),
                    structured_json=structured,
                ),
                report_language,
            )
            out["receiving_network_visualization"] = {
                "markdown_image": md,
                "description": desc,
                "caption": _tr(report_language, "pass_in_caption"),
                "events": summary.get("sample_passes", []),
            }
    except Exception as exc:
        out["receiving_network_visualization"] = _empty_viz(str(exc))

    # Touch density (fills pass_sonar slot — no sonar in scouting_viz)
    try:
        df_t = fetch_match_touch_events(match_id, player_id, project_id=project_id, limit=20_000)
        summary = summarize_touches_df(df_t)
        structured = structured_json_for_llm(summary)
        dloc = df_t.dropna(subset=["location_x", "location_y"])
        if len(dloc) < 3:
            out["pass_sonar_visualization"] = {
                "markdown_image": "",
                "description": _llm_describe(
                    llm,
                    build_viz_description_prompt(
                        viz_title="Touch density (positional involvement)",
                        viz_kind_note="Not a pass-direction sonar; touch KDE unavailable (too few points).",
                        player_name=str(state.get("player_name") or ""),
                        match_label=str(state.get("match_label") or ""),
                        structured_json=structured,
                    ),
                    report_language,
                )
                or _tr(report_language, "touch_caption"),
                "caption": _tr(report_language, "touch_caption"),
                "events": summary.get("sample_events", []),
            }
        else:
            fig, _ = plot_player_touch_kde_vertical(
                df_t,
                title_main,
                subtitle=subtitle,
                left_logo_path=logo,
                team_img_source=team_img,
                player_img_source=player_img,
                pitch_half=False,
            )
            md = fig_to_markdown_png(fig)
            desc = _llm_describe(
                llm,
                build_viz_description_prompt(
                    viz_title="Touch density heatmap",
                    viz_kind_note="KDE heatmap of on-ball touch locations (proxy for involvement zones; not pass sonar).",
                    player_name=str(state.get("player_name") or ""),
                    match_label=str(state.get("match_label") or ""),
                    structured_json=structured,
                ),
                report_language,
            )
            out["pass_sonar_visualization"] = {
                "markdown_image": md,
                "description": desc,
                "caption": _tr(report_language, "touch_caption"),
                "events": summary.get("sample_events", []),
            }
    except Exception as exc:
        out["pass_sonar_visualization"] = _empty_viz(str(exc))

    return out


def node_finalization_visualizations(state: ScoutingReportState) -> dict[str, Any]:
    match_id = int(state["match_id"])
    player_id = int(state["player_id"])
    project_id = get_bq_project_id()
    report_language = state.get("report_language", "English")
    title_main = str(state.get("player_name") or "Player")
    subtitle = str(state.get("match_label") or "Match")
    logo = branding_logo_path()
    llm = create_graph_llm()

    player_img: str | None = state.get("player_image_data_url")
    team_img: str | None = state.get("team_image_data_url")

    out: dict[str, Any] = {}

    # Shots
    try:
        pack = fetch_match_shots_for_player(player_id, match_id, project_id=project_id)
        df_s = pack["shots"] if isinstance(pack.get("shots"), pd.DataFrame) else pd.DataFrame()
        if not player_img:
            player_img = pack.get("player_image_data_url")
        if not team_img:
            team_img = pack.get("team_image_data_url")
        summary = summarize_shots_df(df_s)
        structured = structured_json_for_llm(summary)
        if df_s.empty:
            out["shot_map_visualization"] = {
                "markdown_image": "",
                "description": _llm_describe(
                    llm,
                    build_viz_description_prompt(
                        viz_title="Shot map",
                        viz_kind_note="No shot events for this player in this match.",
                        player_name=str(state.get("player_name") or ""),
                        match_label=str(state.get("match_label") or ""),
                        structured_json=structured,
                    ),
                    report_language,
                )
                or _tr(report_language, "shots_caption"),
                "caption": _tr(report_language, "shots_caption"),
                "events": [],
            }
        else:
            fig, _ = plot_shot_map_vertical(
                df_s,
                title_main,
                subtitle=subtitle,
                left_logo_path=logo,
                team_img_source=team_img,
                player_img_source=player_img,
            )
            md = fig_to_markdown_png(fig)
            desc = _llm_describe(
                llm,
                build_viz_description_prompt(
                    viz_title="Shot map",
                    viz_kind_note="Half-pitch shot map: size ~ xG, goals as football markers.",
                    player_name=str(state.get("player_name") or ""),
                    match_label=str(state.get("match_label") or ""),
                    structured_json=structured,
                ),
                report_language,
            )
            out["shot_map_visualization"] = {
                "markdown_image": md,
                "description": desc,
                "caption": _tr(report_language, "shots_caption"),
                "events": summary.get("sample_shots", []),
            }
    except Exception as exc:
        out["shot_map_visualization"] = _empty_viz(str(exc))

    # Crosses / key passes map
    try:
        df_c = fetch_match_key_pass_crosses(
            match_id=match_id,
            player_id=player_id,
            event_type="both",
            project_id=project_id,
            limit=3_000,
        )
        summary = summarize_crosses_df(df_c)
        structured = structured_json_for_llm(summary)
        if df_c.empty:
            out["crosses_map_visualization"] = {
                "markdown_image": "",
                "description": _llm_describe(
                    llm,
                    build_viz_description_prompt(
                        viz_title="Crosses & key passes map",
                        viz_kind_note="No key pass / cross rows for this player.",
                        player_name=str(state.get("player_name") or ""),
                        match_label=str(state.get("match_label") or ""),
                        structured_json=structured,
                    ),
                    report_language,
                )
                or _tr(report_language, "crosses_caption"),
                "caption": _tr(report_language, "crosses_caption"),
                "events": [],
            }
        else:
            fig, _ = plot_cross_key_pass_map_vertical(
                df_c,
                title_main,
                subtitle=subtitle,
                left_logo_path=logo,
                team_img_source=team_img,
                player_img_source=player_img,
            )
            md = fig_to_markdown_png(fig)
            desc = _llm_describe(
                llm,
                build_viz_description_prompt(
                    viz_title="Crosses & key passes map",
                    viz_kind_note="Comet lines for shot assists / key passes; blocked ends marked.",
                    player_name=str(state.get("player_name") or ""),
                    match_label=str(state.get("match_label") or ""),
                    structured_json=structured,
                ),
                report_language,
            )
            out["crosses_map_visualization"] = {
                "markdown_image": md,
                "description": desc,
                "caption": _tr(report_language, "crosses_caption"),
                "events": summary.get("sample_events", []),
            }
    except Exception as exc:
        out["crosses_map_visualization"] = _empty_viz(str(exc))

    return out
