"""Player comparison page — scout multiple players with interactive charts."""

from __future__ import annotations

import base64
import re
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from services.llm_factory import create_graph_llm
from services.player_compare_service import (
    fetch_cohort_player_ids_cached,
    fetch_player_metrics_aggregated_cached,
    get_available_teams_cached,
    load_metrics_catalog_cached,
    search_players_cached,
)

# === Page config ===
st.set_page_config(layout="wide")
st.title("⚽ Player comparison")
st.caption("Scout and compare players across multiple metrics with interactive visualizations.")

# Session state keys
if "pc_descriptions" not in st.session_state:
    st.session_state.pc_descriptions = {}


def _render_player_image(image_data_url: str | None, fallback: str = "No image") -> None:
    """Render player/team image from data URL."""
    if not image_data_url or not image_data_url.strip():
        st.caption(fallback)
        return
    
    m = re.match(r"^data:image/[^;]+;base64,(.+)$", image_data_url.strip())
    if m:
        try:
            st.image(base64.b64decode(m.group(1)), use_container_width=True)
            return
        except Exception:
            pass
    
    # Try as URL
    try:
        st.image(image_data_url, use_container_width=True)
    except Exception:
        st.caption(fallback)


# === Load catalog ===
catalog = load_metrics_catalog_cached()
all_metrics = catalog.get("metrics", [])
metric_id_to_label = {m["id"]: m["label"] for m in all_metrics}
metric_id_to_category = {m["id"]: m.get("category", "other") for m in all_metrics}
categories_info = catalog.get("categories", {})

# === Section 1: Target player selection ===
st.subheader("1. Select target player")

col_search, col_limit, col_player_img, col_team_img = st.columns([3, 1, 1, 1])

with col_search:
    player_search_query = st.text_input(
        "Search player by name",
        placeholder="e.g. Lamine Yamal",
        key="pc_player_search",
    )
with col_limit:
    search_limit = st.number_input("Max results", min_value=5, max_value=100, value=30, step=5, key="pc_search_limit")

target_player_id = None
target_player_info = None

if player_search_query.strip():
    search_results = search_players_cached(player_search_query.strip(), limit=search_limit)
    if not search_results:
        st.warning("No players found matching your query.")
    else:
        st.caption(f"Found {len(search_results)} player(s):")
        
        def player_label(p: dict) -> str:
            role = f" ({p['role_name']})" if p.get("role_name") else ""
            team = f" — {p['current_team_name']}" if p.get("current_team_name") else ""
            return f"{p['display_name']}{role}{team}"
        
        player_options = {p["player_id"]: player_label(p) for p in search_results}
        
        selected_player_id = st.selectbox(
            "Select target player",
            options=list(player_options.keys()),
            format_func=lambda pid: player_options[pid],
            key="pc_target_player",
        )
        
        if selected_player_id:
            target_player_id = selected_player_id
            target_player_info = next((p for p in search_results if p["player_id"] == selected_player_id), None)
            
            # Display images
            with col_player_img:
                st.caption("Player")
                _render_player_image(target_player_info.get("player_image_data_url"), "No player image")
            
            with col_team_img:
                st.caption("Team")
                _render_player_image(target_player_info.get("team_image_data_url"), "No team image")

# === Section 2: Date range + cohort filters ===
st.subheader("2. Define comparison filters")

col_date_start, col_date_end, col_max_comp, col_min_events = st.columns(4)

with col_date_start:
    default_start = date.today() - timedelta(days=180)
    start_date_input = st.date_input("Start date", value=default_start, key="pc_start_date")
with col_date_end:
    default_end = date.today()
    end_date_input = st.date_input("End date", value=default_end, key="pc_end_date")
with col_max_comp:
    max_comparators = st.number_input("Max comparators", min_value=5, max_value=50, value=20, step=5, key="pc_max_comp")
with col_min_events:
    min_events = st.number_input("Min events", min_value=10, max_value=500, value=50, step=10, key="pc_min_events")

start_date_str = datetime.combine(start_date_input, datetime.min.time()).isoformat()
end_date_str = datetime.combine(end_date_input, datetime.max.time()).isoformat()

st.caption("Optional filters (leave empty to include all):")

# Load available teams for selector
available_teams = get_available_teams_cached()
team_name_to_id = {t["team_name"]: t["team_id"] for t in available_teams}
team_options = sorted(team_name_to_id.keys())

col_teams, col_roles = st.columns(2)

with col_teams:
    selected_team_names = st.multiselect(
        "Filter by teams",
        options=team_options,
        placeholder="Select teams...",
        key="pc_team_names",
    )

with col_roles:
    role_names_input = st.text_input(
        "Filter by roles (comma-separated)",
        placeholder="e.g. Forward, Midfielder",
        key="pc_role_names",
    )

# Convert team names to IDs
team_ids_filter = None
if selected_team_names:
    team_ids_filter = tuple(team_name_to_id[name] for name in selected_team_names)

role_names_filter = None
if role_names_input.strip():
    role_names_filter = tuple(x.strip() for x in role_names_input.split(",") if x.strip())

# === Section 3: Metric selection ===
st.subheader("3. Select metrics")

# Group metrics by category
metrics_by_category: dict[str, list[dict]] = {}
for m in all_metrics:
    cat = m.get("category", "other")
    metrics_by_category.setdefault(cat, []).append(m)

# Multi-column layout for categories
category_keys = sorted(metrics_by_category.keys(), key=lambda c: (c != "finalization", c != "build_up", c != "defensive", c))
cols_cat = st.columns(len(category_keys))

selected_metric_ids = []
for idx, cat_key in enumerate(category_keys):
    with cols_cat[idx]:
        cat_info = categories_info.get(cat_key, {})
        cat_label = cat_info.get("label", cat_key.replace("_", " ").title())
        st.markdown(f"**{cat_label}**")
        for m in metrics_by_category[cat_key]:
            if st.checkbox(m["label"], key=f"pc_metric_{m['id']}"):
                selected_metric_ids.append(m["id"])

if not selected_metric_ids:
    st.info("Select at least one metric to visualize.")
    st.stop()

# === Section 4: Aggregation mode ===
col_mode, col_spacer = st.columns([1, 3])
with col_mode:
    per_match_mode = st.toggle("Per match mode", value=False, key="pc_per_match")
st.caption("When enabled, values are divided by matches played for each player.")

# === Fetch cohort ===
if not target_player_id:
    st.info("Select a target player to continue.")
    st.stop()

with st.spinner("Fetching cohort..."):
    cohort_data = fetch_cohort_player_ids_cached(
        target_player_id=target_player_id,
        start_date=start_date_str,
        end_date=end_date_str,
        team_ids=team_ids_filter,
        role_names=role_names_filter,
        max_comparators=max_comparators,
        min_events_threshold=min_events,
    )

target = cohort_data.get("target")
comparators = cohort_data.get("comparators", [])

if not target:
    st.error("Target player has no activity in the selected date range.")
    st.stop()

if not comparators:
    st.warning("No comparators found matching your filters. Showing target player only.")

all_player_ids = [target["player_id"]] + [c["player_id"] for c in comparators]

# === Fetch metrics ===
with st.spinner("Fetching metrics..."):
    metrics_df = fetch_player_metrics_aggregated_cached(
        player_ids=tuple(all_player_ids),
        metric_ids=tuple(selected_metric_ids),
        start_date=start_date_str,
        end_date=end_date_str,
        per_match_mode=per_match_mode,
    )

if metrics_df.empty:
    st.error("No metric data available for the selected players and date range.")
    st.stop()

# Apply per-match normalization if enabled
if per_match_mode:
    metrics_df["value_normalized"] = metrics_df.apply(
        lambda row: row["value"] / row["matches_played"] if row["matches_played"] > 0 else 0,
        axis=1,
    )
else:
    metrics_df["value_normalized"] = metrics_df["value"]

# Mark target player
metrics_df["is_target"] = metrics_df["player_id"] == target_player_id

# Add player type label for charts
metrics_df["player_type"] = metrics_df["is_target"].map({True: "Target", False: "Comparator"})

# === Section 5: Charts ===
st.subheader("4. Interactive visualizations")

# Pivot data for easier charting
pivot_df = metrics_df.pivot_table(
    index=["player_id", "display_name", "is_target", "player_type"],
    columns="metric_id",
    values="value_normalized",
    fill_value=0,
).reset_index()

# Helper to generate LLM description
def generate_description(chart_id: str, context: str) -> None:
    """Generate LLM description for a chart and store in session state."""
    key = f"pc_descriptions.{chart_id}"
    
    llm = create_graph_llm()
    prompt = f"""
You are analyzing a player comparison chart.

Context:
- Target player: {target['display_name']} ({target.get('role_name', 'N/A')})
- Number of comparators: {len(comparators)}
- Date range: {start_date_input} to {end_date_input}
- Aggregation: {"Per match" if per_match_mode else "Total"}
- Selected metrics: {", ".join([metric_id_to_label[mid] for mid in selected_metric_ids])}

Chart-specific context:
{context}

Provide a concise (2-3 sentences) description highlighting key insights and comparisons. Focus on the target player's performance relative to comparators.
"""
    
    with st.spinner("Generating description..."):
        try:
            response = llm.invoke(prompt)
            description = (getattr(response, "content", "") or "").strip()
            st.session_state.pc_descriptions[key] = description
        except Exception as e:
            st.session_state.pc_descriptions[key] = f"Error generating description: {str(e)}"

# Chart 1: Scatter plot (if at least 2 metrics selected)
if len(selected_metric_ids) >= 2:
    st.markdown("### Scatter plot")
    
    col_x, col_y = st.columns(2)
    with col_x:
        x_metric = st.selectbox("X axis", options=selected_metric_ids, format_func=lambda m: metric_id_to_label[m], key="pc_scatter_x")
    with col_y:
        y_metric = st.selectbox("Y axis", options=selected_metric_ids, format_func=lambda m: metric_id_to_label[m], index=min(1, len(selected_metric_ids)-1), key="pc_scatter_y")
    
    if x_metric and y_metric and x_metric in pivot_df.columns and y_metric in pivot_df.columns:
        fig_scatter = px.scatter(
            pivot_df,
            x=x_metric,
            y=y_metric,
            color="player_type",
            hover_name="display_name",
            labels={
                x_metric: metric_id_to_label[x_metric],
                y_metric: metric_id_to_label[y_metric],
                "player_type": "Type"
            },
            color_discrete_map={"Target": "#e74c3c", "Comparator": "#3498db"},
            title=f"{metric_id_to_label[x_metric]} vs {metric_id_to_label[y_metric]}",
        )
        fig_scatter.update_traces(marker=dict(size=12))
        st.plotly_chart(fig_scatter, use_container_width=True)
        
        with st.expander("Description"):
            desc_key = "pc_descriptions.scatter"
            if desc_key in st.session_state.pc_descriptions:
                st.markdown(st.session_state.pc_descriptions[desc_key])
            else:
                st.caption("No description generated yet.")
            
            if st.button("Generate description", key="pc_scatter_gen"):
                context = f"Chart type: Scatter plot comparing {metric_id_to_label[x_metric]} (X) vs {metric_id_to_label[y_metric]} (Y)."
                generate_description("scatter", context)
                st.rerun()

# Chart 2: Bar chart (rankings for a single metric)
if selected_metric_ids:
    st.markdown("### Bar chart — Rankings")
    
    rank_metric = st.selectbox(
        "Select metric to rank",
        options=selected_metric_ids,
        format_func=lambda m: metric_id_to_label[m],
        key="pc_bar_metric",
    )
    
    if rank_metric and rank_metric in pivot_df.columns:
        bar_df = pivot_df[["display_name", rank_metric, "player_type"]].sort_values(by=rank_metric, ascending=False).head(20)
        
        fig_bar = px.bar(
            bar_df,
            x="display_name",
            y=rank_metric,
            color="player_type",
            labels={
                rank_metric: metric_id_to_label[rank_metric],
                "display_name": "Player",
                "player_type": "Type"
            },
            color_discrete_map={"Target": "#e74c3c", "Comparator": "#3498db"},
            title=f"Top 20 players — {metric_id_to_label[rank_metric]}",
        )
        fig_bar.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_bar, use_container_width=True)
        
        with st.expander("Description"):
            desc_key = "pc_descriptions.bar"
            if desc_key in st.session_state.pc_descriptions:
                st.markdown(st.session_state.pc_descriptions[desc_key])
            else:
                st.caption("No description generated yet.")
            
            if st.button("Generate description", key="pc_bar_gen"):
                top_3 = bar_df.head(3)["display_name"].tolist()
                target_rank = bar_df.reset_index(drop=True).index[bar_df["display_name"] == target["display_name"]].tolist()
                target_rank_str = f"rank {target_rank[0] + 1}" if target_rank else "not in top 20"
                context = f"Chart type: Bar chart ranking {metric_id_to_label[rank_metric]}. Top 3: {', '.join(top_3)}. Target player: {target_rank_str}."
                generate_description("bar", context)
                st.rerun()

# Chart 3: Radar chart (if at least 3 metrics selected)
if len(selected_metric_ids) >= 3:
    st.markdown("### Radar chart")
    st.caption("Metrics are normalized (min-max scaling) within the displayed cohort for comparability.")
    
    # Normalize metrics for radar (0-100 scale)
    radar_metrics = selected_metric_ids[:min(8, len(selected_metric_ids))]  # Limit to 8 metrics for readability
    radar_df = pivot_df[["player_id", "display_name", "is_target"] + radar_metrics].copy()
    
    for metric in radar_metrics:
        min_val = radar_df[metric].min()
        max_val = radar_df[metric].max()
        if max_val > min_val:
            radar_df[f"{metric}_norm"] = 100 * (radar_df[metric] - min_val) / (max_val - min_val)
        else:
            radar_df[f"{metric}_norm"] = 50  # All same value
    
    # Show top N players + target
    radar_top_n = st.slider("Number of players to display", min_value=2, max_value=min(10, len(all_player_ids)), value=5, key="pc_radar_n")
    
    # Get top N by sum of normalized metrics
    radar_df["total_score"] = radar_df[[f"{m}_norm" for m in radar_metrics]].sum(axis=1)
    radar_df_sorted = radar_df.sort_values(by="total_score", ascending=False)
    
    # Ensure target is included
    target_row = radar_df_sorted[radar_df_sorted["is_target"] == True]
    other_rows = radar_df_sorted[radar_df_sorted["is_target"] == False].head(radar_top_n - 1)
    radar_display = pd.concat([target_row, other_rows])
    
    fig_radar = go.Figure()
    
    for _, player_row in radar_display.iterrows():
        values = [player_row[f"{m}_norm"] for m in radar_metrics]
        values.append(values[0])  # Close the radar
        
        categories = [metric_id_to_label[m] for m in radar_metrics]
        categories.append(categories[0])
        
        color = "#e74c3c" if player_row["is_target"] else "#3498db"
        
        fig_radar.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            name=player_row["display_name"],
            line=dict(color=color, width=2 if player_row["is_target"] else 1),
            opacity=0.7 if player_row["is_target"] else 0.4,
        ))
    
    fig_radar.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100])
        ),
        showlegend=True,
        title="Player comparison radar (normalized metrics)",
    )
    
    st.plotly_chart(fig_radar, use_container_width=True)
    
    with st.expander("Description"):
        desc_key = "pc_descriptions.radar"
        if desc_key in st.session_state.pc_descriptions:
            st.markdown(st.session_state.pc_descriptions[desc_key])
        else:
            st.caption("No description generated yet.")
        
        if st.button("Generate description", key="pc_radar_gen"):
            displayed_players = radar_display["display_name"].tolist()
            context = f"Chart type: Radar chart with normalized metrics {', '.join([metric_id_to_label[m] for m in radar_metrics])}. Players: {', '.join(displayed_players)}."
            generate_description("radar", context)
            st.rerun()

# === Section 6: Data table ===
st.subheader("5. Raw data")
with st.expander("Show metric data table", expanded=False):
    display_df = metrics_df[["display_name", "metric_label", "value_normalized", "matches_played", "player_type"]].copy()
    display_df.rename(columns={
        "display_name": "Player",
        "metric_label": "Metric",
        "value_normalized": "Value",
        "matches_played": "Matches",
        "player_type": "Type",
    }, inplace=True)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

st.success("Player comparison complete!")
