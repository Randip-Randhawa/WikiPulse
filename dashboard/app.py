"""WikiPulse Streamlit dashboard.

Run with:
    streamlit run dashboard/app.py

Shows five panels per the project spec (Section 10):
A. System health, B. Live activity, C. Anomalies, D. Edit wars,
E. Historical/trend views.

The dashboard queries PostgreSQL directly (read-only) and is built to
degrade gracefully: every panel handles the "no data yet" / "no anomalies
right now" case instead of erroring, since a healthy pipeline may
legitimately have quiet periods.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from common.config import get_settings
from storage.database import Database

st.set_page_config(page_title="WikiPulse", layout="wide", page_icon="📈")

settings = get_settings()


@st.cache_resource
def get_db() -> Database:
    return Database(settings.postgres)


def run_query(query: str, params: tuple = ()) -> pd.DataFrame:
    try:
        rows = get_db().fetch_all(query, params)
        return pd.DataFrame(rows)
    except Exception as exc:  # pragma: no cover - UI-level guard
        st.warning(f"Query failed (pipeline may still be starting up): {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=settings.dashboard.refresh_seconds)
def load_system_health() -> pd.DataFrame:
    return run_query("""
        SELECT DISTINCT ON (component) *
        FROM pipeline_metrics
        ORDER BY component, recorded_at DESC
    """)


@st.cache_data(ttl=settings.dashboard.refresh_seconds)
def load_live_activity(minutes: int) -> pd.DataFrame:
    return run_query(
        """
        SELECT wiki, page_title, window_start, window_end, edit_count,
               unique_editors, human_edits, bot_edits, edit_velocity
        FROM page_activity_windows
        WHERE window_start >= now() - (%s || ' minutes')::interval
        ORDER BY window_start DESC, edit_velocity DESC
        LIMIT 200
        """,
        (minutes,),
    )


@st.cache_data(ttl=settings.dashboard.refresh_seconds)
def load_recent_anomalies(minutes: int) -> pd.DataFrame:
    return run_query(
        """
        SELECT wiki, page_title, window_start, window_end, edit_count,
               baseline_median, baseline_mad, anomaly_score, anomaly_type,
               threshold_used
        FROM anomaly_events
        WHERE window_start >= now() - (%s || ' minutes')::interval
        ORDER BY window_start DESC
        LIMIT 100
        """,
        (minutes,),
    )


@st.cache_data(ttl=settings.dashboard.refresh_seconds)
def load_edit_wars(minutes: int) -> pd.DataFrame:
    return run_query(
        """
        SELECT wiki, page_title, window_start, window_end, mutual_revert_count,
               editor_a, editor_b, total_reverts, edit_burst_count,
               conflict_score, flag
        FROM edit_war_signals
        WHERE window_start >= now() - (%s || ' minutes')::interval
        ORDER BY conflict_score DESC
        LIMIT 100
        """,
        (minutes,),
    )


@st.cache_data(ttl=settings.dashboard.refresh_seconds)
def load_activity_trend(hours: int) -> pd.DataFrame:
    return run_query(
        """
        SELECT date_trunc('hour', window_start) AS hour_bucket,
               SUM(edit_count) AS total_edits,
               SUM(human_edits) AS human_edits,
               SUM(bot_edits) AS bot_edits
        FROM page_activity_windows
        WHERE window_start >= now() - (%s || ' hours')::interval
        GROUP BY hour_bucket
        ORDER BY hour_bucket
        """,
        (hours,),
    )


@st.cache_data(ttl=settings.dashboard.refresh_seconds)
def load_top_pages(hours: int) -> pd.DataFrame:
    return run_query(
        """
        SELECT wiki, page_title, SUM(edit_count) AS total_edits
        FROM page_activity_windows
        WHERE window_start >= now() - (%s || ' hours')::interval
        GROUP BY wiki, page_title
        ORDER BY total_edits DESC
        LIMIT 10
        """,
        (hours,),
    )


st.title("📈 WikiPulse — Wikipedia Edit-War & Trend Anomaly Engine")
st.caption(
    "Academic prototype for real-time data engineering demonstration. "
    "Not a production Wikimedia moderation system."
)

st.button("🔄 Refresh now", on_click=st.cache_data.clear)

# --- A. System health --------------------------------------------------------
st.header("System Health")
health_df = load_system_health()
if health_df.empty:
    st.info("No pipeline metrics recorded yet. Start the ingestion service and Spark job.")
else:
    cols = st.columns(len(health_df))
    for col, (_, row) in zip(cols, health_df.iterrows()):
        with col:
            st.subheader(row.get("component", "unknown"))
            st.metric("Events received", int(row.get("events_received") or 0))
            st.metric("Events parsed", int(row.get("events_parsed") or 0))
            st.metric("Events rejected", int(row.get("events_rejected") or 0))
            st.metric("Throughput (events/sec)", round(row.get("throughput_per_sec") or 0, 2))
            st.caption(f"Latest event: {row.get('last_event_time')}")
            st.caption(f"Recorded at: {row.get('recorded_at')}")
            if (row.get("error_count") or 0) > 0:
                st.warning(f"{int(row['error_count'])} errors recorded")

st.divider()

# --- B. Live activity ---------------------------------------------------------
st.header("Live Activity")
activity_minutes = st.slider("Activity window (minutes)", 5, 180, settings.dashboard.recent_window_minutes)
activity_df = load_live_activity(activity_minutes)
if activity_df.empty:
    st.info("No page activity recorded in this window yet.")
else:
    left, right = st.columns([2, 1])
    with left:
        st.dataframe(
            activity_df[[
                "wiki", "page_title", "edit_count", "unique_editors",
                "human_edits", "bot_edits", "edit_velocity", "window_start",
            ]],
            use_container_width=True,
            hide_index=True,
        )
    with right:
        bot_human = pd.DataFrame({
            "type": ["Human", "Bot"],
            "edits": [activity_df["human_edits"].sum(), activity_df["bot_edits"].sum()],
        })
        fig = px.pie(bot_human, names="type", values="edits", title="Human vs Bot edits")
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- C. Anomalies --------------------------------------------------------------
st.header("Activity Anomalies")
anomaly_df = load_recent_anomalies(activity_minutes)
if anomaly_df.empty:
    st.success("No activity anomalies detected in this window. Pipeline is running quietly.")
else:
    st.dataframe(
        anomaly_df[[
            "wiki", "page_title", "edit_count", "baseline_median", "baseline_mad",
            "anomaly_score", "anomaly_type", "threshold_used", "window_start",
        ]],
        use_container_width=True,
        hide_index=True,
    )
    fig = px.bar(
        anomaly_df.sort_values("anomaly_score", ascending=False).head(15),
        x="page_title", y="anomaly_score", color="anomaly_type",
        title="Top anomaly scores",
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- D. Edit wars ----------------------------------------------------------------
st.header("Edit-War Signals")
war_df = load_edit_wars(activity_minutes)
if war_df.empty:
    st.success("No edit-war conflict signals in this window.")
else:
    flagged = war_df[war_df["flag"] == True]  # noqa: E712
    st.metric("Flagged pages", len(flagged))
    st.dataframe(
        war_df[[
            "wiki", "page_title", "mutual_revert_count", "editor_a", "editor_b",
            "total_reverts", "conflict_score", "flag", "window_start",
        ]],
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# --- E. Historical / trend views ------------------------------------------------
st.header("Historical Trends")
trend_hours = st.slider("Trend window (hours)", 1, 72, 24)

trend_col, top_col = st.columns(2)
with trend_col:
    trend_df = load_activity_trend(trend_hours)
    if trend_df.empty:
        st.info("Not enough historical data yet for a trend chart.")
    else:
        fig = px.line(
            trend_df, x="hour_bucket", y=["total_edits", "human_edits", "bot_edits"],
            title="Edit activity over time",
        )
        st.plotly_chart(fig, use_container_width=True)

with top_col:
    top_df = load_top_pages(trend_hours)
    if top_df.empty:
        st.info("Not enough historical data yet for top pages.")
    else:
        fig = px.bar(top_df, x="page_title", y="total_edits", title="Top pages by edit volume")
        st.plotly_chart(fig, use_container_width=True)

st.caption(
    f"Dashboard auto-refreshes queries every {settings.dashboard.refresh_seconds}s "
    "(cached); click 'Refresh now' for an immediate update."
)
