"""WikiPulse Streamlit dashboard.

Run with:
    streamlit run dashboard/app.py

Five sections: System Health, Live Activity, Activity Anomalies, Edit-War
Signals and Historical Trends. Data comes from PostgreSQL, which holds the
last POSTGRES_RETENTION_HOURS; the long-range daily totals come from the
Parquet history. Queries live in dashboard/queries.py.

Every panel handles the "no data yet" / "no anomalies right now" case
instead of erroring, since a healthy pipeline may legitimately have quiet
periods, and a metric that was never reported is shown as "unknown" rather
than as zero.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

from common.config import get_settings
from dashboard import queries
from dashboard.queries import Filters, component_status, detail, format_value
from storage.database import Database

st.set_page_config(page_title="WikiPulse", layout="wide", page_icon="📈")

settings = get_settings()
# Parquet history changes once per micro-batch and is only summarized by
# day, so it is re-read less often than the live PostgreSQL panels.
PARQUET_CACHE_SECONDS = 300
TIME_RANGE_OPTIONS = {
    "15 minutes": 15, "1 hour": 60, "3 hours": 180, "6 hours": 360,
    "12 hours": 720, "24 hours": 1440, "72 hours": 4320,
}


@st.cache_resource
def get_db() -> Database:
    return Database(settings.postgres)


def fetch(query: str, params) -> list[dict[str, Any]]:
    return get_db().fetch_all(query, params)


@st.cache_data(ttl=settings.dashboard.refresh_seconds, show_spinner=False)
def cached_query(name: str, *args):
    return getattr(queries, name)(fetch, *args)


def load(name: str, *args) -> Optional[Any]:
    """Run a cached query; on failure show a warning and return None so one
    broken panel does not take down the page."""
    try:
        return cached_query(name, *args)
    except Exception as exc:
        st.warning(f"Could not load {name.replace('_', ' ')}: {exc}")
        return None


@st.cache_data(ttl=PARQUET_CACHE_SECONDS, show_spinner=False)
def parquet_daily_counts(first_day: date, last_day: date) -> pd.DataFrame:
    return queries.parquet_daily_event_counts(settings.parquet.base_path, first_day, last_day)


def utc_label(column: str) -> str:
    return f"{column.replace('_', ' ').capitalize()} (UTC)"


# --- System health -----------------------------------------------------------

def show_status(row: Optional[dict[str, Any]], now: pd.Timestamp) -> None:
    status = component_status(row["recorded_at"] if row else None, now)
    if status == "ok":
        st.success(f"Reporting · last report {row['recorded_at']:%H:%M:%S} UTC")
    elif status == "stale":
        st.warning(f"Stale · last report {row['recorded_at']:%Y-%m-%d %H:%M:%S} UTC")
    else:
        st.info("No metrics reported in the last day")


def render_ingestion_health(row: Optional[dict[str, Any]], now: pd.Timestamp) -> None:
    st.subheader("Ingestion (SSE → Kafka)")
    show_status(row, now)
    if row is None:
        return
    details = row["details"]
    dead_letter_rate = detail(details, "dead_letter_rate")
    st.metric("Throughput", format_value(row["throughput_per_sec"], " events/s", 2))
    st.metric("Dead-letter rate", "unknown" if dead_letter_rate is None else f"{dead_letter_rate:.2%}")
    st.metric("Ingestion latency p95", format_value(detail(details, "ingestion_latency_ms", "p95"), " ms", 0))
    st.caption(
        f"Received {format_value(row['events_received'])} · ignored {format_value(detail(details, 'events_ignored'))}"
        f" · rejected {format_value(row['events_rejected'])} · delivered to Kafka {format_value(row['events_published'])}"
    )
    st.caption(
        f"Delivery failures {format_value(detail(details, 'delivery_failures'))} · "
        f"reconnects {format_value(detail(details, 'reconnects'))} · "
        f"normalize p95 {format_value(detail(details, 'normalize_ms', 'p95'), ' ms', 3)}"
    )


def render_processing_health(row: Optional[dict[str, Any]], now: pd.Timestamp) -> None:
    st.subheader("Processing (Kafka → PostgreSQL / Parquet)")
    show_status(row, now)
    if row is None:
        return
    details = row["details"]
    raw_batch = detail(details, "raw_batch")
    st.metric("Throughput", format_value(row["throughput_per_sec"], " events/s", 2))
    st.metric("End-to-end latency p95", format_value(detail(raw_batch, "end_to_end_latency_ms", "p95"), " ms", 0))
    st.metric("Kafka offsets behind (max)", format_value(detail(details, "kafka_offsets_behind", "max"), "", 0))
    st.caption(
        "Latency p50 / p95 / p99 — processing "
        + " / ".join(format_value(detail(raw_batch, "processing_latency_ms", p), " ms", 0) for p in ("p50", "p95", "p99"))
        + " · end-to-end "
        + " / ".join(format_value(detail(raw_batch, "end_to_end_latency_ms", p), " ms", 0) for p in ("p50", "p95", "p99"))
    )
    parquet = detail(raw_batch, "parquet") or {}
    compression = (
        parquet["uncompressed_bytes"] / parquet["compressed_bytes"] if parquet.get("compressed_bytes") else None
    )
    st.caption(
        f"Last batch: {format_value(detail(raw_batch, 'rows'))} rows · PostgreSQL write "
        f"{format_value(detail(raw_batch, 'postgres_write_ms'), ' ms', 0)} · Parquet write "
        f"{format_value(detail(raw_batch, 'parquet_write_ms'), ' ms', 0)} "
        f"({format_value(parquet.get('file_bytes'), ' bytes')}, Snappy ratio {format_value(compression, 'x', 2)})"
    )
    errors = detail(details, "errors_by_stage") or {}
    if errors:
        st.warning("Errors since start: " + ", ".join(f"{stage} {count}" for stage, count in sorted(errors.items())))
    else:
        st.caption("No PostgreSQL, Parquet or Lift Wing write errors since the job started.")


def render_detection_health(row: Optional[dict[str, Any]]) -> None:
    st.subheader("Detection state")
    details = row["details"] if row else None
    window_batch = detail(details, "window_batch")
    page_history = detail(details, "page_history")
    if window_batch is None:
        st.info("No windows finalized yet (each window is emitted after the watermark passes its end)")
        return
    st.metric("Windows in last batch", format_value(detail(window_batch, "windows")))
    st.metric("Window result delay p95", format_value(detail(window_batch, "window_emit_delay_ms", "p95"), " ms", 0))
    st.metric("Pages with anomaly history", format_value(detail(page_history, "active_pages")))
    st.caption(
        f"Evicted inactive pages {format_value(detail(page_history, 'evicted_pages_total'))} · "
        f"stored window counts {format_value(detail(page_history, 'observations'))} · "
        f"driver memory {format_value(detail(details, 'driver_rss_mb'), ' MB')}"
    )


def render_health(filters: Filters) -> None:
    st.header("System Health")
    latest = load("latest_component_metrics")
    if latest is None:
        return
    now = pd.Timestamp.now(tz="UTC")
    rows = {row["component"]: row for row in latest.to_dict("records")}
    st.caption("PostgreSQL: reachable from the dashboard.")

    ingestion_col, processing_col, detection_col = st.columns(3)
    with ingestion_col:
        render_ingestion_health(rows.get("ingestion"), now)
    with processing_col:
        render_processing_health(rows.get("spark"), now)
    with detection_col:
        render_detection_health(rows.get("spark"))

    history = load("metrics_history", filters.minutes)
    if history is not None and not history.empty:
        throughput_col, latency_col = st.columns(2)
        with throughput_col:
            fig = px.line(history, x="recorded_at", y="throughput_per_sec", color="component", markers=True,
                          title="Throughput", labels={"recorded_at": "Time (UTC)", "throughput_per_sec": "Events/s"})
            st.plotly_chart(fig, use_container_width=True)
        with latency_col:
            spark_history = history[history["component"] == "spark"]
            fig = px.line(spark_history, x="recorded_at", y=["processing_latency_ms", "end_to_end_p95_ms"],
                          markers=True, title="Processing latency p50 and end-to-end p95 per batch",
                          labels={"recorded_at": "Time (UTC)", "value": "Milliseconds", "variable": "Measure"})
            st.plotly_chart(fig, use_container_width=True)


# --- Live activity -----------------------------------------------------------

def render_live_activity(filters: Filters) -> None:
    st.header("Live Activity")
    per_minute = load("edits_per_minute", filters)
    top_pages = load("most_active_pages", filters)
    if per_minute is None or top_pages is None:
        return
    if per_minute.empty:
        st.info("No edits match these filters in the selected time range.")
        return

    chart_col, split_col = st.columns([3, 1])
    with chart_col:
        fig = px.area(per_minute, x="minute", y=["human_edits", "bot_edits"], title="Edits per minute",
                      labels={"minute": utc_label("minute"), "value": "Edits", "variable": "Editor type"})
        st.plotly_chart(fig, use_container_width=True)
    with split_col:
        split = pd.DataFrame({
            "editor": ["Human", "Bot"],
            "edits": [per_minute["human_edits"].sum(), per_minute["bot_edits"].sum()],
        })
        st.plotly_chart(px.pie(split, names="editor", values="edits", title="Human vs bot edits"),
                        use_container_width=True)

    pages_col, velocity_col = st.columns(2)
    with pages_col:
        st.subheader("Most active pages")
        st.caption("Edits in the selected range; edits/min averages over the whole range.")
        st.dataframe(top_pages, use_container_width=True, hide_index=True)
    with velocity_col:
        st.subheader("Current edit velocity")
        st.caption("Edits per minute in each page's latest finalized window.")
        velocity = load("latest_page_velocity", filters)
        if velocity is not None and velocity.empty:
            st.info("No finalized windows in this range yet.")
        elif velocity is not None:
            st.dataframe(velocity, use_container_width=True, hide_index=True)


# --- Anomalies -----------------------------------------------------------------

def render_anomalies(filters: Filters) -> None:
    st.header("Activity Anomalies")
    st.caption(
        "median_mad rows score a window with the modified z-score against the page's own history; "
        "count_fallback rows are pages with too little history, flagged when the raw edit count passes a "
        "fixed threshold (their score is that count). The two scores are not comparable."
    )
    anomalies = load("recent_anomalies", filters)
    if anomalies is None:
        return
    if anomalies.empty:
        st.success("No activity anomalies in this range. The pipeline is running quietly.")
        return

    frequency = load("anomaly_frequency", filters)
    if frequency is not None:
        fig = px.bar(frequency, x="hour", y="anomalies", color="anomaly_type", title="Anomalies per hour",
                     labels={"hour": utc_label("hour"), "anomalies": "Anomalous windows", "anomaly_type": "Type"})
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(anomalies, use_container_width=True, hide_index=True)

    scored = anomalies[anomalies["anomaly_type"] == "median_mad"]
    if not scored.empty:
        top_scores = scored.sort_values("anomaly_score", ascending=False).head(15)
        fig = px.bar(top_scores, x="page_title", y="anomaly_score", hover_data=["window_start", "edit_count",
                     "baseline_median", "baseline_mad"], title="Highest median/MAD scores",
                     labels={"page_title": "Page", "anomaly_score": "Modified z-score"})
        st.plotly_chart(fig, use_container_width=True)


# --- Edit wars -------------------------------------------------------------------

def render_edit_wars(filters: Filters) -> None:
    st.header("Edit-War Signals")
    st.caption(
        "A signal means two editors reverted each other within one window. It is separate from activity "
        "anomalies: high activity alone never produces one, and a signal is an indicator, not a verdict."
    )
    signals = load("recent_edit_war_signals", filters)
    if signals is None:
        return
    if signals.empty:
        st.success("No edit-war signals in this range.")
        return

    flagged = signals[signals["flag"]]
    st.metric("Flagged windows", len(flagged))
    trend = load("edit_war_trend", filters)
    if trend is not None:
        fig = px.bar(trend, x="hour", y=["flagged_windows", "signal_windows"], barmode="group",
                     title="Edit-war signals per hour",
                     labels={"hour": utc_label("hour"), "value": "Windows", "variable": "Measure"})
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(signals, use_container_width=True, hide_index=True)

    pages = signals[["wiki", "page_title"]].drop_duplicates().to_records(index=False)
    choice = st.selectbox("Show the reverts behind a signal", [f"{wiki} · {title}" for wiki, title in pages])
    if choice:
        wiki, page_title = choice.split(" · ", 1)
        evidence = load("revert_evidence", wiki, page_title, filters.minutes)
        if evidence is not None and evidence.empty:
            st.info("No revert events for this page are left in PostgreSQL for this range.")
        elif evidence is not None:
            st.dataframe(evidence, use_container_width=True, hide_index=True)


# --- Historical trends ----------------------------------------------------------

def render_history(filters: Filters) -> None:
    st.header("Historical Trends")
    hourly = load("hourly_edits", filters)
    if hourly is None:
        return
    if hourly.empty:
        st.info("Not enough data in PostgreSQL yet for a trend chart.")
    else:
        fig = px.line(hourly, x="hour", y=["edits", "human_edits", "bot_edits"], title="Edits per hour", markers=True,
                      labels={"hour": utc_label("hour"), "value": "Edits", "variable": "Measure"})
        st.plotly_chart(fig, use_container_width=True)

        top_pages = load("most_active_pages", filters)
        options = [] if top_pages is None else top_pages["page_title"].tolist()
        compared = st.multiselect("Compare pages", options, default=options[:3])
        comparison = load("hourly_edits_for_pages", filters, tuple(compared))
        if comparison is not None and not comparison.empty:
            fig = px.line(comparison, x="hour", y="edits", color="page_title", title="Edits per hour by page",
                          markers=True,
                          labels={"hour": utc_label("hour"), "edits": "Edits", "page_title": "Page"})
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Long-range history (Parquet)")
    today = date.today()
    selected_days = st.date_input("Days", value=(today - timedelta(days=30), today))
    if len(selected_days) != 2:
        st.caption("Pick a first and a last day.")
        return
    first_day, last_day = selected_days
    try:
        daily = parquet_daily_counts(first_day, last_day)
    except Exception as exc:
        st.warning(f"Could not read Parquet history at {settings.parquet.base_path}: {exc}")
        return
    if daily.empty:
        st.info(f"No Parquet raw-event files for these days under {settings.parquet.base_path}.")
        return
    fig = px.bar(daily, x="date", y="events", color="wiki", title="Events per day (Parquet raw_events)",
                 labels={"date": "Date (UTC)", "events": "Events"})
    fig.update_xaxes(type="category")
    st.plotly_chart(fig, use_container_width=True)


# --- Page ------------------------------------------------------------------------

def sidebar_filters() -> tuple[Filters, bool]:
    st.sidebar.header("Filters")
    default_label = next(
        (label for label, minutes in TIME_RANGE_OPTIONS.items()
         if minutes == settings.dashboard.recent_window_minutes),
        "1 hour",
    )
    labels = list(TIME_RANGE_OPTIONS)
    range_label = st.sidebar.selectbox("Time range", labels, index=labels.index(default_label))
    wikis = load("wikis_seen", TIME_RANGE_OPTIONS["72 hours"]) or []
    wiki = st.sidebar.selectbox("Wiki", ["All wikis", *wikis])
    page_search = st.sidebar.text_input("Page title contains").strip()
    editor_type = st.sidebar.radio("Editors", ["all", "human", "bot"], horizontal=True,
                                   help="Applies to panels built from individual edits.")
    auto_refresh = st.sidebar.toggle("Auto-refresh", value=True,
                                     help=f"Re-run the queries every {settings.dashboard.refresh_seconds}s.")
    st.sidebar.button("Refresh now", on_click=st.cache_data.clear)
    filters = Filters(
        minutes=TIME_RANGE_OPTIONS[range_label],
        wiki=None if wiki == "All wikis" else wiki,
        page_search=page_search or None,
        editor_type=editor_type,
    )
    return filters, auto_refresh


def render_panels(filters: Filters) -> None:
    render_health(filters)
    st.divider()
    render_live_activity(filters)
    st.divider()
    render_anomalies(filters)
    st.divider()
    render_edit_wars(filters)
    st.divider()
    render_history(filters)
    st.caption(f"Times are UTC. Queries are cached for {settings.dashboard.refresh_seconds}s.")


st.title("📈 WikiPulse — Wikipedia Edit-War & Trend Anomaly Engine")
st.caption(
    "Academic prototype for real-time data engineering demonstration. "
    "Not a production Wikimedia moderation system."
)

try:
    get_db().fetch_all("SELECT 1")
except Exception as exc:
    st.error(
        f"Cannot reach PostgreSQL at {settings.postgres.host}:{settings.postgres.port}/"
        f"{settings.postgres.database}: {exc}\n\n"
        "Start it with `make infra-up` and create the schema with `make init-db`."
    )
    st.stop()

current_filters, auto_refresh_enabled = sidebar_filters()
run_every = settings.dashboard.refresh_seconds if auto_refresh_enabled else None
st.fragment(run_every=run_every)(render_panels)(current_filters)
