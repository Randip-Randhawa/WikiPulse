"""Data access for the Streamlit dashboard, kept free of Streamlit so it can
be tested directly.

Every query is bounded by a time range and aggregated in PostgreSQL; the
dashboard never pulls whole tables into memory. Edit counts come from
`edit_events` rather than from summing `page_activity_windows.edit_count`:
windows overlap (a 5-minute window sliding every minute holds each edit five
times), so summing them overcounts.

Each function takes `fetch`, a callable with the signature of
`Database.fetch_all`, and returns a DataFrame with fixed columns even when
there are no rows, so the UI can always rely on them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

import pandas as pd

Fetch = Callable[[str, Sequence[Any]], list[dict[str, Any]]]

EDITOR_TYPES = ("all", "human", "bot")
TOP_PAGES_LIMIT = 15
TABLE_ROW_LIMIT = 200
# Metrics rows arrive about every 30 seconds while data flows; a component
# silent for four times that is shown as stale.
STALE_AFTER_SECONDS = 120


@dataclass(frozen=True)
class Filters:
    minutes: int
    wiki: Optional[str] = None
    page_search: Optional[str] = None
    editor_type: str = "all"

    def __post_init__(self) -> None:
        if self.editor_type not in EDITOR_TYPES:
            raise ValueError(f"editor_type must be one of {EDITOR_TYPES}, got {self.editor_type!r}")


def _escape_like(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def where_clause(filters: Filters, time_column: str, has_bot_column: bool) -> tuple[str, list[Any]]:
    """Build a WHERE clause (without the keyword) and its parameters.
    `time_column` must be a trusted column name, never user input."""
    conditions = [f"{time_column} >= now() - make_interval(mins => %s)"]
    params: list[Any] = [filters.minutes]
    if filters.wiki:
        conditions.append("wiki = %s")
        params.append(filters.wiki)
    if filters.page_search:
        conditions.append("page_title ILIKE %s")
        params.append(f"%{_escape_like(filters.page_search)}%")
    if has_bot_column and filters.editor_type == "human":
        conditions.append("NOT is_bot")
    if has_bot_column and filters.editor_type == "bot":
        conditions.append("is_bot")
    return " AND ".join(conditions), params


def _frame(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    """Build a DataFrame with exactly `columns`, showing every timestamp in UTC."""
    frame = pd.DataFrame(rows, columns=columns)
    for column in columns:
        if isinstance(frame[column].dtype, pd.DatetimeTZDtype):
            frame[column] = frame[column].dt.tz_convert("UTC")
    return frame


# --- System health -----------------------------------------------------------

def latest_component_metrics(fetch: Fetch) -> pd.DataFrame:
    columns = [
        "component", "recorded_at", "events_received", "events_parsed", "events_rejected",
        "events_published", "anomalies_detected", "edit_wars_detected", "processing_latency_ms",
        "throughput_per_sec", "last_event_time", "error_count", "details",
    ]
    rows = fetch(
        f"""
        SELECT DISTINCT ON (component) {", ".join(columns)}
        FROM pipeline_metrics
        WHERE recorded_at >= now() - interval '1 day'
        ORDER BY component, recorded_at DESC
        """,
        (),
    )
    return _frame(rows, columns)


def metrics_history(fetch: Fetch, minutes: int) -> pd.DataFrame:
    """Throughput and latency per metrics row, for the health charts."""
    columns = ["recorded_at", "component", "throughput_per_sec", "processing_latency_ms",
               "end_to_end_p95_ms", "error_count"]
    rows = fetch(
        """
        SELECT recorded_at, component, throughput_per_sec, processing_latency_ms,
               (details -> 'raw_batch' -> 'end_to_end_latency_ms' ->> 'p95')::float AS end_to_end_p95_ms,
               error_count
        FROM pipeline_metrics
        WHERE recorded_at >= now() - make_interval(mins => %s)
        ORDER BY recorded_at
        """,
        (minutes,),
    )
    return _frame(rows, columns)


def detail(details: Any, *path: str) -> Any:
    """Read a nested value from a `pipeline_metrics.details` object, or None
    when any level is missing."""
    value = details
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def component_status(recorded_at: Optional[pd.Timestamp], now: pd.Timestamp) -> str:
    if recorded_at is None or pd.isna(recorded_at):
        return "no data"
    if (now - recorded_at).total_seconds() > STALE_AFTER_SECONDS:
        return "stale"
    return "ok"


def format_value(value: Any, unit: str = "", digits: int = 1) -> str:
    """Format a metric for display; a missing value reads 'unknown', never 0."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "unknown"
    if isinstance(value, float):
        return f"{value:,.{digits}f}{unit}"
    if isinstance(value, int):
        return f"{value:,}{unit}"
    return f"{value}{unit}"


# --- Live activity -----------------------------------------------------------

def edits_per_minute(fetch: Fetch, filters: Filters) -> pd.DataFrame:
    where, params = where_clause(filters, "event_time", has_bot_column=True)
    rows = fetch(
        f"""
        SELECT date_trunc('minute', event_time) AS minute,
               count(*) FILTER (WHERE NOT is_bot) AS human_edits,
               count(*) FILTER (WHERE is_bot) AS bot_edits
        FROM edit_events
        WHERE {where}
        GROUP BY minute
        ORDER BY minute
        """,
        params,
    )
    return _frame(rows, ["minute", "human_edits", "bot_edits"])


def most_active_pages(fetch: Fetch, filters: Filters, limit: int = TOP_PAGES_LIMIT) -> pd.DataFrame:
    where, params = where_clause(filters, "event_time", has_bot_column=True)
    rows = fetch(
        f"""
        SELECT wiki, page_title,
               count(*) AS edits,
               count(DISTINCT "user") AS editors,
               count(*) FILTER (WHERE is_bot) AS bot_edits,
               count(*) FILTER (WHERE is_revert) AS reverts,
               max(event_time) AS last_edit
        FROM edit_events
        WHERE {where}
        GROUP BY wiki, page_title
        ORDER BY edits DESC
        LIMIT %s
        """,
        [*params, limit],
    )
    frame = _frame(rows, ["wiki", "page_title", "edits", "editors", "bot_edits", "reverts", "last_edit"])
    frame["edits_per_minute"] = (frame["edits"] / filters.minutes).round(3)
    return frame


def latest_page_velocity(fetch: Fetch, filters: Filters, limit: int = TOP_PAGES_LIMIT) -> pd.DataFrame:
    """Edit velocity of each page's most recent finalized window."""
    where, params = where_clause(filters, "window_start", has_bot_column=False)
    rows = fetch(
        f"""
        SELECT wiki, page_title, window_start, window_end, edit_count, edit_velocity
        FROM (
            SELECT DISTINCT ON (wiki, page_title)
                   wiki, page_title, window_start, window_end, edit_count, edit_velocity
            FROM page_activity_windows
            WHERE {where}
            ORDER BY wiki, page_title, window_start DESC
        ) latest
        ORDER BY edit_velocity DESC
        LIMIT %s
        """,
        [*params, limit],
    )
    return _frame(rows, ["wiki", "page_title", "window_start", "window_end", "edit_count", "edit_velocity"])


def wikis_seen(fetch: Fetch, minutes: int) -> list[str]:
    rows = fetch(
        "SELECT DISTINCT wiki FROM edit_events WHERE event_time >= now() - make_interval(mins => %s) ORDER BY wiki",
        (minutes,),
    )
    return [row["wiki"] for row in rows]


# --- Anomalies -----------------------------------------------------------------

def recent_anomalies(fetch: Fetch, filters: Filters, limit: int = TABLE_ROW_LIMIT) -> pd.DataFrame:
    columns = ["wiki", "page_title", "window_start", "window_end", "edit_count", "unique_editors",
               "baseline_median", "baseline_mad", "anomaly_score", "anomaly_type", "threshold_used"]
    where, params = where_clause(filters, "window_start", has_bot_column=False)
    rows = fetch(
        f"""
        SELECT {", ".join(columns)}
        FROM anomaly_events
        WHERE {where}
        ORDER BY window_start DESC
        LIMIT %s
        """,
        [*params, limit],
    )
    return _frame(rows, columns)


def anomaly_frequency(fetch: Fetch, filters: Filters) -> pd.DataFrame:
    where, params = where_clause(filters, "window_start", has_bot_column=False)
    rows = fetch(
        f"""
        SELECT date_trunc('hour', window_start) AS hour, anomaly_type, count(*) AS anomalies
        FROM anomaly_events
        WHERE {where}
        GROUP BY hour, anomaly_type
        ORDER BY hour
        """,
        params,
    )
    return _frame(rows, ["hour", "anomaly_type", "anomalies"])


# --- Edit wars -------------------------------------------------------------------

def recent_edit_war_signals(fetch: Fetch, filters: Filters, limit: int = TABLE_ROW_LIMIT) -> pd.DataFrame:
    columns = ["wiki", "page_title", "window_start", "window_end", "mutual_revert_count", "editor_a",
               "editor_b", "total_reverts", "edit_burst_count", "conflict_score", "flag"]
    where, params = where_clause(filters, "window_start", has_bot_column=False)
    rows = fetch(
        f"""
        SELECT {", ".join(columns)}
        FROM edit_war_signals
        WHERE {where}
        ORDER BY window_start DESC, conflict_score DESC
        LIMIT %s
        """,
        [*params, limit],
    )
    return _frame(rows, columns)


def edit_war_trend(fetch: Fetch, filters: Filters) -> pd.DataFrame:
    where, params = where_clause(filters, "window_start", has_bot_column=False)
    rows = fetch(
        f"""
        SELECT date_trunc('hour', window_start) AS hour,
               count(*) FILTER (WHERE flag) AS flagged_windows,
               count(*) AS signal_windows,
               max(conflict_score) AS peak_conflict_score
        FROM edit_war_signals
        WHERE {where}
        GROUP BY hour
        ORDER BY hour
        """,
        params,
    )
    return _frame(rows, ["hour", "flagged_windows", "signal_windows", "peak_conflict_score"])


def revert_evidence(fetch: Fetch, wiki: str, page_title: str, minutes: int,
                    limit: int = TABLE_ROW_LIMIT) -> pd.DataFrame:
    """Recent reverts on one page: who reverted which revision."""
    columns = ["event_time", "user", "revision_id", "revert_target_revision_id", "byte_length_delta", "comment"]
    rows = fetch(
        """
        SELECT event_time, "user", revision_id, revert_target_revision_id, byte_length_delta, comment
        FROM edit_events
        WHERE is_revert AND wiki = %s AND page_title = %s
          AND event_time >= now() - make_interval(mins => %s)
        ORDER BY event_time DESC
        LIMIT %s
        """,
        (wiki, page_title, minutes, limit),
    )
    return _frame(rows, columns)


# --- Historical trends ----------------------------------------------------------

def hourly_edits(fetch: Fetch, filters: Filters) -> pd.DataFrame:
    where, params = where_clause(filters, "event_time", has_bot_column=True)
    rows = fetch(
        f"""
        SELECT date_trunc('hour', event_time) AS hour,
               count(*) AS edits,
               count(*) FILTER (WHERE NOT is_bot) AS human_edits,
               count(*) FILTER (WHERE is_bot) AS bot_edits,
               count(DISTINCT page_title) AS distinct_pages
        FROM edit_events
        WHERE {where}
        GROUP BY hour
        ORDER BY hour
        """,
        params,
    )
    return _frame(rows, ["hour", "edits", "human_edits", "bot_edits", "distinct_pages"])


def hourly_edits_for_pages(fetch: Fetch, filters: Filters, page_titles: Sequence[str]) -> pd.DataFrame:
    if not page_titles:
        return _frame([], ["hour", "page_title", "edits"])
    where, params = where_clause(filters, "event_time", has_bot_column=True)
    rows = fetch(
        f"""
        SELECT date_trunc('hour', event_time) AS hour, page_title, count(*) AS edits
        FROM edit_events
        WHERE {where} AND page_title = ANY(%s)
        GROUP BY hour, page_title
        ORDER BY hour
        """,
        [*params, list(page_titles)],
    )
    return _frame(rows, ["hour", "page_title", "edits"])


def parquet_daily_event_counts(base_path: str, first_day: date, last_day: date) -> pd.DataFrame:
    """Events per day and wiki from the Parquet raw_events history, counted
    from file footers so no row data is loaded."""
    import pyarrow as pa
    import pyarrow.dataset as ds

    columns = ["date", "wiki", "events"]
    dataset_path = Path(base_path) / "raw_events"
    if not dataset_path.exists():
        return pd.DataFrame(columns=columns)

    partitioning = ds.partitioning(
        pa.schema([("wiki", pa.string()), ("date", pa.string()), ("hour", pa.string())]), flavor="hive")
    dataset = ds.dataset(dataset_path, format="parquet", partitioning=partitioning)
    in_range = (ds.field("date") >= first_day.isoformat()) & (ds.field("date") <= last_day.isoformat())

    counts: dict[tuple[str, str], int] = {}
    for fragment in dataset.get_fragments(filter=in_range):
        keys = ds.get_partition_keys(fragment.partition_expression)
        key = (keys["date"], keys["wiki"])
        counts[key] = counts.get(key, 0) + fragment.count_rows()
    rows = [{"date": day, "wiki": wiki, "events": events} for (day, wiki), events in sorted(counts.items())]
    return pd.DataFrame(rows, columns=columns)
