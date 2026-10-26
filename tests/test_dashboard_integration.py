"""Runs the dashboard's SQL and the Streamlit page itself against a real,
disposable PostgreSQL database (see tests/conftest.py), both empty and
populated."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common.config import get_settings
from dashboard import queries
from dashboard.queries import Filters
from tests.test_database_integration import anomaly_row, edit_event

pytestmark = pytest.mark.postgres

APP_PATH = "dashboard/app.py"


def seed(database) -> datetime:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    database.init_schema(now=now)
    events = []
    for minute in range(10):
        event = edit_event(f"human-{minute}", now - timedelta(minutes=minute))
        events.append(event)
    for minute in range(4):
        events.append({**edit_event(f"bot-{minute}", now - timedelta(minutes=minute)),
                       "user": "CleanupBot", "is_bot": True, "page_title": "Other Page"})
    events.append({**edit_event("revert-1", now - timedelta(minutes=2)), "user": "Bob",
                   "is_revert": True, "revision_id": 5, "revert_target_revision_id": 1})
    database.upsert_edit_events(events)

    window_start = now - timedelta(minutes=10)
    database.upsert_activity_windows([{
        "wiki": "enwiki", "page_id": 1, "page_title": "Page", "window_start": window_start,
        "window_end": window_start + timedelta(minutes=5), "edit_count": 5, "unique_editors": 2,
        "human_edits": 5, "bot_edits": 0, "revert_count": 1, "net_byte_delta": 10, "edit_velocity": 1.0,
    }])
    database.insert_anomaly_events([{**anomaly_row(20), "window_start": window_start,
                                     "window_end": window_start + timedelta(minutes=5)}])
    database.insert_edit_war_signals([{
        "wiki": "enwiki", "page_id": 1, "page_title": "Page", "window_start": window_start,
        "window_end": window_start + timedelta(minutes=5), "mutual_revert_count": 2, "editor_a": "Alice",
        "editor_b": "Bob", "total_reverts": 2, "edit_burst_count": 1, "conflict_score": 4.5, "flag": True,
    }])
    database.record_pipeline_metrics({
        "component": "spark", "events_received": 15, "events_parsed": 15, "events_rejected": 0,
        "events_published": 15, "anomalies_detected": 1, "edit_wars_detected": 1,
        "processing_latency_ms": 800.0, "throughput_per_sec": 0.5, "last_event_time": now, "error_count": 0,
        "details": {
            "raw_batch": {"rows": 15, "end_to_end_latency_ms": {"p50": 900.0, "p95": 1500.0, "p99": 1900.0},
                          "parquet": {"file_bytes": 2048, "compressed_bytes": 1000, "uncompressed_bytes": 3000}},
            "window_batch": {"windows": 1, "window_emit_delay_ms": {"p95": 150000.0}},
            "page_history": {"active_pages": 1, "evicted_pages_total": 0, "observations": 1},
            "errors_by_stage": {},
        },
    })
    return now


def all_query_frames(database, filters: Filters) -> dict:
    fetch = database.fetch_all
    return {
        "latest_component_metrics": queries.latest_component_metrics(fetch),
        "metrics_history": queries.metrics_history(fetch, filters.minutes),
        "edits_per_minute": queries.edits_per_minute(fetch, filters),
        "most_active_pages": queries.most_active_pages(fetch, filters),
        "latest_page_velocity": queries.latest_page_velocity(fetch, filters),
        "recent_anomalies": queries.recent_anomalies(fetch, filters),
        "anomaly_frequency": queries.anomaly_frequency(fetch, filters),
        "recent_edit_war_signals": queries.recent_edit_war_signals(fetch, filters),
        "edit_war_trend": queries.edit_war_trend(fetch, filters),
        "revert_evidence": queries.revert_evidence(fetch, "enwiki", "Page", filters.minutes),
        "hourly_edits": queries.hourly_edits(fetch, filters),
        "hourly_edits_for_pages": queries.hourly_edits_for_pages(fetch, filters, ["Page"]),
    }


def test_every_query_runs_on_an_empty_database(database):
    database.init_schema()

    frames = all_query_frames(database, Filters(minutes=60, wiki="enwiki", page_search="Pa", editor_type="human"))

    assert all(frame.empty for frame in frames.values())


def test_every_query_returns_rows_on_a_populated_database(database):
    seed(database)

    frames = all_query_frames(database, Filters(minutes=60))

    assert [name for name, frame in frames.items() if frame.empty] == []


def test_each_edit_is_counted_once(database):
    seed(database)

    per_minute = queries.edits_per_minute(database.fetch_all, Filters(minutes=60))

    assert per_minute["human_edits"].sum() == 11
    assert per_minute["bot_edits"].sum() == 4


def test_filters_narrow_the_live_activity_together(database):
    seed(database)

    pages = queries.most_active_pages(database.fetch_all, Filters(minutes=60, page_search="other", editor_type="bot"))

    assert pages[["page_title", "edits"]].to_dict("records") == [{"page_title": "Other Page", "edits": 4}]


def test_revert_evidence_lists_only_reverts(database):
    seed(database)

    evidence = queries.revert_evidence(database.fetch_all, "enwiki", "Page", 60)

    assert evidence["user"].tolist() == ["Bob"]


@pytest.fixture
def dashboard_app(postgres_config, monkeypatch):
    import streamlit as st
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("POSTGRES_DB", postgres_config.database)
    get_settings(force_reload=True)
    # Streamlit caches live for the whole process; start every run cold.
    st.cache_data.clear()
    st.cache_resource.clear()
    app = AppTest.from_file(APP_PATH, default_timeout=60)
    yield app
    get_settings(force_reload=True)


def test_dashboard_renders_without_errors_when_the_database_is_empty(database, dashboard_app):
    database.init_schema()

    dashboard_app.run()

    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert any("No edits match" in info.value for info in dashboard_app.info)


def test_dashboard_renders_without_errors_with_data(database, dashboard_app):
    seed(database)

    dashboard_app.run()

    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert not dashboard_app.warning
    assert "1,500 ms" in [metric.value for metric in dashboard_app.metric]


def test_dashboard_explains_an_unreachable_database(dashboard_app, monkeypatch):
    import streamlit as st

    monkeypatch.setenv("POSTGRES_PORT", "1")
    get_settings(force_reload=True)
    st.cache_resource.clear()

    dashboard_app.run()

    assert "Cannot reach PostgreSQL" in dashboard_app.error[0].value

