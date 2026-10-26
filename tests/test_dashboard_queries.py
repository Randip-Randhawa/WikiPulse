"""Tests for dashboard.queries: filter composition, empty results, metric
formatting and the Parquet history summary. PostgreSQL is replaced by a
fake `fetch`; tests/test_dashboard_integration.py runs the SQL for real."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from common.config import ParquetConfig
from dashboard import queries
from dashboard.queries import Filters, component_status, detail, format_value, where_clause
from storage.parquet_writer import ParquetWriter


class FakeFetch:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []

    def __call__(self, query, params):
        self.calls.append((query, list(params)))
        return self.rows


class TestWhereClause:
    def test_time_range_alone(self):
        where, params = where_clause(Filters(minutes=60), "event_time", has_bot_column=True)

        assert where == "event_time >= now() - make_interval(mins => %s)"
        assert params == [60]

    def test_all_filters_combine_with_and(self):
        filters = Filters(minutes=30, wiki="enwiki", page_search="Python", editor_type="human")

        where, params = where_clause(filters, "event_time", has_bot_column=True)

        assert where.count(" AND ") == 3
        assert "NOT is_bot" in where
        assert params == [30, "enwiki", "%Python%"]

    def test_bot_filter_is_skipped_for_tables_without_a_bot_column(self):
        where, _ = where_clause(Filters(minutes=30, editor_type="bot"), "window_start", has_bot_column=False)

        assert "is_bot" not in where

    def test_like_wildcards_in_the_search_text_are_matched_literally(self):
        _, params = where_clause(Filters(minutes=30, page_search="100%_done"), "event_time", has_bot_column=True)

        assert params[-1] == "%100\\%\\_done%"

    def test_unknown_editor_type_is_rejected(self):
        with pytest.raises(ValueError):
            Filters(minutes=30, editor_type="robots")


class TestEmptyResults:
    @pytest.mark.parametrize("name", [
        "edits_per_minute", "most_active_pages", "latest_page_velocity", "recent_anomalies",
        "anomaly_frequency", "recent_edit_war_signals", "edit_war_trend", "hourly_edits",
    ])
    def test_filtered_queries_return_their_columns_when_empty(self, name):
        frame = getattr(queries, name)(FakeFetch(), Filters(minutes=60))

        assert frame.empty
        assert len(frame.columns) > 0

    def test_page_comparison_without_pages_skips_the_database(self):
        fetch = FakeFetch()

        frame = queries.hourly_edits_for_pages(fetch, Filters(minutes=60), [])

        assert frame.empty
        assert fetch.calls == []


def test_edits_per_minute_is_averaged_over_the_selected_range():
    fetch = FakeFetch([{"wiki": "enwiki", "page_title": "Page", "edits": 30, "editors": 3, "bot_edits": 0,
                        "reverts": 1, "last_edit": datetime(2026, 10, 26, tzinfo=timezone.utc)}])

    frame = queries.most_active_pages(fetch, Filters(minutes=60))

    assert frame.loc[0, "edits_per_minute"] == 0.5


def test_timestamps_are_shown_in_utc():
    ist = datetime(2026, 10, 26, 17, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    fetch = FakeFetch([{"minute": ist, "human_edits": 1, "bot_edits": 0}])

    frame = queries.edits_per_minute(fetch, Filters(minutes=60))

    assert frame.loc[0, "minute"] == pd.Timestamp("2026-10-26 12:00", tz="UTC")


class TestMetricHelpers:
    def test_detail_reads_nested_values(self):
        assert detail({"raw_batch": {"latency": {"p95": 12.0}}}, "raw_batch", "latency", "p95") == 12.0

    def test_detail_is_none_when_a_level_is_missing(self):
        assert detail({"raw_batch": None}, "raw_batch", "latency", "p95") is None
        assert detail(None, "raw_batch") is None

    def test_unknown_metric_is_not_shown_as_zero(self):
        assert format_value(None, " ms") == "unknown"
        assert format_value(float("nan"), " ms") == "unknown"
        assert format_value(0, " ms") == "0 ms"

    def test_component_without_reports_has_no_data(self):
        assert component_status(None, pd.Timestamp.now(tz="UTC")) == "no data"

    def test_component_is_stale_after_the_threshold(self):
        now = pd.Timestamp("2026-10-26 12:00", tz="UTC")

        assert component_status(now - pd.Timedelta(60, unit="s"), now) == "ok"
        assert component_status(now - pd.Timedelta(queries.STALE_AFTER_SECONDS + 1, unit="s"), now) == "stale"


class TestParquetDailyCounts:
    def test_counts_events_per_day_and_wiki_from_file_metadata(self, tmp_path):
        writer = ParquetWriter(ParquetConfig(base_path=str(tmp_path)))
        rows = [
            {"wiki": "enwiki", "event_time": datetime(2026, 10, 25, 9, tzinfo=timezone.utc)},
            {"wiki": "enwiki", "event_time": datetime(2026, 10, 25, 14, tzinfo=timezone.utc)},
            {"wiki": "dewiki", "event_time": datetime(2026, 10, 25, 9, tzinfo=timezone.utc)},
            {"wiki": "enwiki", "event_time": datetime(2026, 10, 26, 1, tzinfo=timezone.utc)},
        ]
        writer.write_records("raw_events", rows, time_field="event_time")

        frame = queries.parquet_daily_event_counts(str(tmp_path), date(2026, 10, 25), date(2026, 10, 26))

        assert frame.to_dict("records") == [
            {"date": "2026-10-25", "wiki": "dewiki", "events": 1},
            {"date": "2026-10-25", "wiki": "enwiki", "events": 2},
            {"date": "2026-10-26", "wiki": "enwiki", "events": 1},
        ]

    def test_days_outside_the_range_are_not_counted(self, tmp_path):
        writer = ParquetWriter(ParquetConfig(base_path=str(tmp_path)))
        writer.write_records("raw_events", [
            {"wiki": "enwiki", "event_time": datetime(2026, 10, 1, tzinfo=timezone.utc)},
        ], time_field="event_time")

        frame = queries.parquet_daily_event_counts(str(tmp_path), date(2026, 10, 25), date(2026, 10, 26))

        assert frame.empty

    def test_missing_history_directory_is_empty_not_an_error(self, tmp_path):
        frame = queries.parquet_daily_event_counts(str(tmp_path / "nothing"), date(2026, 10, 1), date(2026, 10, 2))

        assert frame.empty
