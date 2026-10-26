"""Tests for the Spark-independent parts of processing.streaming_job:
evaluation of finalized windows and the page-history TTL check."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common.config import AnomalyConfig, LiftWingConfig, Settings, SparkConfig
from processing.anomaly_detector import PageHistoryStore
from processing.streaming_job import (
    PipelineMetrics,
    check_history_ttl,
    evaluate_windows,
    event_latencies,
    kafka_offsets_behind,
    to_edit_event_row,
)

WINDOW_START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_settings() -> Settings:
    anomaly = AnomalyConfig(
        mad_threshold=3.5,
        min_history_windows=8,
        fallback_edit_count_threshold=15,
        history_lookback_windows=48,
        mad_epsilon=1e-6,
    )
    return Settings(anomaly=anomaly, liftwing=LiftWingConfig(enabled=False))


def edit(user, revision_id, is_revert=False, target=None):
    return {
        "user": user,
        "revision_id": revision_id,
        "is_bot": False,
        "is_revert": is_revert,
        "revert_target_revision_id": target,
        "byte_length_delta": -10 if is_revert else 10,
        "timestamp": WINDOW_START.timestamp(),
    }


def window_row(page_title, minutes_after_start, records):
    start = WINDOW_START + timedelta(minutes=minutes_after_start)
    return {
        "window": {"start": start, "end": start + timedelta(minutes=5)},
        "wiki": "enwiki",
        "page_title": page_title,
        "page_id": 1,
        "records": records,
    }


def test_windows_of_one_page_are_recorded_in_window_order():
    settings = make_settings()
    store = PageHistoryStore(settings.anomaly)
    rows = [
        window_row("Page", 2, [edit("A", 1), edit("A", 2), edit("A", 3)]),
        window_row("Page", 0, [edit("A", 1)]),
        window_row("Page", 1, [edit("A", 1), edit("A", 2)]),
    ]

    evaluate_windows(rows, settings, store, window_minutes=5)

    assert store.get_history("enwiki", "Page") == [1.0, 2.0, 3.0]


def test_each_finalized_window_produces_one_activity_row():
    settings = make_settings()
    rows = [
        window_row("Page A", 0, [edit("A", 1)]),
        window_row("Page B", 0, [edit("B", 2), edit("C", 3)]),
    ]

    evaluation = evaluate_windows(rows, settings, PageHistoryStore(settings.anomaly), window_minutes=5)

    counts = {row["page_title"]: row["edit_count"] for row in evaluation.activity_rows}
    assert counts == {"Page A": 1, "Page B": 2}


def test_reciprocal_reverts_in_a_window_produce_a_flagged_edit_war_row():
    settings = make_settings()
    rows = [window_row("Contested", 0, [
        edit("Alice", 100),
        edit("Bob", 101, is_revert=True, target=100),
        edit("Alice", 102, is_revert=True, target=101),
    ])]

    evaluation = evaluate_windows(rows, settings, PageHistoryStore(settings.anomaly), window_minutes=5)

    assert len(evaluation.edit_war_rows) == 1
    assert evaluation.edit_war_rows[0]["flag"] is True


def test_history_ttl_shorter_than_window_plus_watermark_is_rejected():
    settings = Settings(
        anomaly=AnomalyConfig(history_ttl_seconds=300),
        spark=SparkConfig(window_duration="5 minutes", watermark_delay="2 minutes"),
    )

    with pytest.raises(ValueError, match="ANOMALY_HISTORY_TTL_SECONDS"):
        check_history_ttl(settings)


def test_history_ttl_covering_window_plus_watermark_is_accepted():
    settings = Settings(
        anomaly=AnomalyConfig(history_ttl_seconds=420),
        spark=SparkConfig(window_duration="5 minutes", watermark_delay="2 minutes"),
    )

    check_history_ttl(settings)


def test_window_times_are_stored_as_utc_aware_datetimes():
    settings = make_settings()
    rows = [window_row("Page", 0, [edit("A", 1)])]

    evaluation = evaluate_windows(rows, settings, PageHistoryStore(settings.anomaly), window_minutes=5)

    assert evaluation.activity_rows[0]["window_start"] == WINDOW_START
    assert evaluation.activity_rows[0]["window_start"].tzinfo == timezone.utc


class TestEventLatencies:
    def test_latencies_split_at_ingestion_and_persistence(self):
        rows = [{"timestamp": 100.0, "ingestion_timestamp": 101.5}]

        latencies = event_latencies(rows, processed_at=110.0)

        assert latencies == {"ingestion": [1.5], "processing": [8.5], "end_to_end": [10.0]}

    def test_event_without_ingestion_time_only_has_end_to_end_latency(self):
        rows = [{"timestamp": 100.0, "ingestion_timestamp": None}]

        latencies = event_latencies(rows, processed_at=110.0)

        assert latencies == {"ingestion": [], "processing": [], "end_to_end": [10.0]}


class TestKafkaOffsetsBehind:
    def test_reads_offsets_behind_from_the_kafka_source_progress(self):
        progress = {"sources": [{"metrics": {
            "maxOffsetsBehindLatest": "42", "avgOffsetsBehindLatest": "14.0", "minOffsetsBehindLatest": "0",
        }}]}

        assert kafka_offsets_behind(progress) == {"max": 42.0, "avg": 14.0}

    def test_unknown_before_the_first_batch(self):
        assert kafka_offsets_behind(None) is None

    def test_unknown_when_the_source_reports_no_offset_metrics(self):
        assert kafka_offsets_behind({"sources": [{"metrics": {}}]}) is None


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class TestPipelineMetrics:
    def test_throughput_covers_events_since_the_previous_snapshot(self):
        clock = FakeClock()
        metrics = PipelineMetrics(clock=clock)
        metrics.add_raw_batch(received=100, parsed=100, rejected=0, last_event_time=None)
        clock.now = 10.0
        metrics.snapshot_and_reset_throughput()

        metrics.add_raw_batch(received=50, parsed=40, rejected=10, last_event_time=None)
        clock.now = 30.0
        snapshot = metrics.snapshot_and_reset_throughput()

        assert snapshot["throughput_per_sec"] == 2.0
        assert snapshot["events_received"] == 150
        assert snapshot["events_rejected"] == 10

    def test_errors_are_counted_per_stage_and_in_total(self):
        metrics = PipelineMetrics(clock=FakeClock())
        metrics.record_error("postgres_raw_events")
        metrics.record_error("postgres_raw_events")
        metrics.record_error("parquet_raw_events")

        snapshot = metrics.snapshot_and_reset_throughput()

        assert snapshot["error_count"] == 3
        assert snapshot["details"]["errors_by_stage"] == {"postgres_raw_events": 2, "parquet_raw_events": 1}

    def test_last_event_time_is_utc_aware(self):
        metrics = PipelineMetrics(clock=FakeClock())
        metrics.add_raw_batch(received=1, parsed=1, rejected=0, last_event_time=WINDOW_START.timestamp())

        assert metrics.snapshot_and_reset_throughput()["last_event_time"] == WINDOW_START

    def test_processing_latency_column_is_the_latest_batch_median(self):
        metrics = PipelineMetrics(clock=FakeClock())
        metrics.set_details("raw_batch", {"processing_latency_ms": {"p50": 1200.0}})

        assert metrics.snapshot_and_reset_throughput()["processing_latency_ms"] == 1200.0

    def test_processing_latency_is_unknown_before_any_batch(self):
        metrics = PipelineMetrics(clock=FakeClock())

        assert metrics.snapshot_and_reset_throughput()["processing_latency_ms"] is None


def test_edit_event_row_keeps_kafka_lineage_and_utc_times():
    row = {
        "event_id": "e1", "timestamp": WINDOW_START.timestamp(), "wiki": "enwiki", "event_type": "edit",
        "page_title": "Page", "ingestion_timestamp": WINDOW_START.timestamp() + 2,
        "kafka_partition": 2, "kafka_offset": 1234,
    }

    event_row = to_edit_event_row(row)

    assert event_row["event_time"] == WINDOW_START
    assert event_row["ingestion_time"] == WINDOW_START + timedelta(seconds=2)
    assert (event_row["kafka_partition"], event_row["kafka_offset"]) == (2, 1234)
    assert event_row["is_bot"] is False
