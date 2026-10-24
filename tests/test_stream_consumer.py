"""Unit tests for processing.stream_consumer (Claim 6 stream computation architecture)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from common.config import AnomalyConfig, EditWarConfig, Settings, StreamConsumerConfig, get_settings
from common.models import UserType, WikipediaEvent
from processing.anomaly_detector import PageHistoryStore
from processing.edit_war_detector import EditRecord
from processing.stream_consumer import (
    ConflictGraphTracker,
    InMemorySlidingWindowEngine,
    StatefulStreamConsumer,
    enrich_lineage_and_latency,
    event_to_db_dict,
)


def _make_event(
    title: str = "TestPage",
    timestamp: float = 1_000_000.0,
    user: str = "User1",
    is_revert: bool = False,
    is_bot: bool = False,
    revision_id: int = 100,
    revert_target_revision_id: int | None = None,
    delta: int = 50,
) -> WikipediaEvent:
    return WikipediaEvent(
        event_id=f"evt-{title}-{timestamp}-{user}",
        timestamp=timestamp,
        wiki="enwiki",
        event_type="edit",
        page_title=title,
        user=user,
        user_type=UserType.REGISTERED,
        byte_length_delta=delta,
        revision_id=revision_id,
        is_bot=is_bot,
        is_revert=is_revert,
        revert_target_revision_id=revert_target_revision_id,
        ingestion_timestamp=timestamp + 0.1,
    )


class TestInMemorySlidingWindowEngine:
    def test_window_engine_buffers_in_300s_rolling_deque(self):
        engine = InMemorySlidingWindowEngine(
            window_duration_seconds=300.0,
            slide_interval_seconds=30.0,
            watermark_delay_seconds=120.0,
        )
        e1 = _make_event("PageA", 1000.0)
        e2 = _make_event("PageA", 1020.0)
        e3 = _make_event("PageB", 1025.0)

        assert engine.add_event(e1) is True
        assert engine.add_event(e2) is True
        assert engine.add_event(e3) is True

        assert engine.active_pages == 2
        assert len(engine._deques[("enwiki", "PageA")]) == 2
        assert len(engine._deques[("enwiki", "PageB")]) == 1

    def test_window_engine_drops_late_events_beyond_watermark(self):
        engine = InMemorySlidingWindowEngine(
            window_duration_seconds=300.0,
            slide_interval_seconds=30.0,
            watermark_delay_seconds=120.0,
        )
        # Event at t=2000 => watermark = 2000 - 120 = 1880
        assert engine.add_event(_make_event("PageA", 2000.0)) is True
        assert engine.watermark == 1880.0

        # Event at t=1800 is before watermark 1880 => dropped
        assert engine.add_event(_make_event("PageA", 1800.0)) is False
        assert engine.late_events_dropped == 1

        # Event at t=1900 is after watermark => accepted
        assert engine.add_event(_make_event("PageA", 1900.0)) is True

    def test_window_engine_evicts_expired_events(self):
        engine = InMemorySlidingWindowEngine(
            window_duration_seconds=300.0,
            slide_interval_seconds=30.0,
            watermark_delay_seconds=120.0,
        )
        engine.add_event(_make_event("PageA", 1000.0))
        engine.add_event(_make_event("PageA", 1150.0))
        engine.add_event(_make_event("PageA", 1350.0))

        # At t=1350, window is [1050, 1350). Event at 1000 is expired.
        evicted = engine.evict_expired(1350.0)
        assert evicted == 1
        assert len(engine._deques[("enwiki", "PageA")]) == 2
        assert engine._deques[("enwiki", "PageA")][0].timestamp == 1150.0

    def test_window_engine_evaluates_activity_and_mad_baseline(self):
        engine = InMemorySlidingWindowEngine(
            window_duration_seconds=300.0,
            slide_interval_seconds=30.0,
        )
        for i in range(10):
            engine.add_event(_make_event("HotPage", 1000.0 + i * 2, user=f"Editor{i}"))

        history_store = PageHistoryStore(AnomalyConfig(min_history_windows=3, mad_threshold=3.5))
        # Seed history
        history_store.record("enwiki", "HotPage", 2)
        history_store.record("enwiki", "HotPage", 2)
        history_store.record("enwiki", "HotPage", 2)

        res = engine.evaluate_windows(
            evaluation_time=1030.0,
            history_store=history_store,
            anomaly_config=AnomalyConfig(min_history_windows=3, mad_threshold=3.5),
            edit_war_config=EditWarConfig(),
        )

        assert res.windows_evaluated == 1
        assert len(res.activity_rows) == 1
        act = res.activity_rows[0]
        assert act["page_title"] == "HotPage"
        assert act["edit_count"] == 10
        assert act["unique_editors"] == 10

        # Check anomaly triggered due to jump from 2 to 10
        assert len(res.anomaly_rows) == 1
        anom = res.anomaly_rows[0]
        assert anom["page_title"] == "HotPage"
        assert anom["anomaly_type"] == "median_mad"


class TestConflictGraphTracker:
    def test_reciprocal_reverts_detected_as_edit_war(self):
        tracker = ConflictGraphTracker(EditWarConfig(mutual_revert_threshold=2))

        # Alice creates rev 101, Bob reverts rev 101 in rev 102.
        # Bob creates rev 102, Alice reverts rev 102 in rev 103.
        records = [
            EditRecord(user="Alice", revision_id=101, is_revert=False, revert_target_revision_id=None, byte_length_delta=10, timestamp=100.0),
            EditRecord(user="Bob", revision_id=102, is_revert=True, revert_target_revision_id=101, byte_length_delta=-10, timestamp=110.0),
            EditRecord(user="Alice", revision_id=103, is_revert=True, revert_target_revision_id=102, byte_length_delta=10, timestamp=120.0),
        ]

        mutual_reverts, ed_a, ed_b, score, flag = tracker.evaluate_window_conflict(records)
        assert mutual_reverts == 2
        assert {ed_a, ed_b} == {"Alice", "Bob"}
        assert score > 0.0
        assert flag is True

    def test_unilateral_vandalism_reverts_not_flagged_as_war(self):
        tracker = ConflictGraphTracker(EditWarConfig(mutual_revert_threshold=2))

        # Mod reverts multiple different vandals
        records = [
            EditRecord(user="Vandal1", revision_id=101, is_revert=False, revert_target_revision_id=None, byte_length_delta=50, timestamp=100.0),
            EditRecord(user="Mod", revision_id=102, is_revert=True, revert_target_revision_id=101, byte_length_delta=-50, timestamp=110.0),
            EditRecord(user="Vandal2", revision_id=103, is_revert=False, revert_target_revision_id=None, byte_length_delta=40, timestamp=120.0),
            EditRecord(user="Mod", revision_id=104, is_revert=True, revert_target_revision_id=103, byte_length_delta=-40, timestamp=130.0),
        ]

        mutual_reverts, ed_a, ed_b, score, flag = tracker.evaluate_window_conflict(records)
        assert mutual_reverts == 0
        assert flag is False


class TestLineageAndLatencyWaterfall:
    def test_provenance_id_and_waterfall_latencies(self):
        event = _make_event("PageX", 1_700_000_000.0)
        proc_time = datetime(2026, 11, 12, 12, 0, 1, tzinfo=timezone.utc)

        enriched = enrich_lineage_and_latency(
            event,
            partition=2,
            offset=45120,
            host_node="stream-node-03",
            processing_time=proc_time,
        )

        assert enriched.provenance_id == "stream-node-03:1700000000:part-2:off-45120"
        assert enriched.processing_timestamp == proc_time

        # Latencies
        assert enriched.ingestion_latency_ms == pytest.approx(100.0, abs=1e-3)
        assert enriched.compute_latency_ms is not None
        assert enriched.end_to_end_latency_ms is not None

        row = event_to_db_dict(enriched)
        assert row["provenance_id"] == "stream-node-03:1700000000:part-2:off-45120"
        assert row["processing_time"] == proc_time


class TestStatefulStreamConsumer:
    def test_consumer_dual_sink_persistence(self):
        settings = get_settings()
        mock_db = MagicMock()
        mock_parquet = MagicMock()

        consumer = StatefulStreamConsumer(settings, db=mock_db, parquet_writer=mock_parquet)

        events = [
            _make_event("PageZ", 1000.0 + i * 5, user=f"User{i}")
            for i in range(10)
        ]

        result = consumer.run_batch(events, persist=True)

        assert mock_db.upsert_edit_events.called
        assert mock_parquet.write_lake_records.called
        assert mock_db.upsert_activity_windows.called
        assert result.windows_evaluated == 1
