"""Stateful stream consumer worker with an in-memory sliding window engine.

Implements the Claim 6 stream computation architecture:
- Custom Python stateful stream consumer worker cluster component
- In-Memory Sliding Window Engine: 300-second rolling deque with 30-second sliding evaluation window
- Dynamic baseline evaluation: Median + Median Absolute Deviation (MAD) modified z-score
- Conflict Graph Tracker: Reciprocal revert tracking over directed editor pairs (Editor A <-> Editor B)
- Provenance & Lineage Injection: Updates provenance_id with Kafka partition, offset, and worker node
- Deterministic latency waterfall auditing: T_event, T_ingest, T_proc
- Dual-sink persistence to PostgreSQL and Parquet Historical Lake
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import signal
import sys
import time
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from common.config import AnomalyConfig, EditWarConfig, Settings, get_settings
from common.logging_setup import configure_logging
from common.models import WikipediaEvent
from processing.aggregations import PageWindowEvent, aggregate_page_window
from processing.anomaly_detector import PageHistoryStore, compute_anomaly
from processing.edit_war_detector import EditRecord, detect_edit_war
from storage.database import Database
from storage.parquet_writer import ParquetWriter

logger = logging.getLogger(__name__)

SECONDS_PER_MINUTE = 60.0


@dataclass
class WindowEvaluationResult:
    """Outputs from one sliding-window evaluation pass."""

    activity_rows: list[dict[str, Any]] = field(default_factory=list)
    anomaly_rows: list[dict[str, Any]] = field(default_factory=list)
    edit_war_rows: list[dict[str, Any]] = field(default_factory=list)
    windows_evaluated: int = 0
    active_pages: int = 0


class ConflictGraphTracker:
    """Tracks directed revert relationships between editors within a window.

    Maintains directed edges: reverter -> reverted_editor. Identifies reciprocal
    reverts (Editor A <-> Editor B) to detect genuine editorial conflict vs.
    routine vandalism cleanup.
    """

    def __init__(self, edit_war_config: EditWarConfig) -> None:
        self.config = edit_war_config

    def evaluate_window_conflict(
        self,
        records: list[EditRecord],
    ) -> tuple[int, Optional[str], Optional[str], float, bool]:
        """Analyze edit records for reciprocal conflict.

        Returns:
            (mutual_revert_count, editor_a, editor_b, conflict_score, flag)
        """
        result = detect_edit_war(records, self.config)
        return (
            result.mutual_revert_count,
            result.editor_a,
            result.editor_b,
            result.conflict_score,
            result.flag,
        )


class InMemorySlidingWindowEngine:
    """In-memory sliding window engine.

    Maintains a 300-second rolling deque of events per (wiki, page_title)
    evaluated every 30 seconds (configurable). Drops late arrivals
    beyond the watermark delay threshold.
    """

    def __init__(
        self,
        window_duration_seconds: float = 300.0,
        slide_interval_seconds: float = 30.0,
        watermark_delay_seconds: float = 120.0,
    ) -> None:
        self.window_duration_seconds = float(window_duration_seconds)
        self.slide_interval_seconds = float(slide_interval_seconds)
        self.watermark_delay_seconds = float(watermark_delay_seconds)

        # (wiki, page_title) -> deque of WikipediaEvent
        self._deques: dict[tuple[str, str], deque[WikipediaEvent]] = defaultdict(deque)
        self.max_event_time: float = 0.0
        self.watermark: float = 0.0
        self.late_events_dropped: int = 0
        self.total_events_added: int = 0

    @property
    def active_pages(self) -> int:
        return len(self._deques)

    def add_event(self, event: WikipediaEvent) -> bool:
        """Add an event to its page's rolling deque.

        Returns False if dropped as late beyond the watermark.
        """
        ts = event.timestamp
        if self.max_event_time > 0 and ts < self.watermark:
            self.late_events_dropped += 1
            return False

        if ts > self.max_event_time:
            self.max_event_time = ts
            self.watermark = max(0.0, ts - self.watermark_delay_seconds)

        key = (event.wiki, event.page_title)
        self._deques[key].append(event)
        self.total_events_added += 1
        return True

    def evict_expired(self, current_time: float) -> int:
        """Evict events older than (current_time - window_duration_seconds) from deques."""
        cutoff = current_time - self.window_duration_seconds
        evicted = 0
        empty_keys = []

        for key, q in self._deques.items():
            while q and q[0].timestamp < cutoff:
                q.popleft()
                evicted += 1
            if not q:
                empty_keys.append(key)

        for key in empty_keys:
            del self._deques[key]

        return evicted

    def evaluate_windows(
        self,
        evaluation_time: float,
        history_store: PageHistoryStore,
        anomaly_config: AnomalyConfig,
        edit_war_config: EditWarConfig,
    ) -> WindowEvaluationResult:
        """Evaluate all active page deques over the window [evaluation_time - window_duration, evaluation_time)."""
        self.evict_expired(evaluation_time)

        window_start_ts = evaluation_time - self.window_duration_seconds
        window_start_dt = datetime.fromtimestamp(window_start_ts, tz=timezone.utc)
        window_end_dt = datetime.fromtimestamp(evaluation_time, tz=timezone.utc)
        window_minutes = self.window_duration_seconds / SECONDS_PER_MINUTE

        activity_rows: list[dict[str, Any]] = []
        anomaly_rows: list[dict[str, Any]] = []
        edit_war_rows: list[dict[str, Any]] = []
        conflict_tracker = ConflictGraphTracker(edit_war_config)

        for (wiki, page_title), events_q in list(self._deques.items()):
            events = list(events_q)
            if not events:
                continue

            page_window_events = [
                PageWindowEvent(
                    user=e.user,
                    is_bot=e.is_bot,
                    is_revert=e.is_revert,
                    byte_length_delta=e.byte_length_delta,
                )
                for e in events
            ]
            metrics = aggregate_page_window(page_window_events, window_minutes)

            # 1. Activity window record
            activity_row = {
                "wiki": wiki,
                "page_title": page_title,
                "window_start": window_start_dt,
                "window_end": window_end_dt,
                "edit_count": metrics.edit_count,
                "unique_editors": metrics.unique_editors,
                "bot_edits": metrics.bot_edits,
                "revert_count": metrics.revert_count,
                "net_byte_delta": metrics.net_byte_delta,
                "edit_velocity": metrics.edit_velocity,
            }
            activity_rows.append(activity_row)

            # 2. Dynamic Baseline Evaluation (Median + MAD Modified Z-Score)
            history = history_store.get_history(wiki, page_title)
            anomaly_res = compute_anomaly(metrics.edit_count, history, anomaly_config)
            history_store.record(wiki, page_title, metrics.edit_count)

            if anomaly_res.is_anomaly:
                anomaly_row = {
                    "wiki": wiki,
                    "page_title": page_title,
                    "window_start": window_start_dt,
                    "window_end": window_end_dt,
                    "anomaly_type": anomaly_res.anomaly_type,
                    "anomaly_score": anomaly_res.anomaly_score,
                    "edit_count": metrics.edit_count,
                    "baseline_median": anomaly_res.baseline_median,
                    "baseline_mad": anomaly_res.baseline_mad,
                    "provenance_id": events[-1].provenance_id,
                }
                anomaly_rows.append(anomaly_row)

            # 3. Conflict Graph Tracker (reciprocal revert tracking over directed editor pairs)
            edit_records = [
                EditRecord(
                    user=e.user,
                    revision_id=e.revision_id,
                    is_revert=e.is_revert,
                    revert_target_revision_id=e.revert_target_revision_id,
                    byte_length_delta=e.byte_length_delta,
                    timestamp=e.timestamp,
                )
                for e in events
            ]
            mutual_reverts, ed_a, ed_b, conflict_score, flag = conflict_tracker.evaluate_window_conflict(edit_records)
            if mutual_reverts > 0 or flag:
                edit_war_row = {
                    "wiki": wiki,
                    "page_title": page_title,
                    "window_start": window_start_dt,
                    "window_end": window_end_dt,
                    "mutual_revert_count": mutual_reverts,
                    "editor_a": ed_a,
                    "editor_b": ed_b,
                    "total_reverts": metrics.revert_count,
                    "conflict_score": conflict_score,
                    "flag": flag,
                    "provenance_id": events[-1].provenance_id,
                }
                edit_war_rows.append(edit_war_row)

        return WindowEvaluationResult(
            activity_rows=activity_rows,
            anomaly_rows=anomaly_rows,
            edit_war_rows=edit_war_rows,
            windows_evaluated=len(activity_rows),
            active_pages=len(self._deques),
        )


def enrich_lineage_and_latency(
    event: WikipediaEvent,
    partition: int = 0,
    offset: int = 0,
    host_node: str = "stream-worker-01",
    processing_time: Optional[datetime] = None,
) -> WikipediaEvent:
    """Enrich an event with lineage provenance tuple and processing timestamp.

    Guarantees deterministic latency waterfall auditing:
    T_event, T_ingest, T_proc
    """
    proc_dt = processing_time or datetime.now(timezone.utc)
    ingest_epoch = int(event.ingestion_timestamp or event.timestamp)
    provenance = f"{host_node}:{ingest_epoch}:part-{partition}:off-{offset}"

    return replace(
        event,
        processing_timestamp=proc_dt,
        provenance_id=provenance,
    )


def event_to_db_dict(event: WikipediaEvent) -> dict[str, Any]:
    """Convert WikipediaEvent into a database-compatible row dict."""
    return {
        "event_id": event.event_id,
        "event_time": datetime.fromtimestamp(event.timestamp, tz=timezone.utc),
        "wiki": event.wiki,
        "page_title": event.page_title,
        "user": event.user,
        "user_type": event.user_type.value if hasattr(event.user_type, "value") else str(event.user_type),
        "user_id": None,
        "is_bot": event.is_bot,
        "is_minor": event.is_minor,
        "is_revert": event.is_revert,
        "is_large_jump": event.is_large_jump,
        "revision_id": event.revision_id,
        "byte_length_delta": event.byte_length_delta,
        "comment": event.comment,
        "comment_length": event.comment_length,
        "ingestion_time": (
            datetime.fromtimestamp(event.ingestion_timestamp, tz=timezone.utc)
            if event.ingestion_timestamp is not None
            else None
        ),
        "processing_time": event.processing_timestamp,
        "provenance_id": event.provenance_id,
    }


class StatefulStreamConsumer:
    """Stateful stream consumer worker coordinating ingestion, windowing, and dual-sink persistence."""

    def __init__(
        self,
        settings: Settings,
        db: Optional[Database] = None,
        parquet_writer: Optional[ParquetWriter] = None,
    ) -> None:
        self.settings = settings
        self.db = db or Database(settings.postgres)
        self.parquet_writer = parquet_writer or ParquetWriter(settings.parquet)

        cfg = settings.stream_consumer
        self.engine = InMemorySlidingWindowEngine(
            window_duration_seconds=cfg.window_duration_seconds,
            slide_interval_seconds=cfg.slide_interval_seconds,
            watermark_delay_seconds=cfg.watermark_delay_seconds,
        )
        self.history_store = PageHistoryStore(settings.anomaly)
        self.host_node = cfg.host_node

        self.last_slide_time: float = 0.0
        self.events_processed = 0
        self.total_anomalies = 0
        self.total_edit_wars = 0

    def process_event(
        self,
        event: WikipediaEvent,
        partition: int = 0,
        offset: int = 0,
    ) -> Optional[WikipediaEvent]:
        """Process a single incoming event: lineage injection, rolling deque buffering, and slide check."""
        enriched = enrich_lineage_and_latency(
            event,
            partition=partition,
            offset=offset,
            host_node=self.host_node,
        )
        accepted = self.engine.add_event(enriched)
        if not accepted:
            return None

        self.events_processed += 1
        if self.last_slide_time == 0.0:
            self.last_slide_time = enriched.timestamp

        return enriched

    def evaluate_and_persist(self, evaluation_time: float, persist: bool = True) -> WindowEvaluationResult:
        """Trigger sliding window evaluation and dual-sink persistence to PostgreSQL and Parquet Lake."""
        result = self.engine.evaluate_windows(
            evaluation_time=evaluation_time,
            history_store=self.history_store,
            anomaly_config=self.settings.anomaly,
            edit_war_config=self.settings.edit_war,
        )
        self.total_anomalies += len(result.anomaly_rows)
        self.total_edit_wars += len(result.edit_war_rows)
        self.last_slide_time = evaluation_time

        if persist:
            if result.activity_rows:
                self.db.upsert_activity_windows(result.activity_rows)
            if result.anomaly_rows:
                self.db.insert_anomaly_events(result.anomaly_rows)
            if result.edit_war_rows:
                self.db.insert_edit_war_signals(result.edit_war_rows)

        return result

    def persist_raw_events(self, events: list[WikipediaEvent], persist: bool = True) -> None:
        """Persist raw events to Hot PostgreSQL and Cold Parquet Lake."""
        if not events or not persist:
            return

        db_rows = [event_to_db_dict(e) for e in events]
        # Dual Sink Persistence:
        # 1. Hot Storage (PostgreSQL 50-row micro-batch)
        self.db.upsert_edit_events(db_rows)
        # 2. Cold Storage (Hierarchical Parquet Lake)
        self.parquet_writer.write_lake_records(db_rows)

    def run_batch(
        self,
        events: list[WikipediaEvent],
        persist: bool = True,
    ) -> WindowEvaluationResult:
        """Convenience method for batch evaluation (e.g. testing or benchmark pipelines)."""
        enriched_events: list[WikipediaEvent] = []
        last_eval_result = WindowEvaluationResult()

        for idx, event in enumerate(events):
            enriched = self.process_event(event, partition=0, offset=idx)
            if enriched:
                enriched_events.append(enriched)

            # Check if slide interval boundary reached
            if enriched and (enriched.timestamp - self.last_slide_time >= self.engine.slide_interval_seconds):
                eval_res = self.evaluate_and_persist(enriched.timestamp, persist=persist)
                last_eval_result = eval_res

        self.persist_raw_events(enriched_events, persist=persist)
        if enriched_events and enriched_events[-1].timestamp > self.last_slide_time:
            last_eval_result = self.evaluate_and_persist(enriched_events[-1].timestamp, persist=persist)

        return last_eval_result

    def run_consumer_loop(self) -> None:
        """Run continuous Kafka consumer worker loop."""
        import confluent_kafka

        kafka_cfg = self.settings.kafka
        topic = kafka_cfg.topic_canonical if hasattr(kafka_cfg, "topic_canonical") else kafka_cfg.topic_edits

        conf = {
            "bootstrap.servers": kafka_cfg.bootstrap_servers,
            "group.id": f"{kafka_cfg.consumer_group}-stateful",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
        consumer = confluent_kafka.Consumer(conf)
        consumer.subscribe([topic])
        logger.info("Started StatefulStreamConsumer on topic %s (bootstrap: %s)", topic, kafka_cfg.bootstrap_servers)

        stop_requested = False

        def _signal_handler(_sig: int, _frame: Any) -> None:
            nonlocal stop_requested
            stop_requested = True
            logger.info("Signal received, shutting down stateful consumer...")

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        pending_events: list[WikipediaEvent] = []

        try:
            while not stop_requested:
                msg = consumer.poll(timeout=1.0)
                now = time.time()

                if msg is not None:
                    if msg.error():
                        if msg.error().code() != confluent_kafka.KafkaError._PARTITION_EOF:
                            logger.error("Kafka consumer error: %s", msg.error())
                        continue

                    try:
                        data = json.loads(msg.value().decode("utf-8"))
                        event = WikipediaEvent.from_dict(data)
                        enriched = self.process_event(
                            event,
                            partition=msg.partition(),
                            offset=msg.offset(),
                        )
                        if enriched:
                            pending_events.append(enriched)
                    except Exception as ex:
                        logger.warning("Failed to decode message: %s", ex)

                if pending_events and (len(pending_events) >= 50 or (now - self.last_slide_time >= self.engine.slide_interval_seconds)):
                    self.persist_raw_events(pending_events)
                    pending_events.clear()

                if self.engine.max_event_time > 0 and (self.engine.max_event_time - self.last_slide_time >= self.engine.slide_interval_seconds):
                    self.evaluate_and_persist(self.engine.max_event_time)

        finally:
            if pending_events:
                self.persist_raw_events(pending_events)
            consumer.close()
            logger.info("StatefulStreamConsumer closed.")


def main() -> None:
    """CLI entry point for stateful stream consumer worker."""
    configure_logging()
    settings = get_settings()

    parser = argparse.ArgumentParser(description="WikiPulse Stateful Stream Consumer Worker")
    parser.add_argument("--window-seconds", type=float, default=settings.stream_consumer.window_duration_seconds)
    parser.add_argument("--slide-seconds", type=float, default=settings.stream_consumer.slide_interval_seconds)
    parser.add_argument("--watermark-seconds", type=float, default=settings.stream_consumer.watermark_delay_seconds)
    parser.add_argument("--host-node", type=str, default=settings.stream_consumer.host_node)

    args = parser.parse_args()
    logger.info("Starting StatefulStreamConsumer CLI with args: %s", args)

    worker = StatefulStreamConsumer(settings)
    worker.run_consumer_loop()


if __name__ == "__main__":
    main()
