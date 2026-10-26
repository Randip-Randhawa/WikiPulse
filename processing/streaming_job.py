"""Spark Structured Streaming job: the core of the WikiPulse pipeline.

Reads normalized events from Kafka, computes five-minute sliding-window
page activity metrics, and runs anomaly + edit-war detection on each
completed window before persisting results to PostgreSQL and Parquet.

Design notes (see README "Architecture" section for the full rationale):
- Spark handles the heavy lifting of event-time windowing (with a
  watermark for late data) and per-window raw-record collection.
- The windowed query runs in append mode, so each window is emitted exactly
  once, after the watermark has passed its end. Update mode would re-emit a
  window every trigger while it fills, which records partial counts in the
  anomaly history and writes duplicate anomaly / edit-war rows.
- The median/MAD anomaly logic and mutual-revert edit-war logic are plain
  Python (processing/anomaly_detector.py, processing/edit_war_detector.py)
  applied per group inside a `foreachBatch` callback, rather than being
  reimplemented as Spark SQL expressions. For an academic-scale local
  deployment this keeps the statistical/graph logic simple, testable, and
  decoupled from Spark, at the cost of not scaling to a true multi-node
  cluster with sharded state -- an explicit, documented simplification.
- Per-page anomaly history is kept in a single in-memory `PageHistoryStore`
  on the driver (see anomaly_detector.py docstring for why this is
  appropriate here).
- Lift Wing enrichment runs on a bounded sample of revisions per batch so a
  slow/unavailable external API cannot stall the streaming pipeline.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import psutil
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common.config import Settings, SparkConfig, get_settings
from common.logging_setup import configure_logging
from common.stats import MILLISECONDS_PER_SECOND, summarize_ms
from enrichment.liftwing_client import LiftWingClient, RevisionScoreRequest
from processing.aggregations import PageWindowEvent, aggregate_page_window
from processing.anomaly_detector import PageHistoryStore
from processing.edit_war_detector import EditRecord, detect_edit_war
from storage.database import Database
from storage.parquet_writer import ParquetWriter

logger = logging.getLogger(__name__)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("timestamp", DoubleType(), nullable=False),
    StructField("wiki", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("page_title", StringType(), nullable=False),
    StructField("page_id", LongType(), nullable=True),
    StructField("namespace", LongType(), nullable=True),
    StructField("revision_id", LongType(), nullable=True),
    StructField("previous_revision_id", LongType(), nullable=True),
    StructField("user", StringType(), nullable=True),
    StructField("user_id", LongType(), nullable=True),
    StructField("is_bot", BooleanType(), nullable=True),
    StructField("comment", StringType(), nullable=True),
    StructField("byte_length_new", LongType(), nullable=True),
    StructField("byte_length_old", LongType(), nullable=True),
    StructField("byte_length_delta", LongType(), nullable=True),
    StructField("is_revert", BooleanType(), nullable=True),
    StructField("revert_target_revision_id", LongType(), nullable=True),
    StructField("ingestion_timestamp", DoubleType(), nullable=True),
])

# Fields EVENT_SCHEMA marks non-nullable; a Kafka message missing any of
# them failed to parse and cannot be stored.
REQUIRED_EVENT_FIELDS = ("event_id", "timestamp", "wiki", "event_type", "page_title")

_DURATION_UNIT_TO_MINUTES = {
    "second": 1 / 60,
    "seconds": 1 / 60,
    "minute": 1.0,
    "minutes": 1.0,
    "hour": 60.0,
    "hours": 60.0,
}

SECONDS_PER_MINUTE = 60
BYTES_PER_MB = 1024 * 1024
# How often the driver reads Kafka offsets-behind from the query progress.
PROGRESS_POLL_SECONDS = 10


def check_history_ttl(settings: Settings) -> None:
    """Reject a page-history TTL shorter than one window plus the watermark
    delay. A page edited at least once per window gets a finalized window
    well within that span, so this floor keeps such pages from being
    evicted."""
    open_window_seconds = SECONDS_PER_MINUTE * (
        _duration_to_minutes(settings.spark.window_duration)
        + _duration_to_minutes(settings.spark.watermark_delay)
    )
    if settings.anomaly.history_ttl_seconds < open_window_seconds:
        raise ValueError(
            f"ANOMALY_HISTORY_TTL_SECONDS ({settings.anomaly.history_ttl_seconds:g}) must be at least "
            f"the window duration plus the watermark delay ({open_window_seconds:g}s)"
        )


def _duration_to_minutes(duration_text: str) -> float:
    """Parse Spark-style duration strings like '5 minutes' into minutes."""
    match = re.match(r"\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*", duration_text)
    if not match:
        raise ValueError(f"Unrecognized duration format: {duration_text!r}")
    value, unit = match.groups()
    unit = unit.lower()
    if unit not in _DURATION_UNIT_TO_MINUTES:
        raise ValueError(f"Unsupported duration unit: {unit!r}")
    return float(value) * _DURATION_UNIT_TO_MINUTES[unit]


class PipelineMetrics:
    """Thread-safe counters shared across the two concurrent streaming
    queries (raw-event sink and windowed-aggregation sink), written to the
    `pipeline_metrics` Postgres table once per raw-event micro-batch.

    Counters are cumulative since the job started; a micro-batch that Spark
    replays after a crash is counted again. `details` holds the latest
    measurements of each stage (see README "Pipeline metrics").
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._lock = threading.Lock()
        self._clock = clock
        self.events_received = 0
        self.events_parsed = 0
        self.events_rejected = 0
        self.anomalies_detected = 0
        self.edit_wars_detected = 0
        self.error_count = 0
        self.errors_by_stage: dict[str, int] = {}
        self.last_event_time: Optional[float] = None
        self._details: dict[str, Any] = {}
        self._last_flush = clock()
        self._last_flush_count = 0

    def add_raw_batch(self, received: int, parsed: int, rejected: int, last_event_time: Optional[float]) -> None:
        with self._lock:
            self.events_received += received
            self.events_parsed += parsed
            self.events_rejected += rejected
            if last_event_time is not None:
                self.last_event_time = max(self.last_event_time or 0, last_event_time)

    def add_detections(self, anomalies: int, edit_wars: int) -> None:
        with self._lock:
            self.anomalies_detected += anomalies
            self.edit_wars_detected += edit_wars

    def record_error(self, stage: str) -> None:
        with self._lock:
            self.error_count += 1
            self.errors_by_stage[stage] = self.errors_by_stage.get(stage, 0) + 1

    def set_details(self, section: str, values: Any) -> None:
        """Replace the latest measurements reported under `section`."""
        with self._lock:
            self._details[section] = values

    def snapshot_and_reset_throughput(self) -> dict[str, Any]:
        with self._lock:
            now = self._clock()
            elapsed = max(now - self._last_flush, 1e-6)
            throughput = (self.events_parsed - self._last_flush_count) / elapsed
            self._last_flush = now
            self._last_flush_count = self.events_parsed
            raw_batch = self._details.get("raw_batch") or {}
            processing_latency = raw_batch.get("processing_latency_ms") or {}
            return {
                "events_received": self.events_received,
                "events_parsed": self.events_parsed,
                "events_rejected": self.events_rejected,
                "events_published": self.events_parsed,
                "anomalies_detected": self.anomalies_detected,
                "edit_wars_detected": self.edit_wars_detected,
                "processing_latency_ms": processing_latency.get("p50"),
                "throughput_per_sec": round(throughput, 4),
                "last_event_time": (
                    None if self.last_event_time is None
                    else datetime.fromtimestamp(self.last_event_time, tz=timezone.utc)
                ),
                "error_count": self.error_count,
                "details": {**self._details, "errors_by_stage": dict(self.errors_by_stage)},
            }


def event_latencies(rows: list[dict[str, Any]], processed_at: float) -> dict[str, list[float]]:
    """Per-event latencies in seconds for one persisted micro-batch.

    - ingestion: Wikimedia event time -> normalized by the ingestion service.
    - processing: normalized -> written to PostgreSQL by this job (includes
      Kafka buffering and waiting for the next trigger).
    - end_to_end: Wikimedia event time -> written to PostgreSQL.

    Wikimedia event times have whole-second resolution, so ingestion and
    end-to-end latencies carry up to a second of rounding (and can come out
    slightly negative). Events without an ingestion timestamp are left out
    of the two latencies that need it.
    """
    latencies: dict[str, list[float]] = {"ingestion": [], "processing": [], "end_to_end": []}
    for row in rows:
        event_ts = row.get("timestamp")
        ingestion_ts = row.get("ingestion_timestamp")
        if event_ts is not None:
            latencies["end_to_end"].append(processed_at - event_ts)
        if ingestion_ts is not None:
            latencies["processing"].append(processed_at - ingestion_ts)
            if event_ts is not None:
                latencies["ingestion"].append(ingestion_ts - event_ts)
    return latencies


def kafka_offsets_behind(progress: Optional[dict[str, Any]]) -> Optional[dict[str, float]]:
    """Read how far the Kafka source is behind the latest offsets from a
    StreamingQuery progress report. Returns None when Spark did not report
    it (no batch yet, or a source without these metrics)."""
    if not progress:
        return None
    for source in progress.get("sources", []):
        source_metrics = source.get("metrics") or {}
        if "maxOffsetsBehindLatest" in source_metrics:
            return {
                "max": float(source_metrics["maxOffsetsBehindLatest"]),
                "avg": float(source_metrics["avgOffsetsBehindLatest"]),
            }
    return None


def build_spark_session(settings: Settings) -> SparkSession:
    return (
        SparkSession.builder.appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.topic_edits)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return raw


def parse_events(raw_df: DataFrame) -> DataFrame:
    """Parse the Kafka `value` column (JSON) into typed columns, keeping the
    Kafka partition and offset as each event's lineage. Rows that do not
    parse against EVENT_SCHEMA come through with null fields; the raw-event
    writer counts and skips them."""
    json_str = raw_df.selectExpr(
        "CAST(value AS STRING) AS json_value",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
    )
    parsed = json_str.withColumn("data", F.from_json(F.col("json_value"), EVENT_SCHEMA))
    exploded = parsed.select("json_value", "kafka_partition", "kafka_offset", "data.*")
    # timestamp_seconds keeps sub-second precision and, unlike formatting
    # through from_unixtime, does not depend on the session time zone.
    return exploded.withColumn("event_time", F.timestamp_seconds(F.col("timestamp")))


def build_windowed_aggregation(parsed_df: DataFrame, spark_config: SparkConfig) -> DataFrame:
    """Group events into sliding event-time windows per (wiki, page_title),
    collecting the per-edit fields the detectors need."""
    return (
        parsed_df
        .withWatermark("event_time", spark_config.watermark_delay)
        .groupBy(
            F.window("event_time", spark_config.window_duration, spark_config.slide_duration),
            F.col("wiki"),
            F.col("page_title"),
        )
        .agg(
            F.first("page_id", ignorenulls=True).alias("page_id"),
            F.collect_list(
                F.struct(
                    "user", "revision_id", "is_bot", "is_revert",
                    "revert_target_revision_id", "byte_length_delta", "timestamp",
                )
            ).alias("records"),
        )
    )


def start_windowed_query(windowed_df: DataFrame, batch_writer, spark_config: SparkConfig) -> StreamingQuery:
    """Start the windowed query in append mode so each window reaches
    `batch_writer` once, with its final contents."""
    return (
        windowed_df.writeStream
        .outputMode("append")
        .foreachBatch(batch_writer)
        .option("checkpointLocation", f"{spark_config.checkpoint_dir}/windowed")
        .trigger(processingTime=spark_config.trigger_interval)
        .start()
    )


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * MILLISECONDS_PER_SECOND, 3)


def _utc_from_epoch(seconds: Optional[float]) -> Optional[datetime]:
    return None if seconds is None else datetime.fromtimestamp(seconds, tz=timezone.utc)


def to_edit_event_row(row: dict[str, Any]) -> dict[str, Any]:
    """Shape one parsed Kafka row as an `edit_events` / raw_events record.
    Times are taken from the epoch fields so they are UTC-aware."""
    return {
        "event_id": row["event_id"],
        "event_time": _utc_from_epoch(row["timestamp"]),
        "wiki": row["wiki"],
        "event_type": row["event_type"],
        "page_title": row["page_title"],
        "page_id": row.get("page_id"),
        "namespace": row.get("namespace"),
        "revision_id": row.get("revision_id"),
        "previous_revision_id": row.get("previous_revision_id"),
        "user": row.get("user"),
        "user_id": row.get("user_id"),
        "is_bot": bool(row.get("is_bot") or False),
        "comment": row.get("comment"),
        "byte_length_new": row.get("byte_length_new"),
        "byte_length_old": row.get("byte_length_old"),
        "byte_length_delta": row.get("byte_length_delta"),
        "is_revert": bool(row.get("is_revert") or False),
        "revert_target_revision_id": row.get("revert_target_revision_id"),
        "ingestion_time": _utc_from_epoch(row.get("ingestion_timestamp")),
        "kafka_partition": row.get("kafka_partition"),
        "kafka_offset": row.get("kafka_offset"),
    }


def make_raw_batch_writer(db: Database, parquet_writer: ParquetWriter, metrics: PipelineMetrics):
    """Returns a foreachBatch callback that persists individual raw events
    to PostgreSQL's `edit_events` table and to Parquet, and records the
    batch's counts, latencies and write timings."""

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_started = time.perf_counter()
        # A single collect: every Spark action on batch_df re-reads the
        # micro-batch from Kafka, so separate count() calls cost a full read each.
        collected = [row.asDict() for row in batch_df.collect()]
        rows = [row for row in collected if all(row.get(name) is not None for name in REQUIRED_EVENT_FIELDS)]
        rejected = len(collected) - len(rows)
        collect_ms = _elapsed_ms(batch_started)
        if rejected:
            logger.warning("Skipped %d unparseable Kafka messages in raw batch %s", rejected, batch_id)
        if not rows:
            metrics.add_raw_batch(len(collected), 0, rejected, None)
            return

        event_rows = [to_edit_event_row(row) for row in rows]

        postgres_started = time.perf_counter()
        postgres_ok = True
        try:
            db.upsert_edit_events(event_rows)
        except Exception:
            postgres_ok = False
            logger.exception("Failed writing raw events batch %s to Postgres", batch_id)
            metrics.record_error("postgres_raw_events")
        postgres_write_ms = _elapsed_ms(postgres_started)
        processed_at = time.time()

        parquet_started = time.perf_counter()
        parquet_stats = None
        try:
            parquet_stats = parquet_writer.write_records("raw_events", event_rows, time_field="event_time")
        except Exception:
            logger.exception("Failed writing raw events batch %s to Parquet", batch_id)
            metrics.record_error("parquet_raw_events")
        parquet_write_ms = _elapsed_ms(parquet_started)

        # Latencies end at the PostgreSQL write, so they only mean something
        # when that write succeeded.
        latencies = event_latencies(rows, processed_at) if postgres_ok else {}
        metrics.add_raw_batch(len(collected), len(rows), rejected, max(row["timestamp"] for row in rows))
        metrics.set_details("raw_batch", {
            "batch_id": batch_id,
            "rows": len(rows),
            "rejected": rejected,
            "collect_ms": collect_ms,
            "postgres_write_ms": postgres_write_ms,
            "parquet_write_ms": parquet_write_ms,
            "batch_ms": _elapsed_ms(batch_started),
            "parquet": asdict(parquet_stats) if parquet_stats else None,
            "ingestion_latency_ms": summarize_ms(latencies.get("ingestion", [])),
            "processing_latency_ms": summarize_ms(latencies.get("processing", [])),
            "end_to_end_latency_ms": summarize_ms(latencies.get("end_to_end", [])),
        })
        metrics.set_details("driver_rss_mb", round(psutil.Process().memory_info().rss / BYTES_PER_MB, 1))

        try:
            db.record_pipeline_metrics({"component": "spark", **metrics.snapshot_and_reset_throughput()})
        except Exception:
            logger.exception("Failed to record pipeline metrics for batch %s", batch_id)

    return _write_batch


@dataclass
class WindowEvaluation:
    """Rows to persist for one micro-batch of finalized windows."""

    activity_rows: list[dict[str, Any]] = field(default_factory=list)
    anomaly_rows: list[dict[str, Any]] = field(default_factory=list)
    edit_war_rows: list[dict[str, Any]] = field(default_factory=list)
    revision_requests: list[RevisionScoreRequest] = field(default_factory=list)
    revision_meta: dict[int, dict[str, Any]] = field(default_factory=dict)


def evaluate_windows(
    rows: list[dict[str, Any]],
    settings: Settings,
    history_store: PageHistoryStore,
    window_minutes: float,
) -> WindowEvaluation:
    """Compute activity metrics, anomaly results and edit-war signals for
    each finalized (wiki, page_title, window) row.

    Rows are evaluated in window order so each page's anomaly history is
    recorded chronologically, even when one micro-batch finalizes several
    windows of the same page.
    """
    evaluation = WindowEvaluation()
    for row in sorted(rows, key=lambda r: r["window"]["start"]):
        # PySpark hands back timestamps as naive local-time datetimes; make
        # them UTC-aware so PostgreSQL and Parquet store the right instant.
        window_start = row["window"]["start"].astimezone(timezone.utc)
        window_end = row["window"]["end"].astimezone(timezone.utc)
        wiki = row["wiki"]
        page_title = row["page_title"]
        records_raw = row.get("records") or []

        window_events = [
            PageWindowEvent(
                user=r.get("user"),
                is_bot=bool(r.get("is_bot") or False),
                is_revert=bool(r.get("is_revert") or False),
                byte_length_delta=r.get("byte_length_delta"),
            )
            for r in records_raw
        ]
        metrics_result = aggregate_page_window(window_events, window_minutes)

        evaluation.activity_rows.append({
            "wiki": wiki,
            "page_id": row.get("page_id"),
            "page_title": page_title,
            "window_start": window_start,
            "window_end": window_end,
            "edit_count": metrics_result.edit_count,
            "unique_editors": metrics_result.unique_editors,
            "human_edits": metrics_result.human_edits,
            "bot_edits": metrics_result.bot_edits,
            "revert_count": metrics_result.revert_count,
            "net_byte_delta": metrics_result.net_byte_delta,
            "edit_velocity": metrics_result.edit_velocity,
        })

        anomaly_result = history_store.evaluate_and_record(
            wiki, page_title, metrics_result.edit_count
        )
        if anomaly_result.is_anomaly:
            evaluation.anomaly_rows.append({
                "wiki": wiki,
                "page_id": row.get("page_id"),
                "page_title": page_title,
                "window_start": window_start,
                "window_end": window_end,
                "edit_count": metrics_result.edit_count,
                "unique_editors": metrics_result.unique_editors,
                "human_edits": metrics_result.human_edits,
                "bot_edits": metrics_result.bot_edits,
                "baseline_median": anomaly_result.baseline_median,
                "baseline_mad": anomaly_result.baseline_mad,
                "anomaly_score": anomaly_result.anomaly_score,
                "anomaly_type": anomaly_result.anomaly_type,
                "threshold_used": anomaly_result.threshold_used,
            })

        edit_records = [
            EditRecord(
                user=r.get("user"),
                revision_id=r.get("revision_id"),
                is_revert=bool(r.get("is_revert") or False),
                revert_target_revision_id=r.get("revert_target_revision_id"),
                byte_length_delta=r.get("byte_length_delta"),
                timestamp=r.get("timestamp") or 0.0,
            )
            for r in records_raw
        ]
        war_result = detect_edit_war(edit_records, settings.edit_war)
        if war_result.mutual_revert_count > 0 or war_result.flag:
            evaluation.edit_war_rows.append({
                "wiki": wiki,
                "page_id": row.get("page_id"),
                "page_title": page_title,
                "window_start": window_start,
                "window_end": window_end,
                "mutual_revert_count": war_result.mutual_revert_count,
                "editor_a": war_result.editor_a,
                "editor_b": war_result.editor_b,
                "total_reverts": war_result.total_reverts,
                "edit_burst_count": war_result.edit_burst_count,
                "conflict_score": war_result.conflict_score,
                "flag": war_result.flag,
            })

        # Sample at most one revision per page/window for Lift Wing
        # enrichment to keep the external-call volume bounded.
        if settings.liftwing.enabled and records_raw:
            latest = max(records_raw, key=lambda r: r.get("timestamp") or 0.0)
            rev_id = latest.get("revision_id")
            if rev_id is not None:
                evaluation.revision_requests.append(RevisionScoreRequest(wiki=wiki, revision_id=rev_id))
                evaluation.revision_meta[rev_id] = {"wiki": wiki, "page_title": page_title}

    return evaluation


def make_windowed_batch_writer(
    settings: Settings,
    db: Database,
    parquet_writer: ParquetWriter,
    history_store: PageHistoryStore,
    liftwing_client: LiftWingClient,
    metrics: PipelineMetrics,
):
    """Returns a foreachBatch callback that evaluates each finalized
    (wiki, page_title, window) group, optionally enriches a sample of
    revisions via Lift Wing, and persists everything to PostgreSQL and
    Parquet."""
    window_minutes = _duration_to_minutes(settings.spark.window_duration)

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_started = time.perf_counter()
        rows = [row.asDict(recursive=True) for row in batch_df.collect()]
        if not rows:
            return

        evaluation_started = time.perf_counter()
        evaluation = evaluate_windows(rows, settings, history_store, window_minutes)
        evaluation_ms = _elapsed_ms(evaluation_started)

        def persist(dataset: str, dataset_rows: list[dict[str, Any]], write_to_postgres) -> None:
            # Parquet is written even when PostgreSQL fails, and vice versa.
            try:
                write_to_postgres(dataset_rows)
            except Exception:
                logger.exception("Failed writing %s for batch %s to Postgres", dataset, batch_id)
                metrics.record_error(f"postgres_{dataset}")
            try:
                parquet_writer.write_records(dataset, dataset_rows, time_field="window_start")
            except Exception:
                logger.exception("Failed writing %s for batch %s to Parquet", dataset, batch_id)
                metrics.record_error(f"parquet_{dataset}")

        persist_started = time.perf_counter()
        persist("activity_windows", evaluation.activity_rows, db.upsert_activity_windows)
        if evaluation.anomaly_rows:
            persist("anomaly_events", evaluation.anomaly_rows, db.insert_anomaly_events)
        if evaluation.edit_war_rows:
            persist("edit_war_signals", evaluation.edit_war_rows, db.insert_edit_war_signals)
        persist_ms = _elapsed_ms(persist_started)
        processed_at = time.time()

        metrics.add_detections(
            anomalies=len(evaluation.anomaly_rows),
            edit_wars=sum(1 for r in evaluation.edit_war_rows if r["flag"]),
        )
        # How long after a window closed (in event time) its results were
        # stored: the watermark delay plus trigger wait plus this batch.
        emit_delays = [processed_at - row["window_end"].timestamp() for row in evaluation.activity_rows]
        metrics.set_details("window_batch", {
            "batch_id": batch_id,
            "windows": len(evaluation.activity_rows),
            "anomalies": len(evaluation.anomaly_rows),
            "edit_war_signals": len(evaluation.edit_war_rows),
            "evaluation_ms": evaluation_ms,
            "persist_ms": persist_ms,
            "batch_ms": _elapsed_ms(batch_started),
            "window_emit_delay_ms": summarize_ms(emit_delays),
        })
        metrics.set_details("page_history", {
            "active_pages": history_store.active_pages,
            "evicted_pages_total": history_store.evicted_pages_total,
            "observations": history_store.observation_count(),
        })

        if evaluation.revision_requests:
            try:
                results = liftwing_client.score_batch(evaluation.revision_requests)
                quality_rows = [
                    {
                        "wiki": r.wiki,
                        "page_title": evaluation.revision_meta.get(r.revision_id, {}).get("page_title", ""),
                        "revision_id": r.revision_id,
                        "model_name": r.model_name,
                        "score": r.score,
                        "raw_response": r.raw_response,
                    }
                    for r in results
                    if r.score is not None
                ]
                if quality_rows:
                    db.upsert_revision_quality_scores(quality_rows)
            except Exception:
                logger.exception("Lift Wing enrichment failed for batch %s (non-fatal)", batch_id)
                metrics.record_error("liftwing")

    return _write_batch


def run() -> None:
    settings = get_settings()
    global logger
    logger = configure_logging("processing.streaming_job")
    check_history_ttl(settings)

    db = Database(settings.postgres)
    db.init_schema()
    parquet_writer = ParquetWriter(settings.parquet)
    history_store = PageHistoryStore(settings.anomaly)
    liftwing_client = LiftWingClient(settings.liftwing)
    metrics = PipelineMetrics()

    spark = build_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")

    raw_kafka_df = read_kafka_stream(spark, settings)
    parsed_df = parse_events(raw_kafka_df)

    raw_query = (
        parsed_df.writeStream
        .foreachBatch(make_raw_batch_writer(db, parquet_writer, metrics))
        .option("checkpointLocation", f"{settings.spark.checkpoint_dir}/raw_events")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )

    start_windowed_query(
        build_windowed_aggregation(parsed_df, settings.spark),
        make_windowed_batch_writer(
            settings, db, parquet_writer, history_store, liftwing_client, metrics
        ),
        settings.spark,
    )

    logger.info("WikiPulse Spark streaming job started (raw + windowed queries)")
    while not spark.streams.awaitAnyTermination(timeout=PROGRESS_POLL_SECONDS):
        metrics.set_details("kafka_offsets_behind", kafka_offsets_behind(raw_query.lastProgress))


if __name__ == "__main__":
    run()
