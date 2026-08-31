"""Spark Structured Streaming job: the core of the WikiPulse pipeline.

Reads normalized events from Kafka, computes five-minute sliding-window
page activity metrics, and runs anomaly + edit-war detection on each
completed window before persisting results to PostgreSQL and Parquet.

Design notes (see README "Architecture" section for the full rationale):
- Spark handles the heavy lifting of event-time windowing (with a
  watermark for late data) and per-window raw-record collection.
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
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common.config import Settings, get_settings
from common.logging_setup import configure_logging
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

_DURATION_UNIT_TO_MINUTES = {
    "second": 1 / 60,
    "seconds": 1 / 60,
    "minute": 1.0,
    "minutes": 1.0,
    "hour": 60.0,
    "hours": 60.0,
}


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
    queries (raw-event sink and windowed-aggregation sink), periodically
    flushed to the `pipeline_metrics` Postgres table for the dashboard."""

    def __init__(self):
        self._lock = threading.Lock()
        self.events_received = 0
        self.events_parsed = 0
        self.events_rejected = 0
        self.anomalies_detected = 0
        self.edit_wars_detected = 0
        self.error_count = 0
        self.last_event_time: Optional[float] = None
        self._last_flush = time.time()
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

    def record_error(self) -> None:
        with self._lock:
            self.error_count += 1

    def snapshot_and_reset_throughput(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            elapsed = max(now - self._last_flush, 1e-6)
            throughput = (self.events_parsed - self._last_flush_count) / elapsed
            self._last_flush = now
            self._last_flush_count = self.events_parsed
            return {
                "events_received": self.events_received,
                "events_parsed": self.events_parsed,
                "events_rejected": self.events_rejected,
                "events_published": self.events_parsed,
                "anomalies_detected": self.anomalies_detected,
                "edit_wars_detected": self.edit_wars_detected,
                "processing_latency_ms": None,
                "throughput_per_sec": round(throughput, 4),
                "last_event_time": (
                    None if self.last_event_time is None
                    else time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(self.last_event_time))
                ),
                "error_count": self.error_count,
            }


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
    """Parse the Kafka `value` column (JSON) into typed columns, dropping
    (but counting) rows that fail to parse against EVENT_SCHEMA."""
    json_str = raw_df.selectExpr("CAST(value AS STRING) AS json_value")
    parsed = json_str.withColumn("data", F.from_json(F.col("json_value"), EVENT_SCHEMA))
    exploded = parsed.select("json_value", "data.*")
    exploded = exploded.withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))
    return exploded


def make_raw_batch_writer(db: Database, parquet_writer: ParquetWriter, metrics: PipelineMetrics):
    """Returns a foreachBatch callback that persists individual raw events
    to PostgreSQL's `edit_events` table and to Parquet, and updates
    ingestion-side metrics counters."""

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        received = batch_df.count()
        valid_df = batch_df.filter(F.col("event_id").isNotNull() & F.col("wiki").isNotNull())
        rejected = received - valid_df.count()

        rows = [row.asDict() for row in valid_df.collect()]
        if not rows:
            metrics.add_raw_batch(received, 0, rejected, None)
            return

        db_rows = []
        parquet_rows = []
        last_event_time = None
        for row in rows:
            event_time_val = row.get("timestamp")
            last_event_time = max(last_event_time or 0, event_time_val or 0)
            db_rows.append({
                "event_id": row["event_id"],
                "event_time": row["event_time"],
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
            })
            parquet_rows.append({**db_rows[-1], "event_time": row["event_time"]})

        try:
            db.upsert_edit_events(db_rows)
        except Exception:
            logger.exception("Failed writing raw events batch %s to Postgres", batch_id)
            metrics.record_error()

        try:
            parquet_writer.write_records("raw_events", parquet_rows, time_field="event_time")
        except Exception:
            logger.exception("Failed writing raw events batch %s to Parquet", batch_id)
            metrics.record_error()

        metrics.add_raw_batch(received, len(rows), rejected, last_event_time)

        try:
            db.record_pipeline_metrics({"component": "spark", **metrics.snapshot_and_reset_throughput()})
        except Exception:
            logger.exception("Failed to record pipeline metrics for batch %s", batch_id)

    return _write_batch


def make_windowed_batch_writer(
    settings: Settings,
    db: Database,
    parquet_writer: ParquetWriter,
    history_store: PageHistoryStore,
    liftwing_client: LiftWingClient,
    metrics: PipelineMetrics,
):
    """Returns a foreachBatch callback that, for each completed
    (wiki, page_title, window) group in the micro-batch:
    1. computes activity metrics,
    2. runs median/MAD (or count-fallback) anomaly detection,
    3. runs mutual-revert edit-war detection,
    4. optionally enriches a sample of revisions via Lift Wing,
    5. persists everything to PostgreSQL and Parquet.
    """
    window_minutes = _duration_to_minutes(settings.spark.window_duration)

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        rows = [row.asDict(recursive=True) for row in batch_df.collect()]

        activity_rows = []
        anomaly_rows = []
        edit_war_rows = []
        revision_requests: list[RevisionScoreRequest] = []
        revision_meta: dict[int, dict[str, Any]] = {}

        for row in rows:
            window = row["window"]
            window_start, window_end = window["start"], window["end"]
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

            activity_rows.append({
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
                anomaly_rows.append({
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
                edit_war_rows.append({
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
                    revision_requests.append(RevisionScoreRequest(wiki=wiki, revision_id=rev_id))
                    revision_meta[rev_id] = {"wiki": wiki, "page_title": page_title}

        try:
            db.upsert_activity_windows(activity_rows)
            parquet_writer.write_records(
                "activity_windows", activity_rows, time_field="window_start"
            )
        except Exception:
            logger.exception("Failed persisting activity windows for batch %s", batch_id)
            metrics.record_error()

        if anomaly_rows:
            try:
                db.insert_anomaly_events(anomaly_rows)
                parquet_writer.write_records(
                    "anomaly_events", anomaly_rows, time_field="window_start"
                )
            except Exception:
                logger.exception("Failed persisting anomaly events for batch %s", batch_id)
                metrics.record_error()

        if edit_war_rows:
            try:
                db.insert_edit_war_signals(edit_war_rows)
                parquet_writer.write_records(
                    "edit_war_signals", edit_war_rows, time_field="window_start"
                )
            except Exception:
                logger.exception("Failed persisting edit-war signals for batch %s", batch_id)
                metrics.record_error()

        metrics.add_detections(
            anomalies=len(anomaly_rows),
            edit_wars=sum(1 for r in edit_war_rows if r["flag"]),
        )

        if revision_requests:
            try:
                results = liftwing_client.score_batch(revision_requests)
                quality_rows = [
                    {
                        "wiki": r.wiki,
                        "page_title": revision_meta.get(r.revision_id, {}).get("page_title", ""),
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
                metrics.record_error()

    return _write_batch


def run() -> None:
    settings = get_settings()
    global logger
    logger = configure_logging("processing.streaming_job")

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

    windowed_df = (
        parsed_df
        .withWatermark("event_time", settings.spark.watermark_delay)
        .groupBy(
            F.window("event_time", settings.spark.window_duration, settings.spark.slide_duration),
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

    windowed_query = (
        windowed_df.writeStream
        .outputMode("update")
        .foreachBatch(
            make_windowed_batch_writer(
                settings, db, parquet_writer, history_store, liftwing_client, metrics
            )
        )
        .option("checkpointLocation", f"{settings.spark.checkpoint_dir}/windowed")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )

    logger.info("WikiPulse Spark streaming job started (raw + windowed queries)")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
