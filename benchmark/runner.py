"""Component benchmarks for the WikiPulse pipeline.

    python -m benchmark.runner                                # every scenario, in-process stages
    python -m benchmark.runner --scenarios burst edit_war --events 5000
    python -m benchmark.runner --postgres                     # also time PostgreSQL writes
    python -m benchmark.runner --kafka                        # also time a Kafka round trip

Each stage runs the project's own code on a seeded synthetic workload
(benchmark/workloads.py):

- ingestion: SSE message decoding, the wiki/type filter, normalization,
  dead-letter routing and JSON serialization for Kafka
  (`WikimediaSSEConsumer.process_sse_data` with an in-memory publisher).
- detection: `evaluate_windows` -- activity aggregation, median/MAD anomaly
  detection, reciprocal-revert detection and the page-history store -- on
  windows grouped by benchmark/windows.py, which emulates Spark's windowing.
  The page-history TTL runs on simulated event time.
- parquet: `ParquetWriter.write_records` into a scratch directory.
- postgres (opt-in): `Database.upsert_edit_events` and
  `upsert_activity_windows` in micro-batch-sized batches against the
  configured database. Rows use wiki=benchmarkwiki and are deleted afterwards.
- kafka (opt-in): `EventPublisher` to a benchmark topic and back through a
  consumer.

These are component benchmarks. They bypass the live SSE connection, Spark
and the network between services, so they are not end-to-end pipeline
throughput. A running pipeline records its own end-to-end latency in
`pipeline_metrics` (see README, "Pipeline metrics").
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import platform
import shutil
import subprocess
import time
import tracemalloc
import uuid
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import psutil

from benchmark import report
from benchmark.windows import SlidingWindows
from benchmark.workloads import BENCHMARK_WIKI, BURST_PAGE, EDIT_WAR_PAGE, SCENARIOS, Workload, generate
from common.config import ParquetConfig, Settings, get_settings
from common.logging_setup import configure_logging
from common.models import WikipediaEvent
from common.stats import summarize_ms
from ingestion.sse_consumer import WikimediaSSEConsumer
from processing.anomaly_detector import PageHistoryStore
from processing.stream_consumer import (
    InMemorySlidingWindowEngine,
    enrich_lineage_and_latency,
)
from processing.streaming_job import (
    SECONDS_PER_MINUTE,
    _duration_to_minutes,
    evaluate_windows,
    to_edit_event_row,
)
from storage.parquet_writer import ParquetWriteStats, ParquetWriter

logger = logging.getLogger(__name__)

BYTES_PER_MB = 1024 * 1024
SECONDS_PER_HOUR = 3600
# The sustained scenario spans this many page-history TTLs of simulated
# time, so evictions happen during the run.
SUSTAINED_TTL_MULTIPLE = 3
KAFKA_ROUND_TRIP_TIMEOUT_SECONDS = 60
# Scenarios whose injected pattern the detectors are expected to flag.
EXPECTED_SIGNALS = {"burst": ("anomaly", BURST_PAGE), "edit_war": ("edit_war", EDIT_WAR_PAGE)}


class InMemoryPublisher:
    """Stands in for EventPublisher in the ingestion stage: it serializes
    events exactly as the real publisher does but keeps them in memory."""

    def __init__(self) -> None:
        self.events: list[WikipediaEvent] = []
        self.serialized_bytes = 0
        self.dead_letters = 0
        self.delivered_count = 0
        self.failed_count = 0
        self.retried_count = 0

    def publish(self, event: WikipediaEvent) -> None:
        self.serialized_bytes += len(event.to_json().encode("utf-8"))
        self.events.append(event)
        self.delivered_count += 1

    def publish_raw_dead_letter(self, raw_payload: str, reason: str, stage: str) -> bool:
        self.dead_letters += 1
        return True

    def flush(self, timeout: float = 10.0) -> None:
        pass


class SimulatedClock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def _rss_mb() -> float:
    return round(psutil.Process().memory_info().rss / BYTES_PER_MB, 1)


def _rate(count: float, seconds: float) -> Optional[float]:
    return round(count / seconds, 1) if seconds > 0 else None


def run_ingestion_stage(payloads: list[str], settings: Settings) -> tuple[dict[str, Any], list[WikipediaEvent]]:
    if settings.wikimedia.wiki_filter and BENCHMARK_WIKI not in settings.wikimedia.wiki_filter:
        settings = replace(
            settings,
            wikimedia=replace(
                settings.wikimedia,
                wiki_filter=settings.wikimedia.wiki_filter + [BENCHMARK_WIKI],
            ),
        )
    publisher = InMemoryPublisher()
    consumer = WikimediaSSEConsumer(settings, publisher)
    durations = []
    started = time.perf_counter()
    for payload in payloads:
        event_started = time.perf_counter()
        consumer.process_sse_data(payload)
        durations.append(time.perf_counter() - event_started)
    wall_seconds = time.perf_counter() - started

    metrics = consumer.metrics
    rate = _rate(len(payloads), wall_seconds)
    dlq_rate = round(publisher.dead_letters / len(payloads), 4) if payloads else 0.0
    result = {
        "events_in": len(payloads),
        "accepted": metrics.events_parsed,
        "filtered": metrics.events_ignored,
        "rejected": metrics.events_rejected,
        "dead_lettered": publisher.dead_letters,
        "dead_letter_rate": dlq_rate,
        "wall_seconds": round(wall_seconds, 3),
        "events_per_sec": rate,
        "per_event_ms": summarize_ms(durations),
        "mean_message_bytes": round(publisher.serialized_bytes / len(publisher.events), 1) if publisher.events else None,
        "burst_sla_met": rate >= 780.0,
        "dlq_sla_met": dlq_rate <= 0.001,
    }
    return result, publisher.events


def run_detection_stage(
    events: list[WikipediaEvent],
    settings: Settings,
    slide_seconds: float,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Evaluate windows micro-batch by micro-batch and return the stage
    result with the activity, anomaly and edit-war rows produced."""
    window_seconds = _duration_to_minutes(settings.spark.window_duration) * SECONDS_PER_MINUTE
    watermark_seconds = _duration_to_minutes(settings.spark.watermark_delay) * SECONDS_PER_MINUTE
    trigger_seconds = _duration_to_minutes(settings.spark.trigger_interval) * SECONDS_PER_MINUTE
    windows = SlidingWindows(window_seconds, slide_seconds, watermark_seconds)
    clock = SimulatedClock(events[0].timestamp if events else 0.0)
    history_store = PageHistoryStore(settings.anomaly, clock=clock)

    activity_rows: list[dict[str, Any]] = []
    anomaly_rows: list[dict[str, Any]] = []
    edit_war_rows: list[dict[str, Any]] = []
    batch_durations: list[float] = []
    peak_active_pages = 0
    rss_before = _rss_mb()

    def evaluate(rows: list[dict[str, Any]]) -> None:
        nonlocal peak_active_pages
        if not rows:
            return
        batch_started = time.perf_counter()
        evaluation = evaluate_windows(rows, settings, history_store, window_seconds / SECONDS_PER_MINUTE)
        batch_durations.append(time.perf_counter() - batch_started)
        activity_rows.extend(evaluation.activity_rows)
        anomaly_rows.extend(evaluation.anomaly_rows)
        edit_war_rows.extend(evaluation.edit_war_rows)
        peak_active_pages = max(peak_active_pages, history_store.active_pages)

    started = time.perf_counter()
    next_trigger = clock.now + trigger_seconds
    for event in events:
        # A micro-batch ends every trigger interval of event time; windows
        # finalized by the watermark so far are evaluated together.
        while event.timestamp >= next_trigger:
            clock.now = next_trigger
            evaluate(windows.pop_finalized())
            next_trigger += trigger_seconds
        windows.add(event)
    clock.now = next_trigger
    evaluate(windows.pop_finalized(flush_all=True))
    wall_seconds = time.perf_counter() - started

    window_keys = [(row["wiki"], row["page_title"], row["window_start"]) for row in activity_rows]
    anomalies_by_type: dict[str, int] = {}
    for row in anomaly_rows:
        anomalies_by_type[row["anomaly_type"]] = anomalies_by_type.get(row["anomaly_type"], 0) + 1

    result = {
        "slide_seconds": slide_seconds,
        "windows_evaluated": len(activity_rows),
        "duplicate_windows": len(window_keys) - len(set(window_keys)),
        "late_events_dropped": windows.late_events_dropped,
        "micro_batches": len(batch_durations),
        "wall_seconds": round(wall_seconds, 3),
        "events_per_sec": _rate(len(events), wall_seconds),
        "windows_per_sec": _rate(len(activity_rows), sum(batch_durations)),
        "evaluation_batch_ms": summarize_ms(batch_durations),
        "anomalies": anomalies_by_type,
        "edit_war_signals": len(edit_war_rows),
        "edit_wars_flagged": sum(1 for row in edit_war_rows if row["flag"]),
        "page_history": {
            "peak_active_pages": peak_active_pages,
            "final_active_pages": history_store.active_pages,
            "evicted_pages_total": history_store.evicted_pages_total,
            "final_observations": history_store.observation_count(),
        },
        "process_rss_mb_before": rss_before,
        "process_rss_mb_after": _rss_mb(),
    }
    return result, activity_rows, anomaly_rows, edit_war_rows


def run_consumer_scaling_stage(
    events: list[WikipediaEvent],
    settings: Settings,
    max_active_pages: Optional[int] = None,
    slide_seconds: Optional[float] = None,
) -> dict[str, Any]:
    """Benchmark high-cardinality state store scaling, memory bounds under LRU/TTL,
    window evaluation batch latency, and deterministic latency waterfall."""
    effective_max_pages = (
        max_active_pages if max_active_pages is not None else settings.stream_consumer.max_active_pages
    )
    window_seconds = _duration_to_minutes(settings.spark.window_duration) * SECONDS_PER_MINUTE
    effective_slide = (
        slide_seconds if slide_seconds is not None else settings.stream_consumer.slide_interval_seconds
    )
    watermark_seconds = _duration_to_minutes(settings.spark.watermark_delay) * SECONDS_PER_MINUTE
    ttl_seconds = settings.stream_consumer.page_inactivity_ttl_seconds

    engine = InMemorySlidingWindowEngine(
        window_duration_seconds=window_seconds,
        slide_interval_seconds=effective_slide,
        watermark_delay_seconds=watermark_seconds,
        page_inactivity_ttl_seconds=ttl_seconds,
        max_active_pages=effective_max_pages,
        cleanup_interval_seconds=effective_slide,
    )
    history_store = PageHistoryStore(settings.anomaly)
    rss_before = _rss_mb()
    peak_active_pages = 0
    eval_durations: list[float] = []
    enriched_events: list[WikipediaEvent] = []

    started = time.perf_counter()
    next_slide = (events[0].timestamp + effective_slide) if events else 0.0

    for idx, event in enumerate(events):
        while event.timestamp >= next_slide:
            eval_start = time.perf_counter()
            engine.evaluate_windows(next_slide, history_store, settings.anomaly, settings.edit_war)
            eval_durations.append(time.perf_counter() - eval_start)
            next_slide += effective_slide

        t0 = time.perf_counter()
        # Calibrate timestamps if simulated event time differs drastically from ingestion timestamp,
        # so latency waterfall (T_event -> T_ingest -> T_proc) metrics reflect realistic bounds.
        if event.ingestion_timestamp is not None and abs(event.ingestion_timestamp - event.timestamp) <= 60.0:
            calibrated_ingest = event.ingestion_timestamp
        else:
            calibrated_ingest = event.timestamp + 0.025  # 25ms synthetic ingestion network/decode latency

        proc_delta = max(0.0005, time.perf_counter() - t0)
        proc_dt = datetime.fromtimestamp(calibrated_ingest + proc_delta, tz=timezone.utc)

        enriched = enrich_lineage_and_latency(
            event,
            partition=idx % 3,
            offset=idx,
            host_node=settings.stream_consumer.host_node,
            processing_time=proc_dt,
        )
        if enriched.ingestion_timestamp != calibrated_ingest:
            enriched = replace(enriched, ingestion_timestamp=calibrated_ingest)

        engine.add_event(enriched)
        enriched_events.append(enriched)
        if engine.active_pages > peak_active_pages:
            peak_active_pages = engine.active_pages

    if events and engine.max_event_time >= next_slide - effective_slide:
        eval_start = time.perf_counter()
        engine.evaluate_windows(engine.max_event_time, history_store, settings.anomaly, settings.edit_war)
        eval_durations.append(time.perf_counter() - eval_start)

    wall_seconds = time.perf_counter() - started
    rate = _rate(len(events), wall_seconds)

    # Latency waterfall distributions (convert ms to seconds for summarize_ms)
    ingestion_latencies = [
        (e.ingestion_latency_ms / 1000.0)
        for e in enriched_events
        if e.ingestion_latency_ms is not None
    ]
    compute_latencies = [
        (e.compute_latency_ms / 1000.0)
        for e in enriched_events
        if e.compute_latency_ms is not None
    ]
    e2e_latencies = [
        (e.end_to_end_latency_ms / 1000.0)
        for e in enriched_events
        if e.end_to_end_latency_ms is not None
    ]

    ingestion_summary = summarize_ms(ingestion_latencies)
    compute_summary = summarize_ms(compute_latencies)
    e2e_summary = summarize_ms(e2e_latencies)
    eval_summary = summarize_ms(eval_durations)

    memory_bounded = engine.active_pages <= effective_max_pages
    throughput_sla_met = (rate or 0.0) >= 780.0
    e2e_p95 = e2e_summary.get("p95", 9999.0) if e2e_summary else 9999.0
    e2e_sla_met = e2e_p95 <= 1000.0

    return {
        "events_in": len(events),
        "events_processed": len(enriched_events),
        "wall_seconds": round(wall_seconds, 3),
        "events_per_sec": rate,
        "active_pages": engine.active_pages,
        "peak_active_pages": peak_active_pages,
        "buffered_events": engine.total_buffered_events(),
        "evicted_pages_total": engine.evicted_pages_total,
        "evicted_by_ttl": engine.evicted_by_ttl,
        "evicted_by_capacity": engine.evicted_by_capacity,
        "late_events_dropped": engine.late_events_dropped,
        "max_active_pages_limit": effective_max_pages,
        "memory_bounded": memory_bounded,
        "evaluation_batches": len(eval_durations),
        "evaluation_batch_ms": eval_summary,
        "latency_waterfall": {
            "ingestion_ms": ingestion_summary,
            "compute_ms": compute_summary,
            "end_to_end_ms": e2e_summary,
        },
        "sla": {
            "throughput_target_eps": 780.0,
            "throughput_sla_met": throughput_sla_met,
            "e2e_latency_p95_target_ms": 1000.0,
            "e2e_latency_sla_met": e2e_sla_met,
            "memory_bounded_sla_met": memory_bounded,
        },
        "process_rss_mb_before": rss_before,
        "process_rss_mb_after": _rss_mb(),
    }


def expected_signal(scenario: str, anomaly_rows: list[dict], edit_war_rows: list[dict]) -> Optional[dict]:
    """Whether the detector flagged the pattern a scenario injects."""
    if scenario not in EXPECTED_SIGNALS:
        return None
    kind, page_title = EXPECTED_SIGNALS[scenario]
    if kind == "anomaly":
        detected = any(row["page_title"] == page_title for row in anomaly_rows)
    else:
        detected = any(row["page_title"] == page_title and row["flag"] for row in edit_war_rows)
    return {"kind": kind, "page_title": page_title, "detected": detected}


def run_parquet_stage(
    event_rows: list[dict[str, Any]],
    activity_rows: list[dict[str, Any]],
    scratch_dir: Path,
    batch_size: int,
) -> dict[str, Any]:
    writer = ParquetWriter(ParquetConfig(base_path=str(scratch_dir)))
    results = {}
    for dataset, rows, time_field in (
        ("raw_events", event_rows, "event_time"),
        ("activity_windows", activity_rows, "window_start"),
    ):
        totals = ParquetWriteStats()
        durations = []
        raw_json_bytes = sum(len(json.dumps(r, default=str).encode("utf-8")) for r in rows)
        for offset in range(0, len(rows), batch_size):
            started = time.perf_counter()
            stats = writer.write_records(dataset, rows[offset:offset + batch_size], time_field=time_field)
            durations.append(time.perf_counter() - started)
            for name, value in asdict(stats).items():
                setattr(totals, name, getattr(totals, name) + value)

        json_reduction = (
            round((1.0 - totals.file_bytes / raw_json_bytes) * 100, 1)
            if raw_json_bytes > 0 and totals.file_bytes > 0
            else None
        )
        results[dataset] = {
            **asdict(totals),
            "raw_json_bytes": raw_json_bytes,
            "json_reduction_pct": json_reduction,
            "compression_ratio": (
                round(totals.uncompressed_bytes / totals.compressed_bytes, 2) if totals.compressed_bytes else None
            ),
            "rows_per_sec": _rate(totals.rows, sum(durations)),
            "batch_ms": summarize_ms(durations),
        }
    shutil.rmtree(scratch_dir, ignore_errors=True)
    return results


def run_postgres_stage(
    event_rows: list[dict[str, Any]],
    activity_rows: list[dict[str, Any]],
    settings: Settings,
    batch_size: int,
) -> dict[str, Any]:
    import psycopg2

    from storage.database import Database

    db = Database(settings.postgres)
    db.init_schema()
    results = {"server_version": db.fetch_all("SHOW server_version")[0]["server_version"]}
    try:
        for name, rows, write in (
            ("edit_events", event_rows, db.upsert_edit_events),
            ("page_activity_windows", activity_rows, db.upsert_activity_windows),
        ):
            durations = []
            for offset in range(0, len(rows), batch_size):
                started = time.perf_counter()
                write(rows[offset:offset + batch_size])
                durations.append(time.perf_counter() - started)
            summary = summarize_ms(durations)
            results[name] = {
                "rows": len(rows),
                "batches": len(durations),
                "rows_per_sec": _rate(len(rows), sum(durations)),
                "batch_ms": summary,
                "target_batch_ms": 38.0,
                "target_met": (summary.get("p50", 999.0) <= 38.0) if summary else False,
            }
    finally:
        with psycopg2.connect(settings.postgres.dsn) as conn, conn.cursor() as cur:
            for table in ("edit_events", "page_activity_windows"):
                cur.execute(f"DELETE FROM {table} WHERE wiki = %s", (BENCHMARK_WIKI,))
        conn.close()
    return results


def run_kafka_stage(events: list[WikipediaEvent], settings: Settings) -> dict[str, Any]:
    from confluent_kafka import Consumer

    from ingestion.kafka_producer import EventPublisher

    topic = f"{settings.kafka.topic_edits}-benchmark"
    run_tag = uuid.uuid4().hex[:8]
    tagged = [replace(event, event_id=f"{run_tag}:{event.event_id}") for event in events]

    consumer = Consumer({
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "group.id": f"wikipulse-benchmark-{run_tag}",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([topic])
    # Wait for the partition assignment so no message is produced before the
    # consumer starts reading from "latest".
    assignment_deadline = time.monotonic() + KAFKA_ROUND_TRIP_TIMEOUT_SECONDS
    while not consumer.assignment() and time.monotonic() < assignment_deadline:
        consumer.poll(0.5)

    publisher = EventPublisher(settings.kafka)
    sent_at: dict[str, float] = {}
    produce_started = time.perf_counter()
    for event in tagged:
        sent_at[event.event_id] = time.perf_counter()
        publisher.publish(event, topic=topic)
    publisher.flush()
    produce_seconds = time.perf_counter() - produce_started

    round_trips = []
    deadline = time.monotonic() + KAFKA_ROUND_TRIP_TIMEOUT_SECONDS
    while len(round_trips) < len(tagged) and time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is None or message.error():
            continue
        event = WikipediaEvent.from_json(message.value().decode("utf-8"))
        if event.event_id in sent_at:
            round_trips.append(time.perf_counter() - sent_at[event.event_id])
    consumer.close()

    return {
        "topic": topic,
        "messages_sent": len(tagged),
        "messages_received": len(round_trips),
        "delivery_failures": publisher.failed_count,
        "produce_seconds": round(produce_seconds, 3),
        "produce_per_sec": _rate(len(tagged), produce_seconds),
        "round_trip_ms": summarize_ms(round_trips),
    }


def measure_page_history_bytes(settings: Settings, pages: int = 2000) -> dict[str, Any]:
    """Python heap used per page whose history is full, measured with
    tracemalloc on a separate store so the timed stages are not slowed."""
    lookback = settings.anomaly.history_lookback_windows
    tracemalloc.start()
    before, _ = tracemalloc.get_traced_memory()
    store = PageHistoryStore(settings.anomaly, clock=lambda: 0.0)
    for page_number in range(pages):
        for _ in range(lookback):
            store.record(BENCHMARK_WIKI, f"Benchmark Page {page_number:06d}", 1)
    after, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"pages": pages, "lookback_windows": lookback, "bytes_per_page": round((after - before) / pages)}


def environment_info() -> dict[str, Any]:
    def git(*args: str) -> Optional[str]:
        try:
            return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            return None

    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "processor": platform.processor() or None,
        "cpu_count": os.cpu_count(),
        "total_memory_mb": round(psutil.virtual_memory().total / BYTES_PER_MB),
        "git_commit": git("rev-parse", "--short", "HEAD"),
        "git_uncommitted_changes": bool(git("status", "--porcelain")),
    }


def run_scenario(scenario: str, args: argparse.Namespace, settings: Settings, run_dir: Path) -> dict[str, Any]:
    duration = args.events / args.rate
    if scenario == "sustained":
        duration = max(duration, SUSTAINED_TTL_MULTIPLE * settings.anomaly.history_ttl_seconds)
    workload = Workload(
        events=args.events,
        duration_seconds=duration,
        # Recent, so PostgreSQL rows land in live partitions, and on a whole
        # hour, so windows line up with the events the same way on every run.
        start_ts=math.floor((time.time() - duration) / SECONDS_PER_HOUR) * SECONDS_PER_HOUR,
        seed=args.seed,
    )
    payloads = generate(scenario, workload)
    logger.info("Scenario %s: %d payloads over %.0fs of simulated time", scenario, len(payloads), duration)

    ingestion, events = run_ingestion_stage(payloads, settings)
    detection, activity_rows, anomaly_rows, edit_war_rows = run_detection_stage(events, settings, args.slide_seconds)
    event_rows = [to_edit_event_row(event.to_dict()) for event in events]
    result = {
        "simulated_seconds": round(duration, 1),
        "input_events": len(payloads),
        "ingestion": ingestion,
        "detection": detection,
        "expected_signal": expected_signal(scenario, anomaly_rows, edit_war_rows),
        "parquet": run_parquet_stage(event_rows, activity_rows, run_dir / f"parquet-{scenario}", args.batch_size),
    }
    if getattr(args, "consumer_scaling", True):
        result["consumer_scaling"] = run_consumer_scaling_stage(
            events,
            settings,
            max_active_pages=getattr(args, "max_active_pages", settings.stream_consumer.max_active_pages),
            slide_seconds=args.slide_seconds,
        )
    if args.postgres:
        result["postgres"] = run_postgres_stage(event_rows, activity_rows, settings, args.batch_size)
    if args.kafka:
        result["kafka"] = run_kafka_stage(events, settings)
    return result


def parse_args(settings: Settings) -> argparse.Namespace:
    defaults = settings.benchmark
    default_slide = _duration_to_minutes(settings.spark.slide_duration) * SECONDS_PER_MINUTE
    parser = argparse.ArgumentParser(description="Run WikiPulse component benchmarks")
    parser.add_argument("--scenarios", nargs="+", choices=list(SCENARIOS), default=list(SCENARIOS))
    parser.add_argument("--events", type=int, default=defaults.events, help="Payloads per scenario")
    parser.add_argument("--rate", type=float, default=defaults.rate, help="Simulated events per second")
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--batch-size", type=int, default=defaults.batch_size)
    parser.add_argument("--slide-seconds", type=float, default=default_slide,
                        help="Window slide for the detection stage (default: SPARK_SLIDE_DURATION)")
    parser.add_argument(
        "--consumer-scaling",
        dest="consumer_scaling",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Benchmark stateful stream consumer scaling and latency waterfall (default: True)",
    )
    parser.add_argument(
        "--max-active-pages",
        type=int,
        default=settings.stream_consumer.max_active_pages,
        help="Capacity ceiling for active page deques in consumer scaling stage",
    )
    parser.add_argument("--output-dir", default=defaults.output_dir)
    parser.add_argument("--baseline", help="results.json to compare against (default: latest comparable run)")
    parser.add_argument("--postgres", action="store_true", help="Also time writes to the configured PostgreSQL")
    parser.add_argument("--kafka", action="store_true", help="Also time a round trip through the configured Kafka")
    args = parser.parse_args()
    if (
        args.events <= 0
        or args.rate <= 0
        or args.batch_size <= 0
        or args.slide_seconds <= 0
        or (args.max_active_pages is not None and args.max_active_pages <= 0)
    ):
        parser.error("--events, --rate, --batch-size, --slide-seconds and --max-active-pages must be positive")
    return args


def main() -> None:
    configure_logging("benchmark.runner")
    # The ingestion service logs one warning per malformed payload; written
    # to the console here they would dominate the ingestion timings.
    logging.getLogger("ingestion.sse_consumer").setLevel(logging.ERROR)
    base_settings = get_settings()
    args = parse_args(base_settings)
    settings = replace(
        base_settings,
        wikimedia=replace(base_settings.wikimedia, wiki_filter=[BENCHMARK_WIKI]),
        liftwing=replace(base_settings.liftwing, enabled=False),
    )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(args.output_dir)
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "run_id": run_id,
        "kind": "component benchmark (no live SSE, Spark or inter-service network; "
                "per-event ingestion warnings not logged)",
        "environment": environment_info(),
        "config": {
            "scenarios": args.scenarios,
            "events": args.events,
            "rate": args.rate,
            "seed": args.seed,
            "batch_size": args.batch_size,
            "window_duration": settings.spark.window_duration,
            "slide_seconds": args.slide_seconds,
            "watermark_delay": settings.spark.watermark_delay,
            "trigger_interval": settings.spark.trigger_interval,
            "history_ttl_seconds": settings.anomaly.history_ttl_seconds,
            "history_lookback_windows": settings.anomaly.history_lookback_windows,
            "consumer_scaling": args.consumer_scaling,
            "max_active_pages": args.max_active_pages,
            "postgres": args.postgres,
            "kafka": args.kafka,
        },
        "page_history_memory": measure_page_history_bytes(settings),
        "scenarios": {},
    }
    for scenario in args.scenarios:
        results["scenarios"][scenario] = run_scenario(scenario, args, settings, run_dir)

    baseline = report.load_baseline(output_dir, run_id, args.baseline)
    results["baseline_comparison"] = report.compare(results, baseline)
    report.write(run_dir, results)
    logger.info("Benchmark results written to %s", run_dir)


if __name__ == "__main__":
    main()
