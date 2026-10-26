"""Runs the windowed streaming query on a local Spark session to check that
each (page, window) reaches the batch writer exactly once, with its final
contents.

Needs pyspark and a Java runtime Spark 3.5 supports (8, 11 or 17). The
tests are skipped when a local Spark session cannot be started.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.config import SparkConfig

pyspark_sql = pytest.importorskip("pyspark.sql")

from processing.streaming_job import (  # noqa: E402
    build_windowed_aggregation,
    parse_events,
    start_windowed_query,
)

pytestmark = pytest.mark.spark

# 2026-01-01T00:00:00Z, a whole number of minutes so window starts line up.
BASE_TS = 1_767_225_600


@pytest.fixture(scope="module")
def spark():
    try:
        session = (
            pyspark_sql.SparkSession.builder.master("local[2]")
            .appName("wikipulse-window-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # JVM missing or unsupported Java version
        pytest.skip(f"could not start a local Spark session: {exc}")
    yield session
    session.stop()


def write_events(directory: Path, file_name: str, page_title: str, offsets_seconds: list[int]) -> None:
    lines = []
    for offset in offsets_seconds:
        lines.append(json.dumps({
            "event_id": f"{page_title}-{offset}",
            "timestamp": BASE_TS + offset,
            "wiki": "enwiki",
            "event_type": "edit",
            "page_title": page_title,
            "user": "Alice",
            "revision_id": offset,
            "is_bot": False,
            "is_revert": False,
            "byte_length_delta": 10,
        }))
    (directory / file_name).write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_each_window_is_emitted_once_with_its_final_count(spark, tmp_path):
    input_dir = tmp_path / "events"
    input_dir.mkdir()
    spark_config = SparkConfig(
        checkpoint_dir=str(tmp_path / "checkpoints"),
        window_duration="5 minutes",
        slide_duration="1 minute",
        watermark_delay="2 minutes",
        trigger_interval="0 seconds",
    )
    emitted = []

    def collect(batch_df, batch_id):
        for row in batch_df.collect():
            emitted.append((row["page_title"], row["window"]["start"], len(row["records"])))

    # Stand in for the Kafka source: one JSON event per line plus the
    # partition/offset columns parse_events expects.
    kafka_like = spark.readStream.text(str(input_dir)).selectExpr(
        "value", "0 AS partition", "CAST(0 AS BIGINT) AS offset")
    parsed = parse_events(kafka_like)
    query = start_windowed_query(build_windowed_aggregation(parsed, spark_config), collect, spark_config)
    try:
        # The first window of "Page" fills across two micro-batches.
        write_events(input_dir, "1.json", "Page", [60, 120])
        query.processAllAvailable()
        write_events(input_dir, "2.json", "Page", [180])
        query.processAllAvailable()
        # Later events move the watermark past every window of "Page"; the
        # watermark set by one batch takes effect in the next.
        write_events(input_dir, "3.json", "Later", [1800])
        query.processAllAvailable()
        write_events(input_dir, "4.json", "Later", [1860])
        query.processAllAvailable()
    finally:
        query.stop()

    page_windows = [(start, count) for page, start, count in emitted if page == "Page"]
    window_starts = [start for start, _ in page_windows]
    assert len(window_starts) == len(set(window_starts)), "a window was emitted more than once"
    # Edits at 00:01, 00:02 and 00:03 fall into the seven windows starting
    # 23:57 through 00:03; each must arrive with its complete count.
    assert sorted(count for _, count in page_windows) == [1, 1, 2, 2, 3, 3, 3]
