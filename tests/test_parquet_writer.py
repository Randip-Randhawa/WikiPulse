"""Tests for storage.parquet_writer: partition layout, compression and the
returned write statistics."""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow.parquet as pq

from common.config import ParquetConfig
from storage.parquet_writer import ParquetWriter


def make_rows(*timestamps: datetime) -> list[dict]:
    return [
        {"wiki": "enwiki", "event_id": str(index), "event_time": ts, "comment": "x" * 50}
        for index, ts in enumerate(timestamps)
    ]


def test_rows_are_split_into_wiki_date_hour_partitions(tmp_path):
    writer = ParquetWriter(ParquetConfig(base_path=str(tmp_path)))
    rows = make_rows(
        datetime(2026, 10, 26, 9, 15, tzinfo=timezone.utc),
        datetime(2026, 10, 26, 10, 5, tzinfo=timezone.utc),
    )

    writer.write_records("raw_events", rows, time_field="event_time")

    partitions = sorted(p.parent.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.parquet"))
    assert partitions == [
        "raw_events/wiki=enwiki/date=2026-10-26/hour=09",
        "raw_events/wiki=enwiki/date=2026-10-26/hour=10",
    ]


def test_naive_timestamps_are_partitioned_as_utc(tmp_path):
    writer = ParquetWriter(ParquetConfig(base_path=str(tmp_path)))

    writer.write_records("raw_events", make_rows(datetime(2026, 10, 26, 23, 30)), time_field="event_time")

    [file_path] = tmp_path.rglob("*.parquet")
    assert "date=2026-10-26/hour=23" in file_path.as_posix()


def test_files_are_snappy_compressed(tmp_path):
    writer = ParquetWriter(ParquetConfig(base_path=str(tmp_path)))

    writer.write_records("raw_events", make_rows(datetime(2026, 10, 26, tzinfo=timezone.utc)), "event_time")

    [file_path] = tmp_path.rglob("*.parquet")
    assert pq.read_metadata(file_path).row_group(0).column(0).compression == "SNAPPY"


def test_write_stats_count_rows_files_and_bytes(tmp_path):
    writer = ParquetWriter(ParquetConfig(base_path=str(tmp_path)))
    rows = make_rows(*[datetime(2026, 10, 26, 9, minute, tzinfo=timezone.utc) for minute in range(20)])

    stats = writer.write_records("raw_events", rows, time_field="event_time")

    [file_path] = tmp_path.rglob("*.parquet")
    assert stats.rows == 20
    assert stats.files == 1
    assert stats.file_bytes == file_path.stat().st_size
    assert 0 < stats.compressed_bytes <= stats.uncompressed_bytes


def test_writing_no_rows_creates_nothing(tmp_path):
    writer = ParquetWriter(ParquetConfig(base_path=str(tmp_path)))

    stats = writer.write_records("raw_events", [], time_field="event_time")

    assert stats.rows == 0
    assert list(tmp_path.rglob("*.parquet")) == []
