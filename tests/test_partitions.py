"""Tests for storage.partitions: partition naming and the date arithmetic
behind partition creation and retention."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from pathlib import Path

from storage.partitions import (
    PartitionHealthReport,
    days_to_cover,
    expired_days,
    is_partition_archived_in_lake,
    partition_day,
    partition_name,
    retention_cutoff,
)

UTC = timezone.utc


class TestPartitionNames:
    def test_partition_name_encodes_the_day(self):
        assert partition_name("edit_events", date(2026, 10, 26)) == "edit_events_p20261026"

    def test_partition_day_reads_back_the_day(self):
        assert partition_day("edit_events", "edit_events_p20261026") == date(2026, 10, 26)

    def test_default_partition_has_no_day(self):
        assert partition_day("edit_events", "edit_events_default") is None

    def test_partition_of_another_table_has_no_day(self):
        assert partition_day("edit_events", "page_activity_windows_p20261026") is None


class TestDaysToCover:
    def test_covers_the_cutoff_day_through_the_horizon(self):
        now = datetime(2026, 10, 26, 10, 0, tzinfo=UTC)

        days = days_to_cover(now, retention_hours=72, horizon_days=3)

        assert days[0] == date(2026, 10, 23)
        assert days[-1] == date(2026, 10, 29)
        assert len(days) == 7

    def test_days_are_utc_days_even_when_now_has_another_offset(self):
        # 01:00 on the 27th in India is still the 26th in UTC.
        now = datetime(2026, 10, 27, 1, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))

        days = days_to_cover(now, retention_hours=24, horizon_days=1)

        assert days == [date(2026, 10, 25), date(2026, 10, 26), date(2026, 10, 27)]


class TestExpiredDays:
    def test_partition_holding_the_cutoff_is_kept(self):
        cutoff = retention_cutoff(datetime(2026, 10, 26, 10, 0, tzinfo=UTC), retention_hours=72)

        expired = expired_days([date(2026, 10, 22), date(2026, 10, 23), date(2026, 10, 24)], cutoff)

        assert expired == [date(2026, 10, 22)]

    def test_partition_ending_exactly_at_the_cutoff_is_expired(self):
        cutoff = datetime(2026, 10, 23, 0, 0, tzinfo=UTC)

        assert expired_days([date(2026, 10, 22)], cutoff) == [date(2026, 10, 22)]

    def test_nothing_expires_when_every_partition_is_recent(self):
        cutoff = datetime(2026, 10, 23, 10, 0, tzinfo=UTC)

        assert expired_days([date(2026, 10, 24), date(2026, 10, 25)], cutoff) == []


class TestPartitionLakeArchive:
    def test_missing_lake_directory_returns_false(self, tmp_path):
        assert is_partition_archived_in_lake(tmp_path / "nonexistent", "edit_events", date(2026, 10, 26)) is False

    def test_empty_lake_directory_returns_false(self, tmp_path):
        lake_dir = tmp_path / "lake"
        lake_dir.mkdir()
        assert is_partition_archived_in_lake(lake_dir, "edit_events", date(2026, 10, 26)) is False

    def test_hierarchical_lake_layout_detected(self, tmp_path):
        lake_dir = tmp_path / "lake"
        part_dir = lake_dir / "wiki=enwiki" / "year=2026" / "month=10" / "day=26" / "hour=14"
        part_dir.mkdir(parents=True)
        (part_dir / "data.parquet").write_bytes(b"PAR1")

        assert is_partition_archived_in_lake(lake_dir, "edit_events", date(2026, 10, 26)) is True
        assert is_partition_archived_in_lake(lake_dir, "edit_events", date(2026, 10, 27)) is False

    def test_flat_date_dataset_layout_detected(self, tmp_path):
        lake_dir = tmp_path / "lake"
        part_dir = lake_dir / "raw_events" / "wiki=enwiki" / "date=2026-10-26"
        part_dir.mkdir(parents=True)
        (part_dir / "data.parquet").write_bytes(b"PAR1")

        assert is_partition_archived_in_lake(lake_dir, "edit_events", date(2026, 10, 26)) is True
        assert is_partition_archived_in_lake(lake_dir, "edit_events", date(2026, 10, 25)) is False


class TestPartitionHealthReport:
    def test_health_report_status_mapping(self):
        healthy_rep = PartitionHealthReport(
            healthy=True,
            status="OK",
            default_partition_rows={"edit_events_default": 0},
            missing_partitions={"edit_events": []},
            active_partitions={"edit_events": ["edit_events_p20261026"]},
            total_default_rows=0,
        )
        assert healthy_rep.healthy is True
        assert healthy_rep.status == "OK"

        crit_rep = PartitionHealthReport(
            healthy=False,
            status="CRITICAL",
            default_partition_rows={"edit_events_default": 5},
            missing_partitions={"edit_events": []},
            active_partitions={"edit_events": ["edit_events_p20261026"]},
            total_default_rows=5,
        )
        assert crit_rep.healthy is False
        assert crit_rep.status == "CRITICAL"
