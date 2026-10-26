"""Daily range partitions for the high-volume PostgreSQL tables.

`edit_events` gets a row per edit and `page_activity_windows` a row per page
per window, so both are partitioned by UTC day on their time column. Expired
days can then be removed with a cheap DROP TABLE instead of a large DELETE.
The other tables receive a few rows per minute and stay unpartitioned.

Partitions are named `<table>_pYYYYMMDD` and cover [day 00:00Z, next day
00:00Z). Each partitioned table also has a `<table>_default` partition that
catches rows outside every daily range (very late or far-future events) so
an insert never fails for want of a partition.

This module only does the date arithmetic; storage.database runs the SQL.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, Optional

# Partitioned table -> the timestamp column it is partitioned on.
PARTITIONED_TABLES = {
    "edit_events": "event_time",
    "page_activity_windows": "window_start",
}

_PARTITION_SUFFIX_RE = re.compile(r"_p(\d{8})$")


def partition_name(table: str, day: date) -> str:
    return f"{table}_p{day:%Y%m%d}"


def default_partition_name(table: str) -> str:
    return f"{table}_default"


def partition_day(table: str, name: str) -> Optional[date]:
    """Return the day a daily partition of `table` covers, or None if
    `name` is not one (for example the default partition)."""
    if not name.startswith(f"{table}_p"):
        return None
    match = _PARTITION_SUFFIX_RE.search(name)
    if match is None:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d").date()


def day_bounds(day: date) -> tuple[datetime, datetime]:
    """Return the [start, end) UTC range of a daily partition."""
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def retention_cutoff(now: datetime, retention_hours: int) -> datetime:
    return now - timedelta(hours=retention_hours)


def days_to_cover(now: datetime, retention_hours: int, horizon_days: int) -> list[date]:
    """Days that should have a partition: from the day holding the retention
    cutoff (late events may still land there) through `horizon_days` after
    today, in UTC."""
    first_day = retention_cutoff(now, retention_hours).astimezone(timezone.utc).date()
    last_day = now.astimezone(timezone.utc).date() + timedelta(days=horizon_days)
    day_count = (last_day - first_day).days + 1
    return [first_day + timedelta(days=offset) for offset in range(day_count)]


def expired_days(days: Iterable[date], cutoff: datetime) -> list[date]:
    """Days whose whole partition lies before `cutoff` and can be dropped.

    The partition holding the cutoff itself is kept; its expired rows are
    deleted individually so the retention period is exact."""
    return sorted(day for day in days if day_bounds(day)[1] <= cutoff)
