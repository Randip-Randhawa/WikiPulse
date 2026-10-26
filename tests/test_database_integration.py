"""PostgreSQL integration tests for storage.database. They need a
disposable database; see tests/conftest.py."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.conftest import run_sql

pytestmark = pytest.mark.postgres

WINDOW_START = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)


def anomaly_row(edit_count: int) -> dict:
    return {
        "wiki": "enwiki",
        "page_id": 1,
        "page_title": "Page",
        "window_start": WINDOW_START,
        "window_end": WINDOW_START + timedelta(minutes=5),
        "edit_count": edit_count,
        "unique_editors": 1,
        "human_edits": edit_count,
        "bot_edits": 0,
        "baseline_median": None,
        "baseline_mad": None,
        "anomaly_score": float(edit_count),
        "anomaly_type": "count_fallback",
        "threshold_used": 15.0,
    }


def test_replayed_anomaly_window_is_stored_once(database):
    database.init_schema()

    database.insert_anomaly_events([anomaly_row(20)])
    database.insert_anomaly_events([anomaly_row(20)])

    assert database.fetch_all("SELECT count(*) AS n FROM anomaly_events")[0]["n"] == 1


def test_schema_upgrade_keeps_the_newest_of_duplicate_anomaly_rows(database, postgres_config):
    database.init_schema()
    # Recreate the pre-upgrade state: no unique key, duplicate rows per window.
    run_sql(postgres_config, "DROP INDEX uq_anomaly_events_window")
    for edit_count in (16, 21):
        run_sql(
            postgres_config,
            """
            INSERT INTO anomaly_events (wiki, page_title, window_start, window_end, edit_count, anomaly_type)
            VALUES ('enwiki', 'Page', %s, %s, %s, 'count_fallback')
            """,
            (WINDOW_START, WINDOW_START + timedelta(minutes=5), edit_count),
        )

    database.init_schema()

    rows = database.fetch_all("SELECT edit_count FROM anomaly_events")
    assert rows == [{"edit_count": 21}]


NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)

# The two hot tables as they were defined before partitioning.
UNPARTITIONED_SCHEMA = """
CREATE TABLE edit_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    wiki TEXT NOT NULL,
    event_type TEXT NOT NULL,
    page_title TEXT NOT NULL,
    page_id BIGINT,
    namespace INTEGER,
    revision_id BIGINT,
    previous_revision_id BIGINT,
    "user" TEXT,
    user_id BIGINT,
    is_bot BOOLEAN NOT NULL DEFAULT FALSE,
    comment TEXT,
    byte_length_new INTEGER,
    byte_length_old INTEGER,
    byte_length_delta INTEGER,
    is_revert BOOLEAN NOT NULL DEFAULT FALSE,
    revert_target_revision_id BIGINT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id)
);
CREATE INDEX idx_edit_events_time ON edit_events (event_time DESC);
CREATE TABLE page_activity_windows (
    id BIGSERIAL PRIMARY KEY,
    wiki TEXT NOT NULL,
    page_id BIGINT,
    page_title TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    edit_count INTEGER NOT NULL DEFAULT 0,
    unique_editors INTEGER NOT NULL DEFAULT 0,
    human_edits INTEGER NOT NULL DEFAULT 0,
    bot_edits INTEGER NOT NULL DEFAULT 0,
    revert_count INTEGER NOT NULL DEFAULT 0,
    net_byte_delta BIGINT NOT NULL DEFAULT 0,
    edit_velocity DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wiki, page_title, window_start, window_end)
);
"""


def edit_event(event_id: str, event_time: datetime) -> dict:
    return {
        "event_id": event_id,
        "event_time": event_time,
        "wiki": "enwiki",
        "event_type": "edit",
        "page_title": "Page",
        "page_id": 1,
        "namespace": 0,
        "revision_id": 1,
        "previous_revision_id": None,
        "user": "Alice",
        "user_id": 1,
        "is_bot": False,
        "comment": None,
        "byte_length_new": None,
        "byte_length_old": None,
        "byte_length_delta": None,
        "is_revert": False,
        "revert_target_revision_id": None,
        "ingestion_time": event_time,
        "kafka_partition": 0,
        "kafka_offset": 1,
    }


def partition_names(database, table: str) -> list[str]:
    rows = database.fetch_all(
        """
        SELECT child.relname AS name FROM pg_inherits
        JOIN pg_class child ON child.oid = pg_inherits.inhrelid
        WHERE pg_inherits.inhparent = %s::regclass
        ORDER BY child.relname
        """,
        (table,),
    )
    return [row["name"] for row in rows]


def partition_of(database, event_id: str) -> str:
    rows = database.fetch_all(
        "SELECT tableoid::regclass::text AS partition FROM edit_events WHERE event_id = %s", (event_id,))
    return rows[0]["partition"]


def test_init_schema_creates_daily_partitions_from_cutoff_day_through_horizon(database):
    database.init_schema(now=NOW)

    names = partition_names(database, "edit_events")

    assert names[0] == "edit_events_default"
    assert names[1] == "edit_events_p20261017"
    assert names[-1] == "edit_events_p20261023"
    assert len(names) == 1 + 7


def test_running_init_schema_again_creates_no_extra_partitions(database):
    database.init_schema(now=NOW)
    first = partition_names(database, "page_activity_windows")

    database.init_schema(now=NOW)

    assert partition_names(database, "page_activity_windows") == first


def test_init_schema_after_a_gap_adds_only_the_missing_days(database):
    database.init_schema(now=NOW)

    database.init_schema(now=NOW + timedelta(days=2))

    names = partition_names(database, "edit_events")
    assert "edit_events_p20261025" in names
    assert len(names) == len(set(names))


def test_event_is_stored_in_its_daily_partition(database):
    database.init_schema(now=NOW)

    database.upsert_edit_events([edit_event("e1", NOW)])

    assert partition_of(database, "e1") == "edit_events_p20261020"


def test_replayed_event_is_stored_once(database):
    database.init_schema(now=NOW)

    database.upsert_edit_events([edit_event("e1", NOW)])
    database.upsert_edit_events([edit_event("e1", NOW)])

    assert database.fetch_all("SELECT count(*) AS n FROM edit_events")[0]["n"] == 1


def test_event_beyond_the_horizon_waits_in_default_then_moves_to_its_new_partition(database):
    database.init_schema(now=NOW)
    far_future = NOW + timedelta(days=10)
    database.upsert_edit_events([edit_event("future", far_future)])
    assert partition_of(database, "future") == "edit_events_default"

    database.init_schema(now=far_future)

    assert partition_of(database, "future") == "edit_events_p20261030"


def test_purge_drops_whole_expired_partitions_and_trims_the_cutoff_partition(database):
    database.init_schema(now=NOW)
    database.upsert_edit_events([
        edit_event("old", datetime(2026, 10, 18, 6, 0, tzinfo=timezone.utc)),
        edit_event("just_expired", datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)),
        edit_event("kept", datetime(2026, 10, 20, 11, 0, tzinfo=timezone.utc)),
    ])

    # 72 hours before 2026-10-23T10:00Z is 2026-10-20T10:00Z.
    result = database.purge_old_data(now=datetime(2026, 10, 23, 10, 0, tzinfo=timezone.utc))

    assert result.dropped_partitions == [
        "edit_events_p20261017", "edit_events_p20261018", "edit_events_p20261019",
        "page_activity_windows_p20261017", "page_activity_windows_p20261018",
        "page_activity_windows_p20261019",
    ]
    assert result.deleted_rows["edit_events"] == 1
    assert database.fetch_all("SELECT event_id FROM edit_events") == [{"event_id": "kept"}]


def test_purge_never_drops_the_default_partition(database):
    database.init_schema(now=NOW)

    database.purge_old_data(now=NOW + timedelta(days=30))

    assert "edit_events_default" in partition_names(database, "edit_events")


def test_purge_deletes_only_expired_rows_from_unpartitioned_tables(database):
    database.init_schema(now=NOW)
    old_start = NOW - timedelta(hours=80)
    recent_start = NOW - timedelta(hours=1)
    database.insert_anomaly_events([
        {**anomaly_row(20), "window_start": old_start, "window_end": old_start + timedelta(minutes=5)},
        {**anomaly_row(20), "window_start": recent_start, "window_end": recent_start + timedelta(minutes=5)},
    ])

    result = database.purge_old_data(now=NOW)

    assert result.deleted_rows["anomaly_events"] == 1
    assert database.fetch_all("SELECT count(*) AS n FROM anomaly_events")[0]["n"] == 1


def test_unpartitioned_tables_from_an_older_install_are_converted_with_their_rows(database, postgres_config):
    run_sql(postgres_config, UNPARTITIONED_SCHEMA)
    run_sql(
        postgres_config,
        "INSERT INTO edit_events (event_id, event_time, wiki, event_type, page_title) VALUES "
        "('recent', %s, 'enwiki', 'edit', 'Page'), ('expired', %s, 'enwiki', 'edit', 'Page')",
        (NOW - timedelta(hours=1), NOW - timedelta(days=10)),
    )

    database.init_schema(now=NOW)

    kinds = database.fetch_all(
        "SELECT relname, relkind::text AS kind FROM pg_class "
        "WHERE relname IN ('edit_events', 'page_activity_windows') ORDER BY relname")
    assert kinds == [{"relname": "edit_events", "kind": "p"}, {"relname": "page_activity_windows", "kind": "p"}]
    assert partition_of(database, "recent") == "edit_events_p20261020"
    # Older than the retention window: kept until the next purge removes it.
    assert partition_of(database, "expired") == "edit_events_default"
    assert database.fetch_all("SELECT to_regclass('edit_events_unpartitioned') AS t")[0]["t"] is None


def test_ids_continue_after_converting_an_older_install(database, postgres_config):
    run_sql(postgres_config, UNPARTITIONED_SCHEMA)
    run_sql(
        postgres_config,
        "INSERT INTO edit_events (event_id, event_time, wiki, event_type, page_title) "
        "VALUES ('converted', %s, 'enwiki', 'edit', 'Page')",
        (NOW,),
    )
    database.init_schema(now=NOW)

    database.upsert_edit_events([edit_event("new", NOW)])

    rows = database.fetch_all("SELECT event_id FROM edit_events ORDER BY id")
    assert [row["event_id"] for row in rows] == ["converted", "new"]


def test_pipeline_metrics_details_are_stored_as_json(database):
    database.init_schema(now=NOW)

    database.record_pipeline_metrics({
        "component": "spark", "events_received": 5, "events_parsed": 5, "events_rejected": 0,
        "events_published": 5, "anomalies_detected": 0, "edit_wars_detected": 0,
        "processing_latency_ms": 900.0, "throughput_per_sec": 1.0, "last_event_time": NOW, "error_count": 0,
        "details": {"raw_batch": {"end_to_end_latency_ms": {"p95": 1500.0}}},
    })

    rows = database.fetch_all(
        "SELECT (details -> 'raw_batch' -> 'end_to_end_latency_ms' ->> 'p95')::float AS p95 FROM pipeline_metrics")
    assert rows == [{"p95": 1500.0}]
