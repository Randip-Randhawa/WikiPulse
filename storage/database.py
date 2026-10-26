"""PostgreSQL access layer.

Used by:
- `scripts/init_db.py` and `scripts/maintain_partitions.py` to apply
  schema.sql, manage daily partitions and enforce retention.
- `processing/streaming_job.py` (via `foreachBatch`) to write aggregation,
  anomaly, and edit-war results.
- `dashboard/app.py` for read queries (also see sql/*.sql for the canonical
  analytical queries this module doesn't wrap directly).

All writes are batched (executemany) rather than row-by-row to keep the
Spark `foreachBatch` sink efficient, and `is_bot`/nullable columns are
handled defensively since upstream data may be incomplete.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from common.config import PostgresConfig
from storage.partitions import (
    PARTITIONED_TABLES,
    day_bounds,
    days_to_cover,
    default_partition_name,
    expired_days,
    partition_day,
    partition_name,
    retention_cutoff,
)

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

# Every process that changes the schema or its partitions takes this
# advisory lock first, so the processing job and the maintenance service
# never create the same partition at the same time. The value is arbitrary.
SCHEMA_LOCK_KEY = 7_204_001

# Table -> timestamp column compared against the retention cutoff.
RETENTION_COLUMNS = {
    "edit_events": "event_time",
    "page_activity_windows": "window_start",
    "anomaly_events": "window_start",
    "edit_war_signals": "window_start",
    "user_aggregates": "window_start",
    "revision_quality_scores": "fetched_at",
    "pipeline_metrics": "recorded_at",
}


@dataclass(frozen=True)
class RetentionResult:
    cutoff: datetime
    dropped_partitions: list[str]
    deleted_rows: dict[str, int]


def _legacy_table_name(table: str) -> str:
    return f"{table}_unpartitioned"


class Database:
    """Thin connection manager + batch-write helpers around psycopg2."""

    def __init__(self, config: PostgresConfig):
        self._config = config

    @contextmanager
    def _connect(self) -> Iterator[psycopg2.extensions.connection]:
        conn = psycopg2.connect(self._config.dsn)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------
    def init_schema(self, now: Optional[datetime] = None) -> None:
        """Apply schema.sql and create any missing daily partitions.

        Idempotent, and safe to run from several processes at once. A
        database created before partitioning was introduced is converted
        in the same transaction: each old table is renamed, its partitioned
        replacement created, and every row copied across. Rows older than
        the retention window land in the default partition and are removed
        by the next `purge_old_data`.
        """
        now = now or datetime.now(timezone.utc)
        schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_KEY,))
                legacy_tables = self._rename_unpartitioned_tables(cur)
                cur.execute(schema_sql)
                created = self._create_missing_partitions(cur, now)
                for table in legacy_tables:
                    self._copy_rows_from_legacy_table(cur, table)
        if created:
            logger.info("Created partitions: %s", ", ".join(created))
        logger.info("Schema applied successfully")

    def _rename_unpartitioned_tables(self, cur) -> list[str]:
        """Move pre-partitioning versions of the partitioned tables, with
        their indexes and id sequences, out of the way so schema.sql can
        create the partitioned tables under the original names."""
        renamed = []
        for table in PARTITIONED_TABLES:
            cur.execute("SELECT relkind FROM pg_class WHERE oid = to_regclass(%s)", (table,))
            row = cur.fetchone()
            # 'r' is an ordinary table; 'p' means it is already partitioned.
            if row is None or row[0] != "r":
                continue

            legacy = _legacy_table_name(table)
            logger.warning("Converting %s to a partitioned table (old table kept as %s until copied)",
                           table, legacy)
            cur.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(table), sql.Identifier(legacy)))
            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() AND tablename = %s",
                (legacy,),
            )
            for number, (index_name,) in enumerate(cur.fetchall()):
                cur.execute(sql.SQL("ALTER INDEX {} RENAME TO {}").format(
                    sql.Identifier(index_name), sql.Identifier(f"{legacy}_idx{number}")))
            cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (legacy,))
            sequence = cur.fetchone()[0]
            # pg_get_serial_sequence returns an already quoted, qualified name.
            cur.execute(sql.SQL("ALTER SEQUENCE {} RENAME TO {}").format(
                sql.SQL(sequence), sql.Identifier(f"{legacy}_id_seq")))
            renamed.append(table)
        return renamed

    def _copy_rows_from_legacy_table(self, cur, table: str) -> None:
        legacy = _legacy_table_name(table)
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            ORDER BY ordinal_position
            """,
            (legacy,),
        )
        columns = sql.SQL(", ").join(sql.Identifier(name) for (name,) in cur.fetchall())
        cur.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
            sql.Identifier(table), columns, columns, sql.Identifier(legacy)))
        copied = cur.rowcount
        # Continue the id sequence after the copied ids.
        cur.execute(
            sql.SQL(
                "SELECT setval(pg_get_serial_sequence(%s, 'id'), (SELECT COALESCE(MAX(id), 0) + 1 FROM {}), false)"
            ).format(sql.Identifier(table)),
            (table,),
        )
        cur.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(legacy)))
        logger.info("Copied %d rows from %s into partitioned %s and dropped %s", copied, legacy, table, legacy)

    def _existing_partition_days(self, cur, table: str) -> dict[date, str]:
        """Map each day that has a daily partition of `table` to its name."""
        cur.execute(
            """
            SELECT child.relname
            FROM pg_inherits
            JOIN pg_class child ON child.oid = pg_inherits.inhrelid
            WHERE pg_inherits.inhparent = to_regclass(%s)
            """,
            (table,),
        )
        partitions = {}
        for (name,) in cur.fetchall():
            day = partition_day(table, name)
            if day is not None:
                partitions[day] = name
        return partitions

    def _create_missing_partitions(self, cur, now: datetime) -> list[str]:
        created = []
        wanted_days = days_to_cover(now, self._config.retention_hours, self._config.partition_horizon_days)
        for table, column in PARTITIONED_TABLES.items():
            existing = self._existing_partition_days(cur, table)
            for day in wanted_days:
                if day in existing:
                    continue
                self._create_partition(cur, table, column, day)
                created.append(partition_name(table, day))
        return created

    def _create_partition(self, cur, table: str, column: str, day: date) -> None:
        """Create the daily partition of `table` for `day`.

        Rows for that day may already sit in the default partition (late
        events, or a gap in maintenance). PostgreSQL refuses to attach a
        partition whose range still has rows in the default partition, so
        they are moved into the new table before it is attached.
        """
        start, end = day_bounds(day)
        partition = sql.Identifier(partition_name(table, day))
        cur.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)").format(
            partition, sql.Identifier(table)))
        cur.execute(
            sql.SQL(
                "WITH moved AS (DELETE FROM {default} WHERE {column} >= %s AND {column} < %s RETURNING *) "
                "INSERT INTO {partition} SELECT * FROM moved"
            ).format(
                default=sql.Identifier(default_partition_name(table)),
                column=sql.Identifier(column),
                partition=partition,
            ),
            (start, end),
        )
        if cur.rowcount:
            logger.info("Moved %d rows from %s into %s",
                        cur.rowcount, default_partition_name(table), partition_name(table, day))
        cur.execute(
            sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES FROM (%s) TO (%s)").format(
                sql.Identifier(table), partition),
            (start, end),
        )

    # ------------------------------------------------------------------
    # Batch writes (used by the Spark foreachBatch sink)
    # ------------------------------------------------------------------
    def upsert_edit_events(self, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        query = """
            INSERT INTO edit_events (
                event_id, event_time, wiki, event_type, page_title, page_id,
                namespace, revision_id, previous_revision_id, "user", user_id,
                is_bot, comment, byte_length_new, byte_length_old,
                byte_length_delta, is_revert, revert_target_revision_id,
                ingestion_time, kafka_partition, kafka_offset
            ) VALUES (
                %(event_id)s, %(event_time)s, %(wiki)s, %(event_type)s, %(page_title)s,
                %(page_id)s, %(namespace)s, %(revision_id)s, %(previous_revision_id)s,
                %(user)s, %(user_id)s, %(is_bot)s, %(comment)s, %(byte_length_new)s,
                %(byte_length_old)s, %(byte_length_delta)s, %(is_revert)s,
                %(revert_target_revision_id)s, %(ingestion_time)s, %(kafka_partition)s,
                %(kafka_offset)s
            )
            ON CONFLICT (event_id, event_time) DO NOTHING
        """
        self._executemany(query, rows)

    def upsert_activity_windows(self, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        query = """
            INSERT INTO page_activity_windows (
                wiki, page_id, page_title, window_start, window_end, edit_count,
                unique_editors, human_edits, bot_edits, revert_count,
                net_byte_delta, edit_velocity
            ) VALUES (
                %(wiki)s, %(page_id)s, %(page_title)s, %(window_start)s, %(window_end)s,
                %(edit_count)s, %(unique_editors)s, %(human_edits)s, %(bot_edits)s,
                %(revert_count)s, %(net_byte_delta)s, %(edit_velocity)s
            )
            ON CONFLICT (wiki, page_title, window_start, window_end) DO UPDATE SET
                edit_count = EXCLUDED.edit_count,
                unique_editors = EXCLUDED.unique_editors,
                human_edits = EXCLUDED.human_edits,
                bot_edits = EXCLUDED.bot_edits,
                revert_count = EXCLUDED.revert_count,
                net_byte_delta = EXCLUDED.net_byte_delta,
                edit_velocity = EXCLUDED.edit_velocity
        """
        self._executemany(query, rows)

    def insert_anomaly_events(self, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        query = """
            INSERT INTO anomaly_events (
                wiki, page_id, page_title, window_start, window_end, edit_count,
                unique_editors, human_edits, bot_edits, baseline_median,
                baseline_mad, anomaly_score, anomaly_type, threshold_used
            ) VALUES (
                %(wiki)s, %(page_id)s, %(page_title)s, %(window_start)s, %(window_end)s,
                %(edit_count)s, %(unique_editors)s, %(human_edits)s, %(bot_edits)s,
                %(baseline_median)s, %(baseline_mad)s, %(anomaly_score)s,
                %(anomaly_type)s, %(threshold_used)s
            )
            ON CONFLICT (wiki, page_title, window_start, window_end) DO NOTHING
        """
        self._executemany(query, rows)

    def insert_edit_war_signals(self, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        query = """
            INSERT INTO edit_war_signals (
                wiki, page_id, page_title, window_start, window_end,
                mutual_revert_count, editor_a, editor_b, total_reverts,
                edit_burst_count, conflict_score, flag
            ) VALUES (
                %(wiki)s, %(page_id)s, %(page_title)s, %(window_start)s, %(window_end)s,
                %(mutual_revert_count)s, %(editor_a)s, %(editor_b)s, %(total_reverts)s,
                %(edit_burst_count)s, %(conflict_score)s, %(flag)s
            )
            ON CONFLICT (wiki, page_title, window_start, window_end) DO NOTHING
        """
        self._executemany(query, rows)

    def upsert_user_aggregates(self, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        query = """
            INSERT INTO user_aggregates (
                wiki, "user", is_bot, window_start, window_end, edit_count,
                revert_count, pages_touched
            ) VALUES (
                %(wiki)s, %(user)s, %(is_bot)s, %(window_start)s, %(window_end)s,
                %(edit_count)s, %(revert_count)s, %(pages_touched)s
            )
            ON CONFLICT (wiki, "user", window_start, window_end) DO UPDATE SET
                edit_count = EXCLUDED.edit_count,
                revert_count = EXCLUDED.revert_count,
                pages_touched = EXCLUDED.pages_touched
        """
        self._executemany(query, rows)

    def upsert_revision_quality_scores(self, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        query = """
            INSERT INTO revision_quality_scores (
                wiki, page_title, revision_id, model_name, score, raw_response
            ) VALUES (
                %(wiki)s, %(page_title)s, %(revision_id)s, %(model_name)s,
                %(score)s, %(raw_response)s
            )
            ON CONFLICT (wiki, revision_id, model_name) DO UPDATE SET
                score = EXCLUDED.score,
                raw_response = EXCLUDED.raw_response,
                fetched_at = now()
        """
        rows = [
            {**row, "raw_response": json.dumps(row.get("raw_response"))
             if row.get("raw_response") is not None else None}
            for row in rows
        ]
        self._executemany(query, rows)

    def record_pipeline_metrics(self, metrics: dict[str, Any]) -> None:
        query = """
            INSERT INTO pipeline_metrics (
                component, events_received, events_parsed, events_rejected,
                events_published, anomalies_detected, edit_wars_detected,
                processing_latency_ms, throughput_per_sec, last_event_time,
                error_count, details
            ) VALUES (
                %(component)s, %(events_received)s, %(events_parsed)s,
                %(events_rejected)s, %(events_published)s, %(anomalies_detected)s,
                %(edit_wars_detected)s, %(processing_latency_ms)s,
                %(throughput_per_sec)s, %(last_event_time)s, %(error_count)s,
                %(details)s
            )
        """
        details = metrics.get("details")
        self._executemany(query, [{**metrics, "details": None if details is None else json.dumps(details)}])

    def _executemany(self, query: str, rows: Sequence[dict[str, Any]]) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, query, rows, page_size=200)

    # ------------------------------------------------------------------
    # Retention / purging
    # ------------------------------------------------------------------
    def purge_old_data(self, now: Optional[datetime] = None) -> RetentionResult:
        """Remove PostgreSQL rows older than POSTGRES_RETENTION_HOURS.

        Daily partitions lying entirely before the cutoff are dropped. Rows
        older than the cutoff that remain -- in the partition holding the
        cutoff, the default partitions and the unpartitioned tables -- are
        then deleted, so the cutoff is exact. Parquet history is untouched.
        """
        now = now or datetime.now(timezone.utc)
        cutoff = retention_cutoff(now, self._config.retention_hours)
        dropped_partitions = []
        deleted_rows = {}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_KEY,))
                for table in PARTITIONED_TABLES:
                    partitions = self._existing_partition_days(cur, table)
                    for day in expired_days(partitions, cutoff):
                        cur.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(partitions[day])))
                        dropped_partitions.append(partitions[day])
                        logger.info("Dropped partition %s (older than %dh cutoff %s)",
                                    partitions[day], self._config.retention_hours, cutoff.isoformat())
                for table, column in RETENTION_COLUMNS.items():
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE {} < %s").format(
                            sql.Identifier(table), sql.Identifier(column)),
                        (cutoff,),
                    )
                    deleted_rows[table] = cur.rowcount
        logger.info("Retention purge complete (cutoff=%s): dropped %d partitions, deleted rows %s",
                    cutoff.isoformat(), len(dropped_partitions), deleted_rows)
        return RetentionResult(cutoff=cutoff, dropped_partitions=dropped_partitions, deleted_rows=deleted_rows)

    # ------------------------------------------------------------------
    # Read helpers used by the dashboard
    # ------------------------------------------------------------------
    def fetch_all(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params or ())
                return [dict(row) for row in cur.fetchall()]
