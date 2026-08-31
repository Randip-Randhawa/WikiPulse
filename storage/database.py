"""PostgreSQL access layer.

Used by:
- `scripts/init_db.py` to apply schema.sql.
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import psycopg2
import psycopg2.extras

from common.config import PostgresConfig

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


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
    def init_schema(self) -> None:
        """Apply schema.sql. Idempotent: uses CREATE TABLE/INDEX IF NOT EXISTS."""
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        logger.info("Schema applied successfully")

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
                byte_length_delta, is_revert, revert_target_revision_id
            ) VALUES (
                %(event_id)s, %(event_time)s, %(wiki)s, %(event_type)s, %(page_title)s,
                %(page_id)s, %(namespace)s, %(revision_id)s, %(previous_revision_id)s,
                %(user)s, %(user_id)s, %(is_bot)s, %(comment)s, %(byte_length_new)s,
                %(byte_length_old)s, %(byte_length_delta)s, %(is_revert)s,
                %(revert_target_revision_id)s
            )
            ON CONFLICT (event_id) DO NOTHING
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
                error_count
            ) VALUES (
                %(component)s, %(events_received)s, %(events_parsed)s,
                %(events_rejected)s, %(events_published)s, %(anomalies_detected)s,
                %(edit_wars_detected)s, %(processing_latency_ms)s,
                %(throughput_per_sec)s, %(last_event_time)s, %(error_count)s
            )
        """
        self._executemany(query, [metrics])

    def _executemany(self, query: str, rows: Sequence[dict[str, Any]]) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, query, rows, page_size=200)

    # ------------------------------------------------------------------
    # Retention / purging
    # ------------------------------------------------------------------
    def purge_old_data(self) -> dict[str, int]:
        """Delete rows older than `retention_days` from the high-volume,
        dashboard-facing tables. Long-term history is expected to live in
        Parquet (see storage/parquet_writer usage in processing), so
        PostgreSQL stays bounded and fast for live queries."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._config.retention_days)
        deleted = {}
        statements = {
            "edit_events": "DELETE FROM edit_events WHERE event_time < %s",
            "page_activity_windows": "DELETE FROM page_activity_windows WHERE window_start < %s",
            "anomaly_events": "DELETE FROM anomaly_events WHERE window_start < %s",
            "edit_war_signals": "DELETE FROM edit_war_signals WHERE window_start < %s",
            "user_aggregates": "DELETE FROM user_aggregates WHERE window_start < %s",
            "revision_quality_scores": "DELETE FROM revision_quality_scores WHERE fetched_at < %s",
            "pipeline_metrics": "DELETE FROM pipeline_metrics WHERE recorded_at < %s",
        }
        with self._connect() as conn:
            with conn.cursor() as cur:
                for table, stmt in statements.items():
                    cur.execute(stmt, (cutoff,))
                    deleted[table] = cur.rowcount
        logger.info("Retention purge complete (cutoff=%s): %s", cutoff.isoformat(), deleted)
        return deleted

    # ------------------------------------------------------------------
    # Read helpers used by the dashboard
    # ------------------------------------------------------------------
    def fetch_all(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params or ())
                return [dict(row) for row in cur.fetchall()]
