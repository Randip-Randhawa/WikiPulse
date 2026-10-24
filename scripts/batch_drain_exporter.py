"""1-Hour Rolling Batch Drain Exporter.

Automated batch-drain mechanism that extracts closed 1-hour window frames
from PostgreSQL operational storage to Snappy-compressed columnar Parquet files
under the hierarchical date lake partition structure:
    /data/lake/wiki=enwiki/year=YYYY/month=MM/day=DD/hour=HH/

Usage:
    python -m scripts.batch_drain_exporter                # run once
    python -m scripts.batch_drain_exporter --loop         # run continuously every hour
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from common.config import ParquetConfig, get_settings
from common.logging_setup import configure_logging
from storage.database import Database
from storage.parquet_writer import ParquetWriteStats, ParquetWriter

logger = logging.getLogger(__name__)


def drain_closed_hour_window(
    db: Database,
    writer: ParquetWriter,
    now: Optional[datetime] = None,
    lag_hours: int = 1,
) -> ParquetWriteStats:
    """Fetch closed 1-hour window frames from PostgreSQL and export them
    to Snappy-compressed columnar Parquet files in the lake partition hierarchy."""
    now = now or datetime.now(timezone.utc)
    # Target closed 1-hour window
    cutoff = now.replace(minute=0, second=0, microsecond=0)

    query = """
        SELECT
            event_id, event_time, wiki, event_type, page_title, page_id,
            namespace, revision_id, previous_revision_id, "user", user_id,
            is_bot, comment, byte_length_new, byte_length_old,
            byte_length_delta, is_revert, revert_target_revision_id,
            ingestion_time, kafka_partition, kafka_offset,
            provenance_id, user_type, is_minor, comment_length, processing_time
        FROM edit_events
        WHERE event_time < %s
        ORDER BY event_time ASC
    """
    rows = db.fetch_all(query, (cutoff,))
    if not rows:
        logger.info("No closed 1-hour frames found to drain before %s", cutoff.isoformat())
        return ParquetWriteStats()

    stats = writer.write_lake_records(rows, time_field="event_time", wiki_field="wiki")
    ratio = (
        round(stats.uncompressed_bytes / stats.compressed_bytes, 2)
        if stats.compressed_bytes
        else 1.0
    )
    compression_pct = (
        round(100 * (1 - stats.compressed_bytes / stats.uncompressed_bytes), 1)
        if stats.uncompressed_bytes
        else 0.0
    )
    logger.info(
        "Drained %d rows across %d files into Parquet Lake (Snappy ratio: %sx, %s%% space saving)",
        stats.rows,
        stats.files,
        ratio,
        compression_pct,
    )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="1-Hour Rolling Batch Drain Exporter to Parquet Lake")
    parser.add_argument("--loop", action="store_true", help="Keep running every 3600 seconds")
    parser.add_argument("--interval", type=int, default=3600, help="Interval in seconds between drain batches")
    parser.add_argument("--lake-dir", default="./data/lake", help="Base path for Parquet Lake")
    args = parser.parse_args()

    configure_logging("scripts.batch_drain_exporter")
    settings = get_settings()
    db = Database(settings.postgres)
    writer = ParquetWriter(ParquetConfig(base_path=args.lake_dir))

    if not args.loop:
        drain_closed_hour_window(db, writer)
        return

    logger.info("Starting rolling batch drain exporter loop (interval=%ds)", args.interval)
    while True:
        try:
            drain_closed_hour_window(db, writer)
        except Exception:
            logger.exception("Batch drain failed; will retry in %ds", args.interval)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
