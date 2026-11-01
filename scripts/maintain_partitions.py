"""Create upcoming daily partitions and apply the PostgreSQL retention policy.

    python -m scripts.maintain_partitions                # run once with safety drain
    python -m scripts.maintain_partitions --loop         # repeat every interval
    python -m scripts.maintain_partitions --check-health # audit partition coverage and default partition overflow

Features:
- Idempotent partition creation for upcoming horizon days.
- Safety "Drain-Before-Drop" gate: verifies Parquet Lake archive coverage before dropping tables older than retention hours.
- Health auditing: monitors overflow into default partitions and alerts on coverage gaps.
- Telemetry recording: logs maintenance status to pipeline_metrics.
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Optional

import psycopg2

from common.config import get_settings
from common.logging_setup import configure_logging
from storage.database import Database
from storage.partitions import PartitionHealthReport


def run_once(
    db: Database,
    logger: logging.Logger,
    parquet_base_path: Optional[str | Path] = None,
    drain_before_drop: bool = True,
) -> PartitionHealthReport:
    """Execute one partition maintenance cycle with safety drain and health audit."""
    db.init_schema()
    result = db.purge_old_data(
        parquet_base_path=parquet_base_path,
        drain_before_drop=drain_before_drop,
    )
    health = db.check_partition_health()

    logger.info(
        "Maintenance cycle complete: dropped %d partitions %s, drained %d partitions %s, deleted rows %s",
        len(result.dropped_partitions),
        result.dropped_partitions,
        len(result.drained_partitions),
        result.drained_partitions,
        result.deleted_rows,
    )

    if not health.healthy:
        logger.warning(
            "Partition Health [%s]: default partition overflow rows=%d, missing coverage=%s",
            health.status,
            health.total_default_rows,
            health.missing_partitions,
        )
    else:
        logger.info("Partition Health [OK]: all upcoming days covered, 0 rows in default partitions.")

    # Record maintenance telemetry to pipeline_metrics
    try:
        db.record_pipeline_metrics({
            "component": "maintenance",
            "events_received": health.total_default_rows,
            "events_parsed": len(health.active_partitions.get("edit_events", [])),
            "events_rejected": 0,
            "events_published": len(result.dropped_partitions),
            "anomalies_detected": 0,
            "edit_wars_detected": 0,
            "error_count": 1 if not health.healthy else 0,
            "details": {
                "healthy": health.healthy,
                "status": health.status,
                "default_partition_rows": health.default_partition_rows,
                "missing_partitions": health.missing_partitions,
                "dropped_partitions": result.dropped_partitions,
                "drained_partitions": result.drained_partitions,
            },
        })
    except Exception as ex:
        logger.warning("Could not record maintenance metrics to pipeline_metrics: %s", ex)

    return health


def main() -> None:
    parser = argparse.ArgumentParser(description="Maintain WikiPulse PostgreSQL partitions and retention")
    parser.add_argument("--loop", action="store_true", help="Keep running at the configured interval")
    parser.add_argument("--check-health", action="store_true", help="Run partition health check and exit")
    parser.add_argument("--lake-dir", default=None, help="Parquet Lake base path for drain-before-drop verification")
    parser.add_argument("--no-drain", action="store_true", help="Disable safety drain-before-drop gate")
    args = parser.parse_args()

    logger = configure_logging("scripts.maintain_partitions")
    settings = get_settings()
    db = Database(settings.postgres)
    lake_dir = args.lake_dir or settings.parquet.base_path
    drain_before_drop = not args.no_drain

    if args.check_health:
        health = db.check_partition_health()
        print(f"Partition Health Status: {health.status}")
        print(f"Healthy: {health.healthy}")
        print(f"Default Partition Rows: {health.default_partition_rows}")
        print(f"Missing Partitions: {health.missing_partitions}")
        print(f"Active Partitions: {health.active_partitions}")
        return

    if not args.loop:
        run_once(db, logger, parquet_base_path=lake_dir, drain_before_drop=drain_before_drop)
        return

    interval = settings.postgres.maintenance_interval_seconds
    logger.info("Running partition maintenance every %ds (lake_dir=%s, drain_before_drop=%s)",
                interval, lake_dir, drain_before_drop)
    while True:
        try:
            run_once(db, logger, parquet_base_path=lake_dir, drain_before_drop=drain_before_drop)
        except psycopg2.Error:
            logger.exception("Partition maintenance failed; retrying in %ds", interval)
        time.sleep(interval)


if __name__ == "__main__":
    main()
