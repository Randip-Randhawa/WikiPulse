"""Create upcoming daily partitions and apply the PostgreSQL retention policy.

    python -m scripts.maintain_partitions          # run once
    python -m scripts.maintain_partitions --loop   # repeat every POSTGRES_MAINTENANCE_INTERVAL_SECONDS

Each run is idempotent and starts from the database's current state, so a
restarted loop simply catches up: missing partitions are created and
everything past the cutoff is removed. A failed run is logged and retried
at the next interval. The `maintenance` Docker Compose service runs the
loop; the processing job also creates missing partitions when it starts.
"""

from __future__ import annotations

import argparse
import logging
import time

import psycopg2

from common.config import get_settings
from common.logging_setup import configure_logging
from storage.database import Database


def run_once(db: Database, logger: logging.Logger) -> None:
    db.init_schema()
    result = db.purge_old_data()
    logger.info("Maintenance complete: dropped partitions %s, deleted rows %s",
                result.dropped_partitions, result.deleted_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Maintain WikiPulse PostgreSQL partitions and retention")
    parser.add_argument("--loop", action="store_true", help="Keep running at the configured interval")
    args = parser.parse_args()

    logger = configure_logging("scripts.maintain_partitions")
    settings = get_settings()
    db = Database(settings.postgres)

    if not args.loop:
        run_once(db, logger)
        return

    interval = settings.postgres.maintenance_interval_seconds
    logger.info("Running partition maintenance every %ds", interval)
    while True:
        try:
            run_once(db, logger)
        except psycopg2.Error:
            logger.exception("Partition maintenance failed; retrying in %ds", interval)
        time.sleep(interval)


if __name__ == "__main__":
    main()
