"""Purge PostgreSQL rows older than POSTGRES_RETENTION_HOURS.

Expired daily partitions are dropped and remaining expired rows deleted;
long-term history is retained in Parquet instead (see storage/database.py
Database.purge_old_data). The maintenance service runs this on a schedule;
this script runs it once:

    python -m scripts.purge_old_data
"""

from __future__ import annotations

from common.config import get_settings
from common.logging_setup import configure_logging
from storage.database import Database


def main() -> None:
    logger = configure_logging("scripts.purge_old_data")
    settings = get_settings()
    db = Database(settings.postgres)
    result = db.purge_old_data()
    logger.info("Purge complete: dropped partitions %s, deleted rows %s",
                result.dropped_partitions, result.deleted_rows)


if __name__ == "__main__":
    main()
