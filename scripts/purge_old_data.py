"""Purge PostgreSQL rows older than POSTGRES_RETENTION_DAYS.

Intended to be run periodically (e.g. via cron or a simple sleep loop) to
keep the live-query tables bounded, since long-term history is retained in
Parquet instead (see storage/database.py Database.purge_old_data).

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
    deleted = db.purge_old_data()
    logger.info("Purge complete: %s", deleted)


if __name__ == "__main__":
    main()
