"""Apply the PostgreSQL schema (idempotent). Run once after Postgres is up:

    python -m scripts.init_db
"""

from __future__ import annotations

from common.config import get_settings
from common.logging_setup import configure_logging
from storage.database import Database


def main() -> None:
    logger = configure_logging("scripts.init_db")
    settings = get_settings()
    db = Database(settings.postgres)
    db.init_schema()
    logger.info("Database schema initialized at %s:%s/%s",
                settings.postgres.host, settings.postgres.port, settings.postgres.database)


if __name__ == "__main__":
    main()
