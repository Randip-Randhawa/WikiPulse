"""Shared fixtures.

The `database` fixture runs tests against a real PostgreSQL database. It
drops and recreates the `public` schema before every test, so it only runs
when WIKIPULSE_TEST_POSTGRES_DB names a database whose name ends in
`_test`. Host, port, user and password come from the usual POSTGRES_*
variables.
"""

from __future__ import annotations

import os

import pytest

from common.config import PostgresConfig

TEST_DATABASE_ENV = "WIKIPULSE_TEST_POSTGRES_DB"


def run_sql(config: PostgresConfig, sql: str, params: tuple = ()) -> None:
    """Execute raw SQL in its own committed transaction."""
    import psycopg2

    with psycopg2.connect(config.dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
    conn.close()


@pytest.fixture
def postgres_config() -> PostgresConfig:
    database_name = os.environ.get(TEST_DATABASE_ENV)
    if not database_name:
        pytest.skip(f"{TEST_DATABASE_ENV} is not set")
    if not database_name.endswith("_test"):
        pytest.fail(f"{TEST_DATABASE_ENV}={database_name!r} must end in '_test'; this fixture wipes it")

    config = PostgresConfig(database=database_name)
    run_sql(config, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    return config


@pytest.fixture
def database(postgres_config):
    from storage.database import Database

    return Database(postgres_config)
