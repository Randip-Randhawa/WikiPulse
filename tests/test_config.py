"""Tests for common.config: environment-variable parsing, defaults, and
type coercion. Uses force_reload=True so tests don't depend on process
ordering or a cached Settings singleton from another test module."""

from __future__ import annotations

from common.config import get_settings


class TestDefaults:
    def test_defaults_are_used_when_env_vars_are_absent(self, monkeypatch):
        for var in [
            "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC_EDITS", "ANOMALY_MAD_THRESHOLD",
            "ANOMALY_MIN_HISTORY_WINDOWS", "LIFTWING_ENABLED", "POSTGRES_PORT",
        ]:
            monkeypatch.delenv(var, raising=False)

        settings = get_settings(force_reload=True)

        assert settings.kafka.bootstrap_servers == "localhost:9092"
        assert settings.kafka.topic_edits == "wikipedia-edits"
        assert settings.anomaly.mad_threshold == 3.5
        assert settings.anomaly.min_history_windows == 8
        assert settings.liftwing.enabled is False
        assert settings.postgres.port == 5432


class TestEnvironmentOverrides:
    def test_string_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
        settings = get_settings(force_reload=True)
        assert settings.kafka.bootstrap_servers == "kafka-broker-1:9092"

    def test_int_override(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PORT", "6543")
        settings = get_settings(force_reload=True)
        assert settings.postgres.port == 6543
        assert isinstance(settings.postgres.port, int)

    def test_float_override(self, monkeypatch):
        monkeypatch.setenv("ANOMALY_MAD_THRESHOLD", "2.75")
        settings = get_settings(force_reload=True)
        assert settings.anomaly.mad_threshold == 2.75

    def test_bool_override_true_variants(self, monkeypatch):
        for value in ["true", "True", "1", "yes", "on"]:
            monkeypatch.setenv("LIFTWING_ENABLED", value)
            settings = get_settings(force_reload=True)
            assert settings.liftwing.enabled is True, f"failed for value={value!r}"

    def test_bool_override_false_variants(self, monkeypatch):
        for value in ["false", "False", "0", "no", "off"]:
            monkeypatch.setenv("LIFTWING_ENABLED", value)
            settings = get_settings(force_reload=True)
            assert settings.liftwing.enabled is False, f"failed for value={value!r}"

    def test_list_override_splits_on_comma(self, monkeypatch):
        monkeypatch.setenv("WIKIMEDIA_WIKI_FILTER", "enwiki, dewiki ,frwiki")
        settings = get_settings(force_reload=True)
        assert settings.wikimedia.wiki_filter == ["enwiki", "dewiki", "frwiki"]

    def test_empty_list_override_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("WIKIMEDIA_WIKI_FILTER", "")
        settings = get_settings(force_reload=True)
        assert settings.wikimedia.wiki_filter == []


class TestDerivedProperties:
    def test_postgres_dsn_contains_all_components(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "db.internal")
        monkeypatch.setenv("POSTGRES_PORT", "5432")
        monkeypatch.setenv("POSTGRES_DB", "wikipulse_test")
        monkeypatch.setenv("POSTGRES_USER", "tester")
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

        settings = get_settings(force_reload=True)
        dsn = settings.postgres.dsn

        assert "host=db.internal" in dsn
        assert "port=5432" in dsn
        assert "dbname=wikipulse_test" in dsn
        assert "user=tester" in dsn
        assert "password=secret" in dsn

    def test_postgres_sqlalchemy_url_format(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "db.internal")
        monkeypatch.setenv("POSTGRES_USER", "tester")
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        monkeypatch.setenv("POSTGRES_DB", "wikipulse_test")

        settings = get_settings(force_reload=True)
        url = settings.postgres.sqlalchemy_url

        assert url.startswith("postgresql+psycopg2://tester:secret@db.internal")
        assert url.endswith("/wikipulse_test")
