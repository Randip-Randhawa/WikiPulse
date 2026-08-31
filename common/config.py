"""Centralized configuration for all WikiPulse services.

All infrastructure addresses, thresholds, and tunables are sourced from
environment variables (optionally loaded from a local `.env` file) so that
no component hard-codes hosts, ports, or magic numbers. Each service should
import the dataclasses it needs from this module rather than reading
`os.environ` directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from dotenv import load_dotenv

# Load a local .env file if present. Real deployments may instead inject
# environment variables directly (e.g. via Docker Compose `env_file:`).
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if _ENV_PATH.exists():
    load_dotenv(dotenv_path=_ENV_PATH)


def _get_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _get_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def _get_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_list(name: str, default: List[str]) -> List[str]:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class WikimediaConfig:
    stream_url: str = field(default_factory=lambda: _get_str(
        "WIKIMEDIA_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"))
    wiki_filter: List[str] = field(default_factory=lambda: _get_list("WIKIMEDIA_WIKI_FILTER", []))
    reconnect_min_delay: float = field(default_factory=lambda: _get_float("SSE_RECONNECT_MIN_DELAY", 1.0))
    reconnect_max_delay: float = field(default_factory=lambda: _get_float("SSE_RECONNECT_MAX_DELAY", 60.0))
    reconnect_backoff_factor: float = field(
        default_factory=lambda: _get_float("SSE_RECONNECT_BACKOFF_FACTOR", 2.0))


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = field(default_factory=lambda: _get_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    topic_edits: str = field(default_factory=lambda: _get_str("KAFKA_TOPIC_EDITS", "wikipedia-edits"))
    topic_dead_letter: str = field(
        default_factory=lambda: _get_str("KAFKA_TOPIC_DEAD_LETTER", "wikipedia-edits-dlq"))
    consumer_group: str = field(default_factory=lambda: _get_str("KAFKA_CONSUMER_GROUP", "wikipulse-spark"))
    producer_acks: str = field(default_factory=lambda: _get_str("KAFKA_PRODUCER_ACKS", "all"))
    producer_linger_ms: int = field(default_factory=lambda: _get_int("KAFKA_PRODUCER_LINGER_MS", 50))


@dataclass(frozen=True)
class SparkConfig:
    master: str = field(default_factory=lambda: _get_str("SPARK_MASTER", "local[*]"))
    app_name: str = field(default_factory=lambda: _get_str("SPARK_APP_NAME", "WikiPulseStreaming"))
    checkpoint_dir: str = field(default_factory=lambda: _get_str("SPARK_CHECKPOINT_DIR", "./data/checkpoints"))
    window_duration: str = field(default_factory=lambda: _get_str("SPARK_WINDOW_DURATION", "5 minutes"))
    slide_duration: str = field(default_factory=lambda: _get_str("SPARK_SLIDE_DURATION", "1 minute"))
    watermark_delay: str = field(default_factory=lambda: _get_str("SPARK_WATERMARK_DELAY", "2 minutes"))
    trigger_interval: str = field(default_factory=lambda: _get_str("SPARK_TRIGGER_INTERVAL", "30 seconds"))


@dataclass(frozen=True)
class AnomalyConfig:
    """Tunables for the median/MAD anomaly detector and its count-based
    fallback for pages with insufficient history (see Lab 2, Section 5)."""

    mad_threshold: float = field(default_factory=lambda: _get_float("ANOMALY_MAD_THRESHOLD", 3.5))
    min_history_windows: int = field(default_factory=lambda: _get_int("ANOMALY_MIN_HISTORY_WINDOWS", 8))
    fallback_edit_count_threshold: int = field(
        default_factory=lambda: _get_int("ANOMALY_FALLBACK_EDIT_COUNT_THRESHOLD", 15))
    history_lookback_windows: int = field(
        default_factory=lambda: _get_int("ANOMALY_HISTORY_LOOKBACK_WINDOWS", 48))
    mad_epsilon: float = field(default_factory=lambda: _get_float("ANOMALY_MAD_EPSILON", 1e-6))


@dataclass(frozen=True)
class EditWarConfig:
    mutual_revert_threshold: int = field(default_factory=lambda: _get_int("EDIT_WAR_MUTUAL_REVERT_THRESHOLD", 2))
    min_edits_in_window: int = field(default_factory=lambda: _get_int("EDIT_WAR_MIN_EDITS_IN_WINDOW", 4))
    score_mutual_weight: float = field(default_factory=lambda: _get_float("EDIT_WAR_SCORE_MUTUAL_WEIGHT", 2.0))
    score_burst_weight: float = field(default_factory=lambda: _get_float("EDIT_WAR_SCORE_BURST_WEIGHT", 0.5))
    flag_score_threshold: float = field(default_factory=lambda: _get_float("EDIT_WAR_FLAG_SCORE_THRESHOLD", 3.0))


@dataclass(frozen=True)
class LiftWingConfig:
    enabled: bool = field(default_factory=lambda: _get_bool("LIFTWING_ENABLED", False))
    base_url: str = field(default_factory=lambda: _get_str(
        "LIFTWING_BASE_URL", "https://api.wikimedia.org/service/lw/inference/v1/models"))
    model_name: str = field(default_factory=lambda: _get_str("LIFTWING_MODEL_NAME", "revertrisk-language-agnostic"))
    timeout_seconds: float = field(default_factory=lambda: _get_float("LIFTWING_TIMEOUT_SECONDS", 2.0))
    max_retries: int = field(default_factory=lambda: _get_int("LIFTWING_MAX_RETRIES", 1))
    max_concurrent_requests: int = field(
        default_factory=lambda: _get_int("LIFTWING_MAX_CONCURRENT_REQUESTS", 4))


@dataclass(frozen=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: _get_str("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: _get_int("POSTGRES_PORT", 5432))
    database: str = field(default_factory=lambda: _get_str("POSTGRES_DB", "wikipulse"))
    user: str = field(default_factory=lambda: _get_str("POSTGRES_USER", "wikipulse"))
    password: str = field(default_factory=lambda: _get_str("POSTGRES_PASSWORD", "wikipulse_dev_password"))
    retention_days: int = field(default_factory=lambda: _get_int("POSTGRES_RETENTION_DAYS", 14))

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class ParquetConfig:
    base_path: str = field(default_factory=lambda: _get_str("PARQUET_BASE_PATH", "./data/parquet"))


@dataclass(frozen=True)
class DashboardConfig:
    refresh_seconds: int = field(default_factory=lambda: _get_int("DASHBOARD_REFRESH_SECONDS", 10))
    recent_window_minutes: int = field(
        default_factory=lambda: _get_int("DASHBOARD_RECENT_WINDOW_MINUTES", 60))


@dataclass(frozen=True)
class LoggingConfig:
    level: str = field(default_factory=lambda: _get_str("LOG_LEVEL", "INFO"))


@dataclass(frozen=True)
class Settings:
    """Aggregate settings object. Instantiate once per process via
    `get_settings()`."""

    wikimedia: WikimediaConfig = field(default_factory=WikimediaConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    anomaly: AnomalyConfig = field(default_factory=AnomalyConfig)
    edit_war: EditWarConfig = field(default_factory=EditWarConfig)
    liftwing: LiftWingConfig = field(default_factory=LiftWingConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    parquet: ParquetConfig = field(default_factory=ParquetConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


_settings: Settings | None = None


def get_settings(force_reload: bool = False) -> Settings:
    """Return the process-wide Settings singleton, constructing it from the
    current environment on first access (or when `force_reload=True`,
    primarily useful in tests)."""
    global _settings
    if _settings is None or force_reload:
        _settings = Settings()
    return _settings
