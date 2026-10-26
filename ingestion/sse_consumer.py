"""Wikimedia Recent Changes SSE ingestion service.

Connects to Wikimedia's live EventStreams `recentchange` feed, normalizes
each event, and publishes it to Kafka. Designed to run indefinitely as a
long-lived process (`python -m ingestion.sse_consumer`).

Reliability behavior:
- Reconnects with exponential backoff (capped) on any connection drop,
  timeout, or unexpected stream error.
- Never crashes on a single malformed event: it is logged, counted, and
  routed to the dead-letter Kafka topic.
- Tracks ingestion metrics (received / ignored / rejected / published,
  normalization time, ingestion latency) and writes them to the
  `pipeline_metrics` Postgres table every METRICS_FLUSH_INTERVAL_SECONDS
  for the dashboard's health panel. A failed metrics write is logged and
  never interrupts ingestion.
"""

from __future__ import annotations

import json
import logging
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Optional

import requests
from sseclient import SSEClient

from common.config import Settings, get_settings
from common.logging_setup import configure_logging
from common.stats import summarize_ms
from ingestion.kafka_producer import EventPublisher
from ingestion.normalizer import MalformedEventError, normalize_event, should_ignore
from storage.database import Database

logger = logging.getLogger(__name__)

METRICS_FLUSH_INTERVAL_SECONDS = 30


@dataclass
class IngestionMetrics:
    events_received: int = 0
    events_parsed: int = 0
    events_ignored: int = 0
    events_rejected: int = 0
    events_published: int = 0  # handed to the Kafka producer; delivery is confirmed separately
    events_dead_lettered: int = 0
    dead_letter_failures: int = 0
    publish_failures: int = 0
    reconnects: int = 0
    last_event_timestamp: Optional[float] = None
    started_at: float = field(default_factory=time.time)
    # Per-event samples in seconds, cleared each time metrics are written.
    normalize_durations: list[float] = field(default_factory=list)
    ingestion_latencies: list[float] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "events_received": self.events_received,
            "events_parsed": self.events_parsed,
            "events_ignored": self.events_ignored,
            "events_rejected": self.events_rejected,
            "events_published": self.events_published,
            "events_dead_lettered": self.events_dead_lettered,
            "reconnects": self.reconnects,
            "last_event_timestamp": self.last_event_timestamp,
            "uptime_seconds": time.time() - self.started_at,
        }


class ShutdownRequested(Exception):
    """Raised internally to unwind the ingestion loop cleanly on SIGINT/SIGTERM."""


class WikimediaSSEConsumer:
    """Consumes the Wikimedia recentchange SSE stream with reconnect/backoff
    and forwards normalized events to Kafka."""

    def __init__(
        self,
        settings: Settings,
        publisher: EventPublisher,
        db: Optional[Database] = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._settings = settings
        self._publisher = publisher
        self._db = db
        self._clock = clock
        self.metrics = IngestionMetrics()
        self._stop_requested = False
        self._last_flush_at = clock()
        self._parsed_at_last_flush = 0

    def request_stop(self) -> None:
        self._stop_requested = True

    def _iter_sse_data(self) -> Iterator[str]:
        """Yield the data of each SSE message from the live stream. Raises
        on connection-level failures so the caller can apply backoff."""
        wm_config = self._settings.wikimedia
        response = requests.get(
            wm_config.stream_url,
            stream=True,
            timeout=(10, 90),
            headers={"User-Agent": wm_config.user_agent},
        )
        response.raise_for_status()
        client = SSEClient(response)
        for sse_event in client.events():
            if self._stop_requested:
                raise ShutdownRequested()
            if sse_event.data:
                yield sse_event.data

    def process_sse_data(self, data: str) -> None:
        """Decode one SSE message and route it: ignore, dead-letter, or
        publish to Kafka."""
        try:
            raw = json.loads(data)
        except json.JSONDecodeError as exc:
            self.metrics.events_received += 1
            self.metrics.events_rejected += 1
            logger.warning("Dropping non-JSON SSE payload: %s", exc)
            self._dead_letter(data, reason=f"invalid_json: {exc}", stage="json_decode")
            return
        self._process_raw_event(raw)

    def _dead_letter(self, raw_payload: str, reason: str, stage: str) -> None:
        if self._publisher.publish_raw_dead_letter(raw_payload, reason=reason, stage=stage):
            self.metrics.events_dead_lettered += 1
        else:
            self.metrics.dead_letter_failures += 1

    def _process_raw_event(self, raw: dict) -> None:
        self.metrics.events_received += 1

        if should_ignore(raw, self._settings.wikimedia.wiki_filter):
            self.metrics.events_ignored += 1
            return

        normalize_started = time.perf_counter()
        try:
            event = normalize_event(raw)
        except MalformedEventError as exc:
            self.metrics.events_rejected += 1
            logger.warning("Malformed event skipped: %s", exc)
            self._dead_letter(json.dumps(raw), reason=str(exc), stage="normalize")
            return
        except Exception as exc:  # pragma: no cover - defensive: never crash ingestion
            self.metrics.events_rejected += 1
            logger.exception("Unexpected error normalizing event; skipping")
            self._dead_letter(json.dumps(raw, default=str), reason=f"unexpected: {exc!r}", stage="normalize")
            return
        self.metrics.normalize_durations.append(time.perf_counter() - normalize_started)

        self.metrics.events_parsed += 1
        self.metrics.last_event_timestamp = event.timestamp
        self.metrics.ingestion_latencies.append(event.ingestion_timestamp - event.timestamp)

        try:
            self._publisher.publish(event)
            self.metrics.events_published += 1
        except Exception:
            self.metrics.publish_failures += 1
            logger.exception("Failed to publish event %s to Kafka", event.event_id)

    def metrics_row(self) -> dict[str, Any]:
        """Build a `pipeline_metrics` row from the cumulative counters and
        the samples gathered since the previous row, then clear the samples.

        `events_published` is the number of events Kafka confirmed; errors
        are delivery, publish and dead-letter failures (malformed input is
        counted separately as rejected)."""
        now = self._clock()
        elapsed = max(now - self._last_flush_at, 1e-6)
        throughput = (self.metrics.events_parsed - self._parsed_at_last_flush) / elapsed
        self._last_flush_at = now
        self._parsed_at_last_flush = self.metrics.events_parsed

        received = self.metrics.events_received
        row = {
            "component": "ingestion",
            "events_received": received,
            "events_parsed": self.metrics.events_parsed,
            "events_rejected": self.metrics.events_rejected,
            "events_published": self._publisher.delivered_count,
            "anomalies_detected": 0,
            "edit_wars_detected": 0,
            "processing_latency_ms": None,
            "throughput_per_sec": round(throughput, 4),
            "last_event_time": (
                None if self.metrics.last_event_timestamp is None
                else datetime.fromtimestamp(self.metrics.last_event_timestamp, tz=timezone.utc)
            ),
            "error_count": (
                self._publisher.failed_count + self.metrics.publish_failures + self.metrics.dead_letter_failures
            ),
            "details": {
                "events_ignored": self.metrics.events_ignored,
                "events_enqueued": self.metrics.events_published,
                "events_dead_lettered": self.metrics.events_dead_lettered,
                "dead_letter_rate": round(self.metrics.events_dead_lettered / received, 6) if received else None,
                "delivery_failures": self._publisher.failed_count,
                "publish_failures": self.metrics.publish_failures,
                "dead_letter_failures": self.metrics.dead_letter_failures,
                "producer_retries": self._publisher.retried_count,
                "reconnects": self.metrics.reconnects,
                "normalize_ms": summarize_ms(self.metrics.normalize_durations),
                "ingestion_latency_ms": summarize_ms(self.metrics.ingestion_latencies),
            },
        }
        self.metrics.normalize_durations.clear()
        self.metrics.ingestion_latencies.clear()
        return row

    def _flush_metrics(self) -> None:
        if self._db is None:
            return
        try:
            self._db.record_pipeline_metrics(self.metrics_row())
        except Exception as exc:
            logger.warning("Could not write ingestion metrics to PostgreSQL: %s", exc)

    def _flush_metrics_if_due(self) -> None:
        if self._clock() - self._last_flush_at >= METRICS_FLUSH_INTERVAL_SECONDS:
            self._flush_metrics()

    def run_forever(self) -> None:
        """Main ingestion loop: connect, consume, reconnect with backoff on
        failure, until a stop is requested."""
        wm_config = self._settings.wikimedia
        delay = wm_config.reconnect_min_delay

        while not self._stop_requested:
            try:
                logger.info("Connecting to Wikimedia stream: %s", wm_config.stream_url)
                for data in self._iter_sse_data():
                    self.process_sse_data(data)
                    self._flush_metrics_if_due()
                    delay = wm_config.reconnect_min_delay  # reset backoff after healthy events
                    if self._stop_requested:
                        break
            except ShutdownRequested:
                logger.info("Shutdown requested; stopping ingestion loop")
                break
            except (requests.RequestException, ConnectionError) as exc:
                self.metrics.reconnects += 1
                logger.error("SSE connection error (%s); reconnecting in %.1fs", exc, delay)
            except Exception:  # pragma: no cover - defensive top-level guard
                self.metrics.reconnects += 1
                logger.exception("Unexpected ingestion error; reconnecting in %.1fs", delay)

            if self._stop_requested:
                break

            time.sleep(delay)
            delay = min(delay * wm_config.reconnect_backoff_factor, wm_config.reconnect_max_delay)

        self._publisher.flush()
        self._flush_metrics()
        logger.info("Ingestion stopped. Final metrics: %s", self.metrics.as_dict())


def main() -> None:
    settings = get_settings()
    global logger
    logger = configure_logging("ingestion.sse_consumer")

    publisher = EventPublisher(settings.kafka)
    consumer = WikimediaSSEConsumer(settings, publisher, db=Database(settings.postgres))

    def _handle_signal(signum, _frame):
        logger.info("Received signal %s, requesting shutdown", signum)
        consumer.request_stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer.run_forever()


if __name__ == "__main__":
    main()
