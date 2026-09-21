"""Wikimedia Recent Changes SSE ingestion service.

Connects to Wikimedia's live EventStreams `recentchange` feed, normalizes
each event, and publishes it to Kafka. Designed to run indefinitely as a
long-lived process (`python -m ingestion.sse_consumer`).

Reliability behavior:
- Reconnects with exponential backoff (capped) on any connection drop,
  timeout, or unexpected stream error.
- Never crashes on a single malformed event: it is logged, counted, and
  routed to the dead-letter Kafka topic.
- Tracks basic ingestion metrics (received / parsed / rejected / published)
  and periodically logs a summary; these counters are also exposed via
  `IngestionMetrics` for reuse by a future metrics endpoint or the
  dashboard's health panel (persisted through the `pipeline_metrics`
  Postgres table by `storage.database`).
"""

from __future__ import annotations

import json
import logging
import signal
import time
from dataclasses import dataclass, field
from typing import Iterator, Optional

import requests
from sseclient import SSEClient

from common.config import Settings, get_settings
from common.logging_setup import configure_logging
from ingestion.kafka_producer import EventPublisher
from ingestion.normalizer import MalformedEventError, normalize_event, should_ignore

logger = logging.getLogger(__name__)


@dataclass
class IngestionMetrics:
    events_received: int = 0
    events_parsed: int = 0
    events_ignored: int = 0
    events_rejected: int = 0
    events_published: int = 0
    reconnects: int = 0
    last_event_timestamp: Optional[float] = None
    started_at: float = field(default_factory=time.time)

    def as_dict(self) -> dict:
        return {
            "events_received": self.events_received,
            "events_parsed": self.events_parsed,
            "events_ignored": self.events_ignored,
            "events_rejected": self.events_rejected,
            "events_published": self.events_published,
            "reconnects": self.reconnects,
            "last_event_timestamp": self.last_event_timestamp,
            "uptime_seconds": time.time() - self.started_at,
        }


class ShutdownRequested(Exception):
    """Raised internally to unwind the ingestion loop cleanly on SIGINT/SIGTERM."""


class WikimediaSSEConsumer:
    """Consumes the Wikimedia recentchange SSE stream with reconnect/backoff
    and forwards normalized events to Kafka."""

    def __init__(self, settings: Settings, publisher: EventPublisher):
        self._settings = settings
        self._publisher = publisher
        self.metrics = IngestionMetrics()
        self._stop_requested = False

    def request_stop(self) -> None:
        self._stop_requested = True

    def _iter_raw_events(self) -> Iterator[dict]:
        """Yield decoded JSON payloads from the live SSE stream. Raises on
        connection-level failures so the caller can apply backoff."""
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
            if not sse_event.data:
                continue
            try:
                yield json.loads(sse_event.data)
            except json.JSONDecodeError as exc:
                self.metrics.events_rejected += 1
                logger.warning("Dropping non-JSON SSE payload: %s", exc)
                self._publisher.publish_raw_dead_letter(sse_event.data, reason="invalid_json")

    def _process_raw_event(self, raw: dict) -> None:
        self.metrics.events_received += 1

        if should_ignore(raw, self._settings.wikimedia.wiki_filter):
            self.metrics.events_ignored += 1
            return

        try:
            event = normalize_event(raw)
        except MalformedEventError as exc:
            self.metrics.events_rejected += 1
            logger.warning("Malformed event skipped: %s", exc)
            self._publisher.publish_raw_dead_letter(json.dumps(raw), reason=str(exc))
            return
        except Exception:  # pragma: no cover - defensive: never crash ingestion
            self.metrics.events_rejected += 1
            logger.exception("Unexpected error normalizing event; skipping")
            return

        self.metrics.events_parsed += 1
        self.metrics.last_event_timestamp = event.timestamp

        try:
            self._publisher.publish(event)
            self.metrics.events_published += 1
        except Exception:
            logger.exception("Failed to publish event %s to Kafka", event.event_id)

    def run_forever(self) -> None:
        """Main ingestion loop: connect, consume, reconnect with backoff on
        failure, until a stop is requested."""
        wm_config = self._settings.wikimedia
        delay = wm_config.reconnect_min_delay

        while not self._stop_requested:
            try:
                logger.info("Connecting to Wikimedia stream: %s", wm_config.stream_url)
                for raw in self._iter_raw_events():
                    self._process_raw_event(raw)
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
        logger.info("Ingestion stopped. Final metrics: %s", self.metrics.as_dict())


def main() -> None:
    settings = get_settings()
    global logger
    logger = configure_logging("ingestion.sse_consumer")

    publisher = EventPublisher(settings.kafka)
    consumer = WikimediaSSEConsumer(settings, publisher)

    def _handle_signal(signum, _frame):
        logger.info("Received signal %s, requesting shutdown", signum)
        consumer.request_stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer.run_forever()


if __name__ == "__main__":
    main()
