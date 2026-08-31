"""Thin, reliable wrapper around confluent_kafka.Producer for publishing
normalized WikipediaEvent records.

Design notes:
- Serialization is JSON via `WikipediaEvent.to_json()`; all producers and
  consumers in this project agree on this single format.
- Delivery is asynchronous with a callback for logging; `flush()` is called
  on shutdown to avoid dropping buffered messages.
- Malformed/undeliverable events are not silently dropped: delivery
  failures are logged and counted so operators can see them on the
  dashboard (see common metrics usage in sse_consumer.py).
"""

from __future__ import annotations

import logging
from typing import Optional

from confluent_kafka import KafkaError, Message, Producer

from common.config import KafkaConfig
from common.models import WikipediaEvent

logger = logging.getLogger(__name__)


class EventPublisher:
    """Publishes normalized WikipediaEvents to the configured Kafka topic."""

    def __init__(self, kafka_config: KafkaConfig):
        self._config = kafka_config
        self._producer = Producer({
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "acks": kafka_config.producer_acks,
            "linger.ms": kafka_config.producer_linger_ms,
            "enable.idempotence": True,
            "retries": 5,
        })
        self.delivered_count = 0
        self.failed_count = 0

    def _delivery_callback(self, err: Optional[KafkaError], msg: Message) -> None:
        if err is not None:
            self.failed_count += 1
            logger.error("Kafka delivery failed: %s", err)
        else:
            self.delivered_count += 1

    def publish(self, event: WikipediaEvent, topic: Optional[str] = None) -> None:
        """Publish a single event asynchronously. Keyed by page_title so all
        edits to the same page land on the same partition, which keeps
        per-page ordering intact for downstream stateful processing."""
        target_topic = topic or self._config.topic_edits
        try:
            self._producer.produce(
                topic=target_topic,
                key=event.page_title.encode("utf-8"),
                value=event.to_json().encode("utf-8"),
                callback=self._delivery_callback,
            )
            # Serve delivery-report callbacks without blocking.
            self._producer.poll(0)
        except BufferError:
            logger.warning("Kafka producer queue full; flushing and retrying once")
            self._producer.flush(5)
            self._producer.produce(
                topic=target_topic,
                key=event.page_title.encode("utf-8"),
                value=event.to_json().encode("utf-8"),
                callback=self._delivery_callback,
            )

    def publish_raw_dead_letter(self, raw_payload: str, reason: str) -> None:
        """Publish an unparseable/malformed payload to the dead-letter topic
        for later inspection, instead of dropping it silently."""
        try:
            self._producer.produce(
                topic=self._config.topic_dead_letter,
                value=raw_payload.encode("utf-8", errors="replace"),
                headers={"reason": reason.encode("utf-8")},
            )
            self._producer.poll(0)
        except Exception:  # pragma: no cover - best-effort, never fatal
            logger.exception("Failed to publish to dead-letter topic")

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    def close(self) -> None:
        self.flush()
