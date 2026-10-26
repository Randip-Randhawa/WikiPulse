"""Tests for ingestion.sse_consumer: event routing (ignored / rejected /
published), dead-letter accounting and the metrics row written to
PostgreSQL. Kafka and PostgreSQL are replaced with small fakes."""

from __future__ import annotations

import time

from common.config import Settings, WikimediaConfig
from ingestion.sse_consumer import METRICS_FLUSH_INTERVAL_SECONDS, WikimediaSSEConsumer


class FakePublisher:
    def __init__(self, dead_letter_succeeds: bool = True):
        self.published = []
        self.dead_letters = []
        self.delivered_count = 0
        self.failed_count = 0
        self.retried_count = 0
        self._dead_letter_succeeds = dead_letter_succeeds

    def publish(self, event):
        self.published.append(event)

    def publish_raw_dead_letter(self, raw_payload, reason, stage):
        self.dead_letters.append((stage, reason))
        return self._dead_letter_succeeds

    def flush(self, timeout=10.0):
        pass


class FakeDatabase:
    def __init__(self):
        self.metrics_rows = []

    def record_pipeline_metrics(self, row):
        self.metrics_rows.append(row)


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def make_consumer(publisher=None, db=None, clock=None, wiki_filter=None):
    settings = Settings(wikimedia=WikimediaConfig(wiki_filter=wiki_filter or ["enwiki"]))
    return WikimediaSSEConsumer(settings, publisher or FakePublisher(), db=db, clock=clock or FakeClock())


def raw_edit(**overrides):
    event = {
        "meta": {"id": "abc"},
        "type": "edit",
        "wiki": "enwiki",
        "title": "Page",
        "timestamp": int(time.time()) - 2,
        "user": "Alice",
        "revision": {"old": 1, "new": 2},
        "length": {"old": 10, "new": 20},
    }
    event.update(overrides)
    return event


def test_valid_event_is_published():
    publisher = FakePublisher()
    consumer = make_consumer(publisher)

    consumer._process_raw_event(raw_edit())

    assert len(publisher.published) == 1
    assert consumer.metrics.events_parsed == 1


def test_event_from_another_wiki_is_ignored_not_rejected():
    publisher = FakePublisher()
    consumer = make_consumer(publisher)

    consumer._process_raw_event(raw_edit(wiki="dewiki"))

    assert consumer.metrics.events_ignored == 1
    assert consumer.metrics.events_rejected == 0
    assert publisher.published == []


def test_malformed_event_is_dead_lettered_with_its_stage():
    publisher = FakePublisher()
    consumer = make_consumer(publisher)

    consumer._process_raw_event(raw_edit(title=None))

    assert consumer.metrics.events_rejected == 1
    assert consumer.metrics.events_dead_lettered == 1
    assert publisher.dead_letters == [("normalize", "missing required field: title")]


def test_failed_dead_letter_is_counted_as_an_error():
    db = FakeDatabase()
    consumer = make_consumer(FakePublisher(dead_letter_succeeds=False), db=db)

    consumer._process_raw_event(raw_edit(title=None))
    consumer._flush_metrics()

    assert consumer.metrics.events_dead_lettered == 0
    assert db.metrics_rows[0]["error_count"] == 1


def test_metrics_row_reports_throughput_since_the_previous_row():
    clock = FakeClock()
    consumer = make_consumer(clock=clock)
    for _ in range(30):
        consumer._process_raw_event(raw_edit())

    clock.now = 15.0
    row = consumer.metrics_row()

    assert row["component"] == "ingestion"
    assert row["throughput_per_sec"] == 2.0


def test_metrics_row_reports_dead_letter_rate_and_latency_samples():
    consumer = make_consumer()
    consumer._process_raw_event(raw_edit())
    consumer._process_raw_event(raw_edit())
    consumer._process_raw_event(raw_edit(title=None))
    consumer._process_raw_event(raw_edit(wiki="dewiki"))

    details = consumer.metrics_row()["details"]

    assert details["dead_letter_rate"] == 0.25
    assert details["normalize_ms"]["count"] == 2
    assert details["ingestion_latency_ms"]["count"] == 2


def test_latency_samples_are_cleared_after_each_row():
    consumer = make_consumer()
    consumer._process_raw_event(raw_edit())
    consumer.metrics_row()

    details = consumer.metrics_row()["details"]

    assert details["normalize_ms"] is None
    assert details["ingestion_latency_ms"] is None


def test_metrics_are_written_once_the_flush_interval_passes():
    clock = FakeClock()
    db = FakeDatabase()
    consumer = make_consumer(db=db, clock=clock)

    consumer._flush_metrics_if_due()
    clock.now = METRICS_FLUSH_INTERVAL_SECONDS
    consumer._flush_metrics_if_due()

    assert len(db.metrics_rows) == 1


def test_database_failure_does_not_stop_ingestion():
    class BrokenDatabase:
        def record_pipeline_metrics(self, row):
            raise ConnectionError("database down")

    publisher = FakePublisher()
    consumer = make_consumer(publisher, db=BrokenDatabase())

    consumer._flush_metrics()
    consumer._process_raw_event(raw_edit())

    assert len(publisher.published) == 1


def test_non_json_message_is_counted_and_dead_lettered():
    publisher = FakePublisher()
    consumer = make_consumer(publisher)

    consumer.process_sse_data("{not json")

    assert consumer.metrics.events_received == 1
    assert consumer.metrics.events_rejected == 1
    assert publisher.dead_letters[0][0] == "json_decode"


def test_json_message_is_routed_like_a_raw_event():
    publisher = FakePublisher()
    consumer = make_consumer(publisher)

    consumer.process_sse_data('{"type": "log", "wiki": "enwiki", "title": "Page", "timestamp": 1}')

    assert consumer.metrics.events_ignored == 1
