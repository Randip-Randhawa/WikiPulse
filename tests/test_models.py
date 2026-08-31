"""Tests for common.models.WikipediaEvent: JSON serialization is the wire
format between the ingestion producer and the Spark consumer, so a
round-trip must preserve every field exactly, including None values."""

from __future__ import annotations

from common.models import WikipediaEvent


def _sample_event(**overrides) -> WikipediaEvent:
    defaults = dict(
        event_id="abc-123",
        timestamp=1_700_000_000.5,
        wiki="enwiki",
        event_type="edit",
        page_title="Test Page",
        page_id=42,
        namespace=0,
        revision_id=100,
        previous_revision_id=99,
        user="Alice",
        user_id=1,
        is_bot=False,
        comment="test edit",
        byte_length_new=100,
        byte_length_old=90,
        byte_length_delta=10,
        is_revert=False,
        revert_target_revision_id=None,
        ingestion_timestamp=1_700_000_001.0,
    )
    defaults.update(overrides)
    return WikipediaEvent(**defaults)


class TestWikipediaEventJsonRoundTrip:
    def test_round_trip_preserves_all_fields(self):
        event = _sample_event()
        restored = WikipediaEvent.from_json(event.to_json())
        assert restored == event

    def test_round_trip_preserves_none_values(self):
        event = _sample_event(page_id=None, revision_id=None, comment=None)
        restored = WikipediaEvent.from_json(event.to_json())
        assert restored.page_id is None
        assert restored.revision_id is None
        assert restored.comment is None

    def test_to_dict_matches_fields(self):
        event = _sample_event()
        data = event.to_dict()
        assert data["event_id"] == "abc-123"
        assert data["wiki"] == "enwiki"
        assert data["is_bot"] is False
