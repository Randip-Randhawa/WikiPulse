"""Tests for ingestion.normalizer: event normalization, missing/malformed
field handling, revert detection, and the wiki/type ignore filter."""

from __future__ import annotations

import pytest

from ingestion.normalizer import MalformedEventError, normalize_event, should_ignore


def _base_raw_event(**overrides) -> dict:
    event = {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {"id": "abc-123"},
        "id": 999888777,
        "type": "edit",
        "namespace": 0,
        "title": "Python (programming language)",
        "comment": "Fixed a typo",
        "timestamp": 1_700_000_000,
        "user": "AliceEditor",
        "userid": 12345,
        "bot": False,
        "minor": False,
        "length": {"old": 5000, "new": 5050},
        "revision": {"old": 111, "new": 112},
        "server_name": "en.wikipedia.org",
        "wiki": "enwiki",
        "tags": [],
    }
    event.update(overrides)
    return event


class TestNormalizeEvent:
    def test_normalizes_a_well_formed_edit_event(self):
        raw = _base_raw_event()
        event = normalize_event(raw)

        assert event.wiki == "enwiki"
        assert event.page_title == "Python (programming language)"
        assert event.event_type == "edit"
        assert event.user == "AliceEditor"
        assert event.user_id == 12345
        assert event.is_bot is False
        assert event.revision_id == 112
        assert event.previous_revision_id == 111
        assert event.byte_length_new == 5050
        assert event.byte_length_old == 5000
        assert event.byte_length_delta == 50
        assert event.is_revert is False
        assert event.ingestion_timestamp is not None

    def test_bot_flag_is_preserved(self):
        raw = _base_raw_event(bot=True, user="CleanupBot")
        event = normalize_event(raw)
        assert event.is_bot is True
        assert event.user == "CleanupBot"

    def test_missing_optional_fields_are_handled_safely(self):
        raw = _base_raw_event()
        del raw["length"]
        del raw["revision"]
        del raw["userid"]
        raw["user"] = None

        event = normalize_event(raw)

        assert event.byte_length_new is None
        assert event.byte_length_old is None
        assert event.byte_length_delta is None
        assert event.revision_id is None
        assert event.previous_revision_id is None
        assert event.user_id is None
        assert event.user is None

    def test_missing_required_field_raises_malformed_event_error(self):
        raw = _base_raw_event()
        del raw["title"]
        with pytest.raises(MalformedEventError):
            normalize_event(raw)

    def test_missing_wiki_raises_malformed_event_error(self):
        raw = _base_raw_event()
        del raw["wiki"]
        del raw["server_name"]
        with pytest.raises(MalformedEventError):
            normalize_event(raw)

    def test_invalid_timestamp_raises_malformed_event_error(self):
        raw = _base_raw_event(timestamp="not-a-timestamp")
        with pytest.raises(MalformedEventError):
            normalize_event(raw)

    def test_non_dict_payload_raises_malformed_event_error(self):
        with pytest.raises(MalformedEventError):
            normalize_event(["not", "a", "dict"])  # type: ignore[arg-type]

    def test_revert_tag_marks_is_revert(self):
        raw = _base_raw_event(
            tags=["mw-rollback"],
            comment="Undid revision 111 by [[Special:Contributions/Bob]]",
        )
        event = normalize_event(raw)
        assert event.is_revert is True
        assert event.revert_target_revision_id == 111

    def test_revert_comment_heuristic_without_tags(self):
        raw = _base_raw_event(tags=[], comment="Reverted edits by Bob to last version by Alice")
        event = normalize_event(raw)
        assert event.is_revert is True

    def test_non_revert_edit_is_not_flagged(self):
        raw = _base_raw_event(tags=[], comment="Added a new section on history")
        event = normalize_event(raw)
        assert event.is_revert is False
        assert event.revert_target_revision_id is None


    def test_revert_tag_mw_revert_marks_is_revert(self):
        raw = _base_raw_event(tags=["mw-revert"])
        event = normalize_event(raw)
        assert event.is_revert is True

    def test_user_type_classification(self):
        bot_event = normalize_event(_base_raw_event(bot=True, user="HelperBot"))
        assert bot_event.user_type == "BOT"

        ipv4_event = normalize_event(_base_raw_event(bot=False, user="192.168.1.100"))
        assert ipv4_event.user_type == "ANONYMOUS"

        ipv6_event = normalize_event(_base_raw_event(bot=False, user="2001:0db8:85a3:0000:0000:8a2e:0370:7334"))
        assert ipv6_event.user_type == "ANONYMOUS"

        reg_event = normalize_event(_base_raw_event(bot=False, user="RegisteredUser"))
        assert reg_event.user_type == "REGISTERED"

    def test_provenance_id_generation(self):
        event = normalize_event(_base_raw_event())
        assert event.provenance_id is not None
        assert "part-0:off-0" in event.provenance_id
        assert "ingest-node-" in event.provenance_id

    def test_large_jump_flagging(self):
        normal_event = normalize_event(_base_raw_event(length={"old": 5000, "new": 5050}))
        assert normal_event.is_large_jump is False

        large_jump_event = normalize_event(_base_raw_event(length={"old": 5000, "new": 110_000}))
        assert large_jump_event.is_large_jump is True

    def test_minor_and_comment_length(self):
        event = normalize_event(_base_raw_event(minor=True, comment="Short comment"))
        assert event.is_minor is True
        assert event.comment_length == 13


class TestShouldIgnore:
    def test_non_mainspace_events_are_ignored(self):
        talk_event = _base_raw_event(type="edit", namespace=3)
        assert should_ignore(talk_event, wiki_filter=[]) is True

        cat_event = _base_raw_event(type="edit", namespace=14)
        assert should_ignore(cat_event, wiki_filter=[]) is True

    def test_mainspace_events_are_not_ignored(self):
        main_event = _base_raw_event(type="edit", namespace=0)
        assert should_ignore(main_event, wiki_filter=[]) is False

    def test_log_events_are_ignored(self):
        raw = _base_raw_event(type="log")
        assert should_ignore(raw, wiki_filter=[]) is True

    def test_categorize_events_are_ignored(self):
        raw = _base_raw_event(type="categorize")
        assert should_ignore(raw, wiki_filter=[]) is True

    def test_edit_events_are_not_ignored_without_filter(self):
        raw = _base_raw_event(type="edit")
        assert should_ignore(raw, wiki_filter=[]) is False

    def test_wiki_filter_excludes_non_matching_wikis(self):
        raw = _base_raw_event(wiki="dewiki")
        assert should_ignore(raw, wiki_filter=["enwiki"]) is True

    def test_wiki_filter_keeps_matching_wikis(self):
        raw = _base_raw_event(wiki="enwiki")
        assert should_ignore(raw, wiki_filter=["enwiki"]) is False
