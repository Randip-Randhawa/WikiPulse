"""Tests for processing.edit_war_detector: reciprocal (mutual) revert
detection, as distinct from a simple revert-count heuristic (Lab 2 Section
6/19: high revert counts alone do not constitute an edit war)."""

from __future__ import annotations

from common.config import EditWarConfig
from processing.edit_war_detector import EditRecord, detect_edit_war


def make_config(**overrides) -> EditWarConfig:
    defaults = dict(
        mutual_revert_threshold=2,
        min_edits_in_window=4,
        score_mutual_weight=2.0,
        score_burst_weight=0.5,
        flag_score_threshold=3.0,
    )
    defaults.update(overrides)
    return EditWarConfig(**defaults)


def rec(user, revision_id=None, is_revert=False, target=None, delta=10, ts=0.0):
    return EditRecord(
        user=user,
        revision_id=revision_id,
        is_revert=is_revert,
        revert_target_revision_id=target,
        byte_length_delta=delta,
        timestamp=ts,
    )


class TestDetectEditWar:
    def test_no_reverts_produces_no_signal(self):
        config = make_config()
        records = [
            rec("Alice", revision_id=1, ts=0),
            rec("Bob", revision_id=2, ts=1),
        ]
        result = detect_edit_war(records, config)

        assert result.mutual_revert_count == 0
        assert result.flag is False
        assert result.total_reverts == 0

    def test_one_directional_reverts_are_not_flagged_as_edit_war(self):
        """A single editor reverting several unrelated edits should NOT be
        treated as an edit war -- there is no reciprocal behavior."""
        config = make_config()
        records = [
            rec("Alice", revision_id=1, ts=0),
            rec("Bob", revision_id=2, ts=1),
            rec("Cleanup", revision_id=3, is_revert=True, target=1, ts=2),
            rec("Cleanup", revision_id=4, is_revert=True, target=2, ts=3),
        ]
        result = detect_edit_war(records, config)

        assert result.mutual_revert_count == 0
        assert result.flag is False
        # Raw revert count is still tracked, just not conflated with edit-war.
        assert result.total_reverts == 2

    def test_reciprocal_reverts_between_two_editors_are_detected(self):
        config = make_config(mutual_revert_threshold=2)
        records = [
            rec("Alice", revision_id=100, ts=0),
            rec("Bob", revision_id=101, is_revert=True, target=100, ts=1),
            rec("Alice", revision_id=102, is_revert=True, target=101, ts=2),
        ]
        result = detect_edit_war(records, config)

        assert result.mutual_revert_count >= 2
        assert {result.editor_a, result.editor_b} == {"Alice", "Bob"}
        assert result.flag is True

    def test_repeated_reciprocal_reverts_increase_conflict_score(self):
        config = make_config(mutual_revert_threshold=2, score_mutual_weight=2.0)
        few_records = [
            rec("Alice", revision_id=1, ts=0),
            rec("Bob", revision_id=2, is_revert=True, target=1, ts=1),
            rec("Alice", revision_id=3, is_revert=True, target=2, ts=2),
        ]
        many_records = few_records + [
            rec("Bob", revision_id=4, is_revert=True, target=3, ts=3),
            rec("Alice", revision_id=5, is_revert=True, target=4, ts=4),
        ]

        few_result = detect_edit_war(few_records, config)
        many_result = detect_edit_war(many_records, config)

        assert many_result.mutual_revert_count > few_result.mutual_revert_count
        assert many_result.conflict_score > few_result.conflict_score

    def test_revert_target_outside_window_has_no_resolvable_author(self):
        """If the reverted revision's author isn't visible in this window,
        we can't attribute the revert to a specific pair -- must not crash
        and must not fabricate a mutual pair."""
        config = make_config()
        records = [
            rec("Alice", revision_id=200, is_revert=True, target=999, ts=0),
        ]
        result = detect_edit_war(records, config)

        assert result.mutual_revert_count == 0
        assert result.editor_a is None
        assert result.flag is False

    def test_empty_records_produce_a_safe_zero_signal(self):
        config = make_config()
        result = detect_edit_war([], config)

        assert result.mutual_revert_count == 0
        assert result.total_reverts == 0
        assert result.edit_burst_count == 0
        assert result.flag is False

    def test_self_revert_is_not_counted_as_a_conflict_edge(self):
        config = make_config()
        records = [
            rec("Alice", revision_id=1, ts=0),
            rec("Alice", revision_id=2, is_revert=True, target=1, ts=1),
        ]
        result = detect_edit_war(records, config)
        assert result.mutual_revert_count == 0
        assert result.flag is False

    def test_edit_burst_counts_negative_byte_delta_edits_above_min_window_size(self):
        config = make_config(min_edits_in_window=3, score_burst_weight=0.5, mutual_revert_threshold=99)
        records = [
            rec("Alice", revision_id=1, delta=-50, ts=0),
            rec("Bob", revision_id=2, delta=-30, ts=1),
            rec("Carol", revision_id=3, delta=10, ts=2),
        ]
        result = detect_edit_war(records, config)
        assert result.edit_burst_count == 2
