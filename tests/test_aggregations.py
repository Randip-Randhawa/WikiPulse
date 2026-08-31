"""Tests for processing.aggregations: bot/human classification and
per-window activity metric computation."""

from __future__ import annotations

from processing.aggregations import PageWindowEvent, aggregate_page_window, is_human_edit


class TestIsHumanEdit:
    def test_non_bot_flag_is_human(self):
        assert is_human_edit(is_bot=False) is True

    def test_bot_flag_is_not_human(self):
        assert is_human_edit(is_bot=True) is False


class TestAggregatePageWindow:
    def test_empty_window_produces_zeroed_metrics(self):
        result = aggregate_page_window([], window_minutes=5)
        assert result.edit_count == 0
        assert result.unique_editors == 0
        assert result.human_edits == 0
        assert result.bot_edits == 0
        assert result.revert_count == 0
        assert result.net_byte_delta == 0
        assert result.edit_velocity == 0.0

    def test_counts_human_and_bot_edits_separately(self):
        events = [
            PageWindowEvent(user="Alice", is_bot=False, is_revert=False, byte_length_delta=10),
            PageWindowEvent(user="Bob", is_bot=False, is_revert=False, byte_length_delta=20),
            PageWindowEvent(user="CleanupBot", is_bot=True, is_revert=False, byte_length_delta=5),
        ]
        result = aggregate_page_window(events, window_minutes=5)

        assert result.edit_count == 3
        assert result.human_edits == 2
        assert result.bot_edits == 1

    def test_unique_editors_deduplicates_repeat_editors(self):
        events = [
            PageWindowEvent(user="Alice", is_bot=False, is_revert=False, byte_length_delta=1),
            PageWindowEvent(user="Alice", is_bot=False, is_revert=False, byte_length_delta=1),
            PageWindowEvent(user="Bob", is_bot=False, is_revert=False, byte_length_delta=1),
        ]
        result = aggregate_page_window(events, window_minutes=5)
        assert result.unique_editors == 2
        assert result.edit_count == 3

    def test_missing_user_is_not_counted_as_a_unique_editor(self):
        events = [
            PageWindowEvent(user=None, is_bot=False, is_revert=False, byte_length_delta=1),
            PageWindowEvent(user="Bob", is_bot=False, is_revert=False, byte_length_delta=1),
        ]
        result = aggregate_page_window(events, window_minutes=5)
        assert result.unique_editors == 1

    def test_revert_count_and_net_byte_delta(self):
        events = [
            PageWindowEvent(user="Alice", is_bot=False, is_revert=True, byte_length_delta=-100),
            PageWindowEvent(user="Bob", is_bot=False, is_revert=False, byte_length_delta=50),
        ]
        result = aggregate_page_window(events, window_minutes=5)
        assert result.revert_count == 1
        assert result.net_byte_delta == -50

    def test_missing_byte_delta_is_treated_as_zero(self):
        events = [
            PageWindowEvent(user="Alice", is_bot=False, is_revert=False, byte_length_delta=None),
            PageWindowEvent(user="Bob", is_bot=False, is_revert=False, byte_length_delta=30),
        ]
        result = aggregate_page_window(events, window_minutes=5)
        assert result.net_byte_delta == 30

    def test_edit_velocity_is_edits_per_minute(self):
        events = [
            PageWindowEvent(user="Alice", is_bot=False, is_revert=False, byte_length_delta=1)
            for _ in range(10)
        ]
        result = aggregate_page_window(events, window_minutes=5)
        assert result.edit_velocity == 2.0

    def test_zero_window_minutes_does_not_divide_by_zero(self):
        events = [PageWindowEvent(user="Alice", is_bot=False, is_revert=False, byte_length_delta=1)]
        result = aggregate_page_window(events, window_minutes=0)
        assert result.edit_velocity == 0.0
