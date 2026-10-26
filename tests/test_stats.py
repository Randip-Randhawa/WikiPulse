"""Tests for common.stats: percentile interpolation and latency summaries."""

from __future__ import annotations

import pytest

from common.stats import percentile, summarize_ms


class TestPercentile:
    def test_median_of_odd_count_is_the_middle_value(self):
        assert percentile([1, 2, 3, 4, 5], 50) == 3

    def test_median_of_even_count_interpolates_between_middle_values(self):
        assert percentile([1, 2, 3, 4], 50) == 2.5

    def test_p95_interpolates_between_nearest_ranks(self):
        values = list(range(1, 101))  # 1..100
        assert percentile(values, 95) == pytest.approx(95.05)

    def test_single_value_is_every_percentile(self):
        assert percentile([7.0], 0) == 7.0
        assert percentile([7.0], 99) == 7.0

    def test_p0_and_p100_are_min_and_max(self):
        assert percentile([2, 4, 8], 0) == 2
        assert percentile([2, 4, 8], 100) == 8

    def test_empty_input_raises(self):
        with pytest.raises(ValueError):
            percentile([], 50)

    def test_percent_outside_0_to_100_raises(self):
        with pytest.raises(ValueError):
            percentile([1, 2], 101)


class TestSummarizeMs:
    def test_converts_seconds_to_milliseconds(self):
        summary = summarize_ms([0.010, 0.020, 0.030])

        assert summary["count"] == 3
        assert summary["p50"] == 20.0
        assert summary["max"] == 30.0
        assert summary["mean"] == 20.0

    def test_unordered_input_gives_the_same_percentiles(self):
        assert summarize_ms([0.3, 0.1, 0.2]) == summarize_ms([0.1, 0.2, 0.3])

    def test_no_values_is_unknown_not_zero(self):
        assert summarize_ms([]) is None
