"""Tests for processing.anomaly_detector: median/MAD scoring, the
insufficient-history count-based fallback, and PageHistoryStore behavior."""

from __future__ import annotations

from common.config import AnomalyConfig
from processing.anomaly_detector import (
    PageHistoryStore,
    compute_anomaly,
    median_absolute_deviation,
)


def make_config(**overrides) -> AnomalyConfig:
    defaults = dict(
        mad_threshold=3.5,
        min_history_windows=8,
        fallback_edit_count_threshold=15,
        history_lookback_windows=48,
        mad_epsilon=1e-6,
    )
    defaults.update(overrides)
    return AnomalyConfig(**defaults)


class TestMedianAbsoluteDeviation:
    def test_mad_of_constant_values_is_zero(self):
        assert median_absolute_deviation([5.0, 5.0, 5.0], median_value=5.0) == 0.0

    def test_mad_of_symmetric_values(self):
        # values: 1, 2, 3, 4, 5 -> median 3 -> deviations: 2,1,0,1,2 -> median 1
        assert median_absolute_deviation([1, 2, 3, 4, 5], median_value=3) == 1.0

    def test_mad_of_empty_list_is_zero(self):
        assert median_absolute_deviation([], median_value=0) == 0.0


class TestComputeAnomalyCountFallback:
    def test_insufficient_history_uses_count_fallback(self):
        config = make_config(min_history_windows=8, fallback_edit_count_threshold=15)
        result = compute_anomaly(current_edit_count=20, history=[2, 3, 4], config=config)

        assert result.anomaly_type == "count_fallback"
        assert result.is_anomaly is True
        assert result.baseline_median is None
        assert result.baseline_mad is None
        assert result.threshold_used == 15.0

    def test_insufficient_history_below_fallback_threshold_is_not_anomalous(self):
        config = make_config(min_history_windows=8, fallback_edit_count_threshold=15)
        result = compute_anomaly(current_edit_count=5, history=[2, 3, 4], config=config)

        assert result.anomaly_type == "none"
        assert result.is_anomaly is False

    def test_zero_history_still_uses_fallback_rule(self):
        config = make_config(min_history_windows=8, fallback_edit_count_threshold=10)
        result = compute_anomaly(current_edit_count=12, history=[], config=config)
        assert result.anomaly_type == "count_fallback"
        assert result.is_anomaly is True


class TestComputeAnomalyMedianMad:
    def test_sufficient_history_uses_median_mad_and_flags_spike(self):
        config = make_config(min_history_windows=5, mad_threshold=3.5)
        # Stable baseline around 5 edits/window.
        history = [4, 5, 5, 6, 5, 4, 5, 6]
        result = compute_anomaly(current_edit_count=40, history=history, config=config)

        assert result.anomaly_type == "median_mad"
        assert result.is_anomaly is True
        assert result.baseline_median == 5.0
        assert result.anomaly_score is not None and result.anomaly_score > config.mad_threshold

    def test_sufficient_history_normal_activity_is_not_anomalous(self):
        config = make_config(min_history_windows=5, mad_threshold=3.5)
        history = [4, 5, 5, 6, 5, 4, 5, 6]
        result = compute_anomaly(current_edit_count=6, history=history, config=config)

        assert result.anomaly_type == "none"
        assert result.is_anomaly is False

    def test_zero_mad_does_not_crash_and_uses_epsilon_guard(self):
        # A page with perfectly constant historical activity (MAD == 0).
        config = make_config(min_history_windows=5, mad_threshold=3.5, mad_epsilon=1e-6)
        history = [5, 5, 5, 5, 5, 5]
        result = compute_anomaly(current_edit_count=5, history=history, config=config)
        assert result.is_anomaly is False

        spike_result = compute_anomaly(current_edit_count=50, history=history, config=config)
        assert spike_result.is_anomaly is True
        assert spike_result.anomaly_score is not None

    def test_large_established_pages_do_not_distort_a_typical_pages_baseline(self):
        # Demonstrates the core Lab 2 rationale: median/MAD (unlike a
        # mean/stddev z-score) is robust to a couple of extreme values.
        config = make_config(min_history_windows=5, mad_threshold=3.5)
        history = [5, 6, 5, 4, 5, 500, 5, 6]  # one huge outlier window
        result = compute_anomaly(current_edit_count=7, history=history, config=config)
        # A normal window (7) should NOT be flagged just because one
        # historical outlier is present.
        assert result.is_anomaly is False


class TestPageHistoryStore:
    def test_history_store_records_and_returns_history(self):
        config = make_config(history_lookback_windows=3)
        store = PageHistoryStore(config)

        store.record("enwiki", "Test Page", 5)
        store.record("enwiki", "Test Page", 6)

        assert store.get_history("enwiki", "Test Page") == [5.0, 6.0]

    def test_history_store_respects_lookback_bound(self):
        config = make_config(history_lookback_windows=3)
        store = PageHistoryStore(config)

        for count in [1, 2, 3, 4, 5]:
            store.record("enwiki", "Test Page", count)

        # Only the most recent 3 observations should be retained.
        assert store.get_history("enwiki", "Test Page") == [3.0, 4.0, 5.0]

    def test_evaluate_and_record_updates_history_after_evaluation(self):
        config = make_config(min_history_windows=2, fallback_edit_count_threshold=10)
        store = PageHistoryStore(config)

        first = store.evaluate_and_record("enwiki", "Test Page", 3)
        assert first.anomaly_type in {"none", "count_fallback"}
        assert store.get_history("enwiki", "Test Page") == [3.0]

        store.evaluate_and_record("enwiki", "Test Page", 4)
        assert store.get_history("enwiki", "Test Page") == [3.0, 4.0]

    def test_different_pages_have_independent_history(self):
        config = make_config()
        store = PageHistoryStore(config)
        store.record("enwiki", "Page A", 10)
        store.record("enwiki", "Page B", 20)

        assert store.get_history("enwiki", "Page A") == [10.0]
        assert store.get_history("enwiki", "Page B") == [20.0]
