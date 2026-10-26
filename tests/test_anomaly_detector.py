"""Tests for processing.anomaly_detector: median/MAD scoring, the
insufficient-history count-based fallback, and PageHistoryStore behavior."""

from __future__ import annotations

import pytest

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

    def test_one_extra_edit_on_a_sparsely_edited_page_is_not_anomalous(self):
        config = make_config(min_history_windows=5, mad_threshold=3.5, mad_epsilon=1.0)
        history = [1, 1, 1, 1, 1, 1, 1, 1]

        result = compute_anomaly(current_edit_count=2, history=history, config=config)

        assert result.is_anomaly is False

    def test_real_spike_on_a_sparsely_edited_page_is_still_anomalous(self):
        config = make_config(min_history_windows=5, mad_threshold=3.5, mad_epsilon=1.0)
        history = [1, 1, 1, 1, 1, 1, 1, 1]

        result = compute_anomaly(current_edit_count=10, history=history, config=config)

        assert result.is_anomaly is True
        assert result.anomaly_score == pytest.approx(0.6745 * 9)

    def test_large_established_pages_do_not_distort_a_typical_pages_baseline(self):
        # The reason for median/MAD: unlike a mean/stddev z-score, it is
        # robust to a couple of extreme values.
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


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_store(clock: FakeClock, ttl: float = 100.0, cleanup_interval: float = 10.0) -> PageHistoryStore:
    config = make_config(history_ttl_seconds=ttl, history_cleanup_interval_seconds=cleanup_interval)
    return PageHistoryStore(config, clock=clock)


class TestPageHistoryEviction:
    def test_recently_recorded_page_survives_a_sweep(self):
        clock = FakeClock()
        store = make_store(clock, ttl=100)
        store.record("enwiki", "Active", 3)

        clock.advance(99)
        store.evict_inactive_pages()

        assert store.get_history("enwiki", "Active") == [3.0]

    def test_page_inactive_for_the_ttl_is_evicted(self):
        clock = FakeClock()
        store = make_store(clock, ttl=100)
        store.record("enwiki", "Idle", 3)

        clock.advance(100)
        evicted = store.evict_inactive_pages()

        assert evicted == 1
        assert store.active_pages == 0

    def test_recording_a_page_again_resets_its_inactivity_timer(self):
        clock = FakeClock()
        store = make_store(clock, ttl=100)
        store.record("enwiki", "Page", 1)
        clock.advance(80)
        store.record("enwiki", "Page", 2)

        clock.advance(80)
        store.evict_inactive_pages()

        assert store.get_history("enwiki", "Page") == [1.0, 2.0]

    def test_only_inactive_pages_are_evicted_when_both_kinds_exist(self):
        clock = FakeClock()
        store = make_store(clock, ttl=100)
        store.record("enwiki", "Idle", 1)
        store.record("enwiki", "Busy", 1)
        clock.advance(60)
        store.record("enwiki", "Busy", 2)

        clock.advance(50)
        store.evict_inactive_pages()

        assert store.get_history("enwiki", "Idle") == []
        assert store.get_history("enwiki", "Busy") == [1.0, 2.0]

    def test_evicted_page_starts_over_with_an_empty_history(self):
        clock = FakeClock()
        store = make_store(clock, ttl=100)
        store.record("enwiki", "Page", 5)
        clock.advance(100)
        store.evict_inactive_pages()

        store.record("enwiki", "Page", 7)

        assert store.get_history("enwiki", "Page") == [7.0]

    def test_evicted_page_is_judged_by_the_count_fallback_again(self):
        clock = FakeClock()
        config = make_config(
            min_history_windows=2, fallback_edit_count_threshold=10,
            history_ttl_seconds=100, history_cleanup_interval_seconds=10,
        )
        store = PageHistoryStore(config, clock=clock)
        for count in (1, 1, 1):
            store.record("enwiki", "Page", count)
        clock.advance(100)
        store.evict_inactive_pages()

        result = store.evaluate_and_record("enwiki", "Page", 4)

        assert result.anomaly_type == "none"
        assert result.threshold_used == 10.0

    def test_reading_history_does_not_create_a_page(self):
        store = make_store(FakeClock())

        store.get_history("enwiki", "Never Edited")

        assert store.active_pages == 0

    def test_recording_sweeps_automatically_once_the_cleanup_interval_passes(self):
        clock = FakeClock()
        store = make_store(clock, ttl=100, cleanup_interval=10)
        store.record("enwiki", "Idle", 1)

        clock.advance(150)
        store.record("enwiki", "Other", 1)

        assert store.get_history("enwiki", "Idle") == []
        assert store.evicted_pages_total == 1

    def test_sweeping_an_empty_store_repeatedly_is_harmless(self):
        store = make_store(FakeClock())

        assert store.evict_inactive_pages() == 0
        assert store.evict_inactive_pages() == 0
        assert store.evicted_pages_total == 0

    def test_page_with_a_window_every_slide_is_never_evicted(self):
        clock = FakeClock()
        store = make_store(clock, ttl=420, cleanup_interval=60)

        for _ in range(500):
            store.record("enwiki", "Steady", 2)
            clock.advance(60)

        assert store.active_pages == 1
        assert store.evicted_pages_total == 0
        assert len(store.get_history("enwiki", "Steady")) == 48

    def test_many_distinct_pages_stay_bounded_by_the_ttl(self):
        clock = FakeClock()
        store = make_store(clock, ttl=100, cleanup_interval=10)
        peak_active_pages = 0

        for page_number in range(10_000):
            store.record("enwiki", f"Page {page_number}", 1)
            peak_active_pages = max(peak_active_pages, store.active_pages)
            clock.advance(1)

        # One new page per second: at most TTL + cleanup interval pages can
        # be waiting for eviction at any time.
        assert peak_active_pages <= 111
        assert store.evicted_pages_total + store.active_pages == 10_000

    def test_observation_count_reflects_evictions(self):
        clock = FakeClock()
        store = make_store(clock, ttl=100)
        store.record("enwiki", "Idle", 1)
        store.record("enwiki", "Idle", 2)
        clock.advance(50)
        store.record("enwiki", "Busy", 3)
        assert store.observation_count() == 3

        clock.advance(60)
        store.evict_inactive_pages()

        assert store.observation_count() == 1
