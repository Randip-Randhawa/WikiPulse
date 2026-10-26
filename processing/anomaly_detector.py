"""Activity-anomaly detection: median + Median Absolute Deviation (MAD).

A plain mean/stddev z-score is not used because a few extremely active
windows (breaking-news edits, a vandalism wave) would inflate the baseline
and hide later spikes. The median and MAD are barely moved by such
outliers.

Robust z-score (a.k.a. modified z-score), per page:

    median = median(recent window edit counts)
    MAD    = median(|edit_count_i - median|)
    score  = 0.6745 * (current_edit_count - median) / MAD

The constant 0.6745 makes the modified z-score comparable in scale to a
standard z-score under a normal distribution, which is the standard
convention for MAD-based robust outlier scoring.

Insufficient history fallback: a brand-new or rarely-edited page has no
meaningful baseline to compare against. Such pages instead use a simple,
configurable count-based rule (`ANOMALY_FALLBACK_EDIT_COUNT_THRESHOLD`)
rather than forcing a statistically unreliable median/MAD computation on
too little data.

This module is deliberately pure-Python/stateless-per-call so it can be
unit tested in isolation from Spark and reused by both the streaming job
and any batch backfill script. Per-page rolling history is maintained by
`PageHistoryStore`, a bounded in-memory store updated by the streaming
job's driver as windows are finalized.
"""

from __future__ import annotations

import statistics
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import Callable, Deque, Optional, Tuple

from common.config import AnomalyConfig

# Standard constant relating MAD to a normal-distribution standard deviation,
# so the "modified z-score" is on a comparable scale to a classic z-score.
_MAD_SCALE_CONSTANT = 0.6745


@dataclass(frozen=True)
class AnomalyResult:
    is_anomaly: bool
    anomaly_type: str  # "median_mad" | "count_fallback" | "none"
    anomaly_score: Optional[float]
    baseline_median: Optional[float]
    baseline_mad: Optional[float]
    threshold_used: Optional[float]


def median_absolute_deviation(values: list[float], median_value: float) -> float:
    """Return MAD = median(|x_i - median|) for the given values."""
    deviations = [abs(v - median_value) for v in values]
    return statistics.median(deviations) if deviations else 0.0


def compute_anomaly(
    current_edit_count: int,
    history: list[float],
    config: AnomalyConfig,
) -> AnomalyResult:
    """Evaluate whether `current_edit_count` is anomalous relative to
    `history` (a list of edit counts from prior windows for the same page,
    oldest first, NOT including the current window).

    Uses median/MAD when enough history exists; otherwise falls back to a
    simple count-based rule (see module docstring).
    """
    if len(history) < config.min_history_windows:
        is_anomaly = current_edit_count >= config.fallback_edit_count_threshold
        return AnomalyResult(
            is_anomaly=is_anomaly,
            anomaly_type="count_fallback" if is_anomaly else "none",
            anomaly_score=float(current_edit_count),
            baseline_median=None,
            baseline_mad=None,
            threshold_used=float(config.fallback_edit_count_threshold),
        )

    baseline_median = statistics.median(history)
    baseline_mad = median_absolute_deviation(history, baseline_median)

    # Edit counts are whole numbers, so a MAD below one edit (the default
    # floor) says nothing about spread. Without the floor, a sparsely edited
    # page whose history is all 1s has MAD 0, and a window of 2 edits would
    # score in the hundreds of thousands.
    effective_mad = baseline_mad if baseline_mad > config.mad_epsilon else config.mad_epsilon

    score = _MAD_SCALE_CONSTANT * (current_edit_count - baseline_median) / effective_mad
    is_anomaly = score >= config.mad_threshold

    return AnomalyResult(
        is_anomaly=is_anomaly,
        anomaly_type="median_mad" if is_anomaly else "none",
        anomaly_score=round(score, 4),
        baseline_median=baseline_median,
        baseline_mad=baseline_mad,
        threshold_used=config.mad_threshold,
    )


@dataclass
class _PageHistory:
    counts: Deque[float]
    last_recorded_at: float


class PageHistoryStore:
    """Bounded rolling history of per-window edit counts, keyed by
    (wiki, page_title). Updated on the Spark driver as windows are finalized.

    This is an intentionally simple, single-process, in-memory structure.
    It is appropriate here because:
    - The project runs as a single local Spark driver (no multi-node
      distribution requiring shared state).
    - Each page keeps at most `ANOMALY_HISTORY_LOOKBACK_WINDOWS` counts.
    A production system would likely externalize this to a fast key-value
    store; that is explicitly out of scope for this academic project.

    The number of pages is bounded by evicting any page that has not had a
    window recorded for `ANOMALY_HISTORY_TTL_SECONDS`; otherwise every page
    ever edited would stay in memory. Inactivity is measured on the
    injected monotonic clock, not event time, so a replayed backlog or a
    wall-clock jump cannot evict pages early. An evicted page that becomes
    active again starts with an empty history and is judged by the
    count-based fallback until it has enough windows again.

    Only finalized windows are recorded here; windows still filling up
    live in Spark's own state, which the watermark bounds.
    """

    def __init__(self, config: AnomalyConfig, clock: Callable[[], float] = time.monotonic):
        self._config = config
        self._clock = clock
        # Least recently recorded page first, so expired pages are always
        # at the front and a sweep stops at the first page still active.
        self._pages: OrderedDict[Tuple[str, str], _PageHistory] = OrderedDict()
        self._last_sweep_at = clock()
        self.evicted_pages_total = 0

    @property
    def active_pages(self) -> int:
        return len(self._pages)

    def observation_count(self) -> int:
        """Total window counts held across all pages (walks every page)."""
        return sum(len(page.counts) for page in self._pages.values())

    def get_history(self, wiki: str, page_title: str) -> list[float]:
        page = self._pages.get((wiki, page_title))
        return list(page.counts) if page else []

    def record(self, wiki: str, page_title: str, edit_count: int) -> None:
        now = self._clock()
        key = (wiki, page_title)
        page = self._pages.get(key)
        if page is None:
            page = _PageHistory(deque(maxlen=self._config.history_lookback_windows), now)
            self._pages[key] = page
        else:
            page.last_recorded_at = now
            self._pages.move_to_end(key)
        page.counts.append(float(edit_count))

        if now - self._last_sweep_at >= self._config.history_cleanup_interval_seconds:
            self.evict_inactive_pages()

    def evict_inactive_pages(self) -> int:
        """Drop pages with no recorded window for at least the TTL and
        return how many were dropped."""
        now = self._clock()
        cutoff = now - self._config.history_ttl_seconds
        evicted = 0
        while self._pages:
            oldest_page = next(iter(self._pages.values()))
            if oldest_page.last_recorded_at > cutoff:
                break
            self._pages.popitem(last=False)
            evicted += 1
        self.evicted_pages_total += evicted
        self._last_sweep_at = now
        return evicted

    def evaluate_and_record(
        self, wiki: str, page_title: str, current_edit_count: int
    ) -> AnomalyResult:
        """Convenience helper: evaluate against current history, then
        record the current observation for future windows."""
        history = self.get_history(wiki, page_title)
        result = compute_anomaly(current_edit_count, history, self._config)
        self.record(wiki, page_title, current_edit_count)
        return result
