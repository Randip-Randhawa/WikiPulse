"""Activity-anomaly detection: median + Median Absolute Deviation (MAD).

Lab 1 originally specified a plain z-score threshold. Lab 2 explicitly
replaces that with a robust median/MAD approach (see project spec Section
5 and Section 20) because a small number of extremely active Wikipedia
pages ("Main Page", breaking-news articles, etc.) would otherwise distort
a mean/stddev-based baseline for everyone else.

Robust z-score (a.k.a. modified z-score), per page:

    median = median(recent window edit counts)
    MAD    = median(|edit_count_i - median|)
    score  = 0.6745 * (current_edit_count - median) / MAD

The constant 0.6745 makes the modified z-score comparable in scale to a
standard z-score under a normal distribution, which is the standard
convention for MAD-based robust outlier scoring.

Insufficient history fallback: a brand-new or rarely-edited page has no
meaningful baseline to compare against. Per Lab 2, such pages instead use
a simple, configurable count-based rule (`ANOMALY_FALLBACK_EDIT_COUNT_THRESHOLD`)
rather than forcing a statistically unreliable median/MAD computation on
too little data.

This module is deliberately pure-Python/stateless-per-call so it can be
unit tested in isolation from Spark and reused by both the streaming job
and any batch backfill script. Per-page rolling history is maintained by
`PageHistoryStore`, a simple bounded in-memory store updated once per
micro-batch by the streaming job's driver (see processing/streaming_job.py
for why this is an appropriate, deliberately simple design for a
local/academic-scale project rather than a distributed streaming K/V store).
"""

from __future__ import annotations

import statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple

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

    # Guard against a zero MAD (e.g. a page with perfectly constant activity)
    # which would otherwise produce a divide-by-zero / infinite score.
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


class PageHistoryStore:
    """Bounded rolling history of per-window edit counts, keyed by
    (wiki, page_title). Updated once per micro-batch on the Spark driver.

    This is an intentionally simple, single-process, in-memory structure.
    It is appropriate here because:
    - The project runs as a single local Spark driver (no multi-node
      distribution requiring shared state).
    - History only needs to span `ANOMALY_HISTORY_LOOKBACK_WINDOWS` windows
      (a bounded, small amount of memory per page).
    A production system would likely externalize this to a fast key-value
    store; that is explicitly out of scope for this academic project.
    """

    def __init__(self, config: AnomalyConfig):
        self._config = config
        self._history: Dict[Tuple[str, str], Deque[float]] = defaultdict(
            lambda: deque(maxlen=config.history_lookback_windows)
        )

    def get_history(self, wiki: str, page_title: str) -> list[float]:
        return list(self._history[(wiki, page_title)])

    def record(self, wiki: str, page_title: str, edit_count: int) -> None:
        self._history[(wiki, page_title)].append(float(edit_count))

    def evaluate_and_record(
        self, wiki: str, page_title: str, current_edit_count: int
    ) -> AnomalyResult:
        """Convenience helper: evaluate against current history, then
        record the current observation for future windows."""
        history = self.get_history(wiki, page_title)
        result = compute_anomaly(current_edit_count, history, self._config)
        self.record(wiki, page_title, current_edit_count)
        return result
