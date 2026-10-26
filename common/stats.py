"""Summary statistics shared by the pipeline metrics and the benchmark runner."""

from __future__ import annotations

import math
from typing import Iterable, Optional, Sequence

MILLISECONDS_PER_SECOND = 1000.0
LATENCY_PERCENTILES = (50, 95, 99)


def percentile(sorted_values: Sequence[float], percent: float) -> float:
    """Return the `percent`-th percentile of already sorted values,
    interpolating linearly between the two nearest ranks (numpy's default)."""
    if not sorted_values:
        raise ValueError("percentile of an empty sequence")
    if not 0 <= percent <= 100:
        raise ValueError(f"percent must be between 0 and 100, got {percent}")
    position = (len(sorted_values) - 1) * percent / 100
    lower = math.floor(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction


def summarize_ms(durations_seconds: Iterable[float]) -> Optional[dict[str, float]]:
    """Summarize durations given in seconds as count, p50/p95/p99, max and
    mean, all in milliseconds. Returns None when there are no values, so a
    missing measurement is reported as unknown rather than as zero."""
    values = sorted(seconds * MILLISECONDS_PER_SECOND for seconds in durations_seconds)
    if not values:
        return None
    summary: dict[str, float] = {"count": len(values)}
    for percent in LATENCY_PERCENTILES:
        summary[f"p{percent}"] = round(percentile(values, percent), 3)
    summary["max"] = round(values[-1], 3)
    summary["mean"] = round(sum(values) / len(values), 3)
    return summary
