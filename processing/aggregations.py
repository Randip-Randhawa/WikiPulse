"""Pure-Python aggregation helpers for page-level activity metrics.

These operate on plain lists/dicts of events for a single (wiki, page,
window) group -- the shape Spark hands to a `pandas`/Python UDF or that a
`foreachBatch` callback assembles after grouping a micro-batch DataFrame.
Keeping this logic outside of Spark-specific code makes it directly unit
testable (see tests/test_aggregations.py conventions in tests/).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass(frozen=True)
class PageWindowEvent:
    """Minimal event shape needed for window aggregation (a subset of
    common.models.WikipediaEvent's fields)."""

    user: Optional[str]
    is_bot: bool
    is_revert: bool
    byte_length_delta: Optional[int]


@dataclass(frozen=True)
class PageActivityMetrics:
    edit_count: int
    unique_editors: int
    human_edits: int
    bot_edits: int
    revert_count: int
    net_byte_delta: int
    edit_velocity: float  # edits per minute


def is_human_edit(is_bot: bool) -> bool:
    """Classify an edit as human vs bot.

    Wikimedia's `bot` flag on the recentchange stream is set by the
    software when the edit was made through a registered bot account or
    flagged bot session. There is no additional heuristic layer here
    (e.g. NLP/user-agent sniffing) per the project's explicit exclusion of
    ML-based classification -- the authoritative Wikimedia flag is used
    directly.
    """
    return not is_bot


def aggregate_page_window(
    events: Iterable[PageWindowEvent], window_minutes: float
) -> PageActivityMetrics:
    """Compute page-level activity metrics for one (page, window) group."""
    events = list(events)
    edit_count = len(events)
    unique_editors = len({e.user for e in events if e.user})
    human_edits = sum(1 for e in events if is_human_edit(e.is_bot))
    bot_edits = edit_count - human_edits
    revert_count = sum(1 for e in events if e.is_revert)
    net_byte_delta = sum(e.byte_length_delta or 0 for e in events)
    edit_velocity = edit_count / window_minutes if window_minutes > 0 else 0.0

    return PageActivityMetrics(
        edit_count=edit_count,
        unique_editors=unique_editors,
        human_edits=human_edits,
        bot_edits=bot_edits,
        revert_count=revert_count,
        net_byte_delta=net_byte_delta,
        edit_velocity=round(edit_velocity, 4),
    )
