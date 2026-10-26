"""Event-time sliding windows emulated in Python for the detection benchmark.

The benchmark does not start Spark. This reproduces the windowing the
streaming job asks Spark for -- sliding windows per (wiki, page_title), each
released once, in append mode, when the watermark passes its end -- so that
`processing.streaming_job.evaluate_windows` receives rows shaped like the
ones Spark hands to it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from common.models import WikipediaEvent


class SlidingWindows:
    def __init__(self, window_seconds: float, slide_seconds: float, watermark_delay_seconds: float):
        self._window_seconds = window_seconds
        self._slide_seconds = slide_seconds
        self._watermark_delay_seconds = watermark_delay_seconds
        self._open: dict[tuple[str, str, float], dict[str, Any]] = {}
        self._max_event_ts = float("-inf")
        self.late_events_dropped = 0

    @property
    def watermark(self) -> float:
        return self._max_event_ts - self._watermark_delay_seconds

    @property
    def open_windows(self) -> int:
        return len(self._open)

    def add(self, event: WikipediaEvent) -> None:
        """Add an event to every window covering its time. Like Spark, a
        window the watermark has already closed no longer accepts events."""
        watermark = self.watermark
        self._max_event_ts = max(self._max_event_ts, event.timestamp)
        record = {
            "user": event.user,
            "revision_id": event.revision_id,
            "is_bot": event.is_bot,
            "is_revert": event.is_revert,
            "revert_target_revision_id": event.revert_target_revision_id,
            "byte_length_delta": event.byte_length_delta,
            "timestamp": event.timestamp,
        }
        accepted = False
        start = event.timestamp - event.timestamp % self._slide_seconds
        while start > event.timestamp - self._window_seconds:
            if start + self._window_seconds > watermark:
                key = (event.wiki, event.page_title, start)
                window = self._open.setdefault(key, {"page_id": event.page_id, "records": []})
                window["records"].append(record)
                accepted = True
            start -= self._slide_seconds
        if not accepted:
            self.late_events_dropped += 1

    def pop_finalized(self, flush_all: bool = False) -> list[dict[str, Any]]:
        """Remove and return the windows whose end the watermark has passed
        (or every open window when `flush_all`), in Spark's row shape."""
        watermark = self.watermark
        finalized_keys = [
            key for key in self._open
            if flush_all or key[2] + self._window_seconds <= watermark
        ]
        rows = []
        for key in finalized_keys:
            wiki, page_title, start = key
            window = self._open.pop(key)
            rows.append({
                "window": {
                    "start": datetime.fromtimestamp(start, tz=timezone.utc),
                    "end": datetime.fromtimestamp(start + self._window_seconds, tz=timezone.utc),
                },
                "wiki": wiki,
                "page_title": page_title,
                "page_id": window["page_id"],
                "records": window["records"],
            })
        return rows
