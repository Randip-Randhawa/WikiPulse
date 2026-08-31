"""The normalized WikipediaEvent schema.

Raw Wikimedia `recentchange` SSE events are heterogeneous: fields are
frequently missing, optional, or only present for certain event types
(edit / new / log / categorize). `WikipediaEvent` is the canonical,
null-safe representation that flows through Kafka into Spark. Every field
that Wikimedia does not guarantee is declared `Optional`.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Optional


@dataclass
class WikipediaEvent:
    """Canonical normalized representation of a single Wikipedia change
    event, independent of Wikimedia's raw wire format."""

    event_id: str
    timestamp: float  # Unix epoch seconds (event time, not ingestion time)
    wiki: str
    event_type: str  # "edit", "new", "log", "categorize", etc.
    page_title: str

    page_id: Optional[int] = None
    namespace: Optional[int] = None

    revision_id: Optional[int] = None
    previous_revision_id: Optional[int] = None

    user: Optional[str] = None
    user_id: Optional[int] = None
    is_bot: bool = False

    comment: Optional[str] = None

    byte_length_new: Optional[int] = None
    byte_length_old: Optional[int] = None
    byte_length_delta: Optional[int] = None

    is_revert: bool = False
    revert_target_revision_id: Optional[int] = None

    ingestion_timestamp: Optional[float] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def from_json(payload: str) -> "WikipediaEvent":
        data: dict[str, Any] = json.loads(payload)
        return WikipediaEvent(**data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
