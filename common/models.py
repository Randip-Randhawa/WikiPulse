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
from enum import Enum
from typing import Any, Optional


class UserType(str, Enum):
    BOT = "BOT"
    ANONYMOUS = "ANONYMOUS"
    REGISTERED = "REGISTERED"


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
    processing_timestamp: Optional[float] = None
    provenance_id: Optional[str] = None
    user_type: Optional[str] = None  # "BOT" | "ANONYMOUS" | "REGISTERED"
    is_minor: bool = False
    comment_length: Optional[int] = None
    is_large_jump: bool = False

    @property
    def ingestion_latency_ms(self) -> Optional[float]:
        """T_ingest - T_event in milliseconds."""
        if self.ingestion_timestamp is not None:
            return round((self.ingestion_timestamp - self.timestamp) * 1000, 3)
        return None

    @property
    def compute_latency_ms(self) -> Optional[float]:
        """T_proc - T_ingest in milliseconds."""
        if self.processing_timestamp is not None and self.ingestion_timestamp is not None:
            proc_ts = (
                self.processing_timestamp.timestamp()
                if hasattr(self.processing_timestamp, "timestamp")
                else float(self.processing_timestamp)
            )
            return round((proc_ts - self.ingestion_timestamp) * 1000, 3)
        return None

    @property
    def end_to_end_latency_ms(self) -> Optional[float]:
        """T_proc - T_event in milliseconds."""
        if self.processing_timestamp is not None:
            proc_ts = (
                self.processing_timestamp.timestamp()
                if hasattr(self.processing_timestamp, "timestamp")
                else float(self.processing_timestamp)
            )
            return round((proc_ts - self.timestamp) * 1000, 3)
        return None

    def to_json(self) -> str:
        d = asdict(self)
        if hasattr(self.processing_timestamp, "timestamp"):
            d["processing_timestamp"] = self.processing_timestamp.timestamp()
        if hasattr(self.user_type, "value"):
            d["user_type"] = self.user_type.value
        return json.dumps(d, separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def from_json(payload: str) -> "WikipediaEvent":
        data: dict[str, Any] = json.loads(payload)
        return WikipediaEvent(**data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
