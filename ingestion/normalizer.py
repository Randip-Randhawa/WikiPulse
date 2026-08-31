"""Normalization of raw Wikimedia `recentchange` SSE payloads.

Wikimedia's recentchange stream (https://stream.wikimedia.org/v2/stream/recentchange)
emits JSON objects whose shape varies by `type` (edit / new / log / categorize)
and which frequently omit fields. This module converts a raw dict into the
canonical `WikipediaEvent` and never raises on missing-but-optional fields;
it only raises `MalformedEventError` when data required to build *any*
meaningful event (id, timestamp, wiki, title, type) is absent or the wrong
type, so the caller can safely skip/log the event without crashing.
"""

from __future__ import annotations

import re
import time
from typing import Any, Optional

from common.models import WikipediaEvent

# Tags Wikimedia attaches to revert-like actions (rollback/undo tools).
_REVERT_TAGS = {"mw-rollback", "mw-undo", "mw-manual-revert"}

# Fallback heuristic: edit summaries that clearly indicate a revert/undo when
# tags are unavailable (e.g. "Undid revision 123456789 by [[Special:...]]").
_UNDO_COMMENT_RE = re.compile(r"undid revision (\d+)", re.IGNORECASE)
_REVERT_COMMENT_RE = re.compile(r"\brevert(ed)?\b", re.IGNORECASE)


class MalformedEventError(ValueError):
    """Raised when a raw event lacks the minimum fields needed to build a
    WikipediaEvent at all."""


def _require(data: dict[str, Any], key: str) -> Any:
    if key not in data or data[key] is None:
        raise MalformedEventError(f"missing required field: {key}")
    return data[key]


def _detect_revert(raw: dict[str, Any], comment: Optional[str]) -> tuple[bool, Optional[int]]:
    """Return (is_revert, revert_target_revision_id) using tags first, then
    a best-effort comment heuristic. Absence of a target id is expected and
    handled safely upstream."""
    tags = raw.get("tags") or []
    if isinstance(tags, list) and _REVERT_TAGS.intersection(tags):
        target_id = None
        if comment:
            match = _UNDO_COMMENT_RE.search(comment)
            if match:
                target_id = int(match.group(1))
        return True, target_id

    if comment and _REVERT_COMMENT_RE.search(comment):
        match = _UNDO_COMMENT_RE.search(comment)
        target_id = int(match.group(1)) if match else None
        return True, target_id

    return False, None


def normalize_event(raw: dict[str, Any]) -> WikipediaEvent:
    """Convert a raw Wikimedia recentchange dict into a WikipediaEvent.

    Raises:
        MalformedEventError: if the payload is missing fields required to
            construct any usable event.
    """
    if not isinstance(raw, dict):
        raise MalformedEventError(f"event is not a JSON object: {type(raw)!r}")

    event_type = _require(raw, "type")
    wiki = raw.get("wiki") or raw.get("server_name")
    if not wiki:
        raise MalformedEventError("missing required field: wiki")

    title = raw.get("title")
    if not title:
        raise MalformedEventError("missing required field: title")

    # Wikimedia sends `timestamp` as unix epoch seconds. Fall back to the
    # nested `meta.dt` (ISO8601) only if strictly necessary; otherwise skip.
    timestamp = raw.get("timestamp")
    if timestamp is None:
        raise MalformedEventError("missing required field: timestamp")
    try:
        timestamp = float(timestamp)
    except (TypeError, ValueError) as exc:
        raise MalformedEventError(f"invalid timestamp: {timestamp!r}") from exc

    meta = raw.get("meta") or {}
    event_id = str(meta.get("id") or raw.get("id") or f"{wiki}:{title}:{timestamp}")

    length = raw.get("length") or {}
    byte_old = length.get("old") if isinstance(length, dict) else None
    byte_new = length.get("new") if isinstance(length, dict) else None
    byte_delta: Optional[int] = None
    if isinstance(byte_old, (int, float)) and isinstance(byte_new, (int, float)):
        byte_delta = int(byte_new) - int(byte_old)

    revision = raw.get("revision") or {}
    revision_id = revision.get("new") if isinstance(revision, dict) else None
    previous_revision_id = revision.get("old") if isinstance(revision, dict) else None

    comment = raw.get("comment")
    is_revert, revert_target = _detect_revert(raw, comment)

    return WikipediaEvent(
        event_id=event_id,
        timestamp=timestamp,
        wiki=str(wiki),
        event_type=str(event_type),
        page_title=str(title),
        page_id=raw.get("page_id"),
        namespace=raw.get("namespace"),
        revision_id=revision_id,
        previous_revision_id=previous_revision_id,
        user=raw.get("user"),
        user_id=raw.get("userid") or raw.get("user_id"),
        is_bot=bool(raw.get("bot", False)),
        comment=comment,
        byte_length_new=int(byte_new) if isinstance(byte_new, (int, float)) else None,
        byte_length_old=int(byte_old) if isinstance(byte_old, (int, float)) else None,
        byte_length_delta=byte_delta,
        is_revert=is_revert,
        revert_target_revision_id=revert_target,
        ingestion_timestamp=time.time(),
    )


def should_ignore(raw: dict[str, Any], wiki_filter: list[str]) -> bool:
    """Return True if this raw event should be dropped before normalization
    (e.g. not one of the wikis we care about, or a non-content event type
    that carries no useful editing signal)."""
    if wiki_filter:
        wiki = raw.get("wiki") or raw.get("server_name")
        if wiki not in wiki_filter:
            return True

    # "log" events (user rights changes, deletions, etc.) and
    # "categorize" events don't represent page-content edits and are not
    # useful for edit-war / activity-anomaly detection.
    if raw.get("type") in {"log", "categorize"}:
        return True

    return False
