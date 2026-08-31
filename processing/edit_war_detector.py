"""Edit-war detection based on reciprocal reverts, not raw revert counts.

Per Lab 2 (project spec Section 6 and Section 19), a high revert count on a
page does NOT by itself constitute an edit war -- a single editor reverting
several unrelated vandalism edits looks the same in raw counts. What
specifically signals a conflict is *reciprocal* behavior: editor A reverts
editor B, and editor B later reverts editor A back, within the same
analysis window.

This module operates on a plain list of `EditRecord`s for a single page
within a single window (already grouped by the caller / Spark job), so it
is trivially unit-testable without Spark.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from common.config import EditWarConfig


@dataclass(frozen=True)
class EditRecord:
    user: Optional[str]
    revision_id: Optional[int]
    is_revert: bool
    revert_target_revision_id: Optional[int]
    byte_length_delta: Optional[int]
    timestamp: float


@dataclass(frozen=True)
class EditWarResult:
    mutual_revert_count: int
    editor_a: Optional[str]
    editor_b: Optional[str]
    total_reverts: int
    edit_burst_count: int
    conflict_score: float
    flag: bool


def _build_revision_authors(records: Iterable[EditRecord]) -> Dict[int, str]:
    """Map revision_id -> author, used to resolve "who authored the
    revision that was reverted" from a revert event's target id. Only
    revisions seen within the current window can be resolved this way,
    which is a reasonable simplification for a five-minute local window."""
    authors: Dict[int, str] = {}
    for record in records:
        if record.revision_id is not None and record.user:
            authors[record.revision_id] = record.user
    return authors


def detect_edit_war(records: list[EditRecord], config: EditWarConfig) -> EditWarResult:
    """Analyze all edit records for a single page within a single window and
    return a page-level edit-war/conflict signal.

    This intentionally returns a *signal* (a score + flag), not a
    definitive judgement -- see project spec Section 6.
    """
    records = list(records)
    total_reverts = sum(1 for r in records if r.is_revert)

    revision_authors = _build_revision_authors(records)

    # revert_edges[(reverter, reverted_author)] = count of times reverter
    # reverted a revision authored by reverted_author within this window.
    revert_edges: Counter[tuple[str, str]] = Counter()
    for record in records:
        if not record.is_revert or not record.user:
            continue
        target_author = None
        if record.revert_target_revision_id is not None:
            target_author = revision_authors.get(record.revert_target_revision_id)
        if target_author and target_author != record.user:
            revert_edges[(record.user, target_author)] += 1

    # Mutual pairs: both (A, B) and (B, A) edges exist. The mutual-revert
    # count for a pair is the sum of reverts in both directions -- each
    # reciprocal exchange counts once per direction.
    mutual_pairs: dict[frozenset, int] = defaultdict(int)
    seen_pairs: set[frozenset] = set()
    for (reverter, reverted), count in revert_edges.items():
        reverse_count = revert_edges.get((reverted, reverter), 0)
        pair_key = frozenset({reverter, reverted})
        if reverse_count > 0 and pair_key not in seen_pairs:
            seen_pairs.add(pair_key)
            mutual_pairs[pair_key] = count + reverse_count

    editor_a: Optional[str] = None
    editor_b: Optional[str] = None
    mutual_revert_count = 0
    if mutual_pairs:
        top_pair, top_count = max(mutual_pairs.items(), key=lambda kv: kv[1])
        mutual_revert_count = top_count
        pair_list = sorted(top_pair)
        editor_a, editor_b = pair_list[0], pair_list[1] if len(pair_list) > 1 else None

    # Edit burst: rapid-fire negative-byte-delta edits are a weak
    # supporting signal (content being stripped/re-added repeatedly),
    # but never the sole basis for a flag.
    edit_burst_count = sum(
        1 for r in records if r.byte_length_delta is not None and r.byte_length_delta < 0
    )

    conflict_score = (
        mutual_revert_count * config.score_mutual_weight
        + (edit_burst_count if len(records) >= config.min_edits_in_window else 0)
        * config.score_burst_weight
    )

    flag = (
        mutual_revert_count >= config.mutual_revert_threshold
        or conflict_score >= config.flag_score_threshold
    )

    return EditWarResult(
        mutual_revert_count=mutual_revert_count,
        editor_a=editor_a,
        editor_b=editor_b,
        total_reverts=total_reverts,
        edit_burst_count=edit_burst_count,
        conflict_score=round(conflict_score, 4),
        flag=flag,
    )
