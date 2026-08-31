"""Generate synthetic Wikipedia-edit-like events and publish them to Kafka.

Per the project spec (Section 15), the pipeline must be demonstrable
without depending on live Wikimedia availability. This script publishes
deterministic-ish synthetic `WikipediaEvent`s directly (bypassing the SSE
normalizer, since we construct already-normalized events) so the rest of
the pipeline -- Spark, anomaly detection, edit-war detection, storage, and
the dashboard -- can be exercised end-to-end offline.

Three scenarios are generated on a loop:
1. `--scenario normal`   -- steady, low-volume background editing.
2. `--scenario spike`    -- a sudden burst of edits on one page (should
                             trigger the median/MAD or count-fallback
                             anomaly detector).
3. `--scenario edit_war` -- two editors repeatedly reverting each other on
                             one page (should trigger the edit-war detector).

Usage:
    python -m scripts.generate_synthetic_events --scenario spike --count 40
    python -m scripts.generate_synthetic_events --scenario edit_war --rounds 6
    python -m scripts.generate_synthetic_events --scenario normal --duration 120
"""

from __future__ import annotations

import argparse
import random
import time
import uuid
from typing import Optional

from common.config import get_settings
from common.logging_setup import configure_logging
from common.models import WikipediaEvent
from ingestion.kafka_producer import EventPublisher

WIKI = "enwiki"
NORMAL_PAGES = ["Python (programming language)", "Solar System", "Great Barrier Reef", "Jazz"]
HUMAN_USERS = ["AliceEditor", "BobContributor", "CarolWrites", "DaveFixesTypos"]
BOT_USERS = ["CleanupBot", "RefFormatterBot"]


def _make_event(
    page_title: str,
    user: str,
    is_bot: bool = False,
    is_revert: bool = False,
    revert_target_revision_id: Optional[int] = None,
    revision_id: Optional[int] = None,
    byte_delta: int = 20,
) -> WikipediaEvent:
    now = time.time()
    rev_id = revision_id or random.randint(10_000_000, 99_999_999)
    return WikipediaEvent(
        event_id=str(uuid.uuid4()),
        timestamp=now,
        wiki=WIKI,
        event_type="edit",
        page_title=page_title,
        page_id=abs(hash(page_title)) % 1_000_000,
        namespace=0,
        revision_id=rev_id,
        previous_revision_id=rev_id - 1,
        user=user,
        user_id=abs(hash(user)) % 100_000,
        is_bot=is_bot,
        comment="Undid revision" if is_revert else "Synthetic test edit",
        byte_length_new=5000 + byte_delta,
        byte_length_old=5000,
        byte_length_delta=byte_delta,
        is_revert=is_revert,
        revert_target_revision_id=revert_target_revision_id,
        ingestion_timestamp=now,
    )


def run_normal(publisher: EventPublisher, duration_seconds: int) -> None:
    """Low-volume, realistic-looking background edits across several pages."""
    end_time = time.time() + duration_seconds
    while time.time() < end_time:
        page = random.choice(NORMAL_PAGES)
        is_bot = random.random() < 0.2
        user = random.choice(BOT_USERS if is_bot else HUMAN_USERS)
        event = _make_event(page, user, is_bot=is_bot, byte_delta=random.randint(-50, 200))
        publisher.publish(event)
        time.sleep(random.uniform(1.5, 4.0))
    publisher.flush()


def run_spike(publisher: EventPublisher, page_title: str, count: int) -> None:
    """Publish a burst of edits on a single page in quick succession, which
    should register as an activity anomaly."""
    for i in range(count):
        user = random.choice(HUMAN_USERS + BOT_USERS)
        is_bot = user in BOT_USERS
        event = _make_event(page_title, user, is_bot=is_bot, byte_delta=random.randint(-100, 300))
        publisher.publish(event)
        time.sleep(0.2)
    publisher.flush()


def run_edit_war(publisher: EventPublisher, page_title: str, rounds: int) -> None:
    """Publish alternating reverts between two editors on the same page,
    which should trigger the mutual-revert edit-war detector."""
    editor_a, editor_b = "EditorAlpha", "EditorBeta"
    last_revision_id = random.randint(10_000_000, 99_999_999)

    # Seed the page with an initial edit from editor_a.
    seed_event = _make_event(page_title, editor_a, revision_id=last_revision_id, byte_delta=100)
    publisher.publish(seed_event)
    time.sleep(0.3)

    current_author = editor_a
    for _ in range(rounds):
        reverter = editor_b if current_author == editor_a else editor_a
        new_revision_id = last_revision_id + 1
        event = _make_event(
            page_title,
            reverter,
            is_revert=True,
            revert_target_revision_id=last_revision_id,
            revision_id=new_revision_id,
            byte_delta=-80,
        )
        publisher.publish(event)
        last_revision_id = new_revision_id
        current_author = reverter
        time.sleep(0.3)
    publisher.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish synthetic WikiPulse events to Kafka")
    parser.add_argument("--scenario", choices=["normal", "spike", "edit_war"], default="normal")
    parser.add_argument("--page", default="Synthetic Demo Page", help="Target page for spike/edit_war")
    parser.add_argument("--count", type=int, default=30, help="Number of edits for the spike scenario")
    parser.add_argument("--rounds", type=int, default=5, help="Revert round-trips for the edit_war scenario")
    parser.add_argument("--duration", type=int, default=60, help="Seconds to run the normal scenario")
    args = parser.parse_args()

    logger = configure_logging("scripts.generate_synthetic_events")
    settings = get_settings()
    publisher = EventPublisher(settings.kafka)

    logger.info("Running synthetic scenario=%s", args.scenario)
    if args.scenario == "normal":
        run_normal(publisher, args.duration)
    elif args.scenario == "spike":
        run_spike(publisher, args.page, args.count)
    elif args.scenario == "edit_war":
        run_edit_war(publisher, args.page, args.rounds)

    publisher.close()
    logger.info("Synthetic event generation complete")


if __name__ == "__main__":
    main()
