"""Seeded synthetic workloads for the benchmark runner.

Each scenario returns raw Wikimedia `recentchange` payloads as JSON strings,
the way the SSE stream delivers them, so the benchmark runs the real
decoder, filter, normalizer and dead-letter paths. Event times are simulated:
`events` payloads spread evenly over `duration_seconds` starting at
`start_ts`, with Wikimedia's whole-second timestamps. Nothing sleeps.

Scenarios:
- normal: steady edits spread over a couple of thousand pages, 20% by bots.
- burst: normal traffic plus a burst of edits on one page in one minute.
- edit_war: normal traffic plus two editors reverting each other on one page.
- multi_page: page popularity follows a Zipf distribution over many pages.
- malformed: normal traffic with a share of broken payloads (for the
  dead-letter path) and of events the ingestion filter drops.
- sustained: a long, high-cardinality stream where half the edits touch a
  page never seen before, to measure state growth and TTL eviction.
"""

from __future__ import annotations

import json
import random
import zlib
from dataclasses import dataclass
from typing import Callable, Optional

BENCHMARK_WIKI = "benchmarkwiki"
FILTERED_WIKI = "otherwiki"
BURST_PAGE = "Benchmark Burst Page"
EDIT_WAR_PAGE = "Benchmark Edit War Page"

NORMAL_PAGE_COUNT = 2000
MULTI_PAGE_COUNT = 5000
ZIPF_EXPONENT = 1.1
BOT_SHARE = 0.2
BURST_EDITS = 200
BURST_SECONDS = 60
EDIT_WAR_ROUNDS = 30
EDIT_WAR_SECONDS_BETWEEN_REVERTS = 8
MALFORMED_SHARE = 0.1
FILTERED_SHARE = 0.1
SUSTAINED_HOT_PAGES = 100
SUSTAINED_NEW_PAGE_SHARE = 0.5

HUMAN_USERS = [f"Editor{number}" for number in range(50)]
BOT_USERS = [f"MaintenanceBot{number}" for number in range(5)]


@dataclass(frozen=True)
class Workload:
    events: int
    duration_seconds: float
    start_ts: float
    seed: int


class _PayloadFactory:
    def __init__(self, rng: random.Random):
        self._rng = rng
        self._next_revision = 1_000_000
        self._next_event = 0

    def edit(
        self,
        ts: float,
        title: str,
        user: str,
        is_bot: bool = False,
        reverts_revision: Optional[int] = None,
        wiki: str = BENCHMARK_WIKI,
        event_type: str = "edit",
    ) -> tuple[dict, int]:
        """Return a recentchange payload and the revision id it creates."""
        revision = self._next_revision
        self._next_revision += 1
        self._next_event += 1
        byte_delta = -80 if reverts_revision else self._rng.randint(-50, 200)
        payload = {
            "meta": {"id": f"benchmark-{self._next_event}"},
            "type": event_type,
            "wiki": wiki,
            "title": title,
            "page_id": zlib.crc32(title.encode("utf-8")),
            "namespace": 0,
            "timestamp": int(ts),
            "user": user,
            "bot": is_bot,
            "revision": {"old": revision - 1, "new": revision},
            "length": {"old": 5000, "new": 5000 + byte_delta},
            "comment": f"Undid revision {reverts_revision}" if reverts_revision else "benchmark edit",
            "tags": ["mw-undo"] if reverts_revision else [],
        }
        return payload, revision

    def background_edit(self, ts: float, title: str) -> dict:
        is_bot = self._rng.random() < BOT_SHARE
        user = self._rng.choice(BOT_USERS if is_bot else HUMAN_USERS)
        return self.edit(ts, title, user, is_bot=is_bot)[0]


def _timestamps(workload: Workload) -> list[float]:
    step = workload.duration_seconds / workload.events
    return [workload.start_ts + index * step for index in range(workload.events)]


def _normal(workload: Workload, rng: random.Random, factory: _PayloadFactory) -> list[dict]:
    pages = [f"Benchmark Page {number}" for number in range(NORMAL_PAGE_COUNT)]
    return [factory.background_edit(ts, rng.choice(pages)) for ts in _timestamps(workload)]


def _burst(workload: Workload, rng: random.Random, factory: _PayloadFactory) -> list[dict]:
    payloads = _normal(workload, rng, factory)
    burst_start = workload.start_ts + workload.duration_seconds / 2
    for index in range(BURST_EDITS):
        ts = burst_start + index * BURST_SECONDS / BURST_EDITS
        payloads.append(factory.edit(ts, BURST_PAGE, rng.choice(HUMAN_USERS))[0])
    return sorted(payloads, key=lambda payload: payload["timestamp"])


def _edit_war(workload: Workload, rng: random.Random, factory: _PayloadFactory) -> list[dict]:
    payloads = _normal(workload, rng, factory)
    war_start = workload.start_ts + workload.duration_seconds / 2
    seed_edit, last_revision = factory.edit(war_start, EDIT_WAR_PAGE, "WarringEditorA")
    payloads.append(seed_edit)
    editors = ["WarringEditorB", "WarringEditorA"]
    for round_number in range(EDIT_WAR_ROUNDS):
        ts = war_start + (round_number + 1) * EDIT_WAR_SECONDS_BETWEEN_REVERTS
        payload, last_revision = factory.edit(
            ts, EDIT_WAR_PAGE, editors[round_number % 2], reverts_revision=last_revision)
        payloads.append(payload)
    return sorted(payloads, key=lambda payload: payload["timestamp"])


def _multi_page(workload: Workload, rng: random.Random, factory: _PayloadFactory) -> list[dict]:
    pages = [f"Benchmark Page {number}" for number in range(MULTI_PAGE_COUNT)]
    weights = [1 / (rank + 1) ** ZIPF_EXPONENT for rank in range(MULTI_PAGE_COUNT)]
    titles = rng.choices(pages, weights=weights, k=workload.events)
    return [factory.background_edit(ts, title) for ts, title in zip(_timestamps(workload), titles)]


def _malformed(workload: Workload, rng: random.Random, factory: _PayloadFactory) -> list:
    """Like normal, but some payloads are broken or filtered. Broken ones are
    returned as strings or dicts that fail decoding or normalization."""
    payloads: list = []
    for index, payload in enumerate(_normal(workload, rng, factory)):
        roll = rng.random()
        if roll < MALFORMED_SHARE:
            kind = index % 4
            if kind == 0:
                payloads.append("{truncated json")
                continue
            if kind == 1:
                payload.pop("title")
            elif kind == 2:
                payload["timestamp"] = "not-a-timestamp"
            else:
                payload.pop("wiki")
        elif roll < MALFORMED_SHARE + FILTERED_SHARE:
            if index % 2:
                payload["wiki"] = FILTERED_WIKI
            else:
                payload["type"] = "log"
        payloads.append(payload)
    return payloads


def _sustained(workload: Workload, rng: random.Random, factory: _PayloadFactory) -> list[dict]:
    hot_pages = [f"Benchmark Hot Page {number}" for number in range(SUSTAINED_HOT_PAGES)]
    payloads = []
    for index, ts in enumerate(_timestamps(workload)):
        if rng.random() < SUSTAINED_NEW_PAGE_SHARE:
            title = f"Benchmark One-off Page {index}"
        else:
            title = rng.choice(hot_pages)
        payloads.append(factory.background_edit(ts, title))
    return payloads


SCENARIOS: dict[str, Callable[[Workload, random.Random, _PayloadFactory], list]] = {
    "normal": _normal,
    "burst": _burst,
    "edit_war": _edit_war,
    "multi_page": _multi_page,
    "malformed": _malformed,
    "sustained": _sustained,
}


def generate(scenario: str, workload: Workload) -> list[str]:
    """Return the scenario's SSE message payloads in event-time order."""
    rng = random.Random(workload.seed)
    payloads = SCENARIOS[scenario](workload, rng, _PayloadFactory(rng))
    return [payload if isinstance(payload, str) else json.dumps(payload) for payload in payloads]
