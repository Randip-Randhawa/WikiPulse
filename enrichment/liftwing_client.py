"""Client for Wikimedia's maintained Lift Wing model-serving platform.

Per Lab 2 (project spec Section 7), WikiPulse does NOT train its own
edit-quality classifier. Instead it calls Wikimedia's existing Lift Wing
`revertrisk` model to get a damage/quality-related score for a revision.

Design constraints honored here:
- The integration is modular and fully disableable
  (`LIFTWING_ENABLED=false` short-circuits every call).
- Calls are wrapped in a timeout and never allowed to raise into the
  caller; failures degrade to a `None` score rather than crashing anything.
- Because a synchronous HTTP call per revision would otherwise serialize
  against Kafka/Spark throughput, batches are enriched through a small
  bounded thread pool (`LIFTWING_MAX_CONCURRENT_REQUESTS`) with an overall
  batch timeout, so a slow/unavailable Lift Wing endpoint degrades
  enrichment coverage rather than stalling the pipeline. This is the
  "clean, bounded approach" called for when a full async architecture
  would be overkill for an academic project; a production system could
  replace this with a proper async enrichment queue.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

import httpx

from common.config import LiftWingConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RevisionScoreRequest:
    wiki: str
    revision_id: int


@dataclass(frozen=True)
class RevisionScoreResult:
    wiki: str
    revision_id: int
    model_name: str
    score: Optional[float]
    raw_response: Optional[dict]
    error: Optional[str] = None


class LiftWingClient:
    """Fetches per-revision edit-quality/damage scores from Lift Wing."""

    def __init__(self, config: LiftWingConfig):
        self._config = config

    def _score_one(self, request: RevisionScoreRequest) -> RevisionScoreResult:
        url = f"{self._config.base_url}/{self._config.model_name}:predict"
        payload = {"rev_id": request.revision_id, "lang": request.wiki.replace("wiki", "")}

        last_error: Optional[str] = None
        for attempt in range(self._config.max_retries + 1):
            try:
                response = httpx.post(
                    url, json=payload, timeout=self._config.timeout_seconds
                )
                response.raise_for_status()
                data = response.json()
                score = _extract_probability(data)
                return RevisionScoreResult(
                    wiki=request.wiki,
                    revision_id=request.revision_id,
                    model_name=self._config.model_name,
                    score=score,
                    raw_response=data,
                )
            except httpx.TimeoutException as exc:
                last_error = f"timeout: {exc}"
            except httpx.HTTPStatusError as exc:
                last_error = f"http_error: {exc.response.status_code}"
                if exc.response.status_code == 429:
                    logger.warning("Lift Wing rate-limited (revision %s)", request.revision_id)
                break  # don't retry on 4xx client errors other than transient ones
            except Exception as exc:  # pragma: no cover - defensive
                last_error = f"unexpected_error: {exc}"

        logger.warning(
            "Lift Wing scoring failed for revision %s after retries: %s",
            request.revision_id,
            last_error,
        )
        return RevisionScoreResult(
            wiki=request.wiki,
            revision_id=request.revision_id,
            model_name=self._config.model_name,
            score=None,
            raw_response=None,
            error=last_error,
        )

    def score_batch(
        self, requests_: list[RevisionScoreRequest]
    ) -> list[RevisionScoreResult]:
        """Score a batch of revisions using a small bounded thread pool.

        If Lift Wing is disabled, returns immediately with no scores
        (callers should treat this as "enrichment skipped", not an error).
        """
        if not self._config.enabled or not requests_:
            return []

        results: list[RevisionScoreResult] = []
        with ThreadPoolExecutor(max_workers=self._config.max_concurrent_requests) as pool:
            futures = {pool.submit(self._score_one, req): req for req in requests_}
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception:  # pragma: no cover - defensive
                    req = futures[future]
                    logger.exception("Unexpected failure scoring revision %s", req.revision_id)
                    results.append(
                        RevisionScoreResult(
                            wiki=req.wiki,
                            revision_id=req.revision_id,
                            model_name=self._config.model_name,
                            score=None,
                            raw_response=None,
                            error="pool_execution_error",
                        )
                    )
        return results


def _extract_probability(data: dict) -> Optional[float]:
    """Lift Wing's revertrisk models return a shape roughly like:
    {"output": {"prediction": bool, "probability": {"true": 0.1, "false": 0.9}}}
    Extract the "true" (i.e. likely-to-be-reverted / damaging) probability
    defensively, since exact response shape can vary across models."""
    try:
        output = data.get("output", {})
        probability = output.get("probability", {})
        if "true" in probability:
            return float(probability["true"])
        # Fall back to any single numeric probability value present.
        for value in probability.values():
            if isinstance(value, (int, float)):
                return float(value)
    except (AttributeError, TypeError, ValueError):
        pass
    return None
