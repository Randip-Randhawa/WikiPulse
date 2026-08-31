"""Tests for enrichment.liftwing_client: the module must be a no-op when
disabled (never make network calls) and must defensively parse Lift Wing's
response shape without crashing on unexpected structures."""

from __future__ import annotations

from common.config import LiftWingConfig
from enrichment.liftwing_client import LiftWingClient, RevisionScoreRequest, _extract_probability


def make_config(**overrides) -> LiftWingConfig:
    defaults = dict(
        enabled=False,
        base_url="https://example.invalid/models",
        model_name="revertrisk-language-agnostic",
        timeout_seconds=2.0,
        max_retries=1,
        max_concurrent_requests=4,
    )
    defaults.update(overrides)
    return LiftWingConfig(**defaults)


class TestLiftWingDisabled:
    def test_disabled_client_returns_empty_without_network_calls(self):
        client = LiftWingClient(make_config(enabled=False))
        requests_ = [RevisionScoreRequest(wiki="enwiki", revision_id=123)]

        results = client.score_batch(requests_)

        assert results == []

    def test_empty_batch_returns_empty_even_when_enabled(self):
        client = LiftWingClient(make_config(enabled=True))
        assert client.score_batch([]) == []


class TestExtractProbability:
    def test_extracts_true_probability(self):
        data = {"output": {"prediction": True, "probability": {"true": 0.87, "false": 0.13}}}
        assert _extract_probability(data) == 0.87

    def test_falls_back_to_any_numeric_probability(self):
        data = {"output": {"probability": {"damaging": 0.42}}}
        assert _extract_probability(data) == 0.42

    def test_missing_output_returns_none(self):
        assert _extract_probability({}) is None

    def test_unexpected_shape_does_not_raise(self):
        assert _extract_probability({"output": {"probability": "not-a-dict"}}) is None
        assert _extract_probability({"output": None}) is None
