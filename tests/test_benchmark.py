"""Tests for the benchmark package: seeded workloads, the windowing
emulation, the detection stage, result comparison and the output files."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import pytest

from benchmark import report, runner
from benchmark.windows import SlidingWindows
from benchmark.workloads import BURST_PAGE, EDIT_WAR_PAGE, Workload, generate
from common.models import WikipediaEvent

WORKLOAD = Workload(events=500, duration_seconds=600, start_ts=1_767_225_600, seed=7)


class TestWorkloads:
    def test_same_seed_gives_the_same_payloads(self):
        assert generate("multi_page", WORKLOAD) == generate("multi_page", WORKLOAD)

    def test_different_seed_gives_different_payloads(self):
        other = Workload(events=500, duration_seconds=600, start_ts=1_767_225_600, seed=8)

        assert generate("normal", WORKLOAD) != generate("normal", other)

    def test_injected_scenarios_stay_in_event_time_order(self):
        for scenario in ("burst", "edit_war"):
            timestamps = [json.loads(payload)["timestamp"] for payload in generate(scenario, WORKLOAD)]
            assert timestamps == sorted(timestamps), scenario

    def test_malformed_scenario_mixes_broken_and_filtered_payloads(self):
        payloads = generate("malformed", WORKLOAD)
        decoded = []
        broken_json = 0
        for payload in payloads:
            try:
                decoded.append(json.loads(payload))
            except json.JSONDecodeError:
                broken_json += 1

        assert broken_json > 0
        assert any("title" not in payload for payload in decoded)
        assert any(payload.get("type") == "log" for payload in decoded)


def make_event(page_title: str, ts: float) -> WikipediaEvent:
    return WikipediaEvent(event_id=f"{page_title}-{ts}", timestamp=ts, wiki="w", event_type="edit",
                          page_title=page_title, user="Alice")


class TestSlidingWindows:
    def test_event_lands_in_every_window_that_covers_it(self):
        windows = SlidingWindows(window_seconds=300, slide_seconds=60, watermark_delay_seconds=120)
        windows.add(make_event("Page", 1_000_000))

        assert windows.open_windows == 5

    def test_window_is_released_once_the_watermark_passes_its_end(self):
        windows = SlidingWindows(window_seconds=300, slide_seconds=300, watermark_delay_seconds=120)
        windows.add(make_event("Page", 1_000_020))  # window [999_900, 1_000_200)
        windows.add(make_event("Other", 1_000_319))  # watermark 1_000_199
        assert windows.pop_finalized() == []

        windows.add(make_event("Other", 1_000_321))  # watermark 1_000_201
        released = windows.pop_finalized()

        assert [row["page_title"] for row in released] == ["Page"]
        assert released[0]["window"]["end"] == datetime.fromtimestamp(1_000_200, tz=timezone.utc)
        assert windows.pop_finalized() == []

    def test_event_older_than_the_watermark_is_dropped(self):
        windows = SlidingWindows(window_seconds=300, slide_seconds=300, watermark_delay_seconds=60)
        windows.add(make_event("Page", 1_000_000))

        windows.add(make_event("Page", 999_000))

        assert windows.late_events_dropped == 1


class TestDetectionStage:
    def test_burst_scenario_flags_the_burst_page(self):
        settings = runner.get_settings()
        result, events = runner.run_ingestion_stage(generate("burst", WORKLOAD), settings)
        detection, activity, anomalies, wars = runner.run_detection_stage(events, settings, slide_seconds=60)

        assert detection["duplicate_windows"] == 0
        assert runner.expected_signal("burst", anomalies, wars)["detected"] is True
        assert any(row["page_title"] == BURST_PAGE for row in anomalies)

    def test_edit_war_scenario_flags_the_contested_page(self):
        settings = runner.get_settings()
        _, events = runner.run_ingestion_stage(generate("edit_war", WORKLOAD), settings)
        _, _, anomalies, wars = runner.run_detection_stage(events, settings, slide_seconds=60)

        assert runner.expected_signal("edit_war", anomalies, wars) == {
            "kind": "edit_war", "page_title": EDIT_WAR_PAGE, "detected": True}


class TestCompare:
    def results(self, events_per_sec: float, events: int = 100) -> dict:
        return {
            "run_id": "20261026T000000Z",
            "config": {"events": events},
            "environment": {"python": "3.12"},
            "scenarios": {"normal": {"ingestion": {"events_per_sec": events_per_sec}}},
        }

    def test_change_is_reported_as_a_percentage(self):
        comparison = report.compare(self.results(1200.0), self.results(1000.0))

        assert comparison["comparable"] is True
        assert comparison["changes"] == [{
            "scenario": "normal", "metric": "ingestion.events_per_sec", "baseline": 1000.0,
            "current": 1200.0, "change_pct": 20.0, "better": True,
        }]

    def test_runs_with_different_configuration_are_not_compared(self):
        comparison = report.compare(self.results(1200.0, events=100), self.results(1000.0, events=200))

        assert comparison["comparable"] is False

    def test_no_baseline_is_reported_as_not_compared(self):
        assert report.compare(self.results(1.0), None)["comparable"] is False


def test_runner_writes_json_and_markdown_results(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", [
        "benchmark.runner", "--scenarios", "normal", "malformed", "--events", "300",
        "--output-dir", str(tmp_path),
    ])

    runner.main()

    [run_dir] = [path for path in tmp_path.iterdir() if path.is_dir()]
    results = json.loads((run_dir / report.RESULTS_FILE).read_text(encoding="utf-8"))
    assert set(results["scenarios"]) == {"normal", "malformed"}
    assert results["scenarios"]["malformed"]["ingestion"]["dead_lettered"] > 0
    assert "## Scenario: malformed" in (run_dir / report.SUMMARY_FILE).read_text(encoding="utf-8")


def test_latest_earlier_run_is_the_default_baseline(tmp_path):
    for run_id in ("20261024T100000Z", "20261025T100000Z", "20261026T100000Z"):
        (tmp_path / run_id).mkdir()
        (tmp_path / run_id / report.RESULTS_FILE).write_text(json.dumps({"run_id": run_id}), encoding="utf-8")

    baseline = report.load_baseline(tmp_path, current_run_id="20261026T100000Z", explicit_path=None)

    assert baseline["run_id"] == "20261025T100000Z"


@pytest.fixture(autouse=True)
def isolated_settings(monkeypatch):
    """Benchmark tests use the defaults, not a developer's .env overrides."""
    for name in ("BENCHMARK_EVENTS", "BENCHMARK_RATE", "BENCHMARK_SEED", "BENCHMARK_BATCH_SIZE",
                 "ANOMALY_HISTORY_TTL_SECONDS", "SPARK_SLIDE_DURATION"):
        monkeypatch.delenv(name, raising=False)
    runner.get_settings(force_reload=True)
