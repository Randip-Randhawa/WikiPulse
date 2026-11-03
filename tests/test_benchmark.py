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

    def test_consumer_scaling_metrics_compared(self):
        baseline = {
            "run_id": "20261026T000000Z",
            "config": {"events": 100},
            "environment": {"python": "3.12"},
            "scenarios": {
                "normal": {
                    "consumer_scaling": {
                        "events_per_sec": 1000.0,
                        "evaluation_batch_ms": {"p95": 10.0},
                        "latency_waterfall": {"end_to_end_ms": {"p95": 40.0}},
                    }
                }
            },
        }
        current = {
            "run_id": "20261026T010000Z",
            "config": {"events": 100},
            "environment": {"python": "3.12"},
            "scenarios": {
                "normal": {
                    "consumer_scaling": {
                        "events_per_sec": 1250.0,
                        "evaluation_batch_ms": {"p95": 8.0},
                        "latency_waterfall": {"end_to_end_ms": {"p95": 35.0}},
                    }
                }
            },
        }
        comparison = report.compare(current, baseline)
        assert comparison["comparable"] is True
        metrics = {c["metric"]: c for c in comparison["changes"]}
        assert "consumer_scaling.events_per_sec" in metrics
        assert metrics["consumer_scaling.events_per_sec"]["change_pct"] == 25.0
        assert metrics["consumer_scaling.events_per_sec"]["better"] is True
        assert "consumer_scaling.evaluation_batch_ms.p95" in metrics
        assert metrics["consumer_scaling.evaluation_batch_ms.p95"]["better"] is True


class TestConsumerScalingBenchmark:
    def test_consumer_scaling_bounds_memory_and_measures_waterfall(self):
        settings = runner.get_settings()
        events = [
            make_event(f"Page_{i % 100}", ts=1_000_000.0 + (i * 2.0))
            for i in range(300)
        ]
        result = runner.run_consumer_scaling_stage(
            events,
            settings,
            max_active_pages=20,
            slide_seconds=30.0,
        )

        assert result["events_processed"] == 300
        assert result["active_pages"] <= 20
        assert result["memory_bounded"] is True
        assert result["evicted_by_capacity"] > 0
        assert result["events_per_sec"] > 0
        assert result["latency_waterfall"]["ingestion_ms"] is not None
        assert result["latency_waterfall"]["compute_ms"] is not None
        assert result["latency_waterfall"]["end_to_end_ms"] is not None
        assert result["sla"]["memory_bounded_sla_met"] is True
        assert result["sla"]["throughput_sla_met"] is True
        assert result["sla"]["e2e_latency_sla_met"] is True

    def test_consumer_scaling_inactivity_ttl_eviction(self):
        from dataclasses import replace
        base_settings = runner.get_settings()
        settings = replace(
            base_settings,
            stream_consumer=replace(
                base_settings.stream_consumer,
                page_inactivity_ttl_seconds=30.0,
            ),
        )
        # Page_Old is touched at t=1,000,000; Page_New is touched at t=1,000,045 (> 30s TTL, < 300s window)
        events = [
            make_event("Page_Old", ts=1_000_000.0),
            make_event("Page_New", ts=1_000_045.0),
        ]
        result = runner.run_consumer_scaling_stage(
            events,
            settings,
            max_active_pages=1000,
            slide_seconds=30.0,
        )

        assert result["evicted_by_ttl"] >= 1
        assert result["evicted_pages_total"] >= 1

    def test_consumer_scaling_empty_events(self):
        settings = runner.get_settings()
        result = runner.run_consumer_scaling_stage([], settings)
        assert result["events_processed"] == 0
        assert result["active_pages"] == 0
        assert result["memory_bounded"] is True
        assert result["events_per_sec"] is None or result["events_per_sec"] == 0.0

    def test_report_renders_consumer_scaling_and_latency_waterfall(self):
        results = {
            "run_id": "20261112T120000Z",
            "kind": "test",
            "environment": {
                "python": "3.12",
                "platform": "windows",
                "cpu_count": 8,
                "total_memory_mb": 16000,
                "git_commit": "abc1234",
                "git_uncommitted_changes": False,
            },
            "config": {"events": 100},
            "page_history_memory": {"pages": 10, "lookback_windows": 12, "bytes_per_page": 256},
            "scenarios": {
                "normal": {
                    "input_events": 100,
                    "simulated_seconds": 60.0,
                    "ingestion": {
                        "events_per_sec": 5000.0,
                        "accepted": 100,
                        "filtered": 0,
                        "rejected": 0,
                        "dead_letter_rate": 0.0,
                        "per_event_ms": {"p50": 0.1, "p95": 0.2, "p99": 0.3, "max": 0.5},
                    },
                    "detection": {
                        "windows_per_sec": 200.0,
                        "windows_evaluated": 20,
                        "slide_seconds": 30.0,
                        "micro_batches": 2,
                        "duplicate_windows": 0,
                        "evaluation_batch_ms": {"p50": 1.0, "p95": 2.0, "p99": 3.0, "max": 4.0},
                        "page_history": {
                            "peak_active_pages": 10,
                            "evicted_pages_total": 0,
                            "final_active_pages": 10,
                        },
                        "process_rss_mb_before": 50.0,
                        "process_rss_mb_after": 52.0,
                        "anomalies": {},
                        "edit_war_signals": 0,
                        "edit_wars_flagged": 0,
                    },
                    "consumer_scaling": {
                        "events_per_sec": 8500.0,
                        "active_pages": 15,
                        "peak_active_pages": 20,
                        "buffered_events": 45,
                        "evicted_pages_total": 5,
                        "evicted_by_ttl": 2,
                        "evicted_by_capacity": 3,
                        "max_active_pages_limit": 20,
                        "evaluation_batch_ms": {"p50": 0.5, "p95": 1.2, "p99": 1.8, "max": 2.1},
                        "latency_waterfall": {
                            "ingestion_ms": {"p50": 12.0, "p95": 25.0, "p99": 28.0, "max": 30.0},
                            "compute_ms": {"p50": 0.8, "p95": 1.5, "p99": 2.0, "max": 2.5},
                            "end_to_end_ms": {"p50": 13.0, "p95": 26.5, "p99": 30.0, "max": 32.5},
                        },
                        "sla": {
                            "throughput_target_eps": 780.0,
                            "throughput_sla_met": True,
                            "e2e_latency_p95_target_ms": 1000.0,
                            "e2e_latency_sla_met": True,
                            "memory_bounded_sla_met": True,
                        },
                        "process_rss_mb_before": 52.0,
                        "process_rss_mb_after": 53.0,
                    },
                    "parquet": {},
                }
            },
            "baseline_comparison": {"comparable": False, "reason": "no earlier run"},
        }
        md = report.render_markdown(results)
        assert "Consumer Scaling (in-memory deque + slide)" in md
        assert "Waterfall: Ingestion (T_ingest - T_event)" in md
        assert "Waterfall: Compute (T_proc - T_ingest)" in md
        assert "Waterfall: End-to-End (T_proc - T_event)" in md
        assert "Consumer SLA: Throughput >= 780" in md
        assert "End-to-End Latency p95 <= 1000" in md


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
