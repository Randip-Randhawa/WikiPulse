"""Benchmark result files: results.json, summary.md and the comparison
with an earlier run.

Runs are only compared when their configuration is identical (scenarios,
event count, rate, seed, batch size, window settings, enabled stages);
otherwise the difference would come from the workload, not the code.
Differences in machine are reported but not checked.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

RESULTS_FILE = "results.json"
SUMMARY_FILE = "summary.md"

# (stage, dotted metric path, whether higher values are better)
COMPARED_METRICS = [
    ("ingestion", "events_per_sec", True),
    ("ingestion", "per_event_ms.p95", False),
    ("detection", "windows_per_sec", True),
    ("detection", "evaluation_batch_ms.p95", False),
    ("parquet", "raw_events.rows_per_sec", True),
    ("postgres", "edit_events.rows_per_sec", True),
    ("postgres", "edit_events.batch_ms.p95", False),
    ("kafka", "round_trip_ms.p95", False),
]


def _lookup(values: Any, dotted_path: str) -> Any:
    for key in dotted_path.split("."):
        if not isinstance(values, dict):
            return None
        values = values.get(key)
    return values


def load_baseline(output_dir: Path, current_run_id: str, explicit_path: Optional[str]) -> Optional[dict]:
    """Return the explicitly requested baseline, or else the most recent
    earlier run in `output_dir` (comparability is checked in `compare`)."""
    if explicit_path:
        return json.loads(Path(explicit_path).read_text(encoding="utf-8"))
    for results_path in sorted(output_dir.glob(f"*/{RESULTS_FILE}"), reverse=True):
        if results_path.parent.name < current_run_id:
            return json.loads(results_path.read_text(encoding="utf-8"))
    return None


def compare(current: dict, baseline: Optional[dict]) -> dict:
    if baseline is None:
        return {"baseline_run_id": None, "comparable": False, "reason": "no earlier run found"}
    if baseline.get("config") != current["config"]:
        return {
            "baseline_run_id": baseline.get("run_id"),
            "comparable": False,
            "reason": "configuration differs; rerun with the same flags to compare",
        }

    changes = []
    for scenario, stages in current["scenarios"].items():
        baseline_stages = baseline.get("scenarios", {}).get(scenario, {})
        for stage, metric, higher_is_better in COMPARED_METRICS:
            current_value = _lookup(stages.get(stage), metric)
            baseline_value = _lookup(baseline_stages.get(stage), metric)
            if current_value is None or not baseline_value:
                continue
            change_pct = round(100 * (current_value - baseline_value) / baseline_value, 1)
            changes.append({
                "scenario": scenario,
                "metric": f"{stage}.{metric}",
                "baseline": baseline_value,
                "current": current_value,
                "change_pct": change_pct,
                "better": change_pct > 0 if higher_is_better else change_pct < 0,
            })
    return {
        "baseline_run_id": baseline["run_id"],
        "comparable": True,
        "same_machine": baseline.get("environment") == current["environment"],
        "changes": changes,
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.3f}" if abs(value) < 10 else f"{value:,.1f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _latency_row(label: str, summary: Optional[dict]) -> str:
    if not summary:
        return f"| {label} | n/a | n/a | n/a | n/a |"
    return f"| {label} | {_fmt(summary['p50'])} | {_fmt(summary['p95'])} | {_fmt(summary['p99'])} | {_fmt(summary['max'])} |"


def render_markdown(results: dict) -> str:
    environment = results["environment"]
    config = results["config"]
    lines = [
        f"# WikiPulse benchmark {results['run_id']}",
        "",
        f"Kind: {results['kind']}. Synthetic, seeded workloads; these numbers describe this machine "
        "and these components, not live Wikimedia throughput.",
        "",
        "## Environment",
        "",
        f"- Python {environment['python']} on {environment['platform']}, {environment['cpu_count']} CPUs, "
        f"{_fmt(environment['total_memory_mb'])} MB RAM",
        f"- Commit {environment['git_commit']}"
        + (" with uncommitted changes" if environment["git_uncommitted_changes"] else ""),
        "",
        "## Configuration",
        "",
    ]
    lines += [f"- {key}: {value}" for key, value in config.items()]

    memory = results["page_history_memory"]
    lines += [
        "",
        "## Page-history memory",
        "",
        f"A page with a full history ({memory['lookback_windows']} window counts) uses about "
        f"{_fmt(memory['bytes_per_page'])} bytes of Python heap (tracemalloc over {_fmt(memory['pages'])} pages).",
    ]

    for scenario, stages in results["scenarios"].items():
        ingestion = stages["ingestion"]
        detection = stages["detection"]
        lines += [
            "",
            f"## Scenario: {scenario}",
            "",
            f"{_fmt(stages['input_events'])} payloads over {_fmt(stages['simulated_seconds'])} s of simulated time.",
            "",
            "| Stage | Throughput | Notes |",
            "|---|---|---|",
            f"| Ingestion (decode, filter, normalize, serialize) | {_fmt(ingestion['events_per_sec'])} events/s | "
            f"accepted {_fmt(ingestion['accepted'])}, filtered {_fmt(ingestion['filtered'])}, "
            f"rejected {_fmt(ingestion['rejected'])}, dead-letter rate {ingestion['dead_letter_rate']:.2%} |",
            f"| Detection (window evaluation) | {_fmt(detection['windows_per_sec'])} windows/s | "
            f"{_fmt(detection['windows_evaluated'])} windows at {_fmt(detection['slide_seconds'])} s slide, "
            f"{_fmt(detection['micro_batches'])} micro-batches, duplicates {detection['duplicate_windows']} |",
        ]
        for dataset, parquet in stages["parquet"].items():
            lines.append(
                f"| Parquet {dataset} | {_fmt(parquet['rows_per_sec'])} rows/s | {_fmt(parquet['files'])} files, "
                f"{_fmt(parquet['file_bytes'])} bytes, Snappy ratio {_fmt(parquet['compression_ratio'])} |")
        if "postgres" in stages:
            for table in ("edit_events", "page_activity_windows"):
                postgres = stages["postgres"][table]
                lines.append(
                    f"| PostgreSQL {table} | {_fmt(postgres['rows_per_sec'])} rows/s | "
                    f"{_fmt(postgres['batches'])} batches, PostgreSQL {stages['postgres']['server_version']} |")
        if "kafka" in stages:
            kafka = stages["kafka"]
            lines.append(
                f"| Kafka produce | {_fmt(kafka['produce_per_sec'])} messages/s | "
                f"received {_fmt(kafka['messages_received'])} of {_fmt(kafka['messages_sent'])} |")

        lines += [
            "",
            "| Latency (ms) | p50 | p95 | p99 | max |",
            "|---|---|---|---|---|",
            _latency_row("Ingestion per event", ingestion["per_event_ms"]),
            _latency_row("Detection per micro-batch", detection["evaluation_batch_ms"]),
            _latency_row("Parquet raw_events per batch", stages["parquet"]["raw_events"]["batch_ms"]),
        ]
        if "postgres" in stages:
            lines.append(_latency_row("PostgreSQL edit_events per batch", stages["postgres"]["edit_events"]["batch_ms"]))
        if "kafka" in stages:
            lines.append(_latency_row("Kafka produce-to-consume", stages["kafka"]["round_trip_ms"]))

        history = detection["page_history"]
        anomalies = ", ".join(f"{kind} {count}" for kind, count in detection["anomalies"].items()) or "none"
        lines += [
            "",
            f"Detections: anomalies {anomalies}; edit-war signals {detection['edit_war_signals']} "
            f"({detection['edit_wars_flagged']} flagged).",
            f"Page history: peak {_fmt(history['peak_active_pages'])} pages, "
            f"{_fmt(history['evicted_pages_total'])} evicted, {_fmt(history['final_active_pages'])} at the end. "
            f"Process RSS {detection['process_rss_mb_before']} → {detection['process_rss_mb_after']} MB.",
        ]
        signal = stages.get("expected_signal")
        if signal:
            outcome = "detected" if signal["detected"] else "NOT detected"
            lines.append(f"Injected {signal['kind']} on '{signal['page_title']}': {outcome}.")

    comparison = results["baseline_comparison"]
    lines += ["", "## Comparison with the previous run", ""]
    if not comparison["comparable"]:
        lines.append(f"Not compared ({comparison['reason']}).")
    else:
        lines.append(f"Baseline {comparison['baseline_run_id']}"
                     + ("" if comparison["same_machine"] else " (different environment)") + ".")
        lines += ["", "| Scenario | Metric | Baseline | Current | Change |", "|---|---|---|---|---|"]
        for change in comparison["changes"]:
            lines.append(f"| {change['scenario']} | {change['metric']} | {_fmt(change['baseline'])} | "
                         f"{_fmt(change['current'])} | {change['change_pct']:+.1f}% |")
    return "\n".join(lines) + "\n"


def write(run_dir: Path, results: dict) -> None:
    (run_dir / RESULTS_FILE).write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    (run_dir / SUMMARY_FILE).write_text(render_markdown(results), encoding="utf-8")
