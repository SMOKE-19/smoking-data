from __future__ import annotations

import hashlib
import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from smoking_data.core.dataset_compare import compare_parquet_datasets
from smoking_data.core.exceptions import ValidationError
from smoking_data.core.parity_fixtures import write_0201_curated_parity_fixture
from smoking_data.core.results import to_json_safe

from .advisor import BASELINE, advise_pipeline

BENCHMARK_SCHEMA_VERSION = "smoking-data.pwq-benchmark.v1"

CANDIDATES = {
    "sparse": {**BASELINE, "max_ranges_per_task": 256},
    "baseline": dict(BASELINE),
    "dense": {
        **BASELINE,
        "max_ranges_per_task": 128,
        "minimum_range_savings_ratio": 0.20,
    },
}


@dataclass(frozen=True, slots=True)
class PwqBenchmarkHandle:
    summary_path: Path
    runs_path: Path
    recommendation_path: Path
    run_count: int
    parity_ok: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary_path": str(self.summary_path),
            "runs_path": str(self.runs_path),
            "recommendation_path": str(self.recommendation_path),
            "run_count": self.run_count,
            "parity_ok": self.parity_ok,
        }


def benchmark_dummy_0201(
    root: str | Path,
    *,
    repetitions: int = 2,
    max_elapsed_sec: float = 120.0,
    max_input_bytes: int = 256 * 1024 * 1024,
) -> PwqBenchmarkHandle:
    """Benchmark PWQ candidates on the reusable 0201 fixture within explicit budgets."""

    from smoking_data.runtime.runner import run_preset_yaml

    if repetitions < 1:
        raise ValidationError("PWQ repetitions must be >= 1.", code="pwq.invalid_budget")
    if max_elapsed_sec <= 0 or max_input_bytes < 1:
        raise ValidationError("PWQ benchmark budgets must be positive.", code="pwq.invalid_budget")
    base = Path(root).expanduser().resolve()
    fixture = write_0201_curated_parity_fixture(base / "fixture")
    source_files = sorted((base / "fixture" / "DATA" / "0101").rglob("*.parquet"))
    input_bytes = sum(path.stat().st_size for path in source_files)
    if input_bytes > max_input_bytes:
        raise ValidationError(
            "PWQ dummy input exceeds the configured I/O budget.",
            code="pwq.input_budget_exceeded",
            context={"input_bytes": input_bytes, "max_input_bytes": max_input_bytes},
        )
    raw = yaml.safe_load(fixture.yaml_path.read_text(encoding="utf-8"))
    initial_recommendation = advise_pipeline(fixture.yaml_path, project_root=base / "fixture")
    initial_validation = json.loads(
        initial_recommendation.validation_path.read_text(encoding="utf-8")
    )
    probe_manifest = (base / "fixture" / initial_validation["probe_manifests"][0]).resolve()
    raw["source"]["upstream"]["probe_manifest"] = {
        "manifest_path": str(probe_manifest),
    }
    raw.setdefault("execution", {})["reset_before_run"] = True
    fixture.yaml_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    benchmark_id = hashlib.sha256(
        f"{fixture.yaml_path}:{repetitions}:{max_input_bytes}".encode()
    ).hexdigest()[:16]
    output_root = base / ".temp" / "metadata" / "pwq" / "benchmarks" / benchmark_id
    output_root.mkdir(parents=True, exist_ok=True)
    baseline_snapshot = output_root / "baseline_output"
    rows: list[dict[str, Any]] = []
    parity_reports: list[dict[str, Any]] = []
    started = time.perf_counter()
    latest_metadata: Path | None = None

    for candidate_name, candidate in CANDIDATES.items():
        for repetition in range(repetitions):
            if time.perf_counter() - started >= max_elapsed_sec:
                raise ValidationError(
                    "PWQ benchmark elapsed-time budget was exceeded.",
                    code="pwq.elapsed_budget_exceeded",
                    context={"max_elapsed_sec": max_elapsed_sec, "completed_runs": len(rows)},
                )
            config_path = output_root / f"{candidate_name}.config.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "execution": {
                            "range_merge_gap_bytes": candidate["range_merge_gap_bytes"],
                            "max_range_bytes": candidate["max_range_bytes"],
                            "max_ranges_per_task": candidate["max_ranges_per_task"],
                            "minimum_range_savings_ratio": candidate["minimum_range_savings_ratio"],
                        }
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            run_started = time.perf_counter()
            result = run_preset_yaml(
                fixture.yaml_path,
                config_path=config_path,
                project_root=base / "fixture",
            )
            elapsed = time.perf_counter() - run_started
            latest_metadata = result.metadata_path
            task_counters = [item.counters for item in result.details.get("task_results") or []]
            output_root_current = Path(str(raw["output"]["output_dir"]))
            if not baseline_snapshot.exists():
                shutil.copytree(output_root_current, baseline_snapshot)
                parity = {"ok": True, "baseline": True}
            else:
                parity = compare_parquet_datasets(
                    baseline_snapshot,
                    output_root_current,
                ).to_dict()
            parity_reports.append({"candidate": candidate_name, "repetition": repetition, **parity})
            rows.append(
                {
                    "schema_version": BENCHMARK_SCHEMA_VERSION,
                    "candidate": candidate_name,
                    "repetition": repetition,
                    "cache_sequence": "first" if not rows else "repeat",
                    "elapsed_sec": elapsed,
                    "input_bytes": input_bytes,
                    "output_rows": int(result.counters.get("output_rows") or 0),
                    "task_count": len(task_counters),
                    "actual_source_bytes_read": int(
                        sum(
                            float(item.get("actual_source_bytes_read") or 0)
                            for item in task_counters
                        )
                    ),
                    "peak_rss_mb": max(
                        (float(item.get("rss_peak_mb") or 0) for item in task_counters),
                        default=0.0,
                    ),
                    "planned_range_bytes": int(
                        sum(float(item.get("planned_range_bytes") or 0) for item in task_counters)
                    ),
                    "planned_row_group_bytes": int(
                        sum(
                            float(item.get("planned_row_group_bytes") or 0)
                            for item in task_counters
                        )
                    ),
                    "parity_ok": bool(parity["ok"]),
                }
            )

    if latest_metadata is None:
        raise AssertionError("PWQ benchmark produced no runs")
    pq.write_table(
        pa.Table.from_pylist(rows),
        output_root / "benchmark_runs.parquet",
        compression=None,
    )
    parity_ok = all(bool(item["ok"]) for item in parity_reports)
    summary = {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "fixture": "0201-curated",
        "candidate_execution_note": (
            "Candidates change the page-range cost decision. The Rust reader remains "
            "page-index-aware and uses the same logical row selection contract."
        ),
        "budgets": {
            "repetitions": repetitions,
            "max_elapsed_sec": max_elapsed_sec,
            "max_input_bytes": max_input_bytes,
        },
        "run_count": len(rows),
        "elapsed_sec": time.perf_counter() - started,
        "input_bytes": input_bytes,
        "parity_ok": parity_ok,
        "parity_reports": parity_reports,
        "cache_note": "first/repeat is sequence metadata; OS page cache is not forcibly dropped.",
    }
    (output_root / "benchmark_summary.json").write_text(
        json.dumps(to_json_safe(summary), ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    recommendation = advise_pipeline(
        fixture.yaml_path,
        metadata_path=latest_metadata,
        project_root=base / "fixture",
    )
    return PwqBenchmarkHandle(
        summary_path=output_root / "benchmark_summary.json",
        runs_path=output_root / "benchmark_runs.parquet",
        recommendation_path=recommendation.recommendation_path,
        run_count=len(rows),
        parity_ok=parity_ok,
    )
