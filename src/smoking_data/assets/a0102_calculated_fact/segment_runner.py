from __future__ import annotations

import hashlib
import json
import os
import time
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from smoking_data.backends.rust_engine import CuratedTaskRequest, execute_curated_task
from smoking_data.core.exceptions import ValidationError
from smoking_data.core.results import StageResult
from smoking_data.runtime.config import load_config
from smoking_data.runtime.paths import ensure_dir, infer_project_root
from smoking_data.runtime.task_telemetry import (
    TaskTelemetryHandle,
    start_task_telemetry_supervisor,
    task_telemetry_phase,
)

from .calculation_status import update_calculation_status
from .planning import CalculatedFactRunPlan, preflight_calculated_fact_yaml
from .segment_append import SegmentAppendTransaction

RUNNER_PRESET = "0102_calculated_fact"
METADATA_SCHEMA_VERSION = "smoking-data.calculated-fact-run-metadata.v1"
UPSTREAM_SNAPSHOT_VERSION = "smoking-data.upstream-snapshot.v1"
_DISABLE_TELEMETRY_ENV = "SMOKING_DATA_INTERNAL_DISABLE_TASK_TELEMETRY"
_DEFAULT_TASK_PEAK_MEMORY_MB = 512.0


@dataclass(frozen=True, slots=True)
class _PendingMaterializeTask:
    future: Future[dict[str, float]]
    request: CuratedTaskRequest
    task_id: str
    coordinate_index: int
    source_segment_id: str


class _BoundedMaterializeExecutor:
    def __init__(
        self,
        *,
        workers: int,
        telemetry_endpoint: dict[str, Any] | None,
        transaction: SegmentAppendTransaction,
    ) -> None:
        self.workers = workers
        self.telemetry_endpoint = telemetry_endpoint
        self.transaction = transaction
        self.executor = (
            ThreadPoolExecutor(max_workers=workers, thread_name_prefix="0102-materialize")
            if workers > 1
            else None
        )
        self.pending: list[_PendingMaterializeTask] = []
        self.statistics: list[dict[str, float]] = []

    def submit(
        self,
        request: CuratedTaskRequest,
        *,
        task_id: str,
        coordinate_index: int,
        source_segment_id: str,
    ) -> None:
        if self.executor is None:
            future: Future[dict[str, float]] = Future()
            try:
                future.set_result(
                    _execute_materialize_task(
                        request,
                        telemetry_endpoint=self.telemetry_endpoint,
                        task_id=task_id,
                    )
                )
            except BaseException as exc:
                future.set_exception(exc)
        else:
            future = self.executor.submit(
                _execute_materialize_task,
                request,
                telemetry_endpoint=self.telemetry_endpoint,
                task_id=task_id,
            )
        self.pending.append(
            _PendingMaterializeTask(
                future=future,
                request=request,
                task_id=task_id,
                coordinate_index=coordinate_index,
                source_segment_id=source_segment_id,
            )
        )
        if len(self.pending) >= self.workers:
            self._adopt_first()

    def finish(self) -> list[dict[str, float]]:
        while self.pending:
            self._adopt_first()
        self._shutdown(wait=True)
        return self.statistics

    def abort(self) -> None:
        for task in self.pending:
            task.future.cancel()
        self._shutdown(wait=True, cancel_futures=True)

    def _adopt_first(self) -> None:
        task = self.pending.pop(0)
        stats = task.future.result()
        with task_telemetry_phase(
            self.telemetry_endpoint,
            "0102.adopt_task_outputs",
            task_id=task.task_id,
        ):
            outputs = sorted(task.request.output_dir.rglob("*.parquet"))
            if not outputs:
                raise ValidationError(
                    "0102 Rust task produced no Parquet output.",
                    code="append.invalid_task_output",
                    context={"coordinate_index": task.coordinate_index},
                )
            for output in outputs:
                self.transaction.adopt_parquet_file(
                    output, source_segment_id=task.source_segment_id
                )
        self.statistics.append(stats)

    def _shutdown(self, *, wait: bool, cancel_futures: bool = False) -> None:
        if self.executor is not None:
            self.executor.shutdown(wait=wait, cancel_futures=cancel_futures)
            self.executor = None


def _execute_materialize_task(
    request: CuratedTaskRequest,
    *,
    telemetry_endpoint: dict[str, Any] | None,
    task_id: str,
) -> dict[str, float]:
    with task_telemetry_phase(
        telemetry_endpoint,
        "0102.calculate_unpivot_write",
        task_id=task_id,
    ):
        return execute_curated_task(request)


def _materialize_admission(
    plan: CalculatedFactRunPlan,
    *,
    config: Any,
) -> dict[str, Any]:
    requested = plan.spec.materialize_workers
    global_policy = config.phase_memory_policy(
        "materialize", requested_workers=requested
    )
    worker_cap = min(plan.spec.materialize_worker_max, global_policy.max_workers)
    budget_mb = float(config.memory_budget_mb) * config.memory_safety_ratio
    estimate_mb = _DEFAULT_TASK_PEAK_MEMORY_MB
    estimate_source = "conservative_default"
    metadata_path = plan.output_root / "_smoking_data" / "metadata.json"
    try:
        previous = json.loads(metadata_path.read_text(encoding="utf-8"))
        maximum = (
            previous.get("phase_telemetry", {})
            .get("phase_statistics", {})
            .get("0102.calculate_unpivot_write", {})
            .get("max_rss_mb", {})
            .get("max")
        )
        if isinstance(maximum, (int, float)) and not isinstance(maximum, bool) and maximum > 0:
            estimate_mb = max(256.0, float(maximum))
            estimate_source = "previous_phase_process_peak"
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        pass
    memory_cap = max(1, int(budget_mb // estimate_mb))
    effective = max(1, min(requested, worker_cap, memory_cap))
    return {
        "requested_workers": requested,
        "effective_workers": effective,
        "phase_worker_max": worker_cap,
        "memory_worker_cap": memory_cap,
        "memory_budget_mb": round(budget_mb, 3),
        "estimated_task_peak_memory_mb": round(estimate_mb, 3),
        "estimate_source": estimate_source,
        "queue_mode": "bounded_to_effective_workers",
        "coordinator_mode": "single_manifest_commit",
    }


class _TelemetrySession:
    def __init__(self, handle: TaskTelemetryHandle) -> None:
        self.handle = handle
        self.profile: dict[str, Any] | None = None

    @property
    def endpoint(self) -> dict[str, Any] | None:
        return self.handle.endpoint

    def stop(self) -> dict[str, Any]:
        if self.profile is None:
            self.profile = self.handle.stop()
        return self.profile

    def __enter__(self) -> _TelemetrySession:
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()


class _ListShapeAccumulator:
    _SAMPLE_LIMIT = 8192

    def __init__(self, columns: Sequence[str]) -> None:
        self.columns = tuple(columns)
        self.rows = 0
        self.null_lists = 0
        self.empty_lists = 0
        self.child_values = 0
        self.max_length = 0
        self._seen_lengths = 0
        self._length_sample: list[int] = []

    def observe(self, batch: pa.RecordBatch) -> None:
        if not self.columns:
            return
        self.rows += batch.num_rows
        for name in self.columns:
            index = batch.schema.get_field_index(name)
            if index < 0:
                continue
            array = batch.column(index)
            lengths = pc.list_value_length(array).to_pylist()
            self.null_lists += array.null_count
            for length in lengths:
                if length is None:
                    continue
                value = int(length)
                self.empty_lists += int(value == 0)
                self.child_values += value
                self.max_length = max(self.max_length, value)
                self._sample(value)

    def to_dict(self, plan: CalculatedFactRunPlan) -> dict[str, Any]:
        ordered = sorted(self._length_sample)
        p95 = (
            ordered[min(len(ordered) - 1, int((len(ordered) - 1) * 0.95))]
            if ordered
            else None
        )
        strategies: dict[str, int] = defaultdict(int)
        for expression in plan.expressions:
            strategies[expression.strategy.value] += 1
        return {
            "observation_mode": "rust_streaming_without_python_payload_rescan",
            "strategy_counts": dict(sorted(strategies.items())),
            "list_columns": list(self.columns),
            "parent_rows_observed": self.rows,
            "null_list_count": self.null_lists,
            "empty_list_count": self.empty_lists,
            "child_value_count": self.child_values,
            "max_list_length": self.max_length,
            "p95_list_length_approx": p95,
            "length_sample_size": len(ordered),
        }

    def _sample(self, value: int) -> None:
        self._seen_lengths += 1
        if len(self._length_sample) < self._SAMPLE_LIMIT:
            self._length_sample.append(value)
            return
        slot = int.from_bytes(
            hashlib.sha256(str(self._seen_lengths).encode()).digest()[:8], "little"
        ) % self._seen_lengths
        if slot < self._SAMPLE_LIMIT:
            self._length_sample[slot] = value


def run_yaml(
    definition_path: str | Path,
    *,
    config_path: str | Path | None = None,
    project_root: str | Path | None = None,
    trigger_type: str = "manual",
) -> StageResult:
    definition = Path(definition_path).expanduser().resolve()
    job_name = definition.stem
    effective_project_root = project_root or infer_project_root(definition)
    try:
        plan = preflight_calculated_fact_yaml(
            definition,
            config_path=config_path,
            project_root=effective_project_root,
        )
        job_name = plan.spec.job_name
        config = load_config(
            config_path=config_path,
            project_root=effective_project_root,
            asset_code="0102",
        )
        result = _execute_plan(plan, config=config, trigger_type=trigger_type)
        artifact = plan.spec.output.get("artifact") or {}
        from smoking_data.runtime.object_store.config import PublicationSpec
        from smoking_data.runtime.object_store.publication import publish_committed_dataset

        publication = PublicationSpec.from_mapping(artifact.get("publication"))
        if publication is not None and result.ok:
            published = publish_committed_dataset(
                plan.output_root,
                project_root=config.project_root,
                publication=publication,
                asset_code="0102",
                job_name=plan.spec.job_name,
                definition_sha256=plan.spec.canonical_hash,
            )
            result.details["remote_publication"] = (
                {
                    "status": published.status,
                    "target": published.target,
                    "dataset_uri": published.dataset_uri,
                    "generation_id": published.generation_id,
                    "manifest_key": published.manifest_key,
                    "receipt_path": str(published.receipt_path),
                }
                if published is not None
                else None
            )
        return result
    except Exception as exc:  # noqa: BLE001
        return StageResult.failure(
            preset=RUNNER_PRESET,
            job_name=job_name,
            yaml_path=definition,
            exc=exc,
            details={"trigger_type": trigger_type},
        )


def _execute_plan(
    plan: CalculatedFactRunPlan,
    *,
    config: Any,
    trigger_type: str,
) -> StageResult:
    from .segment_runtime import execute_segment_plan

    ensure_dir(plan.output_root)
    ensure_dir(config.temp_root)
    if os.environ.get(_DISABLE_TELEMETRY_ENV) == "1":
        handle = TaskTelemetryHandle(
            process=None,
            endpoint=None,
            log_path=None,
            ready_path=None,
            summary_path=None,
            start_profile={
                "schema_version": "smoking-data.task-telemetry.v1",
                "status": "disabled",
                "reason": _DISABLE_TELEMETRY_ENV,
            },
        )
    else:
        handle = start_task_telemetry_supervisor(
            log_path=(
                config.log_root
                / "task-telemetry"
                / f"0102_{plan.spec.job_name}_{time.time_ns()}.jsonl"
            ),
            progress_title=f"smoking-data 0102 · {plan.spec.job_name}",
        )
    with _TelemetrySession(handle) as telemetry:
        return execute_segment_plan(
            plan,
            config=config,
            trigger_type=trigger_type,
            telemetry=telemetry,
        )


def _write_run_metadata(
    plan: CalculatedFactRunPlan,
    *,
    run_key: str,
    trigger_type: str,
    planner_stats: dict[str, Any],
    incremental: Any,
    generation_seq: int | None,
    task_count: int,
    output_files: Sequence[Path],
    reused: bool,
    task_stats: Sequence[dict[str, float]] = (),
    telemetry: dict[str, Any] | None = None,
    list_shapes: dict[str, Any] | None = None,
    upstream_snapshot: dict[str, Any] | None = None,
    planning_mode: str = "segment_manifest_append_only",
    coordinate_passes: int = 1,
    materialize_admission: dict[str, Any] | None = None,
    upstream_delta: dict[str, Any] | None = None,
    schema_change_handling: dict[str, Any] | None = None,
) -> Path:
    path = plan.output_root / "_smoking_data" / "metadata.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": METADATA_SCHEMA_VERSION,
        "asset_code": "0102",
        "job_name": plan.spec.job_name,
        "definition": str(plan.spec.path),
        "definition_hash": plan.spec.canonical_hash,
        "plan_hash": plan.plan_hash,
        "run_key": run_key,
        "trigger_type": trigger_type,
        "generation_seq": generation_seq,
        "reused": reused,
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "planner": planner_stats,
        "upstream_snapshot": upstream_snapshot or {},
        "upstream_delta": upstream_delta or {"mode": "unavailable"},
        "schema_change_handling": schema_change_handling or {},
        "operation_execution": {
            "sequence": list(plan.spec.operation_sequence),
            "model": "task_local_row_group_streaming",
            "worker_scope": "materialize_task",
        },
        "planning_memory": {
            "mode": planning_mode,
            "coordinate_passes": coordinate_passes,
            "max_live_coordinate_chunks": 1,
            "temporary_journal_mode": "not_used",
            "registry_commit_mode": "not_used",
            "snapshot_fast_path": False,
        },
        "incremental": {
            "calculated_facts": incremental.calculate_count,
            "reused_facts": incremental.reuse_count,
            "inactive_facts": incremental.inactive_count,
            "deleted_identities": incremental.deleted_identity_count,
        },
        "tasks": {"count": task_count, "statistics": list(task_stats)},
        "materialize_execution": materialize_admission
        or {
            "requested_workers": plan.spec.materialize_workers,
            "effective_workers": 0 if reused else 1,
            "status": "not_executed" if reused else "default",
        },
        "task_telemetry": telemetry,
        "phase_telemetry": _phase_telemetry(telemetry),
        "list_execution": list_shapes or {},
        "output_files": [str(item) for item in output_files],
    }
    staging = path.with_suffix(path.suffix + ".tmp")
    staging.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    staging.replace(path)
    return path


def _phase_telemetry(profile: dict[str, Any] | None) -> dict[str, Any]:
    phases = [
        item
        for item in (profile or {}).get("phase_profiles") or []
        if isinstance(item, dict)
    ]
    names = sorted({str(item.get("phase_name") or "") for item in phases})
    statistics = {
        name: {
            "instances": len(selected := [item for item in phases if item.get("phase_name") == name]),
            "completed": sum(item.get("status") == "completed" for item in selected),
            "elapsed_sec": _metric_summary(selected, "elapsed_sec"),
            "max_rss_mb": _metric_summary(selected, "max_rss_mb"),
            "cpu_sec": _metric_summary(selected, "cpu_sec"),
            "requested_read_bytes": _metric_summary(selected, "requested_read_bytes"),
            "requested_write_bytes": _metric_summary(selected, "requested_write_bytes"),
        }
        for name in names
    }
    return {
        "schema_version": "smoking-data.phase-telemetry.v1",
        "status": (
            "completed"
            if phases and all(item.get("status") == "completed" for item in phases)
            else ("report_unavailable" if not phases else "partial")
        ),
        "phase_names": names,
        "phase_statistics": statistics,
    }


def schema_change_handling(
    plan: CalculatedFactRunPlan,
    *,
    upstream_delta: Any,
    observed_at: datetime,
    segment_skips: Sequence[dict[str, Any]] = (),
) -> dict[str, Any]:
    """Build the durable per-run record for calculations skipped by schema drift."""

    metadata_path = plan.output_root / "_smoking_data" / "metadata.json"
    previous: dict[str, Any] = {}
    try:
        previous = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    previous_section = previous.get("schema_change_handling") or {}
    previous_items = {
        str(item.get("expression_name")): dict(item)
        for item in previous_section.get("skipped_expressions") or []
        if isinstance(item, dict) and item.get("expression_name")
    }
    timestamp = observed_at.isoformat()
    segment_ids = [str(item.segment_id) for item in getattr(upstream_delta, "current_segments", ())]
    current_names = {item.expression_name for item in plan.skipped_expressions}
    skipped_items: list[dict[str, Any]] = []
    for item in plan.skipped_expressions:
        old = previous_items.get(item.expression_name, {})
        key = ",".join(
            sorted(str(dep.get("logical_name")) for dep in item.missing_dependencies)
        )
        previous_key = ",".join(
            sorted(
                str(dep.get("logical_name"))
                for dep in old.get("missing_dependencies") or []
                if isinstance(dep, dict)
            )
        )
        skipped_items.append(
            {
                "expression_name": item.expression_name,
                "status": item.status,
                "missing_dependencies": list(item.missing_dependencies),
                "source_segment_ids": segment_ids,
                "first_skipped_at": old.get("first_skipped_at", timestamp),
                "last_skipped_at": timestamp,
                "first_schema_hash": old.get("first_schema_hash", plan.source_schema_hash),
                "last_schema_hash": plan.source_schema_hash,
                "resume_condition": "dependency_available",
                "changed_since_previous": key != previous_key,
            }
        )
    resumed: list[dict[str, str]] = []
    for name, old in previous_items.items():
        if name not in current_names and old.get("resumed_at") is None:
            resumed.append(
                {
                    "expression_name": name,
                    "resumed_at": timestamp,
                    "previous_schema_hash": str(old.get("last_schema_hash") or ""),
                    "current_schema_hash": plan.source_schema_hash,
                }
            )
    status_files = update_calculation_status(
        plan.output_root,
        asset_code="0102",
        job_name=plan.spec.job_name,
        upstream_asset_code=plan.spec.upstream_asset_code,
        upstream_generation_id=getattr(upstream_delta, "generation_id", None),
        upstream_schema_hash=plan.source_schema_hash,
        global_skips=skipped_items,
        segment_skips=list(segment_skips),
        active_expression_names={item.name for item in plan.fingerprints},
        observed_segment_ids={
            str(item.segment_id)
            for item in getattr(upstream_delta, "current_segments", ())
        },
        observed_at=observed_at,
    )
    return {
        "policy": "skip_missing_calculations",
        "status": "completed_with_skips" if (skipped_items or segment_skips) else "no_skips",
        "output_effect": "no_new_fact",
        "downstream_selection_owner": "0201",
        "upstream_asset_code": plan.spec.upstream_asset_code,
        "upstream_schema_hash": plan.source_schema_hash,
        "upstream_generation_id": getattr(upstream_delta, "generation_id", None),
        "skipped_expression_count": len(skipped_items),
        "skipped_expressions": skipped_items,
        "segment_skipped_expression_count": len(segment_skips),
        "segment_skips": list(segment_skips),
        "resumed_expressions": resumed,
        "status_files": status_files,
    }


def _metric_summary(items: Sequence[dict[str, Any]], key: str) -> dict[str, float | int | None]:
    values = sorted(
        float(item[key])
        for item in items
        if isinstance(item.get(key), (int, float)) and not isinstance(item.get(key), bool)
    )
    return {
        "count": len(values),
        "avg": sum(values) / len(values) if values else None,
        "max": values[-1] if values else None,
    }


def _counters(
    planner_stats: dict[str, Any],
    incremental: Any,
    *,
    task_count: int,
    plan: CalculatedFactRunPlan | None = None,
) -> dict[str, int | float]:
    skipped = len(plan.skipped_expressions) if plan is not None else 0
    return {
        "selected_rows": planner_stats.get("selected_row_count", 0.0),
        "coordinate_chunks": planner_stats.get("coord_chunk_count", 0.0),
        "calculated_facts": incremental.calculate_count,
        "reused_facts": incremental.reuse_count,
        "inactive_facts": incremental.inactive_count,
        "task_count": task_count,
        "planned_expressions": len(plan.fingerprints) if plan is not None else 0,
        "calculated_expressions": len(plan.fingerprints) if plan is not None else 0,
        "skipped_expressions": skipped,
        "skipped_missing_dependency_expressions": (
            sum(item.status == "skipped_missing_dependency" for item in plan.skipped_expressions)
            if plan is not None
            else 0
        ),
        "skipped_upstream_expression_count": (
            sum(item.status == "skipped_upstream_expression" for item in plan.skipped_expressions)
            if plan is not None
            else 0
        ),
    }


def _upstream_snapshot(files: Sequence[Path]) -> dict[str, Any]:
    entries = []
    digest = hashlib.sha256()
    total_bytes = 0
    for source in sorted(Path(item).expanduser().resolve() for item in files):
        stat = source.stat()
        entry = {
            "path": str(source),
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "ctime_ns": stat.st_ctime_ns,
            "inode": stat.st_ino,
        }
        encoded = json.dumps(entry, sort_keys=True, separators=(",", ":")).encode()
        digest.update(len(encoded).to_bytes(8, "little"))
        digest.update(encoded)
        entries.append(entry)
        total_bytes += stat.st_size
    return {
        "schema_version": UPSTREAM_SNAPSHOT_VERSION,
        "identity_mode": "path_stat",
        "snapshot_hash": digest.hexdigest(),
        "file_count": len(entries),
        "total_bytes": total_bytes,
    }


def _upstream_delta_metadata(plan: Any) -> dict[str, Any]:
    return {
        "mode": plan.mode,
        "reason": plan.reason,
        "dataset_id": plan.dataset_id,
        "generation_id": plan.generation_id,
        "current_segments": len(plan.current_segments),
        "selected_segments": len(plan.selected_segments),
        "removed_segments": len(plan.removed_segment_ids),
        "selected_files": len(plan.selected_files),
    }
