from __future__ import annotations

import gc
import hashlib
import json
import math
import os
import shutil
import time
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from smoking_data.backends.rust_engine import (
    CuratedTaskRequest,
    execute_curated_task,
    validate_dataset_assertions,
)
from smoking_data.backends.streaming_sbdf import SbdfExportRequest, export_sbdf_with_result
from smoking_data.core.barriers import (
    BarrierState,
    ensure_complete_group_within_budget,
)
from smoking_data.core.engine_contract import (
    PAYLOAD_ENGINE,
    TASK_CONTRACT_VERSION,
    engine_metadata,
    normalize_list_restore_type,
    validate_rust_payload_contract,
)
from smoking_data.core.exceptions import SmokingDataError, TaskExecutionError, ValidationError
from smoking_data.core.logical_plan import compile_0201_logical_plan
from smoking_data.core.optimizer import optimize_logical_plan
from smoking_data.core.physical_plan import (
    PhysicalTask,
    SourceSpan,
    admitted_worker_count,
    build_logical_physical_candidates,
    build_physical_plan,
    choose_output_row_group_rows,
    choose_physical_plan,
    estimate_plan_cost,
    reconcile_task_memory,
)
from smoking_data.core.results import StageResult
from smoking_data.core.tasks import TaskResult, TaskSpec
from smoking_data.ops.coordinates import (
    ACTIVE_ORDER_COLUMN,
    PART_INDEX_COLUMN,
    SOURCE_FILE_COLUMN,
    SOURCE_ROW_GROUP_COLUMN,
    SOURCE_ROW_INDEX_COLUMN,
    attach_row_group_ids,
    write_rust_coordinate_file,
)
from smoking_data.ops.fingerprint import combined_fingerprint, file_fingerprint
from smoking_data.ops.parquet_metadata import profile_parquet_files
from smoking_data.ops.projection import (
    apply_add_calc,
    apply_exclude_columns,
    apply_filter_sql,
    apply_include_columns,
    apply_reference_replace,
    apply_type_casts,
    resolve_add_calc_expression,
)
from smoking_data.ops.upstream import discover_parquet_files
from smoking_data.planners.active_sidecar import (
    DECISION_SCHEMA_VERSION as ACTIVE_SIDECAR_DECISION_VERSION,
)
from smoking_data.planners.active_sidecar import (
    build_active_sidecar_decision,
    profile_selector_shape,
)
from smoking_data.planners.phase_memory import build_phase_memory_admission
from smoking_data.planners.pivot_shape import build_pivot_shape_profile
from smoking_data.planners.pivot_sizing import (
    build_adaptive_sizing_shadow_decision,
    build_adaptive_task_boundary_decision,
    build_calibrated_complete_group_guard,
    build_calibrated_worker_admission,
    build_materialize_calibration_plan,
    build_pivot_sizing_reconciliation,
    build_task_peak_memory_model,
    matching_history_compression_ratio,
)
from smoking_data.runtime.active_sidecar_plan import (
    REQUEST_SCHEMA_VERSION as ACTIVE_SIDECAR_PLAN_REQUEST_VERSION,
)
from smoking_data.runtime.active_sidecar_plan import (
    load_active_sidecar_plan,
    run_active_sidecar_pipeline_subprocess,
    run_active_sidecar_plan_subprocess,
)
from smoking_data.runtime.adaptive_sizing_registry import (
    build_adaptive_sizing_key,
    load_adaptive_sizing_model,
    load_phase_memory_history,
    record_adaptive_sizing_model,
)
from smoking_data.runtime.artifacts import (
    active_snapshot_path_for,
    artifact_root_for,
    candidate_manifest_path_for,
    candidate_sidecar_root_for,
)
from smoking_data.runtime.config import RuntimeConfig
from smoking_data.runtime.console_progress import worker_console_enabled
from smoking_data.runtime.intermediate import write_sorted_intermediate
from smoking_data.runtime.memory import current_rss_mb, peak_rss_mb
from smoking_data.runtime.metadata import metadata_path_for, read_metadata, write_metadata
from smoking_data.runtime.naming import (
    NAMING_POLICY_VERSION,
    part_file_name,
    partition_dir_name,
    task_id,
)
from smoking_data.runtime.object_store.remote_upstream import materialize_remote_parquet_files
from smoking_data.runtime.output_contract import resolve_physical_writer_output
from smoking_data.runtime.output_physical_layout import (
    TASK_ADAPTIVE,
    previous_output_physical_layout_matches,
    resolve_configured_row_group_rows,
    resolve_output_physical_layout,
)
from smoking_data.runtime.parquet_probe.probe import (
    parquet_footer_fingerprint,
    validate_probe_manifest,
)
from smoking_data.runtime.parquet_probe.ranges import plan_coordinate_page_ranges
from smoking_data.runtime.paths import (
    ensure_dir,
    reset_path,
    resolve_project_path,
)
from smoking_data.runtime.selector_piece import (
    REQUEST_SCHEMA_VERSION as SELECTOR_PIECE_REQUEST_VERSION,
)
from smoking_data.runtime.selector_piece import run_selector_piece_subprocess
from smoking_data.runtime.task_runner import run_tasks_in_subprocesses
from smoking_data.runtime.task_telemetry import (
    emit_task_telemetry_event,
    start_task_telemetry_supervisor,
    task_telemetry_phase,
)
from smoking_data.runtime.telemetry_reconcile import reconcile_task_telemetry
from smoking_data.runtime.test_run import final_task_limit, select_final_tasks
from smoking_data.runtime.transactions import (
    DatasetTransaction,
    recover_orphan_transactions,
    validate_committed_dataset,
)
from smoking_data.runtime.yaml_loader import PresetSpec

PRESET_NAME = "0201"
RUST_DIRECT_TYPE_MAP = {
    "TEXT": "TEXT",
    "STRING": "TEXT",
    "TINYINT": "TINYINT",
    "INT8": "TINYINT",
    "SMALLINT": "SMALLINT",
    "INT16": "SMALLINT",
    "INT32": "INTEGER",
    "INT64": "BIGINT",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "FLOAT32": "FLOAT",
    "REAL": "FLOAT",
    "FLOAT64": "DOUBLE",
    "DOUBLE": "DOUBLE",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "DATETIME": "TIMESTAMP",
    "DURATION": "DURATION",
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
}
ESTIMATED_PAYLOAD_BYTES_COLUMN = "__estimated_payload_bytes"
SPILL_REQUIRED_COLUMN = "__spill_required"
CANDIDATE_MANIFEST_VERSION = "smoking-data.0201-candidates.v1"
SOURCE_SNAPSHOT_CHANGED_MARKER = "SOURCE_SNAPSHOT_CHANGED "
INTERNAL_DISABLE_TASK_TELEMETRY_ENV = "SMOKING_DATA_INTERNAL_DISABLE_TASK_TELEMETRY"
INTERNAL_DISABLE_ACTIVE_SIDECAR_PLAN_ENV = "SMOKING_DATA_INTERNAL_DISABLE_ACTIVE_SIDECAR_PLAN"
INTERNAL_FORCE_ACTIVE_SIDECAR_PLAN_ENV = "SMOKING_DATA_INTERNAL_FORCE_ACTIVE_SIDECAR_PLAN"
SELECTOR_TARGET_ROWS_PER_BUCKET = 100_000
SNAPSHOT_PARTITION_COLUMN = "__smoking_data_snapshot_partition"


def can_run(preset: str) -> bool:
    return preset == PRESET_NAME


def run(spec: PresetSpec, *, config: RuntimeConfig) -> StageResult:
    raw = spec.raw
    asset_code = str((raw.get("__pipeline") or {}).get("asset_code") or "0201")
    execution = _mapping(raw.get("execution"), section="execution", allow_missing=True)
    test_run_limit = final_task_limit(execution)
    if "payload_engine" in execution or "writer_engine" in execution:
        raise ValidationError(
            "0201 payload engine is an internal runtime contract and cannot be selected in YAML."
        )
    source = _mapping(raw.get("source"), section="source")
    upstream = _mapping(source.get("upstream"), section="source.upstream")
    payload = _mapping(source.get("payload"), section="source.payload")
    row_selection = _mapping(raw.get("row_selection"), section="row_selection", allow_missing=True)
    pivot = _mapping(raw.get("pivot"), section="pivot", allow_missing=True)
    output = resolve_physical_writer_output(raw, asset_code="0201")
    single_file_output = bool((raw.get("__pipeline") or {}).get("single_file_output"))
    physical_raw = {**raw, "output": output}
    list_restore = _mapping(raw.get("list_restore"), section="list_restore", allow_missing=True)
    _validate_reserved_payload_columns(payload)
    expression_ir = _compile_expression_ir(payload.get("add_calc"))
    expression_ir_hash = _expression_ir_hash(expression_ir)
    logical_plan = compile_0201_logical_plan(physical_raw, expression_ir=expression_ir)
    logical_plan_hash = str(
        (raw.get("__pipeline") or {}).get("logical_plan_hash") or logical_plan.plan_hash
    )
    optimization = optimize_logical_plan(logical_plan, enabled=config.optimizer_enabled)
    validate_rust_payload_contract(
        payload,
        list_restore=list_restore,
        expression_ir=expression_ir,
    )

    remote = upstream.get("remote")
    if isinstance(remote, dict):
        files = materialize_remote_parquet_files(
            config.project_root,
            target_name=str(remote.get("target") or ""),
            dataset_prefix=str(remote.get("dataset_prefix") or ""),
            relative_paths=[str(value) for value in remote.get("relative_paths") or []],
            recursive=bool(remote.get("recursive", True)),
        )
        source_paths = [str(item.path) for item in files]
        source_recursive = False
    else:
        source_paths = _string_list(upstream.get("paths"), section="source.upstream.paths")
        source_recursive = bool(upstream.get("recursive", True))
        files = discover_parquet_files(
            [resolve_project_path(path, project_root=config.project_root) for path in source_paths],
            recursive=source_recursive,
        )
    probe_profile: dict[str, Any] | None = None
    probe_handle = upstream.get("probe_manifest")
    if isinstance(probe_handle, dict) and probe_handle.get("manifest_path"):
        probe_manifest = validate_probe_manifest(
            str(probe_handle["manifest_path"]),
            files=[item.path for item in files],
            project_root=config.project_root,
        )
        capabilities = dict(probe_manifest.get("capabilities") or {})
        probe_profile = {
            "manifest_path": str(probe_handle["manifest_path"]),
            "dataset_fingerprint": probe_manifest["dataset_fingerprint"],
            "capabilities": capabilities,
            "read_path": (
                "range_indexed" if capabilities.get("page_index") else "row_group_selected"
            ),
        }
    _validate_lookup_uniqueness(
        payload,
        list_restore=list_restore,
        project_root=config.project_root,
    )
    reference_files = _discover_reference_files(
        payload,
        list_restore=list_restore,
        project_root=config.project_root,
    )
    upstream_fingerprint = combined_fingerprint(files)
    reference_fingerprint = combined_fingerprint(reference_files)
    source_fingerprint = combined_fingerprint([*files, *reference_files])
    task_fingerprint = hashlib.sha256(
        f"{source_fingerprint}:{logical_plan_hash}:{expression_ir_hash or 'none'}".encode()
    ).hexdigest()
    skipped_result = (
        None
        if test_run_limit is not None
        else _maybe_skip_unchanged(
            spec,
            config=config,
            task_fingerprint=task_fingerprint,
            asset_code=str((raw.get("__pipeline") or {}).get("asset_code") or "0201"),
            physical_layout_policy=output.get("physical_layout"),
            compression=str(output.get("compression") or "zstd"),
        )
    )
    if skipped_result is not None:
        return skipped_result

    partition_column = str(output.get("partition_column") or "").strip()
    output_dir_raw = output.get("output_dir")
    if not partition_column:
        raise ValidationError("output.partition_column is required.")
    if not output_dir_raw:
        raise ValidationError("output.output_dir is required.")
    output_dir = resolve_project_path(str(output_dir_raw), project_root=config.project_root)
    recovery_profile = recover_orphan_transactions(output_dir)
    run_started = time.perf_counter()
    phase_profile: dict[str, Any] = {}

    sort_first = _mapping(
        row_selection.get("sort_first"), section="row_selection.sort_first", allow_missing=True
    )
    sort_first_enabled = bool(sort_first.get("enabled", False))
    selector_operation_id = str(sort_first.get("operation_id") or "active_row_selection")
    selector_group_keys = [str(item) for item in (sort_first.get("group_keys") or [])]
    selector_payload_configured = "payload" in sort_first
    selector_payload = _mapping(
        sort_first.get("payload"),
        section="row_selection.sort_first.payload",
        allow_missing=True,
    )
    selector_payload_expression_ir = _compile_expression_ir(selector_payload.get("add_calc"))
    previous_metadata = read_metadata(spec, config=config) or {}
    pipeline_graph = (raw.get("__pipeline") or {}).get("graph") or {}
    canonical_node_keys = sorted(
        str(item.get("node_key") or "")
        for item in pipeline_graph.get("nodes") or []
        if isinstance(item, dict) and item.get("node_key")
    )
    adaptive_canonical_op_hash = (
        hashlib.sha256(json.dumps(canonical_node_keys, separators=(",", ":")).encode()).hexdigest()
        if canonical_node_keys
        else logical_plan_hash
    )
    adaptive_sizing_key = build_adaptive_sizing_key(
        [item.path for item in files],
        canonical_op_hash=adaptive_canonical_op_hash,
        pivot=pivot,
        compression=str(output.get("compression") or "zstd"),
        engine=engine_metadata(expression_ir_hash=expression_ir_hash),
    )
    registry_model = load_adaptive_sizing_model(
        project_root=config.project_root,
        model_key=str(adaptive_sizing_key["model_key"]),
    )
    sidecar_memory_policy = config.phase_memory_policy(
        "build_sidecar", requested_workers=config.sidecar_workers
    )
    sidecar_memory_history = load_phase_memory_history(
        project_root=config.project_root,
        model_key=str(adaptive_sizing_key["model_key"]),
        phase_name="build_sidecar.candidate",
        admission_limit_mb=int(config.memory_budget_mb * config.memory_safety_ratio),
    )
    materialize_memory_policy = config.phase_memory_policy(
        "materialize", requested_workers=config.workers
    )
    materialize_memory_history = load_phase_memory_history(
        project_root=config.project_root,
        model_key=str(adaptive_sizing_key["model_key"]),
        phase_name="materialize.fused",
        admission_limit_mb=int(config.memory_budget_mb * config.memory_safety_ratio),
    )
    registry_history_metadata = deepcopy(
        registry_model["model"] if registry_model is not None else {}
    )
    if registry_model is not None:
        registry_history_metadata.setdefault("result", {}).setdefault("details", {})[
            "logical_plan_hash"
        ] = logical_plan_hash
    registry_boundary_decision = build_adaptive_task_boundary_decision(
        registry_history_metadata,
        logical_plan_hash=logical_plan_hash,
        configured_rows_per_part=config.target_rows_per_part,
        pivot_enabled=bool(pivot.get("enabled", False)),
    )
    local_boundary_decision = build_adaptive_task_boundary_decision(
        previous_metadata,
        logical_plan_hash=logical_plan_hash,
        configured_rows_per_part=config.target_rows_per_part,
        pivot_enabled=bool(pivot.get("enabled", False)),
    )
    learned_registry_model = bool(
        registry_model is not None
        and registry_boundary_decision.get("applied")
        and not materialize_memory_history["recalibration_required"]
    )
    adaptive_history_metadata = (
        registry_history_metadata if learned_registry_model else previous_metadata
    )
    adaptive_task_boundary_decision = (
        registry_boundary_decision if learned_registry_model else local_boundary_decision
    )
    history_compression_ratio = matching_history_compression_ratio(
        adaptive_history_metadata,
        logical_plan_hash=logical_plan_hash,
    )
    rows_per_part = int(adaptive_task_boundary_decision["effective_rows_per_part"])
    learned_registry_details = (
        ((registry_model or {}).get("model") or {}).get("result", {}).get("details", {})
        if registry_model is not None
        else {}
    )
    previous_details = (previous_metadata.get("result") or {}).get("details") or {}
    previous_candidate_sidecar = previous_details.get("candidate_sidecar") or {}
    registry_active_sidecar_decision = learned_registry_details.get("active_sidecar_decision")
    local_active_sidecar_decision = (
        previous_candidate_sidecar.get("active_sidecar_decision")
        if previous_details.get("logical_plan_hash") == logical_plan_hash
        else None
    )
    previous_active_sidecar_decision = (
        registry_active_sidecar_decision
        if isinstance(registry_active_sidecar_decision, dict)
        else local_active_sidecar_decision
        if isinstance(local_active_sidecar_decision, dict)
        else None
    )
    sidecar_memory_admission = build_phase_memory_admission(
        phase="build_sidecar",
        hard_limit_mb=config.memory_budget_mb,
        safety_ratio=config.memory_safety_ratio,
        policy=sidecar_memory_policy,
        requested_workers=config.sidecar_workers,
        historical_worker_peak_p95_mb=sidecar_memory_history["peak_rss_p95_mb"],
        fallback_worker_peak_mb=min(
            config.sidecar_max_projected_bytes_mb,
            max(64, int(config.memory_budget_mb * config.memory_safety_ratio) // 2),
        ),
    )
    sidecar_phase_budget_mb = int(sidecar_memory_admission["safe_envelope_mb"])
    admitted_sidecar_workers = int(sidecar_memory_admission["admitted_workers"])
    sidecar_projected_limit_mb = max(
        1,
        min(
            config.sidecar_max_projected_bytes_mb,
            int(sidecar_memory_admission["worker_pool_mb"]) // max(1, admitted_sidecar_workers),
        ),
    )
    materialize_phase_budget_mb = int(config.memory_budget_mb * config.memory_safety_ratio)
    active_snapshot_target = active_snapshot_path_for(
        spec,
        config=config,
        operation_id=selector_operation_id if sort_first_enabled else None,
    )
    selector_started = time.perf_counter()
    telemetry_disabled_for_benchmark = os.environ.get(INTERNAL_DISABLE_TASK_TELEMETRY_ENV) == "1"
    sidecar_telemetry_handle = None
    sidecar_telemetry_profile: dict[str, Any] | None = None
    if not telemetry_disabled_for_benchmark:
        sidecar_telemetry_handle = start_task_telemetry_supervisor(
            log_path=(
                config.log_root
                / "task-telemetry"
                / f"0201_{partition_dir_name(spec.job_name)}_sidecar_{time.time_ns()}.jsonl"
            ),
            progress_title=f"smoking-data {asset_code} · {spec.job_name} · build_sidecar",
        )
    sidecar_telemetry_endpoint = (
        sidecar_telemetry_handle.endpoint if sidecar_telemetry_handle is not None else None
    )
    try:
        with task_telemetry_phase(
            sidecar_telemetry_endpoint,
            "build_sidecar.active_selection",
        ):
            active_snapshot, sidecar_profile = _build_active_coordinate_snapshot(
                files,
                payload=selector_payload if selector_payload_configured else payload,
                expression_ir=(
                    selector_payload_expression_ir if selector_payload_configured else expression_ir
                ),
                project_root=config.project_root,
                partition_column=partition_column,
                group_keys=selector_group_keys if sort_first_enabled else [partition_column],
                selector_operation_id=selector_operation_id,
                sort=list(sort_first.get("sort") or []),
                rows_per_part=rows_per_part,
                memory_budget_mb=sidecar_phase_budget_mb,
                max_source_files_per_task=config.max_source_files_per_task,
                max_source_row_groups_per_task=config.max_source_row_groups_per_task,
                sidecar_workers=admitted_sidecar_workers,
                sidecar_worker_recycle_mode=config.sidecar_worker_recycle_mode,
                sidecar_max_source_files=config.sidecar_max_source_files,
                sidecar_max_projected_bytes_mb=sidecar_projected_limit_mb,
                candidate_target_bytes=config.sidecar_target_bytes_mb * 1024 * 1024,
                sort_first_enabled=sort_first_enabled,
                pivot=pivot,
                candidate_root=candidate_sidecar_root_for(
                    spec,
                    config=config,
                    operation_id=selector_operation_id if sort_first_enabled else None,
                ),
                candidate_manifest_path=candidate_manifest_path_for(
                    spec,
                    config=config,
                    operation_id=selector_operation_id if sort_first_enabled else None,
                ),
                logical_plan_hash=logical_plan_hash,
                reference_fingerprint=reference_fingerprint,
                previous_active_snapshot_path=active_snapshot_target,
                telemetry_endpoint=sidecar_telemetry_endpoint,
                calibrate_workers=sidecar_memory_history["observations"] == 0
                or sidecar_memory_history["recalibration_required"],
                previous_active_sidecar_decision=previous_active_sidecar_decision,
            )
    finally:
        if sidecar_telemetry_handle is not None:
            sidecar_telemetry_profile = sidecar_telemetry_handle.stop()
    sidecar_profile["phase_elapsed_sec"] = time.perf_counter() - selector_started
    sidecar_profile["memory_admission"] = sidecar_memory_admission
    sidecar_profile["memory_history"] = sidecar_memory_history
    sidecar_profile["effective_max_projected_bytes_mb"] = sidecar_projected_limit_mb
    phase_profile["active_row_selection_sec"] = sidecar_profile["phase_elapsed_sec"]
    active_plan = sidecar_profile.get("active_sidecar_plan")
    if isinstance(active_plan, dict):
        pivot_shape_profile = dict(active_plan["pivot_shape_profile"])
        active_snapshot_path = Path(str(active_plan["active_snapshot_path"]))
        plan_parent = Path(str(active_plan["manifest_path"])).parent
        coordinate_tasks = [
            TaskSpec(
                task_id=str(item["task_id"]),
                partition_value=str(item["partition_value"]),
                part_index=int(item["part_index"]),
                payload={
                    "coordinate_path": str(plan_parent / str(item["coordinate_path"])),
                    "rust_coordinate_path": str(plan_parent / str(item["rust_coordinate_path"])),
                    "sidecar_spill_required": bool(item.get("spill_required", False)),
                },
            )
            for item in active_plan["coordinate_tasks"]
        ]
        phase_profile["pivot_shape_profile_sec"] = 0.0
        phase_profile["coordinate_snapshot_write_sec"] = float(
            active_plan.get("worker", {}).get("elapsed_sec") or 0.0
        )
    else:
        pivot_shape_started = time.perf_counter()
        pivot_shape_profile = build_pivot_shape_profile(active_snapshot, pivot)
        phase_profile["pivot_shape_profile_sec"] = time.perf_counter() - pivot_shape_started
        active_snapshot_path, coordinate_tasks = _write_active_coordinate_snapshot(
            active_snapshot,
            output_path=active_snapshot_target,
            partition_column=partition_column,
        )
        reset_path(
            active_snapshot_target.with_name(
                f"{active_snapshot_target.stem}.active-sidecar-plan.json"
            )
        )
        sidecar_profile.setdefault("parent_memory_boundaries", []).append(
            _parent_memory_boundary(
                "coordinates_written",
                active_snapshot_estimated_mb=active_snapshot.estimated_size() / (1024 * 1024),
            )
        )
        phase_profile["coordinate_snapshot_write_sec"] = (
            time.perf_counter() - selector_started - phase_profile["active_row_selection_sec"]
        )
    page_range_plans: dict[str, dict[str, Any]] = {}
    if probe_profile is not None:
        probe_manifest_path = Path(probe_profile["manifest_path"])
        probe_manifest = json.loads(probe_manifest_path.read_text(encoding="utf-8"))
        access_profile_path = probe_manifest_path.parent / str(
            probe_manifest["artifacts"]["access_profile"]
        )
        access_profile = json.loads(access_profile_path.read_text(encoding="utf-8"))
        projected_columns = list(access_profile.get("required_columns") or [])
        page_range_plans = {
            task.task_id: plan_coordinate_page_ranges(
                probe_manifest_path,
                str(task.payload["coordinate_path"]),
                project_root=config.project_root,
                projected_columns=projected_columns,
                merge_gap_bytes=config.range_merge_gap_bytes,
                max_range_bytes=config.max_range_bytes,
                max_ranges=config.max_ranges_per_task,
                minimum_range_savings_ratio=config.minimum_range_savings_ratio,
            )
            for task in coordinate_tasks
        }
    task_payload = {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "payload_engine": PAYLOAD_ENGINE,
        "task_fingerprint": task_fingerprint,
        "source_fingerprint": source_fingerprint,
        "logical_plan_hash": logical_plan_hash,
        "payload": payload,
        "expression_ir": expression_ir,
        "list_restore": list_restore,
        "pivot": pivot,
        "partition_column": partition_column,
        "compression": str(output.get("compression") or "zstd"),
        "output_dir": str(output_dir),
        "rows_per_part": rows_per_part,
        "project_root": str(config.project_root),
        "source_stats": _build_source_snapshot_stats(files),
        "memory_budget_mb": materialize_phase_budget_mb,
        "spill_recovery_root": str(
            config.temp_root / "spill-recovery" / partition_dir_name(spec.job_name)
        ),
        "ordered_operations": list(
            (raw.get("__pipeline") or {}).get("rust_operation_trace")
            or (raw.get("__pipeline") or {}).get("operation_trace")
            or []
        ),
        "writer_output_columns_hint": list(
            (raw.get("__pipeline") or {}).get("writer_output_columns") or []
        ),
        "probe_manifest_path": probe_profile["manifest_path"] if probe_profile else None,
        "probe_dataset_fingerprint": (
            probe_profile["dataset_fingerprint"] if probe_profile else None
        ),
    }
    tasks = [
        TaskSpec(
            task_id=task.task_id,
            partition_value=task.partition_value,
            part_index=task.part_index,
            payload={
                **task_payload,
                **task.payload,
                "page_range_plan": page_range_plans.get(
                    task.task_id,
                    {"read_path": "row_group_selected", "reason": "probe_unavailable"},
                ),
                "__telemetry_phase_name": "materialize.fused",
            },
        )
        for task in coordinate_tasks
    ]
    planning_started = time.perf_counter()
    physical_plan = _build_0201_physical_plan(
        active_snapshot,
        tasks=tasks,
        files=files,
        logical_plan_hash=logical_plan_hash,
        partition_column=partition_column,
        rows_per_part=rows_per_part,
        memory_budget_mb=materialize_phase_budget_mb,
        max_source_files_per_task=config.max_source_files_per_task,
        max_source_row_groups_per_task=config.max_source_row_groups_per_task,
        output_row_multiplier=_post_operation_row_multiplier(
            list(payload.get("post_operations") or [])
        ),
    )
    physical_candidates = build_logical_physical_candidates(
        physical_plan,
        logical_plan_hashes=(
            [logical_plan_hash]
            if raw.get("__pipeline")
            else [plan.plan_hash for plan in optimization.candidate_plans]
        ),
    )
    physical_plan, physical_candidate_trace = choose_physical_plan(
        physical_candidates,
        memory_budget_bytes=materialize_phase_budget_mb * 1024 * 1024,
        target_rows_per_part=rows_per_part,
    )
    physical_by_id = {task.task_id: task for task in physical_plan.tasks}
    task_row_group_recommendations = {
        task.task_id: choose_output_row_group_rows(
            physical_by_id[task.task_id],
            minimum_rows=(1 if bool((task.payload.get("pivot") or {}).get("enabled")) else 1_000),
        )
        for task in tasks
    }
    output_physical_layout, task_row_group_rows = resolve_output_physical_layout(
        asset_code=asset_code,
        policy=output.get("physical_layout"),
        compression=str(output.get("compression") or "zstd"),
        configured_row_group_rows=resolve_configured_row_group_rows(
            output.get("physical_layout"), fallback=config.output_row_group_rows
        ),
        task_row_group_recommendations=task_row_group_recommendations,
        previous_metadata=previous_metadata,
    )
    tasks = [
        TaskSpec(
            task_id=task.task_id,
            partition_value=task.partition_value,
            part_index=task.part_index,
            payload=(
                lambda base_payload: {
                    **base_payload,
                    "writer_input_contract": _resolve_writer_input_contract(
                        payload={
                            **base_payload,
                            "writer_input_contract": _writer_input_contract(
                                physical_by_id[task.task_id],
                                partition_columns=(
                                    [] if single_file_output else [partition_column]
                                ),
                                output_columns=list(
                                    base_payload.get("writer_output_columns_hint") or []
                                ),
                            ),
                        },
                        pivot=base_payload.get("pivot") or {},
                    ),
                    "output_row_group_rows": task_row_group_rows[task.task_id],
                    "output_physical_layout_profile_hash": output_physical_layout["profile_hash"],
                    "spill_required": (physical_by_id[task.task_id].risk == "spill_required"),
                    "spill_merge_aggregation": _pivot_spill_aggregation(
                        base_payload.get("pivot") or {},
                        payload=base_payload.get("payload") or {},
                    ),
                    "spill_chunk_rows": _spill_chunk_rows(
                        physical_by_id[task.task_id],
                        memory_budget_bytes=materialize_phase_budget_mb * 1024 * 1024,
                    ),
                    "spill_estimated_bytes": int(
                        physical_by_id[task.task_id].state_estimate_bytes or 0
                    ),
                }
            )(dict(task.payload)),
        )
        for task in tasks
    ]
    global_tasks = tasks
    tasks, test_run_profile = select_final_tasks(
        global_tasks,
        limit=test_run_limit,
        task_id=lambda task: task.task_id,
    )
    part_fingerprints = {
        task.task_id: _coordinate_task_fingerprint(
            task,
            logical_plan_hash=logical_plan_hash,
            reference_fingerprint=reference_fingerprint,
            source_stats=task_payload["source_stats"],
        )
        for task in tasks
    }
    previous_part_fingerprints = _previous_part_fingerprints(spec, config=config)
    _validate_probe_snapshot_barrier(
        probe_profile=probe_profile,
        source_paths=source_paths,
        recursive=source_recursive,
        config=config,
        phase="before_materialize",
    )
    transaction = DatasetTransaction.create(
        output_dir,
        manifest_context={
            "preset": spec.preset,
            "job_name": spec.job_name,
            "logical_plan_hash": logical_plan_hash,
            "physical_plan_hash": physical_plan.plan_hash,
            "physical_plan_cost": estimate_plan_cost(
                physical_plan,
                memory_budget_bytes=materialize_phase_budget_mb * 1024 * 1024,
                target_rows_per_part=rows_per_part,
            ),
            "physical_plan_candidates": physical_candidate_trace,
            "writer_input_contracts": {
                task.task_id: task.payload.get("writer_input_contract") for task in tasks
            },
            "naming_policy_version": NAMING_POLICY_VERSION,
            "change_reason": "semantic_or_dependency_change",
            "test_run": test_run_profile,
            "adaptive_task_boundary_decision": adaptive_task_boundary_decision,
            "output_physical_layout": output_physical_layout,
            "artifact_format": str(output.get("format") or "parquet"),
        },
    )
    if test_run_limit is not None or single_file_output:
        staged_results, dirty_tasks = [], tasks
    else:
        staged_results, dirty_tasks = _stage_reusable_coordinate_tasks(
            tasks,
            current_fingerprints=part_fingerprints,
            previous_fingerprints=previous_part_fingerprints,
            output_dir=output_dir,
            staging_root=transaction.staging_root,
        )
    dirty_tasks = [
        TaskSpec(
            task_id=task.task_id,
            partition_value=task.partition_value,
            part_index=task.part_index,
            payload={
                **task.payload,
                "physical_plan_hash": physical_plan.plan_hash,
                "output_dir": str(transaction.staging_root),
            },
        )
        for task in dirty_tasks
    ]
    phase_profile["task_planning_and_staging_sec"] = time.perf_counter() - planning_started
    admitted_workers = admitted_worker_count(
        physical_plan,
        requested_workers=config.workers,
        memory_budget_bytes=materialize_phase_budget_mb * 1024 * 1024,
    )
    materialize_memory_admission = build_phase_memory_admission(
        phase="materialize",
        hard_limit_mb=config.memory_budget_mb,
        safety_ratio=config.memory_safety_ratio,
        policy=materialize_memory_policy,
        requested_workers=admitted_workers,
        historical_worker_peak_p95_mb=materialize_memory_history["peak_rss_p95_mb"],
        fallback_worker_peak_mb=max(
            64.0,
            max(
                (
                    float(item.state_estimate_bytes or item.expected_payload_bytes or 0)
                    / (1024 * 1024)
                    for item in physical_plan.tasks
                ),
                default=64.0,
            ),
        ),
    )
    admitted_workers = int(materialize_memory_admission["admitted_workers"])
    dirty_task_ids = {task.task_id for task in dirty_tasks}
    dirty_physical_plan = {
        "tasks": [
            item
            for item in physical_plan.to_dict().get("tasks") or []
            if str(item.get("task_id") or "") in dirty_task_ids
        ]
    }
    calibration_preflight = build_materialize_calibration_plan(
        dirty_physical_plan,
        pivot_shape_profile,
        dict(learned_registry_details.get("pivot_sizing_reconciliation") or {}),
        memory_budget_mb=materialize_phase_budget_mb,
        admitted_workers=admitted_workers,
        calibration_target_count=1 if learned_registry_model else None,
    )
    calibration_task_ids = {
        str(item.get("task_id") or "")
        for item in (calibration_preflight.get("calibration") or {}).get("selected_tasks", [])
    }
    calibration_tasks = [task for task in dirty_tasks if task.task_id in calibration_task_ids]
    remaining_tasks = [task for task in dirty_tasks if task.task_id not in calibration_task_ids]
    if isinstance(active_plan, dict):
        active_snapshot_stats = dict(active_plan["active_snapshot_stats"])
        window_profile = dict(active_plan["window_planner"])
    else:
        active_snapshot_stats = {
            "rows": active_snapshot.height,
            "partitions": active_snapshot.get_column(partition_column).n_unique(),
            "source_files": active_snapshot.get_column(SOURCE_FILE_COLUMN).n_unique(),
            "source_row_groups": active_snapshot.select(
                [SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN]
            ).n_unique(),
            "estimated_size_bytes": active_snapshot.estimated_size(),
        }
        window_profile = _window_planner_profile(active_snapshot, expression_ir)
        del active_snapshot
    gc.collect()
    sidecar_profile.setdefault("parent_memory_boundaries", []).append(
        _parent_memory_boundary("before_materialize")
    )
    if worker_console_enabled():
        print(
            (
                f"[0201] active_row_selection done job={spec.job_name} "
                f"active_rows={active_snapshot_stats['rows']} "
                f"partitions={active_snapshot_stats['partitions']} "
                f"tasks={len(tasks)} dirty_tasks={len(dirty_tasks)} "
                f"reused_tasks={len(staged_results)} "
                f"selector_elapsed={phase_profile['active_row_selection_sec']:.3f}s "
                f"planning_elapsed={phase_profile['task_planning_and_staging_sec']:.3f}s"
            ),
            flush=True,
        )
    shared_telemetry_handle = None
    try:
        materialize_started = time.perf_counter()
        telemetry_log_path = None
        if not telemetry_disabled_for_benchmark:
            telemetry_log_path = (
                config.log_root
                / "task-telemetry"
                / f"0201_{partition_dir_name(spec.job_name)}_{time.time_ns()}.jsonl"
            )
            shared_telemetry_handle = start_task_telemetry_supervisor(
                log_path=telemetry_log_path,
                progress_title=f"smoking-data {asset_code} · {spec.job_name} · materialize",
            )
        shared_telemetry_endpoint = (
            shared_telemetry_handle.endpoint if shared_telemetry_handle is not None else None
        )
        calibration_results: list[TaskResult] = []
        runner_profiles: list[dict[str, Any]] = []
        if calibration_tasks:
            calibration_results, calibration_runner_profile = run_tasks_in_subprocesses(
                calibration_tasks,
                worker=indexed_curated_task_worker,
                workers=min(admitted_workers, len(calibration_tasks)),
                max_tasks_per_child=config.max_tasks_per_child,
                return_profile=True,
                telemetry_endpoint=shared_telemetry_endpoint,
            )
            runner_profiles.append({**calibration_runner_profile, "phase": "calibration"})
            _raise_failed_curated_tasks(calibration_results, spec=spec, config=config)

        calibration_memory_model = build_task_peak_memory_model(calibration_results)
        calibration_output_paths = [
            path for result in calibration_results for path in result.output_paths
        ]
        calibration_sizing_reconciliation = build_pivot_sizing_reconciliation(
            pivot_shape_profile,
            calibration_output_paths,
            calibration_results,
        )
        calibration_writer_decision = build_adaptive_sizing_shadow_decision(
            pivot_shape_profile,
            calibration_sizing_reconciliation,
            rows_per_part=rows_per_part,
            output_row_group_rows_override=resolve_configured_row_group_rows(
                output.get("physical_layout"), fallback=config.output_row_group_rows
            ),
            memory_budget_mb=materialize_phase_budget_mb,
            admitted_workers=admitted_workers,
            history_compression_ratio=history_compression_ratio,
        )
        recommended_row_group_rows = None
        if (
            resolve_configured_row_group_rows(
                output.get("physical_layout"), fallback=config.output_row_group_rows
            )
            is None
        ):
            recommended_row_group_rows = (
                calibration_writer_decision.get("recommendation") or {}
            ).get("bounded_output_row_group_rows")
        if (
            recommended_row_group_rows is not None
            and output_physical_layout["adaptation_scope"] == TASK_ADAPTIVE
        ):
            remaining_tasks = [
                replace(
                    task,
                    payload={
                        **task.payload,
                        "output_row_group_rows": int(recommended_row_group_rows),
                    },
                )
                for task in remaining_tasks
            ]
            for task in remaining_tasks:
                part_fingerprints[task.task_id] = _coordinate_task_fingerprint(
                    task,
                    logical_plan_hash=logical_plan_hash,
                    reference_fingerprint=reference_fingerprint,
                    source_stats=task_payload["source_stats"],
                )

        calibrated_worker_admission = build_calibrated_worker_admission(
            calibration_results,
            memory_budget_mb=materialize_phase_budget_mb,
            initial_admitted_workers=admitted_workers,
            remaining_tasks=len(remaining_tasks),
        )
        remaining_workers = int(
            calibrated_worker_admission.get("admitted_workers") or admitted_workers
        )
        calibrated_complete_group_guard = build_calibrated_complete_group_guard(
            pivot_shape_profile,
            calibration_memory_model,
            memory_budget_mb=materialize_phase_budget_mb,
            admitted_workers=remaining_workers,
        )
        if remaining_tasks and calibrated_complete_group_guard.get("status") == "over_budget":
            raise ValidationError(
                "A complete pivot row-key group exceeds the calibrated worker peak budget.",
                code="physical_plan.calibrated_oversized_group",
                context=calibrated_complete_group_guard,
            )
        remaining_results: list[TaskResult] = []
        if remaining_tasks:
            remaining_results, remaining_runner_profile = run_tasks_in_subprocesses(
                remaining_tasks,
                worker=indexed_curated_task_worker,
                workers=remaining_workers,
                max_tasks_per_child=config.max_tasks_per_child,
                return_profile=True,
                telemetry_endpoint=shared_telemetry_endpoint,
            )
            runner_profiles.append({**remaining_runner_profile, "phase": "remaining"})
            _raise_failed_curated_tasks(remaining_results, spec=spec, config=config)

        task_results = [*calibration_results, *remaining_results]
        subprocess_runner_profile = _merge_subprocess_runner_profiles(runner_profiles)
        effective_dirty_tasks = {
            task.task_id: task for task in [*calibration_tasks, *remaining_tasks]
        }
        applied_output_row_group_rows = {
            task.task_id: int(task.payload["output_row_group_rows"]) for task in tasks
        }
        applied_output_row_group_rows.update(
            {
                task_id: int(task.payload["output_row_group_rows"])
                for task_id, task in effective_dirty_tasks.items()
            }
        )
        output_physical_layout["task_output_row_group_rows"] = dict(applied_output_row_group_rows)
        adaptive_materialize_execution = {
            "schema_version": "smoking-data.adaptive-materialize-execution.v1",
            "status": ("applied" if calibration_tasks else "not_applicable"),
            "mode": "calibrate_then_execute",
            "calibration_task_ids": [task.task_id for task in calibration_tasks],
            "calibration_selection": [
                item
                for item in (calibration_preflight.get("calibration") or {}).get(
                    "selected_tasks", []
                )
                if str(item.get("task_id") or "") in calibration_task_ids
            ],
            "calibration_tasks": len(calibration_results),
            "calibration_outputs_reused_from_staging": bool(calibration_results),
            "remaining_task_ids": [task.task_id for task in remaining_tasks],
            "remaining_tasks": len(remaining_tasks),
            "replan_count": 1 if calibration_results and remaining_tasks else 0,
            "replan_scope": "not_started_tasks_only",
            "task_boundary_change": bool(
                adaptive_task_boundary_decision.get("task_boundary_changed", False)
            ),
            "task_boundary_reason": adaptive_task_boundary_decision.get("reason"),
            "adaptive_task_boundary_decision": adaptive_task_boundary_decision,
            "initial_admitted_workers": admitted_workers,
            "calibrated_worker_admission": calibrated_worker_admission,
            "calibrated_complete_group_guard": calibrated_complete_group_guard,
            "calibration_memory_model": calibration_memory_model,
            "calibration_writer_decision": calibration_writer_decision,
            "applied_output_row_group_rows": applied_output_row_group_rows,
            "manual_output_row_group_override": resolve_configured_row_group_rows(
                output.get("physical_layout"), fallback=config.output_row_group_rows
            ),
        }
        if transaction.manifest_context is not None:
            transaction.manifest_context["adaptive_materialize_execution"] = (
                adaptive_materialize_execution
            )
            transaction.manifest_context["output_physical_layout"] = output_physical_layout
        phase_profile["materialize_tasks_sec"] = time.perf_counter() - materialize_started
        task_results = [*staged_results, *task_results]
        if single_file_output:
            snapshot_started = time.perf_counter()
            snapshot_profile = _compact_staged_snapshot_to_single_file(
                transaction.staging_root,
                compression=str(output.get("compression") or "zstd"),
                row_group_rows=min(applied_output_row_group_rows.values(), default=1_000),
            )
            phase_profile["single_snapshot_compaction_sec"] = time.perf_counter() - snapshot_started
            if transaction.manifest_context is not None:
                transaction.manifest_context["single_file_output"] = snapshot_profile
        assertion_started = time.perf_counter()
        assertion_profile = _validate_staged_dataset_assertions(
            transaction,
            payload=payload,
            spec=spec,
            config=config,
        )
        phase_profile["dataset_assertion_sec"] = time.perf_counter() - assertion_started
        if single_file_output and str(output.get("format") or "parquet") == "sbdf":
            sbdf_started = time.perf_counter()
            sbdf_profile = _convert_staged_snapshot_to_sbdf(
                transaction,
                config=dict(output.get("sbdf") or {}),
            )
            phase_profile["sbdf_artifact_sec"] = time.perf_counter() - sbdf_started
            if transaction.manifest_context is not None:
                transaction.manifest_context["sbdf_artifact"] = sbdf_profile
        _validate_probe_snapshot_barrier(
            probe_profile=probe_profile,
            source_paths=source_paths,
            recursive=source_recursive,
            config=config,
            phase="before_commit",
        )
        commit_started = time.perf_counter()
        emit_task_telemetry_event(
            shared_telemetry_endpoint,
            "phase_planned",
            task_id=None,
            details={"phase_name": "save_dataset.commit", "total": 1, "unit": "generation"},
        )
        with task_telemetry_phase(
            shared_telemetry_endpoint,
            "save_dataset.commit",
        ):
            output_files, transaction_profile = transaction.commit()
        phase_profile["transaction_commit_sec"] = time.perf_counter() - commit_started
        task_telemetry_profile = (
            shared_telemetry_handle.stop() if shared_telemetry_handle is not None else None
        )
        shared_telemetry_handle = None
        if telemetry_disabled_for_benchmark:
            task_telemetry_profile = {
                "schema_version": "smoking-data.task-telemetry.v1",
                "status": "disabled_for_benchmark",
                "reason": INTERNAL_DISABLE_TASK_TELEMETRY_ENV,
            }
    except BaseException:
        if shared_telemetry_handle is not None:
            shared_telemetry_handle.stop()
        transaction.abort()
        raise
    sizing_reconciliation_started = time.perf_counter()
    physical_plan_actuals = reconcile_task_memory(physical_plan, task_results)
    pivot_sizing_reconciliation = build_pivot_sizing_reconciliation(
        pivot_shape_profile,
        output_files,
        task_results,
    )
    adaptive_sizing_decision = build_adaptive_sizing_shadow_decision(
        pivot_shape_profile,
        pivot_sizing_reconciliation,
        rows_per_part=rows_per_part,
        output_row_group_rows_override=resolve_configured_row_group_rows(
            output.get("physical_layout"), fallback=config.output_row_group_rows
        ),
        memory_budget_mb=materialize_phase_budget_mb,
        admitted_workers=admitted_workers,
        history_compression_ratio=history_compression_ratio,
    )
    materialize_calibration_plan = build_materialize_calibration_plan(
        physical_plan.to_dict(),
        pivot_shape_profile,
        pivot_sizing_reconciliation,
        memory_budget_mb=materialize_phase_budget_mb,
        admitted_workers=admitted_workers,
        calibration_target_count=1 if learned_registry_model else None,
    )
    if adaptive_materialize_execution.get("status") == "applied":
        materialize_calibration_plan = {
            **materialize_calibration_plan,
            "mode": "calibrate_then_execute",
            "applied": True,
            "planning_scope": "current_run",
            "calibration": {
                **dict(materialize_calibration_plan.get("calibration") or {}),
                "selected_tasks": list(
                    adaptive_materialize_execution.get("calibration_selection") or []
                ),
                "output_reuse_policy": "staging_reuse",
                "outputs_reused_this_run": True,
            },
            "recommendation": {
                **dict(materialize_calibration_plan.get("recommendation") or {}),
                "plan_mutation_status": "partially_applied",
                "task_boundary_mutation_status": (
                    "applied_from_history"
                    if adaptive_task_boundary_decision.get("applied")
                    else "retained_configured_boundary"
                ),
            },
            "limitations": [
                "The same-run fit contains only observed input-row sizes and is not history yet.",
                "First-run row recommendations do not exceed twice the largest observed task.",
                (
                    "Task boundaries use validated previous-run history and never mutate after "
                    "the current sidecar is built."
                    if adaptive_task_boundary_decision.get("applied")
                    else "Task boundaries retain the configured seed until reliable matching "
                    "history is available."
                ),
            ],
            "execution": adaptive_materialize_execution,
        }
    phase_profile["pivot_sizing_reconciliation_sec"] = (
        time.perf_counter() - sizing_reconciliation_started
    )
    phase_profile["total_elapsed_sec"] = time.perf_counter() - run_started
    if worker_console_enabled():
        print(
            (
                f"[{asset_code}] materialize done job={spec.job_name} "
                f"dirty_tasks={len(dirty_tasks)} "
                f"materialize_elapsed={phase_profile.get('materialize_tasks_sec', 0.0):.3f}s "
                f"assertion_elapsed={phase_profile.get('dataset_assertion_sec', 0.0):.3f}s "
                f"commit_elapsed={phase_profile.get('transaction_commit_sec', 0.0):.3f}s "
                f"total_elapsed={phase_profile['total_elapsed_sec']:.3f}s"
            ),
            flush=True,
        )
    task_results = _remap_transaction_task_outputs(
        task_results,
        staging_root=transaction.staging_root,
        final_root=output_dir,
        single_output_path=(output_files[0] if single_file_output and output_files else None),
    )
    task_phase_profile = _task_phase_profile(task_results)
    rust_phase_profile = _rust_task_phase_profile(task_results)
    task_telemetry_reconciliation = reconcile_task_telemetry(
        task_results,
        task_telemetry_profile,
    )
    if worker_console_enabled():
        print(
            (
                f"[{asset_code}] task profile job={spec.job_name} "
                f"tasks={task_phase_profile.get('tasks_profiled', 0)} "
                f"task_avg={task_phase_profile.get('task_elapsed_sec', {}).get('avg', 0.0):.3f}s "
                f"write_avg={task_phase_profile.get('write_elapsed_sec', {}).get('avg', 0.0):.3f}s "
                f"source_files_avg={task_phase_profile.get('coordinate_source_files', {}).get('avg', 0.0):.2f} "
                f"rust_restore_avg={rust_phase_profile.get('restore_sec', {}).get('avg', 0.0):.3f}s "
                f"rust_write_avg={rust_phase_profile.get('parquet_write_sec', {}).get('avg', 0.0):.3f}s"
            ),
            flush=True,
        )
    output_rows = int(sum(item.counters.get("output_rows", 0) for item in task_results))
    actual_output_columns = _actual_output_columns_by_task(task_results)
    explicit_boundary_runtime = _resolved_explicit_boundary_runtime(
        raw_pipeline=raw.get("__pipeline"),
        sidecar_profile=sidecar_profile,
        physical_plan=physical_plan,
        all_tasks=global_tasks,
        dirty_tasks=dirty_tasks,
        source_stats=task_payload["source_stats"],
    )

    metadata_path = metadata_path_for(spec, config=config)
    result = StageResult.success(
        preset=spec.preset,
        job_name=spec.job_name,
        yaml_path=spec.yaml_path,
        metadata_path=metadata_path,
        output_paths=output_files,
        counters={
            "input_files": len(files),
            "input_bytes": sum(item.size_bytes for item in files),
            "output_files": len(output_files),
            "output_rows": output_rows,
            "output_partitions": len({str(task.partition_value) for task in tasks}),
            "tasks": len(tasks),
            "global_planned_tasks": len(global_tasks),
            "dirty_tasks": len(dirty_tasks),
            "reused_tasks": len(staged_results),
            "rows_per_part": rows_per_part,
            "configured_rows_per_part": config.target_rows_per_part,
            "active_snapshot_rows": active_snapshot_stats["rows"],
            "coordinate_source_files": active_snapshot_stats["source_files"],
            "coordinate_row_groups": active_snapshot_stats["source_row_groups"],
        },
        details={
            "engine": engine_metadata(expression_ir_hash=expression_ir_hash),
            "source_fingerprint": source_fingerprint,
            "physical_probe": probe_profile,
            "test_run": test_run_profile,
            "page_range_plans": page_range_plans,
            "dependency_graph": {
                "upstream": {
                    "fingerprint": upstream_fingerprint,
                    "files": [str(item.path) for item in files],
                },
                "references": {
                    "fingerprint": reference_fingerprint,
                    "files": [str(item.path) for item in reference_files],
                },
            },
            "task_fingerprint": task_fingerprint,
            "logical_plan": logical_plan.to_dict(),
            "logical_plan_hash": logical_plan_hash,
            "pipeline_contract": {
                **dict(raw.get("__pipeline") or {}),
                "explicit_physical_boundaries": explicit_boundary_runtime,
            },
            "optimizer": optimization.to_dict(),
            "physical_plan": physical_plan.to_dict(),
            "physical_plan_hash": physical_plan.plan_hash,
            "naming_policy_version": NAMING_POLICY_VERSION,
            "physical_plan_cost": estimate_plan_cost(
                physical_plan,
                memory_budget_bytes=materialize_phase_budget_mb * 1024 * 1024,
                target_rows_per_part=rows_per_part,
            ),
            "physical_plan_candidates": physical_candidate_trace,
            "writer_input_contracts": {
                task.task_id: (
                    lambda resolved, actual: {
                        **resolved,
                        "output_columns": list(resolved.get("output_columns") or actual),
                        "actual_output_columns": actual,
                    }
                )(
                    _resolve_writer_input_contract(
                        payload=task.payload,
                        pivot=task.payload.get("pivot") or {},
                    ),
                    actual_output_columns.get(task.task_id, []),
                )
                for task in tasks
            },
            "physical_plan_actuals": physical_plan_actuals,
            "resource_admission": {
                "requested_workers": config.workers,
                "admitted_workers": admitted_workers,
                "calibrated_admitted_workers": (
                    adaptive_materialize_execution.get("calibrated_worker_admission") or {}
                ).get("admitted_workers"),
                "memory_budget_mb": config.memory_budget_mb,
                "memory_contract": {
                    "hard_limit_mb": config.memory_budget_mb,
                    "safety_ratio": config.memory_safety_ratio,
                    "build_sidecar": sidecar_memory_admission,
                    "materialize": materialize_memory_admission,
                    "save_dataset": {
                        "safe_envelope_mb": int(
                            config.memory_budget_mb * config.memory_safety_ratio
                        ),
                        "workers": 1,
                        "enforcement": "global_envelope_observation",
                    },
                },
            },
            "output_dir": str(output_dir),
            "dataset_transaction": transaction_profile,
            "data_assertion": assertion_profile,
            "transaction_recovery": recovery_profile,
            "phase_profile": phase_profile,
            "active_snapshot_path": active_snapshot_path,
            "candidate_sidecar": sidecar_profile,
            "pivot_shape_profile": pivot_shape_profile,
            "adaptive_task_boundary_decision": adaptive_task_boundary_decision,
            "adaptive_sizing_registry": {
                "schema_version": "smoking-data.adaptive-sizing-registry.v1",
                "model_key": adaptive_sizing_key["model_key"],
                "canonical": adaptive_sizing_key["canonical"],
                "lookup_status": "hit" if registry_model is not None else "miss",
                "loaded_observation_count": (
                    registry_model.get("observation_count") if registry_model else 0
                ),
                "learned_calibration_target_count": (1 if learned_registry_model else None),
            },
            "pivot_sizing_reconciliation": pivot_sizing_reconciliation,
            "adaptive_sizing_decision": adaptive_sizing_decision,
            "materialize_calibration_plan": materialize_calibration_plan,
            "adaptive_materialize_execution": adaptive_materialize_execution,
            "output_physical_layout": output_physical_layout,
            "subprocess_runner_profile": subprocess_runner_profile,
            "task_telemetry": task_telemetry_profile,
            "phase_telemetry": _merge_phase_telemetry_profiles(
                sidecar_telemetry_profile,
                task_telemetry_profile,
                admission_limits_mb={
                    "build_sidecar.candidate": sidecar_phase_budget_mb,
                    "build_sidecar.active_selection": sidecar_phase_budget_mb,
                    "build_sidecar.bucketize": sidecar_phase_budget_mb,
                    "build_sidecar.selector_bucket": sidecar_phase_budget_mb,
                    "materialize.fused": materialize_phase_budget_mb,
                    "save_dataset.commit": int(
                        config.memory_budget_mb * config.memory_safety_ratio
                    ),
                },
                hard_limit_mb=config.memory_budget_mb,
            ),
            "task_telemetry_reconciliation": task_telemetry_reconciliation,
            "part_fingerprints": part_fingerprints,
            "task_results": task_results,
            "task_memory": _task_memory_profile(task_results),
            "task_phase_profile": task_phase_profile,
            "rust_task_phase_profile": rust_phase_profile,
            "window_planner": window_profile,
        },
    )
    registry_record = record_adaptive_sizing_model(
        project_root=config.project_root,
        key=adaptive_sizing_key,
        alias=str((raw.get("materialize") or {}).get("alias") or spec.job_name),
        job_name=spec.job_name,
        details=result.details,
        counters=result.counters,
    )
    result.details["adaptive_sizing_registry"] = {
        **dict(result.details["adaptive_sizing_registry"]),
        "record": registry_record,
    }
    written = write_metadata(spec=spec, config=config, result=result.to_dict())
    result.metadata_path = written
    return result


def _resolved_explicit_boundary_runtime(
    *,
    raw_pipeline: Any,
    sidecar_profile: dict[str, Any],
    physical_plan,
    all_tasks: list[TaskSpec],
    dirty_tasks: list[TaskSpec],
    source_stats: dict[str, dict[str, int]],
) -> list[dict[str, Any]]:
    payload = dict(raw_pipeline or {})
    boundaries = deepcopy(list(payload.get("explicit_physical_boundaries") or []))
    if not boundaries:
        return []

    planned_all = _summarize_physical_plan_source_span(tuple(physical_plan.tasks))
    dirty_ids = {str(task.task_id) for task in dirty_tasks}
    planned_executed = _summarize_physical_plan_source_span(
        tuple(task for task in physical_plan.tasks if task.task_id in dirty_ids)
    )
    actual_executed = _summarize_coordinate_task_source_span(
        dirty_tasks,
        source_stats=source_stats,
    )

    resolved: list[dict[str, Any]] = []
    for boundary in boundaries:
        kind = str(boundary.get("kind") or "")
        item = dict(boundary)
        if kind == "build_sidecar":
            item["runtime"] = {
                "columns_mode": (
                    "auto"
                    if str((item.get("config") or {}).get("columns")) == "auto"
                    else "explicit"
                ),
                "resolved_columns": list(sidecar_profile.get("selector_columns") or []),
                "candidate_manifest_path": sidecar_profile.get("manifest_path"),
                "candidate_schema_hash": sidecar_profile.get("schema_hash"),
                "candidate_files": sidecar_profile.get("candidate_files"),
                "candidate_rows": sidecar_profile.get("candidate_rows"),
                "candidate_bytes": sidecar_profile.get("candidate_bytes"),
                "rebuilt_source_files": sidecar_profile.get("rebuilt_source_files"),
                "reused_source_files": sidecar_profile.get("reused_source_files"),
                "execution": deepcopy(sidecar_profile.get("execution") or {}),
            }
        elif kind == "materialize":
            item["runtime"] = {
                "planned_all_tasks": planned_all,
                "planned_executed_tasks": planned_executed,
                "actual_executed_tasks": actual_executed,
                "actual_matches_planned_executed": {
                    "file_count": actual_executed["file_count"] == planned_executed["file_count"],
                    "row_group_count": (
                        actual_executed["row_group_count"] == planned_executed["row_group_count"]
                    ),
                    "selected_rows": (
                        actual_executed["selected_rows"] == planned_executed["selected_rows"]
                    ),
                    "task_count": actual_executed["task_count"] == planned_executed["task_count"],
                },
            }
        resolved.append(item)
    return resolved


def _summarize_physical_plan_source_span(tasks: tuple[Any, ...]) -> dict[str, Any]:
    files: set[str] = set()
    row_groups: set[tuple[str, int]] = set()
    selected_rows = 0
    payload_bytes = 0
    partitions: set[str] = set()
    for task in tasks:
        partitions.add(str(task.partition_value))
        selected_rows += int(task.expected_input_rows or 0)
        payload_bytes += int(task.expected_payload_bytes or 0)
        for span in task.source_spans:
            files.add(str(span.path))
            row_groups.update((str(span.path), int(row_group)) for row_group in span.row_groups)
    return {
        "task_count": len(tasks),
        "partition_count": len(partitions),
        "file_count": len(files),
        "row_group_count": len(row_groups),
        "selected_rows": selected_rows,
        "expected_payload_bytes": payload_bytes,
    }


def _summarize_coordinate_task_source_span(
    tasks: list[TaskSpec],
    *,
    source_stats: dict[str, dict[str, int]],
) -> dict[str, Any]:
    files: set[str] = set()
    row_groups: set[tuple[str, int]] = set()
    partitions: set[str] = set()
    selected_rows = 0
    expected_payload_bytes = 0
    for task in tasks:
        partitions.add(str(task.partition_value))
        coordinates = pl.read_parquet(task.payload["coordinate_path"])
        selected_rows += coordinates.height
        unique_files = [
            str(value)
            for value in coordinates.get_column(SOURCE_FILE_COLUMN).unique(maintain_order=True)
        ]
        files.update(unique_files)
        row_groups.update(
            (
                str(row[SOURCE_FILE_COLUMN]),
                int(row[SOURCE_ROW_GROUP_COLUMN]),
            )
            for row in coordinates.select([SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN])
            .unique()
            .to_dicts()
        )
        for source_path in unique_files:
            stats = source_stats.get(source_path) or {}
            expected_payload_bytes += int(stats.get("size_bytes") or 0)
    return {
        "task_count": len(tasks),
        "partition_count": len(partitions),
        "file_count": len(files),
        "row_group_count": len(row_groups),
        "selected_rows": selected_rows,
        "expected_payload_bytes": expected_payload_bytes,
    }


def _raise_structured_assertion_failure(
    failure: TaskResult,
    *,
    spec: PresetSpec,
    config: RuntimeConfig,
) -> None:
    marker = "DATA_ASSERTION_FAILED "
    message = str(failure.error_message or "")
    if marker not in message:
        return
    raw = message.split(marker, 1)[1].splitlines()[0]
    try:
        detail = json.loads(raw)
    except json.JSONDecodeError:
        detail = {"raw_error": message}
    detail.update(
        {
            "task_id": failure.task_id,
            "partition_value": failure.partition_value,
            "part_index": failure.part_index,
        }
    )
    path = ensure_dir(artifact_root_for(spec, config=config) / "validation") / (
        f"{failure.task_id}.assertion.json"
    )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(detail, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, path)
    raise ValidationError(
        "Data-quality assertion failed before dataset commit.",
        code="data_assertion.failed",
        context={**detail, "artifact_path": str(path)},
    )


def _raise_source_snapshot_failure(failure: TaskResult) -> None:
    message = str(failure.error_message or "")
    if SOURCE_SNAPSHOT_CHANGED_MARKER not in message:
        return
    source_file = message.split(SOURCE_SNAPSHOT_CHANGED_MARKER, 1)[1].splitlines()[0]
    raise SmokingDataError(
        "Source dataset changed after Probe validation; 0201 output was not committed.",
        code="source_snapshot_changed",
        context={"task_id": failure.task_id, "source_file": source_file},
    )


def _validate_probe_snapshot_barrier(
    *,
    probe_profile: dict[str, Any] | None,
    source_paths: list[str],
    recursive: bool,
    config: RuntimeConfig,
    phase: str,
) -> None:
    if probe_profile is None:
        return
    current_files = discover_parquet_files(
        [resolve_project_path(path, project_root=config.project_root) for path in source_paths],
        recursive=recursive,
    )
    try:
        validate_probe_manifest(
            str(probe_profile["manifest_path"]),
            files=[item.path for item in current_files],
            project_root=config.project_root,
        )
    except (OSError, SmokingDataError) as error:
        raise SmokingDataError(
            "Source dataset changed after Probe validation; 0201 output was not committed.",
            code="source_snapshot_changed",
            context={
                "phase": phase,
                "probe_manifest_path": str(probe_profile["manifest_path"]),
                "cause": getattr(error, "code", type(error).__name__),
            },
        ) from error


def _build_source_snapshot_stats(files: list[Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in files:
        path = Path(item.path).resolve()
        try:
            before = path.stat()
            footer_fingerprint = parquet_footer_fingerprint(path)
            after = path.stat()
        except (OSError, SmokingDataError) as error:
            raise SmokingDataError(
                "Source dataset changed while the 0201 snapshot contract was being built.",
                code="source_snapshot_changed",
                context={"phase": "snapshot_contract", "source_file": str(path)},
            ) from error
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise SmokingDataError(
                "Source dataset changed while the 0201 snapshot contract was being built.",
                code="source_snapshot_changed",
                context={"phase": "snapshot_contract", "source_file": str(path)},
            )
        result[str(path)] = {
            "size_bytes": int(before.st_size),
            "modified_ns": int(before.st_mtime_ns),
            "footer_fingerprint": footer_fingerprint,
        }
    return result


def _validate_staged_dataset_assertions(
    transaction: DatasetTransaction,
    *,
    payload: dict[str, Any],
    spec: PresetSpec,
    config: RuntimeConfig,
) -> dict[str, Any]:
    assertion_configs = list(payload.get("dataset_assertions") or [])
    if not assertion_configs:
        return {"enabled": False, "rules_validated": 0}
    rules = [rule for item in assertion_configs for rule in item.get("rules") or []]
    sample_limit = max(
        [int(item.get("sample_limit") or 20) for item in assertion_configs],
        default=20,
    )
    paths = sorted(transaction.staging_root.rglob("*.parquet"))
    try:
        profile = validate_dataset_assertions(
            paths,
            assertion_config={"rules": rules, "sample_limit": sample_limit},
            spill_dir=transaction.staging_root / ".assertion-spill",
        )
    except Exception as error:
        _raise_dataset_assertion_failure(error, spec=spec, config=config)
        raise
    return {"enabled": True, **profile}


def _raise_dataset_assertion_failure(
    error: Exception,
    *,
    spec: PresetSpec,
    config: RuntimeConfig,
) -> None:
    marker = "DATA_ASSERTION_FAILED "
    message = str(error)
    if marker not in message:
        return
    raw = message.split(marker, 1)[1].splitlines()[0]
    try:
        detail = json.loads(raw)
    except json.JSONDecodeError:
        detail = {"raw_error": message}
    path = ensure_dir(artifact_root_for(spec, config=config) / "validation") / (
        f"{spec.job_name}.dataset-assertion.json"
    )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(detail, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)
    raise ValidationError(
        "Data-quality assertion failed before dataset commit.",
        code="data_assertion.failed",
        context={**detail, "artifact_path": str(path)},
    ) from error


def _build_0201_physical_plan(
    active_snapshot: pl.DataFrame | None,
    *,
    tasks: list[TaskSpec],
    files: list[Any],
    logical_plan_hash: str,
    partition_column: str,
    rows_per_part: int,
    memory_budget_mb: int,
    max_source_files_per_task: int,
    max_source_row_groups_per_task: int,
    output_row_multiplier: int,
):
    size_by_path = {str(item.path): int(item.size_bytes) for item in files}
    parquet_profiles = profile_parquet_files([item.path for item in files])
    physical_tasks: list[PhysicalTask] = []
    for task in tasks:
        subset = (
            pl.read_parquet(Path(str(task.payload["coordinate_path"])))
            if active_snapshot is None
            else active_snapshot.filter(
                (pl.col(partition_column).cast(pl.String) == str(task.partition_value))
                & (pl.col(PART_INDEX_COLUMN) == int(task.part_index or 0))
            )
        )
        spans: list[SourceSpan] = []
        for source_path in subset.get_column(SOURCE_FILE_COLUMN).unique().to_list():
            source_rows = subset.filter(pl.col(SOURCE_FILE_COLUMN) == source_path)
            row_indices = source_rows.get_column(SOURCE_ROW_INDEX_COLUMN)
            row_groups = tuple(
                sorted(
                    int(value) for value in source_rows.get_column(SOURCE_ROW_GROUP_COLUMN).unique()
                )
            )
            profile = parquet_profiles.get(str(Path(source_path).resolve()))
            spans.append(
                SourceSpan(
                    source_name="upstream",
                    path=str(source_path),
                    size_bytes=size_by_path.get(str(source_path), 0),
                    estimated_read_bytes=(
                        profile.estimated_compressed_bytes(row_group_ids=row_groups)
                        if profile is not None
                        else None
                    ),
                    estimated_uncompressed_bytes=(
                        profile.estimated_uncompressed_bytes(row_group_ids=row_groups)
                        if profile is not None
                        else None
                    ),
                    row_groups=row_groups,
                    selected_rows=source_rows.height,
                    row_index_min=int(row_indices.min()),
                    row_index_max=int(row_indices.max()),
                )
            )
        input_state_estimate = sum(
            span.estimated_uncompressed_bytes
            if span.estimated_uncompressed_bytes is not None
            else span.size_bytes
            for span in spans
        )
        state_estimate = input_state_estimate * max(1, output_row_multiplier)
        budget_bytes = memory_budget_mb * 1024 * 1024
        risk = "bounded"
        spill_required = bool(task.payload.get("sidecar_spill_required", False)) or (
            SPILL_REQUIRED_COLUMN in subset.columns
            and bool(subset.get_column(SPILL_REQUIRED_COLUMN).any())
        )
        if spill_required:
            risk = "spill_required"
        elif state_estimate > budget_bytes:
            risk = "over_budget"
        elif len(spans) > max_source_files_per_task:
            risk = "fanout_over_limit"
        elif sum(len(span.row_groups) for span in spans) > max_source_row_groups_per_task:
            risk = "row_group_fanout_over_limit"
        physical_tasks.append(
            PhysicalTask(
                task_id=task.task_id,
                partition_value=str(task.partition_value),
                batch_index=None,
                part_index=int(task.part_index or 0),
                single_partition_guaranteed=True,
                source_spans=tuple(spans),
                expected_input_rows=subset.height,
                expected_output_rows=subset.height * max(1, output_row_multiplier),
                expected_payload_bytes=sum(
                    span.estimated_read_bytes
                    if span.estimated_read_bytes is not None
                    else span.size_bytes
                    for span in spans
                ),
                file_fanout=len(spans),
                row_group_fanout=sum(len(span.row_groups) for span in spans),
                state_estimate_bytes=state_estimate,
                risk=risk,
            )
        )
    return build_physical_plan(
        logical_plan_hash=logical_plan_hash,
        tasks=physical_tasks,
        decisions=[
            {
                "decision": "coordinate_group_safe_parts",
                "target_rows_per_part": rows_per_part,
                "source_span_bytes_are_upper_bound": True,
                "memory_budget_mb": memory_budget_mb,
                "max_source_files_per_task": max_source_files_per_task,
                "max_source_row_groups_per_task": max_source_row_groups_per_task,
                "output_row_multiplier": output_row_multiplier,
            }
        ],
    )


def _post_operation_row_multiplier(operations: list[dict[str, Any]]) -> int:
    multiplier = 1
    for operation in operations:
        if operation.get("kind") == "unpivot":
            config = operation.get("config") or {}
            multiplier *= max(1, len(config.get("value_columns") or []))
        elif operation.get("kind") == "unnest":
            config = operation.get("config") or {}
            multiplier *= max(1, int(config.get("max_elements_per_row") or 1024))
    return multiplier


def _spill_chunk_rows(task: PhysicalTask, *, memory_budget_bytes: int) -> int:
    rows = max(1, int(task.expected_input_rows or 1))
    state_bytes = max(1, int(task.state_estimate_bytes or task.expected_payload_bytes or 1))
    if state_bytes <= memory_budget_bytes:
        return rows
    return max(1, math.floor(rows * memory_budget_bytes * 0.50 / state_bytes))


def _writer_input_contract(
    task: PhysicalTask,
    *,
    partition_columns: list[str],
    output_columns: list[str],
    writer_mode: str = "direct_append",
) -> dict[str, Any]:
    return {
        "contract_version": "smoking-data.writer-input.v1",
        "writer_mode": writer_mode,
        "partition_value": task.partition_value,
        "single_partition_guaranteed": bool(task.single_partition_guaranteed),
        "expected_source_files": int(task.file_fanout),
        "expected_row_groups": int(task.row_group_fanout),
        "expected_input_rows": task.expected_input_rows,
        "expected_output_rows": task.expected_output_rows,
        "expected_payload_bytes": int(task.expected_payload_bytes),
        "output_columns": list(output_columns),
        "partition_columns": list(partition_columns),
        "extras": {
            "task_id": task.task_id,
            "part_index": int(task.part_index),
        },
    }


def _resolve_writer_input_contract(
    *,
    payload: dict[str, Any],
    pivot: dict[str, Any],
) -> dict[str, Any]:
    contract = dict(payload.get("writer_input_contract") or {})
    if not contract:
        return {}
    payload_base = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    output_columns_hint = list(payload.get("writer_output_columns_hint") or [])
    include_columns = list(
        payload.get("include_columns") or payload_base.get("include_columns") or []
    )
    if output_columns_hint:
        contract["output_columns"] = output_columns_hint
        return contract
    pivot_enabled = bool(pivot.get("enabled", False))
    pivot_required_columns = list(
        dict.fromkeys(
            [
                *[str(item) for item in (pivot.get("row_keys") or [])],
                *[str(item) for item in (pivot.get("column_keys") or [])],
                *[
                    str(item.get("source_column") or "")
                    for item in [
                        *(pivot.get("value_keys") or []),
                        *(pivot.get("value_keys_without_column") or []),
                    ]
                    if isinstance(item, dict)
                ],
            ]
        )
    )
    output_columns = list(include_columns)
    if pivot_enabled:
        output_columns.extend(
            column for column in pivot_required_columns if column not in output_columns
        )
    if bool(payload.get("final_post_projection", payload_base.get("final_post_projection", False))):
        output_columns = []
    contract["output_columns"] = output_columns
    return contract


def _actual_output_columns_by_task(task_results: list[TaskResult]) -> dict[str, list[str]]:
    output_columns: dict[str, list[str]] = {}
    for result in task_results:
        if not result.ok or not result.output_paths:
            continue
        path = next((path for path in result.output_paths if Path(path).is_file()), None)
        if path is None:
            continue
        try:
            names = list(pq.ParquetFile(path).schema_arrow.names)
        except Exception:
            continue
        output_columns[str(result.task_id)] = names
    return output_columns


def _maybe_skip_unchanged(
    spec: PresetSpec,
    *,
    config: RuntimeConfig,
    task_fingerprint: str,
    asset_code: str,
    physical_layout_policy: dict[str, Any] | None,
    compression: str,
) -> StageResult | None:
    previous = read_metadata(spec, config=config)
    if not previous:
        return None
    previous_result = previous.get("result")
    if not isinstance(previous_result, dict):
        return None
    details = previous_result.get("details")
    if previous.get("yaml_hash") != spec.yaml_hash:
        return None
    if not isinstance(details, dict) or details.get("task_fingerprint") != task_fingerprint:
        return None
    if not previous_output_physical_layout_matches(
        previous,
        asset_code=asset_code,
        policy=physical_layout_policy,
        compression=compression,
        configured_row_group_rows=resolve_configured_row_group_rows(
            physical_layout_policy, fallback=config.output_row_group_rows
        ),
    ):
        return None
    output_paths = [Path(path) for path in previous_result.get("output_paths") or []]
    output_dir = Path(str(details.get("output_dir") or ""))
    output_exists = validate_committed_dataset(output_dir)
    if not output_exists:
        return None
    metadata_path = metadata_path_for(spec, config=config)
    result = StageResult.success(
        preset=spec.preset,
        job_name=spec.job_name,
        yaml_path=spec.yaml_path,
        metadata_path=metadata_path,
        output_paths=output_paths,
        counters={**previous_result.get("counters", {}), "skipped_unchanged": 1},
        details={
            **details,
            "skipped_reason": "yaml_and_task_fingerprint_unchanged",
        },
    )
    written = write_metadata(spec=spec, config=config, result=result.to_dict())
    result.metadata_path = written
    return result


def indexed_curated_task_worker(task: TaskSpec) -> TaskResult:
    payload = task.payload
    partition_value = str(task.partition_value)
    task_started = time.perf_counter()
    coordinates = pl.read_parquet(payload["coordinate_path"])
    coordinate_rows = coordinates.height
    coordinate_source_files = coordinates.get_column(SOURCE_FILE_COLUMN).n_unique()
    coordinate_row_groups = coordinates.select(
        [SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN]
    ).n_unique()
    for source_file in coordinates.get_column(SOURCE_FILE_COLUMN).unique(maintain_order=True):
        _validate_source_file_unchanged(Path(str(source_file)), payload.get("source_stats") or {})
    write_started = time.perf_counter()
    writer_payload = {
        **payload["payload"],
        "writer_input_contract": payload.get("writer_input_contract"),
        "source_projection_columns_hint": payload.get("writer_output_columns_hint") or [],
        "expression_ir": payload.get("expression_ir"),
        "ordered_operations": payload.get("ordered_operations") or [],
        "compression": payload.get("compression") or "zstd",
    }
    if bool(payload.get("spill_required", False)):
        output_files, output_rows, rust_stats = _write_curated_part_spill_fallback(
            coordinates,
            output_dir=Path(payload["output_dir"]),
            partition_value=partition_value,
            part_index=int(task.part_index or 0),
            task_id=task.task_id,
            payload=writer_payload,
            list_restore=payload.get("list_restore") or {},
            pivot=payload.get("pivot") or {},
            output_row_group_rows=int(payload["output_row_group_rows"]),
            project_root=Path(payload["project_root"]),
            chunk_rows=int(payload.get("spill_chunk_rows") or 1),
            memory_budget_mb=int(payload.get("memory_budget_mb") or 1),
            estimated_spill_bytes=int(payload.get("spill_estimated_bytes") or 0),
            recovery_root=Path(str(payload["spill_recovery_root"])),
            merge_aggregation=str(payload.get("spill_merge_aggregation") or ""),
        )
        execution_mode = "rust_coordinate_spill_merge"
    else:
        output_files, output_rows, rust_stats = _write_curated_part_rust_direct(
            coordinates,
            rust_coordinate_path=Path(payload["rust_coordinate_path"]),
            output_dir=Path(payload["output_dir"]),
            partition_value=partition_value,
            part_index=int(task.part_index or 0),
            payload=writer_payload,
            list_restore=payload.get("list_restore") or {},
            pivot=payload.get("pivot") or {},
            output_row_group_rows=int(payload["output_row_group_rows"]),
            project_root=Path(payload["project_root"]),
        )
        execution_mode = "rust_coordinate_direct"
    write_elapsed = time.perf_counter() - write_started
    total_elapsed = time.perf_counter() - task_started
    page_range_plan = dict(payload.get("page_range_plan") or {})
    return TaskResult(
        task_id=task.task_id,
        ok=True,
        pid=os.getpid(),
        partition_value=task.partition_value,
        part_index=task.part_index,
        output_paths=output_files,
        counters={
            "output_rows": output_rows,
            "output_files": len(output_files),
            "source_files_touched": coordinate_source_files,
            "row_groups_touched": coordinate_row_groups,
            "coordinate_rows": coordinate_rows,
            "coordinate_source_files": coordinate_source_files,
            "coordinate_row_groups": coordinate_row_groups,
            "task_elapsed_sec": total_elapsed,
            "write_elapsed_sec": write_elapsed,
            "payload_engine": PAYLOAD_ENGINE,
            "execution_mode": execution_mode,
            "read_path": page_range_plan.get("read_path", "row_group_selected"),
            "source_fingerprint": str(payload.get("source_fingerprint") or ""),
            "probe_dataset_fingerprint": str(payload.get("probe_dataset_fingerprint") or ""),
            "planned_pages_touched": int(page_range_plan.get("page_count") or 0),
            "planned_ranges_read": int(page_range_plan.get("range_count") or 0),
            "planned_range_bytes": int(page_range_plan.get("range_bytes") or 0),
            "planned_row_group_bytes": int(page_range_plan.get("row_group_bytes") or 0),
            "actual_source_bytes_read": int(rust_stats.get("source_bytes_read") or 0),
            "actual_row_groups_read": coordinate_row_groups,
            **{f"rust_{name}": value for name, value in rust_stats.items()},
        },
    )


def _raise_failed_curated_tasks(
    task_results: list[TaskResult],
    *,
    spec: PresetSpec,
    config: RuntimeConfig,
) -> None:
    failed = [item for item in task_results if not item.ok]
    if not failed:
        return
    failure = failed[0]
    _raise_source_snapshot_failure(failure)
    _raise_structured_assertion_failure(failure, spec=spec, config=config)
    raise TaskExecutionError(
        f"0201 indexed curated task failed: {failure.error_message}",
        context={
            "task_id": failure.task_id,
            "partition_value": failure.partition_value,
            "part_index": failure.part_index,
            "error_type": failure.error_type,
            "traceback_tail": failure.traceback_tail,
        },
    )


def _merge_subprocess_runner_profiles(
    profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    if not profiles:
        return {
            "task_count": 0,
            "requested_workers": 0,
            "admitted_workers": 0,
            "submission_mode": "adaptive_phased",
            "submitted_futures": 0,
            "generation_count": 0,
            "generation_profiles": [],
            "submission_sec": 0.0,
            "wait_sec": 0.0,
            "total_elapsed_sec": 0.0,
            "phases": [],
        }
    if len(profiles) == 1:
        return {**profiles[0], "phases": profiles}
    return {
        "task_count": sum(int(item.get("task_count") or 0) for item in profiles),
        "requested_workers": max(
            (int(item.get("requested_workers") or 0) for item in profiles), default=0
        ),
        "admitted_workers": max(
            (int(item.get("admitted_workers") or 0) for item in profiles), default=0
        ),
        "max_tasks_per_child": profiles[0].get("max_tasks_per_child"),
        "submission_mode": "adaptive_phased",
        "submitted_futures": sum(int(item.get("submitted_futures") or 0) for item in profiles),
        "generation_count": sum(int(item.get("generation_count") or 0) for item in profiles),
        "generation_profiles": [
            {**generation, "phase": item.get("phase")}
            for item in profiles
            for generation in item.get("generation_profiles") or []
        ],
        "submission_sec": sum(float(item.get("submission_sec") or 0.0) for item in profiles),
        "wait_sec": sum(float(item.get("wait_sec") or 0.0) for item in profiles),
        "total_elapsed_sec": sum(float(item.get("total_elapsed_sec") or 0.0) for item in profiles),
        "phases": profiles,
    }


def _remap_transaction_task_outputs(
    task_results: list[TaskResult],
    *,
    staging_root: Path,
    final_root: Path,
    single_output_path: Path | None = None,
) -> list[TaskResult]:
    remapped: list[TaskResult] = []
    for result in task_results:
        output_paths = (
            [single_output_path]
            if single_output_path is not None
            else [final_root / path.relative_to(staging_root) for path in result.output_paths]
        )
        remapped.append(replace(result, output_paths=output_paths))
    return remapped


def _compact_staged_snapshot_to_single_file(
    staging_root: Path,
    *,
    compression: str,
    row_group_rows: int,
) -> dict[str, Any]:
    """Stream bounded task parts into one schema-unified 0401 snapshot file."""

    source_paths = [path for path in sorted(staging_root.rglob("*.parquet")) if path.is_file()]
    if not source_paths:
        raise RuntimeError("0401 snapshot transaction contains no Parquet task outputs.")
    schemas = [pq.ParquetFile(path).schema_arrow for path in source_paths]
    try:
        schema = pa.unify_schemas(schemas, promote_options="permissive")
    except TypeError:  # pragma: no cover - older supported PyArrow fallback
        schema = pa.unify_schemas(schemas)
    temporary = staging_root / ".snapshot.parquet.tmp"
    output_path = staging_root / "snapshot.parquet"
    writer: pq.ParquetWriter | None = None
    rows = 0
    try:
        writer = pq.ParquetWriter(
            temporary,
            schema,
            compression=None if compression == "uncompressed" else compression,
        )
        for source_path in source_paths:
            parquet = pq.ParquetFile(source_path)
            for batch in parquet.iter_batches(batch_size=max(1, row_group_rows)):
                table = pa.Table.from_batches([batch])
                arrays: list[pa.ChunkedArray] = []
                for field in schema:
                    if field.name not in table.column_names:
                        arrays.append(pa.chunked_array([pa.nulls(table.num_rows, type=field.type)]))
                        continue
                    column = table.column(field.name)
                    arrays.append(
                        column
                        if column.type == field.type
                        else pc.cast(column, target_type=field.type, safe=True)
                    )
                aligned = pa.Table.from_arrays(arrays, schema=schema)
                writer.write_table(aligned, row_group_size=max(1, row_group_rows))
                rows += aligned.num_rows
        writer.close()
        writer = None
        os.replace(temporary, output_path)
        for source_path in source_paths:
            source_path.unlink()
        for directory in sorted(
            (path for path in staging_root.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            try:
                directory.rmdir()
            except OSError:
                pass
    except BaseException:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
        raise
    return {
        "mode": "streaming_parquet_single_file",
        "input_parts": len(source_paths),
        "output_parts": 1,
        "rows": rows,
        "relative_path": output_path.relative_to(staging_root).as_posix(),
        "row_group_rows": max(1, row_group_rows),
        "schema_unified_by_name": True,
    }


def _convert_staged_snapshot_to_sbdf(
    transaction: DatasetTransaction,
    *,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Convert the validated 0401 Parquet staging file to the committed SBDF artifact."""

    parquet_path = transaction.staging_root / "snapshot.parquet"
    if not parquet_path.is_file():
        raise RuntimeError("0401 SBDF conversion requires snapshot.parquet staging output.")
    row_key_columns = [str(value) for value in config.get("row_key_columns") or []]
    if not row_key_columns:
        raise ValidationError(
            "0401 SBDF output requires output.artifact.sbdf.row_key_columns.",
            code="output.sbdf_key_columns_required",
        )
    schema = pq.ParquetFile(parquet_path).schema_arrow
    missing = [name for name in row_key_columns if name not in schema.names]
    if missing:
        raise ValidationError(
            "0401 SBDF row key columns are missing from the final snapshot.",
            code="output.sbdf_key_column_missing",
            context={"missing": missing, "available": schema.names},
        )
    sbdf_path = transaction.staging_root / "snapshot.sbdf"
    sidecar_path = (
        transaction.staging_root / "_smoking_data" / "sbdf-keys" / "snapshot.keys.parquet"
    )
    ensure_dir(sidecar_path.parent)
    result = export_sbdf_with_result(
        SbdfExportRequest(
            parquet_files=[parquet_path],
            sbdf_path=sbdf_path,
            row_key_columns=row_key_columns,
            sidecar_path=sidecar_path,
            table_id=f"0401:{transaction.transaction_id}",
            batch_size=int(config.get("batch_size") or 50_000),
            encoding_rle=bool(config.get("encoding_rle", True)),
        )
    )
    if not result.output_path.is_file() or not sidecar_path.is_file():
        raise RuntimeError("smoking-sbdf did not produce the 0401 artifact and key sidecar.")
    source_rows = int(pq.ParquetFile(parquet_path).metadata.num_rows)
    if int(result.row_count) != source_rows:
        raise RuntimeError(
            f"0401 Parquet/SBDF row count mismatch: parquet={source_rows}, sbdf={result.row_count}"
        )
    parquet_path.unlink()
    relative = sbdf_path.relative_to(transaction.staging_root).as_posix()
    sidecar_relative = sidecar_path.relative_to(transaction.staging_root).as_posix()
    if transaction.manifest_context is not None:
        transaction.manifest_context["sbdf_parts"] = {
            relative: {
                "rows": source_rows,
                "schema": str(schema),
                "key_sidecar_relative_path": sidecar_relative,
            }
        }
    return {
        "mode": "parquet_staging_to_sbdf_atomic_commit",
        "output_parts": 1,
        "rows": source_rows,
        "relative_path": relative,
        "key_sidecar_relative_path": sidecar_relative,
        "row_key_columns": row_key_columns,
    }


def _coordinate_task_fingerprint(
    task: TaskSpec,
    *,
    logical_plan_hash: str,
    reference_fingerprint: str,
    source_stats: dict[str, dict[str, int]],
) -> str:
    coordinates = pl.read_parquet(task.payload["coordinate_path"])
    selected_sources = sorted(
        str(value) for value in coordinates.get_column(SOURCE_FILE_COLUMN).unique().to_list()
    )
    document = {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "logical_plan_hash": logical_plan_hash,
        "reference_fingerprint": reference_fingerprint,
        "partition_value": task.partition_value,
        "part_index": task.part_index,
        "output_row_group_rows": task.payload.get("output_row_group_rows"),
        "output_physical_layout_profile_hash": task.payload.get(
            "output_physical_layout_profile_hash"
        ),
        "probe_manifest_path": task.payload.get("probe_manifest_path"),
        "page_range_plan": task.payload.get("page_range_plan"),
        "compression": task.payload.get("compression"),
        "ordered_operations": task.payload.get("ordered_operations") or [],
        "coordinate_row_hash": hashlib.sha256(
            json.dumps(
                coordinates.hash_rows(seed=0).to_list(),
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "source_stats": {path: source_stats.get(path) for path in selected_sources},
    }
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _previous_part_fingerprints(
    spec: PresetSpec,
    *,
    config: RuntimeConfig,
) -> dict[str, str]:
    previous = read_metadata(spec, config=config) or {}
    result = previous.get("result") or {}
    details = result.get("details") or {}
    values = details.get("part_fingerprints") or {}
    return {str(key): str(value) for key, value in values.items()}


def _previous_adaptive_row_group_rows(
    spec: PresetSpec,
    *,
    config: RuntimeConfig,
) -> dict[str, int]:
    previous = read_metadata(spec, config=config) or {}
    result = previous.get("result") or {}
    details = result.get("details") or {}
    execution = details.get("adaptive_materialize_execution") or {}
    values = execution.get("applied_output_row_group_rows") or {}
    resolved: dict[str, int] = {}
    for key, value in values.items():
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            resolved[str(key)] = parsed
    return resolved


def _stage_reusable_coordinate_tasks(
    tasks: list[TaskSpec],
    *,
    current_fingerprints: dict[str, str],
    previous_fingerprints: dict[str, str],
    output_dir: Path,
    staging_root: Path,
) -> tuple[list[TaskResult], list[TaskSpec]]:
    reused: list[TaskResult] = []
    dirty: list[TaskSpec] = []
    for task in tasks:
        relative_path = Path(partition_dir_name(str(task.partition_value))) / part_file_name(
            int(task.part_index or 0)
        )
        existing = output_dir / relative_path
        if (
            previous_fingerprints.get(task.task_id) != current_fingerprints[task.task_id]
            or not existing.is_file()
            or existing.stat().st_size == 0
        ):
            dirty.append(task)
            continue
        staged = staging_root / relative_path
        ensure_dir(staged.parent)
        try:
            os.link(existing, staged)
        except OSError:
            shutil.copy2(existing, staged)
        rows = int(pq.ParquetFile(staged).metadata.num_rows)
        reused.append(
            TaskResult(
                task_id=task.task_id,
                ok=True,
                pid=0,
                partition_value=task.partition_value,
                part_index=task.part_index,
                output_paths=[staged],
                counters={
                    "output_rows": rows,
                    "output_files": 1,
                    "source_files_touched": 0,
                    "row_groups_touched": 0,
                    "reused_unchanged": 1,
                    "rust_ordered_operation_count": len(
                        task.payload.get("ordered_operations") or []
                    ),
                },
            )
        )
    return reused, dirty


def _apply_payload_lf(
    lf: pl.LazyFrame,
    *,
    payload: dict[str, Any],
    project_root: Path,
    retained_columns: list[str] | None = None,
) -> pl.LazyFrame:
    retained = list(retained_columns or [])
    lf = apply_filter_sql(lf, payload.get("filter_sql"))
    lf = apply_type_casts(lf, payload.get("type_casts"))
    lf = apply_add_calc(lf, payload.get("add_calc"))
    lf = _apply_reference_replace_configs(
        lf, payload.get("reference_replace"), project_root=project_root
    )
    excluded = [item for item in (payload.get("exclude_columns") or []) if item not in retained]
    lf = apply_exclude_columns(lf, excluded)
    included = list(payload.get("include_columns") or [])
    if included:
        included.extend(column for column in retained if column not in included)
    lf = apply_include_columns(lf, included)
    return lf


def _build_active_coordinate_snapshot(
    files,
    *,
    payload: dict[str, Any],
    expression_ir: dict[str, Any] | None,
    project_root: Path,
    partition_column: str,
    group_keys: list[str],
    selector_operation_id: str,
    sort: list[dict[str, Any]],
    rows_per_part: int,
    memory_budget_mb: int,
    max_source_files_per_task: int,
    max_source_row_groups_per_task: int,
    sidecar_workers: int,
    sidecar_worker_recycle_mode: str,
    sidecar_max_source_files: int,
    sidecar_max_projected_bytes_mb: int,
    candidate_target_bytes: int,
    sort_first_enabled: bool,
    pivot: dict[str, Any],
    candidate_root: Path,
    candidate_manifest_path: Path,
    logical_plan_hash: str,
    reference_fingerprint: str,
    previous_active_snapshot_path: Path,
    telemetry_endpoint: dict[str, Any] | None = None,
    calibrate_workers: bool = False,
    previous_active_sidecar_decision: dict[str, Any] | None = None,
) -> tuple[pl.DataFrame | None, dict[str, Any]]:
    _validate_reserved_payload_columns(payload)
    sort_columns = [str(item.get("column") or "").strip() for item in sort]
    window_partitions = _expression_ir_window_partitions(expression_ir)
    pivot_enabled = bool(pivot.get("enabled", False))
    pivot_row_keys = [str(item) for item in (pivot.get("row_keys") or [])]
    pivot_column_keys = [str(item) for item in (pivot.get("column_keys") or [])]
    pivot_value_columns = [
        str(item.get("source_column") or "")
        for item in [
            *(pivot.get("value_keys") or []),
            *(pivot.get("value_keys_without_column") or []),
        ]
        if isinstance(item, dict)
    ]
    window_columns = [column for keys in window_partitions for column in keys]
    selector_columns = list(
        dict.fromkeys(
            [
                partition_column,
                *group_keys,
                *sort_columns,
                *window_columns,
                *pivot_row_keys,
                *pivot_column_keys,
                *pivot_value_columns,
            ]
        )
    )
    planner_payload = dict(payload)
    planner_payload["add_calc"] = _planner_add_calc_configs(
        payload.get("add_calc"),
        expression_ir=expression_ir,
        selector_columns=selector_columns,
    )
    # Final projection is a payload-writer concern; applying it to the thin planner
    # can remove raw selector columns or request Rust-only calculated columns.
    planner_payload["include_columns"] = []
    planner_payload["exclude_columns"] = []
    reserved_columns = {
        SOURCE_FILE_COLUMN,
        SOURCE_ROW_INDEX_COLUMN,
        SOURCE_ROW_GROUP_COLUMN,
        ACTIVE_ORDER_COLUMN,
        PART_INDEX_COLUMN,
        ESTIMATED_PAYLOAD_BYTES_COLUMN,
        SPILL_REQUIRED_COLUMN,
    }
    parquet_profiles = profile_parquet_files([item.path for item in files])
    selection_group_keys = list(dict.fromkeys([partition_column, *group_keys]))
    candidate_paths, impacted_frames, sidecar_profile = _refresh_candidate_sidecars(
        files,
        candidate_root=candidate_root,
        manifest_path=candidate_manifest_path,
        selector_operation_id=selector_operation_id,
        logical_plan_hash=logical_plan_hash,
        reference_fingerprint=reference_fingerprint,
        selector_columns=selector_columns,
        selection_group_keys=selection_group_keys,
        reserved_columns=reserved_columns,
        planner_payload=planner_payload,
        project_root=project_root,
        parquet_profiles=parquet_profiles,
        workers=max(1, int(sidecar_workers)),
        worker_recycle_mode=sidecar_worker_recycle_mode,
        max_source_files=sidecar_max_source_files,
        max_projected_bytes_mb=sidecar_max_projected_bytes_mb,
        memory_budget_mb=memory_budget_mb,
        telemetry_endpoint=telemetry_endpoint,
        calibrate_workers=calibrate_workers,
    )
    sidecar_profile["parent_memory_boundaries"] = [_parent_memory_boundary("candidate_ready")]
    if not candidate_paths:
        raise ValidationError("0201 coordinate sidecar has no source files.")
    if sort_first_enabled:
        compaction_profile = {
            "skipped": True,
            "skip_reason": "active_selector_uses_adaptive_sidecar_path",
            "input_files": len(candidate_paths),
            "output_files": len(candidate_paths),
        }
    else:
        candidate_paths, compaction_profile = _compact_candidate_sidecars(
            candidate_paths,
            output_root=candidate_root.parent / "candidates_compacted",
            partition_column=partition_column,
            target_bytes=candidate_target_bytes,
        )
    sidecar_profile["compaction"] = compaction_profile
    sidecar_profile["operation_id"] = selector_operation_id
    sidecar_profile["selector_columns"] = selector_columns
    previous_active = _read_previous_active_snapshot(
        previous_active_snapshot_path,
        expected_columns=selector_columns,
    )
    impacted_candidate_bytes = sum(int(frame.estimated_size()) for frame in impacted_frames)
    can_increment = (
        not sidecar_profile["full_rebuild"]
        and previous_active is not None
        and impacted_candidate_bytes <= memory_budget_mb * 1024 * 1024
    )
    if previous_active is not None and impacted_candidate_bytes > memory_budget_mb * 1024 * 1024:
        sidecar_profile["incremental_fallback_reason"] = "impacted_keys_exceed_memory_budget"
    if can_increment and impacted_frames:
        impacted_keys = (
            pl.concat(impacted_frames, how="diagonal_relaxed").select(selection_group_keys).unique()
        )
        previous_base = previous_active.drop(
            [ACTIVE_ORDER_COLUMN, PART_INDEX_COLUMN, SPILL_REQUIRED_COLUMN],
            strict=False,
        )
        unaffected = previous_base.join(
            impacted_keys,
            on=selection_group_keys,
            how="anti",
            nulls_equal=True,
        )
        candidate_lf = pl.concat(
            [pl.scan_parquet(path) for path in candidate_paths],
            how="diagonal_relaxed",
        )
        selected_sidecar_lf = candidate_lf.join(
            impacted_keys.lazy(),
            on=selection_group_keys,
            how="inner",
            nulls_equal=True,
        )
        sidecar_profile["active_recompute_mode"] = "impacted_groups"
        sidecar_profile["impacted_groups"] = impacted_keys.height
    else:
        unaffected = None
        selected_sidecar_lf = None
        sidecar_profile["active_recompute_mode"] = "full"
        sidecar_profile["impacted_groups"] = None
    candidate_bytes = sum(path.stat().st_size for path in candidate_paths)
    spill_profile: dict[str, Any] | None = None
    active_from_spill: pl.DataFrame | None = None
    if sort_first_enabled and unaffected is None:
        selector_shape = profile_selector_shape(
            candidate_paths,
            selection_group_keys=selection_group_keys,
            candidate_rows=int(sidecar_profile["candidate_rows"]),
        )
        active_sidecar_decision = build_active_sidecar_decision(
            candidate_files=len(candidate_paths),
            candidate_bytes=candidate_bytes,
            candidate_rows=int(sidecar_profile["candidate_rows"]),
            memory_budget_mb=memory_budget_mb,
            selector_shape=selector_shape,
            previous_decision=previous_active_sidecar_decision,
            force_enabled=os.getenv(INTERNAL_FORCE_ACTIVE_SIDECAR_PLAN_ENV, "0") == "1",
            force_disabled=os.getenv(INTERNAL_DISABLE_ACTIVE_SIDECAR_PLAN_ENV, "0") == "1",
        )
    elif sort_first_enabled:
        active_sidecar_decision = {
            "schema_version": ACTIVE_SIDECAR_DECISION_VERSION,
            "selected_mode": "incremental_impacted_groups",
            "active_sidecar_plan_enabled": False,
            "reason": "incremental_snapshot_reuse",
            "impacted_groups": sidecar_profile.get("impacted_groups"),
        }
    else:
        active_sidecar_decision = {
            "schema_version": ACTIVE_SIDECAR_DECISION_VERSION,
            "selected_mode": "not_applicable",
            "active_sidecar_plan_enabled": False,
            "reason": "sort_first_disabled",
        }
    sidecar_profile["active_sidecar_decision"] = active_sidecar_decision
    selected_active_mode = str(active_sidecar_decision["selected_mode"])
    direct_selector = selected_active_mode == "direct"
    bucketize_required = sort_first_enabled and unaffected is None and not direct_selector
    if not bucketize_required:
        emit_task_telemetry_event(
            telemetry_endpoint,
            "phase_planned",
            task_id=None,
            details={
                "phase_name": "build_sidecar.bucketize",
                "total": 0,
                "unit": "row_groups",
                "skipped": True,
                "reason": selected_active_mode,
            },
        )
    if sort_first_enabled and unaffected is None and direct_selector:
        selected_sidecar_lf = pl.concat(
            [pl.scan_parquet(path) for path in candidate_paths],
            how="diagonal_relaxed",
        )
        sidecar = selected_sidecar_lf.collect(engine="streaming")
        group_sizes = sidecar.group_by(selection_group_keys).len()
        descending = [str(item.get("direction") or "asc").lower() == "desc" for item in sort]
        nulls_last = [str(item.get("nulls") or "last").lower() == "last" for item in sort]
        active_from_spill = (
            sidecar.sort(
                [
                    *sort_columns,
                    SOURCE_FILE_COLUMN,
                    SOURCE_ROW_GROUP_COLUMN,
                    SOURCE_ROW_INDEX_COLUMN,
                ],
                descending=[*descending, False, False, False],
                nulls_last=[*nulls_last, False, False, False],
            )
            .group_by(selection_group_keys, maintain_order=True)
            .first()
        )
        sidecar = active_from_spill
        spill_profile = {
            "mode": "direct_bounded",
            "candidate_bytes": candidate_bytes,
            "candidate_files": len(candidate_paths),
        }
    elif (
        sort_first_enabled and unaffected is None and selected_active_mode == "active_sidecar_plan"
    ):
        del impacted_frames
        gc.collect()
        if max(1, int(sidecar_workers)) == 1:
            active_plan, bucket_profile = _build_active_sidecar_pipeline(
                candidate_paths,
                root=candidate_root / "_selector_buckets",
                active_snapshot_path=previous_active_snapshot_path,
                partition_column=partition_column,
                selection_group_keys=selection_group_keys,
                sort=sort,
                group_keys=group_keys,
                window_partitions=window_partitions,
                pivot=pivot,
                spill_aggregation=_pivot_spill_aggregation(pivot, payload=payload),
                rows_per_part=rows_per_part,
                memory_budget_mb=memory_budget_mb,
                max_source_files_per_task=max_source_files_per_task,
                max_source_row_groups_per_task=max_source_row_groups_per_task,
                telemetry_endpoint=telemetry_endpoint,
                candidate_rows=int(sidecar_profile["candidate_rows"]),
            )
            active_piece_result = None
        else:
            active_piece_result, bucket_profile = _select_active_rows_in_buckets(
                candidate_paths,
                root=candidate_root / "_selector_buckets",
                partition_column=partition_column,
                selection_group_keys=selection_group_keys,
                sort=sort,
                memory_budget_mb=memory_budget_mb,
                workers=sidecar_workers,
                telemetry_endpoint=telemetry_endpoint,
                return_piece_paths=True,
                candidate_rows_hint=int(sidecar_profile["candidate_rows"]),
            )
        if sidecar_workers != 1 and not isinstance(active_piece_result, list):
            raise TaskExecutionError("0201 selector did not return an active piece dataset.")
        sidecar_profile["parent_memory_boundaries"].extend(
            list(bucket_profile.get("parent_memory_boundaries") or [])
        )
        spill_profile = bucket_profile
        if sidecar_workers != 1:
            active_plan = _build_active_sidecar_plan_from_pieces(
                active_piece_result,
                active_snapshot_path=previous_active_snapshot_path,
                partition_column=partition_column,
                group_keys=group_keys,
                window_partitions=window_partitions,
                pivot=pivot,
                spill_aggregation=_pivot_spill_aggregation(pivot, payload=payload),
                rows_per_part=rows_per_part,
                memory_budget_mb=memory_budget_mb,
                max_source_files_per_task=max_source_files_per_task,
                max_source_row_groups_per_task=max_source_row_groups_per_task,
                telemetry_endpoint=telemetry_endpoint,
            )
        sidecar_profile["active_sidecar_plan"] = active_plan
        sidecar_profile["coordinate_boundary_fanout"] = dict(
            active_plan["coordinate_boundary_fanout"]
        )
        sidecar_profile["pivot_spill_fallback"] = dict(active_plan["pivot_spill_fallback"])
        sidecar_profile["selector_key_cardinality"] = int(
            spill_profile.get("selector_key_cardinality", 0)
        )
        sidecar_profile["max_rows_per_selector_group"] = int(
            spill_profile.get("max_rows_per_selector_group", 0)
        )
        sidecar_profile["avg_rows_per_selector_group"] = float(
            spill_profile.get("avg_rows_per_selector_group", 0.0)
        )
        sidecar_profile["intermediate_spill"] = spill_profile
        sidecar_profile["parent_memory_boundaries"].append(
            _parent_memory_boundary("active_sidecar_plan_ready")
        )
        return None, sidecar_profile
    elif sort_first_enabled and unaffected is None:
        active_from_spill, bucket_profile = _select_active_rows_in_buckets(
            candidate_paths,
            root=candidate_root / "_selector_buckets",
            partition_column=partition_column,
            selection_group_keys=selection_group_keys,
            sort=sort,
            memory_budget_mb=memory_budget_mb,
            workers=sidecar_workers,
            telemetry_endpoint=telemetry_endpoint,
            return_piece_paths=False,
            candidate_rows_hint=int(sidecar_profile["candidate_rows"]),
        )
        if not isinstance(active_from_spill, pl.DataFrame):
            raise TaskExecutionError("0201 benchmark selector did not return active rows.")
        sidecar_profile["parent_memory_boundaries"].extend(
            list(bucket_profile.get("parent_memory_boundaries") or [])
        )
        spill_profile = bucket_profile
        sidecar = active_from_spill
        group_sizes = None
    elif sort_first_enabled and candidate_bytes > memory_budget_mb * 1024 * 1024:
        if selected_sidecar_lf is None:
            selected_sidecar_lf = pl.concat(
                [pl.scan_parquet(path) for path in candidate_paths],
                how="diagonal_relaxed",
            )
        descending = [str(item.get("direction") or "asc").lower() == "desc" for item in sort]
        nulls_last = [str(item.get("nulls") or "last").lower() == "last" for item in sort]
        spill = write_sorted_intermediate(
            selected_sidecar_lf,
            root=candidate_root.parent / "spill",
            sort_columns=[
                *sort_columns,
                SOURCE_FILE_COLUMN,
                SOURCE_ROW_INDEX_COLUMN,
            ],
            descending=[*descending, False, False],
            nulls_last=[*nulls_last, False, False],
        )
        with spill:
            active_from_spill = (
                pl.scan_parquet(spill.path)
                .group_by(selection_group_keys, maintain_order=True)
                .first()
                .collect(engine="streaming")
            )
            spill_profile = spill.profile()
        spill_profile = spill.profile()
        sidecar = active_from_spill
        group_sizes = (
            selected_sidecar_lf.group_by(selection_group_keys).len().collect(engine="streaming")
        )
    else:
        if selected_sidecar_lf is None:
            selected_sidecar_lf = pl.concat(
                [pl.scan_parquet(path) for path in candidate_paths],
                how="diagonal_relaxed",
            )
        sidecar = selected_sidecar_lf.collect(engine="streaming")
        group_sizes = sidecar.group_by(selection_group_keys).len()
    sidecar_profile["intermediate_spill"] = spill_profile
    sidecar_profile["selector_key_cardinality"] = (
        int(spill_profile.get("selector_key_cardinality", 0))
        if group_sizes is None and spill_profile is not None
        else group_sizes.height
    )
    sidecar_profile["max_rows_per_selector_group"] = (
        int(spill_profile.get("max_rows_per_selector_group", 0))
        if group_sizes is None and spill_profile is not None
        else int(group_sizes.get_column("len").max() or 0)
        if group_sizes.height
        else 0
    )
    sidecar_profile["avg_rows_per_selector_group"] = (
        float(spill_profile.get("avg_rows_per_selector_group", 0.0))
        if group_sizes is None and spill_profile is not None
        else float(group_sizes.get_column("len").mean() or 0.0)
        if group_sizes.height
        else 0.0
    )
    if active_from_spill is not None:
        active = active_from_spill
    elif sort_first_enabled:
        descending = [str(item.get("direction") or "asc").lower() == "desc" for item in sort]
        nulls_last = [str(item.get("nulls") or "last").lower() == "last" for item in sort]
        active = (
            sidecar.sort(
                [*sort_columns, SOURCE_FILE_COLUMN, SOURCE_ROW_INDEX_COLUMN],
                descending=[*descending, False, False],
                nulls_last=[*nulls_last, False, False],
            )
            .group_by(selection_group_keys, maintain_order=True)
            .first()
        )
    else:
        active = sidecar
    if unaffected is not None:
        active = pl.concat([unaffected, active], how="diagonal_relaxed")
    active = active.sort(
        list(
            dict.fromkeys(
                [partition_column, *group_keys, SOURCE_FILE_COLUMN, SOURCE_ROW_INDEX_COLUMN]
            )
        )
    )
    active = active.drop(SPILL_REQUIRED_COLUMN, strict=False)
    if active.get_column(partition_column).null_count():
        raise ValidationError(f"0201 partition column contains null values: {partition_column}")
    window_keys = _validate_window_task_boundaries(
        active,
        partition_column=partition_column,
        window_partitions=window_partitions,
    )
    task_group_keys = _task_complete_group_keys(
        active,
        partition_column=partition_column,
        window_keys=window_keys,
        pivot_row_keys=pivot_row_keys if pivot_enabled else None,
    )
    spill_aggregation = _pivot_spill_aggregation(pivot, payload=payload)
    spill_allowed = spill_aggregation is not None and window_keys is None
    sidecar_profile["pivot_spill_fallback"] = {
        "eligible": spill_allowed,
        "merge_aggregation": spill_aggregation,
        "window_barrier_present": window_keys is not None,
    }
    partition_frames: list[pl.DataFrame] = []
    for partition_value in active.get_column(partition_column).unique(maintain_order=True):
        partition_frame = active.filter(pl.col(partition_column) == partition_value).with_row_index(
            ACTIVE_ORDER_COLUMN
        )
        partition_frames.append(
            _assign_window_safe_part_indices(
                partition_frame,
                window_keys=task_group_keys,
                barrier_state=(
                    BarrierState.WINDOW
                    if window_keys is not None
                    else BarrierState.PIVOT
                    if pivot_enabled
                    else BarrierState.SORT_FIRST
                ),
                rows_per_part=rows_per_part,
                max_payload_bytes=memory_budget_mb * 1024 * 1024,
                max_source_files=max_source_files_per_task,
                max_source_row_groups=max_source_row_groups_per_task,
                allow_oversized_group_spill=spill_allowed,
            )
        )
    if not partition_frames:
        empty = active.with_columns(
            pl.lit(None, dtype=pl.Int64).alias(ACTIVE_ORDER_COLUMN),
            pl.lit(None, dtype=pl.Int64).alias(PART_INDEX_COLUMN),
        ).head(0)
        sidecar_profile["coordinate_boundary_fanout"] = _coordinate_boundary_fanout_profile(
            empty,
            partition_column=partition_column,
            max_source_files=max_source_files_per_task,
            max_source_row_groups=max_source_row_groups_per_task,
        )
        sidecar_profile["parent_memory_boundaries"].append(
            _parent_memory_boundary(
                "active_snapshot_ready",
                active_snapshot_estimated_mb=empty.estimated_size() / (1024 * 1024),
            )
        )
        return empty, sidecar_profile
    resolved = pl.concat(partition_frames, how="diagonal_relaxed")
    sidecar_profile["coordinate_boundary_fanout"] = _coordinate_boundary_fanout_profile(
        resolved,
        partition_column=partition_column,
        max_source_files=max_source_files_per_task,
        max_source_row_groups=max_source_row_groups_per_task,
    )
    sidecar_profile["parent_memory_boundaries"].append(
        _parent_memory_boundary(
            "active_snapshot_ready",
            active_snapshot_estimated_mb=resolved.estimated_size() / (1024 * 1024),
        )
    )
    return resolved, sidecar_profile


def _build_active_sidecar_plan_from_pieces(
    active_piece_paths: list[Path],
    *,
    active_snapshot_path: Path,
    partition_column: str,
    group_keys: list[str],
    window_partitions: list[tuple[str, ...]],
    pivot: dict[str, Any],
    spill_aggregation: str | None,
    rows_per_part: int,
    memory_budget_mb: int,
    max_source_files_per_task: int,
    max_source_row_groups_per_task: int,
    telemetry_endpoint: dict[str, Any] | None,
) -> dict[str, Any]:
    ensure_dir(active_snapshot_path.parent)
    plan_path = active_snapshot_path.with_name(
        f"{active_snapshot_path.stem}.active-sidecar-plan.json"
    )
    identifier = f"{os.getpid()}-{time.time_ns()}"
    request_path = active_snapshot_path.parent / f".active-sidecar-plan.{identifier}.request.json"
    result_path = active_snapshot_path.parent / f".active-sidecar-plan.{identifier}.result.json"
    try:
        _atomic_write_json(
            request_path,
            {
                "schema_version": ACTIVE_SIDECAR_PLAN_REQUEST_VERSION,
                "active_piece_paths": [str(path) for path in active_piece_paths],
                "active_snapshot_path": str(active_snapshot_path),
                "plan_path": str(plan_path),
                "partition_column": partition_column,
                "group_keys": group_keys,
                "window_partitions": [list(item) for item in window_partitions],
                "pivot": pivot,
                "spill_aggregation": spill_aggregation,
                "rows_per_part": rows_per_part,
                "memory_budget_bytes": memory_budget_mb * 1024 * 1024,
                "max_source_files_per_task": max_source_files_per_task,
                "max_source_row_groups_per_task": max_source_row_groups_per_task,
                "telemetry_endpoint": telemetry_endpoint,
            },
        )
        worker = run_active_sidecar_plan_subprocess(request_path, result_path)
        plan = load_active_sidecar_plan(plan_path)
    finally:
        reset_path(request_path)
        reset_path(result_path)
    snapshot_relative = Path(str(plan["active_snapshot_path"]))
    if snapshot_relative.is_absolute() or ".." in snapshot_relative.parts:
        raise TaskExecutionError("0201 active-sidecar-plan returned an unsafe snapshot path.")
    resolved_snapshot = (plan_path.parent / snapshot_relative).resolve()
    if not resolved_snapshot.exists():
        raise TaskExecutionError("0201 active-sidecar-plan snapshot is missing after commit.")
    for item in plan.get("coordinate_tasks") or []:
        for key in ("coordinate_path", "rust_coordinate_path"):
            relative = Path(str(item[key]))
            resolved = (plan_path.parent / relative).resolve()
            if relative.is_absolute() or ".." in relative.parts or not resolved.is_file():
                raise TaskExecutionError(
                    "0201 active-sidecar-plan returned an invalid coordinate path.",
                    context={"key": key, "path": str(relative)},
                )
    return {
        **plan,
        "manifest_path": str(plan_path),
        "active_snapshot_path": str(resolved_snapshot),
        "worker": {
            "pid": worker.get("pid"),
            "elapsed_sec": worker.get("elapsed_sec"),
            "rss_mb": worker.get("rss_mb"),
            "peak_rss_mb": worker.get("peak_rss_mb"),
            "io_read_bytes": worker.get("io_read_bytes"),
            "io_write_bytes": worker.get("io_write_bytes"),
        },
    }


def _candidate_projected_source_columns(
    *,
    selector_columns: list[str],
    planner_payload: dict[str, Any],
    parquet_profiles: dict[str, Any],
) -> tuple[str, ...]:
    available = {
        column.name
        for profile in parquet_profiles.values()
        for group in profile.row_groups
        for column in group.columns
    }
    required = {column for column in selector_columns if column in available}
    required.update(
        str(item.get("name") or item.get("column") or "")
        for item in (planner_payload.get("type_casts") or [])
        if isinstance(item, dict)
    )
    expressions: list[str] = []
    filter_sql = str(planner_payload.get("filter_sql") or "").strip()
    if filter_sql:
        expressions.append(filter_sql)
    if planner_payload.get("add_calc"):
        from spotfire_expr_normalizer import normalize_expression

        for index, item in enumerate(planner_payload.get("add_calc") or []):
            if not isinstance(item, dict):
                continue
            dialect, expression = resolve_add_calc_expression(item, index=index)
            expressions.append(
                normalize_expression(expression) if dialect == "spotfire_expression" else expression
            )
    try:
        for expression in expressions:
            required.update(pl.sql_expr(expression).meta.root_names())
    except Exception:
        # Estimation must fail conservatively; execution validation remains in the
        # existing expression path.
        return tuple(sorted(available))
    return tuple(sorted(column for column in required if column in available))


def _refresh_candidate_sidecars(
    files,
    *,
    candidate_root: Path,
    manifest_path: Path,
    selector_operation_id: str,
    logical_plan_hash: str,
    reference_fingerprint: str,
    selector_columns: list[str],
    selection_group_keys: list[str],
    reserved_columns: set[str],
    planner_payload: dict[str, Any],
    project_root: Path,
    parquet_profiles: dict[str, Any],
    workers: int,
    worker_recycle_mode: str,
    max_source_files: int,
    max_projected_bytes_mb: int,
    memory_budget_mb: int,
    telemetry_endpoint: dict[str, Any] | None = None,
    calibrate_workers: bool = False,
) -> tuple[list[Path], list[pl.DataFrame], dict[str, Any]]:
    previous = _read_candidate_manifest(manifest_path)
    expected_contract = {
        "version": CANDIDATE_MANIFEST_VERSION,
        "operation_id": selector_operation_id,
        "logical_plan_hash": logical_plan_hash,
        "reference_fingerprint": reference_fingerprint,
        "selector_columns": selector_columns,
    }
    full_rebuild = any(previous.get(key) != value for key, value in expected_contract.items())
    if full_rebuild:
        reset_path(candidate_root)
        ensure_dir(candidate_root)
        previous_sources: dict[str, Any] = {}
    else:
        previous_sources = previous.get("sources") or {}

    current_paths = {str(Path(item.path).resolve()) for item in files}
    deleted_paths = sorted(set(previous_sources) - current_paths)
    impacted_frames: list[pl.DataFrame] = []
    for source_path in deleted_paths:
        entry = previous_sources.get(source_path) or {}
        candidate_path = Path(str(entry.get("candidate_path") or ""))
        if candidate_path.is_file():
            impacted_frames.append(pl.read_parquet(candidate_path).select(selection_group_keys))
            reset_path(candidate_path)

    source_entries: dict[str, Any] = {}
    candidate_paths: list[Path] = []
    schema_by_source: dict[str, dict[str, str]] = {}
    pending_tasks: list[TaskSpec] = []
    projected_source_columns = _candidate_projected_source_columns(
        selector_columns=selector_columns,
        planner_payload=planner_payload,
        parquet_profiles=parquet_profiles,
    )
    source_states: list[dict[str, Any]] = []
    reused = 0
    for source_file in files:
        source_path = Path(source_file.path)
        resolved_path = str(source_path.resolve())
        fingerprint = file_fingerprint(source_file)
        candidate_path = (
            candidate_root / f"{hashlib.sha256(resolved_path.encode('utf-8')).hexdigest()}.parquet"
        )
        old_entry = previous_sources.get(resolved_path) or {}
        can_reuse = (
            not full_rebuild
            and old_entry.get("fingerprint") == fingerprint
            and Path(str(old_entry.get("candidate_path") or "")) == candidate_path
            and candidate_path.is_file()
            and candidate_path.stat().st_size > 0
        )
        if can_reuse:
            rows = int(old_entry.get("rows") or 0)
            reused += 1
        else:
            if candidate_path.is_file():
                impacted_frames.append(pl.read_parquet(candidate_path).select(selection_group_keys))
            profile = parquet_profiles[resolved_path]
            projected_bytes = max(
                1,
                profile.estimated_uncompressed_bytes(columns=projected_source_columns),
            )
            average_payload_bytes = max(
                1,
                profile.estimated_uncompressed_bytes() // max(1, profile.rows),
            )
            pending_tasks.append(
                TaskSpec(
                    task_id=f"candidate-{hashlib.sha256(resolved_path.encode()).hexdigest()[:16]}",
                    payload={
                        "source_path": str(source_path),
                        "candidate_path": str(candidate_path),
                        "selector_columns": selector_columns,
                        "reserved_columns": sorted(reserved_columns),
                        "planner_payload": planner_payload,
                        "project_root": str(project_root),
                        "average_payload_bytes": average_payload_bytes,
                        "projected_source_bytes": projected_bytes,
                        "__telemetry_phase_name": "build_sidecar.candidate",
                        "__telemetry_phase_only": True,
                    },
                )
            )
            rows = None
        source_states.append(
            {
                "resolved_path": resolved_path,
                "fingerprint": fingerprint,
                "candidate_path": candidate_path,
                "rows": rows,
                "rebuilt": not can_reuse,
            }
        )

    pending_estimated_bytes = sum(
        int(task.payload["projected_source_bytes"]) for task in pending_tasks
    )
    projected_limit_bytes = max(1, int(max_projected_bytes_mb)) * 1024 * 1024
    largest_projected_file = max(
        (int(task.payload["projected_source_bytes"]) for task in pending_tasks),
        default=1,
    )
    adaptive_max_tasks_per_child = min(
        max(1, int(max_source_files)),
        max(1, projected_limit_bytes // largest_projected_file),
    )
    direct_candidate_build = (
        pending_estimated_bytes <= memory_budget_mb * 1024 * 1024 // 4 and len(pending_tasks) <= 16
    )
    candidate_runner_profiles: list[dict[str, Any]] = []
    candidate_attempt_results: list[TaskResult] = []
    fallback_count = 0
    calibrated_workers = max(1, int(workers))
    calibration_peak_rss_mb: float | None = None
    if direct_candidate_build:
        emit_task_telemetry_event(
            telemetry_endpoint,
            "phase_planned",
            task_id=None,
            details={
                "phase_name": "build_sidecar.candidate",
                "total": len(pending_tasks),
                "unit": "files",
                "skipped": not pending_tasks,
            },
        )
        candidate_results = []
        for task_index, task in enumerate(pending_tasks, start=1):
            with task_telemetry_phase(
                telemetry_endpoint,
                "build_sidecar.candidate",
                task_id=task.task_id,
            ):
                candidate_results.append(_candidate_sidecar_task_worker(task))
            emit_task_telemetry_event(
                telemetry_endpoint,
                "phase_progress",
                task_id=task.task_id,
                details={
                    "phase_name": "build_sidecar.candidate",
                    "completed": task_index,
                    "total": len(pending_tasks),
                    "unit": "files",
                },
            )
    else:
        calibration_results: list[TaskResult] = []
        remaining_candidate_tasks = pending_tasks
        if calibrate_workers and len(pending_tasks) > 1:
            calibration_results, calibration_profile = run_tasks_in_subprocesses(
                pending_tasks[:1],
                worker=_candidate_sidecar_task_worker,
                workers=1,
                max_tasks_per_child=1,
                return_profile=True,
                telemetry_endpoint=telemetry_endpoint,
            )
            candidate_runner_profiles.append(calibration_profile)
            candidate_attempt_results.extend(calibration_results)
            calibration_peak_rss_mb = (
                max(
                    (
                        float(result.counters.get("rss_peak_mb") or 0.0)
                        for result in calibration_results
                        if result.ok
                    ),
                    default=0.0,
                )
                or None
            )
            if calibration_peak_rss_mb is not None:
                calibrated_workers = max(
                    1,
                    min(
                        calibrated_workers,
                        math.floor(memory_budget_mb * 0.80 / max(1.0, calibration_peak_rss_mb)),
                    ),
                )
            remaining_candidate_tasks = pending_tasks[1:]
        remaining_results: list[TaskResult] = []
        if remaining_candidate_tasks:
            remaining_results, runner_profile = run_tasks_in_subprocesses(
                remaining_candidate_tasks,
                worker=_candidate_sidecar_task_worker,
                workers=calibrated_workers,
                max_tasks_per_child=adaptive_max_tasks_per_child,
                return_profile=True,
                telemetry_endpoint=telemetry_endpoint,
            )
            candidate_runner_profiles.append(runner_profile)
            candidate_attempt_results.extend(remaining_results)
        candidate_results = [*calibration_results, *remaining_results]
        retry_batch_size = adaptive_max_tasks_per_child
        while retry_batch_size > 1:
            retryable_ids = {
                result.task_id
                for result in candidate_results
                if not result.ok
                and result.error_type in {"ChildProcessAbnormalExit", "MemoryError"}
            }
            if not retryable_ids:
                break
            retry_tasks = [task for task in pending_tasks if task.task_id in retryable_ids]
            retry_batch_size = max(1, retry_batch_size // 2)
            retry_results, retry_profile = run_tasks_in_subprocesses(
                retry_tasks,
                worker=_candidate_sidecar_task_worker,
                workers=workers,
                max_tasks_per_child=retry_batch_size,
                return_profile=True,
                telemetry_endpoint=telemetry_endpoint,
            )
            fallback_count += 1
            candidate_runner_profiles.append(retry_profile)
            candidate_attempt_results.extend(retry_results)
            replacements = {result.task_id: result for result in retry_results}
            candidate_results = [
                replacements.get(result.task_id, result) for result in candidate_results
            ]
    failures = [result for result in candidate_results if not result.ok]
    if failures:
        failure = failures[0]
        raise TaskExecutionError(
            f"0201 candidate sidecar task failed: {failure.error_message}",
            context={
                "task_id": failure.task_id,
                "error_type": failure.error_type,
                "traceback_tail": failure.traceback_tail,
            },
        )
    rows_by_path = {
        str(result.output_paths[0]): int(result.counters.get("candidate_rows", 0))
        for result in candidate_results
    }
    for state in source_states:
        resolved_path = str(state["resolved_path"])
        candidate_path = Path(state["candidate_path"])
        rows = state["rows"]
        if state["rebuilt"]:
            rows = rows_by_path[str(candidate_path)]
            impacted_frames.append(pl.read_parquet(candidate_path).select(selection_group_keys))
        candidate_paths.append(candidate_path)
        candidate_schema = pl.read_parquet_schema(candidate_path)
        schema_by_source[resolved_path] = {
            name: str(dtype) for name, dtype in candidate_schema.items()
        }
        source_entries[resolved_path] = {
            "fingerprint": state["fingerprint"],
            "candidate_path": str(candidate_path),
            "rows": rows,
            "size_bytes": candidate_path.stat().st_size,
        }

    schema_hash = hashlib.sha256(
        json.dumps(schema_by_source, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    manifest = {
        **expected_contract,
        "schemas": schema_by_source,
        "schema_hash": schema_hash,
        "sources": source_entries,
    }
    _atomic_write_json(manifest_path, manifest)
    profile = {
        "manifest_path": str(manifest_path),
        "manifest_version": CANDIDATE_MANIFEST_VERSION,
        "schema_hash": schema_hash,
        "full_rebuild": full_rebuild,
        "rebuilt_source_files": len(pending_tasks),
        "reused_source_files": reused,
        "deleted_source_files": len(deleted_paths),
        "candidate_files": len(source_entries),
        "candidate_rows": sum(int(item["rows"]) for item in source_entries.values()),
        "candidate_bytes": sum(int(item["size_bytes"]) for item in source_entries.values()),
        "candidate_task_processes": len(
            {
                result.pid
                for result in (candidate_attempt_results or candidate_results)
                if result.pid > 0
            }
        ),
        "candidate_execution_mode": (
            "direct_bounded" if direct_candidate_build else "source_task_subprocess"
        ),
        "candidate_task_peak_rss_mb": max(
            (
                float(result.counters.get("rss_peak_mb", 0.0))
                for result in (candidate_attempt_results or candidate_results)
            ),
            default=0.0,
        ),
        "execution": {
            "policy": worker_recycle_mode,
            "requested_workers": workers,
            "calibrated_workers": calibrated_workers,
            "calibration_peak_rss_mb": calibration_peak_rss_mb,
            "max_source_files_per_child": max(1, int(max_source_files)),
            "max_projected_bytes_mb": max(1, int(max_projected_bytes_mb)),
            "projected_source_columns": list(projected_source_columns),
            "projected_source_bytes": pending_estimated_bytes,
            "largest_projected_source_file_bytes": largest_projected_file,
            "effective_max_source_files_per_child": adaptive_max_tasks_per_child,
            "process_generations": sum(
                int(item.get("submitted_futures") or 0) for item in candidate_runner_profiles
            ),
            "fallback_count": fallback_count,
            "runner_attempts": candidate_runner_profiles,
        },
    }
    return candidate_paths, impacted_frames, profile


def _select_active_rows_in_buckets(
    candidate_paths: list[Path],
    *,
    root: Path,
    partition_column: str,
    selection_group_keys: list[str],
    sort: list[dict[str, Any]],
    memory_budget_mb: int,
    workers: int,
    telemetry_endpoint: dict[str, Any] | None = None,
    return_piece_paths: bool = False,
    candidate_rows_hint: int | None = None,
) -> tuple[pl.DataFrame | list[Path], dict[str, Any]]:
    """Hash-shard a full selector rebuild before any global winner collect."""
    candidate_bytes = sum(path.stat().st_size for path in candidate_paths)
    candidate_rows = (
        max(0, int(candidate_rows_hint))
        if candidate_rows_hint is not None
        else sum(pq.ParquetFile(path).metadata.num_rows for path in candidate_paths)
    )
    target_bucket_bytes = max(8 * 1024 * 1024, memory_budget_mb * 1024 * 1024 // 4)
    byte_bucket_count = min(
        1024, (candidate_bytes + target_bucket_bytes - 1) // target_bucket_bytes
    )
    row_bucket_count = min(
        1024,
        (candidate_rows + SELECTOR_TARGET_ROWS_PER_BUCKET - 1) // SELECTOR_TARGET_ROWS_PER_BUCKET,
    )
    bucket_count = max(1, byte_bucket_count, row_bucket_count)
    if return_piece_paths and max(1, int(workers)) == 1:
        return _select_active_piece_dataset_in_subprocess(
            candidate_paths,
            root=root,
            partition_column=partition_column,
            selection_group_keys=selection_group_keys,
            sort=sort,
            memory_budget_mb=memory_budget_mb,
            candidate_rows=candidate_rows,
            bucket_count=bucket_count,
            telemetry_endpoint=telemetry_endpoint,
        )
    staging = root.parent / f".{root.name}.{os.getpid()}.tmp"
    backup = root.parent / f".{root.name}.{os.getpid()}.backup"
    reset_path(staging)
    reset_path(backup)
    ensure_dir(staging)
    parent_memory_boundaries: list[dict[str, Any]] = []
    bucketizer_profile: dict[str, Any] = {}
    selector_runner_profile: dict[str, Any] = {}
    try:
        bucket_plan_path = staging / "_bucket-plan.json"
        bucketizer_work = _plan_bucketizer_work(candidate_paths, workers=workers)
        bucketizer_tasks = [
            TaskSpec(
                task_id=f"selector-bucketize-{index:04d}",
                partition_value=None,
                part_index=index,
                payload={
                    "candidate_work": work,
                    "staging": str(staging),
                    "bucket_plan_path": str(staging / "_bucket-plans" / f"shard-{index:04d}.json"),
                    "piece_root": str(staging / "_bucket-shards" / f"shard-{index:04d}"),
                    "partition_column": partition_column,
                    "selection_group_keys": selection_group_keys,
                    "bucket_count": bucket_count,
                    "__telemetry_phase_name": "build_sidecar.bucketize",
                    "__telemetry_phase_only": True,
                },
            )
            for index, work in enumerate(bucketizer_work)
        ]
        bucketizer_results, bucketizer_profile = run_tasks_in_subprocesses(
            bucketizer_tasks,
            worker=_selector_bucketize_worker,
            workers=min(max(1, int(workers)), len(bucketizer_tasks)),
            max_tasks_per_child=1,
            return_profile=True,
            telemetry_endpoint=telemetry_endpoint,
        )
        bucketizer_failure = next((item for item in bucketizer_results if not item.ok), None)
        if bucketizer_failure is not None:
            raise TaskExecutionError(
                f"0201 selector bucketizer failed: {bucketizer_failure.error_message}",
                context={
                    "task_id": bucketizer_failure.task_id,
                    "error_type": bucketizer_failure.error_type,
                    "traceback_tail": bucketizer_failure.traceback_tail,
                },
            )
        bucketizer_result = bucketizer_results[0]
        bucket_plan = _merge_bucketizer_plans(
            [Path(result.output_paths[0]) for result in bucketizer_results],
            staging=staging,
            bucket_count=bucket_count,
        )
        _atomic_write_json(bucket_plan_path, bucket_plan)
        bucket_entries = list(bucket_plan.get("buckets") or [])
        piece_index = int(bucket_plan.get("candidate_pieces") or 0)
        parent_memory_boundaries.append(_parent_memory_boundary("bucketize_finished"))

        tasks = [
            TaskSpec(
                task_id=f"selector-{index:06d}",
                partition_value=str(entry["partition_value"]),
                part_index=int(entry["bucket_id"]),
                payload={
                    "paths": [str(staging / path) for path in entry["paths"]],
                    "output_path": str(
                        (staging / str(entry["paths"][0])).parent / "active.parquet"
                    ),
                    "selection_group_keys": selection_group_keys,
                    "sort": sort,
                    "memory_budget_bytes": memory_budget_mb * 1024 * 1024,
                    "__telemetry_phase_name": "build_sidecar.selector_bucket",
                    "__telemetry_phase_only": True,
                },
            )
            for index, entry in enumerate(bucket_entries, start=1)
        ]
        tasks_per_child = max(1, math.ceil(len(tasks) / max(1, int(workers))))
        results, selector_runner_profile = run_tasks_in_subprocesses(
            tasks,
            worker=_active_selector_bucket_worker,
            workers=max(1, int(workers)),
            max_tasks_per_child=tasks_per_child,
            return_profile=True,
            telemetry_endpoint=telemetry_endpoint,
        )
        failed = [result for result in results if not result.ok]
        if failed:
            failure = failed[0]
            raise TaskExecutionError(
                f"0201 active selector bucket failed: {failure.error_message}",
                context={
                    "task_id": failure.task_id,
                    "error_type": failure.error_type,
                    "traceback_tail": failure.traceback_tail,
                },
            )
        parent_memory_boundaries.append(_parent_memory_boundary("selector_finished"))
        active_paths = [result.output_paths[0] for result in results]
        active_relative_paths = [path.relative_to(staging) for path in active_paths]
        active: pl.DataFrame | None = None
        if not return_piece_paths:
            active = (
                pl.concat(
                    [pl.scan_parquet(path) for path in active_paths], how="diagonal_relaxed"
                ).collect(engine="streaming")
                if active_paths
                else pl.read_parquet(candidate_paths[0]).head(0)
            )
        manifest = {
            "version": "smoking-data.selector-buckets.v1",
            "hash": "polars.hash_rows.seed0.v1",
            "bucket_count": bucket_count,
            "tasks": len(tasks),
            "candidate_pieces": piece_index,
        }
        (staging / "_selector.manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=True, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        moved_existing = False
        if root.exists():
            os.replace(root, backup)
            moved_existing = True
        try:
            os.replace(staging, root)
        except BaseException:
            if moved_existing and backup.exists() and not root.exists():
                os.replace(backup, root)
            raise
        reset_path(backup)
    except BaseException:
        reset_path(staging)
        raise

    total_groups = sum(int(item.counters.get("selector_groups", 0)) for item in results)
    total_rows = sum(int(item.counters.get("selector_input_rows", 0)) for item in results)
    selected: pl.DataFrame | list[Path] = (
        [root / path for path in active_relative_paths]
        if return_piece_paths
        else active
        if active is not None
        else pl.read_parquet(candidate_paths[0]).head(0)
    )
    return selected, {
        "mode": "hash_bucket_subprocess",
        "execution_mode": "bounded_process_pool",
        "hash_contract": "polars.hash_rows.seed0.v1",
        "bucket_count": bucket_count,
        "bucket_tasks": len(results),
        "candidate_pieces": piece_index,
        "candidate_rows": candidate_rows,
        "target_rows_per_bucket": SELECTOR_TARGET_ROWS_PER_BUCKET,
        "selector_tasks_per_child": tasks_per_child,
        "parent_memory_boundaries": parent_memory_boundaries,
        "bucketizer": {
            "pid": bucketizer_result.pid,
            "pids": sorted({result.pid for result in bucketizer_results if result.pid > 0}),
            "shards": len(bucketizer_results),
            "elapsed_sec": bucketizer_profile.get("total_elapsed_sec"),
            "peak_rss_mb": max(
                (float(result.counters.get("rss_peak_mb") or 0.0) for result in bucketizer_results),
                default=0.0,
            ),
            "runner": bucketizer_profile,
        },
        "selector_pids": sorted({result.pid for result in results if result.pid > 0}),
        "selector_runner": selector_runner_profile,
        "selector_key_cardinality": total_groups,
        "max_rows_per_selector_group": max(
            (int(item.counters.get("max_rows_per_selector_group", 0)) for item in results),
            default=0,
        ),
        "avg_rows_per_selector_group": total_rows / total_groups if total_groups else 0.0,
        "peak_rss_mb": max(
            (float(item.counters.get("rss_peak_mb", 0.0)) for item in results),
            default=0.0,
        ),
    }


def _plan_bucketizer_work(
    candidate_paths: list[Path], *, workers: int
) -> list[list[dict[str, Any]]]:
    """Balance immutable Parquet row groups across isolated bucketizer workers."""
    units: list[tuple[int, str, int]] = []
    for candidate_path in sorted(candidate_paths, key=lambda item: str(item)):
        parquet = pq.ParquetFile(candidate_path)
        for row_group in range(parquet.metadata.num_row_groups):
            metadata = parquet.metadata.row_group(row_group)
            estimated_bytes = max(1, int(metadata.total_byte_size or 0))
            units.append((estimated_bytes, str(candidate_path), row_group))
    if not units:
        return [[]]
    shard_count = min(max(1, int(workers)), len(units))
    shards: list[list[dict[str, Any]]] = [[] for _ in range(shard_count)]
    shard_bytes = [0] * shard_count
    for estimated_bytes, path, row_group in sorted(
        units, key=lambda item: (-item[0], item[1], item[2])
    ):
        shard_index = min(range(shard_count), key=lambda index: (shard_bytes[index], index))
        shards[shard_index].append(
            {
                "path": path,
                "row_group": row_group,
                "estimated_bytes": estimated_bytes,
            }
        )
        shard_bytes[shard_index] += estimated_bytes
    for shard in shards:
        shard.sort(key=lambda item: (str(item["path"]), int(item["row_group"])))
    return shards


def _merge_bucketizer_plans(
    plan_paths: list[Path], *, staging: Path, bucket_count: int
) -> dict[str, Any]:
    buckets: dict[tuple[str, int], list[str]] = {}
    candidate_pieces = 0
    for plan_path in sorted(plan_paths, key=lambda item: str(item)):
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        if plan.get("schema_version") != "smoking-data.selector-bucket-plan.v1":
            raise TaskExecutionError(
                "0201 bucketizer returned an unsupported plan.",
                context={"path": str(plan_path)},
            )
        candidate_pieces += int(plan.get("candidate_pieces") or 0)
        for entry in plan.get("buckets") or []:
            key = (str(entry["partition_value"]), int(entry["bucket_id"]))
            for raw_path in entry.get("paths") or []:
                relative_path = Path(str(raw_path))
                resolved_path = (staging / relative_path).resolve()
                if (
                    relative_path.is_absolute()
                    or ".." in relative_path.parts
                    or not resolved_path.is_file()
                    or staging.resolve() not in resolved_path.parents
                ):
                    raise TaskExecutionError(
                        "0201 bucketizer returned an invalid piece path.",
                        context={"path": str(relative_path), "plan_path": str(plan_path)},
                    )
                buckets.setdefault(key, []).append(str(relative_path))
    return {
        "schema_version": "smoking-data.selector-bucket-plan.v1",
        "path_contract": "staging_relative",
        "bucket_count": bucket_count,
        "candidate_pieces": candidate_pieces,
        "buckets": [
            {
                "partition_value": partition_value,
                "bucket_id": bucket_id,
                "paths": sorted(paths),
            }
            for (partition_value, bucket_id), paths in sorted(buckets.items())
        ],
    }


def _select_active_piece_dataset_in_subprocess(
    candidate_paths: list[Path],
    *,
    root: Path,
    partition_column: str,
    selection_group_keys: list[str],
    sort: list[dict[str, Any]],
    memory_budget_mb: int,
    candidate_rows: int,
    bucket_count: int,
    telemetry_endpoint: dict[str, Any] | None,
) -> tuple[list[Path], dict[str, Any]]:
    staging = root.parent / f".{root.name}.{os.getpid()}.tmp"
    backup = root.parent / f".{root.name}.{os.getpid()}.backup"
    reset_path(staging)
    reset_path(backup)
    ensure_dir(staging)
    request_path = staging / "_selector-piece.request.json"
    result_path = staging / "_selector-piece.result.json"
    try:
        _atomic_write_json(
            request_path,
            {
                "schema_version": SELECTOR_PIECE_REQUEST_VERSION,
                "staging": str(staging),
                "candidate_paths": [str(path) for path in candidate_paths],
                "partition_column": partition_column,
                "selection_group_keys": selection_group_keys,
                "sort": sort,
                "bucket_count": bucket_count,
                "memory_budget_bytes": memory_budget_mb * 1024 * 1024,
                "telemetry_endpoint": telemetry_endpoint,
            },
        )
        result = run_selector_piece_subprocess(request_path, result_path)
        active_relative_paths: list[Path] = []
        for entry in result.get("active_entries") or []:
            relative = Path(str(entry.get("path")))
            resolved = (staging / relative).resolve()
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or not resolved.is_file()
                or staging.resolve() not in resolved.parents
            ):
                raise TaskExecutionError(
                    "0201 selector-piece returned an invalid output path.",
                    context={"path": str(relative)},
                )
            active_relative_paths.append(relative)
        _atomic_write_json(
            staging / "_selector.manifest.json",
            {
                "version": "smoking-data.selector-buckets.v1",
                "hash": "polars.hash_rows.seed0.v1",
                "bucket_count": bucket_count,
                "tasks": int(result.get("bucket_tasks") or 0),
                "candidate_pieces": int(result.get("candidate_pieces") or 0),
            },
        )
        reset_path(request_path)
        reset_path(result_path)
        moved_existing = False
        if root.exists():
            os.replace(root, backup)
            moved_existing = True
        try:
            os.replace(staging, root)
        except BaseException:
            if moved_existing and backup.exists() and not root.exists():
                os.replace(backup, root)
            raise
        reset_path(backup)
    except BaseException:
        reset_path(staging)
        raise
    groups = int(result.get("selector_groups") or 0)
    rows = int(result.get("selector_input_rows") or 0)
    pid = int(result.get("pid") or 0)
    return [root / path for path in active_relative_paths], {
        "mode": "hash_bucket_subprocess",
        "execution_mode": "dedicated_selector_piece_subprocess",
        "hash_contract": "polars.hash_rows.seed0.v1",
        "bucket_count": bucket_count,
        "bucket_tasks": int(result.get("bucket_tasks") or 0),
        "candidate_pieces": int(result.get("candidate_pieces") or 0),
        "candidate_rows": candidate_rows,
        "target_rows_per_bucket": SELECTOR_TARGET_ROWS_PER_BUCKET,
        "selector_tasks_per_child": int(result.get("bucket_tasks") or 0),
        "parent_memory_boundaries": [_parent_memory_boundary("selector_piece_subprocess_finished")],
        "bucketizer": {
            "pid": pid,
            "elapsed_sec": result.get("bucketize_elapsed_sec"),
            "peak_rss_mb": result.get("peak_rss_mb"),
            "runner": {"mode": "dedicated_selector_piece_subprocess"},
        },
        "selector_pids": [pid] if pid > 0 else [],
        "selector_runner": {
            "mode": "dedicated_selector_piece_subprocess",
            "elapsed_sec": result.get("selector_elapsed_sec"),
            "io_read_bytes": result.get("io_read_bytes"),
            "io_write_bytes": result.get("io_write_bytes"),
        },
        "selector_key_cardinality": groups,
        "max_rows_per_selector_group": int(result.get("max_rows_per_selector_group") or 0),
        "avg_rows_per_selector_group": rows / groups if groups else 0.0,
        "peak_rss_mb": float(result.get("peak_rss_mb") or 0.0),
    }


def _build_active_sidecar_pipeline(
    candidate_paths: list[Path],
    *,
    root: Path,
    active_snapshot_path: Path,
    partition_column: str,
    selection_group_keys: list[str],
    sort: list[dict[str, Any]],
    group_keys: list[str],
    window_partitions: list[tuple[str, ...]],
    pivot: dict[str, Any],
    spill_aggregation: str | None,
    rows_per_part: int,
    memory_budget_mb: int,
    max_source_files_per_task: int,
    max_source_row_groups_per_task: int,
    telemetry_endpoint: dict[str, Any] | None,
    candidate_rows: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Select winner pieces and exec into the boundary planner in the same PID."""
    candidate_bytes = sum(path.stat().st_size for path in candidate_paths)
    target_bucket_bytes = max(8 * 1024 * 1024, memory_budget_mb * 1024 * 1024 // 4)
    bucket_count = max(
        1,
        min(1024, (candidate_bytes + target_bucket_bytes - 1) // target_bucket_bytes),
        min(
            1024,
            (candidate_rows + SELECTOR_TARGET_ROWS_PER_BUCKET - 1)
            // SELECTOR_TARGET_ROWS_PER_BUCKET,
        ),
    )
    staging = root.parent / f".{root.name}.{os.getpid()}.tmp"
    backup = root.parent / f".{root.name}.{os.getpid()}.backup"
    reset_path(staging)
    reset_path(backup)
    ensure_dir(staging)
    request_path = staging / "_selector-pipeline.request.json"
    result_path = staging / "_selector-pipeline.result.json"
    plan_path = active_snapshot_path.with_name(
        f"{active_snapshot_path.stem}.active-sidecar-plan.json"
    )
    try:
        _atomic_write_json(
            request_path,
            {
                "schema_version": SELECTOR_PIECE_REQUEST_VERSION,
                "staging": str(staging),
                "candidate_paths": [str(path) for path in candidate_paths],
                "partition_column": partition_column,
                "selection_group_keys": selection_group_keys,
                "sort": sort,
                "bucket_count": bucket_count,
                "memory_budget_bytes": memory_budget_mb * 1024 * 1024,
                "telemetry_endpoint": telemetry_endpoint,
                "active_sidecar_plan_continuation": {
                    "request": {
                        "schema_version": ACTIVE_SIDECAR_PLAN_REQUEST_VERSION,
                        "active_snapshot_path": str(active_snapshot_path),
                        "plan_path": str(plan_path),
                        "partition_column": partition_column,
                        "group_keys": group_keys,
                        "window_partitions": [list(item) for item in window_partitions],
                        "pivot": pivot,
                        "spill_aggregation": spill_aggregation,
                        "rows_per_part": rows_per_part,
                        "memory_budget_bytes": memory_budget_mb * 1024 * 1024,
                        "max_source_files_per_task": max_source_files_per_task,
                        "max_source_row_groups_per_task": max_source_row_groups_per_task,
                        "telemetry_endpoint": telemetry_endpoint,
                    }
                },
            },
        )
        worker = run_active_sidecar_pipeline_subprocess(request_path, result_path)
        selector = dict(worker.get("upstream_selector") or {})
        _atomic_write_json(
            staging / "_selector.manifest.json",
            {
                "version": "smoking-data.selector-buckets.v1",
                "hash": "polars.hash_rows.seed0.v1",
                "bucket_count": bucket_count,
                "tasks": int(selector.get("bucket_tasks") or 0),
                "candidate_pieces": int(selector.get("candidate_pieces") or 0),
            },
        )
        for path in (
            request_path,
            result_path,
            staging / "_selector-piece.completed.json",
            staging / "_active-sidecar-plan.request.json",
        ):
            reset_path(path)
        moved_existing = False
        if root.exists():
            os.replace(root, backup)
            moved_existing = True
        try:
            os.replace(staging, root)
        except BaseException:
            if moved_existing and backup.exists() and not root.exists():
                os.replace(backup, root)
            raise
        reset_path(backup)
    except BaseException:
        reset_path(staging)
        raise
    plan = load_active_sidecar_plan(plan_path)
    snapshot_relative = Path(str(plan["active_snapshot_path"]))
    resolved_snapshot = (plan_path.parent / snapshot_relative).resolve()
    if (
        snapshot_relative.is_absolute()
        or ".." in snapshot_relative.parts
        or not resolved_snapshot.exists()
    ):
        raise TaskExecutionError("0201 active-sidecar pipeline returned an invalid snapshot path.")
    normalized_plan = {
        **plan,
        "manifest_path": str(plan_path),
        "active_snapshot_path": str(resolved_snapshot),
        "worker": {
            "pid": worker.get("pid"),
            "elapsed_sec": worker.get("elapsed_sec"),
            "rss_mb": worker.get("rss_mb"),
            "peak_rss_mb": worker.get("peak_rss_mb"),
            "io_read_bytes": worker.get("io_read_bytes"),
            "io_write_bytes": worker.get("io_write_bytes"),
            "process_replacement": True,
        },
    }
    groups = int(selector.get("selector_groups") or 0)
    rows = int(selector.get("selector_input_rows") or 0)
    selector_pid = int(selector.get("pid") or 0)
    profile = {
        "mode": "hash_bucket_subprocess",
        "execution_mode": "selector_to_boundary_exec",
        "hash_contract": "polars.hash_rows.seed0.v1",
        "bucket_count": bucket_count,
        "bucket_tasks": int(selector.get("bucket_tasks") or 0),
        "candidate_pieces": int(selector.get("candidate_pieces") or 0),
        "candidate_rows": candidate_rows,
        "target_rows_per_bucket": SELECTOR_TARGET_ROWS_PER_BUCKET,
        "selector_tasks_per_child": int(selector.get("bucket_tasks") or 0),
        "parent_memory_boundaries": [_parent_memory_boundary("active_pipeline_finished")],
        "bucketizer": {
            "pid": selector_pid,
            "elapsed_sec": selector.get("bucketize_elapsed_sec"),
            "peak_rss_mb": selector.get("peak_rss_mb"),
            "runner": {"mode": "selector_to_boundary_exec"},
        },
        "selector_pids": [selector_pid] if selector_pid > 0 else [],
        "selector_runner": {
            "mode": "selector_to_boundary_exec",
            "elapsed_sec": selector.get("selector_elapsed_sec"),
            "io_read_bytes": selector.get("io_read_bytes"),
            "io_write_bytes": selector.get("io_write_bytes"),
        },
        "selector_key_cardinality": groups,
        "max_rows_per_selector_group": int(selector.get("max_rows_per_selector_group") or 0),
        "avg_rows_per_selector_group": rows / groups if groups else 0.0,
        "peak_rss_mb": float(selector.get("peak_rss_mb") or 0.0),
    }
    return normalized_plan, profile


def _selector_bucketize_worker(task: TaskSpec) -> TaskResult:
    payload = task.payload
    candidate_work = [
        {
            "path": Path(str(item["path"])),
            "row_group": int(item["row_group"]),
            "estimated_bytes": max(1, int(item.get("estimated_bytes") or 1)),
        }
        for item in payload.get("candidate_work") or []
    ]
    if not candidate_work:
        candidate_work = [
            {
                "path": candidate_path,
                "row_group": row_group,
                "estimated_bytes": max(
                    1,
                    int(parquet.metadata.row_group(row_group).total_byte_size or 0),
                ),
            }
            for candidate_path in [Path(str(item)) for item in payload["candidate_paths"]]
            for parquet in [pq.ParquetFile(candidate_path)]
            for row_group in range(parquet.metadata.num_row_groups)
        ]
    candidate_paths = sorted(
        {Path(str(item["path"])) for item in candidate_work}, key=lambda item: str(item)
    )
    staging = Path(str(payload["staging"]))
    piece_root = Path(str(payload.get("piece_root") or staging))
    bucket_plan_path = Path(str(payload["bucket_plan_path"]))
    partition_column = str(payload["partition_column"])
    selection_group_keys = [str(item) for item in payload["selection_group_keys"]]
    bucket_count = max(1, int(payload["bucket_count"]))
    total_row_groups = len(candidate_work)
    piece_index = 0
    bucket_files: dict[tuple[str, int], list[Path]] = {}
    opened_path: Path | None = None
    parquet: pq.ParquetFile | None = None
    for work in candidate_work:
        candidate_path = Path(str(work["path"]))
        row_group = int(work["row_group"])
        if parquet is None or candidate_path != opened_path:
            parquet = pq.ParquetFile(candidate_path)
            opened_path = candidate_path
        for batch in parquet.iter_batches(batch_size=65_536, row_groups=[row_group]):
            frame = pl.from_arrow(batch)
            if frame.is_empty():
                continue
            hashes = frame.select(selection_group_keys).hash_rows(seed=0)
            frame = frame.with_columns(
                (hashes % bucket_count).cast(pl.UInt32).alias("__selector_bucket")
            )
            for key, piece in frame.partition_by(
                [partition_column, "__selector_bucket"],
                as_dict=True,
                maintain_order=False,
            ).items():
                partition_value, bucket_value = key
                bucket_key = (str(partition_value), int(bucket_value))
                bucket_dir = ensure_dir(
                    piece_root
                    / partition_dir_name(partition_value)
                    / f"bucket-{int(bucket_value):05d}"
                )
                piece_path = bucket_dir / f"candidate-{piece_index:08d}.parquet"
                piece.drop("__selector_bucket").write_parquet(
                    piece_path,
                    compression="uncompressed",
                )
                bucket_files.setdefault(bucket_key, []).append(piece_path)
                piece_index += 1
    bucket_plan = {
        "schema_version": "smoking-data.selector-bucket-plan.v1",
        "path_contract": "staging_relative",
        "bucket_count": bucket_count,
        "candidate_pieces": piece_index,
        "buckets": [
            {
                "partition_value": partition_value,
                "bucket_id": bucket_id,
                "paths": [str(path.relative_to(staging)) for path in paths],
            }
            for (partition_value, bucket_id), paths in sorted(bucket_files.items())
        ],
    }
    _atomic_write_json(bucket_plan_path, bucket_plan)
    return TaskResult(
        task_id=task.task_id,
        ok=True,
        pid=os.getpid(),
        partition_value=task.partition_value,
        part_index=task.part_index,
        output_paths=[bucket_plan_path],
        counters={
            "candidate_files": len(candidate_paths),
            "candidate_bytes": sum(int(item["estimated_bytes"]) for item in candidate_work),
            "candidate_row_groups": total_row_groups,
            "candidate_pieces": piece_index,
            "selector_buckets": len(bucket_files),
        },
    )


def _active_selector_bucket_worker(task: TaskSpec) -> TaskResult:
    payload = task.payload
    paths = [Path(str(item)) for item in payload["paths"]]
    output_path = Path(str(payload["output_path"]))
    group_keys = [str(item) for item in payload["selection_group_keys"]]
    sort = list(payload.get("sort") or [])
    sort_columns = [str(item.get("column") or "") for item in sort]
    descending = [str(item.get("direction") or "asc").lower() == "desc" for item in sort]
    nulls_last = [str(item.get("nulls") or "last").lower() == "last" for item in sort]
    lf = pl.concat([pl.scan_parquet(path) for path in paths], how="diagonal_relaxed")
    ordered_columns = [
        *sort_columns,
        SOURCE_FILE_COLUMN,
        SOURCE_ROW_GROUP_COLUMN,
        SOURCE_ROW_INDEX_COLUMN,
    ]
    ordered_descending = [*descending, False, False, False]
    ordered_nulls_last = [*nulls_last, False, False, False]
    spill_used = sum(path.stat().st_size for path in paths) > int(payload["memory_budget_bytes"])
    if spill_used:
        spill = write_sorted_intermediate(
            lf,
            root=output_path.parent / "spill",
            sort_columns=ordered_columns,
            descending=ordered_descending,
            nulls_last=ordered_nulls_last,
        )
        with spill:
            selected = (
                pl.scan_parquet(spill.path)
                .group_by(group_keys, maintain_order=True)
                .agg(
                    pl.all().first(),
                    pl.len().alias("__smoking_data_selector_group_size"),
                )
                .collect(engine="streaming")
            )
    else:
        selected = (
            lf.sort(
                ordered_columns,
                descending=ordered_descending,
                nulls_last=ordered_nulls_last,
            )
            .group_by(group_keys, maintain_order=True)
            .agg(
                pl.all().first(),
                pl.len().alias("__smoking_data_selector_group_size"),
            )
            .collect(engine="streaming")
        )
    group_size = selected.get_column("__smoking_data_selector_group_size")
    selector_input_rows = int(group_size.sum() or 0)
    selector_groups = selected.height
    max_rows_per_selector_group = int(group_size.max() or 0)
    active = selected.drop("__smoking_data_selector_group_size")
    _atomic_write_parquet(active, output_path)
    return TaskResult(
        task_id=task.task_id,
        ok=True,
        pid=os.getpid(),
        partition_value=task.partition_value,
        part_index=task.part_index,
        output_paths=[output_path],
        counters={
            "selector_input_rows": selector_input_rows,
            "selector_groups": selector_groups,
            "max_rows_per_selector_group": max_rows_per_selector_group,
            "candidate_files": len(paths),
            "spill_used": int(spill_used),
        },
    )


def _read_previous_active_snapshot(
    path: Path,
    *,
    expected_columns: list[str],
) -> pl.DataFrame | None:
    if not path.exists() or (path.is_file() and path.stat().st_size == 0):
        return None
    try:
        frame = pl.read_parquet(path)
    except (OSError, pl.exceptions.PolarsError):
        return None
    required = {
        *expected_columns,
        SOURCE_FILE_COLUMN,
        SOURCE_ROW_GROUP_COLUMN,
        SOURCE_ROW_INDEX_COLUMN,
    }
    return frame if required.issubset(frame.columns) else None


def _compact_candidate_sidecars(
    candidate_paths: list[Path],
    *,
    output_root: Path,
    partition_column: str,
    target_bytes: int,
) -> tuple[list[Path], dict[str, Any]]:
    input_bytes = sum(path.stat().st_size for path in candidate_paths)
    if input_bytes <= target_bytes and len(candidate_paths) <= 16:
        return candidate_paths, {
            "target_bytes": target_bytes,
            "input_files": len(candidate_paths),
            "output_files": len(candidate_paths),
            "output_bytes": input_bytes,
            "partition_boundaries_preserved": False,
            "atomic_replace": False,
            "skipped": True,
            "skip_reason": "below_target_and_file_count_threshold",
        }
    staging = output_root.parent / f".{output_root.name}.{os.getpid()}.tmp"
    backup = output_root.parent / f".{output_root.name}.{os.getpid()}.backup"
    reset_path(staging)
    reset_path(backup)
    ensure_dir(staging)
    writers: dict[str, dict[str, Any]] = {}
    output_paths: list[Path] = []

    def close_writer(state: dict[str, Any]) -> None:
        writer = state.pop("writer", None)
        if writer is not None:
            writer.close()

    try:
        for candidate_path in candidate_paths:
            frame = pl.read_parquet(candidate_path)
            if partition_column not in frame.columns:
                raise ValidationError(
                    f"Candidate sidecar is missing partition column {partition_column!r}: {candidate_path}"
                )
            for key, part in frame.partition_by(
                partition_column, as_dict=True, maintain_order=True
            ).items():
                partition_value = key[0] if isinstance(key, tuple) else key
                partition_name = partition_dir_name(partition_value)
                estimated_bytes = max(1, int(part.estimated_size()))
                state = writers.get(partition_name)
                if (
                    state is not None
                    and state["bytes"]
                    and (state["bytes"] + estimated_bytes > target_bytes)
                ):
                    close_writer(state)
                if state is None or "writer" not in state:
                    part_index = 0 if state is None else int(state["part_index"]) + 1
                    partition_root = ensure_dir(staging / partition_name)
                    path = partition_root / part_file_name(part_index)
                    table = part.to_arrow()
                    state = {
                        "writer": pq.ParquetWriter(
                            path,
                            table.schema,
                            compression=None,
                        ),
                        "part_index": part_index,
                        "bytes": 0,
                        "path": path,
                    }
                    writers[partition_name] = state
                    output_paths.append(path)
                else:
                    table = part.to_arrow()
                state["writer"].write_table(table)
                state["bytes"] += estimated_bytes
        for state in writers.values():
            close_writer(state)
        if not output_paths:
            # Keep the selector schema available even when every source row was
            # filtered out; downstream empty-dataset handling still needs it.
            empty_path = staging / part_file_name(0)
            shutil.copy2(candidate_paths[0], empty_path)
            output_paths.append(empty_path)
        moved_existing = False
        if output_root.exists():
            os.replace(output_root, backup)
            moved_existing = True
        try:
            os.replace(staging, output_root)
        except BaseException:
            if moved_existing and backup.exists() and not output_root.exists():
                os.replace(backup, output_root)
            raise
        reset_path(backup)
    except BaseException:
        for state in writers.values():
            close_writer(state)
        reset_path(staging)
        raise

    compacted = sorted(output_root.rglob("*.parquet"))
    return compacted, {
        "target_bytes": target_bytes,
        "input_files": len(candidate_paths),
        "output_files": len(compacted),
        "output_bytes": sum(path.stat().st_size for path in compacted),
        "partition_boundaries_preserved": True,
        "atomic_replace": True,
        "skipped": False,
    }


def _build_source_candidate_sidecar(
    source_path: Path,
    *,
    selector_columns: list[str],
    reserved_columns: set[str],
    planner_payload: dict[str, Any],
    project_root: Path,
    average_payload_bytes: int,
) -> pl.DataFrame:
    raw_lf = pl.scan_parquet(source_path)
    source_names = raw_lf.collect_schema().names()
    collisions = reserved_columns.intersection(source_names)
    if collisions:
        raise ValidationError(f"0201 source uses reserved coordinate columns: {sorted(collisions)}")
    if SNAPSHOT_PARTITION_COLUMN in selector_columns:
        if SNAPSHOT_PARTITION_COLUMN in source_names:
            raise ValidationError(
                f"0401 source uses reserved snapshot column: {SNAPSHOT_PARTITION_COLUMN}"
            )
        raw_lf = raw_lf.with_columns(
            pl.lit("snapshot", dtype=pl.String).alias(SNAPSHOT_PARTITION_COLUMN)
        )
    transformed = _apply_payload_lf(
        raw_lf.with_row_index(SOURCE_ROW_INDEX_COLUMN),
        payload=planner_payload,
        project_root=project_root,
        retained_columns=[SOURCE_ROW_INDEX_COLUMN],
    )
    thin = transformed.select([*selector_columns, SOURCE_ROW_INDEX_COLUMN]).collect()
    if thin.get_column(SOURCE_ROW_INDEX_COLUMN).n_unique() != thin.height:
        raise ValidationError(
            "0201 payload transformations multiplied source rows; "
            f"reference mappings must be unique: {source_path}"
        )
    return attach_row_group_ids(thin, source_path).with_columns(
        pl.lit(average_payload_bytes, dtype=pl.Int64).alias(ESTIMATED_PAYLOAD_BYTES_COLUMN)
    )


def _candidate_sidecar_task_worker(task: TaskSpec) -> TaskResult:
    payload = task.payload
    source_path = Path(str(payload["source_path"]))
    candidate_path = Path(str(payload["candidate_path"]))
    thin = _build_source_candidate_sidecar(
        source_path,
        selector_columns=[str(item) for item in payload["selector_columns"]],
        reserved_columns={str(item) for item in payload["reserved_columns"]},
        planner_payload=dict(payload["planner_payload"]),
        project_root=Path(str(payload["project_root"])),
        average_payload_bytes=int(payload["average_payload_bytes"]),
    )
    _atomic_write_parquet(thin, candidate_path)
    return TaskResult(
        task_id=task.task_id,
        ok=True,
        pid=os.getpid(),
        output_paths=[candidate_path],
        counters={
            "candidate_rows": thin.height,
            "candidate_bytes": candidate_path.stat().st_size,
            "source_files_touched": 1,
            "row_groups_touched": pq.ParquetFile(source_path).metadata.num_row_groups,
        },
    )


def _read_candidate_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    backup_path = path.with_name(f".{path.name}.{os.getpid()}.backup")
    moved_existing = False
    try:
        frame.write_parquet(temp_path, compression="uncompressed")
        if path.exists():
            os.replace(path, backup_path)
            moved_existing = True
        try:
            os.replace(temp_path, path)
        except BaseException:
            if moved_existing and backup_path.exists() and not path.exists():
                os.replace(backup_path, path)
            raise
        reset_path(backup_path)
    finally:
        reset_path(temp_path)


def _parent_memory_boundary(
    boundary: str,
    *,
    active_snapshot_estimated_mb: float | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "boundary": boundary,
        "timestamp_ns": time.time_ns(),
        "pid": os.getpid(),
        "rss_mb": current_rss_mb(),
        "peak_rss_mb": peak_rss_mb(),
    }
    if active_snapshot_estimated_mb is not None:
        payload["active_snapshot_estimated_mb"] = round(active_snapshot_estimated_mb, 3)
    return payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temp_path, path)
    finally:
        reset_path(temp_path)


def _planner_add_calc_configs(
    configs: Any,
    *,
    expression_ir: dict[str, Any] | None,
    selector_columns: list[str],
) -> list[dict[str, Any]]:
    items = [item for item in (configs or []) if isinstance(item, dict)]
    if not items or expression_ir is None:
        return []
    dependencies: dict[str, set[str]] = {}
    for layer in expression_ir.get("layers") or []:
        for expression in layer.get("expressions") or []:
            name = str(expression.get("name") or "")
            dependencies[name] = {str(item) for item in expression.get("dependencies") or []}
    required = set(selector_columns).intersection(dependencies)
    pending = list(required)
    while pending:
        name = pending.pop()
        for dependency in dependencies.get(name, set()):
            if dependency not in required:
                required.add(dependency)
                pending.append(dependency)
    return [item for item in items if str(item.get("name") or "") in required]


def _write_active_coordinate_snapshot(
    active_snapshot: pl.DataFrame,
    *,
    output_path: Path,
    partition_column: str,
) -> tuple[Path, list[TaskSpec]]:
    ensure_dir(output_path.parent)
    _atomic_write_parquet(active_snapshot, output_path)
    coordinate_root = output_path.parent / "parts"
    coordinate_staging = output_path.parent / f".parts.{os.getpid()}.tmp"
    reset_path(coordinate_staging)
    ensure_dir(coordinate_staging)
    tasks: list[TaskSpec] = []
    coordinate_columns = [
        SOURCE_FILE_COLUMN,
        SOURCE_ROW_GROUP_COLUMN,
        SOURCE_ROW_INDEX_COLUMN,
        ACTIVE_ORDER_COLUMN,
    ]
    task_keys = (
        active_snapshot.select([partition_column, PART_INDEX_COLUMN])
        .unique()
        .sort([partition_column, PART_INDEX_COLUMN])
    )
    for task_key in task_keys.iter_rows(named=True):
        partition_value = str(task_key[partition_column])
        part_index = int(task_key[PART_INDEX_COLUMN])
        coordinates = active_snapshot.filter(
            (pl.col(partition_column).cast(pl.String) == partition_value)
            & (pl.col(PART_INDEX_COLUMN) == part_index)
        ).select(coordinate_columns)
        coordinate_path = ensure_dir(
            coordinate_staging / partition_dir_name(partition_value)
        ) / part_file_name(part_index, suffix=".coordinates.parquet")
        coordinates.write_parquet(coordinate_path, compression="uncompressed")
        rust_coordinate_path = coordinate_path.with_suffix(".arrow")
        write_rust_coordinate_file(coordinates, rust_coordinate_path)
        tasks.append(
            TaskSpec(
                task_id=task_id(partition_value, part_index),
                partition_value=partition_value,
                part_index=part_index,
                payload={
                    "coordinate_path": str(coordinate_path),
                    "rust_coordinate_path": str(rust_coordinate_path),
                },
            )
        )
    reset_path(coordinate_root)
    os.replace(coordinate_staging, coordinate_root)
    tasks = [
        replace(
            task,
            payload={
                **task.payload,
                "coordinate_path": str(
                    coordinate_root
                    / Path(str(task.payload["coordinate_path"])).relative_to(coordinate_staging)
                ),
                "rust_coordinate_path": str(
                    coordinate_root
                    / Path(str(task.payload["rust_coordinate_path"])).relative_to(
                        coordinate_staging
                    )
                ),
            },
        )
        for task in tasks
    ]
    return output_path, tasks


def _write_curated_part_spill_fallback(
    coordinates: pl.DataFrame,
    *,
    output_dir: Path,
    partition_value: str,
    part_index: int,
    task_id: str,
    payload: dict[str, Any],
    list_restore: dict[str, Any],
    pivot: dict[str, Any],
    output_row_group_rows: int,
    project_root: Path,
    chunk_rows: int,
    memory_budget_mb: int,
    estimated_spill_bytes: int,
    recovery_root: Path,
    merge_aggregation: str,
) -> tuple[list[Path], int, dict[str, float]]:
    if merge_aggregation not in {"sum", "count", "min", "max"}:
        raise ValidationError(
            "Pivot spill requires one mergeable aggregation.",
            code="physical_plan.spill_unsupported_pivot",
            context={"aggregation": merge_aggregation, "task_id": task_id},
        )
    task_recovery_root = recovery_root / partition_dir_name(task_id)
    spill_root = task_recovery_root / "work"
    recovery_path = task_recovery_root / "recovery.json"
    reset_path(spill_root)
    ensure_dir(spill_root)
    required_bytes = max(64 * 1024 * 1024, max(1, estimated_spill_bytes) * 2)
    reserve_bytes = 256 * 1024 * 1024
    free_bytes = shutil.disk_usage(task_recovery_root).free
    if free_bytes < required_bytes + reserve_bytes:
        reset_path(spill_root)
        raise ValidationError(
            "Insufficient SSD space for pivot spill fallback.",
            code="physical_plan.spill_space_insufficient",
            context={
                "task_id": task_id,
                "free_bytes": free_bytes,
                "required_bytes": required_bytes,
                "reserve_bytes": reserve_bytes,
            },
        )

    partial_paths: list[Path] = []
    combined_stats: dict[str, float] = {}
    started = time.perf_counter()
    try:
        for chunk_index, offset in enumerate(range(0, coordinates.height, max(1, int(chunk_rows)))):
            chunk = coordinates.slice(offset, max(1, int(chunk_rows)))
            coordinate_path = spill_root / "coordinates" / f"chunk-{chunk_index:05d}.arrow"
            write_rust_coordinate_file(chunk, coordinate_path)
            paths, _, stats = _write_curated_part_rust_direct(
                chunk,
                rust_coordinate_path=coordinate_path,
                output_dir=spill_root / "partials",
                partition_value="partial",
                part_index=chunk_index,
                payload={**payload, "compression": "uncompressed"},
                list_restore=list_restore,
                pivot=pivot,
                output_row_group_rows=max(1, min(output_row_group_rows, chunk.height)),
                project_root=project_root,
            )
            partial_paths.extend(paths)
            for name, value in stats.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    combined_stats[name] = combined_stats.get(name, 0.0) + float(value)

        if not partial_paths:
            raise RuntimeError(f"Pivot spill produced no partial outputs: task={task_id}")
        row_keys = [str(item) for item in (pivot.get("row_keys") or [])]
        partial_merge_aggregation = "sum" if merge_aggregation == "count" else merge_aggregation
        spill_partial_bytes = sum(path.stat().st_size for path in partial_paths)
        spill_chunks = len(partial_paths)
        merge_round = 0
        while len(partial_paths) > 1:
            next_paths: list[Path] = []
            for pair_index in range(0, len(partial_paths), 2):
                pair = partial_paths[pair_index : pair_index + 2]
                if len(pair) == 1:
                    next_paths.append(pair[0])
                    continue
                frames = [pl.read_parquet(path) for path in pair]
                column_order = list(
                    dict.fromkeys(column for frame in frames for column in frame.columns)
                )
                value_columns = [column for column in column_order if column not in row_keys]
                combined = pl.concat(frames, how="diagonal_relaxed")
                aggregate = getattr(pl.col(value_columns), partial_merge_aggregation)()
                merged_pair = (
                    combined.group_by(row_keys, maintain_order=True).agg(aggregate)
                    if row_keys
                    else combined.select(aggregate)
                ).select(column_order)
                merged_path = (
                    spill_root
                    / "merge"
                    / f"round-{merge_round:03d}-part-{pair_index // 2:05d}.parquet"
                )
                _atomic_write_parquet(merged_pair, merged_path)
                spill_partial_bytes += merged_path.stat().st_size
                for path in pair:
                    path.unlink(missing_ok=True)
                next_paths.append(merged_path)
            partial_paths = next_paths
            merge_round += 1
        merged = pl.read_parquet(partial_paths[0])

        partition_dir = ensure_dir(output_dir / partition_dir_name(partition_value))
        output_path = partition_dir / part_file_name(part_index)
        temporary = output_path.with_name(f".{output_path.name}.{os.getpid()}.spill.tmp")
        merged.write_parquet(
            temporary,
            compression=str(payload.get("compression") or "zstd"),
            row_group_size=max(1, int(output_row_group_rows)),
        )
        os.replace(temporary, output_path)
        stats = {
            **combined_stats,
            "rows_written": float(merged.height),
            "spill_enabled": 1.0,
            "spill_chunks": float(spill_chunks),
            "spill_merge_rounds": float(merge_round),
            "spill_partial_bytes": float(spill_partial_bytes),
            "spill_elapsed_sec": time.perf_counter() - started,
            "spill_chunk_rows": float(max(1, int(chunk_rows))),
        }
        reset_path(task_recovery_root)
        return [output_path], merged.height, stats
    except BaseException as error:
        _atomic_write_json(
            recovery_path,
            {
                "schema_version": "smoking-data.pivot-spill-recovery.v1",
                "task_id": task_id,
                "spill_root": str(spill_root),
                "partial_outputs": [str(path) for path in partial_paths],
                "error_type": type(error).__name__,
                "error_message": str(error),
            },
        )
        raise


def _write_curated_part_rust_direct(
    coordinates: pl.DataFrame,
    *,
    rust_coordinate_path: Path,
    output_dir: Path,
    partition_value: str,
    part_index: int,
    payload: dict[str, Any],
    list_restore: dict[str, Any],
    pivot: dict[str, Any],
    output_row_group_rows: int,
    project_root: Path,
) -> tuple[list[Path], int, dict[str, float]]:
    partition_dir = ensure_dir(output_dir / partition_dir_name(partition_value))
    output_path = partition_dir / part_file_name(part_index)
    include_columns = list(payload.get("include_columns") or [])
    pivot_enabled = bool(pivot.get("enabled", False))
    pivot_required_columns = list(
        dict.fromkeys(
            [
                *[str(item) for item in (pivot.get("row_keys") or [])],
                *[str(item) for item in (pivot.get("column_keys") or [])],
                *[
                    str(item.get("source_column") or "")
                    for item in [
                        *(pivot.get("value_keys") or []),
                        *(pivot.get("value_keys_without_column") or []),
                    ]
                    if isinstance(item, dict)
                ],
            ]
        )
    )
    resolved_writer_input_contract = _resolve_writer_input_contract(payload=payload, pivot=pivot)
    pre_pivot_output_columns = list(resolved_writer_input_contract.get("output_columns") or [])
    if not pre_pivot_output_columns:
        pre_pivot_output_columns = list(include_columns)
        if pivot_enabled:
            pre_pivot_output_columns.extend(
                column
                for column in pivot_required_columns
                if column not in pre_pivot_output_columns
            )
    expression_ir = payload.get("expression_ir")
    post_operations = list(payload.get("post_operations") or [])
    pre_pivot_operations = list(payload.get("pre_pivot_operations") or [])
    if bool(payload.get("final_post_projection", False)):
        pre_pivot_output_columns = []
    generated_columns = {
        str(item.get("name") or "")
        for item in (payload.get("add_calc") or [])
        if isinstance(item, dict)
    }
    restore_enabled = bool(list_restore.get("enabled", False))
    resolved_lookup_path = (
        resolve_project_path(str(list_restore["lookup_path"]), project_root=project_root)
        if restore_enabled
        else None
    )
    schema = {
        str(item.get("name") or item.get("column")): _rust_schema_type(str(item["type"]))
        for item in (payload.get("type_casts") or [])
    }
    if restore_enabled:
        schema.update(
            _resolve_list_restore_schema(
                list_restore,
                coordinates=coordinates,
                lookup_path=resolved_lookup_path,
                source_stats=payload.get("source_stats"),
            )
        )
    reference_configs = payload.get("reference_replace") or []
    if isinstance(reference_configs, dict):
        reference_configs = [reference_configs]
    resolved_reference_configs = [
        {
            **dict(item),
            "reference_parquet": str(
                resolve_project_path(str(item["reference_parquet"]), project_root=project_root)
            ),
        }
        for item in reference_configs
        if isinstance(item, dict) and bool(item.get("enabled", True))
    ]
    generated_columns.update(
        str(item.get("output_column") or item.get("source_column") or "")
        for item in resolved_reference_configs
    )
    restore_config = dict(list_restore.get("config") or {})
    restore_config["enabled"] = restore_enabled
    required_source_columns = {
        str(column) for column in include_columns if str(column) not in generated_columns
    }
    required_source_columns.update(_expression_ir_source_columns(expression_ir) - generated_columns)
    required_source_columns.update(
        column for column in pivot_required_columns if column not in generated_columns
    )
    required_source_columns.update(
        str(item["source_column"]) for item in resolved_reference_configs
    )
    required_source_columns.update(
        _post_operation_source_columns(post_operations) - generated_columns
    )
    required_source_columns.update(
        _post_operation_source_columns(pre_pivot_operations) - generated_columns
    )
    post_generated_columns = _post_operation_output_columns(
        [*pre_pivot_operations, *post_operations]
    )
    required_source_columns.update(
        str(column)
        for column in payload.get("source_projection_columns_hint") or []
        if str(column) not in generated_columns and str(column) not in post_generated_columns
    )
    if restore_enabled:
        required_source_columns.update(
            [
                str(restore_config.get("key_column") or ""),
                *[str(item) for item in restore_config.get("value_columns") or []],
                *[str(item) for item in restore_config.get("source_coord_columns") or []],
            ]
        )
    pre_rename_mapping: dict[str, str] = {}
    for operation in pre_pivot_operations:
        if str(operation.get("kind") or "") != "rename_columns":
            continue
        mapping = (operation.get("config") or {}).get("resolved_mapping") or {}
        if isinstance(mapping, dict):
            pre_rename_mapping.update(
                {str(source): str(target) for source, target in mapping.items()}
            )
    for source_column, target_column in pre_rename_mapping.items():
        if source_column in generated_columns:
            required_source_columns.discard(target_column)
        elif target_column in required_source_columns:
            required_source_columns.discard(target_column)
            required_source_columns.add(source_column)
    required_source_columns.discard("")
    projection_columns = [
        {"name": pre_rename_mapping.get(column, column), "source": column}
        for column in sorted(required_source_columns)
    ]
    rust_stats = execute_curated_task(
        CuratedTaskRequest(
            coordinate_path=rust_coordinate_path,
            output_dir=partition_dir,
            output_file_name=output_path.name,
            single_partition_guaranteed=True,
            writer_input_contract=resolved_writer_input_contract or None,
            projection_columns=projection_columns,
            schema=schema,
            expression_ir=expression_ir,
            output_columns=pre_pivot_output_columns,
            lookup_path=resolved_lookup_path,
            restore_config=restore_config,
            reference_replace=resolved_reference_configs,
            pivot=pivot if pivot_enabled else None,
            pre_pivot_operations=pre_pivot_operations,
            post_operations=post_operations,
            ordered_operations=list(payload.get("ordered_operations") or []),
            compression=str(payload.get("compression") or "zstd"),
            output_row_group_rows=output_row_group_rows,
            batch_size=list_restore.get("batch_size"),
            drop_cache_hint=bool(list_restore.get("drop_cache_hint", False)),
            print_timing=bool(list_restore.get("print_timing", False)),
        )
    )
    output_files = [output_path] if output_path.is_file() else []
    if not output_files:
        raise RuntimeError(f"0201 Rust coordinate writer did not create output: {output_path}")
    for path in output_files:
        if path.stat().st_size <= 0:
            raise RuntimeError(f"0201 Rust coordinate writer created empty output: {path}")
    row_count = sum(int(pq.ParquetFile(path).metadata.num_rows) for path in output_files)
    expected_rows = int(rust_stats.get("rows_written", coordinates.height))
    if row_count != expected_rows:
        raise RuntimeError(
            f"0201 Rust coordinate writer row mismatch: expected={expected_rows}, actual={row_count}"
        )
    return output_files, row_count, rust_stats


def _post_operation_source_columns(operations: list[dict[str, Any]]) -> set[str]:
    columns: set[str] = set()
    aliases: dict[str, str] = {}

    def source_name(name: str) -> str:
        while name in aliases:
            name = aliases[name]
        return name

    for operation in operations:
        if not isinstance(operation, dict):
            continue
        kind = str(operation.get("kind") or "")
        config = operation.get("config") or {}
        if not isinstance(config, dict):
            continue
        if kind in {"include_columns", "exclude_columns"}:
            columns.update(source_name(str(item)) for item in config.get("columns") or [])
        elif kind == "rename_columns":
            mapping = config.get("resolved_mapping") or config.get("mapping") or {}
            if isinstance(mapping, dict):
                for source, target in mapping.items():
                    columns.add(source_name(str(source)))
                    aliases[str(target)] = source_name(str(source))
        elif kind == "unpivot":
            columns.update(source_name(str(item)) for item in config.get("id_columns") or [])
            columns.update(source_name(str(item)) for item in config.get("value_columns") or [])
        elif kind == "unnest":
            columns.update(
                source_name(str(item.get("source") or ""))
                for item in config.get("columns") or []
                if isinstance(item, dict)
            )
        elif kind == "data_assertion":
            for rule in config.get("rules") or []:
                if not isinstance(rule, dict):
                    continue
                columns.update(source_name(str(item)) for item in rule.get("columns") or [])
                if rule.get("column"):
                    columns.add(source_name(str(rule["column"])))
    columns.discard("")
    return columns


def _post_operation_output_columns(operations: list[dict[str, Any]]) -> set[str]:
    columns: set[str] = set()
    for operation in operations:
        if not isinstance(operation, dict):
            continue
        kind = str(operation.get("kind") or "")
        config = operation.get("config") or {}
        if not isinstance(config, dict):
            continue
        if kind == "rename_columns":
            mapping = config.get("resolved_mapping") or config.get("mapping") or {}
            if isinstance(mapping, dict):
                columns.update(str(item) for item in mapping.values())
        elif kind == "unpivot":
            columns.update(
                {
                    str(config.get("name_column") or ""),
                    str(config.get("value_column") or ""),
                }
            )
        elif kind == "unnest":
            columns.update(
                str(item.get("output") or item.get("source") or "")
                for item in config.get("columns") or []
                if isinstance(item, dict)
            )
    columns.discard("")
    return columns


def _expression_ir_source_columns(document: dict[str, Any] | None) -> set[str]:
    columns: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("kind") == "column" and value.get("name"):
                columns.add(str(value["name"]))
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)
    return columns


def _expression_ir_window_partitions(
    document: dict[str, Any] | None,
) -> list[tuple[str, ...]]:
    partitions: list[tuple[str, ...]] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("kind") == "window":
                keys: list[str] = []
                for node in value.get("partition_by") or []:
                    if not isinstance(node, dict) or node.get("kind") != "column":
                        raise ValidationError(
                            "Rust window planner requires column-only partition keys."
                        )
                    keys.append(str(node["name"]))
                partitions.append(tuple(keys))
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)
    return list(dict.fromkeys(partitions))


def _validate_window_task_boundaries(
    active: pl.DataFrame,
    *,
    partition_column: str,
    window_partitions: list[tuple[str, ...]],
) -> list[str] | None:
    if not window_partitions:
        return None
    if len(window_partitions) > 1:
        raise ValidationError(
            "Rust window planner requires one shared partition-key contract per task; "
            f"found={window_partitions}."
        )
    keys = list(window_partitions[0])
    if not keys:
        if active.get_column(partition_column).n_unique() > 1:
            raise ValidationError(
                "Global OVER() crosses output partitions; add partition keys or use a future "
                "stateful aggregation phase."
            )
        return []
    cross_partition = (
        active.group_by(keys)
        .agg(pl.col(partition_column).n_unique().alias("__partition_count"))
        .filter(pl.col("__partition_count") > 1)
        .limit(1)
    )
    if cross_partition.height:
        raise ValidationError(
            "Window group crosses output partitions and cannot be split across Rust tasks: "
            f"window_keys={keys}."
        )
    return keys


def _task_complete_group_keys(
    active: pl.DataFrame,
    *,
    partition_column: str,
    window_keys: list[str] | None,
    pivot_row_keys: list[str] | None,
) -> list[str] | None:
    contracts = [
        list(dict.fromkeys(keys)) for keys in (window_keys, pivot_row_keys) if keys is not None
    ]
    if not contracts:
        return None
    for keys in contracts:
        if not keys:
            if active.get_column(partition_column).n_unique() > 1:
                raise ValidationError(
                    "A global complete-group operation crosses output partitions."
                )
            return []
        missing = [column for column in keys if column not in active.columns]
        if missing:
            raise ValidationError(f"Complete-group key columns are missing: {missing}")
        cross_partition = (
            active.group_by(keys)
            .agg(pl.col(partition_column).n_unique().alias("__partition_count"))
            .filter(pl.col("__partition_count") > 1)
            .limit(1)
        )
        if cross_partition.height:
            raise ValidationError(
                "Complete-group keys cross output partitions: "
                f"keys={keys}, partition={partition_column}."
            )
    chosen = min(contracts, key=len)
    chosen_set = set(chosen)
    incompatible = [keys for keys in contracts if not chosen_set.issubset(keys)]
    if incompatible:
        raise ValidationError(
            "Window and pivot complete-group keys are not nested; an intermediate barrier "
            f"is required: contracts={contracts}."
        )
    return chosen


def _coordinate_boundary_fanout_profile(
    frame: pl.DataFrame,
    *,
    partition_column: str,
    max_source_files: int,
    max_source_row_groups: int,
) -> dict[str, Any]:
    task_fanout = (
        frame.group_by([partition_column, PART_INDEX_COLUMN])
        .agg(
            pl.len().alias("rows"),
            pl.col(SOURCE_FILE_COLUMN).n_unique().alias("source_files"),
            pl.struct([SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN])
            .n_unique()
            .alias("source_row_groups"),
        )
        .sort([partition_column, PART_INDEX_COLUMN])
    )
    file_values = [int(value) for value in task_fanout["source_files"].to_list()]
    row_group_values = [int(value) for value in task_fanout["source_row_groups"].to_list()]
    return {
        "schema_version": "smoking-data.coordinate-boundary-fanout.v1",
        "tasks": task_fanout.height,
        "limits": {
            "max_source_files_per_task": int(max_source_files),
            "max_source_row_groups_per_task": int(max_source_row_groups),
        },
        "source_files_per_task": _integer_distribution(file_values),
        "source_row_groups_per_task": _integer_distribution(row_group_values),
        "unsplittable_complete_group_tasks": {
            "source_files_over_limit": sum(value > max_source_files for value in file_values),
            "source_row_groups_over_limit": sum(
                value > max_source_row_groups for value in row_group_values
            ),
        },
        "complete_group_split_allowed": False,
    }


def _integer_distribution(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {"count": 0, "min": None, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)

    def nearest(ratio: float) -> int:
        index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * ratio)))
        return ordered[index]

    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": nearest(0.50),
        "p95": nearest(0.95),
        "max": ordered[-1],
    }


def _pivot_spill_aggregation(
    pivot: dict[str, Any],
    *,
    payload: dict[str, Any],
) -> str | None:
    if not bool(pivot.get("enabled", False)) or payload.get("post_operations"):
        return None
    specs = [
        item
        for item in [
            *(pivot.get("value_keys") or []),
            *(pivot.get("value_keys_without_column") or []),
        ]
        if isinstance(item, dict)
    ]
    aggregations = {str(item.get("aggregation") or "first").strip().lower() for item in specs}
    if len(aggregations) != 1:
        return None
    aggregation = next(iter(aggregations))
    return aggregation if aggregation in {"sum", "count", "min", "max"} else None


def _assign_window_safe_part_indices(
    frame: pl.DataFrame,
    *,
    window_keys: list[str] | None,
    barrier_state: BarrierState,
    rows_per_part: int,
    max_payload_bytes: int,
    max_source_files: int,
    max_source_row_groups: int,
    allow_oversized_group_spill: bool,
) -> pl.DataFrame:
    if window_keys == []:
        payload_bytes = int(frame.get_column(ESTIMATED_PAYLOAD_BYTES_COLUMN).sum() or 0)
        spill_required = payload_bytes > max_payload_bytes and allow_oversized_group_spill
        if not spill_required:
            ensure_complete_group_within_budget(
                state=barrier_state,
                group_key={"global": True},
                estimated_bytes=payload_bytes,
                budget_bytes=max_payload_bytes,
                rows=frame.height,
            )
        return frame.with_columns(
            pl.lit(0, dtype=pl.Int64).alias(PART_INDEX_COLUMN),
            pl.lit(spill_required).alias(SPILL_REQUIRED_COLUMN),
        )
    if window_keys is None:
        return _assign_row_safe_part_indices(
            frame,
            rows_per_part=rows_per_part,
            max_payload_bytes=max_payload_bytes,
            max_source_files=max_source_files,
            max_source_row_groups=max_source_row_groups,
        )
    mapping_keys = [ACTIVE_ORDER_COLUMN] if window_keys is None else list(window_keys)
    groups = frame.group_by(mapping_keys, maintain_order=True).agg(
        pl.len().alias("__group_rows"),
        pl.col(ESTIMATED_PAYLOAD_BYTES_COLUMN).sum().alias("__group_payload_bytes"),
        pl.col(SOURCE_FILE_COLUMN).unique().alias("__group_source_files"),
        pl.struct([SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN])
        .unique()
        .alias("__group_source_row_groups"),
    )
    part_indices: list[int] = []
    spill_flags: list[bool] = []
    part_index = 0
    current_rows = 0
    current_bytes = 0
    current_files: set[str] = set()
    current_row_groups: set[tuple[str, int]] = set()
    for group in groups.iter_rows(named=True):
        size = int(group["__group_rows"])
        payload_bytes = int(group["__group_payload_bytes"] or 0)
        spill_required = payload_bytes > max_payload_bytes and allow_oversized_group_spill
        if not spill_required:
            ensure_complete_group_within_budget(
                state=barrier_state,
                group_key={key: group[key] for key in mapping_keys},
                estimated_bytes=payload_bytes,
                budget_bytes=max_payload_bytes,
                rows=size,
            )
        group_files = {str(item) for item in group["__group_source_files"]}
        group_row_groups = {
            (str(item[SOURCE_FILE_COLUMN]), int(item[SOURCE_ROW_GROUP_COLUMN]))
            for item in group["__group_source_row_groups"]
        }
        candidate_files = current_files | group_files
        candidate_row_groups = current_row_groups | group_row_groups
        exceeds = (
            current_rows + size > rows_per_part
            or current_bytes + payload_bytes > max_payload_bytes
            or len(candidate_files) > max_source_files
            or len(candidate_row_groups) > max_source_row_groups
        )
        if current_rows and exceeds:
            part_index += 1
            current_rows = 0
            current_bytes = 0
            current_files = set()
            current_row_groups = set()
        part_indices.append(part_index)
        spill_flags.append(spill_required)
        current_rows += size
        current_bytes += payload_bytes
        current_files.update(group_files)
        current_row_groups.update(group_row_groups)
    mapping = groups.with_columns(
        pl.Series(PART_INDEX_COLUMN, part_indices, dtype=pl.Int64),
        pl.Series(SPILL_REQUIRED_COLUMN, spill_flags, dtype=pl.Boolean),
    )
    return frame.join(
        mapping.select([*mapping_keys, PART_INDEX_COLUMN, SPILL_REQUIRED_COLUMN]),
        on=mapping_keys,
        how="left",
        nulls_equal=True,
    )


def _assign_row_safe_part_indices(
    frame: pl.DataFrame,
    *,
    rows_per_part: int,
    max_payload_bytes: int,
    max_source_files: int,
    max_source_row_groups: int,
) -> pl.DataFrame:
    """Assign ordinary row boundaries without a one-row group-by/list aggregation."""
    part_indices: list[int] = []
    part_index = 0
    current_rows = 0
    current_bytes = 0
    current_files: set[str] = set()
    current_row_groups: set[tuple[str, int]] = set()
    columns = frame.select(
        [
            ESTIMATED_PAYLOAD_BYTES_COLUMN,
            SOURCE_FILE_COLUMN,
            SOURCE_ROW_GROUP_COLUMN,
        ]
    )
    for payload_bytes, source_file, source_row_group in columns.iter_rows():
        row_bytes = int(payload_bytes or 0)
        source = str(source_file)
        row_group = (source, int(source_row_group))
        exceeds = (
            current_rows + 1 > rows_per_part
            or current_bytes + row_bytes > max_payload_bytes
            or (source not in current_files and len(current_files) >= max_source_files)
            or (
                row_group not in current_row_groups
                and len(current_row_groups) >= max_source_row_groups
            )
        )
        if current_rows and exceeds:
            part_index += 1
            current_rows = 0
            current_bytes = 0
            current_files.clear()
            current_row_groups.clear()
        part_indices.append(part_index)
        current_rows += 1
        current_bytes += row_bytes
        current_files.add(source)
        current_row_groups.add(row_group)
    return frame.with_columns(
        pl.Series(PART_INDEX_COLUMN, part_indices, dtype=pl.Int64),
        pl.lit(False).alias(SPILL_REQUIRED_COLUMN),
    )


def _window_planner_profile(
    active_snapshot: pl.DataFrame,
    expression_ir: dict[str, Any] | None,
) -> dict[str, Any]:
    partitions = _expression_ir_window_partitions(expression_ir)
    if not partitions:
        return {"enabled": False}
    keys = list(partitions[0])
    max_group_rows = (
        active_snapshot.height
        if not keys
        else int(active_snapshot.group_by(keys).len()["len"].max() or 0)
    )
    bytes_per_row = (
        active_snapshot.estimated_size() / active_snapshot.height if active_snapshot.height else 0.0
    )
    return {
        "enabled": True,
        "partition_keys": keys,
        "max_group_rows": max_group_rows,
        "estimated_max_group_sidecar_bytes": int(max_group_rows * bytes_per_row),
        "stateful_sort_functions": [
            "median",
            "percentile",
            "p10",
            "p90",
            "q1",
            "q3",
            "iqr",
            "dense_rank",
            "rank_real",
            "nth_largest",
            "nth_smallest",
            "trimmed_mean",
            "lav",
            "uav",
            "outliers",
            "pct_outliers",
        ],
        "spill_supported": False,
    }


def _task_memory_profile(task_results: list[TaskResult]) -> dict[str, Any]:
    by_pid: dict[int, list[TaskResult]] = {}
    for result in task_results:
        by_pid.setdefault(result.pid, []).append(result)
    pid_profiles: list[dict[str, Any]] = []
    for pid, results in sorted(by_pid.items()):
        ordered = sorted(results, key=lambda item: item.counters.get("task_ordinal", 0))
        starts = [
            float(item.counters["rss_start_mb"])
            for item in ordered
            if "rss_start_mb" in item.counters
        ]
        ends = [
            float(item.counters["rss_end_mb"]) for item in ordered if "rss_end_mb" in item.counters
        ]
        peaks = [
            float(item.counters["rss_peak_mb"])
            for item in ordered
            if "rss_peak_mb" in item.counters
        ]
        pid_profiles.append(
            {
                "pid": pid,
                "tasks": len(ordered),
                "rss_start_mb": starts[0] if starts else None,
                "rss_end_mb": ends[-1] if ends else None,
                "rss_growth_mb": ends[-1] - ends[0] if len(ends) > 1 else 0.0,
                "rss_slope_mb_per_task": _linear_slope(ends),
                "peak_rss_mb": max(peaks) if peaks else None,
            }
        )
    return {
        "processes": len(pid_profiles),
        "max_tasks_per_process": max((item["tasks"] for item in pid_profiles), default=0),
        "max_peak_rss_mb": max(
            (item["peak_rss_mb"] for item in pid_profiles if item["peak_rss_mb"] is not None),
            default=None,
        ),
        "pid_profiles": pid_profiles,
    }


def _merge_phase_telemetry_profiles(
    *telemetry_profiles: dict[str, Any] | None,
    admission_limits_mb: dict[str, int] | None = None,
    hard_limit_mb: int | None = None,
) -> dict[str, Any]:
    profiles = [item for item in telemetry_profiles if isinstance(item, dict)]
    phase_profiles = [
        dict(phase)
        for profile in profiles
        for phase in (profile.get("phase_profiles") or [])
        if isinstance(phase, dict)
    ]
    admission_limits = admission_limits_mb or {}
    phase_statistics = {
        phase_name: _phase_telemetry_statistics(
            [item for item in phase_profiles if item.get("phase_name") == phase_name],
            admission_limit_mb=admission_limits.get(phase_name),
            hard_limit_mb=hard_limit_mb,
        )
        for phase_name in sorted({str(item.get("phase_name") or "") for item in phase_profiles})
    }
    return {
        "schema_version": "smoking-data.phase-telemetry.v2",
        "status": (
            "completed"
            if phase_profiles and all(item.get("status") == "completed" for item in phase_profiles)
            else ("report_unavailable" if not phase_profiles else "partial")
        ),
        "phase_names": sorted({str(item.get("phase_name") or "") for item in phase_profiles}),
        "phases_observed": len(phase_profiles),
        "phase_profiles": phase_profiles,
        "phase_statistics": phase_statistics,
        "source_logs": [
            str(profile.get("log_path")) for profile in profiles if profile.get("log_path")
        ],
    }


def _phase_telemetry_statistics(
    profiles: list[dict[str, Any]],
    *,
    admission_limit_mb: int | None = None,
    hard_limit_mb: int | None = None,
) -> dict[str, Any]:
    metrics: dict[str, dict[str, float | int | None]] = {}
    for key in ("max_rss_mb", "max_peak_rss_mb", "cpu_sec", "elapsed_sec"):
        values = sorted(
            float(item[key]) for item in profiles if isinstance(item.get(key), (int, float))
        )
        metrics[key] = {
            "count": len(values),
            "avg": (sum(values) / len(values)) if values else None,
            "p95": _linear_percentile(values, 0.95),
            "max": values[-1] if values else None,
        }
    observed_p95 = metrics["max_rss_mb"]["p95"]
    pressure = "unobserved"
    if observed_p95 is not None and admission_limit_mb is not None:
        if hard_limit_mb is not None and observed_p95 >= hard_limit_mb * 0.95:
            pressure = "hard_limit_near"
        elif observed_p95 > admission_limit_mb * 0.80:
            pressure = "safe_envelope_near"
        else:
            pressure = "within_envelope"
    return {
        "instances": len(profiles),
        "completed": sum(item.get("status") == "completed" for item in profiles),
        "metrics": metrics,
        "admission_limit_mb": admission_limit_mb,
        "hard_limit_mb": hard_limit_mb,
        "pressure": pressure,
    }


def _linear_percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    position = (len(values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def _task_phase_profile(task_results: list[TaskResult]) -> dict[str, Any]:
    elapsed = [
        float(item.counters["task_elapsed_sec"])
        for item in task_results
        if "task_elapsed_sec" in item.counters
    ]
    write_elapsed = [
        float(item.counters["write_elapsed_sec"])
        for item in task_results
        if "write_elapsed_sec" in item.counters
    ]
    coordinate_rows = [
        int(item.counters["coordinate_rows"])
        for item in task_results
        if "coordinate_rows" in item.counters
    ]
    source_files = [
        int(item.counters["coordinate_source_files"])
        for item in task_results
        if "coordinate_source_files" in item.counters
    ]
    row_groups = [
        int(item.counters["coordinate_row_groups"])
        for item in task_results
        if "coordinate_row_groups" in item.counters
    ]
    return {
        "tasks_profiled": len(elapsed),
        "task_elapsed_sec": {
            "max": max(elapsed) if elapsed else 0.0,
            "avg": (sum(elapsed) / len(elapsed)) if elapsed else 0.0,
        },
        "write_elapsed_sec": {
            "max": max(write_elapsed) if write_elapsed else 0.0,
            "avg": (sum(write_elapsed) / len(write_elapsed)) if write_elapsed else 0.0,
        },
        "coordinate_rows": {
            "max": max(coordinate_rows) if coordinate_rows else 0,
            "avg": (sum(coordinate_rows) / len(coordinate_rows)) if coordinate_rows else 0.0,
        },
        "coordinate_source_files": {
            "max": max(source_files) if source_files else 0,
            "avg": (sum(source_files) / len(source_files)) if source_files else 0.0,
        },
        "coordinate_row_groups": {
            "max": max(row_groups) if row_groups else 0,
            "avg": (sum(row_groups) / len(row_groups)) if row_groups else 0.0,
        },
    }


def _rust_task_phase_profile(task_results: list[TaskResult]) -> dict[str, Any]:
    keys = [
        "rust_restore_sec",
        "rust_parquet_write_sec",
        "rust_total_sec",
        "rust_source_extract_sec",
        "rust_dense_restore_sec",
        "rust_record_batch_build_sec",
        "rust_projection_sec",
        "rust_active_order_sec",
        "rust_reference_replace_sec",
        "rust_expression_project_sec",
        "rust_post_operation_sec",
        "rust_concat_batches_sec",
        "rust_active_order_sort_sec",
        "rust_pivot_sec",
        "rust_writer_write_sec",
        "rust_coord_read_sec",
    ]
    profile: dict[str, Any] = {}
    for key in keys:
        values = [float(item.counters[key]) for item in task_results if key in item.counters]
        normalized = key.removeprefix("rust_")
        profile[normalized] = {
            "max": max(values) if values else 0.0,
            "avg": (sum(values) / len(values)) if values else 0.0,
        }
    return profile


def _linear_slope(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    x_mean = (len(values) - 1) / 2.0
    y_mean = sum(values) / len(values)
    denominator = sum((index - x_mean) ** 2 for index in range(len(values)))
    if denominator == 0:
        return None
    return (
        sum((index - x_mean) * (value - y_mean) for index, value in enumerate(values)) / denominator
    )


def _apply_reference_replace_configs(lf, configs: Any, *, project_root: Path):
    if not configs:
        return lf
    items = configs if isinstance(configs, list) else [configs]
    for item in items:
        mapping = _mapping(item, section="source.payload.reference_replace")
        if not bool(mapping.get("enabled", True)):
            continue
        lf = apply_reference_replace(
            lf,
            reference_parquet=str(
                resolve_project_path(mapping["reference_parquet"], project_root=project_root)
            ),
            source_column=str(mapping["source_column"]),
            reference_input_column=str(mapping["reference_input_column"]),
            reference_output_column=str(mapping["reference_output_column"]),
            output_column=mapping.get("output_column"),
        )
    return lf


def _discover_reference_files(
    payload: dict[str, Any],
    *,
    list_restore: dict[str, Any],
    project_root: Path,
):
    configs = payload.get("reference_replace")
    items = configs if isinstance(configs, list) else [configs] if configs else []
    paths = [
        resolve_project_path(str(item["reference_parquet"]), project_root=project_root)
        for item in items
        if isinstance(item, dict)
        and bool(item.get("enabled", True))
        and item.get("reference_parquet")
    ]
    if bool(list_restore.get("enabled", False)) and list_restore.get("lookup_path"):
        paths.append(
            resolve_project_path(str(list_restore["lookup_path"]), project_root=project_root)
        )
    return discover_parquet_files(paths, recursive=True)


def _validate_lookup_uniqueness(
    payload: dict[str, Any],
    *,
    list_restore: dict[str, Any],
    project_root: Path,
) -> None:
    configs = payload.get("reference_replace") or []
    if isinstance(configs, dict):
        configs = [configs]
    for item in configs:
        if not isinstance(item, dict) or not bool(item.get("enabled", True)):
            continue
        path = resolve_project_path(str(item["reference_parquet"]), project_root=project_root)
        key = str(item["reference_input_column"])
        duplicate = (
            pl.scan_parquet(path)
            .group_by(key)
            .len()
            .filter(pl.col("len") > 1)
            .select(key)
            .limit(1)
            .collect()
        )
        if duplicate.height:
            raise ValidationError(
                "source.payload.reference_replace lookup key must be unique: "
                f"path={path}, column={key}, value={duplicate.item(0, 0)!r}"
            )
    if bool(list_restore.get("enabled", False)):
        config = dict(list_restore.get("config") or {})
        path = resolve_project_path(str(list_restore["lookup_path"]), project_root=project_root)
        keys = [str(config.get("key_column") or ""), str(config.get("order_column") or "")]
        if not all(keys):
            raise ValidationError("list_restore.config requires key_column and order_column.")
        duplicate = (
            pl.scan_parquet(path)
            .group_by(keys)
            .len()
            .filter(pl.col("len") > 1)
            .select(keys)
            .limit(1)
            .collect()
        )
        if duplicate.height:
            raise ValidationError(
                "list_restore lookup key/order pair must be unique: "
                f"path={path}, columns={keys}, value={duplicate.row(0)!r}"
            )


def _validate_reserved_payload_columns(payload: dict[str, Any]) -> None:
    reserved = {
        SOURCE_FILE_COLUMN,
        SOURCE_ROW_INDEX_COLUMN,
        SOURCE_ROW_GROUP_COLUMN,
        ACTIVE_ORDER_COLUMN,
        PART_INDEX_COLUMN,
        ESTIMATED_PAYLOAD_BYTES_COLUMN,
        SPILL_REQUIRED_COLUMN,
    }
    configured_targets: set[str] = set()
    for section in ("type_casts", "add_calc"):
        for item in payload.get(section) or []:
            if isinstance(item, dict):
                configured_targets.add(str(item.get("name") or item.get("column") or ""))
    reference_configs = payload.get("reference_replace") or []
    if isinstance(reference_configs, dict):
        reference_configs = [reference_configs]
    for item in reference_configs:
        if isinstance(item, dict):
            configured_targets.add(
                str(item.get("output_column") or item.get("source_column") or "")
            )
    collisions = sorted(reserved.intersection(configured_targets))
    if collisions:
        raise ValidationError(
            f"0201 payload cannot overwrite reserved coordinate columns: {collisions}"
        )


def _compile_expression_ir(configs: Any) -> dict[str, Any] | None:
    if not configs:
        return None
    from smoking_data_engine_rs import validate_expression_ir
    from spotfire_expr_normalizer import (
        build_raw_expressions,
        compile_expressions_to_ir,
        validate_rust_ir_function_support,
    )

    items: list[tuple[str, str]] = []
    for index, item in enumerate(configs):
        if not isinstance(item, dict):
            raise ValidationError(f"source.payload.add_calc[{index}] must be a mapping.")
        name = str(item.get("name") or "").strip()
        try:
            _, expression = resolve_add_calc_expression(item, index=index)
        except ValueError as error:
            raise ValidationError(str(error)) from error
        if not name:
            raise ValidationError(f"source.payload.add_calc[{index}] requires non-empty name.")
        items.append((name, expression))
    try:
        document = compile_expressions_to_ir(build_raw_expressions(items)).to_dict()
        validate_rust_ir_function_support(document)
        validate_expression_ir(json.dumps(document, ensure_ascii=True))
        return document
    except ValueError as error:
        raise ValidationError(f"Rust expression IR compilation failed: {error}") from error


def _expression_ir_hash(document: dict[str, Any] | None) -> str | None:
    if document is None:
        return None
    encoded = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _list_restore_rust_type(dtype: Any, *, column: str, source: str) -> str:
    """Convert a scalar/list Arrow or Polars dtype to the Rust list contract."""
    if isinstance(dtype, pl.List):
        return _list_restore_rust_type(dtype.inner, column=column, source=source)
    if dtype == pl.Float32:
        return "FLOAT32[]"
    if dtype == pl.Float64 or isinstance(dtype, pl.Decimal):
        return "FLOAT64[]"
    polars_integer_types = {
        pl.Int8: "INT8[]",
        pl.Int16: "INT16[]",
        pl.Int32: "INT32[]",
        pl.Int64: "INT64[]",
        pl.UInt8: "UINT8[]",
        pl.UInt16: "UINT16[]",
        pl.UInt32: "UINT32[]",
        pl.UInt64: "UINT64[]",
    }
    if dtype in polars_integer_types:
        return polars_integer_types[dtype]
    if dtype == pl.String:
        return "STRING[]"
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return _list_restore_rust_type(dtype.value_type, column=column, source=source)
    arrow_integer_types = (
        (pa.types.is_int8, "INT8[]"),
        (pa.types.is_int16, "INT16[]"),
        (pa.types.is_int32, "INT32[]"),
        (pa.types.is_int64, "INT64[]"),
        (pa.types.is_uint8, "UINT8[]"),
        (pa.types.is_uint16, "UINT16[]"),
        (pa.types.is_uint32, "UINT32[]"),
        (pa.types.is_uint64, "UINT64[]"),
    )
    for predicate, type_name in arrow_integer_types:
        if predicate(dtype):
            return type_name
    if pa.types.is_float32(dtype):
        return "FLOAT32[]"
    if pa.types.is_float64(dtype):
        return "FLOAT64[]"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "STRING[]"
    raise ValidationError(
        f"list_restore schema auto cannot infer supported list type for {column}: {dtype!r}",
        code="list_restore.schema_inference_unsupported_type",
        context={"column": column, "source": source, "dtype": str(dtype)},
    )


def _infer_json_list_rust_type(source_path: Path, *, column: str) -> str | None:
    try:
        values = pq.read_table(source_path, columns=[column]).column(0).to_pylist()
    except Exception:
        return None
    for raw in values:
        if raw is None or not str(raw).strip():
            continue
        try:
            parsed = json.loads(str(raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(parsed, list):
            continue
        item = next((value for value in parsed if value is not None), None)
        if item is None:
            continue
        if isinstance(item, bool):
            return "TEXT[]"
        if isinstance(item, int):
            return "INT64[]"
        if isinstance(item, float):
            return "FLOAT64[]"
        if isinstance(item, str):
            return "STRING[]"
    return None


def _resolve_list_restore_schema(
    list_restore: dict[str, Any],
    *,
    coordinates: pl.DataFrame,
    lookup_path: Path | None,
    source_stats: Any = None,
) -> dict[str, str]:
    configured = list_restore.get("schema")
    if isinstance(configured, dict):
        return {str(name): _rust_schema_type(str(dtype)) for name, dtype in configured.items()}
    if configured is None:
        configured = "auto"
    if not isinstance(configured, str) or configured.strip().lower() != "auto":
        raise ValidationError(
            "list_restore.schema must be a mapping or 'auto'.",
            code="list_restore.invalid_schema",
            context={"value": configured},
        )
    if lookup_path is None:
        raise ValidationError(
            "list_restore.schema=auto requires lookup_path.",
            code="list_restore.schema_inference_missing_lookup",
        )
    config = dict(list_restore.get("config") or {})
    columns = [
        str(item)
        for item in [
            *(config.get("value_columns") or []),
            *(config.get("source_coord_columns") or []),
        ]
    ]
    lookup_schema = pq.ParquetFile(lookup_path).schema_arrow
    input_schema: pa.Schema | None = None
    input_path: Path | None = None
    if SOURCE_FILE_COLUMN in coordinates.columns:
        source_values = coordinates.get_column(SOURCE_FILE_COLUMN).drop_nulls().unique().to_list()
        if source_values:
            source_path = Path(str(source_values[0]))
            if source_path.exists():
                input_path = source_path
                input_schema = pq.ParquetFile(source_path).schema_arrow
    if input_schema is None and isinstance(source_stats, dict):
        for source_value in source_stats:
            source_path = Path(str(source_value))
            if source_path.exists():
                input_path = source_path
                input_schema = pq.ParquetFile(source_path).schema_arrow
                break
    inferred: dict[str, str] = {}
    for column in dict.fromkeys(columns):
        input_dtype = coordinates.schema.get(column)
        if input_dtype is not None and isinstance(input_dtype, pl.List):
            inferred[column] = _list_restore_rust_type(input_dtype, column=column, source="input")
            continue
        if input_schema is not None and column in input_schema.names:
            input_dtype = input_schema.field(column).type
            if pa.types.is_list(input_dtype) or pa.types.is_large_list(input_dtype):
                inferred[column] = _list_restore_rust_type(
                    input_dtype, column=column, source="input"
                )
                continue
            if input_path is not None and (
                pa.types.is_string(input_dtype) or pa.types.is_large_string(input_dtype)
            ):
                inferred_json = _infer_json_list_rust_type(input_path, column=column)
                if inferred_json is not None:
                    inferred[column] = inferred_json
                    continue
        if column not in lookup_schema.names:
            raise ValidationError(
                f"list_restore.schema=auto cannot find source column {column!r} in input or lookup.",
                code="list_restore.schema_inference_missing_column",
                context={"column": column, "lookup_path": str(lookup_path)},
            )
        inferred[column] = _list_restore_rust_type(
            lookup_schema.field(column).type,
            column=column,
            source="lookup",
        )
    return inferred


def _rust_schema_type(type_name: str) -> str:
    normalized = type_name.upper().replace(" ", "")
    if normalized.startswith("DECIMAL(") and normalized.endswith(")"):
        return normalized
    if normalized in RUST_DIRECT_TYPE_MAP:
        return RUST_DIRECT_TYPE_MAP[normalized]
    return normalize_list_restore_type(normalized)


def _validate_source_file_unchanged(source_file: Path, source_stats: dict[str, Any]) -> None:
    expected = source_stats.get(str(source_file))
    if not isinstance(expected, dict):
        raise RuntimeError(f"{SOURCE_SNAPSHOT_CHANGED_MARKER}{source_file}")
    try:
        stat = source_file.stat()
        footer_fingerprint = parquet_footer_fingerprint(source_file)
    except (OSError, SmokingDataError) as error:
        raise RuntimeError(f"{SOURCE_SNAPSHOT_CHANGED_MARKER}{source_file}") from error
    if (
        stat.st_size != int(expected["size_bytes"])
        or stat.st_mtime_ns != int(expected["modified_ns"])
        or footer_fingerprint != str(expected["footer_fingerprint"])
    ):
        raise RuntimeError(f"{SOURCE_SNAPSHOT_CHANGED_MARKER}{source_file}")


def _mapping(value: Any, *, section: str, allow_missing: bool = False) -> dict[str, Any]:
    if value is None and allow_missing:
        return {}
    if not isinstance(value, dict):
        raise ValidationError(f"{section} must be a mapping.")
    return value


def _string_list(value: Any, *, section: str) -> list[str]:
    if not isinstance(value, list) or not all(str(item).strip() for item in value):
        raise ValidationError(f"{section} must be a non-empty list.")
    return [str(item) for item in value]
