from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from smoking_data.backends.rust_engine import execute_join_task
from smoking_data.core.engine_contract import TASK_CONTRACT_VERSION, engine_metadata
from smoking_data.core.exceptions import TaskExecutionError, ValidationError
from smoking_data.core.logical_plan import compile_0301_logical_plan
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
from smoking_data.ops.fingerprint import combined_fingerprint, file_fingerprint
from smoking_data.ops.parquet_metadata import profile_parquet_files
from smoking_data.ops.projection import apply_exclude_columns, apply_include_columns
from smoking_data.ops.upstream import discover_parquet_files, scan_parquet_files_union_by_name
from smoking_data.runtime.artifacts import artifact_root_for
from smoking_data.runtime.config import RuntimeConfig
from smoking_data.runtime.metadata import metadata_path_for, read_metadata, write_metadata
from smoking_data.runtime.naming import (
    NAMING_POLICY_VERSION,
    part_file_name,
    partition_dir_name,
    task_id,
)
from smoking_data.runtime.object_store.remote_upstream import materialize_remote_parquet_files
from smoking_data.runtime.output_physical_layout import (
    previous_output_physical_layout_matches,
    resolve_configured_row_group_rows,
    resolve_output_physical_layout,
)
from smoking_data.runtime.paths import ensure_dir, reset_path, resolve_project_path
from smoking_data.runtime.task_runner import run_tasks_in_subprocesses
from smoking_data.runtime.test_run import final_task_limit, select_final_tasks
from smoking_data.runtime.transactions import (
    DatasetTransaction,
    recover_orphan_transactions,
    validate_committed_dataset,
)
from smoking_data.runtime.yaml_loader import PresetSpec

PRESET_NAME = "0301"
RIGHT_INDEX_MANIFEST_VERSION = "smoking-data.0301-right-index.v2"
INTERNAL_JOIN_BACKEND_ENV = "SMOKING_DATA_0301_JOIN_BACKEND"
INTERNAL_DISABLE_BOUNDED_JOIN_ENV = "SMOKING_DATA_DISABLE_0301_BOUNDED_JOIN"
BOUNDED_JOIN_STRATEGY_VERSION = "smoking-data.0301-adaptive-parallel-join.v5"


def can_run(preset: str) -> bool:
    return preset == PRESET_NAME


def run(spec: PresetSpec, *, config: RuntimeConfig) -> StageResult:
    run_started = time.perf_counter()
    phase_elapsed_sec: dict[str, float] = {}
    raw = spec.raw
    execution = _mapping(raw.get("execution"), section="execution", allow_missing=True)
    join_backend = os.getenv(INTERNAL_JOIN_BACKEND_ENV, "arrow_native").strip().lower()
    bounded_join = os.getenv(INTERNAL_DISABLE_BOUNDED_JOIN_ENV) != "1"
    if join_backend not in {"arrow_native", "polars"}:
        raise ValidationError(
            f"Unsupported internal 0301 join backend: {join_backend!r}.",
            code="join.unsupported_internal_backend",
            context={"environment": INTERNAL_JOIN_BACKEND_ENV, "value": join_backend},
        )
    if join_backend == "polars":
        from smoking_data_engine_rs import join_backend_capabilities

        if "polars" not in join_backend_capabilities():
            raise ValidationError(
                "The experimental Polars join backend is not compiled.",
                code="join.backend_not_compiled",
                context={
                    "environment": INTERNAL_JOIN_BACKEND_ENV,
                    "value": join_backend,
                    "required_feature": "polars-join-experiment",
                },
            )
    test_run_limit = final_task_limit(execution)
    logical_plan = compile_0301_logical_plan(raw)
    logical_plan_hash = str(
        (raw.get("__pipeline") or {}).get("execution_plan_hash") or logical_plan.plan_hash
    )
    optimization = optimize_logical_plan(logical_plan, enabled=config.optimizer_enabled)
    left_cfg = _mapping(raw.get("left"), section="left")
    join_cfg = _mapping(raw.get("join"), section="join")
    output = _mapping(raw.get("output"), section="output")
    right_cfgs = _right_source_configs(raw)
    join_operations = [
        operation for operation in logical_plan.operations if operation.kind.value == "join"
    ]

    source_scan_started = time.perf_counter()
    left_files = _discover_from_cfg(left_cfg, config=config)
    right_sources: list[dict[str, Any]] = []
    all_right_files = []
    for source_cfg, operation in zip(right_cfgs, join_operations, strict=True):
        source_files = _discover_from_cfg(source_cfg, config=config)
        all_right_files.extend(source_files)
        right_sources.append(
            {
                "name": operation.config["source_name"],
                "files": [str(item.path) for item in source_files],
                "columns": operation.config.get("columns") or {},
                "left_on": list(operation.config["left_on"]),
                "right_on": list(operation.config["right_on"]),
                "how": operation.config["how"],
                "suffix": operation.config["suffix"],
            }
        )
    left_fingerprint = combined_fingerprint(left_files)
    right_fingerprint = combined_fingerprint(all_right_files)
    skipped_result = (
        None
        if test_run_limit is not None
        else _maybe_skip_unchanged(
            spec,
            config=config,
            left_fingerprint=left_fingerprint,
            right_fingerprint=right_fingerprint,
            logical_plan_hash=logical_plan_hash,
            join_backend=join_backend,
            asset_code=str((raw.get("__pipeline") or {}).get("asset_code") or "0301"),
            physical_layout_policy=output.get("physical_layout"),
            compression=str(output.get("compression") or "zstd"),
        )
    )
    if skipped_result is not None:
        return skipped_result
    left = _apply_column_policy(scan_parquet_files_union_by_name(left_files), left_cfg)
    first_left_on = list(join_operations[0].config["left_on"])
    join_key_groups = left.select(first_left_on).unique().collect().height if first_left_on else 1
    partition_column = str(output.get("partition_column") or "").strip()
    output_dir_raw = output.get("output_dir")
    if not partition_column:
        raise ValidationError("output.partition_column is required.")
    if not output_dir_raw:
        raise ValidationError("output.output_dir is required.")
    output_dir = resolve_project_path(str(output_dir_raw), project_root=config.project_root)
    key_groups_per_part = config.target_key_groups_per_part
    left_partition_key = str(join_cfg.get("left_partition_key_column") or partition_column).strip()
    right_partition_key = str(join_cfg.get("right_partition_key_column") or "").strip()
    for source in right_sources:
        columns = source.get("columns") or {}
        source["keep_right_partition_column"] = bool(
            right_partition_key
            and right_partition_key in [str(item) for item in columns.get("include") or []]
        )
    _reject_null_partitions(
        left,
        partition_column=left_partition_key,
        source_name="left",
    )
    if right_partition_key:
        for source in right_sources:
            right = scan_parquet_files_union_by_name(
                [DatasetFileShim(path) for path in source["files"]]
            )
            _reject_null_partitions(
                right,
                partition_column=right_partition_key,
                source_name=str(source["name"]),
            )
    join_schema_trace = _validate_join_runtime_schema(
        left,
        right_sources=right_sources,
        right_partition_key=right_partition_key,
    )
    partition_values = {
        str(value)
        for value in left.select(pl.col(left_partition_key).cast(pl.String))
        .unique()
        .collect()
        .get_column(left_partition_key)
        .drop_nulls()
        .to_list()
    }
    if right_partition_key:
        for source in right_sources:
            right = scan_parquet_files_union_by_name(
                [DatasetFileShim(path) for path in source["files"]]
            )
            partition_values.update(
                str(value)
                for value in right.select(pl.col(right_partition_key).cast(pl.String))
                .unique()
                .collect()
                .get_column(right_partition_key)
                .drop_nulls()
                .to_list()
            )
    all_profile_paths = [item.path for item in left_files]
    all_profile_paths.extend(Path(path) for source in right_sources for path in source["files"])
    parquet_profiles = profile_parquet_files(all_profile_paths)
    input_profile = _summarize_input_profiles(
        parquet_profiles,
        left_paths=[item.path for item in left_files],
        right_sources=right_sources,
    )
    join_multiplicity_profile = _profile_join_multiplicity(right_sources)
    source_selection = _choose_source_selection(
        left_files=left_files,
        right_sources=right_sources,
        partition_values=sorted(partition_values),
        parquet_profiles=parquet_profiles,
        memory_budget_bytes=config.memory_budget_mb * 1024 * 1024,
        max_source_files_per_task=config.max_source_files_per_task,
        artifact_root=artifact_root_for(spec, config=config),
        join_multiplicity_profile=join_multiplicity_profile,
    )
    source_selection_strategy = str(source_selection["selected"])
    phase_elapsed_sec["source_scan_sec"] = time.perf_counter() - source_scan_started

    index_started = time.perf_counter()
    if source_selection_strategy == "partition_local":
        right_key_file_index = {}
        left_key_file_index = {}
        right_index_rows = 0
        left_index_rows = 0
        right_index_profile = _unused_index_profile("partition_local")
        left_index_profile = _unused_index_profile("partition_local")
    else:
        right_key_file_index, right_index_rows, right_index_profile = _build_right_key_file_index(
            right_sources,
            right_partition_key=right_partition_key,
            candidate_root=artifact_root_for(spec, config=config) / "right_key_index",
            logical_plan_hash=logical_plan_hash,
        )
        left_key_file_index, left_index_rows, left_index_profile = _build_right_key_file_index(
            [
                {
                    "name": "__left",
                    "files": [str(item.path) for item in left_files],
                    "right_on": first_left_on,
                }
            ],
            right_partition_key=left_partition_key,
            candidate_root=artifact_root_for(spec, config=config) / "left_key_index",
            logical_plan_hash=logical_plan_hash,
        )
    phase_elapsed_sec["index_build_sec"] = time.perf_counter() - index_started

    planning_started = time.perf_counter()
    key_plan, key_plan_rows = _build_join_key_part_plan(
        left,
        partition_column=left_partition_key,
        left_on=first_left_on,
        key_groups_per_part=key_groups_per_part,
        rows_per_part=config.target_rows_per_part,
    )
    if any(source["how"] in {"right", "full"} for source in right_sources):
        _extend_plan_with_right_only_partitions(
            key_plan,
            right_sources=right_sources,
            right_partition_key=right_partition_key,
        )
        # Right-only rows must be emitted exactly once per partition. Splitting by
        # left keys would repeat the same unmatched right rows in every part.
        key_plan = {partition: [[]] for partition in key_plan}
        key_plan_rows = {}
    task_payload = {
        "left_columns": left_cfg.get("columns") or {},
        "left_partition_key": left_partition_key,
        "right_partition_key": right_partition_key,
        "partition_column": partition_column,
        "output_dir": str(output_dir),
        "logical_plan_hash": logical_plan_hash,
        "compression": str(output.get("compression") or "zstd"),
        "ordered_operations": list(
            (raw.get("__pipeline") or {}).get("rust_operation_trace")
            or (raw.get("__pipeline") or {}).get("operation_trace")
            or []
        ),
        "post_operations": list((raw.get("__pipeline") or {}).get("join_post_operations") or []),
        "source_selection_strategy": source_selection_strategy,
        "join_backend": join_backend,
    }
    tasks = []
    for partition_value, chunks in key_plan.items():
        for part_index, key_rows in enumerate(chunks):
            if source_selection_strategy == "partition_local":
                selected_left_files = _select_partition_named_files(
                    [DatasetFileShim(str(item.path)) for item in left_files],
                    partition_value=partition_value,
                )
                selected_left_row_groups = {}
                selected_right_sources = _select_partition_local_right_sources(
                    right_sources,
                    partition_value=partition_value,
                )
            else:
                selected_left_files, selected_left_row_groups = _select_left_spans_for_task(
                    [DatasetFileShim(str(item.path)) for item in left_files],
                    key_rows=key_rows,
                    partition_value=partition_value,
                    left_on=first_left_on,
                    key_file_index=left_key_file_index,
                )
                selected_right_sources = _select_right_sources_for_task(
                    right_sources,
                    key_rows=key_rows,
                    partition_value=partition_value,
                    right_partition_key=right_partition_key,
                    key_file_index=right_key_file_index,
                )
            task = TaskSpec(
                task_id=task_id(partition_value, part_index),
                partition_value=partition_value,
                part_index=part_index,
                payload={
                    **task_payload,
                    "left_files": [str(item.path) for item in selected_left_files],
                    "left_row_groups": selected_left_row_groups,
                    "right_sources": selected_right_sources,
                    "key_rows": key_rows,
                    "left_key_filter_required": not (
                        source_selection_strategy == "partition_local" and len(chunks) == 1
                    ),
                    "expected_left_rows": key_plan_rows.get((partition_value, part_index)),
                },
            )
            tasks.append(task)
    physical_plan, input_profile = _build_0301_physical_plan(
        tasks,
        left_files=left_files,
        right_sources=right_sources,
        logical_plan_hash=logical_plan_hash,
        right_partition_key=right_partition_key,
        key_groups_per_part=key_groups_per_part,
        memory_budget_mb=config.memory_budget_mb,
        max_source_files_per_task=config.max_source_files_per_task,
        parquet_profiles=parquet_profiles,
        source_selection_strategy=source_selection_strategy,
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
        memory_budget_bytes=config.memory_budget_mb * 1024 * 1024,
        target_rows_per_part=config.target_rows_per_part,
    )
    physical_by_id = {task.task_id: task for task in physical_plan.tasks}
    asset_code = str((raw.get("__pipeline") or {}).get("asset_code") or "0301")
    previous_metadata = read_metadata(spec, config=config) or {}
    task_row_group_recommendations = {
        task.task_id: choose_output_row_group_rows(physical_by_id[task.task_id])
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
            payload={
                **task.payload,
                "output_row_group_rows": task_row_group_rows[task.task_id],
                "output_physical_layout_profile_hash": output_physical_layout[
                    "profile_hash"
                ],
                "input_batch_rows": _choose_join_input_batch_rows(
                    physical_by_id[task.task_id],
                    memory_budget_bytes=config.memory_budget_mb * 1024 * 1024,
                    fallback_rows=config.target_rows_per_part,
                ),
                "bounded_join": bounded_join,
                "bounded_join_strategy_version": BOUNDED_JOIN_STRATEGY_VERSION,
            },
        )
        for task in tasks
    ]
    global_tasks = tasks
    tasks, test_run_profile = select_final_tasks(
        global_tasks,
        limit=test_run_limit,
        task_id=lambda task: task.task_id,
    )
    task_fingerprints = {
        task.task_id: _join_task_fingerprint(task, logical_plan_hash=logical_plan_hash)
        for task in tasks
    }
    task_fanout_profile = _summarize_task_fanout(physical_plan.tasks)
    admitted_workers = admitted_worker_count(
        physical_plan,
        requested_workers=config.workers,
        memory_budget_bytes=config.memory_budget_mb * 1024 * 1024,
    )
    partition_local_batching_plan = _plan_partition_local_task_batches(
        dirty_tasks=tasks,
        physical_by_id={task.task_id: task for task in physical_plan.tasks},
        source_selection_strategy=source_selection_strategy,
        memory_budget_bytes=config.memory_budget_mb * 1024 * 1024,
        memory_safety_ratio=config.memory_safety_ratio,
        max_tasks_per_child=config.max_tasks_per_child,
        admitted_workers=admitted_workers,
    )
    phase_elapsed_sec["planning_sec"] = time.perf_counter() - planning_started

    transaction_prepare_started = time.perf_counter()
    previous_task_fingerprints = _previous_task_fingerprints(spec, config=config)
    recovery_profile = recover_orphan_transactions(output_dir)
    transaction = DatasetTransaction.create(
        output_dir,
        manifest_context={
            "preset": spec.preset,
            "job_name": spec.job_name,
            "logical_plan_hash": logical_plan_hash,
            "physical_plan_hash": physical_plan.plan_hash,
            "physical_plan_cost": estimate_plan_cost(
                physical_plan,
                memory_budget_bytes=config.memory_budget_mb * 1024 * 1024,
                target_rows_per_part=config.target_rows_per_part,
            ),
            "physical_plan_candidates": physical_candidate_trace,
            "naming_policy_version": NAMING_POLICY_VERSION,
            "change_reason": "semantic_or_dependency_change",
            "test_run": test_run_profile,
            "output_physical_layout": output_physical_layout,
        },
    )
    if test_run_limit is not None:
        staged_results, dirty_tasks = [], tasks
    else:
        staged_results, dirty_tasks = _stage_reusable_join_tasks(
            tasks,
            current_fingerprints=task_fingerprints,
            previous_fingerprints=previous_task_fingerprints,
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
    partition_local_batching_plan = _rebind_partition_local_task_batches(
        partition_local_batching_plan,
        dirty_tasks=dirty_tasks,
    )
    phase_elapsed_sec["transaction_prepare_sec"] = time.perf_counter() - transaction_prepare_started
    try:
        spawn_join_started = time.perf_counter()
        task_results, subprocess_runner_profile = run_tasks_in_subprocesses(
            dirty_tasks,
            worker=join_partition_task_worker,
            workers=admitted_workers,
            max_tasks_per_child=config.max_tasks_per_child,
            task_batches=partition_local_batching_plan["task_batches"],
            max_child_rss_mb=(
                config.memory_budget_mb
                * config.memory_safety_ratio
                / max(1, admitted_workers)
                if partition_local_batching_plan["task_batches"]
                else None
            ),
            return_profile=True,
        )
        phase_elapsed_sec["spawn_and_join_sec"] = time.perf_counter() - spawn_join_started
        failed = [item for item in task_results if not item.ok]
        if failed:
            failure = failed[0]
            raise TaskExecutionError(
                f"0301 partition join task failed: {failure.error_message}",
                context={
                    "task_id": failure.task_id,
                    "partition_value": failure.partition_value,
                    "part_index": failure.part_index,
                    "error_type": failure.error_type,
                    "traceback_tail": failure.traceback_tail,
                },
            )
        task_results = [*staged_results, *task_results]
        transaction_commit_started = time.perf_counter()
        output_files, transaction_profile = transaction.commit()
        phase_elapsed_sec["transaction_commit_sec"] = (
            time.perf_counter() - transaction_commit_started
        )
    except BaseException:
        transaction.abort()
        raise
    task_results = _remap_transaction_task_outputs(
        task_results,
        staging_root=transaction.staging_root,
        final_root=output_dir,
    )
    output_rows = int(sum(item.counters.get("output_rows", 0) for item in task_results))
    rust_join_profile = _summarize_task_counter(task_results, "rust_join_elapsed_sec")
    polars_boundary_profile = {
        name: _summarize_task_metric(task_results, name)
        for name in (
            "polars_bridge_apache_input_bytes",
            "polars_bridge_frame_input_bytes",
            "polars_join_output_bytes",
            "polars_bridge_apache_output_bytes",
            "polars_bridge_import_sec",
            "polars_join_sec",
            "polars_bridge_export_sec",
        )
    }
    task_process_profile = _summarize_task_process_profile(task_results)
    phase_elapsed_sec["total_elapsed_sec"] = time.perf_counter() - run_started
    measured_phase_total = sum(
        elapsed for name, elapsed in phase_elapsed_sec.items() if name != "total_elapsed_sec"
    )
    phase_elapsed_sec["unattributed_sec"] = max(
        0.0,
        phase_elapsed_sec["total_elapsed_sec"] - measured_phase_total,
    )

    metadata_path = metadata_path_for(spec, config=config)
    result = StageResult.success(
        preset=spec.preset,
        job_name=spec.job_name,
        yaml_path=spec.yaml_path,
        metadata_path=metadata_path,
        output_paths=output_files,
        counters={
            "left_files": len(left_files),
            "right_files": len(all_right_files),
            "right_sources": len(right_sources),
            "output_files": len(output_files),
            "output_rows": output_rows,
            "output_partitions": len({str(task.partition_value) for task in tasks}),
            "join_key_groups": join_key_groups,
            "right_key_index_rows": right_index_rows,
            "left_key_index_rows": left_index_rows,
            "tasks": len(tasks),
            "global_planned_tasks": len(global_tasks),
            "dirty_tasks": len(dirty_tasks),
            "reused_tasks": len(staged_results),
            "key_groups_per_part": key_groups_per_part,
        },
        details={
            "engine": engine_metadata(),
            "test_run": test_run_profile,
            "left_fingerprint": left_fingerprint,
            "right_fingerprint": right_fingerprint,
            "dependency_graph": {
                "left": {
                    "fingerprint": left_fingerprint,
                    "files": [str(item.path) for item in left_files],
                },
                "right_sources": [
                    {
                        "name": source["name"],
                        "files": list(source["files"]),
                    }
                    for source in right_sources
                ],
                "right_fingerprint": right_fingerprint,
            },
            "logical_plan": logical_plan.to_dict(),
            "logical_plan_hash": logical_plan_hash,
            "pipeline_contract": raw.get("__pipeline"),
            "optimizer": optimization.to_dict(),
            "join_schema_trace": join_schema_trace,
            "source_selection": source_selection,
            "source_selection_strategy": source_selection_strategy,
            "join_backend": join_backend,
            "candidate_costs": source_selection["candidate_costs"],
            "candidate_rejections": source_selection["candidate_rejections"],
            "partition_locality": source_selection["partition_locality"],
            "join_multiplicity_profile": join_multiplicity_profile,
            "index_cache_state": _index_cache_state(
                source_selection_strategy,
                left_index_profile=left_index_profile,
                right_index_profile=right_index_profile,
            ),
            "phase_elapsed_sec": phase_elapsed_sec,
            "input_profile": input_profile,
            "task_fanout_profile": task_fanout_profile,
            "execution_batching_policy": partition_local_batching_plan["profile"],
            "rust_join_profile": rust_join_profile,
            "polars_boundary_profile": polars_boundary_profile,
            "bounded_join_profile": _summarize_bounded_join(task_results),
            "subprocess_runner_profile": subprocess_runner_profile,
            "task_process_profile": task_process_profile,
            "right_key_index": right_index_profile,
            "left_key_index": left_index_profile,
            "task_fingerprints": task_fingerprints,
            "physical_plan": physical_plan.to_dict(),
            "physical_plan_hash": physical_plan.plan_hash,
            "naming_policy_version": NAMING_POLICY_VERSION,
            "physical_plan_cost": estimate_plan_cost(
                physical_plan,
                memory_budget_bytes=config.memory_budget_mb * 1024 * 1024,
                target_rows_per_part=config.target_rows_per_part,
            ),
            "physical_plan_candidates": physical_candidate_trace,
            "physical_plan_actuals": reconcile_task_memory(physical_plan, task_results),
            "resource_admission": {
                "requested_workers": config.workers,
                "admitted_workers": admitted_workers,
                "memory_budget_mb": config.memory_budget_mb,
            },
            "output_dir": str(output_dir),
            "dataset_transaction": transaction_profile,
            "output_physical_layout": output_physical_layout,
            "transaction_recovery": recovery_profile,
            "task_results": task_results,
        },
    )
    if not raw.get("__pipeline"):
        result.metadata_path = write_metadata(spec=spec, config=config, result=result.to_dict())
    return result


def _maybe_skip_unchanged(
    spec: PresetSpec,
    *,
    config: RuntimeConfig,
    left_fingerprint: str,
    right_fingerprint: str,
    logical_plan_hash: str,
    join_backend: str,
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
    if not isinstance(details, dict):
        return None
    if details.get("left_fingerprint") != left_fingerprint:
        return None
    if details.get("right_fingerprint") != right_fingerprint:
        return None
    if details.get("logical_plan_hash") != logical_plan_hash:
        return None
    if details.get("join_backend", "arrow_native") != join_backend:
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
            "skipped_reason": "left_right_fingerprint_unchanged",
        },
    )
    written = write_metadata(spec=spec, config=config, result=result.to_dict())
    result.metadata_path = written
    return result


def join_partition_task_worker(task: TaskSpec) -> TaskResult:
    payload = task.payload
    partition_value = str(task.partition_value)
    output_dir = Path(payload["output_dir"])
    output_path = (
        output_dir / partition_dir_name(partition_value) / part_file_name(int(task.part_index or 0))
    )
    right_partition_key = str(payload["right_partition_key"])
    right_sources = []
    for source in payload["right_sources"]:
        source_payload = dict(source)
        if not right_partition_key:
            selected = _select_partition_named_files(
                [DatasetFileShim(path) for path in source["files"]],
                partition_value=partition_value,
            )
            source_payload["files"] = [str(item.path) for item in selected]
        right_sources.append(source_payload)
    rust_join_started = time.perf_counter()
    stats = execute_join_task(
        {
            "left_files": payload["left_files"],
            "left_row_groups": payload.get("left_row_groups") or {},
            "left_columns": payload["left_columns"],
            "right_sources": right_sources,
            "partition_value": partition_value,
            "left_partition_key": payload["left_partition_key"],
            "right_partition_key": right_partition_key,
            "output_partition_column": payload["partition_column"],
            "output_path": str(output_path),
            "key_rows": payload.get("key_rows") or [],
            "left_key_filter_required": payload.get("left_key_filter_required", True),
            "output_row_group_rows": payload.get("output_row_group_rows"),
            "input_batch_rows": payload.get("input_batch_rows"),
            "bounded_join": payload.get("bounded_join", True),
            "ordered_operations": payload.get("ordered_operations") or [],
            "post_operations": payload.get("post_operations") or [],
            "compression": payload.get("compression") or "zstd",
            "join_backend": payload.get("join_backend", "arrow_native"),
        }
    )
    stats["rust_join_elapsed_sec"] = time.perf_counter() - rust_join_started
    return TaskResult(
        task_id=task.task_id,
        ok=True,
        pid=os.getpid(),
        partition_value=task.partition_value,
        part_index=task.part_index,
        output_paths=[output_path],
        counters={name: value for name, value in stats.items()},
    )


def _remap_transaction_task_outputs(
    task_results: list[TaskResult],
    *,
    staging_root: Path,
    final_root: Path,
) -> list[TaskResult]:
    return [
        replace(
            result,
            output_paths=[
                final_root / path.relative_to(staging_root) for path in result.output_paths
            ],
        )
        for result in task_results
    ]


def _join_task_fingerprint(task: TaskSpec, *, logical_plan_hash: str) -> str:
    payload = task.payload

    def file_signature(path_text: str) -> dict[str, Any]:
        path = Path(path_text)
        stat = path.stat()
        return {
            "path": str(path.resolve()),
            "size_bytes": stat.st_size,
            "modified_ns": stat.st_mtime_ns,
        }

    document = {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "logical_plan_hash": logical_plan_hash,
        "source_selection_strategy": payload.get("source_selection_strategy"),
        "join_backend": payload.get("join_backend", "arrow_native"),
        "partition_value": task.partition_value,
        "part_index": task.part_index,
        "output_row_group_rows": payload.get("output_row_group_rows"),
        "output_physical_layout_profile_hash": payload.get(
            "output_physical_layout_profile_hash"
        ),
        "input_batch_rows": payload.get("input_batch_rows"),
        "bounded_join": payload.get("bounded_join", True),
        "bounded_join_strategy_version": payload.get("bounded_join_strategy_version"),
        "compression": payload.get("compression"),
        "ordered_operations": payload.get("ordered_operations") or [],
        "post_operations": payload.get("post_operations") or [],
        "key_rows": payload.get("key_rows") or [],
        "left_files": [file_signature(path) for path in payload["left_files"]],
        "left_row_groups": payload.get("left_row_groups") or {},
        "right_sources": [
            {
                "name": source["name"],
                "how": source["how"],
                "left_on": source["left_on"],
                "right_on": source["right_on"],
                "suffix": source["suffix"],
                "columns": source.get("columns") or {},
                "keep_right_partition_column": bool(
                    source.get("keep_right_partition_column", False)
                ),
                "files": [file_signature(path) for path in source["files"]],
                "row_groups": source.get("row_groups") or {},
            }
            for source in payload["right_sources"]
        ],
    }
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _previous_task_fingerprints(
    spec: PresetSpec,
    *,
    config: RuntimeConfig,
) -> dict[str, str]:
    previous = read_metadata(spec, config=config) or {}
    result = previous.get("result") or {}
    details = result.get("details") or {}
    values = details.get("task_fingerprints") or {}
    return {str(key): str(value) for key, value in values.items()}


def _stage_reusable_join_tasks(
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
                    "ordered_operation_count": len(task.payload.get("ordered_operations") or []),
                },
            )
        )
    return reused, dirty


def join_partition_reference_task_worker(task: TaskSpec) -> TaskResult:
    """Polars reference executor retained only for semantic parity tests."""
    payload = task.payload
    partition_column = str(payload["partition_column"])
    partition_value = str(task.partition_value)
    left_files = [DatasetFileShim(path) for path in payload["left_files"]]
    left = _apply_column_policy(
        scan_parquet_files_union_by_name(left_files), {"columns": payload["left_columns"]}
    )
    left_partition_key = str(payload["left_partition_key"])
    right_partition_key = str(payload.get("right_partition_key") or "")
    left = left.filter(pl.col(left_partition_key).cast(pl.String) == partition_value)
    left = left.with_columns(pl.col(left_partition_key).cast(pl.String).alias("__output_partition"))
    key_rows = list(payload.get("key_rows") or [])
    first_left_on = list(payload["right_sources"][0]["left_on"])
    if key_rows and first_left_on:
        key_df = pl.DataFrame(key_rows).select(first_left_on).lazy()
        # Planner key rows are coordinates, not business join semantics. Null keys
        # must survive task selection even though payload joins do not match nulls.
        left = left.join(key_df, on=first_left_on, how="semi", nulls_equal=True)
    joined = left
    matched_rows = 0
    source_files_touched = 0
    temp_partition_columns: list[str] = []
    helper_columns_to_drop: list[str] = []
    for source_index, source in enumerate(payload["right_sources"]):
        right_files = _select_partition_named_files(
            [DatasetFileShim(path) for path in source["files"]],
            partition_value=partition_value,
        )
        source_files_touched += len(right_files)
        right = _apply_join_column_policy(
            scan_parquet_files_union_by_name(right_files),
            source.get("columns") or {},
            required_columns=[
                *source["right_on"],
                *([right_partition_key] if right_partition_key else []),
            ],
            source_name=str(source["name"]),
        )
        temp_partition_column = f"__right_partition_{source_index}"
        if right_partition_key:
            right = right.filter(
                pl.col(right_partition_key).cast(pl.String) == partition_value
            ).with_columns(pl.col(right_partition_key).cast(pl.String).alias(temp_partition_column))
            temp_partition_columns.append(temp_partition_column)
        how = str(source["how"])
        columns_before_join = set(joined.collect_schema().names())
        if how in {"inner", "left"} and source["left_on"]:
            right = _pre_screen_right(
                joined,
                right,
                left_on=source["left_on"],
                right_on=source["right_on"],
            )
        if how == "cross":
            joined = joined.join(right, how="cross", suffix=source["suffix"])
        else:
            joined = joined.join(
                right,
                left_on=source["left_on"],
                right_on=source["right_on"],
                how=how,
                suffix=source["suffix"],
                coalesce=True,
            )
            joined = _normalize_join_key_columns(
                joined,
                left_on=source["left_on"],
                right_on=source["right_on"],
            )
        if right_partition_key and not source.get("keep_right_partition_column", False):
            helper_columns_to_drop.append(
                    f"{right_partition_key}{source['suffix']}"
                    if right_partition_key in columns_before_join
                    else right_partition_key
            )
        if right_partition_key:
            joined = joined.with_columns(
                pl.coalesce([pl.col("__output_partition"), pl.col(temp_partition_column)]).alias(
                    "__output_partition"
                )
            )
        matched_rows = joined.select(pl.len()).collect().item()
    joined = joined.with_columns(pl.col("__output_partition").alias(partition_column)).drop(
        ["__output_partition", *temp_partition_columns, *helper_columns_to_drop], strict=False
    )
    output_files, output_rows = _write_join_part(
        joined,
        output_dir=Path(payload["output_dir"]),
        partition_column=partition_column,
        partition_value=partition_value,
        part_index=int(task.part_index or 0),
    )
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
            "matched_rows": matched_rows,
            "source_files_touched": source_files_touched,
        },
    )


class DatasetFileShim:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        stat = self.path.stat()
        self.size_bytes = stat.st_size
        self.modified_ns = stat.st_mtime_ns


def _build_join_key_part_plan(
    lf,
    *,
    partition_column: str,
    left_on: list[str],
    key_groups_per_part: int,
    rows_per_part: int,
) -> tuple[
    dict[str, list[list[dict[str, Any]]]],
    dict[tuple[str, int], int],
]:
    key_columns = list(dict.fromkeys([partition_column, *left_on]))
    key_df = (
        lf.group_by(key_columns).agg(pl.len().alias("__group_rows")).sort(key_columns).collect()
    )
    result: dict[str, list[list[dict[str, Any]]]] = {}
    part_rows: dict[tuple[str, int], int] = {}
    for partition_value in (
        key_df.get_column(partition_column).drop_nulls().cast(pl.String).unique().sort()
    ):
        subset = key_df.filter(pl.col(partition_column).cast(pl.String) == str(partition_value))
        rows = subset.to_dicts()
        parts: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        current_rows = 0
        for row in rows:
            group_rows = int(row.pop("__group_rows"))
            row["__planner_group_rows"] = group_rows
            if current and (
                len(current) >= key_groups_per_part or current_rows + group_rows > rows_per_part
            ):
                parts.append(current)
                current = []
                current_rows = 0
            current.append(row)
            current_rows += group_rows
        if current:
            parts.append(current)
        result[str(partition_value)] = parts or [[]]
        for part_index, part in enumerate(parts):
            part_rows[(str(partition_value), part_index)] = sum(
                int(item.get("__planner_group_rows", 0)) for item in part
            )
            for item in part:
                item.pop("__planner_group_rows", None)
    return result, part_rows


def _write_join_part(
    lf,
    *,
    output_dir: Path,
    partition_column: str,
    partition_value: str,
    part_index: int,
) -> tuple[list[Path], int]:
    df = lf.collect()
    partition_dir = ensure_dir(output_dir / partition_dir_name(partition_value))
    output_path = partition_dir / part_file_name(part_index)
    df.write_parquet(output_path)
    return [output_path], df.height


def _select_partition_named_files(
    files: list[DatasetFileShim], *, partition_value: str
) -> list[DatasetFileShim]:
    matched = [item for item in files if _path_has_partition_prefix(item.path, partition_value)]
    return matched or files


def _path_has_partition_prefix(path: Path, partition_value: str) -> bool:
    normalized = str(partition_value)
    candidates = [path.stem, path.name, *(parent.name for parent in path.parents)]
    return any(
        candidate == normalized
        or candidate.startswith(f"{normalized}.")
        or candidate.startswith(f"{normalized}__")
        or candidate.startswith(f"{normalized}.dataset")
        for candidate in candidates
    )


def _discover_from_cfg(source_cfg: dict[str, Any], *, config: RuntimeConfig):
    upstream = _mapping(source_cfg.get("upstream"), section="source.upstream")
    remote = upstream.get("remote")
    if isinstance(remote, dict):
        return materialize_remote_parquet_files(
            config.project_root,
            target_name=str(remote.get("target") or ""),
            dataset_prefix=str(remote.get("dataset_prefix") or ""),
            relative_paths=[str(value) for value in remote.get("relative_paths") or []],
            recursive=bool(remote.get("recursive", True)),
        )
    paths = _string_list(upstream.get("paths"), section="source.upstream.paths")
    return discover_parquet_files(
        [resolve_project_path(path, project_root=config.project_root) for path in paths],
        recursive=bool(upstream.get("recursive", True)),
    )


def _apply_column_policy(lf, source_cfg: dict[str, Any]):
    columns = _mapping(source_cfg.get("columns"), section="source.columns", allow_missing=True)
    lf = apply_exclude_columns(lf, columns.get("exclude"))
    lf = apply_include_columns(lf, columns.get("include"))
    return lf


def _apply_join_column_policy(
    lf: pl.LazyFrame,
    columns: dict[str, Any],
    *,
    required_columns: list[str],
    source_name: str,
) -> pl.LazyFrame:
    schema_names = list(lf.collect_schema().names())
    required = list(dict.fromkeys(name for name in required_columns if name))
    missing_required = [name for name in required if name not in schema_names]
    if missing_required:
        raise ValidationError(
            f"Right source {source_name!r} is missing required columns: {missing_required}",
            code="join.missing_required_column",
            context={"source": source_name, "columns": missing_required},
        )
    include = [str(item) for item in columns.get("include") or []]
    patterns = [str(item) for item in columns.get("regex") or []]
    exclude = {str(item) for item in columns.get("exclude") or []}
    excluded_required = sorted(exclude.intersection(required))
    if excluded_required:
        raise ValidationError(
            f"Right source {source_name!r} cannot exclude required columns: {excluded_required}",
            code="join.required_column_excluded",
            context={"source": source_name, "columns": excluded_required},
        )
    if include or patterns:
        missing_include = [name for name in include if name not in schema_names]
        if missing_include:
            raise ValidationError(
                f"Right source {source_name!r} include columns are missing: {missing_include}",
                code="join.missing_included_column",
                context={"source": source_name, "columns": missing_include},
            )
        selected = list(include)
        for name in schema_names:
            if any(re.search(pattern, name) for pattern in patterns) and name not in selected:
                selected.append(name)
    else:
        selected = list(schema_names)
    selected = [name for name in selected if name not in exclude]
    for name in required:
        if name not in selected:
            selected.append(name)
    return lf.select(selected)


def _right_source_configs(raw: dict[str, Any]) -> list[dict[str, Any]]:
    raw_sources = raw.get("right_sources")
    if raw_sources is None:
        return [_mapping(raw.get("right"), section="right")]
    return [
        _mapping(item, section=f"right_sources[{index}]") for index, item in enumerate(raw_sources)
    ]


def _extend_plan_with_right_only_partitions(
    key_plan: dict[str, list[list[dict[str, Any]]]],
    *,
    right_sources: list[dict[str, Any]],
    right_partition_key: str,
) -> None:
    if not right_partition_key:
        return
    for source in right_sources:
        if source["how"] not in {"right", "full"}:
            continue
        files = [DatasetFileShim(path) for path in source["files"]]
        right = scan_parquet_files_union_by_name(files)
        values = (
            right.select(pl.col(right_partition_key).cast(pl.String))
            .unique()
            .collect()
            .get_column(right_partition_key)
            .drop_nulls()
            .to_list()
        )
        for value in values:
            key_plan.setdefault(str(value), [[]])


def _build_right_key_file_index(
    right_sources: list[dict[str, Any]],
    *,
    right_partition_key: str,
    candidate_root: Path,
    logical_plan_hash: str,
) -> tuple[
    dict[
        str,
        dict[tuple[str | None, tuple[str, ...]], dict[str, set[int]]],
    ],
    int,
    dict[str, Any],
]:
    manifest_path = candidate_root.parent / f"{candidate_root.name}.manifest.json"
    previous = _read_json_mapping(manifest_path)
    expected_contract = {
        "version": RIGHT_INDEX_MANIFEST_VERSION,
        "logical_plan_hash": logical_plan_hash,
        "right_partition_key": right_partition_key,
    }
    full_rebuild = any(previous.get(key) != value for key, value in expected_contract.items())
    if full_rebuild:
        reset_path(candidate_root)
        ensure_dir(candidate_root)
        previous_sources: dict[str, Any] = {}
    else:
        previous_sources = previous.get("sources") or {}
    index: dict[
        str,
        dict[tuple[str | None, tuple[str, ...]], dict[str, set[int]]],
    ] = {}
    indexed_rows = 0
    source_entries: dict[str, Any] = {}
    rebuilt_files = 0
    reused_files = 0
    indexed_match_rows = 0
    max_key_rows = 0
    for source in right_sources:
        source_index: dict[tuple[str | None, tuple[str, ...]], dict[str, set[int]]] = {}
        right_on = list(source["right_on"])
        if not right_on:
            index[str(source["name"])] = source_index
            continue
        columns = list(
            dict.fromkeys([*right_on, *([right_partition_key] if right_partition_key else [])])
        )
        for path_text in source["files"]:
            path = Path(path_text)
            resolved_path = str(path.resolve())
            source_id = f"{source['name']}\u001f{resolved_path}"
            fingerprint = file_fingerprint(DatasetFileShim(resolved_path))
            candidate_path = candidate_root / (
                hashlib.sha256(source_id.encode("utf-8")).hexdigest() + ".parquet"
            )
            previous_entry = previous_sources.get(source_id) or {}
            schema_names = pl.read_parquet_schema(path).names()
            if any(column not in schema_names for column in right_on):
                continue
            can_reuse = (
                not full_rebuild
                and previous_entry.get("fingerprint") == fingerprint
                and candidate_path.is_file()
                and candidate_path.stat().st_size > 0
            )
            if can_reuse:
                candidate = pl.read_parquet(candidate_path)
                reused_files += 1
            else:
                candidate = _build_right_key_candidate(
                    path,
                    columns=columns,
                    right_on=right_on,
                    right_partition_key=right_partition_key,
                )
                _atomic_write_frame(candidate, candidate_path)
                rebuilt_files += 1
            indexed_rows += candidate.height
            if "__match_rows" in candidate.columns:
                indexed_match_rows += int(candidate.get_column("__match_rows").sum() or 0)
                max_key_rows = max(
                    max_key_rows,
                    int(candidate.get_column("__match_rows").max() or 0),
                )
            for row in candidate.iter_rows(named=True):
                key = tuple(json.loads(str(row["__key_json"])))
                partition = row["__partition"]
                source_index.setdefault((partition, key), {}).setdefault(str(path), set()).add(
                    int(row["__row_group"])
                )
            source_entries[source_id] = {
                "source_name": str(source["name"]),
                "source_path": resolved_path,
                "fingerprint": fingerprint,
                "candidate_path": str(candidate_path),
                "rows": candidate.height,
                "size_bytes": candidate_path.stat().st_size,
            }
        index[str(source["name"])] = source_index
    deleted_ids = sorted(set(previous_sources) - set(source_entries))
    for source_id in deleted_ids:
        stale_path = Path(str((previous_sources.get(source_id) or {}).get("candidate_path") or ""))
        if stale_path.is_file():
            reset_path(stale_path)
    manifest = {**expected_contract, "sources": source_entries}
    _atomic_write_mapping(manifest_path, manifest)
    profile = {
        "manifest_path": str(manifest_path),
        "manifest_version": RIGHT_INDEX_MANIFEST_VERSION,
        "full_rebuild": full_rebuild,
        "rebuilt_source_files": rebuilt_files,
        "reused_source_files": reused_files,
        "deleted_source_files": len(deleted_ids),
        "candidate_files": len(source_entries),
        "candidate_rows": indexed_rows,
        "distinct_partition_key_row_groups": indexed_rows,
        "matched_payload_rows": indexed_match_rows,
        "max_rows_per_key_row_group": max_key_rows,
        "candidate_bytes": sum(int(item["size_bytes"]) for item in source_entries.values()),
    }
    return index, indexed_rows, profile


def _build_right_key_candidate(
    path: Path,
    *,
    columns: list[str],
    right_on: list[str],
    right_partition_key: str,
) -> pl.DataFrame:
    parquet = pq.ParquetFile(path)
    rows: list[dict[str, Any]] = []
    for row_group_id in range(parquet.metadata.num_row_groups):
        thin = (
            pl.from_arrow(parquet.read_row_group(row_group_id, columns=columns))
            .group_by(columns, maintain_order=True)
            .agg(pl.len().alias("__match_rows"))
        )
        for row in thin.iter_rows(named=True):
            if any(row[column] is None for column in right_on):
                continue
            rows.append(
                {
                    "__partition": (
                        str(row[right_partition_key])
                        if right_partition_key and row[right_partition_key] is not None
                        else None
                    ),
                    "__key_json": json.dumps(
                        [_canonical_key_value(row[column]) for column in right_on],
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ),
                    "__row_group": row_group_id,
                    "__match_rows": int(row["__match_rows"]),
                }
            )
    if rows:
        return (
            pl.DataFrame(rows)
            .group_by(["__partition", "__key_json", "__row_group"])
            .agg(pl.col("__match_rows").sum())
        )
    return pl.DataFrame(
        schema={
            "__partition": pl.String,
            "__key_json": pl.String,
            "__row_group": pl.Int64,
            "__match_rows": pl.Int64,
        }
    )


def _read_json_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _atomic_write_frame(frame: pl.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.write_parquet(temp_path, compression="uncompressed")
        os.replace(temp_path, path)
    finally:
        reset_path(temp_path)


def _atomic_write_mapping(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp_path, path)
    finally:
        reset_path(temp_path)


def _choose_source_selection(
    *,
    left_files: list[Any],
    right_sources: list[dict[str, Any]],
    partition_values: list[str],
    parquet_profiles: dict[str, Any],
    memory_budget_bytes: int,
    max_source_files_per_task: int,
    artifact_root: Path,
    join_multiplicity_profile: dict[str, Any],
) -> dict[str, Any]:
    source_files = {
        "left": [str(item.path) for item in left_files],
        **{
            str(source["name"]): [str(path) for path in source["files"]] for source in right_sources
        },
    }
    total_source_files = sum(len(paths) for paths in source_files.values())
    total_index_scan_bytes = sum(
        profile.estimated_compressed_bytes() for profile in parquet_profiles.values()
    )
    locality_sources: dict[str, Any] = {}
    complete_locality = bool(partition_values)
    for source_name, paths in source_files.items():
        assignments: dict[str, str] = {}
        unassigned: list[str] = []
        ambiguous: list[str] = []
        for path_text in paths:
            matches = [
                value
                for value in partition_values
                if _path_has_partition_prefix(Path(path_text), value)
            ]
            if len(matches) == 1:
                assignments[path_text] = matches[0]
            elif matches:
                ambiguous.append(path_text)
            else:
                unassigned.append(path_text)
        source_complete = len(assignments) == len(paths) and not ambiguous
        complete_locality = complete_locality and source_complete
        locality_sources[source_name] = {
            "files": len(paths),
            "assigned_files": len(assignments),
            "unassigned_files": len(unassigned),
            "ambiguous_files": len(ambiguous),
            "complete": source_complete,
        }

    partition_costs: dict[str, Any] = {}
    max_files = 0
    max_row_groups = 0
    max_compressed = 0
    max_uncompressed = 0
    max_estimated_state = 0
    cumulative_multiplier = max(
        1,
        int(join_multiplicity_profile.get("cumulative_max_multiplier") or 1),
    )
    total_rows = max(1, sum(profile.rows for profile in parquet_profiles.values()))
    estimated_joined_row_width = max(
        1,
        sum(
            group.uncompressed_bytes
            for profile in parquet_profiles.values()
            for group in profile.row_groups
        )
        // total_rows,
    )
    for partition_value in partition_values:
        left_partition_paths = [
            item.path
            for item in _select_partition_named_files(
                [DatasetFileShim(str(item.path)) for item in left_files],
                partition_value=partition_value,
            )
        ]
        paths = list(left_partition_paths)
        for source in right_sources:
            paths.extend(
                item.path
                for item in _select_partition_named_files(
                    [DatasetFileShim(path) for path in source["files"]],
                    partition_value=partition_value,
                )
            )
        profiles = [parquet_profiles[str(path.resolve())] for path in paths]
        file_count = len(profiles)
        row_group_count = sum(len(profile.row_groups) for profile in profiles)
        compressed_bytes = sum(
            group.compressed_bytes for profile in profiles for group in profile.row_groups
        )
        uncompressed_bytes = sum(
            group.uncompressed_bytes for profile in profiles for group in profile.row_groups
        )
        left_rows = sum(parquet_profiles[str(path.resolve())].rows for path in left_partition_paths)
        estimated_output_rows = left_rows * cumulative_multiplier
        estimated_state_bytes = (
            uncompressed_bytes + estimated_output_rows * estimated_joined_row_width
        )
        max_files = max(max_files, file_count)
        max_row_groups = max(max_row_groups, row_group_count)
        max_compressed = max(max_compressed, compressed_bytes)
        max_uncompressed = max(max_uncompressed, uncompressed_bytes)
        max_estimated_state = max(max_estimated_state, estimated_state_bytes)
        partition_costs[partition_value] = {
            "files": file_count,
            "row_groups": row_group_count,
            "compressed_bytes": compressed_bytes,
            "uncompressed_bytes": uncompressed_bytes,
            "left_rows": left_rows,
            "estimated_output_rows": estimated_output_rows,
            "estimated_state_bytes": estimated_state_bytes,
        }

    direct_rejections: list[str] = []
    if not complete_locality:
        direct_rejections.append("partition_locality_incomplete")
    if max_files > max_source_files_per_task:
        direct_rejections.append("source_file_fanout_over_limit")
    if max_estimated_state > memory_budget_bytes:
        direct_rejections.append("estimated_state_over_memory_budget")
    selected = "partition_local" if not direct_rejections else "coordinate_index"
    index_manifests = [
        artifact_root / "left_key_index.manifest.json",
        artifact_root / "right_key_index.manifest.json",
    ]
    estimated_partition_processes = max(1, len(partition_values))
    estimated_index_candidate_write_bytes = int(join_multiplicity_profile.get("candidate_bytes", 0))
    indexed_span_reduction_possible = not complete_locality or max_files < total_source_files
    return {
        "selected": selected,
        "candidate_costs": {
            "partition_local": {
                "feasible": not direct_rejections,
                "max_source_files": max_files,
                "max_row_groups": max_row_groups,
                "max_compressed_bytes": max_compressed,
                "max_uncompressed_bytes": max_uncompressed,
                "max_estimated_state_bytes": max_estimated_state,
                "cumulative_max_multiplier": cumulative_multiplier,
                "estimated_joined_row_width": estimated_joined_row_width,
                "estimated_read_bytes": max_compressed,
                "estimated_file_opens": max_files,
                "estimated_processes": estimated_partition_processes,
                "partition_costs": partition_costs,
            },
            "coordinate_index": {
                "feasible": True,
                "index_cache_manifests_present": sum(path.is_file() for path in index_manifests),
                "estimated_index_scan_bytes": total_index_scan_bytes,
                "estimated_index_write_bytes": estimated_index_candidate_write_bytes,
                "estimated_file_opens": total_source_files,
                "estimated_processes": estimated_partition_processes,
                "estimated_state_bytes_upper_bound": max_estimated_state,
                "span_reduction_possible": indexed_span_reduction_possible,
                "index_build_is_net_loss": not indexed_span_reduction_possible,
            },
        },
        "candidate_rejections": {
            "partition_local": direct_rejections,
            "coordinate_index": [],
        },
        "partition_locality": {
            "complete": complete_locality,
            "partition_values": partition_values,
            "sources": locality_sources,
        },
    }


def _profile_join_multiplicity(right_sources: list[dict[str, Any]]) -> dict[str, Any]:
    sources: dict[str, Any] = {}
    cumulative_multiplier = 1
    total_candidate_bytes = 0
    for source in right_sources:
        right_on = [str(column) for column in source.get("right_on") or []]
        how = str(source.get("how") or "left")
        source_files = [DatasetFileShim(path) for path in source["files"]]
        total_candidate_bytes += sum(int(item.size_bytes) for item in source_files)
        if how == "cross":
            rows = int(
                scan_parquet_files_union_by_name(source_files).select(pl.len()).collect().item()
            )
            profile = {
                "join_type": how,
                "join_keys": [],
                "rows": rows,
                "distinct_non_null_keys": None,
                "max_rows_per_key": rows,
            }
        else:
            right = scan_parquet_files_union_by_name(source_files)
            counts = (
                right.select(right_on)
                .filter(pl.all_horizontal([pl.col(column).is_not_null() for column in right_on]))
                .group_by(right_on)
                .len()
                .select(
                    pl.len().alias("distinct_non_null_keys"),
                    pl.col("len").sum().alias("matched_rows"),
                    pl.col("len").max().alias("max_rows_per_key"),
                )
                .collect()
            )
            profile = {
                "join_type": how,
                "join_keys": right_on,
                "rows": int(counts["matched_rows"][0] or 0),
                "distinct_non_null_keys": int(counts["distinct_non_null_keys"][0] or 0),
                "max_rows_per_key": int(counts["max_rows_per_key"][0] or 0),
            }
        multiplier = max(1, int(profile["max_rows_per_key"] or 1))
        cumulative_multiplier *= multiplier
        sources[str(source["name"])] = profile
    return {
        "sources": sources,
        "cumulative_max_multiplier": cumulative_multiplier,
        "candidate_bytes": total_candidate_bytes,
    }


def _unused_index_profile(reason: str) -> dict[str, Any]:
    return {
        "used": False,
        "skip_reason": reason,
        "full_rebuild": False,
        "rebuilt_source_files": 0,
        "reused_source_files": 0,
        "deleted_source_files": 0,
        "candidate_files": 0,
        "candidate_rows": 0,
        "candidate_bytes": 0,
    }


def _index_cache_state(
    source_selection_strategy: str,
    *,
    left_index_profile: dict[str, Any],
    right_index_profile: dict[str, Any],
) -> str:
    if source_selection_strategy == "partition_local":
        return "not_used"
    rebuilt = int(left_index_profile.get("rebuilt_source_files") or 0) + int(
        right_index_profile.get("rebuilt_source_files") or 0
    )
    reused = int(left_index_profile.get("reused_source_files") or 0) + int(
        right_index_profile.get("reused_source_files") or 0
    )
    if rebuilt and reused:
        return "partial_rebuild"
    if rebuilt:
        return "cold_build"
    return "warm_reuse" if reused else "empty"


def _select_partition_local_right_sources(
    right_sources: list[dict[str, Any]],
    *,
    partition_value: str,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for source in right_sources:
        payload = dict(source)
        payload["files"] = [
            str(item.path)
            for item in _select_partition_named_files(
                [DatasetFileShim(path) for path in source["files"]],
                partition_value=partition_value,
            )
        ]
        payload["row_groups"] = {}
        selected.append(payload)
    return selected


def _select_right_sources_for_task(
    right_sources: list[dict[str, Any]],
    *,
    key_rows: list[dict[str, Any]],
    partition_value: str,
    right_partition_key: str,
    key_file_index: dict[
        str,
        dict[tuple[str | None, tuple[str, ...]], dict[str, set[int]]],
    ],
) -> list[dict[str, Any]]:
    selected_sources: list[dict[str, Any]] = []
    for source in right_sources:
        source_payload = dict(source)
        candidate_files = [DatasetFileShim(path) for path in source["files"]]
        if not right_partition_key:
            candidate_files = _select_partition_named_files(
                candidate_files,
                partition_value=partition_value,
            )
        candidate_paths = {str(item.path) for item in candidate_files}
        left_on = list(source["left_on"])
        if (
            key_rows
            and left_on
            and all(all(column in row for column in left_on) for row in key_rows)
        ):
            wanted = {
                tuple(_canonical_key_value(row[column]) for column in left_on)
                for row in key_rows
                if all(row[column] is not None for column in left_on)
            }
            partition_key = partition_value if right_partition_key else None
            matching_spans: dict[str, set[int]] = {}
            for key in wanted:
                for path, row_groups in (
                    key_file_index.get(str(source["name"]), {})
                    .get((partition_key, key), {})
                    .items()
                ):
                    matching_spans.setdefault(path, set()).update(row_groups)
            narrowed = candidate_paths.intersection(matching_spans)
            if narrowed:
                candidate_paths = narrowed
                source_payload["row_groups"] = {
                    path: sorted(matching_spans[path]) for path in sorted(narrowed)
                }
        source_payload["files"] = sorted(candidate_paths)
        selected_sources.append(source_payload)
    return selected_sources


def _select_left_spans_for_task(
    files: list[DatasetFileShim],
    *,
    key_rows: list[dict[str, Any]],
    partition_value: str,
    left_on: list[str],
    key_file_index: dict[
        str,
        dict[tuple[str | None, tuple[str, ...]], dict[str, set[int]]],
    ],
) -> tuple[list[DatasetFileShim], dict[str, list[int]]]:
    candidates = _select_partition_named_files(files, partition_value=partition_value)
    candidate_by_path = {str(item.path): item for item in candidates}
    if not key_rows or not left_on:
        return candidates, {}
    wanted = {
        tuple(_canonical_key_value(row[column]) for column in left_on)
        for row in key_rows
        if all(column in row and row[column] is not None for column in left_on)
    }
    matching_spans: dict[str, set[int]] = {}
    for key in wanted:
        for path, row_groups in (
            key_file_index.get("__left", {}).get((partition_value, key), {}).items()
        ):
            matching_spans.setdefault(path, set()).update(row_groups)
    narrowed = set(candidate_by_path).intersection(matching_spans)
    if not narrowed:
        return candidates, {}
    return (
        [candidate_by_path[path] for path in sorted(narrowed)],
        {path: sorted(matching_spans[path]) for path in sorted(narrowed)},
    )


def _canonical_key_value(value: Any) -> str:
    return f"{type(value).__name__}:{value}"


def _build_0301_physical_plan(
    tasks: list[TaskSpec],
    *,
    left_files: list[Any],
    right_sources: list[dict[str, Any]],
    logical_plan_hash: str,
    right_partition_key: str,
    key_groups_per_part: int,
    memory_budget_mb: int,
    max_source_files_per_task: int,
    parquet_profiles: dict[str, Any],
    source_selection_strategy: str,
):
    physical_tasks: list[PhysicalTask] = []
    for task in tasks:
        left_row_groups = task.payload.get("left_row_groups") or {}
        spans = [
            SourceSpan(
                source_name="left",
                path=str(item.path),
                size_bytes=int(item.size_bytes),
                estimated_read_bytes=parquet_profiles[
                    str(item.path.resolve())
                ].estimated_compressed_bytes(
                    row_group_ids=_planned_span_row_groups(
                        item.path,
                        selected=left_row_groups,
                        parquet_profiles=parquet_profiles,
                    )
                ),
                estimated_uncompressed_bytes=parquet_profiles[
                    str(item.path.resolve())
                ].estimated_uncompressed_bytes(
                    row_group_ids=_planned_span_row_groups(
                        item.path,
                        selected=left_row_groups,
                        parquet_profiles=parquet_profiles,
                    )
                ),
                row_groups=_planned_span_row_groups(
                    item.path,
                    selected=left_row_groups,
                    parquet_profiles=parquet_profiles,
                ),
            )
            for item in [DatasetFileShim(path) for path in task.payload["left_files"]]
        ]
        for source in task.payload["right_sources"]:
            files = [DatasetFileShim(path) for path in source["files"]]
            if not right_partition_key:
                files = _select_partition_named_files(
                    files,
                    partition_value=str(task.partition_value),
                )
            spans.extend(
                SourceSpan(
                    source_name=str(source["name"]),
                    path=str(item.path),
                    size_bytes=int(item.size_bytes),
                    estimated_read_bytes=parquet_profiles[
                        str(item.path.resolve())
                    ].estimated_compressed_bytes(
                        row_group_ids=_planned_span_row_groups(
                            item.path,
                            selected=source.get("row_groups") or {},
                            parquet_profiles=parquet_profiles,
                        )
                    ),
                    estimated_uncompressed_bytes=parquet_profiles[
                        str(item.path.resolve())
                    ].estimated_uncompressed_bytes(
                        row_group_ids=_planned_span_row_groups(
                            item.path,
                            selected=source.get("row_groups") or {},
                            parquet_profiles=parquet_profiles,
                        )
                    ),
                    row_groups=_planned_span_row_groups(
                        item.path,
                        selected=source.get("row_groups") or {},
                        parquet_profiles=parquet_profiles,
                    ),
                )
                for item in files
            )
        state_estimate = sum(
            span.estimated_uncompressed_bytes
            if span.estimated_uncompressed_bytes is not None
            else span.size_bytes
            for span in spans
        )
        expected_input_rows = (
            int(task.payload["expected_left_rows"])
            if task.payload.get("expected_left_rows") is not None
            else None
        )
        expected_payload_bytes = sum(
            span.estimated_read_bytes if span.estimated_read_bytes is not None else span.size_bytes
            for span in spans
        )
        file_fanout = len(spans)
        row_group_fanout = sum(len(span.row_groups) if span.row_groups else 1 for span in spans)
        budget_bytes = memory_budget_mb * 1024 * 1024
        risk = "bounded"
        if state_estimate > budget_bytes:
            risk = "over_budget"
        elif file_fanout > max_source_files_per_task:
            risk = "fanout_over_limit"
        execution_group_hint = _estimate_partition_local_execution_group_hint(
            source_selection_strategy=source_selection_strategy,
            state_estimate_bytes=state_estimate,
            file_fanout=file_fanout,
            row_group_fanout=row_group_fanout,
            memory_budget_bytes=budget_bytes,
        )
        physical_tasks.append(
            PhysicalTask(
                task_id=task.task_id,
                partition_value=str(task.partition_value),
                batch_index=None,
                part_index=int(task.part_index or 0),
                single_partition_guaranteed=True,
                source_spans=tuple(spans),
                expected_input_rows=expected_input_rows,
                # Duplicate right keys can expand any keyed join, so an exact output
                # estimate remains unknown until right-key multiplicity is profiled.
                expected_output_rows=None,
                expected_payload_bytes=expected_payload_bytes,
                file_fanout=file_fanout,
                row_group_fanout=row_group_fanout,
                state_estimate_bytes=state_estimate,
                expected_spawn_overhead_units=_estimate_spawn_overhead_units(
                    expected_input_rows=expected_input_rows,
                    file_fanout=file_fanout,
                    row_group_fanout=row_group_fanout,
                ),
                execution_group_hint=execution_group_hint,
                risk=risk,
            )
        )
    plan = build_physical_plan(
        logical_plan_hash=logical_plan_hash,
        tasks=physical_tasks,
        decisions=[
            {
                "decision": "partition_join_key_group_parts",
                "target_key_groups_per_part": key_groups_per_part,
                "source_span_bytes_are_upper_bound": True,
                "join_state_estimate": "unknown",
                "memory_budget_mb": memory_budget_mb,
                "max_source_files_per_task": max_source_files_per_task,
                "source_selection_strategy": source_selection_strategy,
                "partition_local_execution_grouping": (
                    "eligible" if source_selection_strategy == "partition_local" else "disabled"
                ),
            }
        ],
    )
    return plan, _summarize_input_profiles(
        parquet_profiles,
        left_paths=[item.path for item in left_files],
        right_sources=right_sources,
    )


def _summarize_input_profiles(
    parquet_profiles: dict[str, Any],
    *,
    left_paths: list[Path],
    right_sources: list[dict[str, Any]],
) -> dict[str, Any]:
    def summarize(paths: list[Path]) -> dict[str, int]:
        profiles = [parquet_profiles[str(path.resolve())] for path in paths]
        return {
            "files": len(profiles),
            "row_groups": sum(len(profile.row_groups) for profile in profiles),
            "rows": sum(profile.rows for profile in profiles),
            "file_size_bytes": sum(profile.file_size_bytes for profile in profiles),
            "compressed_bytes": sum(
                group.compressed_bytes for profile in profiles for group in profile.row_groups
            ),
            "uncompressed_bytes": sum(
                group.uncompressed_bytes for profile in profiles for group in profile.row_groups
            ),
        }

    right_profiles = {
        str(source["name"]): summarize([Path(path) for path in source["files"]])
        for source in right_sources
    }
    all_right_paths = [Path(path) for source in right_sources for path in source["files"]]
    return {
        "left": summarize(left_paths),
        "right": summarize(all_right_paths),
        "right_sources": right_profiles,
        "total": summarize([*left_paths, *all_right_paths]),
    }


def _summarize_task_fanout(tasks: tuple[PhysicalTask, ...]) -> dict[str, Any]:
    if not tasks:
        return {
            "tasks": 0,
            "file_fanout": {"min": 0, "avg": 0.0, "max": 0},
            "row_group_fanout": {"min": 0, "avg": 0.0, "max": 0},
        }

    def summary(values: list[int]) -> dict[str, int | float]:
        return {
            "min": min(values),
            "avg": sum(values) / len(values),
            "max": max(values),
        }

    return {
        "tasks": len(tasks),
        "file_fanout": summary([task.file_fanout for task in tasks]),
        "row_group_fanout": summary([task.row_group_fanout for task in tasks]),
        "estimated_read_bytes": summary([int(task.expected_payload_bytes or 0) for task in tasks]),
        "state_estimate_bytes": summary([int(task.state_estimate_bytes or 0) for task in tasks]),
        "expected_spawn_overhead_units": summary(
            [int(task.expected_spawn_overhead_units or 0) for task in tasks]
        ),
        "execution_group_hint": summary([int(task.execution_group_hint or 1) for task in tasks]),
    }


def _estimate_spawn_overhead_units(
    *,
    expected_input_rows: int | None,
    file_fanout: int,
    row_group_fanout: int,
) -> int:
    row_units = max(0, int(expected_input_rows or 0)) // 64
    return 2_048 + row_units + max(1, file_fanout) * 192 + max(1, row_group_fanout) * 24


def _choose_join_input_batch_rows(
    task: PhysicalTask,
    *,
    memory_budget_bytes: int,
    fallback_rows: int,
) -> int:
    """Choose an internal left-side batch bound without expanding the YAML contract."""
    target_batch_bytes = max(
        8 * 1024 * 1024,
        min(64 * 1024 * 1024, memory_budget_bytes // 64),
    )
    expected_rows = int(task.expected_input_rows or 0)
    state_bytes = int(task.state_estimate_bytes or 0)
    if expected_rows > 0 and state_bytes > 0:
        estimated_bytes_per_row = max(1, state_bytes // expected_rows)
        rows = target_batch_bytes // estimated_bytes_per_row
    else:
        rows = fallback_rows
    return max(1_024, min(65_536, int(rows)))


def _estimate_partition_local_execution_group_hint(
    *,
    source_selection_strategy: str,
    state_estimate_bytes: int,
    file_fanout: int,
    row_group_fanout: int,
    memory_budget_bytes: int,
) -> int:
    if source_selection_strategy != "partition_local":
        return 1
    if file_fanout > 16:
        return 1
    if state_estimate_bytes <= 0:
        return 1
    if state_estimate_bytes > max(8 * 1024 * 1024, memory_budget_bytes * 4 // 5):
        return 1
    # Reused children execute partitions sequentially, so row-group count and
    # per-task state are not additive. RSS is checked after every completed task.
    return 4


def _plan_partition_local_task_batches(
    *,
    dirty_tasks: list[TaskSpec],
    physical_by_id: dict[str, PhysicalTask],
    source_selection_strategy: str,
    memory_budget_bytes: int,
    memory_safety_ratio: float,
    max_tasks_per_child: int | None,
    admitted_workers: int,
) -> dict[str, Any]:
    if os.environ.get("SMOKING_DATA_DISABLE_PARTITION_LOCAL_BATCHING") == "1":
        return {
            "task_batches": None,
            "profile": {
                "enabled": False,
                "reason": "disabled_by_env",
                "batch_count": 0,
                "batched_tasks": len(dirty_tasks),
                "max_tasks_per_batch": 1,
                "target_batch_state_bytes": 0,
            },
        }
    if source_selection_strategy != "partition_local":
        return {
            "task_batches": None,
            "profile": {
                "enabled": False,
                "reason": "coordinate_index",
                "batch_count": 0,
                "batched_tasks": 0,
                "max_tasks_per_batch": 1,
                "target_batch_state_bytes": 0,
            },
        }
    if len(dirty_tasks) <= 1:
        return {
            "task_batches": None,
            "profile": {
                "enabled": False,
                "reason": "single_dirty_task",
                "batch_count": len(dirty_tasks),
                "batched_tasks": len(dirty_tasks),
                "max_tasks_per_batch": 1,
                "target_batch_state_bytes": 0,
            },
        }
    task_limit = min(4, max_tasks_per_child or 4)
    if task_limit <= 1:
        return {
            "task_batches": None,
            "profile": {
                "enabled": False,
                "reason": "max_tasks_per_child_one",
                "batch_count": len(dirty_tasks),
                "batched_tasks": len(dirty_tasks),
                "max_tasks_per_batch": 1,
                "target_batch_state_bytes": 0,
            },
        }
    target_batch_state_bytes = max(
        8 * 1024 * 1024,
        int(memory_budget_bytes * memory_safety_ratio / max(1, admitted_workers)),
    )
    eligible_tasks: list[TaskSpec] = []
    singleton_batches: list[list[TaskSpec]] = []
    for task in dirty_tasks:
        physical = physical_by_id[task.task_id]
        state_estimate = int(physical.state_estimate_bytes or 0)
        eligible = (
            physical.execution_group_hint > 1
            and physical.risk == "bounded"
            and state_estimate > 0
            and state_estimate <= target_batch_state_bytes
        )
        if not eligible:
            singleton_batches.append([task])
            continue
        eligible_tasks.append(task)
    desired_batch_count = min(
        len(eligible_tasks),
        max(
            min(max(1, admitted_workers), len(eligible_tasks)),
            (len(eligible_tasks) + task_limit - 1) // task_limit,
        ),
    )
    balanced_batches = [[] for _ in range(desired_batch_count)]
    for index, task in enumerate(eligible_tasks):
        balanced_batches[index % desired_batch_count].append(task)
    batches = [*balanced_batches, *singleton_batches]
    batches = [batch for batch in batches if batch]
    max_tasks_per_batch = max((len(batch) for batch in batches), default=1)
    enabled = bool(batches)
    return {
        "task_batches": batches if enabled else None,
        "profile": {
            "enabled": enabled,
            "reason": "worker_balanced_partition_local" if enabled else "no_eligible_tasks",
            "admitted_workers": admitted_workers,
            "batch_count": len(batches),
            "batched_tasks": sum(len(batch) for batch in batches),
            "max_tasks_per_batch": max_tasks_per_batch,
            "reused_task_slots": sum(max(0, len(batch) - 1) for batch in batches),
            "target_batch_state_bytes": target_batch_state_bytes,
            "batches": [
                {
                    "task_ids": [task.task_id for task in batch],
                    "partitions": [str(task.partition_value) for task in batch],
                    "tasks": len(batch),
                    "state_estimate_bytes": sum(
                        int(physical_by_id[task.task_id].state_estimate_bytes or 0)
                        for task in batch
                    ),
                    "peak_sequential_state_estimate_bytes": max(
                        int(physical_by_id[task.task_id].state_estimate_bytes or 0)
                        for task in batch
                    ),
                }
                for batch in batches
            ],
        },
    }


def _rebind_partition_local_task_batches(
    partition_local_batching_plan: dict[str, Any],
    *,
    dirty_tasks: list[TaskSpec],
) -> dict[str, Any]:
    task_batches = partition_local_batching_plan.get("task_batches")
    if not task_batches:
        return partition_local_batching_plan
    task_map = {task.task_id: task for task in dirty_tasks}
    rebound_batches = [
        [task_map[task.task_id] for task in batch if task.task_id in task_map]
        for batch in task_batches
    ]
    rebound_batches = [batch for batch in rebound_batches if batch]
    return {**partition_local_batching_plan, "task_batches": rebound_batches or None}


def _summarize_task_counter(
    task_results: list[TaskResult],
    counter_name: str,
) -> dict[str, int | float]:
    values = [
        float(result.counters[counter_name])
        for result in task_results
        if counter_name in result.counters
    ]
    if not values:
        return {"tasks": 0, "sum_sec": 0.0, "avg_sec": 0.0, "max_sec": 0.0}
    return {
        "tasks": len(values),
        "sum_sec": sum(values),
        "avg_sec": sum(values) / len(values),
        "max_sec": max(values),
    }


def _summarize_task_metric(
    task_results: list[TaskResult],
    counter_name: str,
) -> dict[str, int | float]:
    values = [
        float(result.counters[counter_name])
        for result in task_results
        if counter_name in result.counters
    ]
    if not values:
        return {"tasks": 0, "sum": 0.0, "avg": 0.0, "max": 0.0}
    return {
        "tasks": len(values),
        "sum": sum(values),
        "avg": sum(values) / len(values),
        "max": max(values),
    }


def _summarize_bounded_join(task_results: list[TaskResult]) -> dict[str, Any]:
    enabled = [
        result
        for result in task_results
        if float(result.counters.get("bounded_join_enabled", 0)) > 0
    ]
    return {
        "schema_version": BOUNDED_JOIN_STRATEGY_VERSION,
        "tasks": len(task_results),
        "enabled_tasks": len(enabled),
        "fallback_tasks": len(task_results) - len(enabled),
        "input_batches": int(
            sum(result.counters.get("bounded_input_batches", 0) for result in enabled)
        ),
        "configured_input_batch_rows": _summarize_task_metric(
            enabled, "configured_input_batch_rows"
        ),
        "peak_input_batch_rows": _summarize_task_metric(enabled, "peak_input_batch_rows"),
        "peak_output_batch_rows": _summarize_task_metric(enabled, "peak_output_batch_rows"),
        "writer_dictionary_disabled_columns": _summarize_task_metric(
            enabled, "writer_dictionary_disabled_columns"
        ),
        "right_key_index_builds": _summarize_task_metric(enabled, "right_key_index_builds"),
        "right_key_index_unique_keys": _summarize_task_metric(
            enabled, "right_key_index_unique_keys"
        ),
        "right_key_index_rows": _summarize_task_metric(enabled, "right_key_index_rows"),
        "right_key_index_reuses": _summarize_task_metric(enabled, "right_key_index_reuses"),
        "join_key_representation": {
            "binary_row_sources": _summarize_task_metric(
                enabled, "binary_row_key_sources"
            ),
            "display_sources": _summarize_task_metric(enabled, "display_key_sources"),
        },
        "phase_elapsed_sec": {
            name: _summarize_task_counter(enabled, name)
            for name in (
                "right_read_sec",
                "left_schema_sec",
                "left_read_sec",
                "left_preprocess_sec",
                "join_compute_sec",
                "post_operation_sec",
                "parquet_write_sec",
                "writer_close_sec",
                "output_commit_sec",
            )
        },
        "join_kernel_phase_elapsed_sec": {
            name: _summarize_task_counter(enabled, name)
            for name in (
                "right_key_map_build_sec",
                "left_key_encode_sec",
                "hash_probe_sec",
                "result_materialize_sec",
                "join_kernel_unattributed_sec",
            )
        },
        "materialization_phase_elapsed_sec": {
            name: _summarize_task_counter(enabled, name)
            for name in (
                "join_index_array_build_sec",
                "left_take_sec",
                "join_key_zip_sec",
                "right_take_sec",
                "record_batch_build_sec",
                "result_materialize_unattributed_sec",
            )
        },
        "materialization_reuse": {
            "left_identity_batches": _summarize_task_metric(
                enabled, "left_identity_reuse_batches"
            ),
            "right_identity_batches": _summarize_task_metric(
                enabled, "right_identity_reuse_batches"
            ),
        },
        "scan_phase_elapsed_sec": {
            name: _summarize_task_counter(enabled, name)
            for name in (
                "left_reader_setup_sec",
                "left_parquet_decode_sec",
                "left_schema_align_sec",
                "left_read_unattributed_sec",
            )
        },
        "preprocess_phase_elapsed_sec": {
            name: _summarize_task_counter(enabled, name)
            for name in (
                "left_select_sec",
                "left_partition_filter_sec",
                "left_key_filter_sec",
                "left_preprocess_unattributed_sec",
            )
        },
        "preprocess_reuse": {
            "key_filter_identity_batches": _summarize_task_metric(
                enabled, "left_key_filter_identity_batches"
            ),
            "key_filter_skipped_batches": _summarize_task_metric(
                enabled, "left_key_filter_skipped_batches"
            ),
        },
        "parquet_projection": {
            "sources": _summarize_task_metric(enabled, "parquet_projection_sources"),
            "total_columns": _summarize_task_metric(enabled, "parquet_total_columns"),
            "projected_columns": _summarize_task_metric(
                enabled, "parquet_projected_columns"
            ),
        },
        "parquet_decode_throughput": {
            "batches": _summarize_task_metric(enabled, "left_parquet_decode_batches"),
            "rows": _summarize_task_metric(enabled, "left_parquet_decode_rows"),
            "compressed_bytes": _summarize_task_metric(
                enabled, "left_parquet_compressed_bytes"
            ),
            "rows_per_sec": _summarize_task_metric(
                enabled, "left_parquet_decode_rows_per_sec"
            ),
            "mib_per_sec": _summarize_task_metric(
                enabled, "left_parquet_decode_mib_per_sec"
            ),
        },
        "fallback_reasons": (
            []
            if len(enabled) == len(task_results)
            else ["unsupported_join_semantics_or_complete_input_operation"]
        ),
    }


def _summarize_task_process_profile(task_results: list[TaskResult]) -> dict[str, Any]:
    by_pid: dict[int, list[TaskResult]] = {}
    for result in task_results:
        if result.pid <= 0:
            continue
        by_pid.setdefault(int(result.pid), []).append(result)
    if not by_pid:
        return {
            "processes": 0,
            "tasks_per_process": {"count": 0, "min": 0, "avg": 0.0, "max": 0},
            "queue_wait_sec": {"count": 0, "min": 0.0, "avg": 0.0, "max": 0.0},
            "child_start_latency_sec": {"count": 0, "min": 0.0, "avg": 0.0, "max": 0.0},
            "processes_detail": [],
        }

    tasks_per_process = [len(items) for items in by_pid.values()]
    queue_wait_values: list[float] = []
    child_start_values: list[float] = []
    processes_detail: list[dict[str, Any]] = []
    for pid, items in sorted(by_pid.items()):
        ordinals = [
            int(item.counters.get("task_ordinal", 0))
            for item in items
            if int(item.counters.get("task_ordinal", 0)) > 0
        ]
        queue_values = [
            float(item.counters["queue_wait_sec"])
            for item in items
            if "queue_wait_sec" in item.counters
        ]
        start_values = [
            float(item.counters["child_start_latency_sec"])
            for item in items
            if "child_start_latency_sec" in item.counters
        ]
        queue_wait_values.extend(queue_values)
        child_start_values.extend(start_values)
        processes_detail.append(
            {
                "pid": pid,
                "tasks": len(items),
                "first_task_ordinal": min(ordinals) if ordinals else 0,
                "last_task_ordinal": max(ordinals) if ordinals else 0,
                "queue_wait_sec_max": max(queue_values) if queue_values else 0.0,
                "child_start_latency_sec_max": max(start_values) if start_values else 0.0,
            }
        )
    return {
        "processes": len(by_pid),
        "tasks_per_process": _int_summary(tasks_per_process),
        "queue_wait_sec": _float_summary(queue_wait_values),
        "child_start_latency_sec": _float_summary(child_start_values),
        "processes_detail": processes_detail,
    }


def _planned_span_row_groups(
    path: Path,
    *,
    selected: dict[str, list[int]],
    parquet_profiles: dict[str, Any],
) -> tuple[int, ...]:
    selected_ids = tuple(int(value) for value in selected.get(str(path), []))
    if selected_ids:
        return selected_ids
    return tuple(group.row_group_id for group in parquet_profiles[str(path.resolve())].row_groups)


def _int_summary(values: list[int]) -> dict[str, int | float]:
    if not values:
        return {"count": 0, "min": 0, "avg": 0.0, "max": 0}
    return {
        "count": len(values),
        "min": min(values),
        "avg": sum(values) / len(values),
        "max": max(values),
    }


def _float_summary(values: list[float]) -> dict[str, int | float]:
    if not values:
        return {"count": 0, "min": 0.0, "avg": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "min": min(values),
        "avg": sum(values) / len(values),
        "max": max(values),
    }


def _reject_null_partitions(
    lf: pl.LazyFrame,
    *,
    partition_column: str,
    source_name: str,
) -> None:
    names = lf.collect_schema().names()
    if partition_column not in names:
        raise ValidationError(
            f"Source {source_name!r} is missing partition column {partition_column!r}.",
            code="join.missing_partition_column",
            context={"source": source_name, "column": partition_column},
        )
    null_count = lf.select(pl.col(partition_column).is_null().sum()).collect().item()
    if null_count:
        raise ValidationError(
            f"Source {source_name!r} contains {null_count} null partition value(s).",
            code="join.null_partition",
            context={
                "source": source_name,
                "column": partition_column,
                "null_count": int(null_count),
            },
        )


def _validate_join_runtime_schema(
    left: pl.LazyFrame,
    *,
    right_sources: list[dict[str, Any]],
    right_partition_key: str,
) -> list[dict[str, Any]]:
    current = dict(left.collect_schema())
    trace: list[dict[str, Any]] = []
    for source in right_sources:
        source_name = str(source["name"])
        right = _apply_join_column_policy(
            scan_parquet_files_union_by_name([DatasetFileShim(path) for path in source["files"]]),
            source.get("columns") or {},
            required_columns=[
                *source["right_on"],
                *([right_partition_key] if right_partition_key else []),
            ],
            source_name=source_name,
        )
        right_schema = dict(right.collect_schema())
        for left_name, right_name in zip(source["left_on"], source["right_on"], strict=True):
            if left_name not in current:
                raise ValidationError(
                    f"Join source {source_name!r} cannot find left key {left_name!r}.",
                    code="join.missing_left_key",
                    context={"source": source_name, "column": left_name},
                )
            left_dtype = current[left_name]
            right_dtype = right_schema[right_name]
            if (
                getattr(left_dtype, "is_nested", lambda: False)()
                or getattr(right_dtype, "is_nested", lambda: False)()
            ):
                raise ValidationError(
                    "Nested join keys are not supported by the Rust executor.",
                    code="join.unsupported_key_dtype",
                    context={
                        "source": source_name,
                        "left_column": left_name,
                        "right_column": right_name,
                    },
                )
            if left_dtype != right_dtype:
                raise ValidationError(
                    f"Join key dtype mismatch for {left_name!r} and {right_name!r}.",
                    code="join.key_dtype_mismatch",
                    context={
                        "source": source_name,
                        "left_column": left_name,
                        "right_column": right_name,
                        "left_dtype": str(left_dtype),
                        "right_dtype": str(right_dtype),
                    },
                )
        input_schema = {name: str(dtype) for name, dtype in current.items()}
        for name, dtype in right_schema.items():
            if name in source["right_on"]:
                continue
            output_name = name if name not in current else f"{name}{source['suffix']}"
            if output_name in current:
                raise ValidationError(
                    f"Join output column collision: {output_name}",
                    code="join.output_column_collision",
                    context={"source": source_name, "column": output_name},
                )
            current[output_name] = dtype
        trace.append(
            {
                "source": source_name,
                "how": source["how"],
                "input_schema": input_schema,
                "right_schema": {name: str(dtype) for name, dtype in right_schema.items()},
                "output_schema": {name: str(dtype) for name, dtype in current.items()},
            }
        )
    return trace


def _pre_screen_right(left, right, *, left_on: list[str], right_on: list[str]):
    left_keys = left.select(left_on).unique()
    return right.join(left_keys, left_on=right_on, right_on=left_on, how="semi")


def _normalize_join_key_columns(
    lf: pl.LazyFrame,
    *,
    left_on: list[str],
    right_on: list[str],
) -> pl.LazyFrame:
    names = set(lf.collect_schema().names())
    for left_name, right_name in zip(left_on, right_on, strict=True):
        if left_name == right_name:
            continue
        if left_name not in names and right_name in names:
            lf = lf.rename({right_name: left_name})
            names.remove(right_name)
            names.add(left_name)
        elif left_name in names and right_name in names:
            lf = lf.with_columns(
                pl.coalesce([pl.col(left_name), pl.col(right_name)]).alias(left_name)
            ).drop(right_name)
            names.remove(right_name)
    return lf


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
