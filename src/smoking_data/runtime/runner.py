from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from smoking_data.core.engine_contract import engine_metadata
from smoking_data.core.exceptions import SmokingDataError, ValidationError
from smoking_data.core.execution_plan import compile_execution_plan
from smoking_data.core.pipeline import compile_operations
from smoking_data.core.results import StageResult, to_json_safe, utc_now_iso
from smoking_data.runtime.artifacts import artifact_root_for
from smoking_data.runtime.asset_config import asset_code_from_definition_path
from smoking_data.runtime.config import (
    PhaseMemoryPolicy,
    load_config,
    parse_max_tasks_per_child,
)
from smoking_data.runtime.dataset_artifacts import describe_dataset_artifacts
from smoking_data.runtime.events import append_stage_event
from smoking_data.runtime.lowering import lower_pipeline_spec
from smoking_data.runtime.metadata import (
    log_path_for,
    metadata_path_for,
    read_previous_yaml_hash,
    write_metadata,
)
from smoking_data.runtime.operation_registry import (
    TRIGGER_TYPES,
    new_run_id,
    read_completion_catalog,
    record_authoring_insert,
    record_definition,
    record_definition_profiled,
    record_execution,
    record_execution_profiled,
)
from smoking_data.runtime.paths import infer_project_root, reset_path, resolve_project_path
from smoking_data.runtime.yaml_loader import PresetSpec, load_pipeline_spec, load_preset_spec


def run_preset_yaml(
    yaml_path: str | Path,
    *,
    config_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> StageResult:
    """Run an internal lowered preset document.

    Public callers should use ``run_pipeline_yaml`` with ``smoking-data.pipeline.v6``.
    """

    effective_project_root = project_root or infer_project_root(yaml_path)
    config = load_config(
        config_path=config_path,
        project_root=effective_project_root,
        asset_code=asset_code_from_definition_path(yaml_path),
    )
    spec = load_preset_spec(yaml_path, config=config)
    if spec.preset == "0201":
        from smoking_data.assets import a0201_curated as producer
    elif spec.preset == "0301":
        from smoking_data.assets import a0301_join as producer
    else:
        producer = None
    if producer is None:
        raise ValidationError(
            f"Unsupported internal Asset producer: {spec.preset!r}",
            code="asset.unsupported_producer",
            context={"preset": spec.preset},
        )
    config = _apply_execution_overrides(config, spec.raw.get("execution"))
    reset_reason = _reset_reason(spec, config=config)
    if reset_reason == "explicit":
        _reset_runtime_outputs(spec, config=config)
    event_path = log_path_for(spec, config=config)
    append_stage_event(
        event_path,
        event="stage.start",
        preset=spec.preset,
        job_name=spec.job_name,
        details={"yaml_hash": spec.yaml_hash, "reset_reason": reset_reason},
    )
    try:
        result = producer.run(spec, config=config)
    except BaseException as exc:
        append_stage_event(
            event_path,
            event="stage.failure",
            preset=spec.preset,
            job_name=spec.job_name,
            details={
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "error_code": getattr(exc, "code", None),
                "error_context": getattr(exc, "context", None),
            },
        )
        raise
    append_stage_event(
        event_path,
        event="stage.finish",
        preset=spec.preset,
        job_name=spec.job_name,
        details={"ok": result.ok, "counters": result.counters},
    )
    return result


def run_pipeline_yaml(
    yaml_path: str | Path,
    *,
    config_path: str | Path | None = None,
    project_root: str | Path | None = None,
    trigger_type: str = "manual",
) -> StageResult:
    pipeline_started = time.perf_counter()
    pipeline_phases: dict[str, Any] = {}
    if trigger_type not in TRIGGER_TYPES:
        raise ValidationError(
            f"Unsupported pipeline trigger_type: {trigger_type}",
            code="registry.unsupported_trigger_type",
            context={"trigger_type": trigger_type, "expected": sorted(TRIGGER_TYPES)},
        )
    phase_started = time.perf_counter()
    effective_project_root = project_root or infer_project_root(yaml_path)
    config = load_config(
        config_path=config_path,
        project_root=effective_project_root,
        asset_code=asset_code_from_definition_path(yaml_path),
    )
    pipeline_phases["config_load_sec"] = time.perf_counter() - phase_started
    phase_started = time.perf_counter()
    pipeline_spec = load_pipeline_spec(yaml_path, config=config)
    pipeline_phases["yaml_load_sec"] = time.perf_counter() - phase_started
    from smoking_data.runtime.upstream_union import prepare_combined_sources

    phase_started = time.perf_counter()
    pipeline_spec, combined_upstream_profile = prepare_combined_sources(
        pipeline_spec,
        config=config,
    )
    pipeline_phases["combined_upstream_sec"] = time.perf_counter() - phase_started
    pipeline_phases["combined_upstream_profile"] = combined_upstream_profile
    phase_started = time.perf_counter()
    registry_path, definition_registry_profile = record_definition_profiled(
        pipeline_spec, project_root=config.project_root
    )
    pipeline_phases["definition_registry_sec"] = time.perf_counter() - phase_started
    pipeline_phases["definition_registry_profile"] = definition_registry_profile
    config = _apply_output_paths(config, pipeline_spec.raw.get("output"))
    from smoking_data.runtime.parquet_probe import ensure_pipeline_probes_profiled

    phase_started = time.perf_counter()
    probe_handles, probe_profile = ensure_pipeline_probes_profiled(
        pipeline_spec, config=config
    )
    pipeline_phases["probe_sec"] = time.perf_counter() - phase_started
    pipeline_phases["probe_profile"] = probe_profile
    phase_started = time.perf_counter()
    pipeline_spec = _validate_pipeline_against_source_schemas(pipeline_spec, config=config)
    pipeline_phases["schema_validation_sec"] = time.perf_counter() - phase_started
    pipeline_spec = replace(
        pipeline_spec,
        raw={
            **pipeline_spec.raw,
            "__probe_manifests": {name: handle.to_dict() for name, handle in probe_handles.items()},
        },
    )
    config = _apply_execution_overrides(config, pipeline_spec.execution)
    config = replace(config, optimizer_enabled=False)
    phase_started = time.perf_counter()
    target, lowered_spec, initial_execution_plan = lower_pipeline_spec(pipeline_spec)
    pipeline_phases["lowering_sec"] = time.perf_counter() - phase_started
    config = _apply_execution_overrides(config, lowered_spec.raw.get("execution"))
    reset_reason = _reset_reason(pipeline_spec, config=config)
    phase_started = time.perf_counter()
    if reset_reason in {"explicit", "yaml_changed"}:
        _reset_pipeline_outputs(pipeline_spec, config=config)
    pipeline_phases["output_prepare_sec"] = time.perf_counter() - phase_started
    event_path = log_path_for(pipeline_spec, config=config)
    append_stage_event(
        event_path,
        event="pipeline.start",
        preset=pipeline_spec.schema_version,
        job_name=pipeline_spec.job_name,
        details={
            "yaml_hash": pipeline_spec.yaml_hash,
            "graph_hash": pipeline_spec.graph_hash,
            "topological_node_order": pipeline_spec.graph["topological_order"],
            "topological_alias_order": pipeline_spec.graph["topological_alias_order"],
            "logical_plan_hash": pipeline_spec.logical_plan.plan_hash,
            "migration": pipeline_spec.raw.get("migration"),
            "operation_order": [
                item["operation_id"] for item in initial_execution_plan["operations"]
            ],
            "reset_reason": reset_reason,
        },
    )
    execution_started_at = utc_now_iso()
    execution_run_id = new_run_id()
    asset_execution_started = time.perf_counter()
    try:
        if target == "curated":
            from smoking_data.assets import a0201_curated

            result = a0201_curated.run(lowered_spec, config=config)
        else:
            from smoking_data.assets import a0301_join

            result = a0301_join.run(lowered_spec, config=config)
    except BaseException as exc:
        record_execution(
            pipeline_spec,
            project_root=config.project_root,
            trigger_type=trigger_type,
            status="failed",
            started_at=execution_started_at,
            finished_at=utc_now_iso(),
            run_id=execution_run_id,
        )
        append_stage_event(
            event_path,
            event="pipeline.failure",
            preset=pipeline_spec.schema_version,
            job_name=pipeline_spec.job_name,
            details={"error_type": type(exc).__name__, "error_message": str(exc)},
        )
        raise
    pipeline_phases["asset_execution_sec"] = time.perf_counter() - asset_execution_started
    enrichment_started = time.perf_counter()
    physical_plan_hash = result.details.get("physical_plan_hash")
    operation_trace = list((lowered_spec.raw.get("__pipeline") or {}).get("operation_trace") or [])
    rust_operation_trace = list(
        (lowered_spec.raw.get("__pipeline") or {}).get("rust_operation_trace") or operation_trace
    )
    _validate_ordered_execution_receipts(
        result,
        operation_count=len(rust_operation_trace),
        target=target,
    )
    execution_plan = compile_execution_plan(
        pipeline_spec.logical_plan,
        physical_plan_hash=str(physical_plan_hash) if physical_plan_hash else None,
        backend_versions={
            key: str(value) for key, value in engine_metadata().items() if value is not None
        },
    ).to_dict()
    result.preset = pipeline_spec.schema_version
    result.details["execution_plan"] = execution_plan
    result.details["plan_hashes"] = {
        "graph": pipeline_spec.graph_hash,
        "canonical": pipeline_spec.logical_plan.plan_hash,
        "compiled": execution_plan["plan_hash"],
        "executed": execution_plan["plan_hash"],
        "physical": physical_plan_hash,
    }
    result.details["executed_operation_order"] = [
        item["operation_id"] for item in execution_plan["operations"]
    ]
    result.details["pipeline_graph"] = pipeline_spec.graph
    result.details["operation_registry"] = {
        "path": str(registry_path),
        "run_id": execution_run_id,
        "trigger_type": trigger_type,
    }
    operation_status = (
        "reused" if int(result.counters.get("skipped_unchanged", 0)) > 0 else "executed"
    )
    result.details["operation_execution_trace"] = [
        {**item, "status": operation_status, "physical_plan_hash": physical_plan_hash}
        for item in operation_trace
    ]
    result.details["lowered_physical_kernel"] = target
    result.details["physical_probes"] = {
        name: handle.to_dict() for name, handle in probe_handles.items()
    }
    pipeline_phases["result_enrichment_sec"] = time.perf_counter() - enrichment_started
    pipeline_phases["completion_event"] = "pipeline.profile"
    result.details["pipeline_phase_elapsed_sec"] = pipeline_phases
    metadata_started = time.perf_counter()
    try:
        result.metadata_path = write_metadata(
            spec=pipeline_spec,
            config=config,
            result=result.to_dict(),
            extra={"execution_plan": execution_plan},
        )
    except BaseException:
        record_execution(
            pipeline_spec,
            project_root=config.project_root,
            trigger_type=trigger_type,
            status="failed",
            started_at=execution_started_at,
            finished_at=utc_now_iso(),
            run_id=execution_run_id,
        )
        raise
    result.dataset_artifacts = describe_dataset_artifacts(
        result.output_paths,
        metadata_path=result.metadata_path,
        definition_sha256=pipeline_spec.yaml_hash,
    )
    pipeline_phases["metadata_write_sec"] = time.perf_counter() - metadata_started
    publication_started = time.perf_counter()
    artifact = (pipeline_spec.raw.get("output") or {}).get("artifact") or {}
    from smoking_data.runtime.object_store.config import PublicationSpec
    from smoking_data.runtime.object_store.publication import publish_committed_dataset

    artifact_sbdf = artifact.get("sbdf") or {}
    publication = PublicationSpec.from_mapping(
        artifact.get("publication"),
        sbdf_row_key_columns=tuple(artifact_sbdf.get("row_key_columns") or ()),
    )
    if publication is not None:
        output_root = resolve_project_path(
            str(artifact["root_dir"]), project_root=config.project_root
        )
        publication_result = publish_committed_dataset(
            output_root,
            project_root=config.project_root,
            publication=publication,
            asset_code=asset_code_from_definition_path(yaml_path),
            job_name=pipeline_spec.job_name,
            definition_sha256=pipeline_spec.yaml_hash,
        )
        result.details["remote_publication"] = (
            {
                "status": publication_result.status,
                "target": publication_result.target,
                "dataset_uri": publication_result.dataset_uri,
                "generation_id": publication_result.generation_id,
                "manifest_key": publication_result.manifest_key,
                "receipt_path": str(publication_result.receipt_path),
                "uploaded_objects": publication_result.uploaded_objects,
                "reused_objects": publication_result.reused_objects,
            }
            if publication_result is not None
            else None
        )
    pipeline_phases["remote_publication_sec"] = time.perf_counter() - publication_started
    finish_started = time.perf_counter()
    _, execution_registry_profile = record_execution_profiled(
        pipeline_spec,
        project_root=config.project_root,
        trigger_type=trigger_type,
        status="success" if result.ok else "failed",
        started_at=execution_started_at,
        finished_at=result.finished_at or utc_now_iso(),
        run_id=execution_run_id,
    )
    pipeline_phases["execution_registry_profile"] = execution_registry_profile
    append_stage_event(
        event_path,
        event="pipeline.finish",
        preset=pipeline_spec.schema_version,
        job_name=pipeline_spec.job_name,
        details={"ok": result.ok, "operation_count": len(execution_plan["operations"])},
    )
    pipeline_phases["registry_and_finish_event_sec"] = time.perf_counter() - finish_started
    pipeline_phases["total_elapsed_sec"] = time.perf_counter() - pipeline_started
    append_stage_event(
        event_path,
        event="pipeline.profile",
        preset=pipeline_spec.schema_version,
        job_name=pipeline_spec.job_name,
        details={"phase_elapsed_sec": pipeline_phases},
    )
    return result


def _validate_ordered_execution_receipts(
    result: StageResult,
    *,
    operation_count: int,
    target: str,
) -> None:
    counter = "rust_ordered_operation_count" if target == "curated" else "ordered_operation_count"
    task_results = result.details.get("task_results") or []
    mismatches = []
    receipts = 0
    for task_result in task_results:
        counters = (
            task_result.get("counters", {})
            if isinstance(task_result, dict)
            else getattr(task_result, "counters", {})
        ) or {}
        actual = counters.get(counter)
        if actual is not None:
            receipts += 1
            if int(actual) != operation_count:
                task_id = (
                    task_result.get("task_id")
                    if isinstance(task_result, dict)
                    else task_result.task_id
                )
                mismatches.append({"task_id": task_id, "actual": actual})
    if mismatches or (task_results and receipts == 0):
        raise SmokingDataError(
            "Rust task operation receipt differs from the compiled YAML order.",
            code="execution.operation_receipt_mismatch",
            context={"expected": operation_count, "receipts": receipts, "tasks": mismatches},
        )


def _reset_reason(spec: PresetSpec, *, config: Any) -> str | None:
    if config.reset_before_run:
        return "explicit"
    previous_hash = read_previous_yaml_hash(spec, config=config)
    if previous_hash is not None and previous_hash != spec.yaml_hash:
        return "yaml_changed"
    return None


def _apply_execution_overrides(config: Any, execution: Any) -> Any:
    if not isinstance(execution, dict):
        return config
    updates: dict[str, Any] = {}
    if "workers" in execution:
        updates["workers"] = max(1, int(execution.get("workers") or 1))
    if "max_tasks_per_child" in execution:
        updates["max_tasks_per_child"] = parse_max_tasks_per_child(
            execution.get("max_tasks_per_child")
        )
    if "target_rows_per_part" in execution:
        updates["target_rows_per_part"] = max(1, int(execution.get("target_rows_per_part") or 1))
    if "target_key_groups_per_part" in execution:
        updates["target_key_groups_per_part"] = max(
            1, int(execution.get("target_key_groups_per_part") or 1)
        )
    if "memory_budget_mb" in execution:
        updates["memory_budget_mb"] = max(1, int(execution.get("memory_budget_mb") or 1))
    memory = execution.get("memory")
    if isinstance(memory, dict):
        hard_limit_mb = max(
            1,
            int(
                memory.get("hard_limit_mb")
                or updates.get("memory_budget_mb")
                or config.memory_budget_mb
            ),
        )
        safety_ratio = float(memory.get("safety_ratio", config.memory_safety_ratio))
        if not 0.0 < safety_ratio <= 1.0:
            raise ValidationError("execution.memory.safety_ratio must be > 0 and <= 1.")
        phases = memory.get("phases") or {}
        if not isinstance(phases, dict):
            raise ValidationError("execution.memory.phases must be a mapping.")
        phase_memory: dict[str, PhaseMemoryPolicy] = {}
        for phase in ("build_sidecar", "materialize", "save_dataset"):
            raw = phases.get(phase)
            if raw is None:
                continue
            if not isinstance(raw, dict):
                raise ValidationError(f"execution.memory.phases.{phase} must be a mapping.")
            worker_range = raw.get("workers") or {}
            if not isinstance(worker_range, dict):
                raise ValidationError(f"execution.memory.phases.{phase}.workers must be a mapping.")
            minimum = max(1, int(worker_range.get("min") or 1))
            maximum = max(1, int(worker_range.get("max") or minimum))
            if minimum > maximum:
                raise ValidationError(
                    f"execution.memory.phases.{phase}.workers.min must be <= workers.max."
                )
            phase_memory[phase] = PhaseMemoryPolicy(
                target_peak_memory_mb=min(
                    hard_limit_mb,
                    max(1, int(raw.get("target_peak_memory_mb") or hard_limit_mb)),
                ),
                min_workers=minimum,
                max_workers=maximum,
            )
        updates.update(
            {
                "memory_budget_mb": hard_limit_mb,
                "memory_safety_ratio": safety_ratio,
                "phase_memory": phase_memory,
            }
        )
    if "max_source_files_per_task" in execution:
        updates["max_source_files_per_task"] = max(
            1, int(execution.get("max_source_files_per_task") or 1)
        )
    if "max_source_row_groups_per_task" in execution:
        updates["max_source_row_groups_per_task"] = max(
            1, int(execution.get("max_source_row_groups_per_task") or 1)
        )
    if "sidecar_workers" in execution:
        updates["sidecar_workers"] = max(1, int(execution.get("sidecar_workers") or 1))
    if "sidecar_worker_recycle_mode" in execution:
        mode = str(execution.get("sidecar_worker_recycle_mode") or "adaptive").lower()
        if mode != "adaptive":
            raise ValidationError("execution.sidecar_worker_recycle_mode must be adaptive.")
        updates["sidecar_worker_recycle_mode"] = mode
    if "sidecar_max_source_files" in execution:
        updates["sidecar_max_source_files"] = max(
            1, int(execution.get("sidecar_max_source_files") or 1)
        )
    if "sidecar_max_projected_bytes_mb" in execution:
        updates["sidecar_max_projected_bytes_mb"] = max(
            1, int(execution.get("sidecar_max_projected_bytes_mb") or 1)
        )
    if "optimizer_enabled" in execution:
        updates["optimizer_enabled"] = bool(execution.get("optimizer_enabled"))
    if "output_row_group_rows" in execution:
        value = execution.get("output_row_group_rows")
        updates["output_row_group_rows"] = max(1, int(value)) if value is not None else None
    if "reset_before_run" in execution:
        updates["reset_before_run"] = bool(execution.get("reset_before_run"))
    return replace(config, **updates) if updates else config


def _apply_output_paths(config: Any, output: Any) -> Any:
    if not isinstance(output, dict):
        return config
    logging = output.get("logging")
    if not isinstance(logging, dict):
        return config
    return replace(
        config,
        log_root=resolve_project_path(str(logging["root_dir"]), project_root=config.project_root),
    )


def _reset_runtime_outputs(spec: PresetSpec, *, config: Any) -> None:
    reset_path(metadata_path_for(spec, config=config))
    reset_path(log_path_for(spec, config=config))
    reset_path(artifact_root_for(spec, config=config))
    output = spec.raw.get("output")
    if isinstance(output, dict) and output.get("output_dir"):
        reset_path(
            resolve_project_path(str(output["output_dir"]), project_root=config.project_root)
        )


def _reset_pipeline_outputs(spec: Any, *, config: Any) -> None:
    reset_path(metadata_path_for(spec, config=config))
    reset_path(log_path_for(spec, config=config))
    reset_path(artifact_root_for(spec, config=config))
    for sink in spec.sinks.values():
        reset_path(resolve_project_path(sink.path, project_root=config.project_root))


def _validate_pipeline_against_source_schemas(spec: Any, *, config: Any) -> Any:
    import pyarrow.parquet as pq

    from smoking_data.ops.upstream import discover_parquet_files

    source_columns: dict[str, tuple[str, ...]] = {}
    source_dtypes: dict[str, dict[str, str]] = {}
    for name, source in spec.sources.items():
        keyspace_keys = (
            [str(item) for item in source.keyspace.get("keys") or []]
            if source.keyspace is not None
            else []
        )
        roots = [
            resolve_project_path(path, project_root=config.project_root) for path in source.paths
        ]
        files = discover_parquet_files(roots, recursive=True)
        if not files:
            raise SmokingDataError(
                f"Pipeline source has no parquet files: {name}",
                code="source.empty",
                context={"source": name, "paths": list(source.paths)},
            )
        columns: list[str] = []
        dtypes: dict[str, str] = {}
        for item in files:
            schema = pq.ParquetFile(item.path).schema_arrow
            if keyspace_keys:
                missing = [key for key in keyspace_keys if schema.get_field_index(key) < 0]
                if missing:
                    raise SmokingDataError(
                        "Join keyspace source is missing required key columns.",
                        code="join_keyspace.missing_key",
                        context={"source": name, "columns": missing, "path": str(item.path)},
                    )
            for field in schema:
                column = field.name
                if keyspace_keys and column not in keyspace_keys:
                    continue
                dtype = str(field.type)
                if column in dtypes and dtypes[column] != dtype:
                    raise SmokingDataError(
                        "Pipeline source contains incompatible parquet dtype drift.",
                        code="source.incompatible_dtype",
                        context={
                            "source": name,
                            "column": column,
                            "expected": dtypes[column],
                            "actual": dtype,
                            "path": str(item.path),
                        },
                    )
                dtypes[column] = dtype
                if column not in columns:
                    columns.append(column)
        source_columns[name] = tuple(keyspace_keys or columns)
        source_dtypes[name] = dtypes
    expression_irs = {
        operation.operation_id: operation.config["expression_ir"]
        for operation in spec.logical_plan.operations
        if operation.config.get("expression_ir") is not None
    }
    logical_plan = compile_operations(
        spec.raw,
        expression_irs=expression_irs,
        source_columns=source_columns,
        source_dtypes=source_dtypes,
    )
    return replace(
        spec,
        logical_plan=logical_plan,
        raw={**dict(spec.raw), "__source_columns": source_columns},
    )


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] in [["-h"], ["--help"]]:
        _print_cli_help()
        return 0
    if argv[:1] in [
        ["migrate"],
        ["layout"],
        ["smoke"],
        ["chain"],
        ["registry"],
        ["schedule"],
        ["publication"],
        ["pwq"],
        ["inspect"],
        ["update"],
    ]:
        if len(argv) == 1 or argv[1:2] in [["-h"], ["--help"]]:
            _print_cli_help(argv[0])
            return 0
    if argv and argv[0] == "init":
        return _main_init(argv[1:])
    if argv and argv[0] == "source":
        return _main_source(argv[1:])
    if argv and argv[0] == "run":
        return _main_run(argv[1:])
    if argv and argv[0] == "validate":
        return _main_validate(argv[1:])
    if argv and argv[0] == "capabilities":
        return _main_capabilities(argv[1:])
    if argv and argv[0] == "compare":
        return _main_compare(argv[1:])
    if argv and argv[0] == "fixture":
        return _main_fixture(argv[1:])
    if argv[:2] == ["update", "templates"]:
        return _main_update_templates(argv[2:])
    if argv and argv[0] == "parquet-schema":
        return _main_parquet_schema(argv[1:])
    if argv[:2] == ["pwq", "advise"]:
        return _main_pwq_advise(argv[2:])
    if argv[:2] == ["pwq", "benchmark-dummy"]:
        return _main_pwq_benchmark_dummy(argv[2:])
    if argv[:2] == ["migrate", "yaml"]:
        return _main_migrate_yaml(argv[2:])
    if argv[:2] == ["migrate", "parquet"]:
        return _main_migrate_parquet(argv[2:])
    if argv[:3] == ["migrate", "chain", "verify"]:
        return _main_verify_migrated_chain(argv[3:])
    if argv[:3] == ["migrate", "chain", "run"]:
        return _main_migrate_chain_run(argv[3:])
    if argv[:2] == ["smoke", "run"]:
        return _main_smoke_run(argv[2:])
    if argv[:2] == ["layout", "report"]:
        return _main_layout_report(argv[2:])
    if argv[:2] == ["layout", "migrate"]:
        return _main_layout_migrate(argv[2:])
    if argv[:2] == ["publication", "inspect"]:
        return _main_publication_inspect(argv[2:])
    if argv[:2] == ["publication", "retry"]:
        return _main_publication_retry(argv[2:])
    if argv[:2] == ["publication", "gc"]:
        return _main_publication_gc(argv[2:])
    if argv[:2] == ["publication", "read-key"]:
        return _main_publication_read_key(argv[2:])
    if argv[:2] == ["chain", "validate"]:
        return _main_chain_validate(argv[2:])
    if argv[:2] == ["chain", "run"]:
        return _main_chain_run(argv[2:])
    if argv[:2] == ["registry", "list"]:
        return _main_registry_list(argv[2:])
    if argv[:2] == ["registry", "record-insert"]:
        return _main_registry_record_insert(argv[2:])
    if argv[:2] == ["schedule", "validate"]:
        return _main_schedule_validate(argv[2:])
    if argv[:2] == ["schedule", "tick"]:
        return _main_schedule_tick(argv[2:])
    if len(argv) >= 2 and argv[0] == "inspect" and argv[1] in {
        "dataset",
        "failure",
        "missing",
        "profile",
    }:
        return _main_inspect(argv[1], argv[2:])

    if argv and not _looks_like_definition_path(argv[0]):
        print(
            f"[smoking-data] unknown command: {argv[0]}. "
            "Run 'smoking-data --help' for available commands.",
            file=sys.stderr,
        )
        return 2

    parser = argparse.ArgumentParser(description="Run smoking-data pipeline YAML.")
    parser.add_argument("yaml_path", nargs="?", help="pipeline YAML path to validate/run.")
    parser.add_argument(
        "--config", dest="config_path", default=None, help="Runtime config YAML path."
    )
    parser.add_argument("--project-root", default=None, help="Project root for relative paths.")
    parser.add_argument(
        "--trigger-type",
        choices=sorted(TRIGGER_TYPES),
        default="manual",
        help="Classify this execution independently from authoring reuse metrics.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON-safe result.")
    args = parser.parse_args(argv)

    if not args.yaml_path:
        parser.print_help()
        return 0

    return _run_definition_cli(args)


def _main_capabilities(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Return the installed smoking-data capability contract."
    )
    parser.add_argument("--json", action="store_true", help="Print the capability manifest as JSON.")
    args = parser.parse_args(argv)
    from smoking_data.runtime.capabilities import get_capabilities

    payload = get_capabilities()
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    else:
        print(
            f"[smoking-data] capabilities schema={payload['schema_version']} "
            f"package={payload['package_version']} operations={len(payload['operations'])}"
        )
    return 0


def _definition_kind(yaml_path: str | Path) -> str:
    path = Path(yaml_path).expanduser()
    if path.name.endswith((".chain.yaml", ".chain.yml")):
        return "chain"
    if not path.is_file():
        return "asset"
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    header = payload.get("yaml") if isinstance(payload, dict) else None
    if isinstance(header, dict) and header.get("schema_version") == "smoking-data.asset-chain.v2":
        return "chain"
    return "asset"


def _run_definition_cli(args: argparse.Namespace) -> int:
    try:
        effective_project_root = args.project_root or infer_project_root(args.yaml_path)
        if _definition_kind(args.yaml_path) == "chain":
            from smoking_data.runtime.asset_chain import run_asset_chain

            chain_result = run_asset_chain(
                args.yaml_path,
                config_path=args.config_path,
                project_root=effective_project_root,
            )
            payload = chain_result.to_dict()
            ok = chain_result.ok
        else:
            asset_code = asset_code_from_definition_path(args.yaml_path)
            if asset_code == "0101":
                from smoking_data.assets.a0101_source import execute_yaml

                source_result = execute_yaml(args.yaml_path, project_root=effective_project_root)
                payload = source_result.to_dict()
                ok = source_result.ok
            elif asset_code == "0102":
                from smoking_data.assets.a0102_calculated_fact import run_yaml

                result = run_yaml(
                    args.yaml_path,
                    config_path=args.config_path,
                    project_root=effective_project_root,
                    trigger_type=args.trigger_type,
                )
                payload = result.to_dict()
                ok = result.ok
            elif asset_code == "0103":
                from smoking_data.assets.a0103_csv_source import run_yaml

                result = run_yaml(
                    args.yaml_path,
                    config_path=args.config_path,
                    project_root=effective_project_root,
                    trigger_type=args.trigger_type,
                )
                payload = result.to_dict()
                ok = result.ok
            else:
                result = run_pipeline_yaml(
                    args.yaml_path,
                    config_path=args.config_path,
                    project_root=effective_project_root,
                    trigger_type=args.trigger_type,
                )
                payload = result.to_dict()
                ok = result.ok
    except SmokingDataError as exc:
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": exc.code,
            "error_message": str(exc),
            "error_context": exc.context,
            "yaml_path": str(args.yaml_path),
        }
        ok = False
    except Exception as exc:  # noqa: BLE001 - CLI must return JSON-safe failures.
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "definition.unexpected_error",
            "error_message": str(exc),
            "yaml_path": str(args.yaml_path),
        }
        ok = False
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif ok:
        name = payload.get("chain_name") or payload.get("job_name") or "unknown"
        print(f"[smoking-data] run ok name={name}")
    else:
        print(f"[smoking-data] run failed: {payload.get('error_message', 'execution failed')}")
    return 0 if ok else 1


def _main_run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Run an Asset Definition or Asset Chain YAML."
    )
    parser.add_argument("yaml_path", help="Asset or Asset Chain YAML path.")
    parser.add_argument("--config", dest="config_path", default=None)
    parser.add_argument("--project-root", default=None)
    parser.add_argument(
        "--trigger-type",
        choices=sorted(TRIGGER_TYPES),
        default="manual",
        help="Classify this execution independently from authoring reuse metrics.",
    )
    parser.add_argument("--json", action="store_true")
    return _run_definition_cli(parser.parse_args(argv))


_CLI_COMMANDS: dict[str, tuple[str, ...]] = {
    "init": ("Initialize a workspace",),
    "source": ("Run a 0101 Source Definition",),
    "run": ("Run an Asset Definition or Chain",),
    "validate": ("Validate an Asset Definition or Chain",),
    "capabilities": ("Show engine capabilities",),
    "compare": ("Compare execution results",),
    "fixture": ("Run fixture utilities",),
    "parquet-schema": ("Read Parquet footer schema",),
    "migrate": ("Migrate Definitions, Parquet inputs, or Chains",),
    "smoke": ("Run bounded smoke tasks",),
    "layout": ("Inspect or migrate physical layout",),
    "publication": ("Inspect and manage publication state",),
    "chain": ("Validate or run an Asset Chain",),
    "registry": ("Inspect authoring registry",),
    "schedule": ("Validate or tick schedules",),
    "inspect": ("Inspect datasets and metadata",),
    "pwq": ("Advise or benchmark pipeline write quality",),
    "update": ("Update initialized workspace resources",),
}

_CLI_GROUP_COMMANDS: dict[str, tuple[str, ...]] = {
    "migrate": (
        "migrate yaml INPUT.yaml --output NORMALIZED.yaml",
        "migrate parquet INPUT --output migration.0201.yaml --source-asset ASSET",
        "migrate chain verify CHAIN.yaml",
        "migrate chain run CHAIN.yaml",
    ),
    "smoke": ("smoke run DEFINITION.yaml [--tasks N]",),
    "layout": (
        "layout report DATASET",
        "layout migrate MIGRATION.yaml",
    ),
    "chain": ("chain validate CHAIN.yaml", "chain run CHAIN.yaml"),
    "registry": ("registry list", "registry record-insert OP_ID"),
    "schedule": ("schedule validate", "schedule tick"),
    "publication": (
        "publication inspect",
        "publication retry RECEIPT.json",
        "publication gc",
        "publication read-key KEY",
    ),
    "pwq": (
        "pwq advise PIPELINE.yaml",
        "pwq benchmark-dummy --root ROOT",
    ),
    "update": ("update templates [TARGET] [--json]",),
    "inspect": (
        "inspect dataset PATH",
        "inspect failure PATH",
        "inspect missing PATH",
        "inspect profile PATH",
    ),
}


def _print_cli_help(group: str | None = None) -> None:
    if group is None:
        print("usage: smoking-data COMMAND [OPTIONS]")
        print("       smoking-data PIPELINE.yaml [OPTIONS]")
        print("\ncommands:")
        for command, description in _CLI_COMMANDS.items():
            print(f"  {command:<15} {description[0]}")
        print("\nRun 'smoking-data COMMAND --help' for command-specific options.")
        return
    print(f"usage: smoking-data {group} SUBCOMMAND [OPTIONS]")
    print(f"\n{group} commands:")
    for command in _CLI_GROUP_COMMANDS.get(group, ()):
        print(f"  {command}")
    print(f"\nRun 'smoking-data {group} SUBCOMMAND --help' for command-specific options.")


def _looks_like_definition_path(value: str) -> bool:
    path = Path(value).expanduser()
    return path.exists() or path.suffix.casefold() in {".yaml", ".yml"}


def _main_update_templates(argv: list[str]) -> int:
    from smoking_data.workspace_init import backup_paths, initialize_workspace_templates

    parser = argparse.ArgumentParser(
        description=(
            "Update workspace templates from the installed smoking-data package. "
            "Existing templates are backed up before replacement."
        )
    )
    parser.add_argument("target", nargs="?", default=".", help="Workspace root.")
    parser.add_argument("--json", action="store_true", help="Print a JSON result.")
    args = parser.parse_args(argv)
    try:
        history = backup_paths(args.target, names=("templates",))
        templates = initialize_workspace_templates(args.target, force=True)
        payload = {
            "ok": True,
            "command": "update templates",
            "workspace_root": str(Path(args.target).expanduser().resolve()),
            "templates": templates,
            "history": history,
        }
    except (OSError, TypeError, ValueError) as exc:
        payload = {
            "ok": False,
            "command": "update templates",
            "error": str(exc),
            "error_type": type(exc).__name__,
        }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(
            "[smoking-data] templates updated: "
            f"created={len(payload['templates']['created'])} "
            f"replaced={len(payload['templates']['preserved'])} "
            f"history={payload['history']['history_root']}"
        )
    else:
        print(f"[smoking-data] template update failed: {payload['error']}")
    return 0 if payload["ok"] else 1


def _main_init(argv: list[str]) -> int:
    from smoking_data.workspace_init import (
        backup_init_outputs,
        initialize_agent_workspace,
        initialize_asset_configs,
        initialize_cast_types,
        initialize_help,
        initialize_runtime_directories,
        initialize_schedule_templates,
        initialize_workspace,
        initialize_workspace_templates,
    )
    from smoking_data.workspace_init.config_initializer import initialize_runtime_config

    parser = argparse.ArgumentParser(
        description=(
            "Initialize YAML authoring, per-Asset configs, "
            "and workspace runtime directories."
        )
    )
    parser.add_argument("target", nargs="?", default=".")
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Replace files managed by init, including templates, schedules, Asset configs, "
            "HELP.md, and agent guidance. User runtime data, object-store settings, "
            "AGENTS.md, and .agent/local are preserved."
        ),
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        history = (
            backup_init_outputs(args.target)
            if args.force
            else {"history_root": None, "backed_up": []}
        )
        workspace = initialize_workspace(args.target)
        adapter = initialize_runtime_config(args.target)
        asset_configs = initialize_asset_configs(args.target, force=args.force)
        runtime_directories = initialize_runtime_directories(args.target)
        templates = initialize_workspace_templates(args.target, force=args.force)
        schedule_templates = initialize_schedule_templates(args.target, force=args.force)
        help_document = initialize_help(args.target, force=args.force)
        cast_types_document = initialize_cast_types(args.target, force=args.force)
        agent_workspace = initialize_agent_workspace(args.target)
        payload = {
            "ok": True,
            "workspace": workspace,
            "adapter": adapter,
            "asset_configs": asset_configs,
            "runtime_directories": runtime_directories,
            "templates": templates,
            "schedule_templates": schedule_templates,
            "help": help_document,
            "cast_types": cast_types_document,
            "agent_workspace": agent_workspace,
            "force": args.force,
            "history": history,
        }
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        payload = {"ok": False, "error": str(exc), "error_type": type(exc).__name__}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(f"[smoking-data] workspace initialized: {payload['workspace']['vscode_dir']}")
        print(
            "[smoking-data] source adapter: "
            f"{payload['adapter'].get('managed_by', 'installed adapter package')}"
        )
        print(
            "[smoking-data] Asset configs: "
            f"created={len(payload['asset_configs']['created'])} "
            f"preserved={len(payload['asset_configs']['preserved'])}"
        )
        print(
            "[smoking-data] runtime directories: "
            f"created={len(payload['runtime_directories']['created'])} "
            f"preserved={len(payload['runtime_directories']['preserved'])} "
            "skipped_outside_workspace="
            f"{len(payload['runtime_directories']['skipped_outside_workspace'])}"
        )
        print(
            "[smoking-data] templates: "
            f"created={len(payload['templates']['created'])} "
            f"preserved={len(payload['templates']['preserved'])}"
        )
        print(
            "[smoking-data] schedule templates: "
            f"created={len(payload['schedule_templates']['created'])} "
            f"preserved={len(payload['schedule_templates']['preserved'])}"
        )
        print(f"[smoking-data] help: {payload['help']['help_path']}")
        print(
            "[smoking-data] Agent workspace: "
            f"root={payload['agent_workspace']['agent_root']} "
            f"sandbox={payload['agent_workspace']['sandbox_root']} "
            "entrypoint_action="
            f"{payload['agent_workspace']['agents_entrypoint_action']}"
        )
        if payload["agent_workspace"]["manual_link_required"]:
            print(
                "[smoking-data] AGENTS.md에 Agent 지침을 연결하지 못했습니다. "
                f"reason={payload['agent_workspace']['agents_entrypoint_reason']}; "
                ".agent/README.md 링크를 수동으로 추가하세요."
            )
    else:
        print(f"[smoking-data] init failed: {payload['error']}")
    return 0 if payload["ok"] else 1


def _main_publication_inspect(argv: list[str]) -> int:
    from smoking_data.runtime.object_store.operations import (
        inspect_remote_publication,
        list_publication_receipts,
    )

    parser = argparse.ArgumentParser(
        description="Inspect local publication receipts or one pinned remote generation."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--target")
    parser.add_argument("--dataset-prefix")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        if bool(args.target) != bool(args.dataset_prefix):
            raise ValidationError(
                "--target and --dataset-prefix must be provided together.",
                code="remote.inspect_arguments_invalid",
            )
        payload = (
            inspect_remote_publication(
                args.project_root,
                target=args.target,
                dataset_prefix=args.dataset_prefix,
            )
            if args.target
            else {
                "status": "local_receipts",
                "receipts": list_publication_receipts(args.project_root),
            }
        )
        ok = True
    except SmokingDataError as exc:
        payload = exc.to_dict()
        ok = False
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif ok:
        print(
            f"[smoking-data] publication inspect status={payload.get('status')} "
            f"generation={payload.get('generation_id', '-')}"
        )
    else:
        print(f"[smoking-data] publication inspect failed: {payload['error_message']}")
    return 0 if ok else 1


def _main_publication_retry(argv: list[str]) -> int:
    from smoking_data.runtime.object_store.operations import retry_publication_receipt

    parser = argparse.ArgumentParser(description="Retry one pending local publication receipt.")
    parser.add_argument("receipt_path")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = retry_publication_receipt(
            args.receipt_path,
            project_root=args.project_root,
        )
        payload = {
            "ok": True,
            "status": result.status,
            "target": result.target,
            "dataset_uri": result.dataset_uri,
            "generation_id": result.generation_id,
            "manifest_key": result.manifest_key,
            "receipt_path": str(result.receipt_path),
        }
        ok = True
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        ok = False
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif ok:
        print(
            f"[smoking-data] publication retry status={payload['status']} "
            f"generation={payload['generation_id']}"
        )
    else:
        print(f"[smoking-data] publication retry failed: {payload['error_message']}")
    return 0 if ok else 1


def _main_publication_gc(argv: list[str]) -> int:
    from smoking_data.runtime.object_store.operations import garbage_collect_publication

    parser = argparse.ArgumentParser(
        description="Inventory old remote generations; deletion requires --execute and a pinned generation."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--target", required=True)
    parser.add_argument("--dataset-prefix", required=True)
    parser.add_argument("--retain-generations", type=int, default=3)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--expected-generation-id")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = garbage_collect_publication(
            args.project_root,
            target=args.target,
            dataset_prefix=args.dataset_prefix,
            retain_generations=args.retain_generations,
            execute=args.execute,
            expected_generation_id=args.expected_generation_id,
        )
        ok = True
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        ok = False
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif ok:
        print(
            f"[smoking-data] publication gc status={payload['status']} "
            f"candidates={payload['candidate_object_count']}"
        )
    else:
        print(f"[smoking-data] publication gc failed: {payload['error_message']}")
    return 0 if ok else 1


def _main_publication_read_key(argv: list[str]) -> int:
    from smoking_data.runtime.object_store import (
        open_remote_generation,
        read_remote_parquet_key_to_ipc,
    )

    parser = argparse.ArgumentParser(
        description="Read all rows for one exact key from a pinned remote Parquet generation."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--target", required=True)
    parser.add_argument("--dataset-prefix", required=True)
    parser.add_argument("--key-json", required=True)
    parser.add_argument("--key-types-json")
    parser.add_argument("--output", required=True)
    parser.add_argument("--column", action="append", dest="columns")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        key_values = json.loads(args.key_json)
        key_types = json.loads(args.key_types_json) if args.key_types_json else None
        if not isinstance(key_values, dict) or (
            key_types is not None and not isinstance(key_types, dict)
        ):
            raise ValidationError(
                "--key-json and --key-types-json must be JSON objects.",
                code="remote.key_arguments_invalid",
            )
        handle = open_remote_generation(
            args.project_root,
            target_name=args.target,
            dataset_prefix=args.dataset_prefix,
        )
        payload = read_remote_parquet_key_to_ipc(
            handle,
            key_values=key_values,
            key_types=(
                {str(key): str(value) for key, value in key_types.items()}
                if key_types is not None
                else None
            ),
            output_ipc_path=args.output,
            projection=args.columns,
        )
        ok = True
    except (json.JSONDecodeError, SmokingDataError) as exc:
        if isinstance(exc, SmokingDataError):
            payload = {"ok": False, **exc.to_dict()}
        else:
            payload = {
                "ok": False,
                "error_code": "remote.key_arguments_invalid",
                "error_message": str(exc),
            }
        ok = False
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif ok:
        print(
            f"[smoking-data] publication read-key rows={payload['rows']} "
            f"output={payload['output_ipc_path']}"
        )
    else:
        print(f"[smoking-data] publication read-key failed: {payload['error_message']}")
    return 0 if ok else 1


def _main_source(argv: list[str]) -> int:
    from smoking_data.assets.a0101_source import execute_yaml as execute_source_yaml

    parser = argparse.ArgumentParser(description="Run a 0101 Source Asset YAML.")
    parser.add_argument("yaml_path")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = execute_source_yaml(args.yaml_path)
    payload = result.to_dict()
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"[smoking-data] source status={result.status} "
            f"job={result.job_name} datasets={len(result.dataset_paths)}"
        )
    return 0 if result.ok else 1


def _main_migrate_yaml(argv: list[str]) -> int:
    from smoking_data.runtime.yaml_migration import migrate_definition_yaml

    parser = argparse.ArgumentParser(
        description=(
            "Inspect yaml.schema_version and normalize supported legacy or current-schema "
            "Definition YAML structures."
        )
    )
    parser.add_argument("yaml_path", help="Input Definition YAML path")
    parser.add_argument("--output", required=True, help="Normalized YAML output path")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = migrate_definition_yaml(args.yaml_path, output_path=args.output)
        exit_code = 0
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "yaml.migration_failed",
            "error_message": str(exc),
        }
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(f"[smoking-data] yaml migrated output={payload['output']}")
    else:
        print(f"[smoking-data] yaml migration failed: {payload['error_message']}")
    return exit_code


def _main_migrate_parquet(argv: list[str]) -> int:
    from smoking_data.runtime.yaml_migration import generate_parquet_migration_yaml

    parser = argparse.ArgumentParser(
        description="Generate a 0201 migration Definition for an existing Parquet dataset."
    )
    parser.add_argument("input_path", help="Parquet file or recursive dataset directory")
    parser.add_argument("--output", required=True, help="Generated 0201 YAML path")
    parser.add_argument("--source-asset", required=True, choices=["0101", "0102", "0103", "0201", "0301", "0401"])
    parser.add_argument("--job-name", default="parquet_migration")
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = generate_parquet_migration_yaml(
            args.input_path,
            output_path=args.output,
            source_asset=args.source_asset,
            job_name=args.job_name,
            output_root=args.output_root,
        )
        exit_code = 0
    except (OSError, TypeError, ValueError) as exc:
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "yaml.parquet_migration_failed",
            "error_message": str(exc),
        }
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(f"[smoking-data] parquet migration YAML generated output={payload['output']}")
    else:
        print(f"[smoking-data] parquet migration YAML failed: {payload['error_message']}")
    return exit_code


def _main_verify_migrated_chain(argv: list[str]) -> int:
    """Validate and smoke-test each already-migrated Asset YAML in a Chain."""

    parser = argparse.ArgumentParser(
        description="Validate and run one smoke task for each Asset referenced by a migrated Chain."
    )
    parser.add_argument("chain_yaml", help="Current smoking-data.asset-chain.v2 YAML")
    parser.add_argument("--tasks", type=int, default=1)
    parser.add_argument("--isolated-root", default=".temp/chain-migration-smoke")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--config", dest="config_path", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.tasks != 1:
        parser.error("migrated Chain verification requires exactly one smoke task per Asset")
    chain_path = Path(args.chain_yaml).expanduser().resolve()
    project_root = (
        Path(args.project_root).expanduser().resolve()
        if args.project_root is not None
        else infer_project_root(chain_path)
    )
    try:
        document = yaml.safe_load(chain_path.read_text(encoding="utf-8")) or {}
        if not isinstance(document, dict):
            raise ValueError("Chain YAML root must be an object.")
        header = document.get("yaml")
        if not isinstance(header, dict) or header.get("schema_version") != "smoking-data.asset-chain.v2":
            raise ValueError("Chain verification requires smoking-data.asset-chain.v2.")
        assets = document.get("assets")
        if not isinstance(assets, list) or not assets:
            raise ValueError("Chain assets must be a non-empty list.")
        results: list[dict[str, Any]] = []
        overall_ok = True
        for index, item in enumerate(assets):
            if not isinstance(item, dict):
                raise ValueError(f"assets[{index}] must be an object.")
            asset_id = str(item.get("id") or f"asset_{index + 1}")
            definition = str(item.get("definition") or "")
            if not definition:
                raise ValueError(f"assets[{index}].definition is required.")
            definition_path = (chain_path.parent / definition).resolve()
            asset_result: dict[str, Any] = {
                "id": asset_id,
                "definition": str(definition_path),
                "asset_code": item.get("asset_code"),
            }
            if not definition_path.is_file():
                asset_result.update({"ok": False, "phase": "resolve", "error": "definition_missing"})
                overall_ok = False
                results.append(asset_result)
                continue
            validation_output = io.StringIO()
            with contextlib.redirect_stdout(validation_output):
                validation_code = main(
                    [
                        "validate",
                        str(definition_path),
                        "--project-root",
                        str(project_root),
                        "--json",
                    ]
                )
            validation_payload = _parse_cli_json(validation_output.getvalue())
            asset_result["validation"] = validation_payload
            if validation_code != 0:
                asset_result.update({"ok": False, "phase": "validate"})
                overall_ok = False
                results.append(asset_result)
                continue
            smoke_root = Path(args.isolated_root)
            if not smoke_root.is_absolute():
                smoke_root = project_root / smoke_root
            smoke_root = smoke_root / asset_id
            smoke_output = io.StringIO()
            with contextlib.redirect_stdout(smoke_output):
                smoke_code = main(
                    [
                        "smoke",
                        "run",
                        str(definition_path),
                        "--tasks",
                        "1",
                        "--isolated-root",
                        str(smoke_root),
                        "--project-root",
                        str(project_root),
                        *(["--config", str(args.config_path)] if args.config_path else []),
                        "--json",
                    ]
                )
            asset_result["smoke"] = _parse_cli_json(smoke_output.getvalue())
            asset_result["ok"] = smoke_code == 0
            asset_result["phase"] = "smoke"
            overall_ok = overall_ok and smoke_code == 0
            results.append(asset_result)
        result_payload = {
            "ok": overall_ok,
            "chain": str(chain_path),
            "recursive_migration": False,
            "task_limit": 1,
            "assets": results,
        }
        exit_code = 0 if overall_ok else 1
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        result_payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "chain.migration_verification_failed",
            "error_message": str(exc),
            "recursive_migration": False,
        }
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(result_payload), ensure_ascii=False, indent=2))
    elif result_payload["ok"]:
        print(f"[smoking-data] migrated Chain verification passed assets={len(result_payload['assets'])}")
    else:
        print("[smoking-data] migrated Chain verification failed")
    return exit_code


def _main_migrate_chain_run(argv: list[str]) -> int:
    """Smoke current Asset YAMLs, then generate and smoke 0201 migrations."""

    from smoking_data.runtime.yaml_migration import generate_parquet_migration_yaml

    parser = argparse.ArgumentParser(
        description=(
            "Run one smoke task for each non-0101 Chain Asset, generate a 0201 migration "
            "YAML per result, and smoke-test the migration."
        )
    )
    parser.add_argument("chain_yaml", help="Current smoking-data.asset-chain.v2 YAML")
    parser.add_argument("--migration-dir", default="migration")
    parser.add_argument("--isolated-root", default=".temp/chain-migration")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--config", dest="config_path", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    chain_path = Path(args.chain_yaml).expanduser().resolve()
    project_root = (
        Path(args.project_root).expanduser().resolve()
        if args.project_root is not None
        else infer_project_root(chain_path)
    )
    migration_dir = Path(args.migration_dir)
    if not migration_dir.is_absolute():
        migration_dir = project_root / migration_dir
    isolated_root = Path(args.isolated_root)
    if not isolated_root.is_absolute():
        isolated_root = project_root / isolated_root
    try:
        document = yaml.safe_load(chain_path.read_text(encoding="utf-8")) or {}
        if not isinstance(document, dict):
            raise ValueError("Chain YAML root must be an object.")
        header = document.get("yaml")
        if not isinstance(header, dict) or header.get("schema_version") != "smoking-data.asset-chain.v2":
            raise ValueError("Chain migration run requires smoking-data.asset-chain.v2.")
        assets = document.get("assets")
        if not isinstance(assets, list) or not assets:
            raise ValueError("Chain assets must be a non-empty list.")
        migration_dir.mkdir(parents=True, exist_ok=True)
        results: list[dict[str, Any]] = []
        overall_ok = True
        for index, item in enumerate(assets):
            if not isinstance(item, dict):
                raise ValueError(f"assets[{index}] must be an object.")
            asset_id = str(item.get("id") or f"asset_{index + 1}")
            asset_code = str(item.get("asset_code") or "")
            result: dict[str, Any] = {"id": asset_id, "asset_code": asset_code}
            if asset_code == "0101":
                result.update({"status": "skipped", "reason": "0101_excluded"})
                results.append(result)
                continue
            if asset_code not in {"0201", "0301", "0401"}:
                result.update(
                    {
                        "ok": False,
                        "status": "unsupported",
                        "reason": "task_smoke_supported_for_0201_0301_0401_only",
                    }
                )
                overall_ok = False
                results.append(result)
                continue
            definition = str(item.get("definition") or "")
            definition_path = (chain_path.parent / definition).resolve()
            result["definition"] = str(definition_path)
            if not definition_path.is_file():
                result.update({"ok": False, "status": "definition_missing"})
                overall_ok = False
                results.append(result)
                continue
            validation_output = io.StringIO()
            with contextlib.redirect_stdout(validation_output):
                validation_code = main(
                    ["validate", str(definition_path), "--project-root", str(project_root), "--json"]
                )
            result["validation"] = _parse_cli_json(validation_output.getvalue())
            if validation_code != 0:
                result.update({"ok": False, "status": "validation_failed"})
                overall_ok = False
                results.append(result)
                continue
            asset_payload = yaml.safe_load(definition_path.read_text(encoding="utf-8")) or {}
            job = asset_payload.get("job") if isinstance(asset_payload, dict) else {}
            job_name = str(job.get("name") or definition_path.stem) if isinstance(job, dict) else definition_path.stem
            asset_smoke_root = isolated_root / asset_id
            asset_output_root = asset_smoke_root / asset_code / job_name
            smoke_output = io.StringIO()
            with contextlib.redirect_stdout(smoke_output):
                smoke_code = main(
                    [
                        "smoke",
                        "run",
                        str(definition_path),
                        "--tasks",
                        "1",
                        "--isolated-root",
                        str(asset_smoke_root),
                        "--project-root",
                        str(project_root),
                        *(["--config", str(args.config_path)] if args.config_path else []),
                        "--json",
                    ]
                )
            result["smoke"] = _parse_cli_json(smoke_output.getvalue())
            if smoke_code != 0 or not list(asset_output_root.rglob("*.parquet")):
                result.update({"ok": False, "status": "smoke_failed_or_empty_output"})
                overall_ok = False
                results.append(result)
                continue
            migration_yaml = migration_dir / f"{asset_id}.0201.yaml"
            generated = generate_parquet_migration_yaml(
                asset_output_root,
                output_path=migration_yaml,
                source_asset=asset_code,
                job_name=f"{asset_id}_parquet_migration",
                output_root=str(migration_dir / "output" / asset_id),
            )
            result["migration_yaml"] = str(migration_yaml)
            result["migration_generation"] = generated
            migration_validation_output = io.StringIO()
            with contextlib.redirect_stdout(migration_validation_output):
                migration_validation_code = main(
                    [
                        "validate",
                        str(migration_yaml),
                        "--project-root",
                        str(project_root),
                        "--json",
                    ]
                )
            result["migration_validation"] = _parse_cli_json(migration_validation_output.getvalue())
            if migration_validation_code != 0:
                result.update({"ok": False, "status": "migration_validation_failed"})
                overall_ok = False
                results.append(result)
                continue
            migration_smoke_root = isolated_root / "migration" / asset_id
            migration_smoke_output = io.StringIO()
            with contextlib.redirect_stdout(migration_smoke_output):
                migration_smoke_code = main(
                    [
                        "smoke",
                        "run",
                        str(migration_yaml),
                        "--tasks",
                        "1",
                        "--isolated-root",
                        str(migration_smoke_root),
                        "--project-root",
                        str(project_root),
                        "--json",
                    ]
                )
            result["migration_smoke"] = _parse_cli_json(migration_smoke_output.getvalue())
            result["ok"] = migration_smoke_code == 0
            result["status"] = "migrated_and_smoke_verified" if result["ok"] else "migration_smoke_failed"
            overall_ok = overall_ok and result["ok"]
            results.append(result)
        result_payload = {
            "ok": overall_ok,
            "chain": str(chain_path),
            "recursive_migration": False,
            "excluded_asset_codes": ["0101"],
            "task_limit": 1,
            "migration_dir": str(migration_dir),
            "assets": results,
        }
        exit_code = 0 if overall_ok else 1
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        result_payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "chain.migration_run_failed",
            "error_message": str(exc),
            "recursive_migration": False,
        }
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(result_payload), ensure_ascii=False, indent=2))
    elif result_payload["ok"]:
        print(f"[smoking-data] Chain migration completed migration_dir={migration_dir}")
    else:
        print("[smoking-data] Chain migration failed")
    return exit_code


def _parse_cli_json(value: str) -> dict[str, Any]:
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {"ok": False, "error_message": value.strip() or "CLI returned no JSON."}
    return payload if isinstance(payload, dict) else {"ok": False, "payload": payload}


def _main_smoke_run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Run a bounded smoke task against an isolated output root."
    )
    parser.add_argument("yaml_path", help="Asset Definition YAML path")
    parser.add_argument("--tasks", type=int, default=1)
    parser.add_argument("--isolated-root", default=None)
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--config", dest="config_path", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.tasks < 1:
        parser.error("--tasks must be >= 1")
    try:
        source_path = Path(args.yaml_path).expanduser().resolve()
        project_root = (
            Path(args.project_root).expanduser().resolve()
            if args.project_root is not None
            else infer_project_root(source_path)
        )
        payload = yaml.safe_load(source_path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise ValueError("Smoke YAML root must be an object.")
        asset_code = asset_code_from_definition_path(source_path)
        if asset_code not in {"0101", "0201", "0301", "0401"}:
            raise ValueError(
                "smoke run currently supports 0101, 0201, 0301, and 0401 Asset Definitions; "
                "Chain requires an explicit upstream smoke plan."
            )
        output_root = Path(args.isolated_root or ".temp/smoke")
        if not output_root.is_absolute():
            output_root = project_root / output_root
        job_name = str((payload.get("job") or {}).get("name") or source_path.stem)
        smoke_root = output_root / asset_code / job_name
        _apply_smoke_output_root(payload, smoke_root)
        execution = payload.setdefault("execution", {})
        if not isinstance(execution, dict):
            raise ValueError("execution must be an object.")
        execution["test_run"] = {
            "final_task_limit": 1 if _is_template_definition(source_path) else args.tasks
        }
        smoke_dir = output_root / "_definitions"
        smoke_dir.mkdir(parents=True, exist_ok=True)
        smoke_path = smoke_dir / f"smoke_{source_path.name}"
        smoke_path.write_text(
            yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        namespace = argparse.Namespace(
            yaml_path=str(smoke_path),
            config_path=args.config_path,
            project_root=str(project_root),
            trigger_type="manual",
            json=args.json,
        )
        return _run_definition_cli(namespace)
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "smoke.invalid_definition",
            "error_message": str(exc),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(f"[smoking-data] smoke failed: {payload['error_message']}")
        return 1


def _is_template_definition(path: Path) -> bool:
    """Template Definitions are validation inputs and are limited to one task."""
    parts = {part.casefold() for part in path.parts}
    return "template" in parts or "templates" in parts or "template" in path.stem.casefold()


def _apply_smoke_output_root(payload: dict[str, Any], root: Path) -> None:
    output = payload.get("output")
    if not isinstance(output, dict):
        raise ValueError("Smoke Definition requires an output object.")
    artifact = output.get("artifact")
    if not isinstance(artifact, dict):
        raise ValueError("Smoke Definition requires output.artifact.")
    artifact["root_dir"] = str(root)
    logging = output.get("logging")
    if isinstance(logging, dict):
        logging["root_dir"] = str(root / "_logs")


def _main_chain_validate(argv: list[str]) -> int:
    from smoking_data.runtime.asset_chain import load_asset_chain

    parser = argparse.ArgumentParser(description="Validate an Asset Chain YAML without running it.")
    parser.add_argument("yaml_path", help="smoking-data.asset-chain.v2 YAML path")
    parser.add_argument("--config", dest="config_path", default=None)
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    effective_project_root = args.project_root or infer_project_root(args.yaml_path)
    try:
        config = load_config(
            config_path=args.config_path,
            project_root=effective_project_root,
            asset_code=asset_code_from_definition_path(args.yaml_path),
        )
        spec = load_asset_chain(args.yaml_path, config=config)
        payload = {
            "ok": True,
            "schema_version": "smoking-data.asset-chain.v2",
            "chain_name": spec.name,
            "yaml_hash": spec.yaml_hash,
            "graph_hash": spec.graph_hash,
            "topological_order": list(spec.topological_order),
            "assets": [
                {
                    "asset_id": asset.asset_id,
                    "asset_code": asset.asset_code,
                    "definition_path": str(asset.definition_path),
                    "inputs": asset.inputs,
                }
                for asset in spec.assets
            ],
        }
        exit_code = 0
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        exit_code = 1
    except Exception as exc:  # noqa: BLE001 - editor clients require JSON-safe errors.
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "yaml.parse_error",
            "error_message": str(exc),
        }
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(
            f"[smoking-data] valid chain={payload['chain_name']} graph_hash={payload['graph_hash']}"
        )
    else:
        print(
            "[smoking-data] invalid chain "
            f"code={payload['error_code']} message={payload['error_message']}"
        )
    return exit_code


def _main_registry_list(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="List reusable canonical operation specs.")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--op", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.project_root or Path.cwd()).expanduser().resolve()
    payload = read_completion_catalog(root)
    if args.op:
        payload = {
            **payload,
            "operations": [
                item for item in payload.get("operations", []) if item.get("op") == args.op
            ],
        }
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    else:
        for item in payload.get("operations", []):
            print(
                f"{item['last_alias']} [{item['spec_key']}] "
                f"definitions={item['definition_count']} "
                f"authoring={item['authoring_insert_count']}"
            )
    return 0


def _main_registry_record_insert(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Record an explicit completion selection without counting executions."
    )
    parser.add_argument("spec_key")
    parser.add_argument("--alias", default=None)
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.project_root or Path.cwd()).expanduser().resolve()
    record_authoring_insert(
        project_root=root,
        spec_key=args.spec_key,
        alias=args.alias,
    )
    payload = {"ok": True, "spec_key": args.spec_key, "alias": args.alias}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"[smoking-data] recorded authoring insert spec={args.spec_key}")
    return 0


def _main_schedule_validate(argv: list[str]) -> int:
    from smoking_data.runtime.scheduler import load_schedule_directory

    parser = argparse.ArgumentParser(description="Validate schedule YAML definitions.")
    parser.add_argument("schedule_dir", nargs="?", default=None)
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.project_root or Path.cwd()).expanduser().resolve()
    try:
        schedule_dir = args.schedule_dir or str(load_config(project_root=root).schedule_root)
        specs = load_schedule_directory(schedule_dir, project_root=root)
        payload = {
            "ok": True,
            "schema_version": "smoking-data.schedule.v1",
            "schedule_count": len(specs),
            "schedules": [spec.to_dict() for spec in specs],
        }
        exit_code = 0
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(f"[smoking-data] valid schedules={payload['schedule_count']}")
    else:
        print(f"[smoking-data] invalid schedule: {payload['error_message']}")
    return exit_code


def _main_schedule_tick(argv: list[str]) -> int:
    from smoking_data.runtime.scheduler import tick_schedules

    parser = argparse.ArgumentParser(description="Claim and run due schedule YAML definitions.")
    parser.add_argument("schedule_dir", nargs="?", default=None)
    parser.add_argument("--project-root", default=None)
    parser.add_argument(
        "--now",
        default=None,
        help="Timezone-aware ISO timestamp for deterministic operations and testing.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.project_root or Path.cwd()).expanduser().resolve()
    try:
        now = datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now else None
        schedule_dir = args.schedule_dir or str(load_config(project_root=root).schedule_root)
        payload = tick_schedules(schedule_dir, project_root=root, now=now)
        exit_code = 0 if payload["ok"] else 1
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        exit_code = 1
    except ValueError as exc:
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "schedule.invalid_now",
            "error_message": str(exc),
        }
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(f"[smoking-data] schedule tick occurrences={len(payload['occurrences'])}")
    else:
        print(f"[smoking-data] schedule tick failed: {payload.get('error_message', 'run failed')}")
    return exit_code


def _main_chain_run(argv: list[str]) -> int:
    from smoking_data.runtime.asset_chain import run_asset_chain

    parser = argparse.ArgumentParser(description="Run an Asset Chain YAML.")
    parser.add_argument("yaml_path", help="smoking-data.asset-chain.v2 YAML path")
    parser.add_argument("--config", dest="config_path", default=None)
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    effective_project_root = args.project_root or infer_project_root(args.yaml_path)
    try:
        result = run_asset_chain(
            args.yaml_path,
            config_path=args.config_path,
            project_root=effective_project_root,
        )
        payload = result.to_dict()
        exit_code = 0 if result.ok else 1
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        exit_code = 1
    except Exception as exc:  # noqa: BLE001 - callers require JSON-safe terminal errors.
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "asset_chain.unexpected_error",
            "error_message": str(exc),
        }
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(
            f"[smoking-data] chain ok name={payload['chain_name']} "
            f"metadata={payload.get('metadata_path')}"
        )
    else:
        print(
            "[smoking-data] chain failed "
            f"name={payload.get('chain_name', 'unknown')} "
            f"message={payload.get('error_message', 'asset execution failed')}"
        )
    return exit_code


def _main_parquet_schema(argv: list[str]) -> int:
    import pyarrow.parquet as pq

    from smoking_data.ops.upstream import discover_parquet_files

    parser = argparse.ArgumentParser(
        description="Read a Parquet dataset schema from file footers without scanning payload rows."
    )
    parser.add_argument("paths", nargs="+", help="Parquet file or dataset paths")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    project_root = Path(args.project_root or Path.cwd()).expanduser().resolve()
    roots = [resolve_project_path(value, project_root=project_root) for value in args.paths]
    files = discover_parquet_files(roots, recursive=True)
    if not files:
        payload = {
            "ok": False,
            "error_code": "source.empty",
            "error_message": "Parquet 파일을 찾을 수 없습니다.",
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(f"[smoking-data] parquet schema failed: {payload['error_message']}")
        return 1
    schema = pq.ParquetFile(files[0].path).schema_arrow
    payload = {
        "ok": True,
        "schema_version": "smoking-data.parquet-schema.v1",
        "source_file": str(files[0].path),
        "file_count": len(files),
        "fields": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in schema
        ],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"[smoking-data] parquet schema fields={len(schema)} source={files[0].path}")
    return 0


def _main_inspect(mode: str, argv: list[str]) -> int:
    from smoking_data.runtime.inspector import inspect_path

    descriptions = {
        "dataset": "Summarize a dataset and its managed metadata without modifying it.",
        "failure": "Collect structured failure evidence from metadata JSON files.",
        "missing": "Collect missing-data and missing-dependency evidence.",
        "profile": "Collect execution profile metrics from metadata JSON files.",
    }
    parser = argparse.ArgumentParser(description=descriptions[mode])
    parser.add_argument("path", help="Dataset, metadata JSON, or metadata directory path.")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = inspect_path(
            args.path,
            mode=mode,
            project_root=args.project_root,
        )
        exit_code = 0
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(
            f"[smoking-data] inspect mode={mode} "
            f"documents={len(payload.get('documents') or [])} path={args.path}"
        )
    else:
        print(
            f"[smoking-data] inspect failed "
            f"code={payload['error_code']} message={payload['error_message']}"
        )
    return exit_code


def _main_pwq_advise(argv: list[str]) -> int:
    from smoking_data.advisors.pwq import advise_pipeline

    parser = argparse.ArgumentParser(description="Create a PWQ tuning recommendation.")
    parser.add_argument("yaml_path", help="Pipeline YAML path")
    parser.add_argument("--metadata", dest="metadata_path", default=None)
    parser.add_argument("--config", dest="config_path", default=None)
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        handle = advise_pipeline(
            args.yaml_path,
            metadata_path=args.metadata_path,
            config_path=args.config_path,
            project_root=args.project_root,
        )
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        if args.json:
            print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
        else:
            print(f"[smoking-data] pwq advise failed: {payload['error_message']}")
        return 1
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "pwq.advice_failed",
            "error_message": str(exc),
            "error_context": {"yaml_path": str(args.yaml_path)},
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(f"[smoking-data] pwq advise failed: {payload['error_message']}")
        return 1
    payload = {"ok": True, "pwq": handle.to_dict()}
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    else:
        print(f"[smoking-data] pwq recommendation={handle.recommendation_path}")
    return 0


def _main_layout_report(argv: list[str]) -> int:
    from smoking_data.advisors.physical_layout import generate_physical_layout_report

    parser = argparse.ArgumentParser(
        description="Recommend an upstream Parquet layout from downstream execution history."
    )
    parser.add_argument("upstream_yaml")
    parser.add_argument("downstream_yaml")
    parser.add_argument(
        "--history",
        action="append",
        default=[],
        help="Metadata/result JSON file or directory. Repeatable.",
    )
    parser.add_argument("--output", default=None, help="Recommendation YAML path.")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        handle = generate_physical_layout_report(
            args.upstream_yaml,
            args.downstream_yaml,
            history_paths=args.history,
            output_path=args.output,
            project_root=args.project_root,
        )
        payload = {"ok": True, "physical_layout_report": handle.to_dict()}
        exit_code = 0
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(f"[smoking-data] physical layout recommendation={handle.path}")
    else:
        print(f"[smoking-data] physical layout report failed: {payload['error_message']}")
    return exit_code


def _main_layout_migrate(argv: list[str]) -> int:
    from smoking_data.migrations.physical_layout import migrate_layout_yaml

    parser = argparse.ArgumentParser(
        description="Run a YAML-defined 0101 physical-layout migration."
    )
    parser.add_argument("yaml_path", help="Layout migration YAML path.")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = migrate_layout_yaml(
            args.yaml_path,
            project_root=args.project_root,
        )
        payload = result.to_dict()
        exit_code = 0
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        print(
            "[smoking-data] layout migration "
            f"status={payload['status']} dataset={payload['dataset_path']}"
        )
    else:
        print(
            "[smoking-data] layout migration failed "
            f"code={payload['error_code']} message={payload['error_message']}"
        )
    return exit_code


def _main_pwq_benchmark_dummy(argv: list[str]) -> int:
    from smoking_data.advisors.pwq import benchmark_dummy_0201

    parser = argparse.ArgumentParser(description="Benchmark PWQ candidates on dummy 0201 data.")
    parser.add_argument("--root", required=True)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--max-elapsed-sec", type=float, default=120.0)
    parser.add_argument("--max-input-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.json:
            with contextlib.redirect_stdout(io.StringIO()):
                handle = benchmark_dummy_0201(
                    args.root,
                    repetitions=args.repetitions,
                    max_elapsed_sec=args.max_elapsed_sec,
                    max_input_bytes=args.max_input_bytes,
                )
        else:
            handle = benchmark_dummy_0201(
                args.root,
                repetitions=args.repetitions,
                max_elapsed_sec=args.max_elapsed_sec,
                max_input_bytes=args.max_input_bytes,
            )
    except SmokingDataError as exc:
        payload = {"ok": False, **exc.to_dict()}
        if args.json:
            print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
        else:
            print(f"[smoking-data] pwq benchmark failed: {payload['error_message']}")
        return 1
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": "pwq.benchmark_failed",
            "error_message": str(exc),
            "error_context": {"root": str(args.root)},
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(f"[smoking-data] pwq benchmark failed: {payload['error_message']}")
        return 1
    payload = {"ok": True, "benchmark": handle.to_dict()}
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    else:
        print(f"[smoking-data] pwq benchmark={handle.summary_path}")
    return 0


def _main_validate(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Validate an Asset Definition or Asset Chain YAML without running it."
    )
    parser.add_argument("yaml_path", help="Asset or Asset Chain YAML path to validate.")
    parser.add_argument("--config", dest="config_path", default=None)
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    effective_project_root = args.project_root or infer_project_root(args.yaml_path)
    try:
        raw_document = yaml.safe_load(Path(args.yaml_path).read_text(encoding="utf-8")) or {}
        raw_header = raw_document.get("yaml") if isinstance(raw_document, dict) else None
        if isinstance(raw_header, dict) and raw_header.get("schema_version") == "smoking-data.publication.v1":
            from smoking_data.runtime.object_store.config import PublicationSpec

            publication = PublicationSpec.from_mapping(raw_document.get("publication"))
            if publication is None:
                raise ValidationError("publication is required for publication.v1 YAML.")
            payload = {
                "ok": True,
                "kind": "publication",
                "schema_version": "smoking-data.publication.v1",
                "job_name": str((raw_document.get("job") or {}).get("name") or ""),
                "target": publication.target,
                "dataset_prefix": publication.dataset_prefix,
            }
            exit_code = 0
        elif _definition_kind(args.yaml_path) == "chain":
            from smoking_data.runtime.asset_chain import load_asset_chain

            config = load_config(
                config_path=args.config_path,
                project_root=effective_project_root,
                asset_code=asset_code_from_definition_path(args.yaml_path),
            )
            spec = load_asset_chain(args.yaml_path, config=config)
            payload = {
                "ok": True,
                "kind": "chain",
                "schema_version": "smoking-data.asset-chain.v2",
                "chain_name": spec.name,
                "yaml_hash": spec.yaml_hash,
                "graph_hash": spec.graph_hash,
                "topological_order": list(spec.topological_order),
            }
            exit_code = 0
        elif asset_code_from_definition_path(args.yaml_path) == "0102":
            from smoking_data.assets.a0102_calculated_fact.spec import load_calculated_fact_spec

            spec = load_calculated_fact_spec(args.yaml_path)
            payload = {
                "ok": True,
                "kind": "asset",
                "schema_version": "smoking-data.calculated-fact.v2",
                "asset_code": "0102",
                "job_name": spec.job_name,
                "upstream_definition": str(spec.upstream_definition),
                "upstream_asset_code": spec.upstream_asset_code,
                "canonical_hash": spec.canonical_hash,
            }
            exit_code = 0
        elif asset_code_from_definition_path(args.yaml_path) == "0101":
            from smoking_data.assets.a0101_source.pipeline.spec import load_source_spec

            spec = load_source_spec(args.yaml_path)
            payload = {
                "ok": True,
                "kind": "asset",
                "schema_version": "smoking-data.source.v5",
                "asset_code": "0101",
                "job_name": spec.job.name,
            }
            exit_code = 0
        elif asset_code_from_definition_path(args.yaml_path) == "0103":
            from smoking_data.assets.a0103_csv_source import load_csv_source_spec

            spec = load_csv_source_spec(args.yaml_path, project_root=effective_project_root)
            payload = {
                "ok": True,
                "kind": "asset",
                "schema_version": "smoking-data.csv-source.v1",
                "asset_code": "0103",
                "job_name": spec.job_name,
                "source_directory": str(spec.source_directory),
                "output_root": str(spec.output_root),
                "routes": [str(item["route_name"]) for item in spec.routes],
            }
            exit_code = 0
        else:
            config = load_config(
                config_path=args.config_path,
                project_root=effective_project_root,
                asset_code=asset_code_from_definition_path(args.yaml_path),
            )
            spec = load_pipeline_spec(args.yaml_path, config=config)
            registry_path = record_definition(spec, project_root=config.project_root)
            payload = {
                "ok": True,
                "kind": "asset",
                "schema_version": spec.schema_version,
                "asset_code": spec.asset_code,
                "job_name": spec.job_name,
                "graph_hash": spec.graph_hash,
                "topological_node_order": spec.graph["topological_order"],
                "topological_alias_order": spec.graph["topological_alias_order"],
                "edges": spec.graph["edges"],
                "operation_registry": str(registry_path),
            }
            exit_code = 0
    except SmokingDataError as exc:
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "error_code": exc.code,
            "error_context": exc.context,
        }
        exit_code = 1
    except Exception as exc:  # noqa: BLE001 - editor clients require JSON-safe errors.
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "error_code": "yaml.parse_error",
            "error_context": {"yaml_path": args.yaml_path},
        }
        exit_code = 1
    if args.json:
        print(json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2))
    elif payload["ok"]:
        graph_suffix = (
            f" graph_hash={payload['graph_hash']}" if payload.get("graph_hash") else ""
        )
        print(f"[smoking-data] valid job={payload['job_name']}{graph_suffix}")
    else:
        print(
            "[smoking-data] invalid "
            f"code={payload['error_code']} message={payload['error_message']}"
        )
    return exit_code


def _main_compare(argv: list[str]) -> int:
    from smoking_data.core.parity_report import read_parity_report_ok, write_parity_report

    parser = argparse.ArgumentParser(
        description="Compare two parquet datasets and write a parity report."
    )
    parser.add_argument("left_path", help="Left parquet file or dataset directory.")
    parser.add_argument("right_path", help="Right parquet file or dataset directory.")
    parser.add_argument("--report", required=True, help="Output JSON report path.")
    parser.add_argument("--label", default="parity", help="Report label.")
    parser.add_argument("--sample-rows", type=int, default=1000, help="Rows used for sample hash.")
    parser.add_argument("--left-metadata", default=None, help="Optional left metadata JSON path.")
    parser.add_argument("--right-metadata", default=None, help="Optional right metadata JSON path.")
    parser.add_argument(
        "--fail-on-diff",
        action="store_true",
        help="Return exit code 1 when row/schema/sample hash comparison differs.",
    )
    args = parser.parse_args(argv)
    report_path = write_parity_report(
        left_path=args.left_path,
        right_path=args.right_path,
        report_path=args.report,
        label=args.label,
        sample_rows=args.sample_rows,
        left_metadata_path=args.left_metadata,
        right_metadata_path=args.right_metadata,
    )
    ok = read_parity_report_ok(report_path)
    status = "ok" if ok else "diff"
    print(f"[smoking-data] parity_report={report_path} status={status}")
    return 1 if args.fail_on_diff and not ok else 0


def _main_fixture(argv: list[str]) -> int:
    from smoking_data.core.parity_fixtures import (
        write_0201_curated_parity_fixture,
        write_0201_pivot_parity_fixture,
        write_0301_join_parity_fixture,
        write_0301_multi_right_full_parity_fixture,
    )
    parser = argparse.ArgumentParser(description="Create reusable parity smoke fixture inputs.")
    parser.add_argument(
        "preset",
        choices=["0201", "0201-pivot", "0301", "0301-multi-right-full"],
        help="Fixture preset to create.",
    )
    parser.add_argument("--root", required=True, help="Fixture root directory.")
    args = parser.parse_args(argv)
    if args.preset == "0201":
        fixture = write_0201_curated_parity_fixture(args.root)
    elif args.preset == "0201-pivot":
        fixture = write_0201_pivot_parity_fixture(args.root)
    elif args.preset == "0301":
        fixture = write_0301_join_parity_fixture(args.root)
    else:
        fixture = write_0301_multi_right_full_parity_fixture(args.root)
    print(
        "[smoking-data] "
        f"fixture={fixture.preset} yaml={fixture.yaml_path} expected_rows={fixture.expected_rows}"
    )
    return 0
