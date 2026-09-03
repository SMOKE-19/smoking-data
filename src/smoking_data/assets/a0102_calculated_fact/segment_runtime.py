from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from smoking_data.core.results import StageResult
from smoking_data.runtime.dataset_artifacts import describe_dataset_artifacts
from smoking_data.runtime.events import append_stage_event
from smoking_data.runtime.task_telemetry import emit_task_telemetry_event, task_telemetry_phase
from smoking_data_engine_rs import plan_coordinates

from .calculation_manifest import (
    CalculatedSegment,
    read_calculation_manifest,
    write_calculation_manifest,
)
from .coordinates import load_coordinate_batch
from .invalidation import group_all_coordinate_expressions
from .segment_append import SegmentAppendTransaction
from .task import build_calculated_fact_task_request, wide_output_column_names
from .upstream_delta import UpstreamDeltaPlan, UpstreamSegment, plan_upstream_delta


@dataclass(frozen=True, slots=True)
class IncrementalPlanSummary:
    calculate_count: int
    reuse_count: int
    inactive_count: int = 0
    deleted_identity_count: int = 0


def execute_segment_plan(
    plan: Any,
    *,
    config: Any,
    trigger_type: str,
    telemetry: Any,
) -> StageResult:
    # Imports are intentionally local: runner owns telemetry/reporting presentation,
    # while this module owns the cut-over segment execution contract.
    from .segment_runner import (
        RUNNER_PRESET,
        _BoundedMaterializeExecutor,
        _counters,
        _ListShapeAccumulator,
        _materialize_admission,
        _upstream_delta_metadata,
        _upstream_snapshot,
        _write_run_metadata,
        schema_change_handling,
    )

    calculation_manifest_path = (
        plan.output_root / "_smoking_data" / "calculation-manifest.json"
    )
    calculated_at = datetime.now(timezone.utc)
    upstream_delta = plan_upstream_delta(
        plan, calculation_manifest_path=calculation_manifest_path
    )
    if plan.skipped_expressions:
        event_path = config.log_root / "0102_calculated_fact" / f"{plan.spec.job_name}.log"
        for item in plan.skipped_expressions:
            append_stage_event(
                event_path,
                event="calculation.warning",
                preset="0102_calculated_fact",
                job_name=plan.spec.job_name,
                details={
                    "error_code": "calculation.missing_dependency",
                    "severity": "warning",
                    "expression_name": item.expression_name,
                    "status": item.status,
                    "missing_dependencies": list(item.missing_dependencies),
                    "upstream_asset_code": plan.spec.upstream_asset_code,
                    "upstream_generation_id": upstream_delta.generation_id,
                    "upstream_schema_hash": plan.source_schema_hash,
                    "source_segment_ids": [
                        str(segment.segment_id)
                        for segment in upstream_delta.current_segments
                    ],
                },
            )
    upstream_snapshot = _upstream_snapshot(plan.upstream_files)
    previous = read_calculation_manifest(calculation_manifest_path)
    selected = _selected_segments(upstream_delta)
    selected_by_path = {str(item.path.resolve()): item for item in selected}
    expression_count = len(plan.fingerprints)
    if expression_count == 0:
        selected = ()
        selected_by_path = {}
    run_key = _run_key(upstream_delta)
    list_shapes = _ListShapeAccumulator(
        tuple(item.source for item in plan.spec.expand_columns)
    )
    segment_skips: list[dict[str, Any]] = []

    with tempfile.TemporaryDirectory(
        prefix="smoking-data-0102-", dir=config.temp_root
    ) as temporary:
        work_root = Path(temporary)
        coordinate_root = work_root / "coordinates"
        emit_task_telemetry_event(
            telemetry.endpoint,
            "phase_planned",
            task_id=None,
            details={"phase_name": "0102.plan_coordinates", "total": 1, "unit": "plan"},
        )
        with task_telemetry_phase(telemetry.endpoint, "0102.plan_coordinates"):
            if selected:
                planner_stats = plan_coordinates(
                    [str(item.path) for item in selected],
                    str(coordinate_root),
                    planner_config={
                        "row_count": plan.spec.target_rows_per_part,
                        "mode": "source_file_locality",
                        "row_keys": list(plan.spec.identity_columns),
                        # Source lineage must be unambiguous for every output part.
                        "max_source_files_per_chunk": 1,
                    },
                )
            else:
                planner_stats = {
                    "selected_row_count": 0,
                    "coordinate_file_count": 0,
                    "selected_file_count": 0,
                    "selected_row_group_count": 0,
                }
        coordinate_paths = tuple(sorted(coordinate_root.glob("*.arrow")))
        selected_rows = int(planner_stats.get("selected_row_count", 0))
        reused_rows = max(0, sum(item.rows for item in upstream_delta.current_segments) - selected_rows)
        incremental = IncrementalPlanSummary(
            calculate_count=selected_rows * expression_count,
            reuse_count=reused_rows * expression_count,
            inactive_count=0,
            deleted_identity_count=0,
        )

        if not coordinate_paths:
            for phase_name, unit in (
                ("0102.calculate_unpivot_write", "tasks"),
                ("0102.adopt_task_outputs", "tasks"),
                ("0102.transaction_setup", "transaction"),
                ("0102.commit", "generation"),
            ):
                emit_task_telemetry_event(
                    telemetry.endpoint,
                    "phase_planned",
                    task_id=None,
                    details={
                        "phase_name": phase_name,
                        "total": 0,
                        "unit": unit,
                        "skipped": True,
                    },
                )
            active = _next_active_segments(
                previous=previous,
                delta=upstream_delta,
                generation_seq=None,
                output_parts_by_segment={},
            )
            write_calculation_manifest(
                calculation_manifest_path,
                dataset_id=upstream_delta.dataset_id,
                upstream_generation_id=upstream_delta.generation_id,
                calculation_contract_hash=upstream_delta.calculation_contract_hash,
                active_segments=active,
            )
            telemetry_profile = telemetry.stop()
            metadata_path = _write_run_metadata(
                plan,
                run_key=run_key,
                trigger_type=trigger_type,
                planner_stats=planner_stats,
                incremental=incremental,
                generation_seq=None,
                task_count=0,
                output_files=(),
                reused=True,
                telemetry=telemetry_profile,
                list_shapes=list_shapes.to_dict(plan),
                upstream_snapshot=upstream_snapshot,
                planning_mode="segment_manifest_append_only",
                coordinate_passes=0,
                upstream_delta=_upstream_delta_metadata(upstream_delta),
                schema_change_handling=schema_change_handling(
                    plan, upstream_delta=upstream_delta, observed_at=calculated_at
                ),
            )
            result = StageResult.success(
                preset=RUNNER_PRESET,
                job_name=plan.spec.job_name,
                yaml_path=plan.spec.path,
                metadata_path=metadata_path,
                output_paths=[plan.output_root],
                counters=_counters(planner_stats, incremental, task_count=0, plan=plan),
                details={
                    "output_dir": str(plan.output_root),
                    "run_key": run_key,
                    "reused": True,
                    "removed_source_segments_preserved": len(
                        upstream_delta.removed_segment_ids
                    ),
                    "trigger_type": trigger_type,
                },
            )
            result.dataset_artifacts = describe_dataset_artifacts(
                result.output_paths,
                metadata_path=result.metadata_path,
                definition_sha256=plan.spec.canonical_hash,
            )
            return result

        materialize_admission = _materialize_admission(plan, config=config)
        task_count = 0
        calculated_fact_count = 0
        emit_task_telemetry_event(
            telemetry.endpoint,
            "phase_planned",
            task_id=None,
            details={"phase_name": "0102.transaction_setup", "total": 1, "unit": "transaction"},
        )
        with task_telemetry_phase(telemetry.endpoint, "0102.transaction_setup"):
            transaction_context = SegmentAppendTransaction(
                plan.output_root,
                run_key=run_key,
                identity_columns=plan.spec.identity_columns,
                partition_by=plan.spec.partition_by,
                contract=plan.output_mode,
                output_columns=(
                    wide_output_column_names(plan)
                    if plan.output_mode == "wide_calculated_v1"
                    else ()
                ),
            )
        with transaction_context as transaction:
            materializer = _BoundedMaterializeExecutor(
                workers=materialize_admission["effective_workers"],
                telemetry_endpoint=telemetry.endpoint,
                transaction=transaction,
            )
            try:
                for coordinate_index, coordinate_path in enumerate(coordinate_paths):
                    coordinate = load_coordinate_batch(
                        coordinate_path, plan, load_payload=False
                    )
                    source_paths = {
                        str(Path(item.source_file).expanduser().resolve())
                        for item in coordinate.coordinates
                    }
                    if len(source_paths) != 1:
                        _fail_ambiguous_coordinate(coordinate_path, source_paths)
                    source_path = next(iter(source_paths))
                    segment = selected_by_path.get(source_path)
                    if segment is None:
                        _fail_missing_segment(coordinate_path, source_path)
                    expression_names, missing_expressions = _segment_expression_names(
                        plan,
                        available_columns=set(coordinate.source_columns),
                    )
                    for item in missing_expressions:
                        segment_skip = {
                            **item,
                            "source_segment_id": segment.segment_id,
                            "source_file": source_path,
                            "source_schema_hash": coordinate.source_schema_hash,
                        }
                        segment_skips.append(segment_skip)
                        append_stage_event(
                            config.log_root / "0102_calculated_fact" / f"{plan.spec.job_name}.log",
                            event="calculation.warning",
                            preset="0102_calculated_fact",
                            job_name=plan.spec.job_name,
                            details={
                                "error_code": "calculation.segment_missing_dependency",
                                "severity": "warning",
                                **segment_skip,
                                "upstream_schema_hash": coordinate.source_schema_hash,
                                "upstream_generation_id": upstream_delta.generation_id,
                            },
                        )
                    wide_output = plan.output_mode == "wide_calculated_v1"
                    if not expression_names and not wide_output:
                        continue
                    calculated_fact_count += coordinate.selected_row_count * len(
                        expression_names
                    )
                    group = group_all_coordinate_expressions(
                        coordinate,
                        expression_order=expression_names,
                        allow_empty=wide_output,
                    )
                    task_root = work_root / "tasks" / f"{task_count:06d}"
                    request = build_calculated_fact_task_request(
                        plan,
                        coordinate_path=coordinate_path,
                        group=group,
                        generation_seq=transaction.generation_seq,
                        output_dir=task_root / "output",
                        lookup_cache_dir=work_root / "lookup-cache",
                        task_index=task_count,
                        batch_size=plan.spec.target_rows_per_part,
                        source_fingerprint=_segment_fingerprint(
                            segment, upstream_delta.calculation_contract_hash
                        ),
                        calculated_at=calculated_at,
                        available_columns=coordinate.source_columns,
                    )
                    for phase_name in (
                        "0102.calculate_unpivot_write",
                        "0102.adopt_task_outputs",
                    ):
                        emit_task_telemetry_event(
                            telemetry.endpoint,
                            "phase_planned",
                            task_id=None,
                            details={"phase_name": phase_name, "total": 1, "unit": "tasks"},
                        )
                    materializer.submit(
                        request,
                        task_id=f"0102-{task_count:06d}",
                        coordinate_index=coordinate_index,
                        source_segment_id=segment.segment_id,
                    )
                    task_count += 1
                task_stats = materializer.finish()
                if task_count == 0:
                    for phase_name in (
                        "0102.calculate_unpivot_write",
                        "0102.adopt_task_outputs",
                    ):
                        emit_task_telemetry_event(
                            telemetry.endpoint,
                            "phase_planned",
                            task_id=None,
                            details={
                                "phase_name": phase_name,
                                "total": 0,
                                "unit": "tasks",
                                "skipped": True,
                            },
                        )
            except BaseException:
                materializer.abort()
                raise
            if task_count:
                emit_task_telemetry_event(
                    telemetry.endpoint,
                    "phase_planned",
                    task_id=None,
                    details={"phase_name": "0102.commit", "total": 1, "unit": "generation"},
                )
                with task_telemetry_phase(telemetry.endpoint, "0102.commit"):
                    appended = transaction.commit(
                        metadata={
                            "plan_hash": plan.plan_hash,
                            "definition_hash": plan.spec.canonical_hash,
                            "trigger_type": trigger_type,
                            "task_count": task_count,
                            "incremental_mode": "source_segment_append_only",
                        }
                    )
            else:
                emit_task_telemetry_event(
                    telemetry.endpoint,
                    "phase_planned",
                    task_id=None,
                    details={
                        "phase_name": "0102.commit",
                        "total": 0,
                        "unit": "generation",
                        "skipped": True,
                    },
                )
                transaction.rollback()
                appended = None

        incremental = IncrementalPlanSummary(
            calculate_count=calculated_fact_count,
            reuse_count=reused_rows * expression_count,
            inactive_count=0,
            deleted_identity_count=0,
        )

        active = _next_active_segments(
            previous=previous,
            delta=upstream_delta,
            generation_seq=appended.generation_seq if appended is not None else None,
            output_parts_by_segment=appended.output_parts_by_segment if appended is not None else {},
        )
        write_calculation_manifest(
            calculation_manifest_path,
            dataset_id=upstream_delta.dataset_id,
            upstream_generation_id=upstream_delta.generation_id,
            calculation_contract_hash=upstream_delta.calculation_contract_hash,
            active_segments=active,
        )

    telemetry_profile = telemetry.stop()
    metadata_path = _write_run_metadata(
        plan,
        run_key=run_key,
        trigger_type=trigger_type,
        planner_stats=planner_stats,
        incremental=incremental,
        generation_seq=appended.generation_seq if appended is not None else None,
        task_count=task_count,
        output_files=appended.files if appended is not None else (),
        reused=appended.reused if appended is not None else True,
        task_stats=task_stats,
        telemetry=telemetry_profile,
        list_shapes=list_shapes.to_dict(plan),
        upstream_snapshot=upstream_snapshot,
        planning_mode="segment_manifest_append_only",
        coordinate_passes=1,
        materialize_admission=materialize_admission,
        upstream_delta=_upstream_delta_metadata(upstream_delta),
        schema_change_handling=schema_change_handling(
            plan,
            upstream_delta=upstream_delta,
            observed_at=calculated_at,
            segment_skips=segment_skips,
        ),
    )
    result = StageResult.success(
        preset=RUNNER_PRESET,
        job_name=plan.spec.job_name,
        yaml_path=plan.spec.path,
        metadata_path=metadata_path,
        output_paths=[plan.output_root],
        counters=_counters(planner_stats, incremental, task_count=task_count, plan=plan),
        details={
            "output_dir": str(plan.output_root),
            "manifest_path": str(plan.output_root / "_dataset.manifest.json"),
            "calculation_manifest_path": str(calculation_manifest_path),
            "generation_seq": appended.generation_seq if appended is not None else None,
            "run_key": run_key,
            "reused": appended.reused if appended is not None else True,
            "trigger_type": trigger_type,
        },
    )
    result.dataset_artifacts = describe_dataset_artifacts(
        result.output_paths,
        metadata_path=result.metadata_path,
        definition_sha256=plan.spec.canonical_hash,
    )
    return result


def _selected_segments(delta: UpstreamDeltaPlan) -> tuple[UpstreamSegment, ...]:
    if delta.selected_segments:
        return delta.selected_segments
    # Receipt-less input has no durable segment contract. It remains a full-scan
    # fallback, but each file still receives deterministic task lineage.
    return tuple(
        UpstreamSegment(
            segment_id=hashlib.sha256(str(path).encode()).hexdigest(),
            relative_path=str(path),
            path=path,
            sha256=hashlib.sha256(
                f"{path.stat().st_size}:{path.stat().st_mtime_ns}".encode()
            ).hexdigest(),
            rows=0,
        )
        for path in delta.selected_files
    )


def _segment_expression_names(
    plan: Any,
    *,
    available_columns: set[str],
) -> tuple[tuple[str, ...], tuple[dict[str, Any], ...]]:
    selected: list[str] = []
    skipped: list[dict[str, Any]] = []
    for fingerprint in plan.fingerprints:
        missing = sorted(set(fingerprint.source_columns).difference(available_columns))
        if missing:
            skipped.append(
                {
                    "expression_name": fingerprint.name,
                    "status": "skipped_missing_dependency",
                    "missing_dependencies": [
                        {
                            "logical_name": column,
                            "physical_column": column,
                            "kind": "source",
                        }
                        for column in missing
                    ],
                }
            )
            continue
        selected.append(fingerprint.name)
    return tuple(selected), tuple(skipped)


def _run_key(delta: UpstreamDeltaPlan) -> str:
    document = {
        "dataset_id": delta.dataset_id,
        "generation_id": delta.generation_id,
        "calculation_contract_hash": delta.calculation_contract_hash,
        "selected": [
            [item.relative_path, item.sha256] for item in _selected_segments(delta)
        ],
        "removed": sorted(delta.removed_segment_ids),
    }
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _segment_fingerprint(segment: UpstreamSegment, contract_hash: str) -> str:
    return hashlib.sha256(
        f"{segment.segment_id}:{segment.sha256}:{contract_hash}".encode()
    ).hexdigest()


def _next_active_segments(
    *,
    previous: Any,
    delta: UpstreamDeltaPlan,
    generation_seq: int | None,
    output_parts_by_segment: Any,
) -> tuple[CalculatedSegment, ...]:
    old_by_path = previous.segments_by_path if previous is not None else {}
    selected_ids = {item.segment_id for item in delta.selected_segments}
    result = []
    for current in delta.current_segments:
        if current.segment_id in selected_ids:
            result.append(
                CalculatedSegment(
                    segment_id=current.segment_id,
                    relative_path=current.relative_path,
                    sha256=current.sha256,
                    rows=current.rows,
                    output_generation_seq=generation_seq,
                    output_parts=tuple(
                        output_parts_by_segment.get(current.segment_id, ())
                    ),
                )
            )
            continue
        prior = old_by_path.get(current.relative_path)
        if prior is not None and prior.sha256 == current.sha256:
            result.append(prior)
    return tuple(result)


def _fail_ambiguous_coordinate(path: Path, sources: set[str]) -> None:
    from smoking_data.core.exceptions import ValidationError

    raise ValidationError(
        "0102 coordinate must reference exactly one source segment.",
        code="incremental.ambiguous_segment_lineage",
        context={"coordinate": str(path), "source_files": sorted(sources)},
    )


def _fail_missing_segment(path: Path, source: str) -> None:
    from smoking_data.core.exceptions import ValidationError

    raise ValidationError(
        "0102 coordinate source is absent from the selected segment plan.",
        code="incremental.segment_mismatch",
        context={"coordinate": str(path), "source_file": source},
    )
