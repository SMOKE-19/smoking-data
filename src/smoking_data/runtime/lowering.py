from __future__ import annotations

import hashlib
import json
from typing import Any

from smoking_data.core.exceptions import ValidationError
from smoking_data.core.execution_plan import compile_execution_plan
from smoking_data.core.operations import OperationKind
from smoking_data.core.pipeline import PipelineSpec
from smoking_data.ops.projection import resolve_filter_expression
from smoking_data.runtime.yaml_loader import PresetSpec

CURATED_KINDS = frozenset(
    {
        OperationKind.FILTER,
        OperationKind.TYPE_CAST,
        OperationKind.ADD_CALC,
        OperationKind.REFERENCE_REPLACE,
        OperationKind.LIST_RESTORE,
        OperationKind.BUILD_SIDECAR,
        OperationKind.ACTIVE_ROW_SELECTION,
        OperationKind.MATERIALIZE,
        OperationKind.INCLUDE_COLUMNS,
        OperationKind.EXCLUDE_COLUMNS,
        OperationKind.RENAME_COLUMNS,
        OperationKind.DATA_ASSERTION,
        OperationKind.UNPIVOT,
        OperationKind.UNNEST,
        OperationKind.PIVOT,
        OperationKind.WRITE_DATASET,
    }
)


def lower_pipeline_spec(spec: PipelineSpec) -> tuple[str, PresetSpec, dict[str, Any]]:
    operations = spec.logical_plan.operations
    physical_execution_hash = hashlib.sha256(
        json.dumps(
            spec.execution,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()
    execution_plan = compile_execution_plan(
        spec.logical_plan,
        physical_plan_hash=physical_execution_hash,
    )
    operation_trace = [
        {
            "operation_id": operation.operation_id,
            "kind": operation.kind,
            "execution_target": _operation_execution_target(
                operation.kind,
                selector=operation.kind == OperationKind.ACTIVE_ROW_SELECTION.value,
            ),
        }
        for operation in execution_plan.operations
    ]
    if any(operation.kind is OperationKind.JOIN for operation in operations):
        raw = _lower_join(spec)
        target = "join"
        preset = "0301"
    else:
        unsupported = sorted(
            {
                operation.kind.value
                for operation in operations
                if operation.kind not in CURATED_KINDS
            }
        )
        if unsupported:
            raise ValidationError(
                "The operation sequence has no bounded Rust physical kernel.",
                code="physical_kernel.unsupported_sequence",
                context={"operations": unsupported},
            )
        raw = _lower_curated(spec)
        target = "curated"
        preset = "0201"
    raw["__pipeline"] = {
        "schema_version": spec.schema_version,
        "asset_code": spec.asset_code,
        "graph_hash": spec.graph_hash,
        "graph": spec.graph,
        "logical_plan_hash": spec.logical_plan.plan_hash,
        "execution_plan_hash": execution_plan.plan_hash,
        "operation_trace": operation_trace,
        "rust_operation_trace": [
            item
            for item in operation_trace
            if item["kind"]
            not in {
                OperationKind.BUILD_SIDECAR.value,
                OperationKind.MATERIALIZE.value,
            }
        ],
        "writer_output_columns": _infer_curated_writer_output_columns(spec)
        if target == "curated"
        else [],
        "writer_partition_columns": list(
            next(
                operation.config["partition_by"]
                for operation in operations
                if operation.kind is OperationKind.WRITE_DATASET
            )
        ),
        "single_file_output": spec.asset_code == "0401",
        "join_post_operations": _infer_join_post_operations(spec) if target == "join" else [],
        "explicit_physical_boundaries": _explicit_physical_boundaries(spec),
        "probe_manifests": dict(spec.raw.get("__probe_manifests") or {}),
    }
    return (
        target,
        PresetSpec(
            preset=preset,
            job_name=spec.job_name,
            yaml_path=spec.yaml_path,
            raw=raw,
            yaml_hash=spec.yaml_hash,
        ),
        execution_plan.to_dict(),
    )


def _operation_execution_target(kind: str, *, selector: bool) -> str:
    if kind == OperationKind.BUILD_SIDECAR.value:
        return "planner_sidecar"
    if selector:
        return "planner_selector"
    if kind == OperationKind.MATERIALIZE.value:
        return "planner_materialize"
    if kind == OperationKind.FILTER.value:
        return "planner_filter"
    if kind == OperationKind.DATA_ASSERTION.value:
        return "rust_dataset_validator"
    if kind == OperationKind.JOIN.value:
        return "rust_join"
    if kind == OperationKind.WRITE_DATASET.value:
        return "rust_writer"
    return "rust_payload"


def _lower_curated(spec: PipelineSpec) -> dict[str, Any]:
    operations = spec.logical_plan.operations
    operation_phases = dict(spec.raw.get("operation_phases") or {})
    selectors = [
        operation
        for operation in operations
        if operation.kind is OperationKind.ACTIVE_ROW_SELECTION
    ]
    selector = selectors[0] if selectors else None
    _validate_curated_operation_order(
        operations,
        phase_contract=bool(spec.raw.get("operation_phases")),
    )
    write = _write_operation(operations)
    sink = spec.sinks[str(write.config["sink"])]
    source_name = _primary_source_name(spec)
    source = spec.sources[source_name]
    payload: dict[str, Any] = {}
    selector_payload: dict[str, Any] = {}
    list_restore: dict[str, Any] = {}
    pivot: dict[str, Any] = {}
    pre_pivot_operations: list[dict[str, Any]] = []
    post_operations: list[dict[str, Any]] = []
    dataset_assertions: list[dict[str, Any]] = []
    pivot_index = next(
        (
            index
            for index, operation in enumerate(operations)
            if operation.kind is OperationKind.PIVOT
        ),
        None,
    )
    for index, operation in enumerate(operations):
        if operation.kind in {
            OperationKind.BUILD_SIDECAR,
            OperationKind.ACTIVE_ROW_SELECTION,
            OperationKind.MATERIALIZE,
        }:
            continue
        operation_payload = (
            selector_payload
            if operation_phases.get(operation.operation_id) == "build_sidecar"
            else payload
        )
        if operation.kind is OperationKind.FILTER:
            if "filter_sql" in operation_payload:
                raise ValidationError(
                    "The curated physical kernel currently supports one pre-selector filter.",
                    code="physical_kernel.multiple_filter_unsupported",
                    context={"operation_id": operation.operation_id},
                )
            dialect, expression = resolve_filter_expression(operation.config)
            if dialect == "spotfire_expression":
                from spotfire_expr_normalizer import normalize_expression

                expression = normalize_expression(expression)
            operation_payload["filter_sql"] = expression
        elif operation.kind is OperationKind.TYPE_CAST:
            operation_payload.setdefault("type_casts", []).extend(operation.config["columns"])
        elif operation.kind is OperationKind.ADD_CALC:
            operation_payload.setdefault("add_calc", []).extend(operation.config["expressions"])
        elif operation.kind is OperationKind.REFERENCE_REPLACE:
            payload.setdefault("reference_replace", []).append(operation.config)
        elif (
            operation.kind is OperationKind.INCLUDE_COLUMNS
            or operation.kind is OperationKind.EXCLUDE_COLUMNS
        ):
            if pivot_index is not None and index < pivot_index:
                payload["include_columns"] = list(operation.config["resolved_columns"])
            else:
                post_operations.append(
                    {
                        "operation_id": operation.operation_id,
                        "kind": operation.kind.value,
                        "config": operation.config,
                    }
                )
        elif operation.kind is OperationKind.LIST_RESTORE:
            list_restore = {"enabled": True, **operation.config}
        elif operation.kind is OperationKind.PIVOT:
            pivot = {"enabled": True, **operation.config}
        elif operation.kind is OperationKind.DATA_ASSERTION:
            dataset_assertions.append(operation.config)
        elif operation.kind is OperationKind.RENAME_COLUMNS:
            lowered = {
                "operation_id": operation.operation_id,
                "kind": operation.kind.value,
                "config": operation.config,
            }
            mapping = dict(operation.config.get("resolved_mapping") or {})
            later_boundary = pivot_index if pivot_index is not None else len(operations)
            later_operations = operations[index + 1 : later_boundary + 1]
            consumed_before_boundary = any(
                target in _operation_references(later)
                for target in mapping.values()
                for later in later_operations
            )
            if consumed_before_boundary:
                pre_pivot_operations.append(lowered)
            else:
                post_operations.append(lowered)
        elif operation.kind is OperationKind.UNPIVOT:
            post_operations.append(
                {
                    "operation_id": operation.operation_id,
                    "kind": operation.kind.value,
                    "config": operation.config,
                }
            )
        elif operation.kind is OperationKind.UNNEST:
            post_operations.append(
                {
                    "operation_id": operation.operation_id,
                    "kind": operation.kind.value,
                    "config": operation.config,
                }
            )
    if post_operations:
        payload["post_operations"] = post_operations
        payload["final_post_projection"] = True
    if pre_pivot_operations:
        payload["pre_pivot_operations"] = pre_pivot_operations
    if dataset_assertions:
        payload["dataset_assertions"] = dataset_assertions
    partition_by = list(write.config["partition_by"])
    if len(partition_by) != 1:
        raise ValidationError(
            "The curated physical kernel requires exactly one partition column.",
            code="physical_kernel.partition_arity",
            context={"partition_by": partition_by},
        )
    sort_first_config = {
        "enabled": (
            selector is not None and selector.config.get("method") != "all_rows"
        ),
        "operation_id": selector.operation_id if selector is not None else None,
        "group_keys": list(selector.config.get("group_keys") or [])
        if selector is not None
        else [],
        "sort": list(selector.config.get("sort") or []) if selector is not None else [],
        "tie_policy": "source_path_row_index",
    }
    if selector_payload and (
        selector is None or selector.config.get("method") == "all_rows"
    ):
        for key, value in selector_payload.items():
            existing = payload.get(key)
            if isinstance(value, list) and isinstance(existing, list):
                payload[key] = [*value, *existing]
            elif existing is None:
                payload[key] = value
            elif existing != value:
                raise ValidationError(
                    "build_sidecar payload conflicts with materialize payload.",
                    code="physical_kernel.conflicting_payload_operation",
                    context={"key": key},
                )
    if (
        operation_phases
        and selector is not None
        and selector.config.get("method") != "all_rows"
    ):
        sort_first_config["payload"] = selector_payload
    return {
        "preset": "0201",
        "job": {"name": spec.job_name},
        "source": {
            "upstream": {
                "paths": list(source.paths),
                "recursive": True,
                "probe_manifest": dict(spec.raw.get("__probe_manifests") or {}).get(source_name),
            },
            "payload": payload,
        },
        "row_selection": {
            "sort_first": sort_first_config,
        },
        "list_restore": list_restore,
        "pivot": pivot,
        "execution": _lower_execution(spec),
        "output": {
            "artifact": {
                **dict(spec.raw["output"]["artifact"]),
                "root_dir": sink.path,
            },
            "logging": dict(spec.raw["output"].get("logging") or {}),
        },
    }


def _operation_references(operation: Any) -> set[str]:
    """Return logical columns consumed by an operation."""
    references = {
        *getattr(operation, "input_columns", ()),
        *getattr(operation, "group_keys", ()),
        *getattr(operation, "partition_keys", ()),
        *(column for column, _ in getattr(operation, "ordering", ())),
    }
    return {str(value) for value in references if str(value)}


def _infer_curated_writer_output_columns(spec: PipelineSpec) -> list[str]:
    source_name = _primary_source_name(spec)
    source_columns_map = spec.raw.get("__source_columns") if isinstance(spec.raw, dict) else {}
    source_columns = []
    if isinstance(source_columns_map, dict):
        source_columns = list(source_columns_map.get(source_name, ()) or [])
    columns = list(source_columns)
    for operation in spec.logical_plan.operations:
        kind = operation.kind
        if kind in {
            OperationKind.FILTER,
            OperationKind.ACTIVE_ROW_SELECTION,
            OperationKind.DATA_ASSERTION,
            OperationKind.WRITE_DATASET,
        }:
            continue
        if kind is OperationKind.INCLUDE_COLUMNS:
            columns = list(
                operation.config.get("resolved_columns") or operation.config.get("columns") or []
            )
            continue
        if kind is OperationKind.EXCLUDE_COLUMNS:
            resolved = operation.config.get("resolved_columns")
            if resolved:
                columns = list(resolved)
            else:
                excluded = set(operation.config.get("columns") or [])
                columns = [column for column in columns if column not in excluded]
            continue
        if kind is OperationKind.RENAME_COLUMNS:
            mapping = dict(operation.config.get("resolved_mapping") or {})
            columns = [mapping.get(column, column) for column in columns]
            continue
        if kind is OperationKind.UNPIVOT:
            columns = list(operation.config.get("id_columns") or [])
            columns.extend(
                [
                    str(operation.config.get("name_column") or ""),
                    str(operation.config.get("value_column") or ""),
                ]
            )
            columns = [column for column in columns if column]
            continue
        if kind is OperationKind.UNNEST:
            mapping = {
                str(item["source"]): str(item["output"])
                for item in operation.config.get("columns") or []
            }
            columns = [mapping.get(column, column) for column in columns]
            continue
        if kind is OperationKind.PIVOT:
            return []
        for output in operation.output_columns:
            if output.name not in columns:
                columns.append(output.name)
    return columns


def _validate_curated_operation_order(
    operations: tuple[Any, ...],
    *,
    phase_contract: bool = False,
) -> None:
    for kind, maximum in {
        OperationKind.FILTER: 1,
        OperationKind.LIST_RESTORE: 1,
        OperationKind.BUILD_SIDECAR: 1,
        OperationKind.ACTIVE_ROW_SELECTION: 1,
        OperationKind.MATERIALIZE: 1,
        OperationKind.PIVOT: 1,
    }.items():
        matches = [operation.operation_id for operation in operations if operation.kind is kind]
        if len(matches) > maximum:
            raise ValidationError(
                "The bounded curated kernel does not support repeated operations of this kind.",
                code="physical_kernel.repeated_operation_unsupported",
                context={"operation": kind.value, "operation_ids": matches},
            )
    assertions = [
        index
        for index, operation in enumerate(operations)
        if operation.kind is OperationKind.DATA_ASSERTION
    ]
    if assertions:
        suffix = operations[assertions[0] :]
        if any(
            operation.kind not in {OperationKind.DATA_ASSERTION, OperationKind.WRITE_DATASET}
            for operation in suffix
        ):
            raise ValidationError(
                "data_assertion must be a terminal validation suffix immediately before write_dataset.",
                code="operation.assertion_position_invalid",
                context={"operation_id": operations[assertions[0]].operation_id},
            )

    pivot_index = next(
        (
            index
            for index, operation in enumerate(operations)
            if operation.kind is OperationKind.PIVOT
        ),
        None,
    )
    prior_phase = -1
    for index, operation in enumerate(operations):
        if operation.kind is OperationKind.FILTER:
            phase = (
                5
                if phase_contract
                and any(
                    prior.kind is OperationKind.MATERIALIZE for prior in operations[:index]
                )
                else 0
            )
        elif phase_contract and any(
            prior.kind is OperationKind.MATERIALIZE for prior in operations[:index]
        ) and operation.kind in {
            OperationKind.TYPE_CAST,
            OperationKind.REFERENCE_REPLACE,
            OperationKind.ADD_CALC,
        }:
            phase = 5
        elif operation.kind is OperationKind.TYPE_CAST:
            phase = 1
        elif operation.kind is OperationKind.REFERENCE_REPLACE:
            phase = 2
        elif operation.kind is OperationKind.ADD_CALC:
            phase = 3
        elif (
            operation.kind is OperationKind.BUILD_SIDECAR
            or operation.kind is OperationKind.ACTIVE_ROW_SELECTION
            or operation.kind is OperationKind.MATERIALIZE
        ):
            phase = 4
        elif operation.kind is OperationKind.LIST_RESTORE:
            phase = (
                5
                if any(prior.kind is OperationKind.MATERIALIZE for prior in operations[:index])
                else 1
            )
        elif operation.kind in {OperationKind.INCLUDE_COLUMNS, OperationKind.EXCLUDE_COLUMNS}:
            phase = 5 if pivot_index is not None and index < pivot_index else 7
        elif operation.kind is OperationKind.PIVOT:
            phase = 6
        elif operation.kind in {
            OperationKind.RENAME_COLUMNS,
            OperationKind.UNPIVOT,
            OperationKind.UNNEST,
        }:
            phase = 7
        elif operation.kind is OperationKind.DATA_ASSERTION:
            phase = 8
        else:
            phase = 9
        if phase < prior_phase:
            raise ValidationError(
                "Operation order cannot be preserved by the bounded Rust physical kernel.",
                code="physical_kernel.operation_order_unsupported",
                context={
                    "operation_id": operation.operation_id,
                    "operation": operation.kind.value,
                    "required_phase": phase,
                    "prior_phase": prior_phase,
                },
            )
        prior_phase = phase


def _lower_join(spec: PipelineSpec) -> dict[str, Any]:
    operations = spec.logical_plan.operations
    illegal = [
        operation.kind.value
        for operation in operations
        if operation.kind
        not in {
            OperationKind.JOIN,
            OperationKind.MATERIALIZE,
            OperationKind.INCLUDE_COLUMNS,
            OperationKind.EXCLUDE_COLUMNS,
            OperationKind.RENAME_COLUMNS,
            OperationKind.UNPIVOT,
            OperationKind.DATA_ASSERTION,
            OperationKind.WRITE_DATASET,
        }
    ]
    if illegal:
        raise ValidationError(
            "Join physical lowering currently accepts join operations followed by write_dataset.",
            code="physical_kernel.unsupported_sequence",
            context={"operations": illegal},
        )
    seen_suffix = False
    for operation in operations:
        if operation.kind in {OperationKind.WRITE_DATASET, OperationKind.MATERIALIZE}:
            continue
        if operation.kind is OperationKind.JOIN:
            if seen_suffix:
                raise ValidationError(
                    "Join operations must appear before post-join suffix operations.",
                    code="physical_kernel.operation_order_unsupported",
                    context={
                        "operation_id": operation.operation_id,
                        "operation": operation.kind.value,
                    },
                )
            continue
        seen_suffix = True
    write = _write_operation(operations)
    sink = spec.sinks[str(write.config["sink"])]
    right_names = {
        str(operation.config["right_source"])
        for operation in operations
        if operation.kind is OperationKind.JOIN
    }
    left_names = [name for name in spec.sources if name not in right_names]
    if len(left_names) != 1:
        raise ValidationError(
            "Join pipeline requires exactly one source not referenced as right_source.",
            code="join.left_source_ambiguous",
            context={"candidates": left_names},
        )
    partition_by = list(write.config["partition_by"])
    if len(partition_by) != 1:
        raise ValidationError(
            "Join output requires one partition column.", code="physical_kernel.partition_arity"
        )
    joins = [operation for operation in operations if operation.kind is OperationKind.JOIN]
    first = joins[0]
    right_sources = []
    for operation in joins:
        source = spec.sources[str(operation.config["right_source"])]
        right_sources.append(
            {
                "name": source.name,
                "upstream": {"paths": list(source.paths), "recursive": True},
                "join": {
                    "left_on": list(operation.config["left_on"]),
                    "right_on": list(operation.config["right_on"]),
                    "how": "left",
                },
                "suffix": operation.config.get("suffix", f"_{source.name}"),
                "columns": operation.config.get("columns") or {},
            }
        )
    left = spec.sources[left_names[0]]
    if left.keyspace is None:
        raise ValidationError(
            "0301 lowering requires a build_sidecar keyspace source.",
            code="join_keyspace.required",
        )
    return {
        "preset": "0301",
        "job": {"name": spec.job_name},
        "left": {
            "upstream": {"paths": list(left.paths), "recursive": True},
            **({"keyspace": dict(left.keyspace)} if left.keyspace is not None else {}),
        },
        "right_sources": right_sources,
        "join": {
            "left_on": list(first.config["left_on"]),
            "right_on": list(first.config["right_on"]),
            "how": "left",
            "left_partition_key_column": partition_by[0],
            "right_partition_key_column": "",
        },
        "execution": _lower_execution(spec),
        "output": {
            "artifact": {
                **dict(spec.raw["output"]["artifact"]),
                "root_dir": sink.path,
            },
            "logging": dict(spec.raw["output"].get("logging") or {}),
        },
    }


def _infer_join_post_operations(spec: PipelineSpec) -> list[dict[str, Any]]:
    operations = spec.logical_plan.operations
    return [
        {
            "operation_id": operation.operation_id,
            "kind": operation.kind.value,
            "config": operation.config,
        }
        for operation in operations
        if operation.kind
        in {
            OperationKind.INCLUDE_COLUMNS,
            OperationKind.EXCLUDE_COLUMNS,
            OperationKind.RENAME_COLUMNS,
            OperationKind.UNPIVOT,
            OperationKind.DATA_ASSERTION,
        }
    ]


def _write_operation(operations: tuple[Any, ...]) -> Any:
    writes = [
        operation for operation in operations if operation.kind is OperationKind.WRITE_DATASET
    ]
    if len(writes) != 1 or operations[-1] is not writes[0]:
        raise ValidationError(
            "Exactly one final write_dataset operation is required.",
            code="pipeline.write_required",
        )
    return writes[0]


def _lower_execution(spec: PipelineSpec) -> dict[str, Any]:
    execution = dict(spec.execution)
    materializations = [
        operation
        for operation in spec.logical_plan.operations
        if operation.kind is OperationKind.MATERIALIZE
    ]
    if not materializations:
        return execution
    materialize = materializations[-1]
    boundary = dict(materialize.config.get("part_boundary") or {})
    execution["workers"] = int(materialize.config["workers"])
    execution["max_tasks_per_child"] = int(materialize.config["max_tasks_per_child"])
    execution["target_rows_per_part"] = int(boundary["target_rows"])
    if boundary.get("target_key_groups") is not None:
        execution["target_key_groups_per_part"] = int(boundary["target_key_groups"])
    return execution


def _explicit_physical_boundaries(spec: PipelineSpec) -> list[dict[str, Any]]:
    return [
        {
            "operation_id": operation.operation_id,
            "kind": operation.kind.value,
            "config": operation.config,
        }
        for operation in spec.logical_plan.operations
        if operation.kind in {OperationKind.BUILD_SIDECAR, OperationKind.MATERIALIZE}
    ]


def _primary_source_name(spec: PipelineSpec) -> str:
    referenced = {
        str(operation.config.get("right_source"))
        for operation in spec.logical_plan.operations
        if operation.kind is OperationKind.JOIN
    }
    candidates = [name for name in spec.sources if name not in referenced]
    if len(candidates) != 1:
        raise ValidationError(
            "Curated pipeline requires exactly one primary source.",
            code="source.primary_ambiguous",
            context={"candidates": candidates},
        )
    return candidates[0]
