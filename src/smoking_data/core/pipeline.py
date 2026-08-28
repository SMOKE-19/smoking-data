from __future__ import annotations

import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from smoking_data.core.exceptions import ValidationError
from smoking_data.core.logical_plan import (
    LogicalOperationPlan,
    _compile_pivot_operation,
    _ir_expression_lineage,
    _ir_input_columns,
    _operation,
    _ordering,
    _plan,
    _sql_identifier_columns,
)
from smoking_data.core.operations import ColumnContract, OperationKind
from smoking_data.core.pipeline_dag import (
    CURATED_PIPELINE_SCHEMA_VERSION,
    PIPELINE_SCHEMA_VERSION,
    PUBLIC_EXECUTION_KEYS,
    normalize_pipeline_document,
)

SUPPORTED_PIPELINE_SCHEMA_VERSIONS = frozenset(
    {PIPELINE_SCHEMA_VERSION, CURATED_PIPELINE_SCHEMA_VERSION}
)
INTERNAL_ROOT_KEYS = frozenset(
    {
        "schema_version",
        "job",
        "migration",
        "sources",
        "operations",
        "sinks",
        "output",
        "execution",
        "operation_phases",
    }
)
SOURCE_KEYS = frozenset(
    {
        "kind",
        "paths",
        "union_by_name",
        "missing_columns",
        "incompatible_dtypes",
        "asset_definition",
        "asset_definition_hash",
        "asset_code",
        "combined_members",
        "source_column",
        "duplicate_path_policy",
    }
)
SINK_KEYS = frozenset({"kind", "path", "compression", "overwrite"})
EXECUTION_KEYS = PUBLIC_EXECUTION_KEYS


@dataclass(frozen=True, slots=True)
class SourceSpec:
    name: str
    kind: str
    paths: tuple[str, ...]
    union_by_name: bool
    missing_columns: str
    incompatible_dtypes: str
    asset_definition: str | None = None
    asset_definition_hash: str | None = None
    asset_code: str | None = None
    combined_members: tuple[dict[str, Any], ...] = ()
    source_column: dict[str, Any] | None = None
    duplicate_path_policy: str | None = None


@dataclass(frozen=True, slots=True)
class SinkSpec:
    name: str
    kind: str
    path: str
    compression: str
    overwrite: bool


@dataclass(frozen=True, slots=True)
class PipelineSpec:
    job_name: str
    yaml_path: Path
    raw: dict[str, Any]
    yaml_hash: str
    sources: dict[str, SourceSpec]
    sinks: dict[str, SinkSpec]
    execution: dict[str, Any]
    logical_plan: LogicalOperationPlan
    graph: dict[str, Any]
    graph_hash: str
    asset_code: str
    schema_version: str = PIPELINE_SCHEMA_VERSION

    @property
    def preset(self) -> str:
        """Internal bridge for mature artifact and metadata helpers."""
        return self.schema_version.removeprefix("smoking-data.")


def validate_pipeline_document(raw: dict[str, Any]) -> None:
    normalize_pipeline_document(raw)


def _validate_internal_pipeline_document(raw: dict[str, Any]) -> None:
    _reject_unknown(raw, INTERNAL_ROOT_KEYS, path="$")
    version = str(raw.get("schema_version") or "")
    if version not in SUPPORTED_PIPELINE_SCHEMA_VERSIONS:
        raise ValidationError(
            f"Unsupported schema_version: {version or '<missing>'}",
            code="yaml.unsupported_schema_version",
            context={
                "expected": sorted(SUPPORTED_PIPELINE_SCHEMA_VERSIONS),
                "actual": version or None,
            },
        )
    job = _mapping(raw.get("job"), path="job")
    _reject_unknown(job, {"name"}, path="job")
    _required_string(job.get("name"), path="job.name")
    sources = _mapping(raw.get("sources"), path="sources")
    if not sources:
        _required(path="sources")
    for name, value in sources.items():
        _required_string(name, path="sources.<name>")
        source = _mapping(value, path=f"sources.{name}")
        _reject_unknown(source, SOURCE_KEYS, path=f"sources.{name}")
        if source.get("kind") != "parquet_dataset":
            raise ValidationError(
                "Only parquet_dataset sources are supported by the public pipeline schema.",
                code="source.unsupported_kind",
                context={"path": f"sources.{name}.kind", "value": source.get("kind")},
            )
        paths = source.get("paths")
        if not isinstance(paths, list) or not paths or not all(str(item).strip() for item in paths):
            raise ValidationError(
                "Source paths must be a non-empty string list.",
                code="yaml.invalid_type",
                context={"path": f"sources.{name}.paths"},
            )
        if source.get("union_by_name", True) is not True:
            raise ValidationError(
                "Public pipeline parquet sources require union_by_name=true.",
                code="source.union_by_name_required",
                context={"path": f"sources.{name}.union_by_name"},
            )
        missing_policy = str(source.get("missing_columns") or "insert_null")
        if missing_policy != "insert_null":
            raise ValidationError(
                "Public pipelines currently require missing_columns=insert_null.",
                code="source.unsupported_missing_column_policy",
                context={"path": f"sources.{name}.missing_columns", "value": missing_policy},
            )
        dtype_policy = str(source.get("incompatible_dtypes") or "error")
        if dtype_policy != "error":
            raise ValidationError(
                "Public pipelines currently require incompatible_dtypes=error.",
                code="source.unsupported_dtype_policy",
                context={"path": f"sources.{name}.incompatible_dtypes", "value": dtype_policy},
            )
    sinks = _mapping(raw.get("sinks"), path="sinks")
    if not sinks:
        _required(path="sinks")
    for name, value in sinks.items():
        sink = _mapping(value, path=f"sinks.{name}")
        _reject_unknown(sink, SINK_KEYS, path=f"sinks.{name}")
        if sink.get("kind") != "parquet_dataset":
            raise ValidationError(
                "Only parquet_dataset sinks are supported by the public pipeline schema.",
                code="sink.unsupported_kind",
                context={"path": f"sinks.{name}.kind", "value": sink.get("kind")},
            )
        _required_string(sink.get("path"), path=f"sinks.{name}.path")
        compression = str(sink.get("compression") or "zstd").lower()
        if compression not in {"snappy", "zstd", "uncompressed", "none"}:
            raise ValidationError(
                "Unsupported parquet sink compression.",
                code="sink.unsupported_compression",
                context={"path": f"sinks.{name}.compression", "value": compression},
            )
        if "overwrite" in sink and not isinstance(sink["overwrite"], bool):
            raise ValidationError(
                "sink overwrite must be boolean.",
                code="yaml.invalid_type",
                context={"path": f"sinks.{name}.overwrite"},
            )
    operations = raw.get("operations")
    if not isinstance(operations, list) or not operations:
        raise ValidationError(
            "operations must be a non-empty ordered list.",
            code="yaml.invalid_type",
            context={"path": "operations"},
        )
    execution = _mapping(raw.get("execution") or {}, path="execution")
    _reject_unknown(execution, EXECUTION_KEYS, path="execution")
    for key in (
        "workers",
        "max_tasks_per_child",
        "memory_budget_mb",
        "target_rows_per_part",
        "target_key_groups_per_part",
        "max_source_files_per_task",
        "max_source_row_groups_per_task",
        "sidecar_workers",
        "sidecar_max_source_files",
        "sidecar_max_projected_bytes_mb",
    ):
        value = execution.get(key)
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool) or value < 1
        ):
            raise ValidationError(
                "Execution numeric values must be positive integers or null.",
                code="yaml.invalid_type",
                context={"path": f"execution.{key}", "value": value},
            )
    recycle_mode = str(execution.get("sidecar_worker_recycle_mode") or "adaptive")
    if recycle_mode != "adaptive":
        raise ValidationError(
            "execution.sidecar_worker_recycle_mode must be adaptive.",
            code="build_sidecar.unsupported_worker_recycle_mode",
            context={"value": recycle_mode},
        )
    memory = execution.get("memory")
    if memory is not None:
        memory = _mapping(memory, path="execution.memory")
        _reject_unknown(
            memory, {"hard_limit_mb", "safety_ratio", "phases"}, path="execution.memory"
        )
        hard_limit = memory.get("hard_limit_mb")
        if not isinstance(hard_limit, int) or isinstance(hard_limit, bool) or hard_limit < 1:
            raise ValidationError(
                "execution.memory.hard_limit_mb must be an integer >= 1.",
                code="yaml.invalid_type",
                context={"path": "execution.memory.hard_limit_mb", "value": hard_limit},
            )
        safety_ratio = memory.get("safety_ratio")
        if (
            not isinstance(safety_ratio, (int, float))
            or isinstance(safety_ratio, bool)
            or not 0 < float(safety_ratio) <= 1
        ):
            raise ValidationError(
                "execution.memory.safety_ratio must be > 0 and <= 1.",
                code="yaml.invalid_type",
                context={"path": "execution.memory.safety_ratio", "value": safety_ratio},
            )
        phases = _mapping(memory.get("phases"), path="execution.memory.phases")
        _reject_unknown(
            phases,
            {"build_sidecar", "materialize", "save_dataset"},
            path="execution.memory.phases",
        )
        for phase_name, phase_value in phases.items():
            phase = _mapping(phase_value, path=f"execution.memory.phases.{phase_name}")
            _reject_unknown(
                phase,
                {"target_peak_memory_mb", "workers"},
                path=f"execution.memory.phases.{phase_name}",
            )
            target = phase.get("target_peak_memory_mb")
            if not isinstance(target, int) or isinstance(target, bool) or target < 1:
                raise ValidationError(
                    "Phase target_peak_memory_mb must be an integer >= 1.",
                    code="yaml.invalid_type",
                    context={
                        "path": f"execution.memory.phases.{phase_name}.target_peak_memory_mb",
                        "value": target,
                    },
                )
            worker_range = _mapping(
                phase.get("workers"), path=f"execution.memory.phases.{phase_name}.workers"
            )
            _reject_unknown(
                worker_range,
                {"min", "max"},
                path=f"execution.memory.phases.{phase_name}.workers",
            )
            minimum, maximum = worker_range.get("min"), worker_range.get("max")
            if (
                any(
                    not isinstance(value, int) or isinstance(value, bool) or value < 1
                    for value in (minimum, maximum)
                )
                or minimum > maximum
            ):
                raise ValidationError(
                    "Phase worker min/max must be positive integers with min <= max.",
                    code="yaml.invalid_type",
                    context={
                        "path": f"execution.memory.phases.{phase_name}.workers",
                        "value": worker_range,
                    },
                )
    if "reset_before_run" in execution and not isinstance(execution["reset_before_run"], bool):
        raise ValidationError(
            "execution.reset_before_run must be boolean.",
            code="yaml.invalid_type",
            context={"path": "execution.reset_before_run"},
        )
    test_run = execution.get("test_run")
    if test_run is not None:
        test_run = _mapping(test_run, path="execution.test_run")
        _reject_unknown(test_run, {"final_task_limit"}, path="execution.test_run")
        limit = test_run.get("final_task_limit")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValidationError(
                "execution.test_run.final_task_limit must be an integer >= 1.",
                code="yaml.invalid_type",
                context={"path": "execution.test_run.final_task_limit", "value": limit},
            )
        artifact = _mapping(
            _mapping(raw.get("output"), path="output").get("artifact"),
            path="output.artifact",
        )
        asset_code = {
            "curated_dataset": "0201",
            "joined_dataset": "0301",
            "analysis_snapshot": "0401",
        }.get(str(artifact.get("type") or ""), "")
        if asset_code not in {"0201", "0301", "0401"}:
            raise ValidationError(
                "execution.test_run is supported only for 0201, 0301, and 0401 pipelines.",
                code="execution.unsupported_test_run_asset",
                context={"asset_code": asset_code},
            )


def parse_sources(raw: dict[str, Any]) -> dict[str, SourceSpec]:
    return {
        str(name): SourceSpec(
            name=str(name),
            kind=str(config["kind"]),
            paths=tuple(str(path) for path in config["paths"]),
            union_by_name=True,
            missing_columns=str(config.get("missing_columns") or "insert_null"),
            incompatible_dtypes=str(config.get("incompatible_dtypes") or "error"),
            asset_definition=(
                str(config["asset_definition"]) if config.get("asset_definition") else None
            ),
            asset_definition_hash=(
                str(config["asset_definition_hash"])
                if config.get("asset_definition_hash")
                else None
            ),
            asset_code=(str(config["asset_code"]) if config.get("asset_code") else None),
            combined_members=tuple(
                dict(item) for item in config.get("combined_members") or []
            ),
            source_column=(
                dict(config["source_column"]) if config.get("source_column") else None
            ),
            duplicate_path_policy=(
                str(config["duplicate_path_policy"])
                if config.get("duplicate_path_policy")
                else None
            ),
        )
        for name, config in raw["sources"].items()
    }


def parse_sinks(raw: dict[str, Any]) -> dict[str, SinkSpec]:
    return {
        str(name): SinkSpec(
            name=str(name),
            kind=str(config["kind"]),
            path=str(config["path"]),
            compression=str(config.get("compression") or "zstd"),
            overwrite=bool(config.get("overwrite", True)),
        )
        for name, config in raw["sinks"].items()
    }


def compile_operations(
    raw: dict[str, Any],
    *,
    expression_irs: dict[str, dict[str, Any]] | None = None,
    source_columns: dict[str, tuple[str, ...]] | None = None,
    source_dtypes: dict[str, dict[str, str]] | None = None,
) -> LogicalOperationPlan:
    normalized = raw
    if "sources" not in raw or "sinks" not in raw:
        normalized, _ = normalize_pipeline_document(
            raw,
            _allow_legacy_curated_internal=True,
        )
    _validate_internal_pipeline_document(normalized)
    raw = normalized
    schema_version = str(raw["schema_version"])
    sources = set(raw["sources"])
    sinks = set(raw["sinks"])
    operations = []
    ids: set[str] = set()
    right_source_names = {
        str(item.get("right_source"))
        for item in raw["operations"]
        if isinstance(item, dict) and item.get("op") == "join" and item.get("right_source")
    }
    primary_sources = [name for name in raw["sources"] if name not in right_source_names]
    known_columns: set[str] = set()
    validate_columns = source_columns is not None
    if source_columns is not None and len(primary_sources) == 1:
        known_columns.update(source_columns.get(primary_sources[0], ()))
    else:
        for columns in (source_columns or {}).values():
            known_columns.update(columns)
    known_dtypes = dict(
        (source_dtypes or {}).get(primary_sources[0], {}) if len(primary_sources) == 1 else {}
    )

    for index, value in enumerate(raw["operations"]):
        path = f"operations[{index}]"
        config = _mapping(value, path=path)
        operation_id = _required_string(config.get("id"), path=f"{path}.id")
        if operation_id in ids:
            raise ValidationError(
                f"Duplicate operation id: {operation_id}",
                code="logical_plan.duplicate_operation_id",
                context={"path": f"{path}.id", "operation_id": operation_id},
            )
        ids.add(operation_id)
        op_name = _required_string(config.get("op"), path=f"{path}.op")
        try:
            kind = OperationKind(op_name)
        except ValueError as error:
            raise ValidationError(
                f"Unsupported operation: {op_name}",
                code="operation.unsupported",
                context={"path": f"{path}.op", "operation": op_name},
            ) from error
        body = {key: item for key, item in config.items() if key not in {"id", "op"}}
        operation = _compile_operation(
            operation_id,
            kind,
            body,
            path=path,
            sources=sources,
            sinks=sinks,
            expression_ir=(expression_irs or {}).get(operation_id),
            known_columns=known_columns,
            source_columns=source_columns,
            known_dtypes=known_dtypes,
            source_dtypes=source_dtypes,
            schema_version=schema_version,
        )
        if validate_columns:
            missing = sorted(set(operation.input_columns).difference(known_columns))
            if missing:
                raise ValidationError(
                    "Operation references columns not produced by prior operations.",
                    code="operation.missing_input_columns",
                    context={"path": path, "operation_id": operation_id, "columns": missing},
                )
            duplicate_outputs = sorted(
                column.name
                for column in operation.output_columns
                if column.name in known_columns
                and kind
                not in {
                    OperationKind.TYPE_CAST,
                    OperationKind.ADD_CALC,
                    OperationKind.REFERENCE_REPLACE,
                    OperationKind.RENAME_COLUMNS,
                    OperationKind.INCLUDE_COLUMNS,
                    OperationKind.UNPIVOT,
                }
            )
            if duplicate_outputs:
                raise ValidationError(
                    "Operation output shadows an existing column.",
                    code="operation.alias_shadowing",
                    context={"path": path, "columns": duplicate_outputs},
                )
        operations.append(operation)
        _advance_known_columns(known_columns, operation)
        _advance_known_dtypes(known_dtypes, operation)

    if operations[-1].kind is not OperationKind.WRITE_DATASET:
        raise ValidationError(
            "The final operation must be write_dataset.",
            code="pipeline.write_required",
            context={"path": f"operations[{len(operations) - 1}]"},
        )
    _validate_explicit_physical_boundaries(operations, sources=sources)
    return _plan(schema_version, operations)


def _compile_operation(
    operation_id: str,
    kind: OperationKind,
    config: dict[str, Any],
    *,
    path: str,
    sources: set[str],
    sinks: set[str],
    expression_ir: dict[str, Any] | None,
    known_columns: set[str],
    source_columns: dict[str, tuple[str, ...]] | None,
    known_dtypes: dict[str, str],
    source_dtypes: dict[str, dict[str, str]] | None,
    schema_version: str,
):
    if kind is OperationKind.FILTER:
        from smoking_data.ops.projection import resolve_filter_expression
        from spotfire_expr_normalizer import normalize_expression

        _keys(config, {"sql", "spotfire_expression"}, path=path)
        try:
            dialect, expression = resolve_filter_expression(config)
        except ValueError as error:
            raise ValidationError(
                str(error),
                code="expression.invalid",
                context={"path": path, "operation_id": operation_id},
            ) from error
        planner_expression = (
            normalize_expression(expression) if dialect == "spotfire_expression" else expression
        )
        return _operation(
            operation_id,
            kind,
            config,
            input_columns=_sql_identifier_columns(planner_expression),
        )
    if kind is OperationKind.TYPE_CAST:
        _keys(config, {"columns"}, path=path)
        columns = _mapping_list(config.get("columns"), path=f"{path}.columns")
        for index, item in enumerate(columns):
            _keys(item, {"name", "type", "failure_policy"}, path=f"{path}.columns[{index}]")
            if str(item.get("failure_policy") or "error") != "error":
                raise ValidationError(
                    "Rust type_cast currently requires failure_policy=error.",
                    code="cast.unsupported_failure_policy",
                    context={"path": f"{path}.columns[{index}].failure_policy"},
                )
        outputs = tuple(
            ColumnContract(
                _required_string(item.get("name"), path=f"{path}.columns[{index}].name"),
                dtype=_required_string(item.get("type"), path=f"{path}.columns[{index}].type"),
                nullable=True,
            )
            for index, item in enumerate(columns)
        )
        _reject_duplicate_output_names(outputs, path=path)
        return _operation(
            operation_id,
            kind,
            config,
            input_columns=tuple(item.name for item in outputs),
            output_columns=outputs,
        )
    if kind is OperationKind.ADD_CALC:
        _keys(config, {"expressions"}, path=path)
        expressions = _mapping_list(config.get("expressions"), path=f"{path}.expressions")
        for index, item in enumerate(expressions):
            _keys(
                item,
                {"name", "sql", "spotfire_expression"},
                path=f"{path}.expressions[{index}]",
            )
            expression_sources = [
                str(item.get(key) or "").strip()
                for key in ("sql", "spotfire_expression")
                if str(item.get(key) or "").strip()
            ]
            if len(expression_sources) != 1:
                raise ValidationError(
                    "add_calc expression requires exactly one of sql or spotfire_expression.",
                    code="expression.source_ambiguous",
                    context={"path": f"{path}.expressions[{index}]"},
                )
        if expression_ir is None:
            raise ValidationError(
                "add_calc expressions must compile to typed expression IR.",
                code="expression.ir_required",
                context={"path": path, "operation_id": operation_id},
            )
        outputs = tuple(
            ColumnContract(
                _required_string(item.get("name"), path=f"{path}.expressions[{index}].name"),
                nullable=True,
            )
            for index, item in enumerate(expressions)
        )
        _reject_duplicate_output_names(outputs, path=path)
        lineage = _ir_expression_lineage(expression_ir)
        inputs = tuple(sorted(_ir_input_columns(expression_ir)))
        return _operation(
            operation_id,
            kind,
            {**config, "expression_ir": expression_ir},
            input_columns=inputs,
            output_columns=outputs,
            alias_lineage={item.name: lineage.get(item.name, inputs) for item in outputs},
        )
    if kind is OperationKind.BUILD_SIDECAR:
        _keys(config, {"source", "columns"}, path=path)
        source = str(config.get("source") or "").strip()
        if source and source not in sources:
            raise ValidationError(
                f"Unknown sidecar source: {source}",
                code="operation.unknown_source",
                context={"path": f"{path}.source", "source": source},
            )
        columns_value = config.get("columns", "auto")
        if columns_value == "auto":
            columns: tuple[str, ...] = ()
        else:
            columns = _string_list(columns_value, path=f"{path}.columns")
        return _operation(
            operation_id,
            kind,
            {"source": source or None, "columns": "auto" if not columns else list(columns)},
            input_columns=columns,
        )
    if kind is OperationKind.ACTIVE_ROW_SELECTION:
        allowed_keys = {"method", "group_keys", "sort", "sidecar"}
        _keys(config, allowed_keys, path=path)
        method = str(config.get("method") or "sort_first").strip().lower()
        if method != "sort_first":
            raise ValidationError(
                "active_row_selection currently requires method=sort_first.",
                code="active_row_selection.unsupported_method",
                context={"path": f"{path}.method", "value": method},
            )
        group_key_items = _mapping_list(config.get("group_keys"), path=f"{path}.group_keys")
        canonical_group_keys: list[dict[str, str]] = []
        names: set[str] = set()
        direct_inputs: list[str] = []
        for index, item in enumerate(group_key_items):
            item_path = f"{path}.group_keys[{index}]"
            _keys(
                item,
                {"name", "column", "sql", "spotfire_expression"},
                path=item_path,
            )
            name = _required_string(item.get("name"), path=f"{item_path}.name")
            if name in names:
                raise ValidationError(
                    "active_row_selection group key names must be unique.",
                    code="active_row_selection.duplicate_group_key",
                    context={"path": f"{item_path}.name", "name": name},
                )
            names.add(name)
            sources = {
                key: str(item.get(key) or "").strip()
                for key in ("column", "sql", "spotfire_expression")
                if str(item.get(key) or "").strip()
            }
            # A group key that only declares its logical name refers to the
            # input column with the same name.  Keep the expanded form in the
            # canonical plan so downstream lineage and execution do not need
            # to distinguish the shorthand.
            if not sources:
                sources = {"column": name}
            if len(sources) != 1:
                raise ValidationError(
                    "active_row_selection group key requires exactly one of column, sql or spotfire_expression.",
                    code="expression.source_ambiguous",
                    context={"path": item_path},
                )
            source_kind, source = next(iter(sources.items()))
            canonical_group_keys.append({"name": name, source_kind: source})
            if source_kind == "column":
                direct_inputs.append(source)
        computed_names = {item["name"] for item in canonical_group_keys if "column" not in item}
        if computed_names and expression_ir is None:
            raise ValidationError(
                "active_row_selection expressions must compile to typed expression IR.",
                code="expression.ir_required",
                context={"path": path, "operation_id": operation_id},
            )
        if expression_ir is not None and _selector_ir_is_stateful_or_nondeterministic(
            expression_ir
        ):
            raise ValidationError(
                "active_row_selection group key expressions must be row-local.",
                code="active_row_selection.non_row_local_expression",
                context={"path": f"{path}.group_keys", "operation_id": operation_id},
            )
        sort_items = _mapping_list(config.get("sort"), path=f"{path}.sort")
        for index, item in enumerate(sort_items):
            _keys(item, {"column", "direction", "nulls"}, path=f"{path}.sort[{index}]")
            if str(item.get("nulls") or "last").lower() not in {"first", "last"}:
                raise ValidationError(
                    "active_row_selection sort nulls must be first or last.",
                    code="yaml.invalid_sort",
                    context={"path": f"{path}.sort[{index}].nulls"},
                )
        ordering = _ordering(sort_items, path=f"{path}.sort")
        expression_inputs = _ir_input_columns(expression_ir).difference(computed_names)
        inputs = tuple(
            dict.fromkeys(
                [*direct_inputs, *sorted(expression_inputs), *(name for name, _ in ordering)]
            )
        )
        lineage = _ir_expression_lineage(expression_ir)
        for item in canonical_group_keys:
            if "column" in item:
                lineage[item["name"]] = (item["column"],)
        return _operation(
            operation_id,
            kind,
            {
                "method": method,
                "sidecar": str(config.get("sidecar") or "").strip() or None,
                "group_keys": canonical_group_keys,
                "sort": list(config.get("sort") or []),
                "expression_ir": expression_ir,
            },
            input_columns=inputs,
            alias_lineage=lineage,
            group_keys=tuple(item["name"] for item in canonical_group_keys),
            ordering=ordering,
        )
    if kind is OperationKind.MATERIALIZE:
        _keys(
            config,
            {
                "coordinates_from",
                "source",
                "partition_by",
                "part_boundary",
                "workers",
                "max_tasks_per_child",
            },
            path=path,
        )
        coordinates_from = str(config.get("coordinates_from") or "").strip()
        source = str(config.get("source") or "").strip()
        if bool(coordinates_from) == bool(source):
            raise ValidationError(
                "materialize requires exactly one of coordinates_from or source.",
                code="materialize.source_ambiguous",
                context={"path": path},
            )
        if source and source not in sources:
            raise ValidationError(
                f"Unknown materialize source: {source}",
                code="operation.unknown_source",
                context={"path": f"{path}.source", "source": source},
            )
        partition_by = _string_list(config.get("partition_by"), path=f"{path}.partition_by")
        boundary = _mapping(config.get("part_boundary"), path=f"{path}.part_boundary")
        _keys(
            boundary,
            {"target_rows", "target_key_groups", "preserve_groups"},
            path=f"{path}.part_boundary",
        )
        target_rows = _positive_integer(
            boundary.get("target_rows"), path=f"{path}.part_boundary.target_rows"
        )
        target_key_groups = boundary.get("target_key_groups")
        if target_key_groups is not None:
            target_key_groups = _positive_integer(
                target_key_groups,
                path=f"{path}.part_boundary.target_key_groups",
            )
        preserve_groups = _string_list(
            boundary.get("preserve_groups") or partition_by,
            path=f"{path}.part_boundary.preserve_groups",
        )
        workers = _positive_integer(config.get("workers", 1), path=f"{path}.workers")
        max_tasks = _positive_integer(
            config.get("max_tasks_per_child", 1),
            path=f"{path}.max_tasks_per_child",
        )
        canonical = {
            "coordinates_from": coordinates_from or None,
            "source": source or None,
            "partition_by": list(partition_by),
            "part_boundary": {
                "target_rows": target_rows,
                "target_key_groups": target_key_groups,
                "preserve_groups": list(preserve_groups),
            },
            "workers": workers,
            "max_tasks_per_child": max_tasks,
        }
        return _operation(
            operation_id,
            kind,
            canonical,
            input_columns=tuple(
                partition_by
                if coordinates_from
                else dict.fromkeys([*partition_by, *preserve_groups])
            ),
            group_keys=preserve_groups,
            partition_keys=partition_by,
        )
    if kind in {OperationKind.INCLUDE_COLUMNS, OperationKind.EXCLUDE_COLUMNS}:
        _keys(config, {"columns"}, path=path)
        columns = _string_list(config.get("columns"), path=f"{path}.columns")
        resolved_columns = (
            list(columns)
            if kind is OperationKind.INCLUDE_COLUMNS
            else sorted(known_columns.difference(columns))
        )
        return _operation(
            operation_id,
            kind,
            {**config, "resolved_columns": resolved_columns},
            input_columns=columns,
            output_columns=tuple(ColumnContract(name) for name in columns)
            if kind is OperationKind.INCLUDE_COLUMNS
            else (),
        )
    if kind is OperationKind.RENAME_COLUMNS:
        _keys(config, {"mapping", "regex", "case_sensitive", "unmatched"}, path=path)
        mapping = _resolve_rename_mapping(config, known_columns=known_columns, path=path)
        return _operation(
            operation_id,
            kind,
            {**config, "resolved_mapping": mapping},
            input_columns=tuple(mapping),
            output_columns=tuple(ColumnContract(name) for name in mapping.values()),
            alias_lineage={target: (source,) for source, target in mapping.items()},
        )
    if kind is OperationKind.DATA_ASSERTION:
        _keys(config, {"rules", "sample_limit"}, path=path)
        sample_limit = config.get("sample_limit", 20)
        if not isinstance(sample_limit, int) or sample_limit < 1:
            raise ValidationError(
                "data_assertion sample_limit must be a positive integer.",
                code="assertion.invalid_sample_limit",
                context={"path": f"{path}.sample_limit"},
            )
        rules = _mapping_list(config.get("rules"), path=f"{path}.rules")
        inputs = _assertion_columns(rules, path=path)
        _validate_static_assertion_dtypes(rules, known_dtypes=known_dtypes, path=path)
        return _operation(operation_id, kind, config, input_columns=inputs)
    if kind is OperationKind.UNPIVOT:
        _keys(
            config,
            {
                "id_columns",
                "value_columns",
                "name_column",
                "value_column",
                "coercion",
                "preserve_nulls",
            },
            path=path,
        )
        id_columns = _string_list(config.get("id_columns"), path=f"{path}.id_columns")
        value_columns = _string_list(config.get("value_columns"), path=f"{path}.value_columns")
        name_column = _required_string(config.get("name_column"), path=f"{path}.name_column")
        value_column = _required_string(config.get("value_column"), path=f"{path}.value_column")
        if (
            name_column == value_column
            or name_column in {*id_columns, *value_columns}
            or value_column in {*id_columns, *value_columns}
        ):
            raise ValidationError(
                "unpivot output columns collide with input columns.",
                code="unpivot.column_collision",
                context={"path": path},
            )
        coercion = str(config.get("coercion") or "strict")
        if coercion not in {"strict", "string"}:
            raise ValidationError(
                "unpivot coercion must be strict or string.",
                code="unpivot.invalid_coercion",
                context={"path": f"{path}.coercion", "value": coercion},
            )
        if "preserve_nulls" in config and not isinstance(config["preserve_nulls"], bool):
            raise ValidationError(
                "unpivot preserve_nulls must be boolean.",
                code="yaml.invalid_type",
                context={"path": f"{path}.preserve_nulls"},
            )
        return _operation(
            operation_id,
            kind,
            config,
            input_columns=tuple(dict.fromkeys([*id_columns, *value_columns])),
            output_columns=tuple(
                [
                    *(ColumnContract(name) for name in id_columns),
                    ColumnContract(name_column, "TEXT"),
                    ColumnContract(value_column),
                ]
            ),
        )
    if kind is OperationKind.PIVOT:
        _keys(
            config,
            {
                "row_keys",
                "column_keys",
                "value_keys",
                "value_keys_without_column",
                "null_column_key_policy",
                "column_key_separator",
                "first_duplicate_policy",
            },
            path=path,
        )
        for section in ("value_keys", "value_keys_without_column"):
            for index, item in enumerate(config.get(section) or []):
                value = _mapping(item, path=f"{path}.{section}[{index}]")
                _keys(
                    value,
                    {"name", "source_column", "aggregation", "output_dtype", "column_name_rule"},
                    path=f"{path}.{section}[{index}]",
                )
        return replace(
            _compile_pivot_operation({"enabled": True, **config}),
            operation_id=operation_id,
        )
    if kind is OperationKind.JOIN:
        _keys(
            config,
            {
                "right_source",
                "how",
                "left_on",
                "right_on",
                "right_partition_column",
                "suffix",
                "columns",
            },
            path=path,
        )
        right_source = _required_string(config.get("right_source"), path=f"{path}.right_source")
        if right_source not in sources:
            raise ValidationError(
                f"Unknown right source: {right_source}",
                code="operation.unknown_source",
                context={"path": f"{path}.right_source", "source": right_source},
            )
        how = str(config.get("how") or "left").lower()
        if how not in {"inner", "left", "right", "full", "cross"}:
            raise ValidationError(
                "Unsupported join type.", code="join.invalid_type", context={"path": f"{path}.how"}
            )
        if how == "cross":
            if config.get("left_on") not in (None, []) or config.get("right_on") not in (None, []):
                raise ValidationError(
                    "cross join must not define join keys.",
                    code="join.cross_keys_forbidden",
                    context={"path": path},
                )
            left_on: tuple[str, ...] = ()
            right_on: tuple[str, ...] = ()
        else:
            left_on = _string_list(config.get("left_on"), path=f"{path}.left_on")
            right_on = _string_list(config.get("right_on"), path=f"{path}.right_on")
            if len(left_on) != len(right_on):
                raise ValidationError(
                    "join key lengths differ.",
                    code="join.key_length_mismatch",
                    context={"path": path},
                )
        if how in {"right", "full"} and not str(config.get("right_partition_column") or "").strip():
            raise ValidationError(
                "right/full join requires right_partition_column.",
                code="join.partition_mapping_required",
                context={"path": f"{path}.right_partition_column", "how": how},
            )
        missing_right = sorted(
            set(right_on).difference((source_columns or {}).get(right_source, ()))
        )
        right_partition = str(config.get("right_partition_column") or "").strip()
        if (
            source_columns is not None
            and right_partition
            and right_partition not in (source_columns or {}).get(right_source, ())
        ):
            missing_right.append(right_partition)
        if source_columns is not None and missing_right:
            raise ValidationError(
                "Join references columns missing from the right source.",
                code="operation.missing_input_columns",
                context={"path": path, "source": right_source, "columns": missing_right},
            )
        right_dtypes = (source_dtypes or {}).get(right_source, {})
        mismatches = [
            {
                "left": left,
                "left_dtype": known_dtypes.get(left),
                "right": right,
                "right_dtype": right_dtypes.get(right),
            }
            for left, right in zip(left_on, right_on, strict=True)
            if known_dtypes.get(left)
            and right_dtypes.get(right)
            and known_dtypes[left] != right_dtypes[right]
        ]
        if mismatches:
            raise ValidationError(
                "Join key dtypes differ.",
                code="join.key_dtype_mismatch",
                context={"path": path, "mismatches": mismatches},
            )
        right_outputs = _resolve_join_output_columns(
            config.get("columns") or {},
            source_columns=(source_columns or {}).get(right_source, ()),
            right_on=right_on,
            known_columns=known_columns,
            suffix=str(config.get("suffix") or f"_{right_source}"),
            path=path,
        )
        return _operation(
            operation_id,
            kind,
            {**config, "how": how},
            input_columns=left_on,
            output_columns=tuple(ColumnContract(name) for name in right_outputs),
            group_keys=left_on,
        )
    if kind is OperationKind.REFERENCE_REPLACE:
        _keys(
            config,
            {
                "reference_parquet",
                "source_column",
                "reference_input_column",
                "reference_output_column",
                "output_column",
                "missing_policy",
                "duplicate_policy",
            },
            path=path,
        )
        for key in (
            "reference_parquet",
            "source_column",
            "reference_input_column",
            "reference_output_column",
        ):
            _required_string(config.get(key), path=f"{path}.{key}")
        if str(config.get("missing_policy") or "keep_source") != "keep_source":
            raise ValidationError(
                "reference_replace currently requires missing_policy=keep_source.",
                code="reference_replace.unsupported_missing_policy",
                context={"path": f"{path}.missing_policy"},
            )
        if str(config.get("duplicate_policy") or "error") != "error":
            raise ValidationError(
                "reference_replace currently requires duplicate_policy=error.",
                code="reference_replace.unsupported_duplicate_policy",
                context={"path": f"{path}.duplicate_policy"},
            )
        source_column = _required_string(config.get("source_column"), path=f"{path}.source_column")
        output_column = str(config.get("output_column") or source_column)
        return _operation(
            operation_id,
            kind,
            config,
            input_columns=(source_column,),
            output_columns=(ColumnContract(output_column),),
            alias_lineage={output_column: (source_column,)},
        )
    if kind is OperationKind.LIST_RESTORE:
        _keys(
            config,
            {"lookup_path", "schema", "config", "batch_size", "drop_cache_hint", "print_timing"},
            path=path,
        )
        _required_string(config.get("lookup_path"), path=f"{path}.lookup_path")
        raw_schema = config.get("schema")
        schema_auto = isinstance(raw_schema, str) and raw_schema.strip().lower() == "auto"
        if schema_auto:
            schema: dict[str, Any] = {}
        else:
            schema = _mapping(raw_schema, path=f"{path}.schema")
        restore_config = _mapping(config.get("config"), path=f"{path}.config")
        _keys(
            restore_config,
            {
                "key_column",
                "order_column",
                "value_columns",
                "source_coord_columns",
                "lookup_coord_columns",
            },
            path=f"{path}.config",
        )
        _required_string(restore_config.get("key_column"), path=f"{path}.config.key_column")
        _required_string(restore_config.get("order_column"), path=f"{path}.config.order_column")
        value_columns = _string_list(
            restore_config.get("value_columns"), path=f"{path}.config.value_columns"
        )
        source_coord = _string_list(
            restore_config.get("source_coord_columns"),
            path=f"{path}.config.source_coord_columns",
        )
        lookup_coord = _string_list(
            restore_config.get("lookup_coord_columns"),
            path=f"{path}.config.lookup_coord_columns",
        )
        if len(source_coord) != len(lookup_coord):
            raise ValidationError(
                "list_restore source and lookup coordinate lengths differ.",
                code="list_restore.coordinate_length_mismatch",
                context={"path": path},
            )
        missing_schema = sorted(set([*value_columns, *source_coord]).difference(schema))
        if missing_schema and not schema_auto:
            raise ValidationError(
                "list_restore schema is missing restored columns.",
                code="list_restore.schema_missing_columns",
                context={"path": f"{path}.schema", "columns": missing_schema},
            )
        key_column = str(restore_config["key_column"])
        return _operation(
            operation_id,
            kind,
            config,
            input_columns=tuple(dict.fromkeys([key_column, *value_columns, *source_coord])),
            output_columns=tuple(
                ColumnContract(name, dtype=str(schema[name]), nullable=True)
                for name in [*value_columns, *source_coord]
            ),
            group_keys=(key_column,),
        )
    if kind is OperationKind.WRITE_DATASET:
        _keys(config, {"sink", "partition_by"}, path=path)
        sink = _required_string(config.get("sink"), path=f"{path}.sink")
        if sink not in sinks:
            raise ValidationError(
                f"Unknown sink: {sink}",
                code="operation.unknown_sink",
                context={"path": f"{path}.sink", "sink": sink},
            )
        partition_by = _string_list(config.get("partition_by"), path=f"{path}.partition_by")
        return _operation(
            operation_id,
            kind,
            config,
            input_columns=partition_by,
            partition_keys=partition_by,
        )
    raise ValidationError(
        f"Operation is not public in this pipeline schema: {kind.value}",
        code="operation.unsupported",
        context={"path": path, "operation": kind.value},
    )


def _advance_known_columns(columns: set[str], operation: Any) -> None:
    if operation.kind is OperationKind.INCLUDE_COLUMNS:
        columns.intersection_update(operation.config["columns"])
    elif operation.kind is OperationKind.EXCLUDE_COLUMNS:
        columns.difference_update(operation.config["columns"])
    elif operation.kind is OperationKind.RENAME_COLUMNS:
        for source, target in operation.config["resolved_mapping"].items():
            columns.discard(source)
            columns.add(target)
    elif operation.kind is OperationKind.UNPIVOT:
        columns.clear()
        columns.update(column.name for column in operation.output_columns)
    else:
        columns.update(column.name for column in operation.output_columns)


def _selector_ir_is_stateful_or_nondeterministic(document: dict[str, Any]) -> bool:
    forbidden_functions = {
        "current_date",
        "current_timestamp",
        "now",
        "rand",
        "random",
        "uuid",
    }

    def visit(value: Any) -> bool:
        if isinstance(value, dict):
            if value.get("kind") in {"window", "aggregate"}:
                return True
            if (
                value.get("kind") == "call"
                and str(value.get("function") or "").lower() in forbidden_functions
            ):
                return True
            return any(visit(child) for child in value.values())
        if isinstance(value, list):
            return any(visit(child) for child in value)
        return False

    return visit(document)


def _advance_known_dtypes(dtypes: dict[str, str], operation: Any) -> None:
    if operation.kind is OperationKind.INCLUDE_COLUMNS:
        selected = set(operation.config["columns"])
        for name in list(dtypes):
            if name not in selected:
                dtypes.pop(name)
    elif operation.kind is OperationKind.EXCLUDE_COLUMNS:
        for name in operation.config["columns"]:
            dtypes.pop(name, None)
    elif operation.kind is OperationKind.RENAME_COLUMNS:
        for source, target in operation.config["resolved_mapping"].items():
            if source in dtypes:
                dtypes[target] = dtypes.pop(source)
    elif operation.kind is OperationKind.UNPIVOT:
        retained = {
            name: dtype for name, dtype in dtypes.items() if name in operation.config["id_columns"]
        }
        retained[operation.config["name_column"]] = "string"
        retained[operation.config["value_column"]] = "unknown"
        dtypes.clear()
        dtypes.update(retained)
    else:
        for column in operation.output_columns:
            if column.dtype:
                dtypes[column.name] = _canonical_dtype(column.dtype)
            else:
                dtypes.setdefault(column.name, "unknown")


def _resolve_rename_mapping(
    config: dict[str, Any], *, known_columns: set[str], path: str
) -> dict[str, str]:
    mapping = config.get("mapping") or {}
    if not isinstance(mapping, dict):
        raise ValidationError(
            "rename mapping must be a mapping.",
            code="yaml.invalid_type",
            context={"path": f"{path}.mapping"},
        )
    resolved = {str(source): str(target) for source, target in mapping.items()}
    regex_rules = config.get("regex") or []
    if not isinstance(regex_rules, list):
        raise ValidationError(
            "rename regex must be a list.",
            code="yaml.invalid_type",
            context={"path": f"{path}.regex"},
        )
    case_sensitive = config.get("case_sensitive", True)
    if not isinstance(case_sensitive, bool):
        raise ValidationError(
            "rename case_sensitive must be boolean.",
            code="yaml.invalid_type",
            context={"path": f"{path}.case_sensitive"},
        )
    unmatched = str(config.get("unmatched") or "ignore")
    if unmatched not in {"ignore", "error"}:
        raise ValidationError(
            "rename unmatched must be ignore or error.",
            code="rename.invalid_unmatched_policy",
            context={"path": f"{path}.unmatched", "value": unmatched},
        )
    flags = 0 if case_sensitive else re.IGNORECASE
    for index, rule in enumerate(regex_rules):
        item = _mapping(rule, path=f"{path}.regex[{index}]")
        _keys(item, {"pattern", "replacement"}, path=f"{path}.regex[{index}]")
        pattern = re.compile(
            _required_string(item.get("pattern"), path=f"{path}.regex[{index}].pattern"), flags
        )
        replacement = str(item.get("replacement") or "")
        matched = 0
        for column in sorted(known_columns):
            if pattern.search(column):
                resolved.setdefault(column, pattern.sub(replacement, column))
                matched += 1
        if matched == 0 and unmatched == "error":
            raise ValidationError(
                "rename regex matched no input columns.",
                code="rename.unmatched_pattern",
                context={"path": f"{path}.regex[{index}]", "pattern": pattern.pattern},
            )
    targets = list(resolved.values())
    if any(not target for target in targets) or len(targets) != len(set(targets)):
        raise ValidationError(
            "rename targets must be non-empty and unique.",
            code="rename.target_collision",
            context={"path": path},
        )
    unaffected = known_columns.difference(resolved)
    collision = sorted(unaffected.intersection(targets))
    if collision:
        raise ValidationError(
            "rename target collides with an existing column.",
            code="rename.target_collision",
            context={"path": path, "columns": collision},
        )
    reserved = sorted(
        target
        for target in targets
        if target in {"__source_file", "__source_row_group", "__source_row_index", "__active_order"}
    )
    if reserved:
        raise ValidationError(
            "rename target uses a reserved runtime column.",
            code="rename.reserved_target",
            context={"path": path, "columns": reserved},
        )
    return resolved


def _resolve_join_output_columns(
    policy: Any,
    *,
    source_columns: tuple[str, ...],
    right_on: tuple[str, ...],
    known_columns: set[str],
    suffix: str,
    path: str,
) -> list[str]:
    if not isinstance(policy, dict):
        raise ValidationError(
            "join columns must be a mapping.",
            code="yaml.invalid_type",
            context={"path": f"{path}.columns"},
        )
    _keys(policy, {"include", "exclude", "regex"}, path=f"{path}.columns")
    include = [str(item) for item in policy.get("include") or []]
    exclude = {str(item) for item in policy.get("exclude") or []}
    patterns = []
    for pattern in policy.get("regex") or []:
        try:
            patterns.append(re.compile(str(pattern)))
        except re.error as error:
            raise ValidationError(
                "Invalid join column regex.",
                code="join.invalid_column_regex",
                context={"path": f"{path}.columns.regex", "pattern": pattern},
            ) from error
    missing = sorted(set(include).difference(source_columns))
    if source_columns and missing:
        raise ValidationError(
            "Join include columns are missing from the right source.",
            code="operation.missing_input_columns",
            context={"path": f"{path}.columns.include", "columns": missing},
        )
    selected = (
        list(source_columns)
        if not include and not patterns
        else list(
            dict.fromkeys(
                [
                    *include,
                    *[name for name in source_columns if any(p.search(name) for p in patterns)],
                ]
            )
        )
    )
    selected = [name for name in selected if name not in exclude and name not in right_on]
    outputs = [f"{name}{suffix}" if name in known_columns else name for name in selected]
    if len(outputs) != len(set(outputs)) or any(name in known_columns for name in outputs):
        raise ValidationError(
            "Join output column collision cannot be resolved by suffix.",
            code="join.output_column_collision",
            context={"path": path, "columns": outputs},
        )
    return outputs


def _assertion_columns(rules: list[dict[str, Any]], *, path: str) -> tuple[str, ...]:
    supported = {
        "required_columns",
        "dtype",
        "not_null",
        "unique",
        "accepted_values",
        "range",
        "row_count",
    }
    columns: list[str] = []
    rule_ids: set[str] = set()
    for index, rule in enumerate(rules):
        rule_path = f"{path}.rules[{index}]"
        _keys(
            rule,
            {
                "id",
                "kind",
                "columns",
                "column",
                "dtype",
                "values",
                "min",
                "max",
                "inclusive",
                "min_rows",
                "max_rows",
            },
            path=rule_path,
        )
        rule_id = _required_string(rule.get("id"), path=f"{rule_path}.id")
        if rule_id in rule_ids:
            raise ValidationError(
                "Assertion rule IDs must be unique.",
                code="assertion.duplicate_rule_id",
                context={"path": rule_path},
            )
        rule_ids.add(rule_id)
        kind = _required_string(rule.get("kind"), path=f"{rule_path}.kind")
        if kind not in supported:
            raise ValidationError(
                "Unsupported assertion rule.",
                code="assertion.unsupported_rule",
                context={"path": rule_path, "kind": kind},
            )
        values = rule.get("columns") or ([rule["column"]] if rule.get("column") else [])
        if not isinstance(values, list) or not all(str(item).strip() for item in values):
            raise ValidationError(
                "Assertion columns must be a string list.",
                code="assertion.invalid_rule",
                context={"path": rule_path, "kind": kind},
            )
        normalized = [str(item) for item in values]
        if len(normalized) != len(set(normalized)):
            raise ValidationError(
                "Assertion columns must be unique.",
                code="assertion.invalid_rule",
                context={"path": rule_path, "kind": kind},
            )
        if kind in {"required_columns", "dtype", "not_null", "unique"} and not normalized:
            raise ValidationError(
                "Assertion rule requires columns.",
                code="assertion.invalid_rule",
                context={"path": rule_path, "kind": kind},
            )
        if kind in {"accepted_values", "range"} and len(normalized) != 1:
            raise ValidationError(
                "Assertion rule requires exactly one column.",
                code="assertion.invalid_rule",
                context={"path": rule_path, "kind": kind},
            )
        if kind == "dtype" and not str(rule.get("dtype") or "").strip():
            raise ValidationError(
                "dtype assertion requires dtype.",
                code="assertion.invalid_rule",
                context={"path": rule_path},
            )
        if kind == "accepted_values" and not isinstance(rule.get("values"), list):
            raise ValidationError(
                "accepted_values assertion requires a values list.",
                code="assertion.invalid_rule",
                context={"path": rule_path},
            )
        if kind == "range" and rule.get("min") is None and rule.get("max") is None:
            raise ValidationError(
                "range assertion requires min or max.",
                code="assertion.invalid_rule",
                context={"path": rule_path},
            )
        if kind == "range":
            bounds = [value for value in (rule.get("min"), rule.get("max")) if value is not None]
            if any(
                not isinstance(value, (int, float, str)) or isinstance(value, bool)
                for value in bounds
            ):
                raise ValidationError(
                    "range bounds must be numeric or ISO-like strings.",
                    code="assertion.invalid_rule",
                    context={"path": rule_path},
                )
            if len(bounds) == 2 and (isinstance(bounds[0], str) != isinstance(bounds[1], str)):
                raise ValidationError(
                    "range min and max must use the same scalar category.",
                    code="assertion.invalid_rule",
                    context={"path": rule_path},
                )
        if kind == "row_count":
            if normalized or (rule.get("min_rows") is None and rule.get("max_rows") is None):
                raise ValidationError(
                    "row_count assertion requires min_rows or max_rows and no columns.",
                    code="assertion.invalid_rule",
                    context={"path": rule_path},
                )
            minimum = rule.get("min_rows")
            maximum = rule.get("max_rows")
            if any(
                value is not None and (not isinstance(value, int) or value < 0)
                for value in (minimum, maximum)
            ):
                raise ValidationError(
                    "row_count bounds must be non-negative integers.",
                    code="assertion.invalid_rule",
                    context={"path": rule_path},
                )
            if minimum is not None and maximum is not None and minimum > maximum:
                raise ValidationError(
                    "row_count min_rows must not exceed max_rows.",
                    code="assertion.invalid_rule",
                    context={"path": rule_path},
                )
        columns.extend(normalized)
    return tuple(dict.fromkeys(columns))


def _validate_static_assertion_dtypes(
    rules: list[dict[str, Any]],
    *,
    known_dtypes: dict[str, str],
    path: str,
) -> None:
    for index, rule in enumerate(rules):
        if rule.get("kind") != "dtype":
            continue
        expected = _canonical_dtype(str(rule["dtype"]))
        columns = rule.get("columns") or [rule.get("column")]
        mismatches = {
            str(column): known_dtypes[str(column)]
            for column in columns
            if str(column) in known_dtypes
            and known_dtypes[str(column)] != "unknown"
            and _canonical_dtype(known_dtypes[str(column)]) != expected
        }
        if mismatches:
            raise ValidationError(
                "dtype assertion is incompatible with the compiled input schema.",
                code="assertion.dtype_mismatch",
                context={
                    "path": f"{path}.rules[{index}]",
                    "expected": expected,
                    "actual": mismatches,
                },
            )


def _canonical_dtype(value: str) -> str:
    normalized = value.strip().lower().replace(" ", "")
    aliases = {
        "text": "string",
        "utf8": "string",
        "large_string": "string",
        "string": "string",
        "int8": "int8",
        "tinyint": "int8",
        "int16": "int16",
        "smallint": "int16",
        "int32": "int32",
        "integer": "int32",
        "int64": "int64",
        "bigint": "int64",
        "float": "float32",
        "float32": "float32",
        "double": "float64",
        "float64": "float64",
        "bool": "bool",
        "boolean": "bool",
        "date": "date32[day]",
        "datetime": "timestamp[us]",
        "timestamp": "timestamp[us]",
    }
    return aliases.get(normalized, normalized)


def _reject_duplicate_output_names(
    outputs: tuple[ColumnContract, ...],
    *,
    path: str,
) -> None:
    names = [item.name for item in outputs]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValidationError(
            "Operation output column names must be unique.",
            code="operation.duplicate_output_columns",
            context={"path": path, "columns": duplicates},
        )


def _validate_explicit_physical_boundaries(
    operations: list[Any],
    *,
    sources: set[str],
) -> None:
    by_id = {operation.operation_id: operation for operation in operations}
    positions = {operation.operation_id: index for index, operation in enumerate(operations)}
    materializations = [
        operation for operation in operations if operation.kind is OperationKind.MATERIALIZE
    ]
    for operation in operations:
        if operation.kind is OperationKind.ACTIVE_ROW_SELECTION:
            sidecar_id = str(operation.config.get("sidecar") or "")
            referenced = by_id.get(sidecar_id)
            if referenced is None or referenced.kind is not OperationKind.BUILD_SIDECAR:
                raise ValidationError(
                    "active_row_selection must reference a build_sidecar operation.",
                    code="active_row_selection.sidecar_required",
                    context={"operation_id": operation.operation_id, "sidecar": sidecar_id or None},
                )
            if positions[sidecar_id] >= positions[operation.operation_id]:
                raise ValidationError(
                    "active_row_selection sidecar must appear before the selector.",
                    code="operation.forward_reference",
                    context={"operation_id": operation.operation_id, "reference": sidecar_id},
                )
            sidecar_columns = referenced.config.get("columns")
            if sidecar_columns != "auto":
                missing = sorted(set(operation.input_columns).difference(sidecar_columns or []))
                if missing:
                    raise ValidationError(
                        "build_sidecar columns do not cover the selector inputs.",
                        code="build_sidecar.missing_selector_columns",
                        context={
                            "operation_id": referenced.operation_id,
                            "selector": operation.operation_id,
                            "columns": missing,
                        },
                    )
        if operation.kind is OperationKind.MATERIALIZE:
            reference = str(operation.config.get("coordinates_from") or "")
            if reference:
                referenced = by_id.get(reference)
                if referenced is None or referenced.kind is not OperationKind.ACTIVE_ROW_SELECTION:
                    raise ValidationError(
                        "materialize coordinates_from must reference active_row_selection.",
                        code="materialize.invalid_coordinate_source",
                        context={"operation_id": operation.operation_id, "reference": reference},
                    )
                if positions[reference] >= positions[operation.operation_id]:
                    raise ValidationError(
                        "materialize coordinate source must appear before materialize.",
                        code="operation.forward_reference",
                        context={"operation_id": operation.operation_id, "reference": reference},
                    )
            source = str(operation.config.get("source") or "")
            if source and source not in sources:
                raise ValidationError(
                    "materialize references an unknown source.",
                    code="operation.unknown_source",
                    context={"operation_id": operation.operation_id, "source": source},
                )

    heavy_kinds = {
        OperationKind.LIST_RESTORE,
        OperationKind.PIVOT,
        OperationKind.JOIN,
    }
    for operation in operations:
        if operation.kind not in heavy_kinds:
            continue
        prior = [
            item
            for item in materializations
            if positions[item.operation_id] < positions[operation.operation_id]
        ]
        if not prior:
            raise ValidationError(
                "Heavy operations require an upstream materialize boundary.",
                code="materialize.required_before_heavy_operation",
                context={"operation_id": operation.operation_id, "operation": operation.kind.value},
            )
        # A materialize boundary may intentionally preserve only a subset of
        # the groups used by the downstream heavy operation.  The sidecar
        # still computes the complete logical group key, while this setting
        # controls the coarser physical boundary used for payload reads.
        # Do not require every logical group to be repeated here.

    for operation in operations:
        if operation.kind is not OperationKind.ACTIVE_ROW_SELECTION:
            continue
        consumers = [
            item
            for item in materializations
            if item.config.get("coordinates_from") == operation.operation_id
        ]
        if not consumers:
            raise ValidationError(
                "active_row_selection must be consumed by a materialize boundary.",
                code="materialize.selector_not_consumed",
                context={"operation_id": operation.operation_id},
            )
        # preserve_groups is a physical partition-boundary hint.  It is valid
        # for it to be a proper subset of the selector groups; the selected
        # coordinates remain authoritative for row selection.

    right_sources = {
        str(operation.config["right_source"])
        for operation in operations
        if operation.kind is OperationKind.JOIN
    }
    primary_sources = sources.difference(right_sources)
    for operation in materializations:
        source = str(operation.config.get("source") or "")
        if source and source not in primary_sources:
            raise ValidationError(
                "materialize source must be the pipeline primary source.",
                code="materialize.non_primary_source",
                context={
                    "operation_id": operation.operation_id,
                    "source": source,
                    "primary_sources": sorted(primary_sources),
                },
            )

    write = operations[-1]
    if materializations:
        final_partition = tuple(materializations[-1].config["partition_by"])
        write_partition = tuple(write.config["partition_by"])
        if final_partition != write_partition:
            raise ValidationError(
                "The final materialize and write_dataset partition_by contracts must match.",
                code="materialize.partition_mismatch",
                context={
                    "materialize": list(final_partition),
                    "write_dataset": list(write_partition),
                },
            )


def _positive_integer(value: Any, *, path: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValidationError(
            f"{path} must be a positive integer.",
            code="yaml.invalid_type",
            context={"path": path, "value": value},
        )
    return value


def _reject_unknown(
    value: dict[str, Any], allowed: set[str] | frozenset[str], *, path: str
) -> None:
    unknown = sorted(set(value).difference(allowed))
    if unknown:
        raise ValidationError(
            "Unknown YAML keys.", code="yaml.unknown_key", context={"path": path, "keys": unknown}
        )


def _keys(value: dict[str, Any], allowed: set[str], *, path: str) -> None:
    _reject_unknown(value, allowed, path=path)


def _mapping(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(
            f"{path} must be a mapping.", code="yaml.invalid_type", context={"path": path}
        )
    return value


def _mapping_list(value: Any, *, path: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValidationError(
            f"{path} must be a non-empty list.", code="yaml.invalid_type", context={"path": path}
        )
    return [_mapping(item, path=f"{path}[{index}]") for index, item in enumerate(value)]


def _string_list(value: Any, *, path: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or not all(str(item).strip() for item in value):
        raise ValidationError(
            f"{path} must be a non-empty string list.",
            code="yaml.invalid_type",
            context={"path": path},
        )
    return tuple(str(item) for item in value)


def _required_string(value: Any, *, path: str) -> str:
    result = str(value or "").strip()
    if not result:
        _required(path=path)
    return result


def _required(*, path: str) -> None:
    raise ValidationError(f"{path} is required.", code="yaml.required_key", context={"path": path})
