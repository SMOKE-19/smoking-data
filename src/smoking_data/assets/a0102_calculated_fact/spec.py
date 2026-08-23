from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from smoking_data.core.exceptions import ValidationError
from smoking_data.runtime.object_store.config import PublicationSpec

SCHEMA_VERSION = "smoking-data.calculated-fact.v2"
SUPPORTED_UPSTREAM_CODES = frozenset({"0201", "0301"})
SUPPORTED_EXTERNAL_SUFFIXES = frozenset({".csv", ".parquet"})


@dataclass(frozen=True, slots=True)
class ExpressionFileSpec:
    path: Path
    checksum: str
    column_name_field: str
    expression_field: str


@dataclass(frozen=True, slots=True)
class LookupFileSpec:
    alias: str
    path: Path
    checksum: str
    source_keys: tuple[str, ...]
    lookup_keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ColumnAliasFileSpec:
    alias: str
    path: Path
    checksum: str
    source_column_field: str
    alias_column_field: str


@dataclass(frozen=True, slots=True)
class ListColumnSpec:
    source: str
    target: str


@dataclass(frozen=True, slots=True)
class CalculatedFactSpec:
    path: Path
    job_name: str
    upstream_definition: Path
    upstream_asset_code: str
    identity_columns: tuple[str, ...]
    partition_by: tuple[str, ...]
    expression_file: ExpressionFileSpec
    lookup_files: tuple[LookupFileSpec, ...]
    column_alias_files: tuple[ColumnAliasFileSpec, ...]
    expand_columns: tuple[ListColumnSpec, ...]
    compact_columns: tuple[ListColumnSpec, ...]
    phase_aliases: tuple[str, ...]
    target_rows_per_part: int
    max_source_files_per_chunk: int
    materialize_workers: int
    materialize_worker_max: int
    materialize_target_peak_memory_mb: int
    memory_hard_limit_mb: int
    memory_safety_ratio: float
    output: dict[str, Any]
    raw: dict[str, Any]
    canonical_hash: str


def load_calculated_fact_spec(path: str | Path) -> CalculatedFactSpec:
    definition_path = Path(path).expanduser().resolve()
    if not definition_path.is_file():
        _fail("external_file.not_found", "0102 definition does not exist.", path=definition_path)
    raw = yaml.safe_load(definition_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        _fail("yaml.invalid_type", "0102 definition root must be a mapping.", path="$.")
    _unknown(
        raw,
        {
            "yaml",
            "job",
            "output",
            "define_upstream",
            "build_sidecar",
            "materialize",
            "save_dataset",
            "execution",
        },
        path="$",
    )
    header = _mapping(raw.get("yaml"), path="yaml")
    _unknown(header, {"schema_version", "asset_code"}, path="yaml")
    if header != {"schema_version": SCHEMA_VERSION, "asset_code": "0102"}:
        _fail(
            "yaml.unsupported_schema_version",
            "0102 definition requires the calculated-fact v2 phase contract.",
            expected={"schema_version": SCHEMA_VERSION, "asset_code": "0102"},
        )
    if _filename_asset_code(definition_path) not in {None, "0102"}:
        _fail("yaml.asset_code_mismatch", "Filename suffix and yaml.asset_code differ.")

    job = _mapping(raw.get("job"), path="job")
    _unknown(job, {"name"}, path="job")
    job_name = _string(job.get("name"), path="job.name")

    upstreams = raw.get("define_upstream")
    if not isinstance(upstreams, list) or len(upstreams) != 1:
        _fail(
            "yaml.invalid_type",
            "define_upstream must contain exactly one define_asset operation.",
            path="define_upstream",
        )
    upstream = _mapping(upstreams[0], path="define_upstream[0]")
    _unknown(upstream, {"op", "alias", "definition"}, path="define_upstream[0]")
    if upstream.get("op") != "define_asset":
        _fail("operation.unsupported", "0102 upstream operation must be define_asset.")
    upstream_alias = _string(upstream.get("alias"), path="define_upstream[0].alias")
    upstream_definition = _external_path(
        upstream.get("definition"), definition_path, path="define_upstream[0].definition"
    )
    upstream_asset_code = _upstream_code(upstream_definition)

    build = _mapping(raw.get("build_sidecar"), path="build_sidecar")
    _unknown(
        build,
        {"alias", "source", "operations", "execution"},
        path="build_sidecar",
    )
    build_alias = _string(build.get("alias"), path="build_sidecar.alias")
    if _string(build.get("source"), path="build_sidecar.source") != upstream_alias:
        _fail(
            "dag.unknown_input_operation",
            "build_sidecar.source must reference define_upstream.alias.",
        )
    build_operations = _operation_list(
        build.get("operations"), path="build_sidecar.operations"
    )
    if len(build_operations) != 1 or build_operations[0].get("op") != "incremental_fact_selection":
        _fail(
            "build_sidecar.selector_position_invalid",
            "0102 build_sidecar requires exactly one incremental_fact_selection operation.",
        )
    selector = build_operations[0]
    _unknown(
        selector,
        {"op", "alias", "identity_columns"},
        path="build_sidecar.operations[0]",
    )
    selector_alias = _string(
        selector.get("alias"), path="build_sidecar.operations[0].alias"
    )
    identity_columns = _strings(
        selector.get("identity_columns"),
        path="build_sidecar.operations[0].identity_columns",
    )
    max_source_files_per_chunk = _build_execution(build.get("execution"))

    materialize = _mapping(raw.get("materialize"), path="materialize")
    _unknown(
        materialize,
        {
            "alias",
            "source",
            "coordinates",
            "partition_by",
            "part_boundary",
            "workers",
            "max_tasks_per_child",
            "operations",
        },
        path="materialize",
    )
    materialize_alias = _string(materialize.get("alias"), path="materialize.alias")
    if _string(materialize.get("source"), path="materialize.source") != upstream_alias:
        _fail(
            "dag.unknown_input_operation",
            "materialize.source must reference define_upstream.alias.",
        )
    if _string(materialize.get("coordinates"), path="materialize.coordinates") != build_alias:
        _fail(
            "materialize.invalid_coordinate_source",
            "materialize.coordinates must reference build_sidecar.alias.",
        )
    partition_by = _strings(
        materialize.get("partition_by"), path="materialize.partition_by"
    )
    if not set(partition_by).issubset(identity_columns):
        _fail(
            "incremental.invalid_identity",
            "materialize.partition_by must be a subset of identity_columns.",
        )
    target_rows_per_part = _part_boundary(
        materialize.get("part_boundary"), identity_columns=identity_columns
    )
    materialize_workers = _positive_int(
        materialize.get("workers", 1), path="materialize.workers"
    )
    _one(
        materialize.get("max_tasks_per_child", 1),
        path="materialize.max_tasks_per_child",
    )

    operations = _operation_list(
        materialize.get("operations"), path="materialize.operations"
    )
    operation_kinds = [str(item.get("op") or "") for item in operations]
    allowed_kinds = {
        "expand_list_rows",
        "add_calc_cols_from_file",
        "compact_list_rows",
        "unpivot_0102",
    }
    if any(kind not in allowed_kinds for kind in operation_kinds):
        _fail(
            "operation.unsupported",
            "0102 materialize contains an unsupported operation.",
            operations=operation_kinds,
        )
    if operation_kinds.count("add_calc_cols_from_file") != 1:
        _fail(
            "operation.required",
            "0102 materialize requires exactly one add_calc_cols_from_file operation.",
        )
    if operation_kinds.count("unpivot_0102") != 1 or operation_kinds[-1] != "unpivot_0102":
        _fail(
            "operation.required",
            "0102 materialize requires one final unpivot_0102 operation.",
        )
    has_expand = "expand_list_rows" in operation_kinds
    has_compact = "compact_list_rows" in operation_kinds
    if has_expand != has_compact or operation_kinds.count("expand_list_rows") > 1 or operation_kinds.count("compact_list_rows") > 1:
        _fail(
            "list.phase_pair_required",
            "expand_list_rows and compact_list_rows must occur exactly once as a pair.",
        )
    expected_order = (
        ["expand_list_rows", "add_calc_cols_from_file", "compact_list_rows", "unpivot_0102"]
        if has_expand
        else ["add_calc_cols_from_file", "unpivot_0102"]
    )
    if operation_kinds != expected_order:
        _fail(
            "operation.order_invalid",
            "0102 materialize operations must follow the calculated-fact phase order.",
            expected=expected_order,
            actual=operation_kinds,
        )

    phase_aliases = [upstream_alias, build_alias, selector_alias, materialize_alias]
    expand_columns: tuple[ListColumnSpec, ...] = ()
    compact_columns: tuple[ListColumnSpec, ...] = ()
    expand_alias: str | None = None
    operation_aliases: list[str] = []
    expression_file: ExpressionFileSpec | None = None
    lookup_files: tuple[LookupFileSpec, ...] = ()
    alias_files: tuple[ColumnAliasFileSpec, ...] = ()
    for index, operation in enumerate(operations):
        path = f"materialize.operations[{index}]"
        kind = str(operation["op"])
        if kind == "expand_list_rows":
            expand_alias, expand_columns = _expand_operation(operation, path=path)
            operation_aliases.append(expand_alias)
        elif kind == "add_calc_cols_from_file":
            calculate_alias, expression_file, lookup_files, alias_files = _calculate_operation(
                operation, owner=definition_path, path=path
            )
            operation_aliases.append(calculate_alias)
        elif kind == "compact_list_rows":
            compact_alias, compact_columns = _compact_operation(operation, path=path)
            if operation.get("expansion") != expand_alias:
                _fail(
                    "list.invalid_expansion_reference",
                    "compact_list_rows.expansion must reference expand_list_rows.alias.",
                )
            operation_aliases.append(compact_alias)
        else:
            operation_aliases.append(_unpivot_operation(operation, path=path))
    assert expression_file is not None
    phase_aliases.extend(operation_aliases)

    save = _mapping(raw.get("save_dataset"), path="save_dataset")
    _unknown(save, {"alias", "input", "partition_by", "operations"}, path="save_dataset")
    save_alias = _string(save.get("alias"), path="save_dataset.alias")
    if _string(save.get("input"), path="save_dataset.input") != materialize_alias:
        _fail("save_dataset.invalid_input", "save_dataset.input must reference materialize.alias.")
    if _strings(save.get("partition_by"), path="save_dataset.partition_by") != partition_by:
        _fail("incremental.partition_mismatch", "save_dataset.partition_by must match incremental.partition_by.")
    if save.get("operations") not in (None, []):
        _fail(
            "operation.unsupported",
            "0102 save_dataset.operations must be empty.",
        )
    phase_aliases.append(save_alias)
    if len(set(phase_aliases)) != len(phase_aliases):
        _fail("dag.duplicate_operation_alias", "0102 phase aliases must be unique.")

    (
        execution_max_files,
        memory_hard_limit_mb,
        memory_safety_ratio,
        materialize_target_peak_memory_mb,
        materialize_worker_max,
    ) = _execution(
        raw.get("execution"), requested_materialize_workers=materialize_workers
    )
    max_source_files_per_chunk = min(
        max_source_files_per_chunk, execution_max_files
    )

    output = _mapping(raw.get("output"), path="output")
    artifact = _mapping(output.get("artifact"), path="output.artifact")
    PublicationSpec.from_mapping(artifact.get("publication"))
    canonical_hash = _canonical_hash(
        raw,
        [expression_file, *lookup_files, *alias_files],
    )
    return CalculatedFactSpec(
        path=definition_path,
        job_name=job_name,
        upstream_definition=upstream_definition,
        upstream_asset_code=upstream_asset_code,
        identity_columns=identity_columns,
        partition_by=partition_by,
        expression_file=expression_file,
        lookup_files=lookup_files,
        column_alias_files=alias_files,
        expand_columns=expand_columns,
        compact_columns=compact_columns,
        phase_aliases=tuple(phase_aliases),
        target_rows_per_part=target_rows_per_part,
        max_source_files_per_chunk=max_source_files_per_chunk,
        materialize_workers=materialize_workers,
        materialize_worker_max=materialize_worker_max,
        materialize_target_peak_memory_mb=materialize_target_peak_memory_mb,
        memory_hard_limit_mb=memory_hard_limit_mb,
        memory_safety_ratio=memory_safety_ratio,
        output=dict(output),
        raw=raw,
        canonical_hash=canonical_hash,
    )


def _expression_file(value: Any, *, owner: Path) -> ExpressionFileSpec:
    item = _mapping(value, path="calculate_columns.expression_file")
    _unknown(item, {"path", "column_name_field", "expression_field"}, path="calculate_columns.expression_file")
    path = _supported_external_path(item.get("path"), owner)
    return ExpressionFileSpec(
        path=path,
        checksum=_sha256(path),
        column_name_field=_string(
            item.get("column_name_field"), path="expression_file.column_name_field"
        ),
        expression_field=_string(
            item.get("expression_field"), path="expression_file.expression_field"
        ),
    )


def _lookup_files(value: Any, *, owner: Path) -> tuple[LookupFileSpec, ...]:
    if not isinstance(value, list):
        _fail("yaml.invalid_type", "lookup_files must be a list.")
    result: list[LookupFileSpec] = []
    for index, raw in enumerate(value):
        item = _mapping(raw, path=f"lookup_files[{index}]")
        _unknown(item, {"alias", "path", "source_keys", "lookup_keys"}, path=f"lookup_files[{index}]")
        source_keys = _strings(item.get("source_keys"), path=f"lookup_files[{index}].source_keys")
        lookup_keys = _strings(item.get("lookup_keys"), path=f"lookup_files[{index}].lookup_keys")
        if len(source_keys) != len(lookup_keys):
            _fail("lookup.invalid_key_mapping", "source_keys and lookup_keys lengths differ.")
        file_path = _supported_external_path(item.get("path"), owner)
        result.append(
            LookupFileSpec(
                alias=_string(item.get("alias"), path=f"lookup_files[{index}].alias"),
                path=file_path,
                checksum=_sha256(file_path),
                source_keys=source_keys,
                lookup_keys=lookup_keys,
            )
        )
    _unique_aliases(result, "lookup")
    return tuple(result)


def _column_alias_files(value: Any, *, owner: Path) -> tuple[ColumnAliasFileSpec, ...]:
    if not isinstance(value, list):
        _fail("yaml.invalid_type", "column_alias_files must be a list.")
    result: list[ColumnAliasFileSpec] = []
    for index, raw in enumerate(value):
        item = _mapping(raw, path=f"column_alias_files[{index}]")
        _unknown(
            item,
            {"alias", "path", "source_column_field", "alias_column_field"},
            path=f"column_alias_files[{index}]",
        )
        file_path = _supported_external_path(item.get("path"), owner)
        result.append(
            ColumnAliasFileSpec(
                alias=_string(item.get("alias"), path=f"column_alias_files[{index}].alias"),
                path=file_path,
                checksum=_sha256(file_path),
                source_column_field=_string(
                    item.get("source_column_field"),
                    path="column_alias_files.source_column_field",
                ),
                alias_column_field=_string(
                    item.get("alias_column_field"),
                    path="column_alias_files.alias_column_field",
                ),
            )
        )
    _unique_aliases(result, "column alias")
    return tuple(result)


def _operation_list(value: Any, *, path: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        _fail("yaml.invalid_type", f"{path} must be a non-empty list.")
    return [_mapping(item, path=f"{path}[{index}]") for index, item in enumerate(value)]


def _build_execution(value: Any) -> int:
    execution = _mapping(value or {}, path="build_sidecar.execution")
    _unknown(execution, {"workers", "worker_recycle"}, path="build_sidecar.execution")
    _one(execution.get("workers", 1), path="build_sidecar.execution.workers")
    recycle = _mapping(
        execution.get("worker_recycle") or {},
        path="build_sidecar.execution.worker_recycle",
    )
    _unknown(
        recycle,
        {"max_source_files", "max_projected_bytes_mb"},
        path="build_sidecar.execution.worker_recycle",
    )
    max_source_files = _positive_int(
        recycle.get("max_source_files", 16),
        path="build_sidecar.execution.worker_recycle.max_source_files",
    )
    _positive_int(
        recycle.get("max_projected_bytes_mb", 512),
        path="build_sidecar.execution.worker_recycle.max_projected_bytes_mb",
    )
    return max_source_files


def _part_boundary(value: Any, *, identity_columns: tuple[str, ...]) -> int:
    boundary = _mapping(value, path="materialize.part_boundary")
    _unknown(
        boundary,
        {"target_rows", "preserve_groups"},
        path="materialize.part_boundary",
    )
    target_rows = _positive_int(
        boundary.get("target_rows"), path="materialize.part_boundary.target_rows"
    )
    preserve_groups = _strings(
        boundary.get("preserve_groups"),
        path="materialize.part_boundary.preserve_groups",
    )
    if not set(identity_columns).issubset(preserve_groups):
        _fail(
            "materialize.invalid_part_boundary",
            "materialize.part_boundary.preserve_groups must contain all identity columns.",
        )
    return target_rows


def _execution(
    value: Any, *, requested_materialize_workers: int
) -> tuple[int, int, float, int, int]:
    execution = _mapping(value or {}, path="execution")
    _unknown(
        execution,
        {"memory", "max_source_files_per_task", "reset_before_run"},
        path="execution",
    )
    if execution.get("reset_before_run", False) is not False:
        _fail(
            "execution.unsupported_reset",
            "0102 append-generation execution requires reset_before_run=false.",
        )
    maximum_files = _positive_int(
        execution.get("max_source_files_per_task", 40),
        path="execution.max_source_files_per_task",
    )
    memory = _mapping(execution.get("memory") or {}, path="execution.memory")
    _unknown(
        memory,
        {"hard_limit_mb", "safety_ratio", "phases"},
        path="execution.memory",
    )
    hard_limit = _positive_int(
        memory.get("hard_limit_mb", 4096), path="execution.memory.hard_limit_mb"
    )
    safety_ratio = memory.get("safety_ratio", 0.8)
    if (
        not isinstance(safety_ratio, (int, float))
        or isinstance(safety_ratio, bool)
        or not 0 < float(safety_ratio) <= 1
    ):
        _fail(
            "yaml.invalid_type",
            "execution.memory.safety_ratio must be > 0 and <= 1.",
        )
    phases = _mapping(memory.get("phases") or {}, path="execution.memory.phases")
    _unknown(
        phases,
        {"build_sidecar", "materialize", "save_dataset"},
        path="execution.memory.phases",
    )
    materialize_target = hard_limit
    materialize_worker_max = requested_materialize_workers
    for phase_name, phase_value in phases.items():
        phase = _mapping(
            phase_value, path=f"execution.memory.phases.{phase_name}"
        )
        _unknown(
            phase,
            {"target_peak_memory_mb", "workers"},
            path=f"execution.memory.phases.{phase_name}",
        )
        target = _positive_int(
            phase.get("target_peak_memory_mb"),
            path=f"execution.memory.phases.{phase_name}.target_peak_memory_mb",
        )
        if target > hard_limit:
            _fail(
                "execution.invalid_memory_budget",
                "Phase target_peak_memory_mb cannot exceed hard_limit_mb.",
                phase=phase_name,
            )
        workers = _mapping(
            phase.get("workers"),
            path=f"execution.memory.phases.{phase_name}.workers",
        )
        _unknown(
            workers,
            {"min", "max"},
            path=f"execution.memory.phases.{phase_name}.workers",
        )
        minimum = _positive_int(
            workers.get("min"),
            path=f"execution.memory.phases.{phase_name}.workers.min",
        )
        maximum = _positive_int(
            workers.get("max"),
            path=f"execution.memory.phases.{phase_name}.workers.max",
        )
        if minimum > maximum:
            _fail(
                "execution.invalid_worker_range",
                "Phase worker min must be <= max.",
                phase=phase_name,
            )
        if phase_name == "materialize":
            materialize_target = target
            materialize_worker_max = maximum
    return (
        maximum_files,
        hard_limit,
        float(safety_ratio),
        materialize_target,
        materialize_worker_max,
    )


def _expand_operation(
    operation: dict[str, Any], *, path: str
) -> tuple[str, tuple[ListColumnSpec, ...]]:
    _unknown(operation, {"op", "alias", "columns"}, path=path)
    alias = _string(operation.get("alias"), path=f"{path}.alias")
    return alias, _list_columns(
        operation.get("columns"),
        source_key="source",
        target_key="element_alias",
        path=f"{path}.columns",
    )


def _calculate_operation(
    operation: dict[str, Any], *, owner: Path, path: str
) -> tuple[
    str,
    ExpressionFileSpec,
    tuple[LookupFileSpec, ...],
    tuple[ColumnAliasFileSpec, ...],
]:
    _unknown(
        operation,
        {"op", "alias", "expression_file", "lookup_files", "column_alias_files"},
        path=path,
    )
    return (
        _string(operation.get("alias"), path=f"{path}.alias"),
        _expression_file(operation.get("expression_file"), owner=owner),
        _lookup_files(operation.get("lookup_files") or [], owner=owner),
        _column_alias_files(operation.get("column_alias_files") or [], owner=owner),
    )


def _compact_operation(
    operation: dict[str, Any], *, path: str
) -> tuple[str, tuple[ListColumnSpec, ...]]:
    _unknown(operation, {"op", "alias", "expansion", "columns"}, path=path)
    alias = _string(operation.get("alias"), path=f"{path}.alias")
    return alias, _list_columns(
        operation.get("columns"),
        source_key="source",
        target_key="output",
        path=f"{path}.columns",
    )


def _unpivot_operation(operation: dict[str, Any], *, path: str) -> str:
    _unknown(operation, {"op", "alias", "contract", "columns"}, path=path)
    if operation.get("contract") != "long_fact_v1":
        _fail("long_fact.invalid_contract", "unpivot_0102.contract must be long_fact_v1.")
    if operation.get("columns") != "calculated":
        _fail(
            "long_fact.invalid_contract",
            "unpivot_0102.columns must be calculated.",
        )
    return _string(operation.get("alias"), path=f"{path}.alias")


def _one(value: Any, *, path: str) -> None:
    if value != 1 or isinstance(value, bool):
        _fail(
            "execution.unsupported_parallelism",
            f"{path} currently supports only 1.",
            value=value,
        )


def _positive_int(value: Any, *, path: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        _fail("yaml.invalid_type", f"{path} must be an integer >= 1.", value=value)
    return value


def _list_columns(value: Any, *, source_key: str, target_key: str, path: str) -> tuple[ListColumnSpec, ...]:
    if not isinstance(value, list) or not value:
        _fail("yaml.invalid_type", f"{path} must be a non-empty list.")
    result: list[ListColumnSpec] = []
    for index, raw in enumerate(value):
        item = _mapping(raw, path=f"{path}[{index}]")
        _unknown(item, {source_key, target_key}, path=f"{path}[{index}]")
        result.append(
            ListColumnSpec(
                _string(item.get(source_key), path=f"{path}[{index}].{source_key}"),
                _string(item.get(target_key), path=f"{path}[{index}].{target_key}"),
            )
        )
    if len({item.source for item in result}) != len(result) or len({item.target for item in result}) != len(result):
        _fail("list.duplicate_binding", f"{path} source and target names must be unique.")
    return tuple(result)


def _supported_external_path(value: Any, owner: Path) -> Path:
    path = _external_path(value, owner, path="external_file.path")
    if path.suffix.lower() not in SUPPORTED_EXTERNAL_SUFFIXES:
        _fail("external_file.unsupported_extension", "External files must be CSV or Parquet.", path=path)
    return path


def _external_path(value: Any, owner: Path, *, path: str) -> Path:
    text = _string(value, path=path)
    result = Path(text).expanduser()
    if not result.is_absolute():
        result = owner.parent / result
    result = result.resolve()
    if not result.is_file():
        _fail("external_file.not_found", "Referenced file does not exist.", path=result)
    return result


def _upstream_code(path: Path) -> str:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    header = payload.get("yaml") if isinstance(payload, dict) else None
    code = str(header.get("asset_code") or "") if isinstance(header, dict) else ""
    if code not in SUPPORTED_UPSTREAM_CODES:
        _fail(
            "upstream.unsupported_artifact_type",
            "0102 upstream must be a 0201 Curated or 0301 Joined Curated definition.",
            asset_code=code or None,
        )
    return code


def _canonical_hash(
    raw: dict[str, Any],
    files: list[ExpressionFileSpec | LookupFileSpec | ColumnAliasFileSpec],
) -> str:
    payload = {
        "definition": raw,
        "external_files": [
            {
                "kind": type(item).__name__,
                "alias": getattr(item, "alias", None),
                "checksum": item.checksum,
                "contract": {
                    key: value
                    for key, value in asdict(item).items()
                    if key not in {"path", "checksum"}
                },
            }
            for item in files
        ],
        "contract_version": SCHEMA_VERSION,
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _mapping(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail("yaml.invalid_type", f"{path} must be a mapping.", path=path)
    return value


def _strings(value: Any, *, path: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        _fail("yaml.invalid_type", f"{path} must be a non-empty string list.", path=path)
    result = tuple(_string(item, path=f"{path}[]") for item in value)
    if len(set(result)) != len(result):
        _fail("yaml.duplicate_value", f"{path} contains duplicates.", path=path)
    return result


def _string(value: Any, *, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail("yaml.invalid_type", f"{path} must be a non-empty string.", path=path)
    return value.strip()


def _unknown(value: dict[str, Any], allowed: set[str], *, path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        _fail("yaml.unknown_key", f"Unsupported keys at {path}: {unknown}", path=path, keys=unknown)


def _unique_aliases(files: list[LookupFileSpec] | list[ColumnAliasFileSpec], kind: str) -> None:
    aliases = [item.alias for item in files]
    if len(set(aliases)) != len(aliases):
        _fail("yaml.duplicate_alias", f"Duplicate {kind} file alias.")


def _filename_asset_code(path: Path) -> str | None:
    parts = path.name.split(".")
    return parts[-2] if len(parts) >= 3 and parts[-1].lower() in {"yaml", "yml"} else None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fail(code: str, message: str, **context: Any) -> None:
    raise ValidationError(message, code=code, context=context)
