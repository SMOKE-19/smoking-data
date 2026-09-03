from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, replace
from typing import Any

from smoking_data.core.exceptions import ValidationError
from smoking_data.core.operations import (
    OPERATION_PROPERTIES,
    ColumnContract,
    OperationKind,
    OperationProperties,
    OperationSpec,
)

LOGICAL_PLAN_VERSION = "smoking-data.logical-plan.v1"
JOIN_TYPES = frozenset({"inner", "left", "right", "full", "cross"})
PIVOT_AGGREGATIONS = frozenset(
    {"first", "count", "sum", "avg", "mean", "min", "max", "unique_concatenate"}
)
PIVOT_OUTPUT_DTYPES = frozenset(
    {
        "TEXT",
        "STRING",
        "INT32",
        "INTEGER",
        "INT64",
        "BIGINT",
        "FLOAT",
        "FLOAT32",
        "DOUBLE",
        "FLOAT64",
        "BOOL",
        "BOOLEAN",
    }
)


@dataclass(frozen=True, slots=True)
class LogicalOperationPlan:
    preset: str
    operations: tuple[OperationSpec, ...]
    column_lineage: dict[str, tuple[str, ...]]
    version: str = LOGICAL_PLAN_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def plan_hash(self) -> str:
        payload = json.dumps(
            self.to_dict(),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


def compile_0201_logical_plan(
    raw: dict[str, Any],
    *,
    expression_ir: dict[str, Any] | None,
) -> LogicalOperationPlan:
    _validate_0201_yaml_shape(raw)
    source = _mapping(raw.get("source"), path="source")
    payload = _mapping(source.get("payload"), path="source.payload", allow_missing=True)
    row_selection = _mapping(raw.get("row_selection"), path="row_selection", allow_missing=True)
    sort_first = _mapping(
        row_selection.get("sort_first"),
        path="row_selection.sort_first",
        allow_missing=True,
    )
    pivot = _mapping(raw.get("pivot"), path="pivot", allow_missing=True)
    list_restore = _mapping(raw.get("list_restore"), path="list_restore", allow_missing=True)
    output = _mapping(raw.get("output"), path="output")
    operations: list[OperationSpec] = []

    filter_sql = str(payload.get("filter_sql") or "").strip()
    if filter_sql:
        operations.append(
            _operation(
                "filter",
                OperationKind.FILTER,
                {"sql": filter_sql},
                input_columns=_sql_identifier_columns(filter_sql),
            )
        )

    casts = payload.get("type_casts") or []
    if casts:
        outputs = tuple(
            ColumnContract(
                name=str(item.get("name") or item.get("column") or ""),
                dtype=str(item.get("type") or "") or None,
                nullable=True,
            )
            for item in casts
            if isinstance(item, dict)
        )
        operations.append(
            _operation(
                "type_cast",
                OperationKind.TYPE_CAST,
                {"columns": casts},
                input_columns=tuple(item.name for item in outputs),
                output_columns=outputs,
            )
        )

    add_calc = payload.get("add_calc") or []
    if add_calc:
        expression_columns = tuple(sorted(_ir_input_columns(expression_ir)))
        expression_lineage = _ir_expression_lineage(expression_ir)
        outputs = tuple(
            ColumnContract(name=str(item.get("name") or ""), nullable=True)
            for item in add_calc
            if isinstance(item, dict)
        )
        operations.append(
            _operation(
                "add_calc",
                OperationKind.ADD_CALC,
                {"expressions": add_calc, "expression_ir": expression_ir},
                input_columns=expression_columns,
                output_columns=outputs,
                alias_lineage={
                    item.name: expression_lineage.get(item.name, expression_columns)
                    for item in outputs
                },
                properties=(
                    replace(
                        OPERATION_PROPERTIES[OperationKind.ADD_CALC],
                        order_sensitive=True,
                        requires_complete_group=True,
                        pushdown_safe=False,
                        pushdown_capabilities=(),
                    )
                    if _ir_contains_kind(expression_ir, "window")
                    else None
                ),
            )
        )

    reference_replace = payload.get("reference_replace") or []
    if reference_replace:
        configs = reference_replace if isinstance(reference_replace, list) else [reference_replace]
        inputs: list[str] = []
        outputs: list[ColumnContract] = []
        lineage: dict[str, tuple[str, ...]] = {}
        for item in configs:
            if not isinstance(item, dict) or not bool(item.get("enabled", True)):
                continue
            source_column = str(item.get("source_column") or "")
            output_column = str(item.get("output_column") or source_column)
            inputs.append(source_column)
            outputs.append(ColumnContract(output_column, nullable=True))
            lineage[output_column] = (source_column,)
        if outputs:
            operations.append(
                _operation(
                    "reference_replace",
                    OperationKind.REFERENCE_REPLACE,
                    {"items": configs},
                    input_columns=tuple(inputs),
                    output_columns=tuple(outputs),
                    alias_lineage=lineage,
                )
            )

    if bool(sort_first.get("enabled", False)):
        group_key_items = sort_first.get("group_keys") or []
        if not isinstance(group_key_items, list) or not group_key_items:
            raise ValidationError(
                "row_selection.sort_first.group_keys must be a non-empty list.",
                code="yaml.invalid_type",
                context={"path": "row_selection.sort_first.group_keys"},
            )
        group_keys = tuple(
            str(item.get("name") if isinstance(item, dict) else item) for item in group_key_items
        )
        ordering = _ordering(sort_first.get("sort"), path="row_selection.sort_first.sort")
        operations.append(
            _operation(
                "sort_first",
                OperationKind.SORT_FIRST,
                dict(sort_first),
                input_columns=tuple(dict.fromkeys([*group_keys, *(name for name, _ in ordering)])),
                group_keys=group_keys,
                ordering=ordering,
            )
        )

    if bool(list_restore.get("enabled", False)):
        config = _mapping(list_restore.get("config"), path="list_restore.config")
        restore_schema = list_restore.get("schema")
        if not isinstance(restore_schema, dict):
            restore_schema = {}
        value_columns = tuple(str(item) for item in config.get("value_columns") or [])
        coord_columns = tuple(str(item) for item in config.get("source_coord_columns") or [])
        key_column = str(config.get("key_column") or "")
        operations.append(
            _operation(
                "list_restore",
                OperationKind.LIST_RESTORE,
                dict(list_restore),
                input_columns=tuple(dict.fromkeys([key_column, *value_columns, *coord_columns])),
                output_columns=tuple(
                    ColumnContract(
                        name, dtype=restore_schema.get(name), nullable=True
                    )
                    for name in (*value_columns, *coord_columns)
                ),
                group_keys=(key_column,) if key_column else (),
            )
        )

    include = tuple(str(item) for item in payload.get("include_columns") or [])
    exclude = tuple(str(item) for item in payload.get("exclude_columns") or [])
    if include or exclude:
        operations.append(
            _operation(
                "projection",
                OperationKind.PROJECTION,
                {"include": include, "exclude": exclude},
                input_columns=include,
                output_columns=tuple(ColumnContract(name) for name in include),
            )
        )

    if pivot.get("enabled", False):
        operations.append(_compile_pivot_operation(pivot))

    partition_column = str(output.get("partition_column") or "").strip()
    operations.append(
        _operation(
            "partition_write",
            OperationKind.PARTITION_WRITE,
            {"partition_column": partition_column},
            input_columns=(partition_column,) if partition_column else (),
            partition_keys=(partition_column,) if partition_column else (),
        )
    )
    return _plan(str(raw.get("preset") or "0201"), operations)


def compile_0301_logical_plan(raw: dict[str, Any]) -> LogicalOperationPlan:
    _validate_0301_yaml_shape(raw)
    join = _mapping(raw.get("join"), path="join")
    output = _mapping(raw.get("output"), path="output")
    right_sources = raw.get("right_sources")
    if right_sources is None:
        right = _mapping(raw.get("right"), path="right")
        right_sources = [{"name": "right", **right}]
    if not isinstance(right_sources, list) or not right_sources:
        raise ValidationError(
            "right_sources must be a non-empty list.",
            code="yaml.invalid_type",
            context={"path": "right_sources"},
        )
    operations: list[OperationSpec] = []
    default_how = _join_type(join.get("how") or "left", path="join.how")
    left_partition_key = str(join.get("left_partition_key_column") or "").strip()
    right_partition_key = str(join.get("right_partition_key_column") or "").strip()
    if right_partition_key and not left_partition_key:
        raise ValidationError(
            "right_partition_key_column requires a left partition key.",
            code="join.left_partition_key_required",
            context={"path": "join"},
        )
    default_left_on, default_right_on = _join_keys(
        join,
        how=default_how,
        path="join",
    )
    source_names: set[str] = set()
    for index, source in enumerate(right_sources):
        source_cfg = _mapping(source, path=f"right_sources[{index}]")
        source_join = _mapping(
            source_cfg.get("join"), path=f"right_sources[{index}].join", allow_missing=True
        )
        how = _join_type(
            source_join.get("how") or default_how,
            path=f"right_sources[{index}].join.how",
        )
        effective_join = {
            "left_on": source_join.get("left_on", default_left_on),
            "right_on": source_join.get("right_on", default_right_on),
        }
        left_on, right_on = _join_keys(
            effective_join,
            how=how,
            path=f"right_sources[{index}].join",
        )
        name = str(source_cfg.get("name") or "").strip()
        if not name:
            raise ValidationError(
                "right source name is required.",
                code="yaml.required_key",
                context={"path": f"right_sources[{index}].name"},
            )
        if name in source_names:
            raise ValidationError(
                f"Duplicate right source name: {name}",
                code="join.duplicate_source_name",
                context={"path": f"right_sources[{index}].name", "name": name},
            )
        source_names.add(name)
        operations.append(
            _operation(
                f"join_{index + 1}_{name}",
                OperationKind.JOIN,
                {
                    "source_name": name,
                    "how": how,
                    "left_on": left_on,
                    "right_on": right_on,
                    "suffix": str(source_cfg.get("suffix") or join.get("suffix") or f"_{name}"),
                    "columns": source_cfg.get("columns") or {},
                },
                input_columns=tuple(dict.fromkeys([*left_on, *right_on])),
                group_keys=left_on,
                partition_keys=tuple(
                    key for key in (left_partition_key, right_partition_key) if key
                ),
            )
        )
        if how in {"right", "full"} and not right_partition_key:
            raise ValidationError(
                "right/full join requires explicit partition key mapping.",
                code="join.partition_mapping_required",
                context={"path": f"right_sources[{index}].join.how", "how": how},
            )
    partition_column = str(output.get("partition_column") or "").strip()
    operations.append(
        _operation(
            "partition_write",
            OperationKind.PARTITION_WRITE,
            {"partition_column": partition_column},
            input_columns=(partition_column,) if partition_column else (),
            partition_keys=(partition_column,) if partition_column else (),
        )
    )
    return _plan(str(raw.get("preset") or "0301"), operations)


def _compile_pivot_operation(pivot: dict[str, Any]) -> OperationSpec:
    row_keys = _string_tuple(pivot.get("row_keys"), path="pivot.row_keys")
    value_keys = pivot.get("value_keys") or []
    value_keys_without_column = pivot.get("value_keys_without_column") or []
    if not isinstance(value_keys, list) or not isinstance(value_keys_without_column, list):
        raise ValidationError(
            "pivot value keys must be lists.",
            code="yaml.invalid_type",
            context={"path": "pivot"},
        )
    if not value_keys and not value_keys_without_column:
        raise ValidationError(
            "pivot requires value_keys or value_keys_without_column.",
            code="yaml.required_key",
            context={"path": "pivot"},
        )
    column_keys = (
        _string_tuple(pivot.get("column_keys"), path="pivot.column_keys") if value_keys else ()
    )
    source_columns = tuple(
        str(item.get("source_column") or "")
        for item in [*value_keys, *value_keys_without_column]
        if isinstance(item, dict) and item.get("source_column")
    )
    _validate_pivot_value_keys(value_keys, path="pivot.value_keys", has_column=True)
    _validate_pivot_value_keys(
        value_keys_without_column,
        path="pivot.value_keys_without_column",
        has_column=False,
    )
    duplicate_policy = str(pivot.get("first_duplicate_policy") or "warn").lower()
    if duplicate_policy not in {"allow", "warn", "error"}:
        raise ValidationError(
            "pivot.first_duplicate_policy must be allow, warn or error.",
            code="pivot.invalid_duplicate_policy",
            context={"path": "pivot.first_duplicate_policy", "value": duplicate_policy},
        )
    null_policy = str(pivot.get("null_column_key_policy") or "error").lower()
    if null_policy not in {"error", "label"}:
        raise ValidationError(
            "pivot.null_column_key_policy must be error or label.",
            code="pivot.invalid_null_column_key_policy",
            context={"path": "pivot.null_column_key_policy", "value": null_policy},
        )
    return _operation(
        "pivot",
        OperationKind.PIVOT,
        dict(pivot),
        input_columns=tuple(dict.fromkeys([*row_keys, *column_keys, *source_columns])),
        group_keys=row_keys,
    )


def _operation(
    operation_id: str,
    kind: OperationKind,
    config: dict[str, Any],
    *,
    input_columns: tuple[str, ...] = (),
    output_columns: tuple[ColumnContract, ...] = (),
    alias_lineage: dict[str, tuple[str, ...]] | None = None,
    group_keys: tuple[str, ...] = (),
    partition_keys: tuple[str, ...] = (),
    ordering: tuple[tuple[str, str], ...] = (),
    properties: OperationProperties | None = None,
) -> OperationSpec:
    normalized_inputs = tuple(name for name in input_columns if name)
    return OperationSpec(
        operation_id=operation_id,
        kind=kind,
        config=config,
        input_columns=normalized_inputs,
        input_contracts=tuple(ColumnContract(name) for name in normalized_inputs),
        output_columns=output_columns,
        alias_lineage=alias_lineage or {},
        group_keys=group_keys,
        partition_keys=partition_keys,
        ordering=ordering,
        properties=properties or OPERATION_PROPERTIES[kind],
    )


def _plan(preset: str, operations: list[OperationSpec]) -> LogicalOperationPlan:
    ids = [item.operation_id for item in operations]
    if len(ids) != len(set(ids)):
        raise ValidationError(
            "Logical operation IDs must be unique.",
            code="logical_plan.duplicate_operation_id",
            context={"operation_ids": ids},
        )
    return LogicalOperationPlan(
        preset=preset,
        operations=tuple(operations),
        column_lineage=resolve_column_lineage(operations),
    )


def _ir_input_columns(document: dict[str, Any] | None) -> set[str]:
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


def _ir_contains_kind(document: dict[str, Any] | None, kind: str) -> bool:
    found = False

    def visit(value: Any) -> None:
        nonlocal found
        if found:
            return
        if isinstance(value, dict):
            if value.get("kind") == kind:
                found = True
                return
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)
    return found


_SQL_KEYWORDS = frozenset(
    {
        "and",
        "as",
        "between",
        "case",
        "cast",
        "else",
        "end",
        "false",
        "bigint",
        "boolean",
        "date",
        "double",
        "float",
        "in",
        "int",
        "integer",
        "is",
        "like",
        "not",
        "null",
        "or",
        "string",
        "text",
        "then",
        "timestamp",
        "true",
        "varchar",
        "when",
    }
)


def _sql_identifier_columns(expression: str) -> tuple[str, ...]:
    quoted = re.findall(r'"([^"]+)"', expression)
    without_strings = re.sub(r"'(?:''|[^'])*'", " ", expression)
    without_quoted = re.sub(r'"[^"]+"', " ", without_strings)
    bare: list[str] = []
    for match in re.finditer(r"\b[A-Za-z_][A-Za-z0-9_]*\b", without_quoted):
        token = match.group(0)
        if token.lower() in _SQL_KEYWORDS:
            continue
        tail = without_quoted[match.end() :].lstrip()
        if tail.startswith("("):
            continue
        bare.append(token)
    return tuple(dict.fromkeys([*quoted, *bare]))


def _ir_expression_lineage(
    document: dict[str, Any] | None,
) -> dict[str, tuple[str, ...]]:
    if not isinstance(document, dict):
        return {}
    result: dict[str, tuple[str, ...]] = {}
    for layer in document.get("layers") or []:
        if not isinstance(layer, dict):
            continue
        for expression in layer.get("expressions") or []:
            if not isinstance(expression, dict):
                continue
            name = str(expression.get("name") or "").strip()
            if not name:
                continue
            result[name] = tuple(sorted(_ir_input_columns(expression.get("expr"))))
    return result


def resolve_column_lineage(
    operations: list[OperationSpec],
) -> dict[str, tuple[str, ...]]:
    lineage: dict[str, tuple[str, ...]] = {}
    for operation in operations:
        for name in operation.input_columns:
            lineage.setdefault(name, (name,))
        for alias, direct in operation.alias_lineage.items():
            roots: list[str] = []
            for source in direct:
                for root in lineage.get(source, (source,)):
                    if root not in roots:
                        roots.append(root)
            lineage[alias] = tuple(roots)
        for output in operation.output_columns:
            direct = operation.alias_lineage.get(output.name)
            if direct is None:
                direct = (output.name,) if output.name in lineage else operation.input_columns
            roots: list[str] = []
            for source in direct:
                for root in lineage.get(source, (source,)):
                    if root not in roots:
                        roots.append(root)
            lineage[output.name] = tuple(roots)
        if operation.kind is OperationKind.PROJECTION:
            include = tuple(operation.config.get("include") or ())
            exclude = set(operation.config.get("exclude") or ())
            if include:
                lineage = {
                    name: lineage.get(name, (name,)) for name in include if name not in exclude
                }
            elif exclude:
                lineage = {name: roots for name, roots in lineage.items() if name not in exclude}
    return lineage


def _mapping(value: Any, *, path: str, allow_missing: bool = False) -> dict[str, Any]:
    if value is None and allow_missing:
        return {}
    if not isinstance(value, dict):
        raise ValidationError(
            f"{path} must be a mapping.",
            code="yaml.invalid_type",
            context={"path": path, "expected": "mapping"},
        )
    return value


def _string_tuple(value: Any, *, path: str) -> tuple[str, ...]:
    if (
        not isinstance(value, (list, tuple))
        or not value
        or not all(str(item).strip() for item in value)
    ):
        raise ValidationError(
            f"{path} must be a non-empty list.",
            code="yaml.invalid_type",
            context={"path": path, "expected": "non_empty_string_list"},
        )
    return tuple(str(item) for item in value)


def _ordering(value: Any, *, path: str) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list) or not value:
        raise ValidationError(
            f"{path} must be a non-empty list.",
            code="yaml.invalid_type",
            context={"path": path},
        )
    result: list[tuple[str, str]] = []
    for index, item in enumerate(value):
        mapping = _mapping(item, path=f"{path}[{index}]")
        column = str(mapping.get("column") or "").strip()
        direction = str(mapping.get("direction") or "asc").lower()
        if not column or direction not in {"asc", "desc"}:
            raise ValidationError(
                f"{path}[{index}] requires column and asc/desc direction.",
                code="yaml.invalid_sort",
                context={"path": f"{path}[{index}]"},
            )
        result.append((column, direction))
    return tuple(result)


def _validate_0201_yaml_shape(raw: dict[str, Any]) -> None:
    _reject_unknown(
        raw,
        {
            "preset",
            "job",
            "execution",
            "source",
            "row_selection",
            "pivot",
            "list_restore",
            "output",
            "__pipeline",
        },
        path="$",
    )
    _validate_common_sections(raw)
    source = _mapping(raw.get("source"), path="source")
    _reject_unknown(source, {"upstream", "payload"}, path="source")
    _validate_upstream(source.get("upstream"), path="source.upstream")
    payload = _mapping(source.get("payload"), path="source.payload", allow_missing=True)
    _reject_unknown(
        payload,
        {
            "filter_sql",
            "type_casts",
            "add_calc",
            "reference_replace",
            "include_columns",
            "exclude_columns",
            "pre_pivot_operations",
            "post_operations",
            "final_post_projection",
            "dataset_assertions",
        },
        path="source.payload",
    )
    _validate_mapping_items(
        payload.get("type_casts"),
        allowed={"name", "type", "failure_policy"},
        path="source.payload.type_casts",
    )
    _validate_mapping_items(
        payload.get("add_calc"),
        allowed={"name", "sql", "spotfire_expression"},
        path="source.payload.add_calc",
    )
    reference_replace = payload.get("reference_replace")
    if isinstance(reference_replace, dict):
        reference_replace = [reference_replace]
    _validate_mapping_items(
        reference_replace,
        allowed={
            "enabled",
            "reference_parquet",
            "source_column",
            "reference_input_column",
            "reference_output_column",
            "output_column",
            "missing_policy",
            "duplicate_policy",
        },
        path="source.payload.reference_replace",
    )
    row_selection = _mapping(raw.get("row_selection"), path="row_selection", allow_missing=True)
    _reject_unknown(row_selection, {"sort_first"}, path="row_selection")
    sort_first = _mapping(
        row_selection.get("sort_first"), path="row_selection.sort_first", allow_missing=True
    )
    _reject_unknown(
        sort_first,
        {
            "enabled",
            "operation_id",
            "group_keys",
            "sort",
            "expression_ir",
            "payload",
            "tie_policy",
        },
        path="row_selection.sort_first",
    )
    if sort_first.get("group_keys") and all(
        isinstance(item, dict) for item in sort_first.get("group_keys") or []
    ):
        _validate_mapping_items(
            sort_first.get("group_keys"),
            allowed={"name", "column", "sql", "spotfire_expression"},
            path="row_selection.sort_first.group_keys",
        )
    _validate_mapping_items(
        sort_first.get("sort"),
        allowed={"column", "direction", "nulls"},
        path="row_selection.sort_first.sort",
    )
    list_restore = _mapping(raw.get("list_restore"), path="list_restore", allow_missing=True)
    _reject_unknown(
        list_restore,
        {
            "enabled",
            "lookup_path",
            "schema",
            "config",
            "batch_size",
            "drop_cache_hint",
            "print_timing",
        },
        path="list_restore",
    )
    if list_restore:
        config = _mapping(
            list_restore.get("config"), path="list_restore.config", allow_missing=True
        )
        _reject_unknown(
            config,
            {
                "key_column",
                "order_column",
                "value_columns",
                "source_coord_columns",
                "lookup_coord_columns",
            },
            path="list_restore.config",
        )
    pivot = _mapping(raw.get("pivot"), path="pivot", allow_missing=True)
    _reject_unknown(
        pivot,
        {
            "enabled",
            "row_keys",
            "column_keys",
            "value_keys",
            "value_keys_without_column",
            "column_name_rule",
            "first_duplicate_policy",
            "null_column_key_policy",
            "column_key_separator",
        },
        path="pivot",
    )
    if "enabled" in pivot:
        _require_bool(pivot["enabled"], path="pivot.enabled")
    _validate_mapping_items(
        pivot.get("value_keys"),
        allowed={"name", "source_column", "aggregation", "output_dtype", "column_name_rule"},
        path="pivot.value_keys",
    )
    _validate_mapping_items(
        pivot.get("value_keys_without_column"),
        allowed={
            "name",
            "source_column",
            "aggregation",
            "output_dtype",
            "column_name_rule",
        },
        path="pivot.value_keys_without_column",
    )
    output = _mapping(raw.get("output"), path="output")
    _reject_unknown(
        output,
        {
            "output_dir",
            "partition_column",
            "overwrite",
            "compression",
            "format",
            "sbdf",
            "physical_layout",
        },
        path="output",
    )


def _validate_0301_yaml_shape(raw: dict[str, Any]) -> None:
    _reject_unknown(
        raw,
        {
            "preset",
            "job",
            "execution",
            "left",
            "right",
            "right_sources",
            "join",
            "output",
            "__pipeline",
        },
        path="$",
    )
    _validate_common_sections(raw)
    _validate_join_source(raw.get("left"), path="left", require_name=False)
    if raw.get("right_sources") is not None and raw.get("right") is not None:
        raise ValidationError(
            "Define either right or right_sources, not both.",
            code="yaml.mutually_exclusive_keys",
            context={"paths": ["right", "right_sources"]},
        )
    if raw.get("right_sources") is None:
        _validate_join_source(raw.get("right"), path="right", require_name=False)
    else:
        sources = raw.get("right_sources")
        if not isinstance(sources, list) or not sources:
            raise ValidationError(
                "right_sources must be a non-empty list.",
                code="yaml.invalid_type",
                context={"path": "right_sources"},
            )
        for index, source in enumerate(sources):
            _validate_join_source(source, path=f"right_sources[{index}]", require_name=True)
    join = _mapping(raw.get("join"), path="join")
    _reject_unknown(
        join,
        {
            "left_on",
            "right_on",
            "how",
            "suffix",
            "left_partition_key_column",
            "right_partition_key_column",
        },
        path="join",
    )
    output = _mapping(raw.get("output"), path="output")
    _reject_unknown(
        output,
        {"output_dir", "partition_column", "overwrite", "compression", "physical_layout"},
        path="output",
    )


def _validate_common_sections(raw: dict[str, Any]) -> None:
    job = _mapping(raw.get("job"), path="job")
    _reject_unknown(job, {"name"}, path="job")
    execution = _mapping(raw.get("execution"), path="execution", allow_missing=True)
    _reject_unknown(
        execution,
        {
            "workers",
            "max_tasks_per_child",
            "target_rows_per_part",
            "target_key_groups_per_part",
            "memory",
            "max_source_files_per_task",
            "max_source_row_groups_per_task",
            "optimizer_enabled",
            "output_row_group_rows",
            "reset_before_run",
            "test_run",
            "sidecar_workers",
            "sidecar_worker_recycle_mode",
            "sidecar_max_source_files",
            "sidecar_max_projected_bytes_mb",
        },
        path="execution",
    )
    test_run = execution.get("test_run")
    if test_run is not None:
        test_run = _mapping(test_run, path="execution.test_run")
        _reject_unknown(
            test_run,
            {"final_task_limit"},
            path="execution.test_run",
        )
        limit = test_run.get("final_task_limit")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValidationError(
                "execution.test_run.final_task_limit must be an integer >= 1.",
                code="yaml.invalid_type",
                context={"path": "execution.test_run.final_task_limit", "value": limit},
            )


def _validate_upstream(value: Any, *, path: str) -> None:
    upstream = _mapping(value, path=path)
    _reject_unknown(
        upstream,
        {"paths", "recursive", "metadata_paths", "probe_manifest", "remote"},
        path=path,
    )
    remote = _mapping(upstream.get("remote"), path=f"{path}.remote", allow_missing=True)
    _reject_unknown(
        remote,
        {"target", "dataset_prefix", "relative_paths", "recursive"},
        path=f"{path}.remote",
    )


def _validate_join_source(value: Any, *, path: str, require_name: bool) -> None:
    source = _mapping(value, path=path)
    allowed = {"upstream", "columns", "join", "suffix", "keyspace"}
    if require_name:
        allowed.add("name")
    _reject_unknown(source, allowed, path=path)
    _validate_upstream(source.get("upstream"), path=f"{path}.upstream")
    columns = _mapping(source.get("columns"), path=f"{path}.columns", allow_missing=True)
    _reject_unknown(
        columns,
        {"include", "exclude", "regex", "exclude_regex"},
        path=f"{path}.columns",
    )
    source_join = _mapping(source.get("join"), path=f"{path}.join", allow_missing=True)
    _reject_unknown(
        source_join,
        {"left_on", "right_on", "how"},
        path=f"{path}.join",
    )


def _validate_mapping_items(value: Any, *, allowed: set[str], path: str) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise ValidationError(
            f"{path} must be a list.",
            code="yaml.invalid_type",
            context={"path": path, "expected": "list"},
        )
    for index, item in enumerate(value):
        mapping = _mapping(item, path=f"{path}[{index}]")
        _reject_unknown(mapping, allowed, path=f"{path}[{index}]")


def _reject_unknown(mapping: dict[str, Any], allowed: set[str], *, path: str) -> None:
    unknown = sorted(set(mapping).difference(allowed))
    if unknown:
        raise ValidationError(
            f"Unknown YAML key(s) at {path}: {unknown}",
            code="yaml.unknown_key",
            context={"path": path, "unknown_keys": unknown},
        )


def _join_type(value: Any, *, path: str) -> str:
    how = str(value).strip().lower()
    if how not in JOIN_TYPES:
        raise ValidationError(
            f"{path} must be one of {sorted(JOIN_TYPES)}.",
            code="join.invalid_type",
            context={"path": path, "value": how, "allowed": sorted(JOIN_TYPES)},
        )
    return how


def _join_keys(
    join: dict[str, Any],
    *,
    how: str,
    path: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    left_value = join.get("left_on")
    right_value = join.get("right_on")
    if how == "cross":
        if left_value or right_value:
            raise ValidationError(
                "Cross join must not define left_on or right_on.",
                code="join.cross_keys_forbidden",
                context={"path": path},
            )
        return (), ()
    left_on = _string_tuple(left_value, path=f"{path}.left_on")
    right_on = _string_tuple(right_value, path=f"{path}.right_on")
    if len(left_on) != len(right_on):
        raise ValidationError(
            "left_on and right_on must have the same length.",
            code="join.key_length_mismatch",
            context={"path": path, "left_count": len(left_on), "right_count": len(right_on)},
        )
    return left_on, right_on


def _validate_pivot_value_keys(
    value_keys: list[Any],
    *,
    path: str,
    has_column: bool,
) -> None:
    for index, item in enumerate(value_keys):
        value = _mapping(item, path=f"{path}[{index}]")
        source_column = str(value.get("source_column") or "").strip()
        aggregation = str(value.get("aggregation") or "").strip().lower()
        if not source_column:
            raise ValidationError(
                "Pivot value key requires source_column.",
                code="pivot.missing_source_column",
                context={"path": f"{path}[{index}].source_column"},
            )
        if aggregation not in PIVOT_AGGREGATIONS:
            raise ValidationError(
                f"Unsupported pivot aggregation: {aggregation}",
                code="pivot.unsupported_aggregation",
                context={"path": f"{path}[{index}].aggregation", "value": aggregation},
            )
        output_dtype = str(value.get("output_dtype") or "").strip().upper()
        if output_dtype and output_dtype not in PIVOT_OUTPUT_DTYPES:
            raise ValidationError(
                f"Unsupported pivot output dtype: {output_dtype}",
                code="pivot.unsupported_output_dtype",
                context={"path": f"{path}[{index}].output_dtype", "value": output_dtype},
            )
        rule = str(value.get("column_name_rule") or "")
        if not has_column and ("<column_key_value>" in rule or "{column_key_value}" in rule):
            raise ValidationError(
                "Pivot value without column cannot reference column_key_value.",
                code="pivot.invalid_column_name_rule",
                context={"path": f"{path}[{index}].column_name_rule"},
            )


def _require_bool(value: Any, *, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(
            f"{path} must be a boolean.",
            code="yaml.invalid_type",
            context={"path": path, "expected": "boolean"},
        )
    return value
