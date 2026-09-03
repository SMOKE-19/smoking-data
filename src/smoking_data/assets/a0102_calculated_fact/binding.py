from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Mapping

from smoking_data.core.exceptions import ValidationError

from .external_files import load_column_alias_registry, project_rows
from .planner import expression_column_references
from .spec import CalculatedFactSpec, LookupFileSpec


@dataclass(frozen=True, slots=True)
class DependencyBinding:
    logical_name: str
    kind: str
    physical_name: str
    lookup_alias: str | None = None


@dataclass(frozen=True, slots=True)
class LookupValidationStats:
    alias: str
    row_count: int
    non_null_key_count: int
    duplicate_group_count: int


@dataclass(frozen=True, slots=True)
class ExpressionSkip:
    expression_name: str
    status: str
    missing_dependencies: tuple[dict[str, str], ...]


@dataclass(frozen=True, slots=True)
class BindingPlan:
    bindings: tuple[DependencyBinding, ...]
    source_projection: tuple[str, ...]
    lookup_projection: dict[str, tuple[str, ...]]
    lookup_stats: tuple[LookupValidationStats, ...]
    binding_hash: str
    skipped_expressions: tuple[ExpressionSkip, ...] = ()


def build_binding_plan(
    spec: CalculatedFactSpec,
    ir_document: Mapping[str, Any],
    *,
    source_dtypes: Mapping[str, str],
) -> BindingPlan:
    alias_registry = load_column_alias_registry(spec.column_alias_files)
    for item in spec.expand_columns:
        if item.target == item.source:
            continue
        existing = alias_registry.get(item.target)
        if existing is not None and existing != item.source:
            _fail(
                "list.duplicate_binding",
                "List element alias conflicts with a column alias binding.",
                alias=item.target,
                sources=sorted({existing, item.source}),
            )
        alias_registry[item.target] = item.source
    _validate_alias_registry(alias_registry, source_dtypes, allow_missing=True)
    lookup_schemas = {item.alias: external_schema(item) for item in spec.lookup_files}
    expression_outputs = _expression_outputs(ir_document)
    available_outputs: set[str] = set()
    bindings: list[DependencyBinding] = []
    binding_by_name: dict[str, DependencyBinding] = {}
    lookup_values: dict[str, set[str]] = {item.alias: set() for item in spec.lookup_files}
    skipped: list[ExpressionSkip] = []
    skipped_names: set[str] = set()
    active_names: set[str] = set()

    for expression in _expressions(ir_document):
        expression_name = str(expression.get("name") or "")
        node = expression.get("expr")
        if not isinstance(node, Mapping):
            _fail("expression.invalid_ir", "Expression node must be a mapping.")
        missing: list[dict[str, str]] = []
        expression_bindings: list[tuple[str, DependencyBinding]] = []
        expression_lookup_values: list[tuple[str, str]] = []
        for logical_name in expression_column_references(node):
            if logical_name in skipped_names:
                missing.append(
                    {
                        "logical_name": logical_name,
                        "physical_column": logical_name,
                        "kind": "expression",
                        "reason": "upstream_expression_skipped",
                    }
                )
                continue
            if logical_name in available_outputs:
                binding = DependencyBinding(logical_name, "expression", logical_name)
            else:
                binding, issue = _bind_leaf_or_missing(
                    logical_name,
                    source_dtypes=source_dtypes,
                    alias_registry=alias_registry,
                    lookup_schemas=lookup_schemas,
                    expression_outputs=expression_outputs,
                )
                if issue is not None:
                    missing.append(issue)
                    continue
                assert binding is not None
                if binding.kind in {"source", "virtual_alias"} and binding.physical_name not in source_dtypes:
                    missing.append(
                        {
                            "logical_name": logical_name,
                            "physical_column": binding.physical_name,
                            "kind": binding.kind,
                            "reason": "source_column_missing",
                        }
                    )
                    continue
                if binding.kind == "lookup":
                    assert binding.lookup_alias is not None
                    lookup = next(item for item in spec.lookup_files if item.alias == binding.lookup_alias)
                    missing_keys = [key for key in lookup.lookup_keys if key not in lookup_schemas[lookup.alias]]
                    missing_source_keys = [
                        key
                        for key in lookup.source_keys
                        if _source_key_missing(key, source_dtypes, alias_registry)
                    ]
                    if missing_keys or missing_source_keys:
                        missing.append(
                            {
                                "logical_name": logical_name,
                                "physical_column": binding.physical_name,
                                "kind": "lookup",
                                "reason": "lookup_key_missing",
                                "missing_lookup_keys": ",".join(missing_keys),
                                "missing_source_keys": ",".join(missing_source_keys),
                            }
                        )
                        continue
            expression_bindings.append((logical_name, binding))
            existing = binding_by_name.get(logical_name)
            if existing is not None and existing != binding:
                _fail(
                    "expression.ambiguous_dependency",
                    "One logical dependency resolved to different physical inputs.",
                    dependency=logical_name,
                )
            if binding.kind == "lookup":
                assert binding.lookup_alias is not None
                expression_lookup_values.append((binding.lookup_alias, binding.physical_name))
        if missing:
            status = (
                "skipped_upstream_expression"
                if any(item["kind"] == "expression" for item in missing)
                else "skipped_missing_dependency"
            )
            skipped.append(
                ExpressionSkip(
                    expression_name=expression_name,
                    status=status,
                    missing_dependencies=tuple(missing),
                )
            )
            skipped_names.add(expression_name)
            continue
        for logical_name, binding in expression_bindings:
            existing = binding_by_name.get(logical_name)
            if existing is None:
                binding_by_name[logical_name] = binding
                bindings.append(binding)
        for alias, physical_name in expression_lookup_values:
            lookup_values[alias].add(physical_name)
        active_names.add(expression_name)
        available_outputs.add(str(expression.get("name") or ""))

    source_projection = list(
        dict.fromkeys(
            (*spec.identity_columns, *spec.partition_by, *spec.include_columns)
        )
    )
    for binding in bindings:
        if binding.kind in {"source", "virtual_alias"}:
            source_projection.append(binding.physical_name)
    for lookup in spec.lookup_files:
        if not lookup_values[lookup.alias]:
            continue
        for source_key in lookup.source_keys:
            source_projection.append(
                _bind_source_key(source_key, source_dtypes, alias_registry).physical_name
            )
    source_projection.extend(
        item.source for item in spec.expand_columns if item.source in source_dtypes
    )
    source_projection = list(dict.fromkeys(source_projection))
    missing_source = [
        name
        for name in source_projection
        if name not in source_dtypes
        and name
        in set(spec.identity_columns)
        .union(spec.partition_by)
        .union(spec.include_columns)
    ]
    if missing_source:
        _fail(
            "expression.unresolved_dependency",
            "Projected physical source columns do not exist.",
            columns=missing_source,
        )

    lookup_projection: dict[str, tuple[str, ...]] = {}
    lookup_stats: list[LookupValidationStats] = []
    for lookup in spec.lookup_files:
        if not lookup_values[lookup.alias]:
            continue
        schema = lookup_schemas[lookup.alias]
        missing_keys = [name for name in lookup.lookup_keys if name not in schema]
        if missing_keys:
            _fail(
                "lookup.invalid_key_mapping",
                "Lookup key columns do not exist.",
                lookup_alias=lookup.alias,
                columns=missing_keys,
            )
        _validate_lookup_key_types(lookup, source_dtypes, alias_registry, schema)
        projection = tuple(dict.fromkeys((*lookup.lookup_keys, *sorted(lookup_values[lookup.alias]))))
        lookup_projection[lookup.alias] = projection
        lookup_stats.append(validate_lookup_key_uniqueness(lookup))

    payload = {
        "bindings": [
            {
                "logical_name": item.logical_name,
                "kind": item.kind,
                "physical_name": item.physical_name,
                "lookup_alias": item.lookup_alias,
            }
            for item in bindings
        ],
        "source_projection": source_projection,
        "lookup_projection": lookup_projection,
        "contract_version": "smoking-data.binding-plan.v1",
    }
    binding_hash = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return BindingPlan(
        bindings=tuple(bindings),
        source_projection=tuple(source_projection),
        lookup_projection=lookup_projection,
        lookup_stats=tuple(lookup_stats),
        binding_hash=binding_hash,
        skipped_expressions=tuple(skipped),
    )


def external_schema(file: LookupFileSpec) -> dict[str, str]:
    try:
        if file.path.suffix.lower() == ".parquet":
            import pyarrow.parquet as pq

            schema = pq.ParquetFile(file.path).schema_arrow
        else:
            import pyarrow.csv as pcsv

            schema = pcsv.open_csv(file.path).schema
    except Exception as exc:
        _fail(
            "external_file.format_mismatch",
            "Lookup schema could not be inspected.",
            lookup_alias=file.alias,
            path=str(file.path),
            reason=str(exc),
        )
    return {field.name: str(field.type) for field in schema}


def validate_lookup_key_uniqueness(file: LookupFileSpec) -> LookupValidationStats:
    connection = sqlite3.connect("")
    try:
        quoted_columns = [f'"key_{index}" TEXT NOT NULL' for index in range(len(file.lookup_keys))]
        connection.execute(f"CREATE TABLE lookup_keys ({', '.join(quoted_columns)})")
        placeholders = ", ".join("?" for _ in file.lookup_keys)
        row_count = 0
        non_null_count = 0
        buffer: list[tuple[str, ...]] = []
        for row in project_rows(file.path, file.lookup_keys):
            row_count += 1
            values = tuple(row.get(key) for key in file.lookup_keys)
            if any(value is None for value in values):
                continue
            non_null_count += 1
            buffer.append(tuple(_stable_key(value) for value in values))
            if len(buffer) >= 10_000:
                connection.executemany(
                    f"INSERT INTO lookup_keys VALUES ({placeholders})", buffer
                )
                buffer.clear()
        if buffer:
            connection.executemany(f"INSERT INTO lookup_keys VALUES ({placeholders})", buffer)
        group_columns = ", ".join(f'"key_{index}"' for index in range(len(file.lookup_keys)))
        duplicate_rows = connection.execute(
            f"SELECT {group_columns}, COUNT(*) AS duplicate_count "
            f"FROM lookup_keys GROUP BY {group_columns} HAVING COUNT(*) > 1 "
            "ORDER BY duplicate_count DESC LIMIT 6"
        ).fetchall()
        duplicate_group_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM ("
                f"SELECT 1 FROM lookup_keys GROUP BY {group_columns} HAVING COUNT(*) > 1)"
            ).fetchone()[0]
        )
        if duplicate_group_count:
            _fail(
                "lookup.non_unique_keys",
                "Lookup composite key must be unique before calculation.",
                lookup_alias=file.alias,
                key_columns=list(file.lookup_keys),
                duplicate_group_count=duplicate_group_count,
                samples=[
                    {"key": list(row[:-1]), "row_count": row[-1]} for row in duplicate_rows[:5]
                ],
            )
        return LookupValidationStats(
            alias=file.alias,
            row_count=row_count,
            non_null_key_count=non_null_count,
            duplicate_group_count=0,
        )
    finally:
        connection.close()


def _bind_leaf(
    logical_name: str,
    *,
    source_dtypes: Mapping[str, str],
    alias_registry: Mapping[str, str],
    lookup_schemas: Mapping[str, Mapping[str, str]],
    expression_outputs: set[str],
) -> DependencyBinding:
    if logical_name in source_dtypes:
        return DependencyBinding(logical_name, "source", logical_name)
    if logical_name in alias_registry:
        return DependencyBinding(logical_name, "virtual_alias", alias_registry[logical_name])
    if "." in logical_name:
        alias, column = logical_name.split(".", 1)
        if alias in lookup_schemas:
            if column not in lookup_schemas[alias]:
                _fail(
                    "expression.unresolved_dependency",
                    "Qualified Lookup column does not exist.",
                    dependency=logical_name,
                )
            return DependencyBinding(logical_name, "lookup", column, alias)
    lookup_candidates = [
        alias for alias, schema in lookup_schemas.items() if logical_name in schema
    ]
    if len(lookup_candidates) == 1:
        return DependencyBinding(logical_name, "lookup", logical_name, lookup_candidates[0])
    if len(lookup_candidates) > 1:
        _fail(
            "expression.ambiguous_dependency",
            "Unqualified Lookup dependency exists in multiple Lookup files.",
            dependency=logical_name,
            lookup_aliases=lookup_candidates,
        )
    if logical_name in expression_outputs:
        _fail(
            "expression.dependency_cycle",
            "Expression references a result that is not available in its prior layer.",
            dependency=logical_name,
        )
    _fail(
        "expression.unresolved_dependency",
        "Dependency does not exist in source, aliases, or Lookup files.",
        dependency=logical_name,
    )


def _bind_leaf_or_missing(
    logical_name: str,
    *,
    source_dtypes: Mapping[str, str],
    alias_registry: Mapping[str, str],
    lookup_schemas: Mapping[str, Mapping[str, str]],
    expression_outputs: set[str],
) -> tuple[DependencyBinding | None, dict[str, str] | None]:
    if logical_name in source_dtypes:
        return DependencyBinding(logical_name, "source", logical_name), None
    if logical_name in alias_registry:
        return DependencyBinding(logical_name, "virtual_alias", alias_registry[logical_name]), None
    if "." in logical_name:
        alias, column = logical_name.split(".", 1)
        if alias in lookup_schemas:
            if column not in lookup_schemas[alias]:
                return None, {
                    "logical_name": logical_name,
                    "physical_column": column,
                    "kind": "lookup",
                    "reason": "lookup_column_missing",
                }
            return DependencyBinding(logical_name, "lookup", column, alias), None
    lookup_candidates = [
        alias for alias, schema in lookup_schemas.items() if logical_name in schema
    ]
    if len(lookup_candidates) == 1:
        alias = lookup_candidates[0]
        return DependencyBinding(logical_name, "lookup", logical_name, alias), None
    if len(lookup_candidates) > 1:
        _fail(
            "expression.ambiguous_dependency",
            "Unqualified Lookup dependency exists in multiple Lookup files.",
            dependency=logical_name,
            lookup_aliases=lookup_candidates,
        )
    if logical_name in expression_outputs:
        _fail(
            "expression.dependency_cycle",
            "Expression references a result that is not available in its prior layer.",
            dependency=logical_name,
        )
    return None, {
        "logical_name": logical_name,
        "physical_column": logical_name,
        "kind": "source",
        "reason": "source_column_missing",
    }


def _bind_source_key(
    logical_name: str,
    source_dtypes: Mapping[str, str],
    alias_registry: Mapping[str, str],
) -> DependencyBinding:
    if logical_name in source_dtypes:
        return DependencyBinding(logical_name, "source", logical_name)
    if logical_name in alias_registry:
        return DependencyBinding(logical_name, "virtual_alias", alias_registry[logical_name])
    _fail(
        "lookup.invalid_key_mapping",
        "Lookup source key does not bind to a physical or virtual source column.",
        source_key=logical_name,
    )


def _validate_alias_registry(
    registry: Mapping[str, str], source_dtypes: Mapping[str, str], *, allow_missing: bool = False
) -> None:
    missing = sorted({source for source in registry.values() if source not in source_dtypes})
    if missing and not allow_missing:
        _fail(
            "column_alias.invalid_binding",
            "Column alias references missing physical source columns.",
            columns=missing,
        )
    collisions = sorted(set(registry).intersection(source_dtypes))
    if collisions:
        _fail(
            "column_alias.invalid_binding",
            "Virtual alias collides with a physical source column.",
            aliases=collisions,
        )


def _source_key_missing(
    logical_name: str,
    source_dtypes: Mapping[str, str],
    alias_registry: Mapping[str, str],
) -> bool:
    physical = alias_registry.get(logical_name, logical_name)
    return physical not in source_dtypes


def _validate_lookup_key_types(
    lookup: LookupFileSpec,
    source_dtypes: Mapping[str, str],
    alias_registry: Mapping[str, str],
    lookup_schema: Mapping[str, str],
) -> None:
    mismatches: list[dict[str, str]] = []
    for source_key, lookup_key in zip(lookup.source_keys, lookup.lookup_keys, strict=True):
        physical = _bind_source_key(source_key, source_dtypes, alias_registry).physical_name
        source_dtype = _normalize_dtype(source_dtypes[physical])
        lookup_dtype = _normalize_dtype(lookup_schema[lookup_key])
        if source_dtype != lookup_dtype:
            mismatches.append(
                {
                    "source_key": source_key,
                    "source_dtype": source_dtype,
                    "lookup_key": lookup_key,
                    "lookup_dtype": lookup_dtype,
                }
            )
    if mismatches:
        _fail(
            "lookup.invalid_key_mapping",
            "Lookup key dtypes must match their bound source key dtypes.",
            lookup_alias=lookup.alias,
            mismatches=mismatches,
        )


def _normalize_dtype(dtype: str) -> str:
    aliases = {
        "string": "string",
        "large_string": "string",
        "utf8": "string",
        "int64": "int64",
        "bigint": "int64",
        "int32": "int32",
        "integer": "int32",
        "double": "double",
        "float64": "double",
        "bool": "bool",
        "boolean": "bool",
    }
    normalized = str(dtype).lower().strip()
    return aliases.get(normalized, normalized)


def _expressions(ir_document: Mapping[str, Any]):
    for layer in ir_document.get("layers") or []:
        if isinstance(layer, Mapping):
            for expression in layer.get("expressions") or []:
                if isinstance(expression, Mapping):
                    yield expression


def _expression_outputs(ir_document: Mapping[str, Any]) -> set[str]:
    return {str(item.get("name") or "") for item in _expressions(ir_document)}


def _stable_key(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str, separators=(",", ":"))


def _fail(code: str, message: str, **context: Any) -> None:
    raise ValidationError(message, code=code, context=context)
