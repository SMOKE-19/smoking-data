from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from smoking_data.core.exceptions import ValidationError
from smoking_data.ops.upstream import discover_parquet_files
from smoking_data.runtime.config import load_config
from smoking_data.runtime.paths import resolve_project_path
from smoking_data.runtime.template_resolution import resolve_contract_templates
from smoking_data.runtime.yaml_loader import load_pipeline_spec

from .binding import BindingPlan, ExpressionSkip, build_binding_plan, external_schema
from .external_files import compile_expression_file
from .fingerprint import (
    ExpressionFingerprintSpec,
    derive_expression_fingerprint_specs,
)
from .planner import ExpressionExecutionPlan, plan_expression_execution
from .spec import CalculatedFactSpec

CALCULATED_FACT_PLAN_VERSION = "smoking-data.calculated-fact-plan.v1"


@dataclass(frozen=True, slots=True)
class CalculatedFactRunPlan:
    spec: CalculatedFactSpec
    upstream_files: tuple[Path, ...]
    source_schema: pa.Schema
    expression_ir: dict[str, Any]
    binding: BindingPlan
    expressions: tuple[ExpressionExecutionPlan, ...]
    fingerprints: tuple[ExpressionFingerprintSpec, ...]
    fact_source_names: dict[str, str]
    output_root: Path
    compression: str
    output_row_group_rows: int | None
    plan_hash: str
    source_schema_hash: str
    skipped_expressions: tuple[ExpressionSkip, ...] = ()


def preflight_calculated_fact_yaml(
    definition_path: str | Path,
    *,
    config_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> CalculatedFactRunPlan:
    from .spec import load_calculated_fact_spec

    config = load_config(
        config_path=config_path,
        project_root=project_root,
        asset_code="0102",
    )
    definition = resolve_project_path(definition_path, project_root=config.project_root)
    spec = load_calculated_fact_spec(definition)
    upstream_config = load_config(
        project_root=config.project_root,
        asset_code=spec.upstream_asset_code,
    )
    upstream = load_pipeline_spec(spec.upstream_definition, config=upstream_config)
    upstream_root = _artifact_root(
        upstream.raw.get("output"),
        owner=spec.upstream_definition,
        project_root=config.project_root,
    )
    upstream_files = [item.path for item in discover_parquet_files([upstream_root])]
    resolved_output = resolve_contract_templates(
        spec.output,
        scope={
            **config.template_scope(),
            "asset_code": "0102",
            "job_name": spec.job_name,
        },
        source=definition,
    )
    output_root = _artifact_root(
        resolved_output,
        owner=definition,
        project_root=config.project_root,
    )
    return build_calculated_fact_plan(
        spec,
        upstream_files=upstream_files,
        output_root=output_root,
    )


def build_calculated_fact_plan(
    spec: CalculatedFactSpec,
    *,
    upstream_files: list[Path] | tuple[Path, ...],
    output_root: str | Path,
) -> CalculatedFactRunPlan:
    files = tuple(sorted((Path(item).expanduser().resolve() for item in upstream_files), key=str))
    if not files or any(not item.is_file() for item in files):
        _fail(
            "upstream.dataset_empty",
            "0102 upstream must contain at least one existing Parquet file.",
            files=[str(item) for item in files],
        )
    if any(item.suffix.lower() != ".parquet" for item in files):
        _fail(
            "upstream.unsupported_format",
            "0102 upstream files must all be Parquet.",
        )
    try:
        source_schema = _unify_source_schemas(files)
    except Exception as exc:
        _fail(
            "upstream.schema_mismatch",
            "0102 upstream Parquet schemas cannot be unified.",
            reason=str(exc),
        )
    _validate_expand_list_types(spec, source_schema)
    source_dtypes = {field.name: str(field.type) for field in source_schema}
    expression_ir = compile_expression_file(spec.expression_file)
    binding = build_binding_plan(
        spec,
        expression_ir,
        source_dtypes=source_dtypes,
    )
    skipped_names = {item.expression_name for item in binding.skipped_expressions}
    active_expression_ir = _filter_expression_ir(expression_ir, skipped_names)
    logical_dtypes = dict(source_dtypes)
    lookup_schemas = {item.alias: external_schema(item) for item in spec.lookup_files}
    for item in binding.bindings:
        if item.kind in {"source", "virtual_alias"}:
            logical_dtypes[item.logical_name] = source_dtypes[item.physical_name]
        elif item.kind == "lookup" and item.lookup_alias is not None:
            logical_dtypes[item.logical_name] = lookup_schemas[item.lookup_alias][
                item.physical_name
            ]
    expressions = plan_expression_execution(
        active_expression_ir,
        source_dtypes=logical_dtypes,
        expansion_available=bool(spec.expand_columns),
    )
    raw_fingerprints = derive_expression_fingerprint_specs(spec, active_expression_ir, binding)
    active_expression_names = {item.name for item in expressions}
    compact_names = {
        item.source: item.target
        for item in spec.compact_columns
        if item.source in active_expression_names
    }
    unknown_compact_sources = sorted(set(compact_names).difference(item.name for item in expressions))
    if unknown_compact_sources:
        _fail(
            "list.compaction_source_missing",
            "compact_lists references expression outputs that do not exist.",
            columns=unknown_compact_sources,
        )
    fact_source_names = {
        compact_names.get(item.name, item.name): item.name for item in raw_fingerprints
    }
    if len(fact_source_names) != len(raw_fingerprints):
        _fail(
            "list.duplicate_binding",
            "Compacted output names collide with calculated expression names.",
        )
    fingerprints = tuple(
        ExpressionFingerprintSpec(
            name=compact_names.get(item.name, item.name),
            expression_hash=item.expression_hash,
            binding_hash=item.binding_hash,
            source_columns=item.source_columns,
            constants=item.constants,
        )
        for item in raw_fingerprints
    )
    artifact = spec.output.get("artifact")
    if not isinstance(artifact, dict):
        _fail("asset.invalid_output_contract", "0102 output.artifact must be a mapping.")
    if artifact.get("type") != "calculated_fact_dataset" or artifact.get("format") != "parquet":
        _fail(
            "asset.invalid_output_contract",
            "0102 output must be a calculated_fact_dataset in Parquet format.",
        )
    if artifact.get("write_policy") != "append_generation":
        _fail(
            "asset.invalid_output_contract",
            "0102 output.write_policy must be append_generation.",
        )
    compression = str(artifact.get("compression") or "zstd").lower()
    if compression != "zstd":
        _fail(
            "asset.invalid_output_contract",
            "0102 published generations use fixed zstd compression.",
        )
    physical_layout = artifact.get("physical_layout") or {}
    if not isinstance(physical_layout, dict):
        _fail(
            "asset.invalid_output_contract",
            "0102 output.artifact.physical_layout must be a mapping.",
        )
    row_group_value = physical_layout.get("row_group_rows", "auto")
    output_row_group_rows = None if row_group_value == "auto" else int(row_group_value)
    if output_row_group_rows is not None and output_row_group_rows < 1:
        _fail(
            "asset.invalid_output_contract",
            "0102 row_group_rows must be auto or a positive integer.",
        )
    resolved_output = Path(output_root).expanduser().resolve()
    payload = {
        "version": CALCULATED_FACT_PLAN_VERSION,
        "definition_hash": spec.canonical_hash,
        "upstream_files": [
            {"path": str(item), "size": item.stat().st_size} for item in files
        ],
        "source_schema": str(source_schema),
        "binding_hash": binding.binding_hash,
        "expressions": [
            {
                "name": item.name,
                "strategy": item.strategy.value,
                "output_dtype": item.output_dtype,
                "dependencies": item.dependencies,
            }
            for item in expressions
        ],
        "fingerprints": [
            {
                "name": item.name,
                "expression_hash": item.expression_hash,
                "binding_hash": item.binding_hash,
                "source_columns": item.source_columns,
                "constants": item.constants,
            }
            for item in fingerprints
        ],
        "output": {
            "root": str(resolved_output),
            "compression": compression,
            "row_group_rows": output_row_group_rows,
        },
    }
    plan_hash = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return CalculatedFactRunPlan(
        spec=spec,
        upstream_files=files,
        source_schema=source_schema,
        expression_ir=active_expression_ir,
        binding=binding,
        expressions=expressions,
        fingerprints=fingerprints,
        fact_source_names=fact_source_names,
        output_root=resolved_output,
        compression=compression,
        output_row_group_rows=output_row_group_rows,
        plan_hash=plan_hash,
        source_schema_hash=hashlib.sha256(str(source_schema).encode()).hexdigest(),
        skipped_expressions=binding.skipped_expressions,
    )


def _fail(code: str, message: str, **context: object) -> None:
    raise ValidationError(message, code=code, context=context)


def _validate_expand_list_types(spec: CalculatedFactSpec, schema: pa.Schema) -> None:
    for item in spec.expand_columns:
        index = schema.get_field_index(item.source)
        if index < 0:
            continue
        dtype = schema.field(index).type
        if not pa.types.is_list(dtype):
            _fail(
                "list.unsupported_source_type",
                "expand_lists source must be an Arrow List column.",
                column=item.source,
                dtype=str(dtype),
            )
        if pa.types.is_list(dtype.value_type) or pa.types.is_large_list(dtype.value_type):
            _fail(
                "list.unsupported_nested_type",
                "Nested List columns are not supported by expand_lists.",
                column=item.source,
                dtype=str(dtype),
            )


def _filter_expression_ir(
    document: dict[str, Any], skipped_names: set[str]
) -> dict[str, Any]:
    if not skipped_names:
        return document
    layers: list[dict[str, Any]] = []
    for layer in document.get("layers") or []:
        if not isinstance(layer, dict):
            continue
        expressions = [
            item
            for item in layer.get("expressions") or []
            if isinstance(item, dict) and str(item.get("name") or "") not in skipped_names
        ]
        if expressions:
            layers.append({**layer, "expressions": expressions})
    return {**document, "layers": layers}


def _unify_source_schemas(files: tuple[Path, ...]) -> pa.Schema:
    try:
        schemas = [pq.ParquetFile(path).schema_arrow for path in files]
        return pa.unify_schemas(schemas)
    except Exception as exc:
        _fail(
            "upstream.schema_mismatch",
            "0102 upstream Parquet schemas cannot be unified.",
            reason=str(exc),
        )
def _artifact_root(output: Any, *, owner: Path, project_root: Path) -> Path:
    artifact = output.get("artifact") if isinstance(output, dict) else None
    root = artifact.get("root_dir") if isinstance(artifact, dict) else None
    if not isinstance(root, str) or not root.strip():
        _fail(
            "asset.invalid_output_contract",
            "Asset output.artifact.root_dir must resolve to a non-empty path.",
            definition=str(owner),
        )
    path = Path(root).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()
