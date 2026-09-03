from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from smoking_data.core.exceptions import ValidationError
from smoking_data.core.pipeline import (
    EXECUTION_KEYS,
    SUPPORTED_PIPELINE_SCHEMA_VERSIONS,
    PipelineSpec,
    compile_operations,
    parse_sinks,
    parse_sources,
    validate_pipeline_document,
)
from smoking_data.core.pipeline_dag import (
    PIPELINE_ASSET_CODES,
    PIPELINE_SCHEMA_VERSION_BY_ASSET,
    normalize_pipeline_document,
)
from smoking_data.runtime.asset_config import deep_merge, load_effective_asset_config
from smoking_data.runtime.asset_contract import load_effective_asset_contract
from smoking_data.runtime.config import RuntimeConfig
from smoking_data.runtime.paths import file_sha256, resolve_project_path
from smoking_data.runtime.template_resolution import resolve_contract_templates


@dataclass(frozen=True, slots=True)
class PresetSpec:
    """Internal lowered-kernel contract, not a public YAML schema."""

    preset: str
    job_name: str
    yaml_path: Path
    raw: dict[str, Any]
    yaml_hash: str


def load_pipeline_spec(yaml_path: str | Path, *, config: RuntimeConfig) -> PipelineSpec:
    path = resolve_project_path(yaml_path, project_root=config.project_root)
    if not path.exists():
        raise ValidationError(f"YAML file does not exist: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValidationError(f"YAML root must be a mapping: {path}")
    yaml_header = payload.get("yaml")
    asset_code = (
        str(yaml_header.get("asset_code") or "") if isinstance(yaml_header, dict) else ""
    )
    if asset_code in PIPELINE_ASSET_CODES:
        asset_defaults = load_effective_asset_config(config.project_root, asset_code).payload
        asset_contract = load_effective_asset_contract(config.project_root, asset_code)
        # Asset config also contains engine-only tuning values. Only fields that
        # belong to the public Definition contract participate in this merge.
        configured_execution = asset_defaults.get("execution")
        contract_defaults: dict[str, Any] = dict(asset_contract.definition)
        if isinstance(configured_execution, dict):
            contract_defaults["execution"] = {
                key: value
                for key, value in configured_execution.items()
                if key in EXECUTION_KEYS and key != "memory"
            }
        payload = deep_merge(contract_defaults, payload)
        output = payload.get("output")
        if isinstance(output, dict):
            job = payload.get("job")
            job_name = str(job.get("name") or "") if isinstance(job, dict) else ""
            payload["output"] = resolve_contract_templates(
                output,
                scope={
                    **config.template_scope(),
                    "asset_code": asset_code,
                    "job_name": job_name,
                },
                source=path,
            )
    validate_pipeline_document(payload)
    yaml_header = payload["yaml"]
    asset_code = str(yaml_header["asset_code"])
    suffix_code = _definition_asset_code(path)
    if suffix_code is not None and suffix_code != asset_code:
        raise ValidationError(
            "YAML filename suffix and yaml.asset_code do not match.",
            code="yaml.asset_code_mismatch",
            context={"filename_asset_code": suffix_code, "yaml_asset_code": asset_code},
        )
    normalized, graph = normalize_pipeline_document(
        payload,
        asset_resolver=lambda definition, selection: _resolve_asset_source(
            definition,
            selection=selection,
            owner_path=path,
            config=config,
        ),
    )
    expression_irs = _compile_pipeline_expression_irs(normalized.get("operations"))
    logical_plan = compile_operations(normalized, expression_irs=expression_irs)
    return PipelineSpec(
        job_name=str(payload["job"]["name"]),
        yaml_path=path,
        raw=normalized,
        yaml_hash=file_sha256(path),
        sources=parse_sources(normalized),
        sinks=parse_sinks(normalized),
        execution=dict(normalized.get("execution") or {}),
        logical_plan=logical_plan,
        graph=graph,
        graph_hash=str(graph["graph_hash"]),
        asset_code=asset_code,
        schema_version=str(yaml_header["schema_version"]),
    )


def load_yaml_spec(yaml_path: str | Path, *, config: RuntimeConfig) -> PipelineSpec:
    """Load the only public pipeline YAML contract supported by smoking-data."""
    path = resolve_project_path(yaml_path, project_root=config.project_root)
    if not path.exists():
        raise ValidationError(f"YAML file does not exist: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValidationError(f"YAML root must be a mapping: {path}")
    yaml_header = payload.get("yaml")
    version = str(yaml_header.get("schema_version") or "") if isinstance(yaml_header, dict) else ""
    if version not in SUPPORTED_PIPELINE_SCHEMA_VERSIONS:
        raise ValidationError(
            "Unsupported YAML contract; use a supported smoking-data pipeline schema.",
            code="yaml.unsupported_schema_version",
            context={
                "expected": sorted(SUPPORTED_PIPELINE_SCHEMA_VERSIONS),
                "actual": version or None,
            },
        )
    return load_pipeline_spec(path, config=config)


def _definition_asset_code(path: Path) -> str | None:
    parts = path.name.split(".")
    if len(parts) < 3:
        return None
    candidate = parts[-2]
    return candidate if candidate in PIPELINE_ASSET_CODES else None


def _resolve_asset_source(
    definition: str,
    *,
    selection: dict[str, Any] | None,
    owner_path: Path,
    config: RuntimeConfig,
) -> dict[str, Any]:
    definition_path = Path(definition).expanduser()
    if not definition_path.is_absolute():
        definition_path = owner_path.parent / definition_path
    definition_path = definition_path.resolve()
    if not definition_path.is_file():
        raise ValidationError(
            f"define_asset definition does not exist: {definition_path}",
            code="asset.definition_missing",
            context={"definition": str(definition_path), "owner": str(owner_path)},
        )
    payload = yaml.safe_load(definition_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValidationError(
            "define_asset definition root must be a mapping.",
            code="yaml.invalid_type",
            context={"definition": str(definition_path)},
        )
    yaml_header = payload.get("yaml")
    if not isinstance(yaml_header, dict):
        raise ValidationError(
            "define_asset definition must declare yaml header.",
            code="yaml.required_key",
            context={"definition": str(definition_path), "path": "yaml"},
        )
    asset_code = str(yaml_header.get("asset_code") or "")
    if asset_code == "0101":
        from smoking_data.assets.a0101_source.pipeline.spec import load_source_spec

        source_spec = load_source_spec(definition_path)
        root_dir = Path(source_spec.storage.raw_dir).resolve()
    elif asset_code == "0103":
        from smoking_data.assets.a0103_csv_source import load_csv_source_spec

        source_spec = load_csv_source_spec(definition_path, project_root=config.project_root)
        root_dir = source_spec.output_root.resolve()
    elif asset_code in PIPELINE_ASSET_CODES:
        expected_version = PIPELINE_SCHEMA_VERSION_BY_ASSET[asset_code]
        if yaml_header.get("schema_version") != expected_version:
            raise ValidationError(
                "define_asset pipeline definition has an unsupported schema version.",
                code="yaml.unsupported_schema_version",
                context={"definition": str(definition_path)},
            )
        asset_defaults = load_effective_asset_config(config.project_root, asset_code).payload
        asset_contract = load_effective_asset_contract(config.project_root, asset_code)
        effective = deep_merge(asset_contract.definition, asset_defaults)
        effective = deep_merge(effective, payload)
        job = effective.get("job")
        job_name = str(job.get("name") or "") if isinstance(job, dict) else ""
        output = effective.get("output")
        if not isinstance(output, dict):
            raise ValidationError(
                "define_asset definition must resolve output contract.",
                code="asset.invalid_output_contract",
                context={"definition": str(definition_path)},
            )
        resolved_output = resolve_contract_templates(
            output,
            scope={
                **config.template_scope(),
                "asset_code": asset_code,
                "job_name": job_name,
            },
            source=definition_path,
        )
        artifact = resolved_output.get("artifact")
        root_value = artifact.get("root_dir") if isinstance(artifact, dict) else None
        if not root_value or not isinstance(artifact, dict) or artifact.get("format") != "parquet":
            raise ValidationError(
                "define_asset requires a parquet output.artifact.root_dir.",
                code="asset.invalid_output_contract",
                context={"definition": str(definition_path)},
            )
        root_dir = resolve_project_path(str(root_value), project_root=config.project_root)
    else:
        raise ValidationError(
            "define_asset supports data-producing Asset codes 0101, 0103, 0201, 0301, and 0401.",
            code="asset.unsupported_upstream",
            context={"definition": str(definition_path), "asset_code": asset_code or None},
        )
    selected_paths = _select_asset_dataset_paths(
        root_dir,
        selection=selection,
        asset_code=asset_code,
    )
    return {
        "paths": [str(item) for item in selected_paths],
        "asset_definition": str(definition_path),
        "asset_definition_hash": file_sha256(definition_path),
        "asset_code": asset_code,
    }


def _select_asset_dataset_paths(
    root_dir: Path,
    *,
    selection: dict[str, Any] | None,
    asset_code: str,
) -> list[Path]:
    if not selection:
        return [root_dir]
    if not isinstance(selection, dict) or set(selection) != {"labels"}:
        raise ValidationError(
            "define_upstream.select supports labels only.",
            code="asset.invalid_dataset_selection",
        )
    labels = selection.get("labels")
    if not isinstance(labels, dict) or not labels:
        raise ValidationError(
            "define_upstream.select.labels must be a non-empty mapping.",
            code="asset.invalid_dataset_selection",
        )
    requested: dict[str, set[str]] = {}
    for key, values in labels.items():
        if not isinstance(values, list) or not values or not all(str(item).strip() for item in values):
            raise ValidationError(
                "Dataset label selection values must be non-empty string lists.",
                code="asset.invalid_dataset_selection",
                context={"label": str(key)},
            )
        requested[str(key)] = {str(item) for item in values}
    candidates = _asset_dataset_catalog(root_dir, asset_code=asset_code)
    selected = [
        root_dir / str(item["relative_path"])
        for item in candidates
        if all(str((item.get("labels") or {}).get(key)) in values for key, values in requested.items())
    ]
    selected = sorted({item.resolve() for item in selected if item.is_dir()})
    if not selected:
        raise ValidationError(
            "Dataset label selection matched no managed datasets.",
            code="asset.dataset_selection_empty",
            context={"asset_code": asset_code, "labels": {key: sorted(value) for key, value in requested.items()}},
        )
    return selected


def _asset_dataset_catalog(root_dir: Path, *, asset_code: str) -> list[dict[str, Any]]:
    import json

    catalog_path = root_dir / "_smoking_data" / "dataset-catalog.json"
    try:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = None
    datasets = payload.get("datasets") if isinstance(payload, dict) else None
    if isinstance(datasets, list):
        return [dict(item) for item in datasets if isinstance(item, dict)]
    if asset_code != "0101":
        return []
    result: list[dict[str, Any]] = []
    for dataset in sorted(root_dir.glob("*.dataset")):
        metadata_path = dataset / "_smoking_data" / "metadata.json"
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        record = metadata.get("result") if isinstance(metadata, dict) else None
        if not isinstance(record, dict):
            continue
        result.append(
            {
                "relative_path": dataset.relative_to(root_dir).as_posix(),
                "labels": {
                    "asset_code": "0101",
                    "sub_job": record.get("sub_job_name"),
                    "date_from": record.get("date_from"),
                    "date_to": record.get("date_to"),
                },
            }
        )
    return result


def load_preset_spec(yaml_path: str | Path, *, config: RuntimeConfig) -> PresetSpec:
    """Load an internal lowered preset document used by runtime adapters and tests."""

    path = resolve_project_path(yaml_path, project_root=config.project_root)
    if not path.exists():
        raise ValidationError(f"YAML file does not exist: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValidationError(f"YAML root must be a mapping: {path}")
    preset = str(payload.get("preset") or "").strip()
    if not preset:
        raise ValidationError("YAML must define 'preset'.")
    job = payload.get("job")
    if not isinstance(job, dict):
        raise ValidationError("YAML must define 'job' mapping.")
    job_name = str(job.get("name") or "").strip()
    if not job_name:
        raise ValidationError("YAML must define 'job.name'.")
    return PresetSpec(
        preset=preset,
        job_name=job_name,
        yaml_path=path,
        raw=payload,
        yaml_hash=file_sha256(path),
    )


def _compile_pipeline_expression_irs(operations: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(operations, list):
        return {}
    import json

    from smoking_data.ops.projection import resolve_add_calc_expression
    from smoking_data_engine_rs import validate_expression_ir
    from spotfire_expr_normalizer import (
        build_raw_expressions,
        compile_expressions_to_ir,
        validate_rust_ir_function_support,
    )

    result: dict[str, dict[str, Any]] = {}
    for operation in operations:
        if not isinstance(operation, dict) or operation.get("op") not in {
            "add_calc",
            "active_row_selection",
        }:
            continue
        operation_id = str(operation.get("alias") or operation.get("id") or "")
        operation_kind = str(operation.get("op") or "")
        expressions = (
            operation.get("expressions")
            if operation_kind == "add_calc"
            else operation.get("group_keys")
        ) or []
        if not isinstance(expressions, list) or not expressions:
            continue
        items: list[tuple[str, str]] = []
        for index, expression in enumerate(expressions):
            if not isinstance(expression, dict):
                raise ValidationError(
                    f"{operation_kind} expression must be a mapping.",
                    code="yaml.invalid_type",
                    context={"operation_id": operation_id, "index": index},
                )
            name = str(expression.get("name") or "").strip()
            column = str(expression.get("column") or "").strip()
            if operation_kind == "active_row_selection" and not column:
                has_expression = any(
                    str(expression.get(key) or "").strip()
                    for key in ("sql", "spotfire_expression")
                )
                if not has_expression and name:
                    # The public shorthand `{name: key}` means the input
                    # column named `key`; it is not an expression to compile.
                    continue
            if operation_kind == "active_row_selection" and column:
                if any(
                    str(expression.get(key) or "").strip() for key in ("sql", "spotfire_expression")
                ):
                    raise ValidationError(
                        "active_row_selection column key cannot also define an expression.",
                        code="expression.source_ambiguous",
                        context={"operation_id": operation_id, "index": index},
                    )
                if not name:
                    raise ValidationError(
                        "active_row_selection group key name is required.",
                        code="yaml.required_key",
                        context={"operation_id": operation_id, "index": index},
                    )
                continue
            try:
                _, source = resolve_add_calc_expression(expression, index=index)
            except ValueError as error:
                raise ValidationError(
                    str(error),
                    code="expression.invalid",
                    context={"operation_id": operation_id, "index": index},
                ) from error
            if not name:
                raise ValidationError(
                    f"{operation_kind} expression name is required.",
                    code="yaml.required_key",
                    context={"operation_id": operation_id, "index": index},
                )
            items.append((name, source))
        if not items:
            continue
        try:
            document = compile_expressions_to_ir(build_raw_expressions(items)).to_dict()
            validate_rust_ir_function_support(document)
            validate_expression_ir(json.dumps(document, ensure_ascii=True))
            result[operation_id] = document
        except Exception as error:
            raise ValidationError(
                f"Failed to compile {operation_kind} expression IR: {error}",
                code="expression.compile_failed",
                context={"operation_id": operation_id},
            ) from error
    return result
