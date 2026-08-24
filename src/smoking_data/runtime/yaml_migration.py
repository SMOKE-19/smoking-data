"""Deterministic conversion of supported legacy Definition YAML contracts."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import yaml

CURRENT_SOURCE_SCHEMA = "smoking-data.source.v4"
CURRENT_PIPELINE_SCHEMA = "smoking-data.pipeline.v6"
CURRENT_CURATED_SCHEMA = "smoking-data.pipeline.v7"
CURRENT_CHAIN_SCHEMA = "smoking-data.asset-chain.v2"


def migrate_definition_yaml(
    input_path: str | Path,
    *,
    output_path: str | Path,
) -> dict[str, Any]:
    source_path = Path(input_path).expanduser().resolve()
    target_path = Path(output_path).expanduser().resolve()
    payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Legacy YAML root must be an object.")

    if _is_current_source(payload) or _schema_name(payload) in {
        CURRENT_PIPELINE_SCHEMA,
        CURRENT_CURATED_SCHEMA,
        CURRENT_CHAIN_SCHEMA,
    }:
        converted = deepcopy(payload)
        changes: list[dict[str, str]] = []
        warnings = [f"입력 YAML이 이미 {_schema_name(payload)}입니다."]
    elif _legacy_stage_id(payload) in {"etl.01.04", "etl.02.04", "etl.03.01"}:
        converted, changes, warnings = _convert_legacy_pipeline(payload)
    elif _legacy_stage_id(payload) == "etl.chain":
        converted, changes, warnings = _convert_legacy_chain(payload)
    else:
        converted, changes, warnings = _convert_legacy_source(payload)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = target_path.with_suffix(target_path.suffix + ".tmp")
    temporary.write_text(
        yaml.safe_dump(converted, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    temporary.replace(target_path)
    return {
        "ok": True,
        "input": str(source_path),
        "output": str(target_path),
        "source_schema": _source_schema(payload),
        "target_schema": _schema_name(converted),
        "changes": changes,
        "warnings": warnings,
        "validation": {"status": "written"},
    }


def generate_parquet_migration_yaml(
    input_path: str | Path,
    *,
    output_path: str | Path,
    source_asset: str,
    job_name: str = "parquet_migration",
    output_root: str | None = None,
) -> dict[str, Any]:
    """Create a current 0201 migration Definition for an existing Parquet dataset.

    The source asset is lineage only. The generated YAML is executed by the normal
    0201 producer and never mutates the input dataset.
    """

    source = Path(input_path).expanduser().resolve()
    target = Path(output_path).expanduser().resolve()
    files = sorted(source.rglob("*.parquet")) if source.is_dir() else [source]
    files = [item for item in files if item.is_file()]
    if not files:
        raise ValueError(f"Parquet input is empty: {source}")
    fields = [field.name for field in pq.ParquetFile(files[0]).schema_arrow]
    if not fields:
        raise ValueError(f"Parquet input has no columns: {source}")
    partition_column = fields[0]
    group_keys = [{"name": name, "column": name} for name in fields]
    root = output_root or f"DATA/0201/{job_name}"
    payload: dict[str, Any] = {
        "yaml": {"schema_version": CURRENT_CURATED_SCHEMA, "asset_code": "0201"},
        "job": {"name": job_name},
        "migration": {
            "id": f"{job_name}_v1",
            "mode": "pass_through",
            "purpose": "Rewrite an existing Parquet dataset through the current 0201 contract.",
            "source_asset": source_asset,
            "source_path": str(source),
        },
        "output": {
            "artifact": {
                "type": "curated_dataset",
                "root_dir": root,
                "format": "parquet",
                "compression": "zstd",
                "write_policy": "atomic_replace",
            },
            "logging": {"root_dir": f".temp/logs/0201/{job_name}"},
        },
        "define_upstream": [
            {
                "op": "define_dataset",
                "alias": "main",
                "paths": [str(input_path)],
                "format": "parquet",
                "union_by_name": True,
                "missing_columns": "insert_null",
                "incompatible_dtypes": "error",
            }
        ],
        "build_sidecar": {
            "alias": "select_rows",
            "source": "main",
            "columns": "auto",
            "operations": [
                {
                    "op": "active_row_selection",
                    "method": "sort_first",
                    "group_keys": group_keys,
                    "sort": [{"column": partition_column, "direction": "asc", "nulls": "last"}],
                }
            ],
        },
        "materialize": {
            "alias": "materialize_rows",
            "source": "main",
            "coordinates": "select_rows",
            "partition_by": [partition_column],
            "part_boundary": {"target_rows": 20000, "preserve_groups": fields},
            "workers": 1,
            "max_tasks_per_child": 1,
            "operations": [],
        },
        "save_dataset": {
            "alias": "write_migration",
            "input": "materialize_rows",
            "partition_by": [partition_column],
            "operations": [],
        },
        "execution": {"reset_before_run": False},
    }
    _write_yaml_atomic(payload, target)
    return {
        "ok": True,
        "input": str(source),
        "output": str(target),
        "source_asset": source_asset,
        "source_schema": {"fields": fields, "sample_file": str(files[0])},
        "target_schema": CURRENT_CURATED_SCHEMA,
        "warnings": [
            "행 전체를 selection key로 사용하므로 완전히 동일한 duplicate row는 하나로 축약될 수 있습니다.",
            "원본 보존이 필요하면 source에 유일한 row key를 추가한 뒤 migration YAML을 수정하십시오.",
        ],
    }


def _write_yaml_atomic(payload: dict[str, Any], target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = target_path.with_suffix(target_path.suffix + ".tmp")
    temporary.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    temporary.replace(target_path)


def _schema_name(payload: dict[str, Any]) -> str:
    header = payload.get("yaml")
    return str(header.get("schema_version") if isinstance(header, dict) else "unknown")


def _legacy_stage_id(payload: dict[str, Any]) -> str:
    stage = payload.get("stage")
    if not isinstance(stage, dict):
        return ""
    return str(stage.get("id") or "").lower().replace("_", ".")


def _legacy_source_paths(value: Any, *, path: str) -> list[str]:
    if not isinstance(value, list) or not value or not all(str(item).strip() for item in value):
        raise ValueError(f"{path}는 비어 있지 않은 경로 목록이어야 합니다.")
    return [str(item) for item in value]


def _mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{path}는 object여야 합니다.")
    return value


def _pipeline_output(payload: dict[str, Any], *, artifact_type: str, asset_code: str) -> dict[str, Any]:
    output = payload.get("output") if isinstance(payload.get("output"), dict) else {}
    root = output.get("output_dir") or f"DATA/{asset_code}/{{job_name}}"
    logging = payload.get("logging") if isinstance(payload.get("logging"), dict) else {}
    root = _current_template(str(root), fallback=f"DATA/{asset_code}/{{job_name}}")
    logging_root = _current_template(
        str(logging.get("path") or ""), fallback=f".temp/logs/{asset_code}"
    )
    return {
        "artifact": {
            "type": artifact_type,
            "root_dir": root,
            "format": "parquet",
            "compression": "zstd",
            "write_policy": "atomic_replace",
        },
        "logging": {"root_dir": logging_root},
    }


def _current_template(value: str, *, fallback: str) -> str:
    if not value:
        return fallback
    return (
        value.replace("{stage_id}", "{asset_code}")
        .replace("{table_id}", "{job_name}")
        .replace("stage_id", "asset_code")
        .replace("table_id", "job_name")
    )


def _define_dataset(alias: str, paths: list[str]) -> dict[str, Any]:
    return {
        "op": "define_dataset",
        "alias": alias,
        "paths": paths,
        "format": "parquet",
        "union_by_name": True,
        "missing_columns": "insert_null",
        "incompatible_dtypes": "error",
    }


def _convert_legacy_pipeline(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, str]], list[str]]:
    stage_id = _legacy_stage_id(payload)
    job = deepcopy(_required_mapping(payload, "job"))
    name = str(job.get("name") or "migrated_job")
    changes: list[dict[str, str]] = []
    warnings: list[str] = []
    if stage_id == "etl.01.04":
        upstream = _required_mapping(payload, "upstream")
        source = _required_mapping(payload, "source")
        paths = _legacy_source_paths(upstream.get("source_paths"), path="upstream.source_paths")
        keys = [str(item) for item in (_mapping(payload.get("row_selection"), "row_selection").get("sort_first", {}).get("group_keys") or [])]
        keys = keys or [str(item.get("name")) for item in source.get("payload", {}).get("select", []) if isinstance(item, dict) and item.get("name")]
        keys = keys or ["partition"]
        operations = [
            _define_dataset("main", paths),
        ]
        output = _pipeline_output(payload, artifact_type="curated_dataset", asset_code="0201")
        converted = _current_pipeline(name, "0201", CURRENT_CURATED_SCHEMA, output, operations, keys)
        warnings.append("legacy transform/list_restore/pivot 세부 동작은 자동 추정하지 않았습니다.")
        changes.append(_change("stage.id etl.01.04", "yaml.asset_code 0201"))
        return converted, changes, warnings
    if stage_id == "etl.02.04":
        left = _required_mapping(payload, "left")
        join = _required_mapping(payload, "join")
        left_paths = _legacy_source_paths(left.get("source_paths"), path="left.source_paths")
        partition_column = str(join.get("left_partition_key_column") or "partition")
        operations = [
            _define_dataset("left", left_paths),
            {
                "op": "materialize",
                "alias": "materialize_left",
                "inputs": {"source": "left"},
                "partition_by": [partition_column],
                "part_boundary": {
                    "target_rows": 20000,
                    "target_key_groups": int(join.get("target_key_groups_per_part") or 5000),
                    "preserve_groups": [partition_column, *(join.get("left_on") or [])],
                },
                "workers": 1,
                "max_tasks_per_child": 1,
            },
        ]
        right_sources = join.get("right_sources") or []
        if not isinstance(right_sources, list) or not right_sources:
            raise ValueError("legacy join.right_sources가 필요합니다.")
        joins: list[dict[str, Any]] = []
        current = "materialize_left"
        for index, item in enumerate(right_sources):
            right = _mapping(item, f"join.right_sources[{index}]")
            alias = f"right_{index + 1}"
            operations.append(_define_dataset(alias, _legacy_source_paths(right.get("source_paths"), path=f"join.right_sources[{index}].source_paths")))
            joins.append({"alias": alias, "source": right})
            join_alias = f"join_{index + 1}"
            joinspec = {
                "op": "join",
                "alias": join_alias,
                "inputs": {"left": current, "right": alias},
                "how": join.get("how", "left"),
                "left_on": join.get("left_on") or [],
                "right_on": join.get("right_on") or [],
            }
            if join.get("right_partition_key_column"):
                joinspec["right_partition_column"] = join["right_partition_key_column"]
            operations.append(joinspec)
            current = join_alias
        operations.append(
            {
                "op": "save_dataset",
                "alias": "write_output",
                "inputs": {"data": current},
                "partition_by": [partition_column],
            }
        )
        converted = _current_pipeline(name, "0301", CURRENT_PIPELINE_SCHEMA, _pipeline_output(payload, artifact_type="joined_dataset", asset_code="0301"), operations)
        warnings.append("legacy right_select/right_exclude patterns are recorded but not auto-translated to post-join operations.")
        changes.append(_change(f"stage.id {stage_id}", "yaml.asset_code 0301"))
        return converted, changes, warnings
    upstream = _required_mapping(payload, "upstream")
    paths = _legacy_source_paths(upstream.get("source_paths"), path="upstream.source_paths")
    date_window = payload.get("date_window") if isinstance(payload.get("date_window"), dict) else {}
    operations: list[dict[str, Any]] = [_define_dataset("source", paths), {"op": "materialize", "alias": "materialize_snapshot", "inputs": {"source": "source"}, "partition_by": [str(date_window.get("column") or "partition")], "part_boundary": {"target_rows": 20000, "preserve_groups": [str(date_window.get("column") or "partition")]}, "workers": 1, "max_tasks_per_child": 1}, {"op": "save_dataset", "alias": "write_snapshot", "inputs": {"data": "materialize_snapshot"}, "partition_by": [str(date_window.get("column") or "partition")] }]
    converted = _current_pipeline(name, "0401", CURRENT_PIPELINE_SCHEMA, _pipeline_output(payload, artifact_type="analysis_snapshot", asset_code="0401"), operations)
    warnings.append("legacy filters, joins, column_rename and indexed_snapshot options require manual operation mapping.")
    changes.append(_change(f"stage.id {stage_id}", "yaml.asset_code 0401"))
    return converted, changes, warnings


def _current_pipeline(job_name: str, asset_code: str, schema: str, output: dict[str, Any], operations: list[dict[str, Any]], keys: list[str] | None = None) -> dict[str, Any]:
    if keys is not None:
        partition = keys[0]
        return {"yaml": {"schema_version": schema, "asset_code": asset_code}, "job": {"name": job_name}, "output": output, "define_upstream": [{**operations[0]}], "build_sidecar": {"alias": "select_rows", "source": operations[0]["alias"], "columns": "auto", "operations": [{"op": "active_row_selection", "method": "sort_first", "group_keys": [{"name": key, "column": key} for key in keys], "sort": [{"column": partition, "direction": "asc", "nulls": "last"}]}]}, "materialize": {"alias": "materialize_rows", "source": operations[0]["alias"], "coordinates": "select_rows", "partition_by": [partition], "part_boundary": {"target_rows": 20000, "preserve_groups": keys}, "workers": 1, "max_tasks_per_child": 1, "operations": []}, "save_dataset": {"alias": "write_output", "input": "materialize_rows", "partition_by": [partition], "operations": []}, "execution": {"reset_before_run": False}}
    return {"yaml": {"schema_version": schema, "asset_code": asset_code}, "job": {"name": job_name}, "output": output, "operations": operations, "execution": {"reset_before_run": False}}


def _convert_legacy_chain(payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, str]], list[str]]:
    chain = _required_mapping(payload, "chain")
    stages = payload.get("stages")
    if not isinstance(stages, list) or not stages:
        raise ValueError("legacy chain.stages가 필요합니다.")
    assets: list[dict[str, Any]] = []
    changes: list[dict[str, str]] = []
    warnings: list[str] = []
    for index, item in enumerate(stages):
        stage = _mapping(item, f"stages[{index}]")
        path = str(stage.get("yaml_path") or "")
        if not path:
            raise ValueError(f"stages[{index}].yaml_path가 필요합니다.")
        normalized = path.lower().replace("_", ".")
        code = "0401" if "03.01" in normalized else "0301" if "02.04" in normalized else "0201" if "01.04" in normalized else "0101"
        asset = {"id": str(stage.get("name") or f"stage_{index + 1}"), "asset_code": code, "definition": path}
        depends = stage.get("depends_on")
        if isinstance(depends, list) and depends:
            asset["inputs"] = {"source": str(depends[-1])}
        assets.append(asset)
    converted = {"yaml": {"schema_version": CURRENT_CHAIN_SCHEMA}, "chain": {"name": str(chain.get("name") or "migrated_chain")}, "assets": assets, "execution": {"failure_policy": "stop_downstream", "unchanged_policy": "skip", "max_parallel_assets": 1}}
    changes.append(_change("stage.id etl.chain", "yaml.schema_version smoking-data.asset-chain.v2"))
    warnings.append("legacy stage yaml_path는 자동 변환되지 않으므로 각 Asset YAML도 migrate yaml로 별도 변환해야 합니다.")
    return converted, changes, warnings


def _is_current_source(payload: dict[str, Any]) -> bool:
    header = payload.get("yaml")
    return isinstance(header, dict) and header.get("schema_version") == CURRENT_SOURCE_SCHEMA


def _source_schema(payload: dict[str, Any]) -> str:
    header = payload.get("yaml")
    if isinstance(header, dict):
        return str(header.get("schema_version") or header.get("version") or "unknown")
    return "unknown"


def _convert_legacy_source(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, str]], list[str]]:
    header = payload.get("yaml")
    asset = payload.get("asset")
    if not isinstance(header, dict) or "version" not in header:
        raise ValueError("지원하는 legacy YAML 형식이 아닙니다: yaml.version이 없습니다.")
    if not isinstance(asset, dict) or str(asset.get("code")) != "0101":
        raise ValueError("현재 결정론적 변환기는 legacy Source asset code 0101만 지원합니다.")
    source = _required_mapping(payload, "source")
    request = _required_mapping(source, "api_request")
    legacy_output = _required_mapping(payload, "output")

    converted_source = deepcopy(source)
    converted_request = deepcopy(request)
    changes: list[dict[str, str]] = []
    warnings: list[str] = []

    spi_options = converted_request.pop("spi", None)
    if spi_options is not None:
        if not isinstance(spi_options, dict):
            raise ValueError("source.api_request.spi는 object여야 합니다.")
        if "adapter_options" in converted_request:
            raise ValueError("source.api_request.spi와 adapter_options를 동시에 사용할 수 없습니다.")
        converted_request["adapter_options"] = spi_options
        changes.append(_change("source.api_request.spi", "source.api_request.adapter_options"))
    converted_request.setdefault("adapter", "spi")
    changes.append(_change("implicit legacy source adapter", "source.api_request.adapter"))
    converted_source["api_request"] = converted_request

    output = _convert_output(legacy_output, changes, warnings)
    execution = _convert_execution(payload.get("execution"), changes, warnings)
    converted: dict[str, Any] = {
        "yaml": {
            "schema_version": CURRENT_SOURCE_SCHEMA,
            "asset_code": "0101",
        },
        "job": deepcopy(_required_mapping(payload, "job")),
        "source": converted_source,
        "execution": execution,
        "output": output,
    }
    return converted, changes, warnings


def _convert_output(
    legacy: dict[str, Any],
    changes: list[dict[str, str]],
    warnings: list[str],
) -> dict[str, Any]:
    root_dir = legacy.get("output_dir")
    if not root_dir:
        raise ValueError("legacy output.output_dir가 필요합니다.")
    file_rule = _required_mapping(legacy, "file_name_rule")
    writer = legacy.get("parquet_writer", {})
    if not isinstance(writer, dict):
        raise ValueError("legacy output.parquet_writer는 object여야 합니다.")
    logging_root = legacy.get("logging_dir")
    logging_rule = file_rule.get("logging")
    if not logging_root or not logging_rule:
        raise ValueError("legacy output.logging_dir와 file_name_rule.logging이 필요합니다.")
    if legacy.get("metadata_dir"):
        warnings.append("legacy output.metadata_dir는 현재 엔진이 관리하므로 제거했습니다.")
    changes.extend(
        [
            _change("output.output_dir", "output.artifact.root_dir"),
            _change("output.file_name_rule.raw_dataset", "output.artifact.file_name_rule.raw_dataset"),
            _change("output.logging_dir", "output.logging.root_dir"),
            _change("output.file_name_rule.logging", "output.logging.file_name_rule.logging"),
        ]
    )
    return {
        "artifact": {
            "type": "source_dataset",
            "root_dir": root_dir,
            "format": "parquet",
            "write_policy": "atomic_replace",
            "file_name_rule": {"raw_dataset": file_rule["raw_dataset"]},
            "parquet_writer": deepcopy(writer),
        },
        "logging": {
            "root_dir": logging_root,
            "file_name_rule": {"logging": logging_rule},
        },
    }


def _convert_execution(
    legacy: Any,
    changes: list[dict[str, str]],
    warnings: list[str],
) -> dict[str, Any]:
    if legacy is None:
        return {}
    if not isinstance(legacy, dict):
        raise ValueError("legacy execution은 object여야 합니다.")
    execution = deepcopy(legacy)
    if "write_response_profile_json" in execution:
        execution["write_source_profile_json"] = execution.pop("write_response_profile_json")
        changes.append(
            _change(
                "execution.write_response_profile_json",
                "execution.write_source_profile_json",
            )
        )
    if "write_template_sql" in execution:
        execution.pop("write_template_sql")
        warnings.append("legacy execution.write_template_sql은 현재 contract에 없어 제거했습니다.")
    return execution


def _required_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key}는 object여야 합니다.")
    return value


def _change(source: str, target: str) -> dict[str, str]:
    return {"from": source, "to": target}
