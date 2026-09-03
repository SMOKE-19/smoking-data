"""Deterministic conversion of supported legacy Definition YAML contracts."""

from __future__ import annotations

import re
from copy import deepcopy
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import yaml

LEGACY_SOURCE_SCHEMA = "smoking-data.source.v4"
CURRENT_SOURCE_SCHEMA = "smoking-data.source.v5"
LEGACY_CALCULATED_FACT_PHASE_SCHEMA = "smoking-data.calculated-fact.v2"
LEGACY_CALCULATED_FACT_OPERATION_SCHEMA = "smoking-data.calculated-fact.v3"
CURRENT_CALCULATED_FACT_SCHEMA = "smoking-data.calculated-fact.v4"
CURRENT_CSV_SOURCE_SCHEMA = "smoking-data.csv-source.v1"
CURRENT_PIPELINE_SCHEMA = "smoking-data.pipeline.v6"
CURRENT_CURATED_SCHEMA = "smoking-data.pipeline.v7"
CURRENT_SNAPSHOT_SCHEMA = "smoking-data.pipeline.v8"
CURRENT_CHAIN_SCHEMA = "smoking-data.asset-chain.v2"
CURRENT_PUBLICATION_SCHEMA = "smoking-data.publication.v1"


class _ExpressionText(str):
    """Marker for SQL expressions that must be emitted as literal YAML text."""


class _MigrationDumper(yaml.SafeDumper):
    pass


def _represent_expression_text(dumper: yaml.Dumper, value: _ExpressionText) -> yaml.Node:
    return dumper.represent_scalar("tag:yaml.org,2002:str", str(value), style="|")


_MigrationDumper.add_representer(_ExpressionText, _represent_expression_text)


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

    if _source_schema(payload) in {LEGACY_SOURCE_SCHEMA, CURRENT_SOURCE_SCHEMA}:
        if _source_schema(payload) != CURRENT_SOURCE_SCHEMA or _needs_source_normalization(payload):
            converted, changes, warnings = _normalize_current_source(payload)
        else:
            converted = deepcopy(payload)
            changes = []
            warnings = [f"입력 YAML이 이미 {_schema_name(payload)}입니다."]
    elif _schema_name(payload) in {
        LEGACY_CALCULATED_FACT_PHASE_SCHEMA,
        LEGACY_CALCULATED_FACT_OPERATION_SCHEMA,
        CURRENT_PIPELINE_SCHEMA,
        CURRENT_CURATED_SCHEMA,
        CURRENT_SNAPSHOT_SCHEMA,
        CURRENT_CALCULATED_FACT_SCHEMA,
        CURRENT_CSV_SOURCE_SCHEMA,
        CURRENT_CHAIN_SCHEMA,
        CURRENT_PUBLICATION_SCHEMA,
    }:
        converted = deepcopy(payload)
        changes = []
        _normalize_public_output(
            converted,
            asset_code=str((converted.get("yaml") or {}).get("asset_code") or ""),
            changes=changes,
        )
        _normalize_calculated_fact_phase_contract(converted, changes=changes)
        _normalize_calculated_fact_operation_contract(converted, changes=changes)
        _normalize_calculated_fact_in_place_list_contract(converted, changes=changes)
        _normalize_curated_phase_contract(converted, changes=changes)
        _normalize_join_upstream_contract(converted, changes=changes)
        _normalize_snapshot_phase_contract(converted, changes=changes)
        warnings = [f"입력 YAML이 이미 {_schema_name(payload)}입니다."]
    elif _legacy_stage_id(payload) in {"etl.01.04", "etl.02.04", "etl.03.01"}:
        converted, changes, warnings = _convert_legacy_pipeline(payload)
    elif _legacy_stage_id(payload) == "etl.chain":
        converted, changes, warnings = _convert_legacy_chain(payload)
    elif _legacy_stage_id(payload) == "etl.04.01":
        converted, changes, warnings = _convert_legacy_publication(payload)
    else:
        converted, changes, warnings = _convert_legacy_source(payload)

    _normalize_definition_global_memory(converted, changes=changes)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = target_path.with_suffix(target_path.suffix + ".tmp")
    temporary.write_text(
        _dump_migration_yaml(converted),
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


def _normalize_definition_global_memory(
    payload: dict[str, Any],
    *,
    changes: list[dict[str, str]],
) -> None:
    """Remove process-wide memory limits from Asset Definitions."""

    execution = payload.get("execution")
    if not isinstance(execution, dict):
        return
    if execution.pop("memory_budget_mb", None) is not None:
        changes.append(
            _change(
                "execution.memory_budget_mb",
                ".smoking-data/config.yaml execution.memory.hard_limit_mb",
            )
        )
    memory = execution.get("memory")
    if not isinstance(memory, dict):
        return
    for key in ("hard_limit_mb", "safety_ratio"):
        if memory.pop(key, None) is not None:
            changes.append(
                _change(
                    f"execution.memory.{key}",
                    f".smoking-data/config.yaml execution.memory.{key}",
                )
            )
    phases = memory.get("phases")
    if isinstance(phases, dict):
        for phase_name, phase in list(phases.items()):
            if not isinstance(phase, dict):
                continue
            if phase.pop("target_peak_memory_mb", None) is not None:
                changes.append(
                    _change(
                        f"execution.memory.phases.{phase_name}.target_peak_memory_mb",
                        ".smoking-data/config.yaml execution.memory hard envelope",
                    )
                )
            if not phase:
                phases.pop(phase_name, None)
        if not phases:
            memory.pop("phases", None)
    if not memory:
        execution.pop("memory", None)


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
    group_keys = list(fields)
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
                "physical_layout": {
                    "profile": "curated_reuse_v1",
                    "adaptation_scope": "generation_fixed",
                    "row_group_rows": "auto",
                },
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
            "partition_by": [partition_column],
            "part_boundary": {"target_rows": 20000, "preserve_groups": fields},
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
            "workers": 1,
            "max_tasks_per_child": 1,
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
        _dump_migration_yaml(payload), encoding="utf-8"
    )
    temporary.replace(target_path)


def _dump_migration_yaml(payload: dict[str, Any]) -> str:
    marked = deepcopy(payload)
    _mark_expression_text(marked)
    return yaml.dump(marked, Dumper=_MigrationDumper, allow_unicode=True, sort_keys=False)


def _mark_expression_text(value: Any, *, key: str | None = None) -> Any:
    if isinstance(value, dict):
        for name, item in list(value.items()):
            value[name] = _mark_expression_text(item, key=name)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            value[index] = _mark_expression_text(item, key=key)
    elif key == "expr" and isinstance(value, str):
        return _ExpressionText(value)
    return value


def _schema_name(payload: dict[str, Any]) -> str:
    header = payload.get("yaml")
    return str(header.get("schema_version") if isinstance(header, dict) else "unknown")


def _normalize_calculated_fact_phase_contract(
    payload: dict[str, Any],
    *,
    changes: list[dict[str, str]],
) -> None:
    """Normalize 0102 v2's exposed selector/save phases into the v3 contract."""

    if _schema_name(payload) != LEGACY_CALCULATED_FACT_PHASE_SCHEMA:
        return
    header = payload.get("yaml")
    build = payload.get("build_sidecar")
    materialize = payload.get("materialize")
    if not isinstance(header, dict) or str(header.get("asset_code") or "") != "0102":
        raise ValueError("calculated-fact v2 YAML의 asset_code는 0102여야 합니다.")
    if not isinstance(build, dict) or not isinstance(materialize, dict):
        raise ValueError("0102 v2 YAML에는 build_sidecar와 materialize object가 필요합니다.")

    operations = build.pop("operations", None)
    if (
        not isinstance(operations, list)
        or len(operations) != 1
        or not isinstance(operations[0], dict)
        or operations[0].get("op") != "incremental_fact_selection"
    ):
        raise ValueError(
            "0102 v2 build_sidecar.operations는 하나의 incremental_fact_selection이어야 합니다."
        )
    identity_columns = operations[0].get("identity_columns")
    if not isinstance(identity_columns, list) or not identity_columns:
        raise ValueError("incremental_fact_selection.identity_columns가 필요합니다.")
    build["identity_columns"] = deepcopy(identity_columns)
    changes.append(
        _change(
            "build_sidecar.operations[0].identity_columns",
            "build_sidecar.identity_columns",
        )
    )

    upstream_alias = str(build.get("source") or "").strip()
    sidecar_alias = str(build.get("alias") or "").strip()
    for field, expected in (("source", upstream_alias), ("coordinates", sidecar_alias)):
        value = str(materialize.pop(field, "") or "").strip()
        if not expected or value != expected:
            raise ValueError(
                f"0102 v2 materialize.{field}는 build_sidecar 연결과 일치해야 합니다. "
                f"value={value!r}, expected={expected!r}"
            )
        changes.append(_change(f"materialize.{field}", "implicit build_sidecar link"))

    partition_by = materialize.pop("partition_by", None)
    if not isinstance(partition_by, list) or not partition_by:
        raise ValueError("0102 v2 materialize.partition_by가 필요합니다.")
    build["partition_by"] = deepcopy(partition_by)
    changes.append(_change("materialize.partition_by", "build_sidecar.partition_by"))

    part_boundary = materialize.pop("part_boundary", None)
    if not isinstance(part_boundary, dict):
        raise ValueError("0102 v2 materialize.part_boundary가 필요합니다.")
    build["part_boundary"] = deepcopy(part_boundary)
    changes.append(_change("materialize.part_boundary", "build_sidecar.part_boundary"))

    build_execution = build.pop("execution", None)
    if build_execution is not None:
        if not isinstance(build_execution, dict):
            raise ValueError("0102 v2 build_sidecar.execution은 object여야 합니다.")
        recycle = build_execution.get("worker_recycle")
        build_max_files = (
            recycle.get("max_source_files") if isinstance(recycle, dict) else None
        )
        if build_max_files is not None:
            execution = payload.setdefault("execution", {})
            if not isinstance(execution, dict):
                raise ValueError("0102 execution은 object여야 합니다.")
            root_max_files = execution.get("max_source_files_per_task")
            execution["max_source_files_per_task"] = min(
                int(build_max_files), int(root_max_files or build_max_files)
            )
        changes.append(
            _change("build_sidecar.execution", "execution.max_source_files_per_task")
        )

    save = payload.pop("save_dataset", None)
    if not isinstance(save, dict):
        raise ValueError("0102 v2 save_dataset object가 필요합니다.")
    if str(save.get("input") or "").strip() != str(materialize.get("alias") or "").strip():
        raise ValueError("0102 v2 save_dataset.input은 materialize.alias와 일치해야 합니다.")
    if save.get("partition_by") != partition_by:
        raise ValueError("0102 v2 save_dataset.partition_by는 materialize.partition_by와 일치해야 합니다.")
    if save.get("operations") not in (None, []):
        raise ValueError("0102 v2 save_dataset.operations는 비어 있어야 합니다.")
    changes.append(_change("save_dataset", "implicit terminal dataset commit"))

    execution = payload.get("execution")
    memory = execution.get("memory") if isinstance(execution, dict) else None
    phases = memory.get("phases") if isinstance(memory, dict) else None
    if isinstance(phases, dict) and phases.pop("save_dataset", None) is not None:
        changes.append(
            _change(
                "execution.memory.phases.save_dataset",
                "removed (internal commit phase)",
            )
        )

    header["schema_version"] = LEGACY_CALCULATED_FACT_OPERATION_SCHEMA
    changes.append(
        _change(
            LEGACY_CALCULATED_FACT_PHASE_SCHEMA,
            LEGACY_CALCULATED_FACT_OPERATION_SCHEMA,
        )
    )


def _normalize_calculated_fact_operation_contract(
    payload: dict[str, Any],
    *,
    changes: list[dict[str, str]],
) -> None:
    """Split 0102 v3's compound calculation operation into ordered v4 operations."""

    if _schema_name(payload) != LEGACY_CALCULATED_FACT_OPERATION_SCHEMA:
        return
    header = payload.get("yaml")
    materialize = payload.get("materialize")
    if not isinstance(header, dict) or str(header.get("asset_code") or "") != "0102":
        raise ValueError("calculated-fact v3 YAML의 asset_code는 0102여야 합니다.")
    if not isinstance(materialize, dict):
        raise ValueError("0102 v3 YAML에는 materialize object가 필요합니다.")
    operations = materialize.get("operations")
    if not isinstance(operations, list):
        raise ValueError("0102 v3 materialize.operations는 list여야 합니다.")
    compound = [
        item
        for item in operations
        if isinstance(item, dict) and item.get("op") == "add_calc_cols_from_file"
    ]
    if len(compound) != 1:
        raise ValueError(
            "0102 v3 materialize.operations에는 add_calc_cols_from_file이 정확히 하나 필요합니다."
        )
    calculate = deepcopy(compound[0])
    calculate_alias = str(calculate.get("alias") or "calculate").strip() or "calculate"
    used_aliases = {
        str(item.get("alias") or "").strip()
        for item in operations
        if isinstance(item, dict)
    }
    used_aliases.update(
        str(section.get("alias") or "").strip()
        for name in ("build_sidecar", "materialize")
        if isinstance((section := payload.get(name)), dict)
    )
    upstreams = payload.get("define_upstream")
    if isinstance(upstreams, list):
        used_aliases.update(
            str(item.get("alias") or "").strip()
            for item in upstreams
            if isinstance(item, dict)
        )
    used_aliases.discard("")

    include_columns = calculate.pop("include_columns", None)
    alias_files = calculate.pop("column_alias_files", None)
    inline_expand = calculate.pop("expand_list_rows", None)
    inline_compact = calculate.pop("compact_list_rows", None)
    calculate["op"] = "add_calculated_cols"
    if calculate.get("lookup_files") == []:
        calculate.pop("lookup_files")

    standalone_expand = next(
        (
            deepcopy(item)
            for item in operations
            if isinstance(item, dict) and item.get("op") == "expand_list_rows"
        ),
        None,
    )
    standalone_compact = next(
        (
            deepcopy(item)
            for item in operations
            if isinstance(item, dict) and item.get("op") == "compact_list_rows"
        ),
        None,
    )
    if (inline_expand is not None or inline_compact is not None) and (
        standalone_expand is not None or standalone_compact is not None
    ):
        raise ValueError("0102 v3 List 단계가 inline과 standalone에 중복 선언되었습니다.")
    if (inline_expand is None) != (inline_compact is None):
        raise ValueError("0102 v3 inline expand/compact는 함께 선언해야 합니다.")

    normalized: list[dict[str, Any]] = []
    if include_columns is not None:
        normalized.append(
            {
                "op": "include_columns",
                "alias": _next_migration_alias(f"{calculate_alias}_include", used_aliases),
                "columns": deepcopy(include_columns),
            }
        )
    if alias_files not in (None, []):
        normalized.append(
            {
                "op": "reference_replace",
                "alias": _next_migration_alias(
                    f"{calculate_alias}_reference_replace", used_aliases
                ),
                "files": deepcopy(alias_files),
            }
        )

    if standalone_expand is not None:
        normalized.append(standalone_expand)
        expand_alias = str(standalone_expand.get("alias") or "").strip()
    elif inline_expand is not None:
        expand_alias = _next_migration_alias(f"{calculate_alias}_expand", used_aliases)
        normalized.append(
            {
                "op": "expand_list_rows",
                "alias": expand_alias,
                "columns": deepcopy(inline_expand.get("columns")),
            }
        )
    else:
        expand_alias = ""

    normalized.append(calculate)
    if standalone_compact is not None:
        normalized.append(standalone_compact)
    elif inline_compact is not None:
        normalized.append(
            {
                "op": "compact_list_rows",
                "alias": _next_migration_alias(f"{calculate_alias}_compact", used_aliases),
                "expansion_alias_list": [expand_alias, calculate_alias],
                "columns": deepcopy(inline_compact.get("columns")),
            }
        )
    normalized.extend(
        deepcopy(item)
        for item in operations
        if isinstance(item, dict) and item.get("op") == "unpivot_0102"
    )
    materialize["operations"] = normalized
    header["schema_version"] = CURRENT_CALCULATED_FACT_SCHEMA
    if include_columns is not None:
        changes.append(
            _change("add_calc_cols_from_file.include_columns", "include_columns operation")
        )
    if alias_files not in (None, []):
        changes.append(
            _change(
                "add_calc_cols_from_file.column_alias_files",
                "reference_replace operation",
            )
        )
    if inline_expand is not None:
        changes.append(
            _change(
                "add_calc_cols_from_file.expand_list_rows/compact_list_rows",
                "expand_list_rows/compact_list_rows operations",
            )
        )
    changes.extend(
        [
            _change("add_calc_cols_from_file", "add_calculated_cols operation"),
            _change(
                LEGACY_CALCULATED_FACT_OPERATION_SCHEMA,
                CURRENT_CALCULATED_FACT_SCHEMA,
            ),
        ]
    )


def _normalize_calculated_fact_in_place_list_contract(
    payload: dict[str, Any],
    *,
    changes: list[dict[str, str]],
) -> None:
    """Normalize pre-release v4 List bindings to in-place column-name lists."""

    if _schema_name(payload) != CURRENT_CALCULATED_FACT_SCHEMA:
        return
    header = payload.get("yaml")
    materialize = payload.get("materialize")
    if not isinstance(header, dict) or str(header.get("asset_code") or "") != "0102":
        return
    if not isinstance(materialize, dict):
        return
    operations = materialize.get("operations")
    if not isinstance(operations, list):
        raise ValueError("0102 v4 materialize.operations는 list여야 합니다.")

    expand_alias = next(
        (
            str(item.get("alias") or "").strip()
            for item in operations
            if isinstance(item, dict) and item.get("op") == "expand_list_rows"
        ),
        "",
    )
    calculate_alias = next(
        (
            str(item.get("alias") or "").strip()
            for item in operations
            if isinstance(item, dict) and item.get("op") == "add_calculated_cols"
        ),
        "",
    )

    for operation in operations:
        if not isinstance(operation, dict):
            continue
        kind = operation.get("op")
        if kind not in {"expand_list_rows", "compact_list_rows"}:
            continue
        columns = operation.get("columns")
        if not isinstance(columns, list) or not columns:
            continue
        if kind == "compact_list_rows":
            expansion_aliases = operation.get("expansion_alias_list")
            if not isinstance(expansion_aliases, list):
                expansion = operation.pop("expansion", None)
                if isinstance(expansion, dict):
                    legacy_expand_alias = str(expansion.get("expand") or expand_alias).strip()
                    legacy_calculate_alias = str(
                        expansion.get("calculate") or calculate_alias
                    ).strip()
                else:
                    legacy_expand_alias = str(expansion or expand_alias).strip()
                    legacy_calculate_alias = calculate_alias
                operation["expansion_alias_list"] = [
                    legacy_expand_alias,
                    legacy_calculate_alias,
                ]
                changes.append(
                    _change(
                        "compact_list_rows.expansion",
                        "compact_list_rows.expansion_alias_list",
                    )
                )
        if all(isinstance(item, str) for item in columns):
            continue
        source_key, target_key = (
            ("source", "element_alias")
            if kind == "expand_list_rows"
            else ("source", "output")
        )
        names: list[str] = []
        for item in columns:
            if not isinstance(item, dict):
                raise ValueError(f"0102 {kind}.columns는 문자열 list여야 합니다.")
            source = str(item.get(source_key) or "").strip()
            target = str(item.get(target_key) or "").strip()
            if not source or source != target:
                raise ValueError(
                    f"0102 {kind}의 기존 {source_key}/{target_key} 이름이 다릅니다. "
                    "in-place columns 계약으로 자동 변환할 수 없으므로 계산식과 칼럼명을 "
                    "같게 정리한 뒤 다시 마이그레이션하십시오."
                )
            names.append(source)
        operation["columns"] = names
        changes.append(
            _change(
                f"{kind}.columns.{source_key}/{target_key}",
                f"{kind}.columns[] (in-place)",
            )
        )


def _next_migration_alias(base: str, used: set[str]) -> str:
    candidate = base
    suffix = 2
    while candidate in used:
        candidate = f"{base}_{suffix}"
        suffix += 1
    used.add(candidate)
    return candidate


def _normalize_curated_phase_contract(
    payload: dict[str, Any],
    *,
    changes: list[dict[str, str]],
) -> None:
    """Normalize legacy 0201 phase links into the sidecar-owned partition contract."""

    if _schema_name(payload) != CURRENT_CURATED_SCHEMA:
        return
    build_sidecar = payload.get("build_sidecar")
    materialize = payload.get("materialize")
    if not isinstance(build_sidecar, dict) or not isinstance(materialize, dict):
        return

    sidecar_operations = build_sidecar.get("operations")
    if isinstance(sidecar_operations, list):
        for selector_index, selector in enumerate(sidecar_operations):
            if not isinstance(selector, dict) or selector.get("op") != "active_row_selection":
                continue
            raw_group_keys = selector.get("group_keys")
            if not isinstance(raw_group_keys, list) or not raw_group_keys:
                continue
            if all(isinstance(item, str) for item in raw_group_keys):
                continue
            names: list[str] = []
            calculated: list[dict[str, str]] = []
            for index, item in enumerate(raw_group_keys):
                if isinstance(item, str):
                    name = item.strip()
                    if not name:
                        raise ValueError(
                            f"build_sidecar.operations[{selector_index}].group_keys[{index}]가 비어 있습니다."
                        )
                    names.append(name)
                    continue
                if not isinstance(item, dict):
                    raise ValueError(
                        f"build_sidecar.operations[{selector_index}].group_keys[{index}]는 문자열 또는 객체여야 합니다."
                    )
                name = str(item.get("name") or "").strip()
                sources = {
                    key: str(item.get(key) or "").strip()
                    for key in ("column", "sql", "spotfire_expression")
                    if str(item.get(key) or "").strip()
                }
                if not name or len(sources) > 1:
                    raise ValueError(
                        f"build_sidecar.operations[{selector_index}].group_keys[{index}]를 "
                        "단일 add_calc와 문자열 키로 변환할 수 없습니다."
                    )
                names.append(name)
                if not sources:
                    continue
                source_kind, source = next(iter(sources.items()))
                if source_kind == "column" and source == name:
                    continue
                calculated.append(
                    {
                        "name": name,
                        (
                            "sql" if source_kind == "column" else source_kind
                        ): (
                            '"' + source.replace('"', '""') + '"'
                            if source_kind == "column"
                            else source
                        ),
                    }
                )
            if len(set(names)) != len(names):
                raise ValueError("active_row_selection.group_keys에는 중복 이름을 사용할 수 없습니다.")
            selector["group_keys"] = names
            if calculated:
                used_aliases = {
                    str(operation.get("alias") or "")
                    for operation in sidecar_operations
                    if isinstance(operation, dict)
                }
                sidecar_operations.insert(
                    selector_index,
                    {
                        "op": "add_calc",
                        "alias": _next_migration_alias("calculate_group_keys", used_aliases),
                        "expressions": calculated,
                    },
                )
            changes.append(
                _change(
                    "build_sidecar.operations[].active_row_selection.group_keys objects",
                    "preceding add_calc plus group_keys string list",
                )
            )
            break

    expected = {
        "source": str(build_sidecar.get("source") or "").strip(),
        "coordinates": str(build_sidecar.get("alias") or "").strip(),
    }
    for field, target in expected.items():
        if field not in materialize:
            continue
        value = str(materialize.get(field) or "").strip()
        if not target or value != target:
            counterpart = "build_sidecar.source" if field == "source" else "build_sidecar.alias"
            raise ValueError(
                f"materialize.{field}는 {counterpart}와 일치해야 생략형으로 정규화할 수 있습니다. "
                f"value={value!r}, expected={target!r}"
            )
        materialize.pop(field)
        changes.append(
            _change(
                f"materialize.{field}",
                "implicit build_sidecar.source"
                if field == "source"
                else "implicit build_sidecar.alias",
            )
        )

    sidecar_partition = build_sidecar.get("partition_by")
    materialize_partition = materialize.pop("partition_by", None)
    if sidecar_partition is None:
        if not isinstance(materialize_partition, list) or not materialize_partition:
            raise ValueError(
                "0201 YAML에는 build_sidecar.partition_by 또는 legacy materialize.partition_by가 필요합니다."
            )
        build_sidecar["partition_by"] = deepcopy(materialize_partition)
        sidecar_partition = build_sidecar["partition_by"]
        changes.append(
            _change("materialize.partition_by", "build_sidecar.partition_by")
        )
    elif materialize_partition is not None:
        if materialize_partition != sidecar_partition:
            raise ValueError(
                "materialize.partition_by는 build_sidecar.partition_by와 일치해야 정규화할 수 있습니다."
            )
        changes.append(
            _change("materialize.partition_by", "build_sidecar.partition_by")
        )

    materialize_boundary = materialize.pop("part_boundary", None)
    sidecar_boundary = build_sidecar.get("part_boundary")
    if sidecar_boundary is None:
        if not isinstance(materialize_boundary, dict):
            raise ValueError(
                "0201 YAML에는 build_sidecar.part_boundary 또는 legacy materialize.part_boundary가 필요합니다."
            )
        build_sidecar["part_boundary"] = deepcopy(materialize_boundary)
        changes.append(
            _change("materialize.part_boundary", "build_sidecar.part_boundary")
        )
    elif materialize_boundary is not None:
        if materialize_boundary != sidecar_boundary:
            raise ValueError(
                "materialize.part_boundary는 build_sidecar.part_boundary와 일치해야 정규화할 수 있습니다."
            )
        changes.append(
            _change("materialize.part_boundary", "build_sidecar.part_boundary")
        )

    execution = payload.get("execution")
    memory = execution.get("memory") if isinstance(execution, dict) else None
    phases = memory.get("phases") if isinstance(memory, dict) else None
    if isinstance(phases, dict) and phases.pop("save_dataset", None) is not None:
        changes.append(
            _change(
                "execution.memory.phases.save_dataset",
                "removed (internal commit phase)",
            )
        )

    save = payload.pop("save_dataset", None)
    if save is None:
        return
    if not isinstance(save, dict):
        raise ValueError("save_dataset은 object여야 합니다.")
    save_input = str(save.get("input") or "").strip()
    materialize_alias = str(materialize.get("alias") or "").strip()
    if save_input != materialize_alias:
        raise ValueError(
            "save_dataset.input은 materialize.alias와 일치해야 자동 terminal write로 정규화할 수 있습니다."
        )
    save_partition = save.get("partition_by")
    if save_partition != sidecar_partition:
        raise ValueError(
            "save_dataset.partition_by는 build_sidecar.partition_by와 일치해야 정규화할 수 있습니다."
        )
    assertions = save.get("operations") or []
    if not isinstance(assertions, list) or any(
        not isinstance(item, dict) or item.get("op") != "data_assertion"
        for item in assertions
    ):
        raise ValueError(
            "save_dataset.operations는 data_assertion 목록이어야 materialize.operations로 이동할 수 있습니다."
        )
    materialize_operations = materialize.setdefault("operations", [])
    if not isinstance(materialize_operations, list):
        raise ValueError("materialize.operations는 list여야 합니다.")
    materialize_operations.extend(deepcopy(assertions))
    changes.append(
        _change(
            "save_dataset",
            "implicit terminal write; data_assertion moved to materialize.operations",
        )
    )


def _normalize_join_upstream_contract(
    payload: dict[str, Any],
    *,
    changes: list[dict[str, str]],
) -> None:
    """Normalize 0301 into the mandatory keyspace-first public contract."""

    header = payload.get("yaml")
    if (
        _schema_name(payload) != CURRENT_PIPELINE_SCHEMA
        or not isinstance(header, dict)
        or str(header.get("asset_code") or "") != "0301"
    ):
        return
    public_materialize = payload.get("materialize")
    operations = payload.get("operations")
    if public_materialize is not None and operations is not None:
        raise ValueError("0301은 루트 materialize와 operations를 동시에 선언할 수 없습니다.")
    if public_materialize is None and not isinstance(operations, list):
        return
    if public_materialize is not None and not isinstance(public_materialize, dict):
        raise ValueError("0301 materialize는 object여야 합니다.")

    flat_operations = operations if isinstance(operations, list) else []
    source_ops = [
        deepcopy(item)
        for item in flat_operations
        if isinstance(item, dict)
        and item.get("op") in {"define_asset", "define_dataset"}
    ]
    existing = payload.get("define_upstream")
    if existing is not None:
        if not isinstance(existing, list) or not existing:
            raise ValueError("0301 define_upstream은 비어 있지 않은 list여야 합니다.")
        if source_ops:
            raise ValueError(
                "0301 source operation은 define_upstream과 operations에 중복 선언할 수 없습니다."
            )
    else:
        if not source_ops:
            raise ValueError(
                "0301 YAML에는 define_asset 또는 define_dataset source가 필요합니다."
            )
        payload["define_upstream"] = source_ops
        changes.append(
            _change(
                "operations define_asset/define_dataset",
                "define_upstream",
            )
        )

    downstream = [
        deepcopy(item)
        for item in flat_operations
        if not (
            isinstance(item, dict)
            and item.get("op") in {"define_asset", "define_dataset"}
        )
    ]
    upstream = payload.get("define_upstream")
    first_alias = (
        str(upstream[0].get("alias") or "")
        if isinstance(upstream, list) and upstream and isinstance(upstream[0], dict)
        else ""
    )
    existing_keyspace = payload.get("build_sidecar")
    has_keyspace = isinstance(existing_keyspace, dict)

    if public_materialize is not None:
        materialize = deepcopy(public_materialize)
        nested = deepcopy(materialize.get("operations") or [])
        if not isinstance(nested, list):
            raise ValueError("0301 materialize.operations는 list여야 합니다.")
        source_alias = str(materialize.pop("source", None) or first_alias)
        legacy_boundary = materialize.pop("part_boundary", None)
        legacy_partition = materialize.pop("partition_by", None)
    else:
        materialize_ops = [
            item
            for item in downstream
            if isinstance(item, dict) and item.get("op") == "materialize"
        ]
        save_ops = [
            item
            for item in downstream
            if isinstance(item, dict) and item.get("op") == "save_dataset"
        ]
        if len(materialize_ops) > 1 or len(save_ops) != 1:
            raise ValueError(
                "0301 정규화에는 최대 하나의 materialize와 정확히 하나의 save_dataset이 필요합니다."
            )
        save = save_ops[0]
        legacy_partition = save.get("partition_by")
        if not isinstance(legacy_partition, list) or not legacy_partition:
            raise ValueError("0301 save_dataset.partition_by가 필요합니다.")
        if materialize_ops:
            operation = materialize_ops[0]
            inputs = operation.get("inputs")
            source_alias = str(
                (inputs.get("source") if isinstance(inputs, dict) else None) or first_alias
            )
            legacy_boundary = operation.get("part_boundary")
            if operation.get("partition_by") != legacy_partition:
                raise ValueError(
                    "0301 materialize.partition_by와 save_dataset.partition_by가 일치해야 합니다."
                )
            materialize = {
                key: deepcopy(value)
                for key, value in operation.items()
                if key not in {"op", "inputs", "partition_by", "part_boundary"}
            }
        else:
            source_alias = first_alias
            legacy_boundary = None
            materialize = {
                "alias": "materialize_selected_payload",
                "workers": 1,
                "max_tasks_per_child": 1,
            }
        nested = [
            item
            for item in downstream
            if not isinstance(item, dict)
            or item.get("op") not in {"materialize", "save_dataset"}
        ]
        terminal_alias = (
            str(nested[-1].get("alias") or "")
            if nested and isinstance(nested[-1], dict)
            else source_alias
        )
        save_inputs = save.get("inputs")
        if not isinstance(save_inputs, dict) or save_inputs.get("data") != terminal_alias:
            raise ValueError(
                "0301 save_dataset 입력은 materialize.operations의 마지막 alias와 일치해야 합니다."
            )

    for operation in nested:
        if not isinstance(operation, dict) or operation.get("op") != "join":
            continue
        how = str(operation.pop("how", "left") or "left").strip().lower()
        operation.pop("right_partition_column", None)
        if how not in {"left", "left_outer"}:
            raise ValueError(
                "0301 migrate는 keyspace 기반 left join으로 보존 가능한 기존 how만 지원합니다."
            )
        columns = operation.pop("columns", None)
        if columns is not None:
            if not isinstance(columns, dict):
                raise ValueError("0301 join.columns는 object여야 합니다.")
            if "include_columns" in operation or "exclude_columns" in operation:
                raise ValueError(
                    "0301 join은 columns와 include_columns/exclude_columns를 함께 사용할 수 없습니다."
                )
            include = [str(item) for item in columns.get("include") or []]
            include_regex = [str(item) for item in columns.get("regex") or []]
            exclude = [str(item) for item in columns.get("exclude") or []]
            exclude_regex = [str(item) for item in columns.get("exclude_regex") or []]
            if include_regex:
                exact_patterns = [f"^(?:{re.escape(name)})$" for name in include]
                operation["include_columns"] = "|".join(
                    [*exact_patterns, *(f"(?:{pattern})" for pattern in include_regex)]
                )
            elif include:
                operation["include_columns"] = include
            if exclude_regex:
                exact_patterns = [f"^(?:{re.escape(name)})$" for name in exclude]
                operation["exclude_columns"] = "|".join(
                    [*exact_patterns, *(f"(?:{pattern})" for pattern in exclude_regex)]
                )
            elif exclude:
                operation["exclude_columns"] = exclude

    if has_keyspace:
        keyspace = deepcopy(existing_keyspace)
        if str(keyspace.get("method") or "").strip().lower() != "union_distinct_keys":
            raise ValueError("0301 build_sidecar.method는 union_distinct_keys여야 합니다.")
    else:
        first_join = next(
            (
                item
                for item in nested
                if isinstance(item, dict) and item.get("op") == "join"
            ),
            None,
        )
        left_keys = first_join.get("left_on") if isinstance(first_join, dict) else []
        boundary_groups = (
            legacy_boundary.get("preserve_groups")
            if isinstance(legacy_boundary, dict)
            else None
        )
        keys: list[str] = []
        for value in [*(legacy_partition or []), *(boundary_groups or []), *(left_keys or [])]:
            text = str(value).strip()
            if text and text not in keys:
                keys.append(text)
        if not keys:
            raise ValueError("0301 keyspace keys를 기존 join/materialize 계약에서 추론할 수 없습니다.")
        partition_by = [str((legacy_partition or keys)[0])]
        boundary = deepcopy(legacy_boundary) if isinstance(legacy_boundary, dict) else {}
        boundary.setdefault("target_rows", 20000)
        boundary["preserve_groups"] = deepcopy(keys)
        keyspace = {
            "alias": "join_keyspace",
            "method": "union_distinct_keys",
            "sources": [source_alias],
            "keys": deepcopy(keys),
            "partition_by": partition_by,
            "part_boundary": boundary,
            "null_key_policy": "error",
        }
        materialize_alias = str(materialize.get("alias") or "materialize_selected_payload")
        used_aliases = {
            str(item.get("alias") or "") for item in nested if isinstance(item, dict)
        }
        payload_alias = f"join_{source_alias}"
        suffix = 2
        while payload_alias in used_aliases:
            payload_alias = f"join_{source_alias}_{suffix}"
            suffix += 1
        for item in nested:
            inputs = item.get("inputs") if isinstance(item, dict) else None
            if not isinstance(inputs, dict):
                continue
            for port in ("left", "data", "source"):
                value = inputs.get(port)
                if value in {source_alias, materialize_alias}:
                    inputs[port] = payload_alias
        nested.insert(
            0,
            {
                "op": "join",
                "alias": payload_alias,
                "input_right": source_alias,
                "left_on": deepcopy(keys),
                "right_on": deepcopy(keys),
            },
        )
        changes.append(
            _change(
                "0301 materialize boundary",
                "build_sidecar union_distinct_keys with payload restoration join",
            )
        )

    current_alias = str(materialize.get("alias") or "materialize_selected_payload")
    for operation in nested:
        if not isinstance(operation, dict):
            continue
        if operation.get("op") == "join":
            inputs = operation.pop("inputs", None)
            input_right = operation.get("input_right")
            if input_right is not None and inputs is not None:
                raise ValueError("0301 join은 input_right와 inputs를 동시에 선언할 수 없습니다.")
            if input_right is None:
                if not isinstance(inputs, dict):
                    raise ValueError("0301 join에는 inputs 또는 input_right가 필요합니다.")
                if inputs.get("left") != current_alias:
                    raise ValueError(
                        "0301 순차 join migration에서 inputs.left는 직전 operation alias여야 합니다."
                    )
                input_right = inputs.get("right")
                if not str(input_right or "").strip():
                    raise ValueError("0301 join inputs.right 값이 필요합니다.")
                operation["input_right"] = str(input_right)
            _reorder_mapping(
                operation,
                (
                    "op",
                    "alias",
                    "input_right",
                    "left_on",
                    "right_on",
                    "suffix",
                    "include_columns",
                    "exclude_columns",
                ),
            )
        current_alias = str(operation.get("alias") or current_alias)

    materialize.pop("source", None)
    materialize.pop("partition_by", None)
    materialize.pop("part_boundary", None)
    materialize["operations"] = nested
    _reorder_mapping(
        materialize,
        (
            "alias",
            "workers",
            "max_tasks_per_child",
            "operations",
        ),
    )
    payload["build_sidecar"] = keyspace
    payload["materialize"] = materialize
    payload.pop("operations", None)
    changes.append(
        _change(
            "operations materialize/join/save_dataset",
            "materialize phase with implicit terminal save",
        )
    )
    _reorder_mapping(
        payload,
        (
            "yaml",
            "job",
            "output",
            "define_upstream",
            "build_sidecar",
            "materialize",
            "execution",
        ),
    )


def _normalize_snapshot_phase_contract(
    payload: dict[str, Any], *, changes: list[dict[str, str]]
) -> None:
    """Normalize flat 0401 DAG YAML into the public single-snapshot phase contract."""

    header = payload.get("yaml")
    if not isinstance(header, dict) or str(header.get("asset_code") or "") != "0401":
        return
    if header.get("schema_version") == CURRENT_SNAPSHOT_SCHEMA:
        materialize = payload.get("materialize")
        if isinstance(materialize, dict) and isinstance(
            materialize.get("operations"), list
        ):
            operations = materialize["operations"]
            retained = [
                item
                for item in operations
                if not isinstance(item, dict) or item.get("op") != "data_assertion"
            ]
            if len(retained) != len(operations):
                materialize["operations"] = retained
                changes.append(
                    _change(
                        "0401 materialize.operations[].data_assertion",
                        "removed (0401 snapshot assertion contract removed)",
                    )
                )
        if isinstance(payload.get("build_sidecar"), dict):
            sidecar = payload["build_sidecar"]
            sidecar_operations = sidecar.get("operations")
            if isinstance(sidecar_operations, list):
                retained = [
                    item
                    for item in sidecar_operations
                    if not isinstance(item, dict)
                    or item.get("op") != "active_row_selection"
                ]
                if len(retained) != len(sidecar_operations):
                    if retained:
                        sidecar["operations"] = retained
                    else:
                        sidecar.pop("operations", None)
                    changes.append(
                        _change(
                            "0401 build_sidecar.operations[].active_row_selection",
                            "removed (all filtered rows become coordinates)",
                        )
                    )
            return
        upstreams = payload.get("define_upstream")
        materialize = payload.get("materialize")
        if (
            not isinstance(upstreams, list)
            or len(upstreams) != 1
            or not isinstance(upstreams[0], dict)
            or not isinstance(materialize, dict)
        ):
            raise ValueError(
                "0401 v8 coordinate 정규화에는 단일 define_upstream과 materialize가 필요합니다."
            )
        boundary = materialize.pop("part_boundary", None)
        if not isinstance(boundary, dict):
            boundary = {"target_rows": 20000}
        source_alias = str(upstreams[0].get("alias") or "").strip()
        if not source_alias:
            raise ValueError("0401 define_upstream.alias가 필요합니다.")
        payload["build_sidecar"] = {
            "alias": "select_snapshot_rows",
            "source": source_alias,
            "columns": "auto",
            "part_boundary": boundary,
        }
        _reorder_mapping(
            materialize,
            ("alias", "workers", "max_tasks_per_child", "operations"),
        )
        _reorder_mapping(
            payload,
            (
                "yaml",
                "job",
                "output",
                "define_upstream",
                "build_sidecar",
                "materialize",
                "execution",
            ),
        )
        changes.append(
            _change(
                "0401 materialize.part_boundary",
                "build_sidecar.part_boundary + direct coordinate collection",
            )
        )
        return
    operations = payload.get("operations")
    if not isinstance(operations, list):
        raise ValueError("0401 v6 YAML에는 operations 목록이 필요합니다.")
    upstreams = [
        deepcopy(item)
        for item in operations
        if isinstance(item, dict) and item.get("op") in {"define_asset", "define_dataset"}
    ]
    materializations = [
        deepcopy(item)
        for item in operations
        if isinstance(item, dict) and item.get("op") == "materialize"
    ]
    saves = [
        deepcopy(item)
        for item in operations
        if isinstance(item, dict) and item.get("op") == "save_dataset"
    ]
    if len(upstreams) != 1 or len(materializations) != 1 or len(saves) != 1:
        raise ValueError(
            "0401 정규화에는 source, materialize, save_dataset이 각각 정확히 하나 필요합니다."
        )
    upstream = upstreams[0]
    materialize = materializations[0]
    source_alias = str(upstream.get("alias") or "").strip()
    materialize_inputs = materialize.get("inputs")
    if not isinstance(materialize_inputs, dict) or str(
        materialize_inputs.get("source") or ""
    ) != source_alias:
        raise ValueError("0401 materialize.inputs.source가 단일 upstream alias와 일치해야 합니다.")
    save = saves[0]
    save_inputs = save.get("inputs")
    if not isinstance(save_inputs, dict) or not str(save_inputs.get("data") or "").strip():
        raise ValueError("0401 save_dataset.inputs.data가 필요합니다.")

    nested: list[dict[str, Any]] = []
    for item in operations:
        if not isinstance(item, dict) or item.get("op") in {
            "define_asset",
            "define_dataset",
            "materialize",
            "save_dataset",
            "data_assertion",
        }:
            continue
        normalized = deepcopy(item)
        normalized.pop("inputs", None)
        normalized.pop("partition_by", None)
        nested.append(normalized)
    boundary = deepcopy(materialize.get("part_boundary") or {"target_rows": 20000})
    materialize = {
        key: deepcopy(value)
        for key, value in materialize.items()
        if key not in {"op", "inputs", "partition_by", "part_boundary"}
    }
    materialize["operations"] = nested
    _reorder_mapping(
        materialize,
        ("alias", "workers", "max_tasks_per_child", "operations"),
    )
    header["schema_version"] = CURRENT_SNAPSHOT_SCHEMA
    payload["define_upstream"] = [upstream]
    payload["build_sidecar"] = {
        "alias": "select_snapshot_rows",
        "source": source_alias,
        "columns": "auto",
        "part_boundary": boundary,
    }
    payload["materialize"] = materialize
    payload.pop("operations", None)
    changes.append(_change(CURRENT_PIPELINE_SCHEMA, CURRENT_SNAPSHOT_SCHEMA))
    changes.append(
        _change(
            "0401 operations source/materialize/save_dataset",
            "define_upstream + materialize.operations + implicit single-file write",
        )
    )
    _reorder_mapping(
        payload,
        (
            "yaml",
            "job",
            "output",
            "define_upstream",
            "build_sidecar",
            "materialize",
            "execution",
        ),
    )


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
    artifact = {
        "type": artifact_type,
        "root_dir": root,
        "format": "sbdf" if asset_code == "0401" else "parquet",
        **({} if asset_code == "0401" else {"compression": "zstd"}),
        **(
            {
                "sbdf": {
                    "row_key_columns": ["partition"],
                    "batch_size": 50_000,
                    "encoding_rle": True,
                }
            }
            if asset_code == "0401"
            else {}
        ),
        "physical_layout": {
            "profile": {
                "0201": "curated_reuse_v1",
                "0301": "joined_reuse_v1",
                "0401": "analysis_snapshot_adaptive_v1",
            }[asset_code],
            "adaptation_scope": (
                "task_adaptive" if asset_code == "0401" else "generation_fixed"
            ),
            "row_group_rows": "auto",
        },
    }
    return {
        "artifact": {
            **artifact,
        },
        "logging": {"root_dir": logging_root},
    }


def _normalize_public_output(
    payload: dict[str, Any],
    *,
    asset_code: str,
    changes: list[dict[str, str]],
) -> None:
    """Normalize the user-facing dataset output envelope without policy fields."""

    output = payload.get("output")
    if not isinstance(output, dict):
        return
    artifact = output.get("artifact")
    if not isinstance(artifact, dict):
        return
    if asset_code == "0401":
        if artifact.get("format") != "sbdf":
            artifact["format"] = "sbdf"
            changes.append(_change("output.artifact.format", "sbdf (0401 default)"))
        artifact.pop("compression", None)
        if not isinstance(artifact.get("sbdf"), dict):
            artifact["sbdf"] = {
                "row_key_columns": _infer_0401_sbdf_keys(payload),
                "batch_size": 50_000,
                "encoding_rle": True,
            }
            changes.append(_change("0401 output keys", "output.artifact.sbdf"))
    if artifact.pop("write_policy", None) is not None:
        changes.append(
            _change("output.artifact.write_policy", "removed (atomic publication invariant)")
        )
    writer = artifact.get("parquet_writer")
    writer = writer if isinstance(writer, dict) else {}
    legacy_compression = writer.pop("compression", None)
    legacy_row_group_rows = writer.pop("row_group_size", None)
    if asset_code != "0401" and "compression" not in artifact:
        artifact["compression"] = str(legacy_compression or "zstd")
        changes.append(
            _change("output.artifact.parquet_writer.compression", "output.artifact.compression")
        )
    profiles = {
        "0101": "source_ingest_v1",
        "0102": "fact_append_v1",
        "0103": "source_ingest_v1",
        "0201": "curated_reuse_v1",
        "0301": "joined_reuse_v1",
        "0401": "analysis_snapshot_adaptive_v1",
    }
    layout = artifact.get("physical_layout")
    if not isinstance(layout, dict):
        layout = {}
        artifact["physical_layout"] = layout
    layout_changed = False
    for key, value in {
        "profile": profiles.get(asset_code, "reusable_dataset_v1"),
        "adaptation_scope": (
            "task_adaptive" if asset_code == "0401" else "generation_fixed"
        ),
        "row_group_rows": legacy_row_group_rows or "auto",
    }.items():
        if key not in layout:
            layout[key] = value
            layout_changed = True
    if layout_changed:
        changes.append(
            _change("output writer layout", "output.artifact.physical_layout")
        )
    if isinstance(artifact.get("parquet_writer"), dict) and not artifact["parquet_writer"]:
        artifact.pop("parquet_writer")
    if asset_code == "0101":
        _order_legacy_source_output(output)
    else:
        _reorder_mapping(output, ("artifact", "logging"))
        _reorder_mapping(
            artifact,
            (
                "type",
                "root_dir",
                "format",
                "compression",
                "physical_layout",
                "sbdf",
                "file_name_rule",
                "parquet_writer",
                "publication",
            ),
        )


def _infer_0401_sbdf_keys(payload: dict[str, Any]) -> list[str]:
    sidecar = payload.get("build_sidecar")
    if isinstance(sidecar, dict):
        boundary = sidecar.get("part_boundary")
        if isinstance(boundary, dict):
            values = boundary.get("preserve_groups")
            if isinstance(values, list) and values and all(str(value).strip() for value in values):
                return [str(value) for value in values]
    return ["partition"]


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
    converted = _current_pipeline(name, "0401", CURRENT_SNAPSHOT_SCHEMA, _pipeline_output(payload, artifact_type="analysis_snapshot", asset_code="0401"), operations)
    warnings.append("legacy filters, joins, column_rename and indexed_snapshot options require manual operation mapping.")
    changes.append(_change(f"stage.id {stage_id}", "yaml.asset_code 0401"))
    return converted, changes, warnings


def _current_pipeline(job_name: str, asset_code: str, schema: str, output: dict[str, Any], operations: list[dict[str, Any]], keys: list[str] | None = None) -> dict[str, Any]:
    if keys is not None:
        partition = keys[0]
        return {"yaml": {"schema_version": schema, "asset_code": asset_code}, "job": {"name": job_name}, "output": output, "define_upstream": [{**operations[0]}], "build_sidecar": {"alias": "select_rows", "source": operations[0]["alias"], "partition_by": [partition], "part_boundary": {"target_rows": 20000, "preserve_groups": keys}, "columns": "auto", "operations": [{"op": "active_row_selection", "method": "sort_first", "group_keys": list(keys), "sort": [{"column": partition, "direction": "asc", "nulls": "last"}]}]}, "materialize": {"alias": "materialize_rows", "workers": 1, "max_tasks_per_child": 1, "operations": []}, "execution": {"reset_before_run": False}}
    if asset_code == "0301":
        result = {"yaml": {"schema_version": schema, "asset_code": asset_code}, "job": {"name": job_name}, "output": output, "operations": operations, "execution": {"reset_before_run": False}}
        _normalize_join_upstream_contract(result, changes=[])
        return result
    if asset_code == "0401":
        result = {"yaml": {"schema_version": CURRENT_PIPELINE_SCHEMA, "asset_code": asset_code}, "job": {"name": job_name}, "output": output, "operations": operations, "execution": {"reset_before_run": False}}
        _normalize_snapshot_phase_contract(result, changes=[])
        return result
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
    return isinstance(header, dict) and header.get("schema_version") in {
        LEGACY_SOURCE_SCHEMA,
        CURRENT_SOURCE_SCHEMA,
    }


def _needs_source_normalization(payload: dict[str, Any]) -> bool:
    output = payload.get("output")
    artifact = output.get("artifact") if isinstance(output, dict) else None
    if isinstance(artifact, dict) and (
        "write_policy" in artifact
        or "compression" not in artifact
        or "physical_layout" not in artifact
    ):
        return True
    source = payload.get("source")
    if not isinstance(source, dict):
        return False
    request = source.get("api_request")
    if not isinstance(request, dict):
        return False
    if "table_id" in source or "payload" in request or "date_window" in request:
        return True
    sql = request.get("sql")
    if not isinstance(sql, dict):
        return False
    select = sql.get("select")
    return any(
        isinstance(item, dict)
        and list(item)[:2] != ["name", "expr"]
        and "expr" in item
        for item in select or []
    )


def _normalize_current_source(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, str]], list[str]]:
    """Canonicalize an old internal layout that was labeled as source.v4."""

    converted = deepcopy(payload)
    source = _required_mapping(converted, "source")
    request = _required_mapping(source, "api_request")
    changes: list[dict[str, str]] = []
    warnings = [
        "0101 Source YAML을 source.v5 canonical 구조로 정규화했습니다."
    ]
    converted.setdefault("yaml", {})["schema_version"] = CURRENT_SOURCE_SCHEMA
    if _source_schema(payload) != CURRENT_SOURCE_SCHEMA:
        changes.append(_change(LEGACY_SOURCE_SCHEMA, CURRENT_SOURCE_SCHEMA))

    source_table_id = source.pop("table_id", None)
    sql = request.get("sql")
    if not isinstance(sql, dict):
        sql = {"table_id": source_table_id}
        legacy_payload = request.pop("payload", None)
        if isinstance(legacy_payload, dict):
            sql.update(legacy_payload)
        for key in ("sql_file_path", "date_window"):
            if key in request:
                sql[key] = request.pop(key)
        request["sql"] = sql
        changes.append(_change("source.table_id/api_request.payload", "source.api_request.sql"))
    else:
        if source_table_id is not None and "table_id" not in sql:
            sql["table_id"] = source_table_id
            changes.append(_change("source.table_id", "source.api_request.sql.table_id"))
        if "payload" in request:
            legacy_payload = request.pop("payload")
            if isinstance(legacy_payload, dict):
                for key, value in legacy_payload.items():
                    sql.setdefault(key, value)
            changes.append(_change("source.api_request.payload", "source.api_request.sql"))
        for key in ("sql_file_path", "date_window"):
            if key in request:
                sql.setdefault(key, request.pop(key))
                changes.append(_change(f"source.api_request.{key}", f"source.api_request.sql.{key}"))

    _normalize_legacy_source_filters(request, changes=changes, warnings=warnings)
    _order_legacy_source_fields(source, request)
    _normalize_public_output(converted, asset_code="0101", changes=changes)
    return converted, changes, warnings


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
    changes: list[dict[str, str]] = []
    warnings: list[str] = []
    if asset is None:
        source_candidate = payload.get("source")
        if not isinstance(source_candidate, dict) or not isinstance(
            source_candidate.get("api_request"), dict
        ) or not isinstance(payload.get("output"), dict):
            raise ValueError("legacy Source YAML의 0101 식별 정보가 없습니다.")
        changes.append(_change("implicit legacy source asset", "asset_code 0101"))
        warnings.append("legacy Source YAML에 asset.code가 없어 source 구조를 기준으로 0101을 추론했습니다.")
        asset = {"code": "0101"}
    if not isinstance(asset, dict) or str(asset.get("code")) != "0101":
        raise ValueError("현재 결정론적 변환기는 legacy Source asset code 0101만 지원합니다.")
    source = _required_mapping(payload, "source")
    request = _required_mapping(source, "api_request")
    legacy_output = _required_mapping(payload, "output")
    legacy_job = _required_mapping(payload, "job")

    converted_source = deepcopy(source)
    converted_request = deepcopy(request)
    _drop_legacy_source_types(
        payload,
        converted_source=converted_source,
        converted_request=converted_request,
        legacy_output=legacy_output,
        changes=changes,
        warnings=warnings,
    )
    legacy_table_id = converted_source.pop("table_id", None)
    if not str(legacy_table_id or "").strip():
        legacy_table_id = str(legacy_job.get("name") or "source")
        changes.append(_change("missing source.table_id", "source.api_request.sql.table_id"))
        warnings.append(
            "legacy Source YAML에 source.table_id가 없어 job.name을 SQL table_id로 사용했습니다."
        )
    converted_request["sql"] = _build_legacy_sql_definition(
        converted_request=converted_request,
        table_id=legacy_table_id,
    )
    _normalize_legacy_source_filters(
        converted_request,
        changes=changes,
        warnings=warnings,
    )
    job = _normalize_legacy_source_job(legacy_job, changes, warnings)

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
    _order_legacy_source_fields(converted_source, converted_request)
    converted_source["api_request"] = converted_request

    output = _convert_output(legacy_output, changes, warnings)
    execution = _convert_execution(payload.get("execution"), changes, warnings)
    _order_legacy_source_execution(execution)
    _order_legacy_source_output(output)
    converted: dict[str, Any] = {
        "yaml": {
            "schema_version": CURRENT_SOURCE_SCHEMA,
            "asset_code": "0101",
        },
        "job": job,
        "source": converted_source,
        "execution": execution,
        "output": output,
    }
    return converted, changes, warnings


def _build_legacy_sql_definition(
    *,
    converted_request: dict[str, Any],
    table_id: Any,
) -> dict[str, Any]:
    """Collect legacy SQL-building fields into the current ``sql`` block."""

    sql: dict[str, Any] = {"table_id": table_id}
    legacy_payload = converted_request.pop("payload", None)
    if isinstance(legacy_payload, dict):
        sql.update(legacy_payload)
    for key in ("sql_file_path", "date_window"):
        if key in converted_request:
            sql[key] = converted_request.pop(key)
    return sql


def _order_legacy_source_fields(
    source: dict[str, Any],
    request: dict[str, Any],
) -> None:
    """Arrange migrated 0101 fields in the current authoring order."""

    _reorder_mapping(source, ("table_id", "api_request"))
    _reorder_mapping(
        request,
        ("query_mode", "adapter", "adapter_options", "sql", "http"),
    )
    sql = request.get("sql")
    if isinstance(sql, dict):
        _reorder_mapping(sql, ("table_id", "select", "filters", "sql_file_path", "date_window"))
        select = sql.get("select")
        if isinstance(select, list):
            for item in select:
                if isinstance(item, dict):
                    _reorder_mapping(item, ("name", "expr"))
        filters = sql.get("filters")
        if isinstance(filters, dict):
            _reorder_mapping(filters, ("common", "sub_job"))
            sub_jobs = filters.get("sub_job")
            if isinstance(sub_jobs, list):
                for item in sub_jobs:
                    if isinstance(item, dict):
                        _reorder_mapping(item, ("sub_job_name", "sub_job_filtering"))
    date_window = sql.get("date_window") if isinstance(sql, dict) else None
    if isinstance(date_window, dict):
        _reorder_mapping(date_window, ("column", "step", "date_window"))


def _order_legacy_source_execution(execution: dict[str, Any]) -> None:
    _reorder_mapping(
        execution,
        (
            "reset_before_run",
            "write_source_profile_json",
            "retryable_error_substrings",
            "data_api_print_capture",
            "workers",
            "warmup_first_task",
            "worker_start_delay_sec",
            "max_retries",
            "retry_backoff_sec",
            "test_run",
        ),
    )
    capture = execution.get("data_api_print_capture")
    if isinstance(capture, dict):
        _reorder_mapping(capture, ("rules",))
        rules = capture.get("rules")
        if isinstance(rules, dict):
            _reorder_mapping(rules, ("enabled", "fields"))
            fields = rules.get("fields")
            if isinstance(fields, list):
                for item in fields:
                    if isinstance(item, dict):
                        _reorder_mapping(item, ("field", "enabled", "capture", "regex"))


def _order_legacy_source_output(output: dict[str, Any]) -> None:
    _reorder_mapping(output, ("artifact", "logging"))
    artifact = output.get("artifact")
    if isinstance(artifact, dict):
        _reorder_mapping(
            artifact,
            (
                "type",
                "root_dir",
                "format",
                "compression",
                "physical_layout",
                "file_name_rule",
                "parquet_writer",
                "publication",
            ),
        )
        file_name_rule = artifact.get("file_name_rule")
        if isinstance(file_name_rule, dict):
            _reorder_mapping(file_name_rule, ("raw_dataset",))
        writer = artifact.get("parquet_writer")
        if isinstance(writer, dict):
            _reorder_mapping(
                writer,
                (
                    "index",
                    "engine",
                    "compression",
                    "row_group_size",
                    "write_page_index",
                    "write_statistics",
                    "data_page_size",
                    "max_rows_per_page",
                    "use_dictionary",
                ),
            )
    logging = output.get("logging")
    if isinstance(logging, dict):
        _reorder_mapping(logging, ("root_dir", "file_name_rule"))
        file_name_rule = logging.get("file_name_rule")
        if isinstance(file_name_rule, dict):
            _reorder_mapping(file_name_rule, ("logging",))


def _reorder_mapping(mapping: dict[str, Any], preferred_keys: tuple[str, ...]) -> None:
    """Move known keys to a stable order while retaining unknown keys at the end."""

    ordered: dict[str, Any] = {
        key: mapping[key] for key in preferred_keys if key in mapping
    }
    ordered.update({key: value for key, value in mapping.items() if key not in ordered})
    mapping.clear()
    mapping.update(ordered)


def _normalize_legacy_source_filters(
    request: dict[str, Any],
    *,
    changes: list[dict[str, str]],
    warnings: list[str],
) -> None:
    """Translate the permissive legacy filter aliases to the v4 Source shape."""

    container = request.get("sql") or request.get("payload")
    if not isinstance(container, dict) or "filters" not in container:
        return

    raw = container.get("filters")
    if raw is None:
        container["filters"] = {"common": [], "sub_job": []}
        changes.append(_change("source.api_request.filters: null", "filters.common/sub_job"))
        warnings.append("legacy 0101 filters의 null 값을 빈 canonical filter 블록으로 변환했습니다.")
        return

    if isinstance(raw, list):
        container["filters"] = {"common": [str(item) for item in raw], "sub_job": []}
        changes.append(_change("source.api_request.filters (list)", "filters.common"))
        warnings.append("legacy 0101 filters list를 filters.common 목록으로 변환했습니다.")
        return

    if not isinstance(raw, dict):
        raise ValueError("legacy source.api_request.payload.filters는 list 또는 object여야 합니다.")

    unknown = sorted(set(raw) - {"common", "sub_job", "sub_jobs"})
    if unknown:
        raise ValueError(f"legacy source.api_request.payload.filters의 알 수 없는 키입니다: {unknown}")
    if "sub_job" in raw and "sub_jobs" in raw:
        raise ValueError(
            "legacy source.api_request.payload.filters에 sub_job과 sub_jobs를 동시에 사용할 수 없습니다."
        )

    common = raw.get("common", [])
    if common is None:
        common_filters: list[str] = []
    elif isinstance(common, str):
        common_filters = [common]
    elif isinstance(common, list):
        common_filters = [str(item) for item in common]
    else:
        raise ValueError("legacy source.api_request.payload.filters.common은 str 또는 list여야 합니다.")

    raw_sub_jobs = raw.get("sub_job", raw.get("sub_jobs"))
    if raw_sub_jobs is None:
        sub_job_items: list[Any] = []
    elif isinstance(raw_sub_jobs, dict):
        sub_job_items = [raw_sub_jobs]
    elif isinstance(raw_sub_jobs, list):
        sub_job_items = raw_sub_jobs
    else:
        raise ValueError("legacy source.api_request.payload.filters.sub_job은 dict 또는 list여야 합니다.")

    sub_jobs: list[dict[str, Any]] = []
    used_alias = "sub_jobs" in raw
    for index, item in enumerate(sub_job_items, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"legacy filters.sub_job[{index}]은 object여야 합니다.")
        name = item.get("sub_job_name", item.get("name"))
        if not str(name or "").strip():
            raise ValueError(f"legacy filters.sub_job[{index}].sub_job_name 값이 필요합니다.")
        raw_filter_values = item.get("sub_job_filtering", item.get("filters", []))
        if raw_filter_values is None:
            filter_values: list[str] = []
        elif isinstance(raw_filter_values, str):
            filter_values = [raw_filter_values]
        elif isinstance(raw_filter_values, list):
            filter_values = [str(value) for value in raw_filter_values]
        else:
            raise ValueError(
                f"legacy filters.sub_job[{index}].sub_job_filtering은 str 또는 list여야 합니다."
            )
        sub_jobs.append(
            {
                "sub_job_name": str(name).strip(),
                "sub_job_filtering": filter_values,
            }
        )

    canonical = {"common": common_filters, "sub_job": sub_jobs}
    if raw != canonical:
        container["filters"] = canonical
        changes.append(_change("source.api_request.filters (legacy aliases)", "filters.common/sub_job"))
        warnings.append("legacy 0101 filters alias를 현재 canonical 형태로 정규화했습니다.")
    elif used_alias:
        container["filters"] = canonical
        changes.append(_change("source.api_request.filters.sub_jobs", "filters.sub_job"))
        warnings.append("legacy 0101 filters.sub_jobs를 filters.sub_job으로 정규화했습니다.")


def _normalize_legacy_source_job(
    legacy_job: dict[str, Any],
    changes: list[dict[str, str]],
    warnings: list[str],
) -> dict[str, Any]:
    """Keep only fields supported by the current 0101 job contract."""

    job = {key: deepcopy(legacy_job[key]) for key in ("name", "description") if key in legacy_job}
    discarded = sorted(set(legacy_job) - set(job))
    for key in discarded:
        changes.append(_change(f"job.{key}", "removed (unused legacy 0101 field)"))
    if discarded:
        warnings.append("legacy 0101 job의 현재 계약 외 필드를 제거했습니다.")
    return job


def _drop_legacy_source_types(
    payload: dict[str, Any],
    *,
    converted_source: dict[str, Any],
    converted_request: dict[str, Any],
    legacy_output: dict[str, Any],
    changes: list[dict[str, str]],
    warnings: list[str],
) -> None:
    """Remove unused type markers from the legacy 0101 Source contract.

    The current Source contract fixes the asset identity and output artifact
    type in its own schema. Legacy source files may still carry redundant
    markers at these legacy locations; none of them are copied to the current
    document. The current output artifact type is intentionally not handled
    here because it is a required field of the new contract.
    """

    legacy_mappings = (
        ("asset", payload.get("asset")),
        ("source", converted_source),
        ("source.api_request", converted_request),
        ("output", legacy_output),
    )
    removed = False
    removed_type = False
    for path, value in legacy_mappings:
        if isinstance(value, dict) and "type" in value:
            value.pop("type", None)
            changes.append(_change(f"{path}.type", "removed (unused legacy 0101 field)"))
            removed = True
            removed_type = True
    if "type" in payload:
        changes.append(_change("type", "removed (unused legacy 0101 field)"))
        removed = True
        removed_type = True
    for key in ("stage", "stage_id", "asset_code"):
        if key in payload:
            changes.append(_change(key, "removed (legacy identity field)"))
            removed = True
    for path, value in (("source", converted_source), ("source.api_request", converted_request)):
        if not isinstance(value, dict):
            continue
        for key in ("stage", "stage_id", "asset", "asset_code"):
            if key in value:
                value.pop(key, None)
                changes.append(_change(f"{path}.{key}", "removed (legacy identity field)"))
                removed = True
    if removed:
        if removed_type:
            warnings.append("legacy 0101의 사용되지 않는 식별·type 필드를 제거했습니다.")
        else:
            warnings.append("legacy 0101의 사용되지 않는 식별 필드를 제거했습니다.")


def _convert_output(
    legacy: dict[str, Any],
    changes: list[dict[str, str]],
    warnings: list[str],
) -> dict[str, Any]:
    root_dir = legacy.get("output_dir")
    if not root_dir:
        raise ValueError("legacy output.output_dir가 필요합니다.")
    file_rule = _required_mapping(legacy, "file_name_rule")
    writer = deepcopy(legacy.get("parquet_writer", {}))
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
            "compression": str(writer.pop("compression", "zstd")),
            "physical_layout": {
                "profile": "source_ingest_v1",
                "adaptation_scope": "generation_fixed",
                "row_group_rows": writer.pop("row_group_size", 1000),
            },
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


def _convert_legacy_publication(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, str]], list[str]]:
    aws = _mapping(payload.get("aws"), "aws")
    sync = _mapping(payload.get("sync"), "sync")
    target = str(aws.get("profile_name") or "default").replace(" ", "_")
    prefix = str(sync.get("s3_prefix") or "legacy-migration")
    converted = {
        "yaml": {"schema_version": CURRENT_PUBLICATION_SCHEMA},
        "job": deepcopy(_required_mapping(payload, "job")),
        "publication": {
            "enabled": True,
            "target": target,
            "dataset_prefix": prefix,
            "mode": "mirror_after_local_commit",
            "failure_policy": "required",
            "parquet": {
                "enabled": True,
                "random_access_index": {
                    "level": "row_group",
                    "writer_page_index": "disabled",
                    "key_columns": [],
                    "key_null_policy": "error",
                    "key_hash": "sha256_trunc128_v1",
                    "hash_buckets": 256,
                },
            },
            "sbdf": {
                "enabled": False,
                "shard_policy": "mirror_parquet_parts",
                "row_key_columns": [],
                "batch_size": 65536,
                "encoding_rle": True,
                "key_hash": "sha256_trunc128_v1",
                "hash_buckets": 256,
            },
            "verification": {
                "checksum": "sha256",
                "verify_remote_head": True,
                "verify_sidecar_references": True,
            },
        },
    }
    changes = [
        _change("stage.id etl.04.01", f"yaml.schema_version {CURRENT_PUBLICATION_SCHEMA}"),
        _change("aws.profile_name", "publication.target"),
        _change("sync.s3_prefix", "publication.dataset_prefix"),
    ]
    warnings = [
        "legacy sync.mode download/sync는 현재 mirror_after_local_commit publication으로 직접 실행되지 않습니다.",
        "legacy AWS credentials/access_key_id/secret_access_key는 보안상 변환 결과에서 제거했습니다.",
        "생성 결과는 publication fragment이며 Asset 0401 Snapshot YAML이 아닙니다.",
    ]
    return converted, changes, warnings


def _required_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key}는 object여야 합니다.")
    return value


def _change(source: str, target: str) -> dict[str, str]:
    return {"from": source, "to": target}
