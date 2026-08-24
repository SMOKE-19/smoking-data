"""SOURCE YAML을 최종 SourceSpec으로 조립하는 메인 파서."""

from __future__ import annotations

import re
from copy import deepcopy
from pathlib import Path

from smoking_data.assets.a0101_source.core.yaml_loader import resolve_yaml_literals
from smoking_data.assets.a0101_source.spec_common.project import (
    ProjectPaths,
    load_project_paths,
)
from smoking_data.assets.a0101_source.spec_common.sections import with_yaml_error_context
from smoking_data.runtime.asset_config import deep_merge
from smoking_data.runtime.object_store.config import PublicationSpec

from .defaults import apply_asset_defaults, load_yaml_dict
from .models import (
    SourceApiPrintCaptureRule,
    SourceExecutionSpec,
    SourceJobSpec,
    SourceLoggingSpec,
    SourceSpec,
    SourceStorageSpec,
)
from .sections import (
    get_value,
    parse_request,
    require_str,
)

SOURCE_SCHEMA_VERSION = "smoking-data.source.v4"
SOURCE_EXECUTION_KEYS = {
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
}


def load_source_spec(path: str | Path) -> SourceSpec:
    yaml_path = Path(path).resolve()
    try:
        project = load_project_paths(yaml_path)
        raw = load_yaml_dict(yaml_path)
        _validate_asset_contract(raw)
        configured_execution = project.config_payload.get("execution", {})
        runtime_defaults = {
            "execution": {
                key: value
                for key, value in configured_execution.items()
                if key in SOURCE_EXECUTION_KEYS
            }
        }
        defaults = deep_merge(project.contract_definition, runtime_defaults)
        merged = apply_asset_defaults(raw=raw, defaults=defaults)
        # Definition YAML is an override document. Validate the complete effective
        # contract after bundled/workspace config merging so ``output: {}`` remains
        # a valid Slim authoring form without weakening unknown-key rejection.
        _validate_source_yaml_shape(merged)
        global_scope = project.template_scope(
            job_name=str(merged.get("job", {}).get("name") or ""),
            asset_code="0101",
            extra_scope={
                "table_id": str(merged.get("source", {}).get("table_id") or ""),
            },
        )
        # HTTP query/header values may intentionally contain runtime-only ${ENV}
        # credentials. Keep the HTTP block out of authoring-template resolution so
        # secrets are neither required nor expanded while loading the definition.
        resolution_input = deepcopy(merged)
        runtime_http = (
            resolution_input.get("source", {}).get("api_request", {}).pop("http", None)
        )
        resolved = resolve_yaml_literals(
            resolution_input, extra_scope=global_scope, yaml_path=yaml_path
        )
        if runtime_http is not None:
            resolved["source"]["api_request"]["http"] = runtime_http
        request = parse_request(
            resolved, path_resolver=lambda value: str(project.resolve_path(value))
        )

        return SourceSpec(
            schema_version=SOURCE_SCHEMA_VERSION,
            path=yaml_path,
            project=project,
            raw=merged,
            resolved=resolved,
            job=SourceJobSpec(
                name=require_str(resolved, "job", "name"),
                description=str(get_value(resolved, "job", "description") or ""),
            ),
            request=request,
            storage=SourceStorageSpec(
                temp_root=str(project.temp_root),
                root_dir=str(project.data_root),
                raw_dir=str(
                    project.resolve_path(
                        require_str(resolved, "output", "artifact", "root_dir")
                    )
                ),
                raw_dataset_file_name=require_str(
                    resolved, "output", "artifact", "file_name_rule", "raw_dataset"
                ),
                parquet_writer_options=_parse_parquet_writer_options(resolved),
            ),
            logging=SourceLoggingSpec(
                path=_build_output_artifact_path(
                    project,
                    resolved,
                    section="logging",
                    file_rule_key="logging",
                ),
            ),
            execution=SourceExecutionSpec(
                reset_before_run=bool(
                    get_value(resolved, "execution", "reset_before_run") or False
                ),
                write_source_profile_json=bool(
                    get_value(resolved, "execution", "write_source_profile_json")
                ),
                retryable_error_substrings=_parse_retryable_error_substrings(resolved),
                data_api_print_capture_rules=_parse_data_api_print_capture_rules(
                    resolved
                ),
                workers=int(get_value(resolved, "execution", "workers") or 1),
                warmup_first_task=bool(
                    get_value(resolved, "execution", "warmup_first_task")
                ),
                worker_start_delay_sec=float(
                    get_value(resolved, "execution", "worker_start_delay_sec") or 0.0
                ),
                max_retries=int(get_value(resolved, "execution", "max_retries") or 0),
                retry_backoff_sec=float(
                    get_value(resolved, "execution", "retry_backoff_sec") or 0.0
                ),
                test_run_final_task_limit=_parse_test_run_final_task_limit(resolved),
            ),
        )
    except (ValueError, KeyError) as exc:
        raise with_yaml_error_context(exc, yaml_path) from exc


def _validate_source_yaml_shape(raw: dict[str, object]) -> None:
    _reject_unknown_keys(
        raw,
        {"yaml", "job", "source", "output", "execution"},
        path="$",
    )
    _reject_unknown_keys(
        _mapping(raw.get("yaml"), path="yaml"),
        {"schema_version", "asset_code"},
        path="yaml",
    )
    _reject_unknown_keys(
        _mapping(raw.get("job"), path="job"),
        {"name", "description"},
        path="job",
    )
    source = _mapping(raw.get("source"), path="source")
    _reject_unknown_keys(source, {"table_id", "api_request"}, path="source")
    request = _mapping(source.get("api_request"), path="source.api_request")
    _reject_unknown_keys(
        request,
        {"query_mode", "payload", "sql_file_path", "date_window", "http", "spi"},
        path="source.api_request",
    )
    payload = request.get("payload")
    if payload is not None:
        payload_mapping = _mapping(payload, path="source.api_request.payload")
        _reject_unknown_keys(
            payload_mapping,
            {"select", "filters"},
            path="source.api_request.payload",
        )
        select = payload_mapping.get("select")
        if select is not None:
            if not isinstance(select, list):
                raise ValueError("source.api_request.payload.select 는 list여야 합니다.")
            for index, item in enumerate(select):
                _reject_unknown_keys(
                    _mapping(item, path=f"source.api_request.payload.select[{index}]"),
                    {"name", "expr"},
                    path=f"source.api_request.payload.select[{index}]",
                )
    date_window = request.get("date_window")
    if date_window is not None:
        _reject_unknown_keys(
            _mapping(date_window, path="source.api_request.date_window"),
            {"column", "step", "date_window"},
            path="source.api_request.date_window",
        )

    execution = raw.get("execution")
    if execution is not None:
        execution_mapping = _mapping(execution, path="execution")
        _reject_unknown_keys(
            execution_mapping,
            SOURCE_EXECUTION_KEYS,
            path="execution",
        )
        capture = execution_mapping.get("data_api_print_capture")
        if capture is not None:
            capture_mapping = _mapping(capture, path="execution.data_api_print_capture")
            _reject_unknown_keys(
                capture_mapping,
                {"rules"},
                path="execution.data_api_print_capture",
            )
            rules = capture_mapping.get("rules")
            if rules is not None:
                rules_mapping = _mapping(rules, path="execution.data_api_print_capture.rules")
                _reject_unknown_keys(
                    rules_mapping,
                    {"enabled", "fields"},
                    path="execution.data_api_print_capture.rules",
                )
                fields = rules_mapping.get("fields")
                if fields is not None:
                    if not isinstance(fields, list):
                        raise ValueError(
                            "execution.data_api_print_capture.rules.fields 는 list여야 합니다."
                        )
                    for index, item in enumerate(fields):
                        _reject_unknown_keys(
                            _mapping(
                                item,
                                path=(
                                    "execution.data_api_print_capture.rules."
                                    f"fields[{index}]"
                                ),
                            ),
                            {"field", "enabled", "capture", "regex"},
                            path=f"execution.data_api_print_capture.rules.fields[{index}]",
                        )
        test_run = execution_mapping.get("test_run")
        if test_run is not None:
            test_run_mapping = _mapping(test_run, path="execution.test_run")
            _reject_unknown_keys(
                test_run_mapping,
                {"final_task_limit"},
                path="execution.test_run",
            )
            limit = test_run_mapping.get("final_task_limit")
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError("execution.test_run.final_task_limit은 1 이상의 정수여야 합니다.")

    output = _mapping(raw.get("output"), path="output")
    _reject_unknown_keys(
        output,
        {"artifact", "logging"},
        path="output",
    )
    artifact = _mapping(output.get("artifact"), path="output.artifact")
    _reject_unknown_keys(
        artifact,
        {"type", "root_dir", "format", "write_policy", "file_name_rule", "parquet_writer", "publication"},
        path="output.artifact",
    )
    if artifact.get("type") != "source_dataset":
        raise ValueError("output.artifact.type은 'source_dataset'이어야 합니다.")
    if artifact.get("format") != "parquet":
        raise ValueError("output.artifact.format은 'parquet'이어야 합니다.")
    if artifact.get("write_policy") != "atomic_replace":
        raise ValueError("output.artifact.write_policy는 'atomic_replace'여야 합니다.")
    file_name_rule = artifact.get("file_name_rule")
    if file_name_rule is not None:
        _reject_unknown_keys(
            _mapping(file_name_rule, path="output.artifact.file_name_rule"),
            {"raw_dataset"},
            path="output.artifact.file_name_rule",
        )
    parquet_writer = artifact.get("parquet_writer")
    if parquet_writer is not None:
        parquet_writer = _mapping(parquet_writer, path="output.artifact.parquet_writer")
        _reject_unknown_keys(
            parquet_writer,
            {
                "index",
                "engine",
                "compression",
                "row_group_size",
                "write_page_index",
                "write_statistics",
                "data_page_size",
                "max_rows_per_page",
                "use_dictionary",
            },
            path="output.artifact.parquet_writer",
        )
        compression = str(parquet_writer.get("compression") or "zstd").lower()
        if compression not in {"snappy", "zstd", "uncompressed"}:
            raise ValueError(
                "output.artifact.parquet_writer.compression은 "
                "snappy, zstd, uncompressed 중 하나여야 합니다."
            )
    PublicationSpec.from_mapping(artifact.get("publication"))
    for section_name, rule_name in (("logging", "logging"),):
        section = _mapping(output.get(section_name), path=f"output.{section_name}")
        _reject_unknown_keys(
            section,
            {"root_dir", "file_name_rule"},
            path=f"output.{section_name}",
        )
        file_rule = _mapping(
            section.get("file_name_rule"),
            path=f"output.{section_name}.file_name_rule",
        )
        _reject_unknown_keys(
            file_rule,
            {rule_name},
            path=f"output.{section_name}.file_name_rule",
        )


def _mapping(value: object, *, path: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{path} 는 dict여야 합니다.")
    return value


def _reject_unknown_keys(
    payload: dict[str, object], allowed: set[str], *, path: str
) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise ValueError(f"{path} 의 지원하지 않는 SOURCE 키입니다: {unknown}")


def _validate_asset_contract(raw: dict[str, object]) -> None:
    yaml_header = _mapping(raw.get("yaml"), path="yaml")
    schema_version = str(yaml_header.get("schema_version") or "").strip()
    if schema_version != SOURCE_SCHEMA_VERSION:
        raise ValueError(
            f"schema_version은 {SOURCE_SCHEMA_VERSION!r}이어야 합니다: "
            f"{schema_version or '<missing>'!r}"
        )
    asset_code = str(yaml_header.get("asset_code") or "").strip()
    if asset_code != "0101":
        raise ValueError(f"yaml.asset_code는 '0101'이어야 합니다: {asset_code!r}")


def _build_output_artifact_path(
    project: ProjectPaths,
    resolved: dict[str, object],
    *,
    section: str,
    file_rule_key: str,
) -> str:
    directory = require_str(resolved, "output", section, "root_dir")
    file_name = require_str(
        resolved, "output", section, "file_name_rule", file_rule_key
    )
    relative_file = Path(file_name)
    if (
        relative_file.is_absolute()
        or relative_file.name != file_name
        or file_name in {".", ".."}
    ):
        raise ValueError(
            f"output.{section}.file_name_rule.{file_rule_key} 는 디렉터리를 포함하지 않는 "
            "파일명이어야 합니다: "
            f"{file_name!r}"
        )
    resolved_directory = project.resolve_path(directory)
    return str(resolved_directory / file_name)


def _parse_retryable_error_substrings(resolved: dict[str, object]) -> list[str]:
    raw_value = get_value(resolved, "execution", "retryable_error_substrings")
    if raw_value is None:
        return []
    if not isinstance(raw_value, list):
        raise ValueError("execution.retryable_error_substrings 는 리스트여야 합니다.")
    result: list[str] = []
    for index, item in enumerate(raw_value, start=1):
        text = str(item).strip()
        if not text:
            raise ValueError(
                f"execution.retryable_error_substrings[{index}] 값이 비어 있습니다."
            )
        result.append(text.lower())
    return result


def _parse_parquet_writer_options(resolved: dict[str, object]) -> dict[str, object]:
    payload = get_value(resolved, "output", "artifact", "parquet_writer")
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("output.artifact.parquet_writer 는 dict 여야 합니다.")
    return {str(key): value for key, value in payload.items()}


def _parse_test_run_final_task_limit(resolved: dict[str, object]) -> int | None:
    payload = get_value(resolved, "execution", "test_run")
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ValueError("execution.test_run은 dict여야 합니다.")
    value = payload.get("final_task_limit")
    return int(value) if value is not None else None


def _parse_data_api_print_capture_rules(
    resolved: dict[str, object],
) -> list[SourceApiPrintCaptureRule]:
    payload = get_value(resolved, "execution", "data_api_print_capture")
    if not isinstance(payload, dict):
        return []
    if "enabled" in payload:
        raise ValueError(
            "execution.data_api_print_capture.enabled 계약은 제거되었습니다. "
            "execution.data_api_print_capture.rules.enabled를 사용하세요."
        )
    rules_config = get_value(payload, "rules")
    if rules_config is None:
        return []
    if not isinstance(rules_config, dict):
        raise ValueError("execution.data_api_print_capture.rules 는 dict 여야 합니다.")
    if not bool(get_value(rules_config, "enabled")):
        return []
    raw_rules = get_value(rules_config, "fields")
    if raw_rules is None:
        return []
    if not isinstance(raw_rules, list):
        raise ValueError("execution.data_api_print_capture.rules.fields 는 리스트여야 합니다.")
    result: list[SourceApiPrintCaptureRule] = []
    for index, item in enumerate(raw_rules, start=1):
        if not isinstance(item, dict):
            raise ValueError(
                f"execution.data_api_print_capture.rules.fields[{index}] 는 dict 여야 합니다."
            )
        field = str(item.get("field") or "").strip()
        if not field:
            raise ValueError(
                f"execution.data_api_print_capture.rules.fields[{index}].field 값이 비어 있습니다."
            )
        enabled = bool(item.get("enabled", True))
        capture = str(item.get("capture") or "regex").strip().lower()
        if capture not in {"regex", "full_text"}:
            raise ValueError(
                f"execution.data_api_print_capture.rules.fields[{index}].capture 는 "
                "regex 또는 full_text 여야 합니다."
            )
        regex = str(item.get("regex") or "").strip() or None
        if capture == "regex":
            if not regex:
                raise ValueError(
                    f"execution.data_api_print_capture.rules.fields[{index}].regex 값이 비어 있습니다."
                )
            try:
                re.compile(regex)
            except re.error as exc:
                raise ValueError(
                    f"execution.data_api_print_capture.rules.fields[{index}].regex 가 "
                    f"올바른 정규식이 아닙니다: {exc}"
                ) from exc
        result.append(
            SourceApiPrintCaptureRule(
                field=field,
                enabled=enabled,
                capture=capture,
                regex=regex,
            )
        )
    return result
