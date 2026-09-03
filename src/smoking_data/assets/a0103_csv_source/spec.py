from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from smoking_data.core.exceptions import ValidationError
from smoking_data.ops.projection import POLARS_TYPE_MAP, resolve_add_calc_expression
from smoking_data.runtime.asset_config import deep_merge, load_effective_asset_config
from smoking_data.runtime.config import load_config
from smoking_data.runtime.object_store.config import PublicationSpec
from smoking_data.runtime.paths import file_sha256, infer_project_root, resolve_project_path
from smoking_data.runtime.publication_defaults import publication_aware_defaults
from smoking_data.runtime.template_resolution import resolve_contract_templates

CSV_SOURCE_SCHEMA_VERSION = "smoking-data.csv-source.v1"


@dataclass(frozen=True, slots=True)
class CsvSourceSpec:
    yaml_path: Path
    yaml_hash: str
    project_root: Path
    job_id: str
    job_name: str
    source_transport: str
    source_directory: Path | None
    download_options: dict[str, Any] | None
    glob: str
    recursive: bool
    ordering: str
    empty_directory_policy: str
    csv_options: dict[str, Any]
    file_name_column: str
    operations: tuple[dict[str, Any], ...]
    routes: tuple[dict[str, Any], ...]
    overlap_policy: str
    unmatched_policy: str
    output_root: Path
    compression: str
    dataset_rule: str
    parquet_rule: str
    posix_separator_replacement: str
    windows_separator_replacement: str
    collision_hash_length: int
    row_group_size: int | None
    publication: PublicationSpec | None
    target_rows_per_part: int
    resolved: dict[str, Any]


def load_csv_source_spec(
    yaml_path: str | Path,
    *,
    project_root: str | Path | None = None,
) -> CsvSourceSpec:
    path = Path(yaml_path).expanduser().resolve()
    root = (
        Path(project_root).expanduser().resolve()
        if project_root is not None
        else infer_project_root(path)
    )
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        _fail("0103 YAML root must be a mapping.", path="$")
    defaults = publication_aware_defaults(
        load_effective_asset_config(root, "0103").payload,
        payload=payload,
        asset_code="0103",
    )
    effective = deep_merge(
        {key: value for key, value in defaults.items() if key not in {"config", "paths", "contract"}},
        payload,
    )
    _keys(
        effective,
        {"yaml", "job", "source", "materialize", "routing", "output", "execution"},
        path="$",
    )
    header = _mapping(effective.get("yaml"), path="yaml")
    _keys(header, {"schema_version", "asset_code"}, path="yaml")
    if header.get("schema_version") != CSV_SOURCE_SCHEMA_VERSION or str(
        header.get("asset_code") or ""
    ) != "0103":
        _fail("0103 YAML header is incompatible.", path="yaml")

    job = _mapping(effective.get("job"), path="job")
    _keys(job, {"id", "name"}, path="job")
    job_id = _name(job.get("id"), path="job.id")
    job_name = _name(job.get("name"), path="job.name")

    runtime_config = load_config(project_root=root, asset_code="0103")
    output = _mapping(effective.get("output"), path="output")
    _keys(output, {"artifact", "logging"}, path="output")
    unresolved_artifact = _mapping(output.get("artifact"), path="output.artifact")
    runtime_file_name_rule = unresolved_artifact.get("file_name_rule")
    output_for_resolution = {
        **output,
        "artifact": {
            key: value
            for key, value in unresolved_artifact.items()
            if key != "file_name_rule"
        },
    }
    output = resolve_contract_templates(
        output_for_resolution,
        scope={
            **runtime_config.template_scope(),
            "asset_code": "0103",
            "job_id": job_id,
            "job_name": job_name,
        },
        source=path,
    )
    artifact = {
        **_mapping(output.get("artifact"), path="output.artifact"),
        "file_name_rule": runtime_file_name_rule,
    }
    output["artifact"] = artifact
    _keys(
        artifact,
        {
            "type",
            "root_dir",
            "format",
            "compression",
            "write_policy",  # hidden compatibility input; omitted from public schema
            "physical_layout",
            "file_name_rule",
            "parquet_writer",  # hidden compatibility input
            "publication",
        },
        path="output.artifact",
    )
    required_constants = {
        "type": "source_dataset",
        "format": "parquet",
    }
    for key, expected in required_constants.items():
        if artifact.get(key) != expected:
            _fail(f"output.artifact.{key} must be {expected!r}.", path=f"output.artifact.{key}")
    if artifact.get("write_policy") not in {None, "atomic_replace"}:
        _fail(
            "Legacy output.artifact.write_policy only accepts 'atomic_replace'.",
            path="output.artifact.write_policy",
        )
    output_root = resolve_project_path(
        _name(artifact.get("root_dir"), path="output.artifact.root_dir"), project_root=root
    )
    compression = str(artifact.get("compression") or "zstd").lower()
    if compression not in {"zstd", "snappy", "uncompressed"}:
        _fail("Unsupported Parquet compression.", path="output.artifact.compression")
    rules = _mapping(artifact.get("file_name_rule"), path="output.artifact.file_name_rule")
    _keys(rules, {"dataset", "parquet", "relative_path_token"}, path="output.artifact.file_name_rule")
    token = _mapping(
        rules.get("relative_path_token"),
        path="output.artifact.file_name_rule.relative_path_token",
    )
    _keys(
        token,
        {
            "posix_separator_replacement",
            "windows_separator_replacement",
            "collision_hash_length",
        },
        path="output.artifact.file_name_rule.relative_path_token",
    )
    posix_replacement = _single_filename_character(
        token.get("posix_separator_replacement"),
        path="output.artifact.file_name_rule.relative_path_token.posix_separator_replacement",
    )
    windows_replacement = _single_filename_character(
        token.get("windows_separator_replacement"),
        path="output.artifact.file_name_rule.relative_path_token.windows_separator_replacement",
    )
    hash_length = int(token.get("collision_hash_length") or 8)
    if not 4 <= hash_length <= 64:
        _fail("collision_hash_length must be between 4 and 64.", path="output.artifact.file_name_rule.relative_path_token.collision_hash_length")
    physical_layout = _mapping(
        artifact.get("physical_layout") or {}, path="output.artifact.physical_layout"
    )
    legacy_writer = _mapping(
        artifact.get("parquet_writer") or {}, path="output.artifact.parquet_writer"
    )
    _keys(legacy_writer, {"row_group_size"}, path="output.artifact.parquet_writer")
    _keys(
        physical_layout,
        {"profile", "adaptation_scope", "row_group_rows"},
        path="output.artifact.physical_layout",
    )
    _name(
        physical_layout.get("profile"), path="output.artifact.physical_layout.profile"
    )
    if physical_layout.get("adaptation_scope") != "generation_fixed":
        _fail(
            "output.artifact.physical_layout.adaptation_scope must be 'generation_fixed'.",
            path="output.artifact.physical_layout.adaptation_scope",
        )
    row_group_size = physical_layout.get("row_group_rows")
    if row_group_size == "auto":
        row_group_size = None
    if row_group_size is not None and (not isinstance(row_group_size, int) or row_group_size < 1):
        _fail("row_group_size must be a positive integer.", path="output.artifact.parquet_writer.row_group_size")
    publication = PublicationSpec.from_mapping(artifact.get("publication"))

    source = _mapping(effective.get("source"), path="source")
    _keys(
        source,
        {
            "transport",
            "directory",
            "download",
            "glob",
            "recursive",
            "ordering",
            "empty_directory_policy",
            "csv",
            "source_file",
        },
        path="source",
    )
    source_transport = str(source.get("transport") or "directory").strip()
    if source_transport not in {"directory", "http_download"}:
        _fail("source.transport must be directory or http_download.", path="source.transport")
    source_directory = None
    download_options = None
    if source_transport == "directory":
        source_directory = resolve_project_path(
            _name(source.get("directory"), path="source.directory"), project_root=root
        )
        if source.get("download") is not None:
            _fail("source.download is only valid for http_download.", path="source.download")
    else:
        if source.get("directory") is not None:
            _fail("source.directory is only valid for directory transport.", path="source.directory")
        download_options = _download_options(source.get("download"))
    glob = str(source.get("glob") or "*.csv").strip()
    if not glob:
        _fail("source.glob must be non-empty.", path="source.glob")
    recursive = bool(source.get("recursive", True))
    ordering = str(source.get("ordering") or "path_asc")
    if ordering != "path_asc":
        _fail("source.ordering currently supports path_asc only.", path="source.ordering")
    empty_policy = str(source.get("empty_directory_policy") or "error")
    if empty_policy != "error":
        _fail("source.empty_directory_policy currently supports error only.", path="source.empty_directory_policy")
    csv_options = _csv_options(source.get("csv"))
    source_file = _mapping(source.get("source_file"), path="source.source_file")
    _keys(
        source_file,
        {"output_column", "value", "dtype", "existing_column_policy"},
        path="source.source_file",
    )
    if source_file.get("value") != "relative_path" or source_file.get("dtype") != "STRING":
        _fail("source.source_file requires value=relative_path and dtype=STRING.", path="source.source_file")
    if source_file.get("existing_column_policy") != "replace_with_warning":
        _fail("source.source_file existing_column_policy must be replace_with_warning.", path="source.source_file.existing_column_policy")
    file_name_column = _name(source_file.get("output_column"), path="source.source_file.output_column")

    materialize = _mapping(effective.get("materialize"), path="materialize")
    _keys(materialize, {"operations"}, path="materialize")
    raw_operations = materialize.get("operations")
    if not isinstance(raw_operations, list) or len(raw_operations) < 2:
        _fail("materialize.operations must contain type_cast and unpivot.", path="materialize.operations")
    operations = tuple(_validate_operations(raw_operations, file_name_column=file_name_column))

    routing = _mapping(effective.get("routing"), path="routing")
    _keys(routing, {"input", "routes", "overlap_policy", "unmatched_policy"}, path="routing")
    routes = _routes(routing.get("routes"))
    overlap_policy = str(routing.get("overlap_policy") or "duplicate")
    unmatched_policy = str(routing.get("unmatched_policy") or "drop")
    if overlap_policy != "duplicate" or unmatched_policy != "drop":
        _fail("0103 currently requires overlap_policy=duplicate and unmatched_policy=drop.", path="routing")

    execution = _mapping(effective.get("execution") or {}, path="execution")
    target_rows = int(execution.get("target_rows_per_part") or 100_000)
    if target_rows < 1:
        _fail("execution.target_rows_per_part must be positive.", path="execution.target_rows_per_part")

    effective["output"] = output
    return CsvSourceSpec(
        yaml_path=path,
        yaml_hash=file_sha256(path),
        project_root=root,
        job_id=job_id,
        job_name=job_name,
        source_transport=source_transport,
        source_directory=source_directory,
        download_options=download_options,
        glob=glob,
        recursive=recursive,
        ordering=ordering,
        empty_directory_policy=empty_policy,
        csv_options=csv_options,
        file_name_column=file_name_column,
        operations=operations,
        routes=routes,
        overlap_policy=overlap_policy,
        unmatched_policy=unmatched_policy,
        output_root=output_root,
        compression=compression,
        dataset_rule=_name(rules.get("dataset"), path="output.artifact.file_name_rule.dataset"),
        parquet_rule=_name(rules.get("parquet"), path="output.artifact.file_name_rule.parquet"),
        posix_separator_replacement=posix_replacement,
        windows_separator_replacement=windows_replacement,
        collision_hash_length=hash_length,
        row_group_size=row_group_size,
        publication=publication,
        target_rows_per_part=target_rows,
        resolved=effective,
    )


def _download_options(value: Any) -> dict[str, Any]:
    raw = _mapping(value, path="source.download")
    _keys(
        raw,
        {
            "url",
            "discovery",
            "query",
            "headers",
            "timeout_sec",
            "format",
            "file_name",
            "max_download_bytes",
            "max_archive_members",
            "max_extracted_bytes",
        },
        path="source.download",
    )
    url = str(raw.get("url") or "").strip()
    discovery = _gdelt_discovery(raw.get("discovery"))
    if bool(url) == bool(discovery):
        _fail("source.download requires exactly one of url or discovery.", path="source.download")
    if url and not url.startswith(("http://", "https://")):
        _fail("source.download.url must use http or https.", path="source.download.url")
    query = _string_mapping(raw.get("query"), path="source.download.query")
    headers = _string_mapping(raw.get("headers"), path="source.download.headers")
    timeout_sec = float(raw.get("timeout_sec") or 60)
    if timeout_sec <= 0:
        _fail("timeout_sec must be positive.", path="source.download.timeout_sec")
    archive_format = str(raw.get("format") or "auto").lower()
    if archive_format not in {"auto", "dsv", "zip"}:
        _fail("source.download.format must be auto, dsv, or zip.", path="source.download.format")
    file_name = str(raw.get("file_name") or ("" if discovery else "download.csv")).strip()
    if discovery and file_name:
        _fail("source.download.file_name is selected by discovery.", path="source.download.file_name")
    if not discovery and (not file_name or Path(file_name).name != file_name):
        _fail("source.download.file_name must be a safe filename.", path="source.download.file_name")
    limits = {
        "max_download_bytes": int(raw.get("max_download_bytes") or 1_073_741_824),
        "max_archive_members": int(raw.get("max_archive_members") or 10_000),
        "max_extracted_bytes": int(raw.get("max_extracted_bytes") or 4_294_967_296),
    }
    if any(number < 1 for number in limits.values()):
        _fail("download size/member limits must be positive.", path="source.download")
    return {
        "url": url,
        "discovery": discovery,
        "query": query,
        "headers": headers,
        "timeout_sec": timeout_sec,
        "format": archive_format,
        "file_name": file_name,
        **limits,
    }


def _gdelt_discovery(value: Any) -> dict[str, str] | None:
    if value is None:
        return None
    raw = _mapping(value, path="source.download.discovery")
    _keys(
        raw,
        {"type", "url", "file_kind", "selection", "stability_lag_minutes"},
        path="source.download.discovery",
    )
    if raw.get("type") != "gdelt_v2_masterfile":
        _fail("discovery.type must be gdelt_v2_masterfile.", path="source.download.discovery.type")
    url = _name(raw.get("url"), path="source.download.discovery.url")
    if not url.startswith(("http://", "https://")):
        _fail("discovery.url must use http or https.", path="source.download.discovery.url")
    file_kind = str(raw.get("file_kind") or "events")
    if file_kind not in {"events", "mentions", "gkg"}:
        _fail("discovery.file_kind must be events, mentions, or gkg.", path="source.download.discovery.file_kind")
    selection = str(raw.get("selection") or "latest")
    if selection != "latest":
        _fail("discovery.selection currently supports latest only.", path="source.download.discovery.selection")
    stability_lag_minutes = int(raw.get("stability_lag_minutes") or 0)
    if not 0 <= stability_lag_minutes <= 60:
        _fail(
            "discovery.stability_lag_minutes must be between 0 and 60.",
            path="source.download.discovery.stability_lag_minutes",
        )
    return {
        "type": "gdelt_v2_masterfile",
        "url": url,
        "file_kind": file_kind,
        "selection": selection,
        "stability_lag_minutes": stability_lag_minutes,
    }


def _string_mapping(value: Any, *, path: str) -> dict[str, str]:
    if value is None:
        return {}
    raw = _mapping(value, path=path)
    result: dict[str, str] = {}
    for key, item in raw.items():
        name = str(key).strip()
        if not name or isinstance(item, (dict, list)):
            _fail(f"{path} must contain scalar string values.", path=path)
        result[name] = str(item)
    return result


def _csv_options(value: Any) -> dict[str, Any]:
    raw = _mapping(value, path="source.csv")
    _keys(
        raw,
        {"encoding", "delimiter", "quote_char", "header", "column_names", "null_values", "malformed_row_policy"},
        path="source.csv",
    )
    options = {
        "encoding": str(raw.get("encoding") or "utf8"),
        "separator": str(raw.get("delimiter") or ","),
        "quote_char": str(raw.get("quote_char") or '"'),
        "has_header": bool(raw.get("header", True)),
        "new_columns": raw.get("column_names"),
        "null_values": raw.get("null_values"),
    }
    if len(options["separator"]) != 1 or len(options["quote_char"]) != 1:
        _fail("CSV delimiter and quote_char must be one character.", path="source.csv")
    if options["has_header"] and options["new_columns"] is not None:
        _fail("source.csv.column_names is only valid when header=false.", path="source.csv.column_names")
    if not options["has_header"]:
        names = _names(options["new_columns"], path="source.csv.column_names")
        if len(set(names)) != len(names):
            _fail("source.csv.column_names must be unique.", path="source.csv.column_names")
        options["new_columns"] = names
    if str(raw.get("malformed_row_policy") or "error") != "error":
        _fail("0103 currently requires malformed_row_policy=error.", path="source.csv.malformed_row_policy")
    return options


def _validate_operations(values: list[Any], *, file_name_column: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen_aliases: set[str] = set()
    for index, value in enumerate(values):
        path = f"materialize.operations[{index}]"
        operation = dict(_mapping(value, path=path))
        kind = _name(operation.get("op"), path=f"{path}.op")
        alias = _name(operation.get("alias"), path=f"{path}.alias")
        if alias in seen_aliases:
            _fail("Operation aliases must be unique.", path=f"{path}.alias")
        seen_aliases.add(alias)
        if kind == "type_cast":
            _keys(
                operation,
                {
                    "op", "alias", "load_as", "columns_by_type", "default_type",
                    "cast_failure_policy", "missing_named_column_policy",
                },
                path=path,
            )
            if operation.get("load_as") != "STRING":
                _fail("0103 type_cast load_as must be STRING.", path=f"{path}.load_as")
            if operation.get("cast_failure_policy") != "error" or operation.get("missing_named_column_policy") != "error":
                _fail("0103 type_cast requires error policies.", path=path)
            columns_by_type = _mapping(operation.get("columns_by_type") or {}, path=f"{path}.columns_by_type")
            assigned: set[str] = set()
            for type_name, columns in columns_by_type.items():
                _require_type(str(type_name), path=f"{path}.columns_by_type.{type_name}")
                for column in _names(columns, path=f"{path}.columns_by_type.{type_name}"):
                    if column == file_name_column:
                        _fail("file_name is system-owned STRING and cannot be recast.", path=path)
                    if column in assigned:
                        _fail("A column cannot appear in multiple type groups.", path=path)
                    assigned.add(column)
            _require_type(str(operation.get("default_type") or ""), path=f"{path}.default_type")
        elif kind == "unpivot":
            _keys(
                operation,
                {
                    "op", "alias", "id_columns", "value_columns", "name_column",
                    "value_column", "value_type", "preserve_nulls",
                },
                path=path,
            )
            id_columns = _names(operation.get("id_columns"), path=f"{path}.id_columns")
            if file_name_column not in id_columns:
                _fail("unpivot.id_columns must include the system file_name column.", path=path)
            if operation.get("value_columns") != "remaining":
                _fail("0103 unpivot.value_columns must be remaining.", path=f"{path}.value_columns")
            _name(operation.get("name_column"), path=f"{path}.name_column")
            _name(operation.get("value_column"), path=f"{path}.value_column")
            _require_type(str(operation.get("value_type") or ""), path=f"{path}.value_type")
            if not isinstance(operation.get("preserve_nulls"), bool):
                _fail("unpivot.preserve_nulls must be boolean.", path=f"{path}.preserve_nulls")
        elif kind == "add_calc":
            _keys(operation, {"op", "alias", "expressions"}, path=path)
            expressions = operation.get("expressions")
            if not isinstance(expressions, list) or not expressions:
                _fail("add_calc.expressions must be non-empty.", path=f"{path}.expressions")
            for expression_index, expression in enumerate(expressions):
                item = _mapping(expression, path=f"{path}.expressions[{expression_index}]")
                _keys(item, {"name", "sql", "spotfire_expression"}, path=f"{path}.expressions[{expression_index}]")
                _name(item.get("name"), path=f"{path}.expressions[{expression_index}].name")
                resolve_add_calc_expression(item, index=expression_index)
        else:
            _fail("0103 materialize supports type_cast, unpivot, and add_calc.", path=f"{path}.op")
        result.append(operation)
    if result[0]["op"] != "type_cast" or result[1]["op"] != "unpivot":
        _fail("0103 materialize must start with type_cast followed by unpivot.", path="materialize.operations")
    if any(item["op"] == "type_cast" for item in result[1:]) or any(
        item["op"] == "unpivot" for item in result[2:]
    ):
        _fail("0103 allows one leading type_cast and one unpivot.", path="materialize.operations")
    return result


def _routes(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list) or not value:
        _fail("routing.routes must be a non-empty list.", path="routing.routes")
    result: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, raw in enumerate(value):
        path = f"routing.routes[{index}]"
        route = dict(_mapping(raw, path=path))
        _keys(route, {"route_name", "filtering"}, path=path)
        name = _name(route.get("route_name"), path=f"{path}.route_name")
        if name in names:
            _fail("route_name values must be unique.", path=f"{path}.route_name")
        names.add(name)
        filters = route.get("filtering")
        if not isinstance(filters, list) or not filters:
            _fail("route filtering must be non-empty.", path=f"{path}.filtering")
        normalized: list[dict[str, str]] = []
        for filter_index, raw_filter in enumerate(filters):
            if isinstance(raw_filter, str):
                item = {"sql": raw_filter}
            else:
                item = dict(_mapping(raw_filter, path=f"{path}.filtering[{filter_index}]"))
            _keys(item, {"sql", "spotfire_expression"}, path=f"{path}.filtering[{filter_index}]")
            sql = str(item.get("sql") or "").strip()
            spotfire = str(item.get("spotfire_expression") or "").strip()
            if bool(sql) == bool(spotfire):
                _fail("Route filter requires exactly one expression dialect.", path=f"{path}.filtering[{filter_index}]")
            normalized.append({"sql": sql} if sql else {"spotfire_expression": spotfire})
        result.append({"route_name": name, "filtering": normalized})
    return tuple(result)


def _require_type(value: str, *, path: str) -> str:
    normalized = value.strip().upper()
    if normalized not in POLARS_TYPE_MAP:
        _fail(f"Unsupported type: {normalized or '<missing>'}", path=path)
    return normalized


def _single_filename_character(value: Any, *, path: str) -> str:
    text = str(value or "")
    if len(text) != 1 or text in {"/", "\\", "\0"}:
        _fail("Path separator replacement must be one safe character.", path=path)
    return text


def _mapping(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail("Expected a mapping.", path=path)
    return value


def _keys(value: dict[str, Any], allowed: set[str], *, path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        _fail(f"Unsupported keys: {unknown}", path=path)


def _name(value: Any, *, path: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail("Expected a non-empty string.", path=path)
    return text


def _names(value: Any, *, path: str) -> list[str]:
    if not isinstance(value, list) or not value:
        _fail("Expected a non-empty string list.", path=path)
    result = [_name(item, path=f"{path}[]") for item in value]
    if len(set(result)) != len(result):
        _fail("List values must be unique.", path=path)
    return result


def _fail(message: str, *, path: str) -> None:
    raise ValidationError(message, code="0103.invalid_definition", context={"path": path})
