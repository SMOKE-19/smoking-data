from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from smoking_data.core.exceptions import SmokingDataError
from smoking_data.core.results import StageResult, to_json_safe, utc_now_iso
from smoking_data.ops.projection import POLARS_TYPE_MAP, apply_add_calc
from smoking_data.runtime.naming import partition_dir_name
from smoking_data.runtime.object_store.publication import publish_committed_dataset
from smoking_data.runtime.paths import file_sha256
from smoking_data.runtime.transactions import (
    DATASET_MANIFEST_VERSION,
    DatasetTransaction,
    refresh_dataset_manifest_provenance,
)

from .acquisition import prepare_csv_source
from .spec import CSV_SOURCE_SCHEMA_VERSION, CsvSourceSpec, load_csv_source_spec
from .type_rules import TYPE_RULE_VERSION, name_type_rule

SOURCE_FILE_MANIFEST_VERSION = "smoking-data.csv-source-file-manifest.v1"
DATASET_CATALOG_VERSION = "smoking-data.dataset-catalog.v1"
_SOURCE_MANIFEST = Path("_smoking_data/source-file-manifest.json")
_DATASET_CATALOG = Path("_smoking_data/dataset-catalog.json")
_METADATA = Path("_smoking_data/metadata.json")


def run_yaml(
    yaml_path: str | Path,
    *,
    config_path: str | Path | None = None,
    project_root: str | Path | None = None,
    trigger_type: str = "manual",
) -> StageResult:
    del config_path  # 0103 resolves the standard project/Asset config layers directly.
    path = Path(yaml_path).expanduser().resolve()
    try:
        spec = load_csv_source_spec(path, project_root=project_root)
        return _run(spec, trigger_type=trigger_type)
    except Exception as exc:  # noqa: BLE001 - public Asset boundary returns StageResult.
        job_name = _best_effort_job_name(path)
        return StageResult.failure(
            preset="0103",
            job_name=job_name,
            yaml_path=path,
            exc=exc,
        )


def _run(spec: CsvSourceSpec, *, trigger_type: str) -> StageResult:
    with prepare_csv_source(spec) as prepared:
        return _run_prepared(
            spec,
            source_root=prepared.root,
            acquisition=prepared.metadata,
            trigger_type=trigger_type,
        )


def _run_prepared(
    spec: CsvSourceSpec,
    *,
    source_root: Path,
    acquisition: dict[str, Any],
    trigger_type: str,
) -> StageResult:
    files = _discover_csv_files(spec, source_root=source_root)
    if not files:
        raise SmokingDataError(
            f"0103 prepared source has no matching DSV files: {source_root}",
            code="0103.source.empty",
            context={"transport": spec.source_transport, "glob": spec.glob},
        )
    contract_hash = _transform_contract_hash(spec)
    previous = _read_mapping(spec.output_root / _SOURCE_MANIFEST)
    previous_entries = {
        str(item.get("relative_path")): item
        for item in (previous or {}).get("files") or []
        if isinstance(item, dict) and item.get("relative_path")
    }
    transaction = DatasetTransaction.create(
        spec.output_root,
        manifest_context={
            "asset_code": "0103",
            "artifact_type": "source_dataset",
            "job_id": spec.job_id,
            "job_name": spec.job_name,
            "logical_plan_hash": contract_hash,
            "change_reason": "csv_source_snapshot",
        },
    )
    now = utc_now_iso()
    run_date = datetime.now().astimezone().strftime("%Y%m%d")
    entries: list[dict[str, Any]] = []
    catalog: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    counters = {
        "source_files": len(files),
        "processed_files": 0,
        "reused_files": 0,
        "same_content_files": 0,
        "deleted_files": len(set(previous_entries) - {_relative_path(source_root, item) for item in files}),
        "input_rows": 0,
        "unpivot_rows": 0,
        "output_rows": 0,
        "unmatched_rows": 0,
        "duplicate_route_rows": 0,
        "file_name_overrides": 0,
    }
    try:
        for csv_path in files:
            relative_path = _relative_path(source_root, csv_path)
            stat = csv_path.stat()
            prior = previous_entries.get(relative_path)
            reusable, content_hash, same_content = _reusable_entry(
                spec,
                prior,
                csv_path=csv_path,
                size_bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
                contract_hash=contract_hash,
            )
            if reusable and prior is not None:
                copied_outputs = _copy_previous_outputs(
                    spec.output_root,
                    transaction.staging_root,
                    prior,
                )
                entry = {
                    **prior,
                    "size_bytes": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "sha256": content_hash or str(prior.get("sha256") or ""),
                    "last_seen_at": now,
                    "status": "unchanged_content" if same_content else "unchanged",
                    "outputs": copied_outputs,
                }
                entries.append(entry)
                counters["reused_files"] += 1
                counters["same_content_files"] += int(same_content)
                continue

            entry, file_catalog, file_warnings, file_counters = _process_file(
                spec,
                csv_path=csv_path,
                relative_path=relative_path,
                source_stat=stat,
                source_sha256=content_hash or file_sha256(csv_path),
                contract_hash=contract_hash,
                staging_root=transaction.staging_root,
                run_date=run_date,
                now=now,
                first_seen_at=str((prior or {}).get("first_seen_at") or now),
            )
            entries.append(entry)
            catalog.extend(file_catalog)
            warnings.extend(file_warnings)
            counters["processed_files"] += 1
            for key, value in file_counters.items():
                counters[key] += value

        # Reused entries are normalized into fresh child manifests/catalog entries.
        for entry in entries:
            if str(entry.get("status")) not in {"unchanged", "unchanged_content"}:
                continue
            catalog.extend(_catalog_from_entry(entry))
        _write_child_manifests(transaction.staging_root, entries)
        source_manifest = {
            "schema_version": SOURCE_FILE_MANIFEST_VERSION,
            "asset_code": "0103",
            "job_id": spec.job_id,
            "job_name": spec.job_name,
            "created_at": str((previous or {}).get("created_at") or now),
            "updated_at": now,
            "transform_contract_hash": contract_hash,
            "files": sorted(entries, key=lambda item: str(item["relative_path"])),
        }
        dataset_catalog = {
            "schema_version": DATASET_CATALOG_VERSION,
            "asset_code": "0103",
            "job_name": spec.job_name,
            "updated_at": now,
            "datasets": sorted(catalog, key=lambda item: str(item["relative_path"])),
        }
        metadata = {
            "schema_version": "smoking-data.artifact-metadata.v1",
            "created_at": now,
            "asset": {"code": "0103", "job_id": spec.job_id, "job_name": spec.job_name},
            "definition": {"path": str(spec.yaml_path), "sha256": spec.yaml_hash},
            "trigger_type": trigger_type,
            "acquisition": acquisition,
            "transform_contract_hash": contract_hash,
            "counters": counters,
            "warnings": warnings,
            "warning_count": len(warnings),
        }
        _write_json(transaction.staging_root / _SOURCE_MANIFEST, source_manifest)
        _write_json(transaction.staging_root / _DATASET_CATALOG, dataset_catalog)
        _write_json(transaction.staging_root / _METADATA, metadata)
        output_paths, transaction_profile = transaction.commit()
        refresh_dataset_manifest_provenance(spec.output_root)
        publication_result = (
            publish_committed_dataset(
                spec.output_root,
                project_root=spec.project_root,
                publication=spec.publication,
                asset_code="0103",
                job_name=spec.job_name,
                definition_sha256=spec.yaml_hash,
            )
            if spec.publication is not None
            else None
        )
    except BaseException:
        transaction.abort()
        raise

    metadata_path = spec.output_root / _METADATA
    return StageResult.success(
        preset="0103",
        job_name=spec.job_name,
        yaml_path=spec.yaml_path,
        metadata_path=metadata_path,
        output_paths=[spec.output_root],
        counters=counters,
        details={
            "artifact_type": "source_dataset",
            "output_dir": str(spec.output_root),
            "parquet_parts": len(output_paths),
            "source_file_manifest": str(spec.output_root / _SOURCE_MANIFEST),
            "dataset_catalog": str(spec.output_root / _DATASET_CATALOG),
            "transaction": transaction_profile,
            "warnings": warnings,
            "acquisition": acquisition,
            "remote_publication": (
                {
                    "status": publication_result.status,
                    "target": publication_result.target,
                    "dataset_uri": publication_result.dataset_uri,
                    "generation_id": publication_result.generation_id,
                    "manifest_key": publication_result.manifest_key,
                    "receipt_path": str(publication_result.receipt_path),
                }
                if publication_result is not None
                else None
            ),
        },
    )


def _discover_csv_files(spec: CsvSourceSpec, *, source_root: Path) -> list[Path]:
    if not source_root.is_dir():
        raise SmokingDataError(
            f"0103 source directory does not exist: {source_root}",
            code="0103.source.directory_missing",
        )
    iterator = (
        source_root.rglob(spec.glob)
        if spec.recursive
        else source_root.glob(spec.glob)
    )
    return sorted(path.resolve() for path in iterator if path.is_file())


def _reusable_entry(
    spec: CsvSourceSpec,
    prior: dict[str, Any] | None,
    *,
    csv_path: Path,
    size_bytes: int,
    mtime_ns: int,
    contract_hash: str,
) -> tuple[bool, str | None, bool]:
    if not prior or prior.get("transform_contract_hash") != contract_hash:
        return False, None, False
    if not _outputs_available(spec.output_root, prior):
        return False, None, False
    if int(prior.get("size_bytes") or -1) == size_bytes and int(
        prior.get("mtime_ns") or -1
    ) == mtime_ns:
        return True, str(prior.get("sha256") or ""), False
    current_hash = file_sha256(csv_path)
    if current_hash == str(prior.get("sha256") or ""):
        return True, current_hash, True
    return False, current_hash, False


def _outputs_available(root: Path, entry: dict[str, Any]) -> bool:
    outputs = entry.get("outputs")
    if not isinstance(outputs, list):
        return False
    for output in outputs:
        if not isinstance(output, dict) or not output.get("relative_path"):
            return False
        path = root / str(output["relative_path"])
        if not path.is_file() or path.stat().st_size != int(output.get("size_bytes") or -1):
            return False
    return True


def _copy_previous_outputs(
    previous_root: Path,
    staging_root: Path,
    entry: dict[str, Any],
) -> list[dict[str, Any]]:
    copied: list[dict[str, Any]] = []
    for raw in entry.get("outputs") or []:
        output = dict(raw)
        relative = Path(str(output["relative_path"]))
        source = previous_root / relative
        target = staging_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        copied.append(output)
    return copied


def _process_file(
    spec: CsvSourceSpec,
    *,
    csv_path: Path,
    relative_path: str,
    source_stat: os.stat_result,
    source_sha256: str,
    contract_hash: str,
    staging_root: Path,
    run_date: str,
    now: str,
    first_seen_at: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    before = csv_path.stat()
    frame = pl.read_csv(
        csv_path,
        infer_schema=False,
        ignore_errors=False,
        **{key: value for key, value in spec.csv_options.items() if value is not None},
    )
    warnings: list[dict[str, Any]] = []
    if spec.file_name_column in frame.columns:
        warnings.append(
            {
                "code": "0103.source_file_column_overridden",
                "relative_path": relative_path,
                "column": spec.file_name_column,
                "original_dtype": str(frame.schema[spec.file_name_column]),
            }
        )
        frame = frame.drop(spec.file_name_column)
    frame = frame.with_columns(pl.lit(relative_path, dtype=pl.String).alias(spec.file_name_column))
    input_rows = frame.height
    frame = _apply_materialize(spec, frame)
    unpivot_rows = frame.height
    after = csv_path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise SmokingDataError(
            f"0103 source changed during materialization: {relative_path}",
            code="0103.source.snapshot_changed",
            context={"relative_path": relative_path},
        )

    source_token = _source_file_token(spec, relative_path)
    outputs: list[dict[str, Any]] = []
    catalog: list[dict[str, Any]] = []
    matched_rows = pl.Series("__matched", [False] * frame.height, dtype=pl.Boolean)
    route_match_total = 0
    for route in spec.routes:
        route_frame = _filter_route(frame, route["filtering"])
        route_rows = route_frame.height
        if route_rows:
            route_mask = _route_mask(frame, route["filtering"])
            matched_rows = matched_rows | route_mask
            route_match_total += int(route_mask.sum())
            dataset_relative, route_outputs = _write_route_dataset(
                spec,
                route_frame,
                staging_root=staging_root,
                route_name=str(route["route_name"]),
                source_token=source_token,
                run_date=run_date,
                contract_hash=contract_hash,
            )
            outputs.extend(route_outputs)
            catalog.append(
                {
                    "relative_path": dataset_relative,
                    "labels": {
                        "asset_code": "0103",
                        "route": str(route["route_name"]),
                        "source_file": relative_path,
                    },
                    "rows": route_rows,
                }
            )
    unmatched_rows = int((~matched_rows).sum())
    output_rows = sum(int(item["rows"]) for item in outputs)
    duplicate_rows = max(0, route_match_total - int(matched_rows.sum()))
    entry = {
        "relative_path": relative_path,
        "size_bytes": source_stat.st_size,
        "mtime_ns": source_stat.st_mtime_ns,
        "sha256": source_sha256,
        "first_seen_at": first_seen_at,
        "last_seen_at": now,
        "last_processed_at": now,
        "transform_contract_hash": contract_hash,
        "status": "processed",
        "outputs": outputs,
        "route_datasets": catalog,
    }
    return entry, catalog, warnings, {
        "input_rows": input_rows,
        "unpivot_rows": unpivot_rows,
        "output_rows": output_rows,
        "unmatched_rows": unmatched_rows,
        "duplicate_route_rows": duplicate_rows,
        "file_name_overrides": len(warnings),
    }


def _apply_materialize(spec: CsvSourceSpec, frame: pl.DataFrame) -> pl.DataFrame:
    for operation in spec.operations:
        kind = str(operation["op"])
        if kind == "type_cast":
            frame = _apply_grouped_cast(frame, operation, file_name_column=spec.file_name_column)
        elif kind == "unpivot":
            frame = _apply_unpivot(frame, operation)
        elif kind == "add_calc":
            frame = apply_add_calc(frame.lazy(), operation["expressions"]).collect()
    return frame


def _apply_grouped_cast(
    frame: pl.DataFrame,
    operation: dict[str, Any],
    *,
    file_name_column: str,
) -> pl.DataFrame:
    explicit: dict[str, str] = {}
    for type_name, columns in dict(operation.get("columns_by_type") or {}).items():
        for column in columns:
            explicit[str(column)] = str(type_name).upper()
    missing = sorted(set(explicit) - set(frame.columns))
    if missing:
        raise SmokingDataError(
            "0103 CSV is missing explicitly typed columns.",
            code="0103.cast.missing_columns",
            context={"missing_columns": missing},
        )
    default_type = str(operation["default_type"]).upper()
    expressions = []
    for column in frame.columns:
        type_name = (
            "STRING"
            if column == file_name_column
            else explicit.get(column) or name_type_rule(column) or default_type
        )
        expression = pl.col(column)
        if type_name == "DATETIME":
            expression = expression.str.to_datetime(strict=True)
        else:
            expression = expression.cast(POLARS_TYPE_MAP[type_name], strict=True)
        expressions.append(expression.alias(column))
    return frame.with_columns(expressions)


def _apply_unpivot(frame: pl.DataFrame, operation: dict[str, Any]) -> pl.DataFrame:
    id_columns = [str(item) for item in operation["id_columns"]]
    missing = sorted(set(id_columns) - set(frame.columns))
    if missing:
        raise SmokingDataError(
            "0103 unpivot is missing id columns.",
            code="0103.unpivot.missing_columns",
            context={"missing_columns": missing},
        )
    value_columns = [column for column in frame.columns if column not in id_columns]
    if not value_columns:
        raise SmokingDataError(
            "0103 unpivot has no remaining value columns.",
            code="0103.unpivot.empty_values",
        )
    value_type = str(operation["value_type"]).upper()
    frame = frame.with_columns(
        [pl.col(column).cast(POLARS_TYPE_MAP[value_type], strict=True) for column in value_columns]
    )
    result = frame.unpivot(
        on=value_columns,
        index=id_columns,
        variable_name=str(operation["name_column"]),
        value_name=str(operation["value_column"]),
    )
    if not bool(operation["preserve_nulls"]):
        result = result.filter(pl.col(str(operation["value_column"])).is_not_null())
    return result


def _filter_route(frame: pl.DataFrame, filters: list[dict[str, str]]) -> pl.DataFrame:
    result = frame
    for item in filters:
        result = result.filter(_filter_expression(item))
    return result


def _route_mask(frame: pl.DataFrame, filters: list[dict[str, str]]) -> pl.Series:
    expression = pl.lit(True)
    for item in filters:
        expression = expression & _filter_expression(item)
    return frame.select(expression.alias("__route_match")).get_column("__route_match")


def _filter_expression(item: dict[str, str]) -> pl.Expr:
    if item.get("sql"):
        return pl.sql_expr(item["sql"])
    from spotfire_expr_normalizer import normalize_expression

    return pl.sql_expr(normalize_expression(item["spotfire_expression"]))


def _write_route_dataset(
    spec: CsvSourceSpec,
    frame: pl.DataFrame,
    *,
    staging_root: Path,
    route_name: str,
    source_token: str,
    run_date: str,
    contract_hash: str,
) -> tuple[str, list[dict[str, Any]]]:
    values = {
        "run_date": run_date,
        "source_file_flat": source_token,
        "job_id": spec.job_id,
        "job_name": spec.job_name,
        "route_name": route_name,
        "transform_revision16": contract_hash[:16],
    }
    dataset_name = _render_filename(spec.dataset_rule, values, suffix=".dataset")
    dataset_relative = Path(f"route={partition_dir_name(route_name)}") / dataset_name
    dataset_root = staging_root / dataset_relative
    dataset_root.mkdir(parents=True, exist_ok=True)
    outputs: list[dict[str, Any]] = []
    for part_index, offset in enumerate(range(0, frame.height, spec.target_rows_per_part), start=1):
        values["part_index"] = part_index
        parquet_name = _render_filename(spec.parquet_rule, values, suffix=".parquet")
        path = dataset_root / parquet_name
        part = frame.slice(offset, spec.target_rows_per_part)
        part.write_parquet(
            path,
            compression=spec.compression,
            row_group_size=spec.row_group_size,
            statistics=True,
            use_pyarrow=True,
            pyarrow_options={"write_page_index": True},
        )
        outputs.append(
            {
                "route": route_name,
                "dataset_relative_path": dataset_relative.as_posix(),
                "relative_path": path.relative_to(staging_root).as_posix(),
                "rows": part.height,
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    return dataset_relative.as_posix(), outputs


def _write_child_manifests(staging_root: Path, entries: list[dict[str, Any]]) -> None:
    by_dataset: dict[str, list[dict[str, Any]]] = {}
    labels_by_dataset: dict[str, dict[str, Any]] = {}
    for entry in entries:
        labels = {
            str(item["relative_path"]): dict(item.get("labels") or {})
            for item in entry.get("route_datasets") or []
            if isinstance(item, dict) and item.get("relative_path")
        }
        for output in entry.get("outputs") or []:
            dataset = str(output["dataset_relative_path"])
            by_dataset.setdefault(dataset, []).append(output)
            labels_by_dataset.setdefault(dataset, labels.get(dataset, {}))
    for dataset, outputs in by_dataset.items():
        dataset_root = staging_root / dataset
        parts = []
        for output in sorted(outputs, key=lambda item: str(item["relative_path"])):
            path = staging_root / str(output["relative_path"])
            parts.append(
                {
                    "relative_path": path.relative_to(dataset_root).as_posix(),
                    "rows": int(output["rows"]),
                    "size_bytes": path.stat().st_size,
                    "sha256": str(output["sha256"]),
                    "schema": str(pq.ParquetFile(path).schema_arrow),
                }
            )
        _write_json(
            dataset_root / "_dataset.manifest.json",
            {
                "version": DATASET_MANIFEST_VERSION,
                "transaction_id": "source-0103-child",
                "rows": sum(int(item["rows"]) for item in parts),
                "parts": parts,
                "context": {"asset_code": "0103", "labels": labels_by_dataset[dataset]},
            },
        )


def _catalog_from_entry(entry: dict[str, Any]) -> list[dict[str, Any]]:
    route_datasets = entry.get("route_datasets")
    if isinstance(route_datasets, list):
        return [dict(item) for item in route_datasets if isinstance(item, dict)]
    grouped: dict[str, dict[str, Any]] = {}
    for output in entry.get("outputs") or []:
        dataset = str(output["dataset_relative_path"])
        item = grouped.setdefault(
            dataset,
            {
                "relative_path": dataset,
                "labels": {
                    "asset_code": "0103",
                    "route": str(output["route"]),
                    "source_file": str(entry["relative_path"]),
                },
                "rows": 0,
            },
        )
        item["rows"] += int(output["rows"])
    return list(grouped.values())


def _source_file_token(spec: CsvSourceSpec, relative_path: str) -> str:
    flattened = relative_path.replace("/", spec.posix_separator_replacement).replace(
        "\\", spec.windows_separator_replacement
    )
    flattened = re.sub(r"[^A-Za-z0-9._-]+", "_", flattened).strip("._") or "source"
    digest = hashlib.sha256(relative_path.encode("utf-8")).hexdigest()[: spec.collision_hash_length]
    return f"{flattened}__{digest}"


def _render_filename(template: str, values: dict[str, Any], *, suffix: str) -> str:
    try:
        rendered = template.format_map(values)
    except (KeyError, ValueError) as exc:
        raise SmokingDataError(
            f"0103 file_name_rule cannot be rendered: {exc}",
            code="0103.output.invalid_file_name_rule",
        ) from exc
    if not rendered.endswith(suffix):
        rendered += suffix
    if Path(rendered).name != rendered or rendered in {suffix, f".{suffix}"}:
        raise SmokingDataError(
            f"0103 rendered filename is unsafe: {rendered}",
            code="0103.output.unsafe_filename",
        )
    return rendered


def _transform_contract_hash(spec: CsvSourceSpec) -> str:
    document = {
        "schema_version": CSV_SOURCE_SCHEMA_VERSION,
        "type_rule_version": TYPE_RULE_VERSION,
        "source_transport": spec.source_transport,
        "csv": spec.csv_options,
        "file_name_column": spec.file_name_column,
        "operations": spec.operations,
        "routes": spec.routes,
        "overlap_policy": spec.overlap_policy,
        "unmatched_policy": spec.unmatched_policy,
        "compression": spec.compression,
        "dataset_rule": spec.dataset_rule,
        "parquet_rule": spec.parquet_rule,
        "separator_replacements": [
            spec.posix_separator_replacement,
            spec.windows_separator_replacement,
        ],
        "collision_hash_length": spec.collision_hash_length,
        "row_group_size": spec.row_group_size,
        "target_rows_per_part": spec.target_rows_per_part,
    }
    encoded = json.dumps(to_json_safe(document), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _relative_path(source_root: Path, path: Path) -> str:
    return path.resolve().relative_to(source_root.resolve()).as_posix()


def _read_mapping(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_json_safe(value), ensure_ascii=False, indent=2), encoding="utf-8")


def _best_effort_job_name(path: Path) -> str:
    try:
        import yaml

        value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        job = value.get("job") if isinstance(value, dict) else None
        return str(job.get("name") or path.stem) if isinstance(job, dict) else path.stem
    except Exception:  # noqa: BLE001 - failure reporting only.
        return path.stem
