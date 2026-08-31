from __future__ import annotations

import base64
import hashlib
import importlib.metadata
import json
import os
import shutil
import struct
import time as time_module
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from smoking_data.core.exceptions import SmokingDataError, ValidationError
from smoking_data.core.operations import OperationKind
from smoking_data.core.pipeline import PipelineSpec, SourceSpec
from smoking_data.ops.upstream import discover_parquet_files
from smoking_data.runtime.asset_config import deep_merge
from smoking_data.runtime.asset_contract import (
    load_effective_asset_contract,
    partition_grid_anchor,
    partition_grid_step_days,
)
from smoking_data.runtime.config import RuntimeConfig
from smoking_data.runtime.events import append_stage_event
from smoking_data.runtime.paths import ensure_dir, resolve_project_path
from smoking_data.runtime.yaml_loader import load_pipeline_spec

from .partitioning import ProbePartitionGrid, partition_directory_name

MANIFEST_VERSION = "smoking-data.probe-manifest.v3"
ROW_GROUP_SCHEMA_VERSION = "smoking-data.row-group-sidecar.v2"
PAGE_SCHEMA_VERSION = "smoking-data.parquet-pages.v1"
FINGERPRINT_VERSION = "parquet-footer-v1"
ACCESS_PROFILE_SCHEMA_VERSION = "smoking-data.probe-access-profile.v2"
PHYSICAL_PROBE_VERSION = "smoking-data.physical-probe.v1"
PROBE_SCHEMA_VERSION = "smoking-data.internal-parquet-probe.v1"


@dataclass(frozen=True, slots=True)
class ProbeHandle:
    source_name: str
    manifest_path: Path
    dataset_fingerprint: str
    probe_schema_version: str
    capabilities: dict[str, bool]
    artifact_paths: dict[str, Path]
    metadata_path: Path | None = None
    log_path: Path | None = None
    reused: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "manifest_path": str(self.manifest_path),
            "dataset_fingerprint": self.dataset_fingerprint,
            "probe_schema_version": self.probe_schema_version,
            "capabilities": dict(self.capabilities),
            "artifact_paths": {key: str(value) for key, value in self.artifact_paths.items()},
            "metadata_path": str(self.metadata_path) if self.metadata_path else None,
            "log_path": str(self.log_path) if self.log_path else None,
            "reused": self.reused,
        }


def ensure_pipeline_probes(
    spec: PipelineSpec,
    *,
    config: RuntimeConfig,
    index_level: str = "auto",
    allow_additive_columns: bool = False,
) -> dict[str, ProbeHandle]:
    handles, _ = ensure_pipeline_probes_profiled(
        spec,
        config=config,
        index_level=index_level,
        allow_additive_columns=allow_additive_columns,
    )
    return handles


def ensure_pipeline_probes_profiled(
    spec: PipelineSpec,
    *,
    config: RuntimeConfig,
    index_level: str = "auto",
    allow_additive_columns: bool = False,
) -> tuple[dict[str, ProbeHandle], dict[str, Any]]:
    total_started = time_module.perf_counter()
    handles: dict[str, ProbeHandle] = {}
    source_profiles: dict[str, dict[str, Any]] = {}
    for source_name, source in spec.sources.items():
        if source.keyspace is not None:
            source_profiles[source_name] = {
                "elapsed_sec": 0.0,
                "reused": False,
                "synthetic": True,
                "method": source.keyspace.get("method"),
            }
            continue
        source_started = time_module.perf_counter()
        handles[source_name] = ensure_source_probe(
            source_name=source_name,
            source=source,
            config=config,
            index_level=index_level,
            allow_additive_columns=allow_additive_columns,
        )
        source_profiles[source_name] = {
            "elapsed_sec": time_module.perf_counter() - source_started,
            "reused": handles[source_name].reused,
            "manifest_path": str(handles[source_name].manifest_path),
        }
    profile = {
        "total_sec": time_module.perf_counter() - total_started,
        "source_count": len(handles),
        "reused_source_count": sum(handle.reused for handle in handles.values()),
        "rebuilt_source_count": sum(not handle.reused for handle in handles.values()),
        "sources": source_profiles,
    }
    return handles, profile


def ensure_source_probe(
    *,
    source_name: str,
    source: SourceSpec,
    config: RuntimeConfig,
    index_level: str = "auto",
    allow_additive_columns: bool = False,
    output_root: str | Path | None = None,
) -> ProbeHandle:
    return ensure_probe(
        source_name=source_name,
        source=source,
        access_profile={},
        downstream_plan_fingerprints=[],
        config=config,
        index_level=index_level,
        allow_additive_columns=allow_additive_columns,
        output_root=output_root or _source_probe_root(source, config=config),
    )


def _source_probe_root(source: SourceSpec, *, config: RuntimeConfig) -> Path:
    if source.asset_code == "0101" and len(source.paths) == 1:
        root = resolve_project_path(source.paths[0], project_root=config.project_root)
        if root.is_dir():
            return root / "_smoking_data" / "physical_probe"
    return config.data_root / "_physical_probes"


def ensure_probe(
    *,
    source_name: str,
    source: SourceSpec,
    access_profile: dict[str, Any],
    downstream_plan_fingerprints: list[str],
    config: RuntimeConfig,
    index_level: str = "auto",
    allow_additive_columns: bool = False,
    output_root: str | Path | None = None,
) -> ProbeHandle:
    probe_started = time_module.perf_counter()
    asset_contract = load_effective_asset_contract(config.project_root, "0101")
    partition_grid = ProbePartitionGrid(
        anchor_date=date.fromisoformat(partition_grid_anchor(asset_contract)),
        step_days=partition_grid_step_days(asset_contract),
    )
    if index_level not in {"auto", "row_group", "page"}:
        raise ValidationError(
            "probe.index.level must be auto, row_group, or page.",
            code="probe.invalid_index_level",
            context={"value": index_level},
        )
    roots = [resolve_project_path(path, project_root=config.project_root) for path in source.paths]
    files = discover_parquet_files(roots, recursive=True)
    if not files:
        raise ValidationError(
            f"Probe source has no parquet files: {source_name}",
            code="probe.source_empty",
            context={"source": source_name, "paths": list(source.paths)},
        )

    source_identity = _hash_json(
        {
            "paths": sorted(_portable_path(root.resolve(), config.project_root) for root in roots),
        }
    )[:16]
    artifact_root = (
        resolve_project_path(output_root, project_root=config.project_root)
        if output_root is not None
        else config.data_root / "_physical_probes"
    )
    output_root = (artifact_root / f"source-{source_identity}").resolve()
    source_directories = [root.resolve() for root in roots if root.is_dir()]
    if any(
        output_root == root
        or (
            output_root.is_relative_to(root)
            and "_smoking_data" not in output_root.relative_to(root).parts
        )
        for root in source_directories
    ):
        raise ValidationError(
            "Probe output must not be located inside a source dataset.",
            code="probe.output_inside_source",
            context={"source": source_name},
        )
    output_root = ensure_dir(output_root)
    previous = _read_latest_manifest(output_root)
    if previous is not None and (
        previous.get("schema_version") != MANIFEST_VERSION
        or not _artifacts_valid(
            Path(str(previous["__manifest_path"])).parent,
            previous,
        )
    ):
        previous = None
    identities = [
        _inspect_file_identity(item.path, project_root=config.project_root) for item in files
    ]
    dataset_fingerprint = _hash_json(
        {
            "version": FINGERPRINT_VERSION,
            "files": [
                {
                    "source_file": item["source_file"],
                    "size_bytes": item["size_bytes"],
                    "footer_fingerprint": item["footer_fingerprint"],
                }
                for item in identities
            ],
        }
    )
    previous_files = _read_previous_file_profiles(previous)
    unchanged_files = {
        item["source_file"]
        for item in identities
        if _same_file_identity(item, previous_files.get(item["source_file"]))
    }
    inspected_profiles: dict[str, dict[str, Any]] = {}
    first_identity = identities[0]
    first_previous = previous_files.get(first_identity["source_file"])
    if first_identity["source_file"] in unchanged_files and first_previous is not None:
        first_profile = dict(first_previous)
    else:
        first_profile = _inspect_file(files[0].path, project_root=config.project_root)
        inspected_profiles[first_identity["source_file"]] = first_profile
    actual_columns = {item["name"] for item in first_profile["schema"]["fields"]}
    requested_columns = set(access_profile.get("required_columns") or actual_columns)
    missing_columns = sorted(requested_columns - actual_columns)
    if missing_columns:
        raise ValidationError(
            "Probe access profile references columns missing from the source schema.",
            code="probe.missing_required_column",
            context={"source": source_name, "columns": missing_columns},
        )
    logical_required = sorted(
        set(access_profile.get("logical_required_columns") or requested_columns)
    )
    column_lineage = dict(access_profile.get("column_lineage") or {})
    for column in logical_required:
        column_lineage.setdefault(column, [column])
    access_profile = {
        **access_profile,
        "schema_version": ACCESS_PROFILE_SCHEMA_VERSION,
        "required_columns": sorted(requested_columns),
        "physical_columns": sorted(requested_columns),
        "logical_required_columns": logical_required,
        "derived_columns": sorted(access_profile.get("derived_columns") or []),
        "column_lineage": column_lineage,
        "derived_lineage": dict(access_profile.get("derived_lineage") or {}),
        "selector_derived_columns": sorted(
            access_profile.get("selector_derived_columns") or []
        ),
    }
    access_profile = {**access_profile, "fingerprint": _hash_json(access_profile)}
    policy_fingerprint = _hash_json(
        {
            "allow_additive_columns": allow_additive_columns,
            "index_level": index_level,
            "physical_probe_version": PHYSICAL_PROBE_VERSION,
            "partition_grid": {
                "anchor_date": partition_grid.anchor_date.isoformat(),
                "step_days": partition_grid.step_days,
            },
        }
    )
    if _manifest_reusable(
        previous,
        dataset_fingerprint=dataset_fingerprint,
        access_fingerprint=access_profile["fingerprint"],
        policy_fingerprint=policy_fingerprint,
    ):
        return replace(
            _handle_from_manifest(
                Path(str(previous["__manifest_path"])), previous, reused=True
            ),
            source_name=source_name,
        )

    file_profiles: list[dict[str, Any]] = []
    changed_paths: list[Path] = []
    for dataset_file, identity in zip(files, identities, strict=True):
        source_file = identity["source_file"]
        if source_file in inspected_profiles:
            profile = inspected_profiles[source_file]
            changed_paths.append(dataset_file.path)
        elif source_file in unchanged_files:
            profile = dict(previous_files[source_file])
        else:
            profile = _inspect_file(dataset_file.path, project_root=config.project_root)
            changed_paths.append(dataset_file.path)
        file_profiles.append(profile)

    for profile in file_profiles:
        profile["partition_keys"] = list(
            partition_grid.keys_for_source_file(str(profile["source_file"]))
        )

    canonical_schema = file_profiles[0]["schema"]
    drift = _schema_drift_report(
        file_profiles=file_profiles,
        canonical_schema=canonical_schema,
        previous=previous,
        required_columns=list(access_profile["required_columns"]),
        allow_additive_columns=allow_additive_columns,
    )
    if drift["status"] == "error":
        incompatible_file_schema = any(
            item.get("scope") == "dataset_file" and item.get("change") == "field_changed"
            for item in drift["differences"]
        )
        raise SmokingDataError(
            "Probe detected incompatible Parquet schema drift.",
            code="source.incompatible_dtype" if incompatible_file_schema else "probe.schema_drift",
            context={"source": source_name, "differences": drift["differences"]},
        )

    lock_path = output_root / ".probe.lock"
    lock_fd = _acquire_lock(lock_path)
    staging = output_root / ".temp" / uuid.uuid4().hex
    try:
        _recover_orphan_staging(output_root / ".temp")
        ensure_dir(staging)
        _write_json(
            staging / ".probe-staging.json",
            {"schema_version": PHYSICAL_PROBE_VERSION, "pid": os.getpid()},
        )
        previous_root = (
            Path(str(previous["__manifest_path"])).parent if previous is not None else None
        )
        artifact_result = _write_partitioned_probe_artifacts(
            staging=staging,
            file_profiles=file_profiles,
            files=files,
            previous_files=previous_files,
            previous_root=previous_root,
            previous=previous,
            unchanged_files=unchanged_files,
            project_root=config.project_root,
            page_index_enabled=index_level in {"auto", "page"},
        )
        row_group_count = int(artifact_result["row_group_rows"])
        page_row_count = int(artifact_result["page_rows"])
        page_available = bool(artifact_result["page_available"])
        if index_level == "page" and not page_available:
            raise SmokingDataError(
                "probe.index.level=page requires OffsetIndex in every input file.",
                code="probe.page_index_required",
                context={"source": source_name},
            )
        artifact_paths = {
            "schema_drift": "schema_drift.json",
            "access_profile": "access_profile.json",
            "files": "files",
            "row_groups": "row_groups",
        }
        _write_json(staging / "schema_drift.json", drift)
        _write_json(staging / "access_profile.json", access_profile)
        if page_available:
            artifact_paths["pages"] = "pages"
        capabilities = {
            "footer": True,
            "row_group": True,
            "column_chunk": True,
            "statistics": any(
                bool(profile.get("statistics_available", True)) for profile in file_profiles
            ),
            "page_index": page_available,
            "local_files": True,
            "object_range_read": False,
        }
        indexed_columns = sorted(access_profile["required_columns"])
        excluded_columns = sorted(actual_columns - set(indexed_columns))
        manifest = {
            "schema_version": MANIFEST_VERSION,
            "probe_schema_version": PHYSICAL_PROBE_VERSION,
            "source_name": source_name,
            "source_identity": source_identity,
            "dataset_fingerprint": dataset_fingerprint,
            "fingerprint_version": FINGERPRINT_VERSION,
            "policy_fingerprint": policy_fingerprint,
            "access_profile_fingerprint": access_profile["fingerprint"],
            "downstream_plan_fingerprints": sorted(set(downstream_plan_fingerprints)),
            "canonical_schema": canonical_schema,
            "canonical_schema_fingerprint": _hash_json(canonical_schema),
            "schema_drift_status": drift["status"],
            "file_count": len(file_profiles),
            "row_group_rows": row_group_count,
            "page_rows": page_row_count,
            "capabilities": capabilities,
            "artifacts": artifact_paths,
            "layout": "partitioned",
            "partitioning": {
                "anchor_date": partition_grid.anchor_date.isoformat(),
                "step_days": partition_grid.step_days,
                "partitions": artifact_result["partitions"],
                "partition_count": len(artifact_result["partitions"]),
                "unassigned_file_count": artifact_result["unassigned_file_count"],
            },
            "index_level_requested": index_level,
            "page_index_reason": "available" if page_available else "offset_index_missing",
            "column_indexing": {
                "indexed_columns": indexed_columns,
                "excluded_columns": [
                    {"column": column, "reason": "not_required_by_downstream"}
                    for column in excluded_columns
                ],
            },
            "generator": _tool_versions(),
            "reuse": {
                "reused_files": len(unchanged_files),
                "rebuilt_files": len(changed_paths),
                "removed_files": len(
                    set(previous_files) - {item["source_file"] for item in identities}
                ),
                "reused_partitions": artifact_result["reused_partitions"],
                "rebuilt_partitions": artifact_result["rebuilt_partitions"],
                "removed_partitions": artifact_result["removed_partitions"],
            },
            "execution": {
                "elapsed_sec": time_module.perf_counter() - probe_started,
                "identity_files_inspected": len(identities),
                "footer_profiles_reused": len(unchanged_files),
                "footer_profiles_rebuilt": len(changed_paths),
            },
        }
        manifest["artifact_details"] = {
            name: _artifact_detail(staging, relative_path)
            for name, relative_path in artifact_paths.items()
        }
        _write_json(staging / "manifest.json", manifest)
        _validate_source_unchanged(files, file_profiles, project_root=config.project_root)
        generation_id = uuid.uuid4().hex
        final_root = output_root / "generations" / generation_id
        ensure_dir(final_root.parent)
        _publish_directory(staging, final_root)
        _write_json_atomic(
            output_root / "latest.json",
            {
                "schema_version": "smoking-data.probe-catalog.v1",
                "manifest": f"generations/{generation_id}/manifest.json",
                "dataset_fingerprint": dataset_fingerprint,
            },
        )
        _cleanup_probe_generations(
            output_root,
            keep={
                generation_id,
                _generation_id_from_manifest(previous),
            },
        )
        _cleanup_empty_temp(output_root / ".temp")
        return _handle_from_manifest(final_root / "manifest.json", manifest, reused=False)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        _cleanup_empty_temp(output_root / ".temp")
        raise
    finally:
        os.close(lock_fd)
        lock_path.unlink(missing_ok=True)


def _apply_probe_operation_defaults(
    operation: dict[str, Any],
    defaults: dict[str, Any],
) -> dict[str, Any]:
    result = dict(operation)
    for key in ("index", "schema_drift"):
        configured = defaults.get(key)
        if isinstance(configured, dict):
            result[key] = deep_merge(configured, result.get(key) or {})
    return result


def _execute_probe_operations(
    path: Path,
    *,
    payload: dict[str, Any],
    config: RuntimeConfig,
) -> dict[str, ProbeHandle]:
    job_name = str(_mapping(payload.get("job"), path="job").get("name") or "")
    output = _parse_probe_output(payload.get("output"), project_root=config.project_root)
    operation = next(item for item in payload["operations"] if item["op"] == "probe_pipeline")
    pipeline_paths = operation["pipeline_yaml"]
    index = _mapping(operation.get("index") or {}, path="operations.probe_pipeline.index")
    drift = _mapping(
        operation.get("schema_drift") or {},
        path="operations.probe_pipeline.schema_drift",
    )
    source_override = _mapping(
        operation.get("source_override") or {},
        path="operations.probe_pipeline.source_override",
    )
    override_paths = source_override.get("paths")

    grouped: dict[tuple[str, tuple[str, ...]], dict[str, Any]] = {}
    for pipeline_value in pipeline_paths:
        pipeline_path = Path(str(pipeline_value))
        if not pipeline_path.is_absolute():
            pipeline_path = path.parent / pipeline_path
        pipeline_spec = load_pipeline_spec(pipeline_path, config=config)
        if override_paths is not None and len(pipeline_spec.sources) != 1:
            raise ValidationError(
                "probe_pipeline.source_override.paths requires each downstream pipeline "
                "to have one source."
            )
        resolved_sources = {
            name: (
                SourceSpec(
                    name=original_source.name,
                    kind=original_source.kind,
                    paths=tuple(str(value) for value in override_paths),
                    union_by_name=original_source.union_by_name,
                    missing_columns=original_source.missing_columns,
                    incompatible_dtypes=original_source.incompatible_dtypes,
                )
                if override_paths is not None
                else original_source
            )
            for name, original_source in pipeline_spec.sources.items()
        }
        profiles = _resolve_pipeline_access_profiles(
            pipeline_spec,
            sources=resolved_sources,
            config=config,
        )
        for name, source in resolved_sources.items():
            key = (name, tuple(sorted(source.paths)))
            state = grouped.setdefault(
                key,
                {
                    "source": source,
                    "profile": {
                        "schema_version": ACCESS_PROFILE_SCHEMA_VERSION,
                        "source_name": name,
                        "required_columns": set(),
                        "selector_columns": set(),
                        "partition_and_group_columns": set(),
                        "payload_columns": set(),
                        "logical_required_columns": set(),
                        "derived_columns": set(),
                        "selector_derived_columns": set(),
                        "column_lineage": {},
                        "derived_lineage": {},
                    },
                    "plans": [],
                    "jobs": [],
                },
            )
            profile = profiles[name]
            for field in (
                "required_columns",
                "selector_columns",
                "partition_and_group_columns",
                "payload_columns",
                "logical_required_columns",
                "derived_columns",
                "selector_derived_columns",
            ):
                state["profile"][field].update(profile.get(field) or [])
            for field in ("column_lineage", "derived_lineage"):
                target = state["profile"][field]
                for column, roots in (profile.get(field) or {}).items():
                    target.setdefault(column, set()).update(roots)
            state["plans"].append(pipeline_spec.logical_plan.plan_hash)
            state["jobs"].append(pipeline_spec.job_name)
    handles: dict[str, ProbeHandle] = {}
    for (name, _), state in sorted(grouped.items()):
        profile = {
            key: (
                {column: sorted(roots) for column, roots in sorted(value.items())}
                if isinstance(value, dict)
                else sorted(value)
                if isinstance(value, set)
                else value
            )
            for key, value in state["profile"].items()
        }
        profile["physical_columns"] = list(profile["required_columns"])
        profile["projection_width"] = len(profile["required_columns"])
        handle = ensure_probe(
            source_name=name,
            source=state["source"],
            access_profile=profile,
            downstream_plan_fingerprints=state["plans"],
            config=config,
            index_level=str(index.get("level") or "auto"),
            allow_additive_columns=bool(drift.get("allow_additive_columns", False)),
            output_root=output["artifact_root"],
        )
        handles[f"{'+'.join(sorted(set(state['jobs'])))}:{name}"] = handle
    log_path = output["logging_root"] / f"{_safe_name(job_name)}.jsonl"
    localized_handles: dict[str, ProbeHandle] = {}
    for name, handle in handles.items():
        provenance_root = handle.manifest_path.parent / "_smoking_data"
        ensure_dir(provenance_root)
        metadata_path = provenance_root / "metadata.json"
        definition_path = provenance_root / "definition.yaml"
        definition_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        localized = replace(handle, metadata_path=metadata_path, log_path=log_path)
        _write_json_atomic(
            metadata_path,
            {
                "schema_version": "smoking-data.artifact-metadata.v1",
                "probe_schema_version": PROBE_SCHEMA_VERSION,
                "runtime": {"kind": "parquet_probe", "job_name": job_name},
                "definition_path": "_smoking_data/definition.yaml",
                "output": localized.to_dict(),
            },
        )
        localized_handles[name] = localized
    append_stage_event(
        log_path,
        event="probe.finish",
        preset=PROBE_SCHEMA_VERSION,
        job_name=job_name,
        details={"output_count": len(localized_handles)},
    )
    return localized_handles


def validate_probe_manifest(
    manifest_path: str | Path,
    *,
    files: Iterable[Path],
    project_root: Path,
) -> dict[str, Any]:
    path = Path(manifest_path)
    manifest = _read_json(path)
    if not manifest or manifest.get("schema_version") != MANIFEST_VERSION:
        raise SmokingDataError("Invalid Probe manifest.", code="probe.invalid_manifest")
    if not _artifacts_valid(path.parent, manifest):
        raise SmokingDataError(
            "Probe manifest artifact is missing or has a checksum mismatch.",
            code="probe.invalid_artifact",
        )
    profiles = [_inspect_file(Path(item), project_root=project_root) for item in files]
    current = _hash_json(
        {
            "version": FINGERPRINT_VERSION,
            "files": [
                {
                    "source_file": item["source_file"],
                    "size_bytes": item["size_bytes"],
                    "footer_fingerprint": item["footer_fingerprint"],
                }
                for item in profiles
            ],
        }
    )
    if current != manifest.get("dataset_fingerprint"):
        raise SmokingDataError(
            "Probe manifest does not match the current source dataset.",
            code="probe.stale_manifest",
            context={"expected": manifest.get("dataset_fingerprint"), "actual": current},
        )
    return manifest


def parquet_footer_fingerprint(path: str | Path) -> str:
    """Return the bounded-cost Parquet identity used by Probe manifests."""

    return _footer_fingerprint(Path(path).resolve())


def _pipeline_access_profiles(spec: PipelineSpec) -> dict[str, dict[str, Any]]:
    logical_required: set[str] = set()
    selector_logical: set[str] = set()
    partition_logical: set[str] = set()
    column_lineage: dict[str, set[str]] = {}
    generated_columns = {
        output.name
        for operation in spec.logical_plan.operations
        for output in operation.output_columns
    }
    derived_columns: set[str] = set()
    selector_derived: set[str] = set()
    for operation in spec.logical_plan.operations:
        columns = set(operation.input_columns)
        columns.update(operation.partition_keys)
        columns.update(operation.group_keys)
        columns.update(column for column, _ in operation.ordering)
        logical_required.update(columns)
        for column in columns:
            direct = operation.alias_lineage.get(column)
            roots = {
                root
                for source_column in (direct or (column,))
                for root in spec.logical_plan.column_lineage.get(source_column, (source_column,))
            }
            column_lineage.setdefault(column, set()).update(roots)
            if column in generated_columns or direct is not None:
                derived_columns.add(column)
        if operation.kind in {OperationKind.BUILD_SIDECAR, OperationKind.ACTIVE_ROW_SELECTION}:
            selector_logical.update(columns)
            selector_derived.update(
                column
                for column in columns
                if column in generated_columns or operation.alias_lineage.get(column) is not None
            )
        partition_logical.update(operation.partition_keys)
        partition_logical.update(operation.group_keys)
    physical_required = {
        root for column in logical_required for root in column_lineage.get(column, {column})
    }
    selector_physical = {
        root for column in selector_logical for root in column_lineage.get(column, {column})
    }
    partition_physical = {
        root for column in partition_logical for root in column_lineage.get(column, {column})
    }
    derived_lineage = {
        column: sorted(column_lineage[column])
        for column in sorted(derived_columns.intersection(logical_required))
    }
    result: dict[str, dict[str, Any]] = {}
    for source_name in spec.sources:
        result[source_name] = {
            "schema_version": ACCESS_PROFILE_SCHEMA_VERSION,
            "source_name": source_name,
            "required_columns": sorted(physical_required),
            "physical_columns": sorted(physical_required),
            "logical_required_columns": sorted(logical_required),
            "derived_columns": sorted(derived_columns.intersection(logical_required)),
            "column_lineage": {
                column: sorted(roots) for column, roots in sorted(column_lineage.items())
            },
            "derived_lineage": derived_lineage,
            "selector_columns": sorted(selector_physical),
            "selector_derived_columns": sorted(selector_derived),
            "partition_and_group_columns": sorted(partition_physical),
            "payload_columns": sorted(physical_required - selector_physical),
            "projection_width": len(physical_required),
        }
    return result


def _resolve_pipeline_access_profiles(
    spec: PipelineSpec,
    *,
    sources: dict[str, SourceSpec],
    config: RuntimeConfig,
) -> dict[str, dict[str, Any]]:
    profiles = _pipeline_access_profiles(spec)
    available_by_source = {
        name: _available_source_columns(source, config=config) for name, source in sources.items()
    }
    available_union = set().union(*available_by_source.values()) if available_by_source else set()
    required = set().union(
        *(set(profile["required_columns"]) for profile in profiles.values())
    )
    missing = sorted(required - available_union)
    if available_union and missing:
        affected = {
            column: roots
            for profile in profiles.values()
            for column, roots in profile["column_lineage"].items()
            if set(roots).intersection(missing)
        }
        raise ValidationError(
            "Probe could not resolve downstream columns to the physical source schema.",
            code="probe.unresolved_column_lineage",
            context={
                "columns": missing,
                "logical_columns": sorted(affected),
                "available_columns": sorted(available_union),
            },
        )
    return {
        name: _source_scoped_access_profile(
            sources[name],
            profiles[name],
            config=config,
            available=available_by_source[name],
        )
        for name in sources
    }


def _source_scoped_access_profile(
    source: SourceSpec,
    profile: dict[str, Any],
    *,
    config: RuntimeConfig,
    available: set[str] | None = None,
) -> dict[str, Any]:
    available = available if available is not None else _available_source_columns(source, config=config)
    scoped = dict(profile)
    for field in (
        "required_columns",
        "selector_columns",
        "partition_and_group_columns",
        "payload_columns",
    ):
        scoped[field] = sorted(available.intersection(profile.get(field) or []))
    scoped["physical_columns"] = list(scoped["required_columns"])
    scoped["column_lineage"] = {
        column: sorted(set(roots).intersection(available))
        for column, roots in (profile.get("column_lineage") or {}).items()
        if set(roots).intersection(available)
    }
    scoped["derived_lineage"] = {
        column: sorted(set(roots).intersection(available))
        for column, roots in (profile.get("derived_lineage") or {}).items()
        if set(roots).intersection(available)
    }
    scoped["derived_columns"] = sorted(scoped["derived_lineage"])
    scoped["logical_required_columns"] = sorted(scoped["column_lineage"])
    scoped["selector_derived_columns"] = sorted(
        set(profile.get("selector_derived_columns") or []).intersection(
            scoped["derived_lineage"]
        )
    )
    if not scoped["required_columns"]:
        scoped["required_columns"] = sorted(available)
        scoped["physical_columns"] = list(scoped["required_columns"])
    scoped["projection_width"] = len(scoped["required_columns"])
    return scoped


def _available_source_columns(source: SourceSpec, *, config: RuntimeConfig) -> set[str]:
    roots = [resolve_project_path(path, project_root=config.project_root) for path in source.paths]
    files = discover_parquet_files(roots, recursive=True)
    if not files:
        return set()
    try:
        return set(pq.ParquetFile(files[0].path).schema_arrow.names)
    except (OSError, pa.ArrowException):
        return set()


def _inspect_file_identity(path: Path, *, project_root: Path) -> dict[str, Any]:
    resolved = path.resolve()
    before = resolved.stat()
    footer_fingerprint = _footer_fingerprint(resolved)
    after = resolved.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise SmokingDataError(
            "Parquet source changed while its identity was being inspected.",
            code="probe.source_changed",
            context={"path": str(resolved)},
        )
    return {
        "source_file": _portable_path(resolved, project_root),
        "size_bytes": int(before.st_size),
        "modified_ns_observed": int(before.st_mtime_ns),
        "footer_fingerprint": footer_fingerprint,
    }


def _same_file_identity(current: dict[str, Any], previous: dict[str, Any] | None) -> bool:
    return bool(previous) and all(
        current.get(key) == previous.get(key)
        for key in ("source_file", "size_bytes", "footer_fingerprint")
    )


def _inspect_file(path: Path, *, project_root: Path) -> dict[str, Any]:
    resolved = path.resolve()
    before = resolved.stat()
    footer_fingerprint = _footer_fingerprint(resolved)
    try:
        parquet = pq.ParquetFile(resolved)
    except (OSError, pa.ArrowException) as error:
        message = str(error).lower()
        code = "probe.encrypted_parquet" if "encrypt" in message else "probe.invalid_footer"
        raise SmokingDataError(
            "Unable to read Parquet metadata.",
            code=code,
            context={"source_file": _portable_path(resolved, project_root)},
        ) from error
    metadata = parquet.metadata
    schema = _schema_document(parquet.schema_arrow)
    first_row = 0
    row_groups: list[dict[str, Any]] = []
    for row_group_id in range(metadata.num_row_groups):
        group = metadata.row_group(row_group_id)
        for column_index in range(group.num_columns):
            column = group.column(column_index)
            statistics = column.statistics
            row_groups.append(
                {
                    "source_file": _portable_path(resolved, project_root),
                    "file_fingerprint": footer_fingerprint,
                    "row_group_id": row_group_id,
                    "first_row_index": first_row,
                    "row_count": int(group.num_rows),
                    "column_path": str(column.path_in_schema),
                    "physical_type": str(column.physical_type),
                    "column_chunk_offset": int(
                        min(
                            value
                            for value in (column.dictionary_page_offset, column.data_page_offset)
                            if value is not None
                        )
                    ),
                    "data_page_offset": int(column.data_page_offset),
                    "dictionary_page_offset": (
                        int(column.dictionary_page_offset)
                        if column.dictionary_page_offset is not None
                        else None
                    ),
                    "compressed_bytes": int(column.total_compressed_size),
                    "uncompressed_bytes": int(column.total_uncompressed_size),
                    "value_count": int(column.num_values),
                    "compression": str(column.compression),
                    "encodings": [str(value) for value in column.encodings],
                    "statistics_available": statistics is not None,
                    "statistics_status": "available" if statistics is not None else "unavailable",
                    # 서로 다른 Parquet physical type의 min/max를 한 sidecar column에
                    # 안전하게 담기 위해 canonical JSON 문자열로 보존한다.
                    "statistics_json": json.dumps(
                        _statistics_document(statistics),
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ),
                    "column_index_available": bool(column.has_column_index),
                    "offset_index_available": bool(column.has_offset_index),
                    "row_group_schema_version": ROW_GROUP_SCHEMA_VERSION,
                }
            )
        first_row += int(group.num_rows)
    after = resolved.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise SmokingDataError(
            "Parquet source changed while its footer was being inspected.",
            code="probe.source_changed",
            context={"path": str(resolved)},
        )
    return {
        "source_file": _portable_path(resolved, project_root),
        "size_bytes": int(before.st_size),
        "modified_ns_observed": int(before.st_mtime_ns),
        "footer_fingerprint": footer_fingerprint,
        "schema_fingerprint": _hash_json(schema),
        "schema": schema,
        "rows": int(metadata.num_rows),
        "row_group_count": int(metadata.num_row_groups),
        "statistics_available": any(row["statistics_available"] for row in row_groups),
        "row_groups": row_groups,
    }


def _write_partitioned_probe_artifacts(
    *,
    staging: Path,
    file_profiles: list[dict[str, Any]],
    files: list[Any],
    previous_files: dict[str, dict[str, Any]],
    previous_root: Path | None,
    previous: dict[str, Any] | None,
    unchanged_files: set[str],
    project_root: Path,
    page_index_enabled: bool,
) -> dict[str, Any]:
    profiles = {str(item["source_file"]): item for item in file_profiles}
    paths = {
        _portable_path(item.path.resolve(), project_root): item.path.resolve() for item in files
    }
    current_files = set(profiles)
    changed_files = current_files - unchanged_files
    removed_files = set(previous_files) - current_files
    affected: set[str] = set()
    for source_file in changed_files:
        affected.update(profiles[source_file].get("partition_keys") or [])
        affected.update(previous_files.get(source_file, {}).get("partition_keys") or [])
    for source_file in removed_files:
        affected.update(previous_files[source_file].get("partition_keys") or [])

    current_partitions = {
        str(key)
        for profile in file_profiles
        for key in profile.get("partition_keys") or []
    }
    if previous is None or not affected:
        # A policy/access-profile change with identical source files must still
        # rebuild every partition under the new Probe generation.
        affected = set(current_partitions)

    previous_partition_names = set(
        ((previous or {}).get("partitioning") or {}).get("partitions") or []
    )
    unaffected = current_partitions.intersection(previous_partition_names) - affected
    required_artifacts = ["files", "row_groups"]
    if page_index_enabled and bool((previous or {}).get("capabilities", {}).get("page_index")):
        required_artifacts.append("pages")
    for partition_key in list(unaffected):
        if any(
            _partition_artifact_file(previous_root, previous, name, partition_key) is None
            for name in required_artifacts
        ):
            unaffected.remove(partition_key)
            affected.add(partition_key)

    materialized = dict(profiles)
    materialized_files = {
        source_file
        for source_file, profile in profiles.items()
        if affected.intersection(profile.get("partition_keys") or [])
    }
    for source_file in sorted(materialized_files):
        if "row_groups" in materialized[source_file]:
            continue
        refreshed = _inspect_file(paths[source_file], project_root=project_root)
        refreshed["partition_keys"] = list(profiles[source_file]["partition_keys"])
        materialized[source_file] = refreshed
        profiles[source_file].update(refreshed)

    page_rows: dict[str, list[dict[str, Any]]] = {}
    page_available = page_index_enabled
    if page_index_enabled:
        if unaffected and not bool((previous or {}).get("capabilities", {}).get("page_index")):
            page_available = False
        if page_available:
            for source_file in sorted(materialized_files):
                rows = _inspect_page_rows(paths[source_file], project_root=project_root)
                if rows is None:
                    page_available = False
                    page_rows.clear()
                    break
                page_rows[source_file] = rows

    for artifact_name in ("files", "row_groups", "pages"):
        if artifact_name == "pages" and not page_available:
            continue
        for partition_key in sorted(unaffected):
            source_path = _partition_artifact_file(
                previous_root, previous, artifact_name, partition_key
            )
            if source_path is None:
                affected.add(partition_key)
                continue
            target = _partition_output_file(staging / artifact_name, partition_key)
            ensure_dir(target.parent)
            shutil.copy2(source_path, target)

    for partition_key in sorted(affected.intersection(current_partitions)):
        partition_profiles = [
            materialized[source_file]
            for source_file in sorted(materialized)
            if partition_key in (materialized[source_file].get("partition_keys") or [])
        ]
        inventory_rows = [_inventory_row(profile) for profile in partition_profiles]
        _write_partition_rows(staging / "files", partition_key, inventory_rows)
        row_group_rows = [
            row
            for profile in partition_profiles
            for row in profile.get("row_groups") or []
        ]
        _write_partition_rows(staging / "row_groups", partition_key, row_group_rows)
        if page_available:
            partition_page_rows = [
                row
                for profile in partition_profiles
                for row in page_rows.get(str(profile["source_file"]), [])
            ]
            _write_partition_rows(staging / "pages", partition_key, partition_page_rows)

    if not page_available:
        shutil.rmtree(staging / "pages", ignore_errors=True)

    return {
        "row_group_rows": _parquet_dataset_row_count(staging / "row_groups"),
        "page_rows": _parquet_dataset_row_count(staging / "pages") if page_available else 0,
        "page_available": page_available,
        "partitions": sorted(current_partitions),
        "reused_partitions": len(unaffected),
        "rebuilt_partitions": len(affected.intersection(current_partitions)),
        "removed_partitions": len(previous_partition_names - current_partitions),
        "unassigned_file_count": sum(
            1
            for profile in file_profiles
            if "unassigned" in (profile.get("partition_keys") or [])
        ),
    }


def _inspect_page_rows(path: Path, *, project_root: Path) -> list[dict[str, Any]] | None:
    try:
        from smoking_data_engine_rs import inspect_parquet_pages
    except ImportError:
        return None
    document = inspect_parquet_pages(path)
    if not bool(document.get("page_index_available")):
        return None
    return [
        {
            **dict(row),
            "source_file": _portable_path(path.resolve(), project_root),
            "file_fingerprint": _footer_fingerprint(path),
            "page_schema_version": PAGE_SCHEMA_VERSION,
        }
        for row in document.get("pages", [])
    ]


def _inventory_row(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_file": str(profile["source_file"]),
        "size_bytes": int(profile["size_bytes"]),
        "modified_ns_observed": int(profile["modified_ns_observed"]),
        "footer_fingerprint": str(profile["footer_fingerprint"]),
        "schema_fingerprint": str(profile["schema_fingerprint"]),
        "schema_json": json.dumps(
            profile["schema"],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "rows": int(profile["rows"]),
        "row_group_count": int(profile["row_group_count"]),
        "statistics_available": bool(profile.get("statistics_available")),
        "partition_keys": list(profile.get("partition_keys") or []),
    }


def _write_partition_rows(
    artifact_root: Path,
    partition_key: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    target = _partition_output_file(artifact_root, partition_key)
    ensure_dir(target.parent)
    table = pa.Table.from_pylist(rows)
    table = _normalize_nullable_int64(
        table, {"dictionary_page_offset", "dictionary_page_length"}
    )
    pq.write_table(table, target, compression=None, write_statistics=True)


def _partition_output_file(artifact_root: Path, partition_key: str) -> Path:
    return artifact_root / partition_directory_name(partition_key) / "part-000.parquet"


def _partition_artifact_file(
    previous_root: Path | None,
    previous: dict[str, Any] | None,
    artifact_name: str,
    partition_key: str,
) -> Path | None:
    root = _previous_artifact_path(previous_root, previous, artifact_name)
    if root is None:
        return None
    candidate = _partition_output_file(root, partition_key)
    return candidate if candidate.is_file() else None


def _parquet_dataset_row_count(root: Path) -> int:
    return sum(
        int(pq.ParquetFile(path).metadata.num_rows)
        for path in root.glob("partition_start=*/part-*.parquet")
    )


def _normalize_nullable_int64(table: pa.Table, names: set[str]) -> pa.Table:
    for name in names.intersection(table.column_names):
        index = table.schema.get_field_index(name)
        if pa.types.is_null(table.schema.field(index).type):
            table = table.set_column(
                index, name, pa.array([None] * table.num_rows, type=pa.int64())
            )
    return table


def _previous_artifact_path(
    previous_root: Path | None,
    previous: dict[str, Any] | None,
    name: str,
) -> Path | None:
    if previous_root is None or previous is None:
        return None
    relative = (previous.get("artifacts") or {}).get(name)
    if not relative:
        return None
    path = previous_root / str(relative)
    return path if path.exists() else None


def _read_previous_file_profiles(
    previous: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    if previous is None:
        return {}
    root = Path(str(previous.get("__manifest_path") or ".")).parent
    relative = (previous.get("artifacts") or {}).get("files")
    if not relative:
        return {}
    dataset_root = root / str(relative)
    if not dataset_root.exists():
        return {}
    profiles: dict[str, dict[str, Any]] = {}
    for parquet_path in sorted(dataset_root.rglob("*.parquet")):
        for row in pq.ParquetFile(parquet_path).read().to_pylist():
            source_file = str(row.get("source_file") or "")
            if not source_file or source_file in profiles:
                continue
            profiles[source_file] = {
                "source_file": source_file,
                "size_bytes": int(row["size_bytes"]),
                "modified_ns_observed": int(row["modified_ns_observed"]),
                "footer_fingerprint": str(row["footer_fingerprint"]),
                "schema_fingerprint": str(row["schema_fingerprint"]),
                "schema": json.loads(str(row["schema_json"])),
                "rows": int(row["rows"]),
                "row_group_count": int(row["row_group_count"]),
                "statistics_available": bool(row["statistics_available"]),
                "partition_keys": [str(value) for value in row.get("partition_keys") or []],
            }
    return profiles


def _schema_document(schema: pa.Schema) -> dict[str, Any]:
    return {
        "fields": [_field_document(field) for field in schema],
        "metadata": _metadata_document(schema.metadata),
    }


def _field_document(field: pa.Field) -> dict[str, Any]:
    dtype = field.type
    children: list[dict[str, Any]] = []
    if pa.types.is_struct(dtype):
        children = [_field_document(child) for child in dtype]
    elif (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
    ):
        children = [_field_document(dtype.value_field)]
    elif pa.types.is_map(dtype):
        children = [_field_document(dtype.key_field), _field_document(dtype.item_field)]
    return {
        "name": field.name,
        "type": str(dtype),
        "nullable": field.nullable,
        "metadata": _metadata_document(field.metadata),
        "children": children,
    }


def _metadata_document(metadata: dict[bytes, bytes] | None) -> dict[str, str]:
    if not metadata:
        return {}
    return {
        base64.b64encode(key).decode("ascii"): base64.b64encode(value).decode("ascii")
        for key, value in sorted(metadata.items())
    }


def _statistics_document(statistics: Any) -> dict[str, Any] | None:
    if statistics is None:
        return None
    return {
        "has_min_max": bool(statistics.has_min_max),
        "min": _json_scalar(statistics.min) if statistics.has_min_max else None,
        "max": _json_scalar(statistics.max) if statistics.has_min_max else None,
        "null_count": int(statistics.null_count) if statistics.has_null_count else None,
        "distinct_count": int(statistics.distinct_count) if statistics.has_distinct_count else None,
        "physical_type": str(statistics.physical_type),
    }


def _tool_versions() -> dict[str, str]:
    try:
        smoking_data_version = importlib.metadata.version("smoking-data")
    except importlib.metadata.PackageNotFoundError:
        smoking_data_version = "development"
    return {
        "smoking_data": smoking_data_version,
        "pyarrow": pa.__version__,
        "physical_probe": PHYSICAL_PROBE_VERSION,
    }


def _json_scalar(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def _schema_drift_report(
    *,
    file_profiles: list[dict[str, Any]],
    canonical_schema: dict[str, Any],
    previous: dict[str, Any] | None,
    required_columns: list[str],
    allow_additive_columns: bool,
) -> dict[str, Any]:
    differences: list[dict[str, Any]] = []
    canonical_fields = {item["name"]: item for item in canonical_schema["fields"]}
    for profile in file_profiles[1:]:
        if profile["schema"] != canonical_schema:
            differences.extend(
                _compare_schema(
                    canonical_schema,
                    profile["schema"],
                    scope="dataset_file",
                    source_file=profile["source_file"],
                    allow_additive_columns=allow_additive_columns,
                )
            )
    if previous and isinstance(previous.get("canonical_schema"), dict):
        differences.extend(
            _compare_schema(
                previous["canonical_schema"],
                canonical_schema,
                scope="previous_baseline",
                source_file=None,
                allow_additive_columns=allow_additive_columns,
            )
        )
    for column in required_columns:
        if column not in canonical_fields:
            differences.append(
                {
                    "scope": "downstream_required",
                    "column": column,
                    "change": "missing_required_column",
                    "severity": "error",
                }
            )
    status = (
        "error"
        if any(item["severity"] == "error" for item in differences)
        else ("warning" if differences else "ok")
    )
    return {
        "schema_version": "smoking-data.schema-drift.v1",
        "status": status,
        "allow_additive_columns": allow_additive_columns,
        "canonical_schema": canonical_schema,
        "canonical_schema_fingerprint": _hash_json(canonical_schema),
        "file_schema_fingerprints": {
            item["source_file"]: item["schema_fingerprint"] for item in file_profiles
        },
        "required_columns": required_columns,
        "differences": differences,
    }


def _compare_schema(
    before: dict[str, Any],
    after: dict[str, Any],
    *,
    scope: str,
    source_file: str | None,
    allow_additive_columns: bool,
) -> list[dict[str, Any]]:
    old = {item["name"]: item for item in before["fields"]}
    new = {item["name"]: item for item in after["fields"]}
    result: list[dict[str, Any]] = []
    for name in old.keys() - new.keys():
        result.append(_difference(scope, source_file, name, "removed", "error"))
    for name in new.keys() - old.keys():
        result.append(
            _difference(
                scope, source_file, name, "added", "allowed" if allow_additive_columns else "error"
            )
        )
    for name in old.keys() & new.keys():
        if old[name] != new[name]:
            result.append(_difference(scope, source_file, name, "field_changed", "error"))
    old_order = [item["name"] for item in before["fields"]]
    new_order = [item["name"] for item in after["fields"]]
    if old_order != new_order and set(old_order) == set(new_order):
        result.append(_difference(scope, source_file, None, "column_order_changed", "warning"))
    return result


def _difference(
    scope: str, source_file: str | None, column: str | None, change: str, severity: str
) -> dict[str, Any]:
    return {
        "scope": scope,
        "source_file": source_file,
        "column": column,
        "change": change,
        "severity": severity,
    }


def _footer_fingerprint(path: Path) -> str:
    size = path.stat().st_size
    if size < 12:
        raise SmokingDataError(f"Invalid Parquet file: {path}", code="probe.invalid_footer")
    with path.open("rb") as handle:
        handle.seek(-8, os.SEEK_END)
        trailer = handle.read(8)
        if trailer[4:] != b"PAR1":
            raise SmokingDataError(
                f"Invalid Parquet footer magic: {path}", code="probe.invalid_footer"
            )
        footer_length = struct.unpack("<I", trailer[:4])[0]
        footer_start = size - 8 - footer_length
        if footer_start < 4:
            raise SmokingDataError(
                f"Invalid Parquet footer length: {path}", code="probe.invalid_footer"
            )
        handle.seek(footer_start)
        footer = handle.read(footer_length)
    digest = hashlib.sha256()
    digest.update(FINGERPRINT_VERSION.encode("ascii"))
    digest.update(struct.pack("<Q", size))
    digest.update(footer)
    return f"{FINGERPRINT_VERSION}:{digest.hexdigest()}"


def _portable_path(path: Path, project_root: Path) -> str:
    try:
        return path.relative_to(project_root.resolve()).as_posix()
    except ValueError:
        digest = hashlib.sha256(str(path.parent).encode("utf-8")).hexdigest()[:12]
        return f"external/{digest}/{path.name}"


def _write_rows_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        pq.write_table(
            pa.table({"__empty": pa.array([], type=pa.bool_())}),
            path,
            compression=None,
        )
        return
    pq.write_table(pa.Table.from_pylist(rows), path, compression=None, write_statistics=True)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False),
        encoding="utf-8",
    )


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    _write_json(temporary, payload)
    os.replace(temporary, path)


def _artifact_detail(staging: Path, relative_path: str) -> dict[str, Any]:
    path = staging / relative_path
    if path.is_file():
        return {
            "relative_path": relative_path,
            "kind": "file",
            "size_bytes": path.stat().st_size,
            "sha256": _file_sha256(path),
        }
    files = sorted(item for item in path.rglob("*.parquet") if item.is_file())
    return {
        "relative_path": relative_path,
        "kind": "parquet_dataset",
        "size_bytes": sum(item.stat().st_size for item in files),
        "files": [
            {
                "relative_path": item.relative_to(path).as_posix(),
                "size_bytes": item.stat().st_size,
                "sha256": _file_sha256(item),
            }
            for item in files
        ],
    }


def _publish_directory(staging: Path, final: Path) -> None:
    backup = final.with_name(f".{final.name}.backup-{uuid.uuid4().hex}")
    moved = False
    try:
        if final.exists():
            os.replace(final, backup)
            moved = True
        os.replace(staging, final)
    except BaseException:
        if moved and backup.exists() and not final.exists():
            os.replace(backup, final)
        raise
    shutil.rmtree(backup, ignore_errors=True)


def _generation_id_from_manifest(manifest: dict[str, Any] | None) -> str:
    if manifest is None:
        return ""
    path = Path(str(manifest.get("__manifest_path") or ""))
    return path.parent.name if path.parent.parent.name == "generations" else ""


def _cleanup_probe_generations(output_root: Path, *, keep: set[str]) -> None:
    generations = output_root / "generations"
    if not generations.is_dir():
        return
    retained = {value for value in keep if value}
    for candidate in generations.iterdir():
        if candidate.is_dir() and candidate.name not in retained:
            shutil.rmtree(candidate)


def _manifest_reusable(
    manifest: dict[str, Any] | None,
    *,
    dataset_fingerprint: str,
    access_fingerprint: str,
    policy_fingerprint: str,
) -> bool:
    if not manifest:
        return False
    if (
        manifest.get("schema_version") != MANIFEST_VERSION
        or manifest.get("dataset_fingerprint") != dataset_fingerprint
        or manifest.get("access_profile_fingerprint") != access_fingerprint
        or manifest.get("policy_fingerprint") != policy_fingerprint
    ):
        return False
    root = Path(str(manifest.get("__manifest_path") or ".")).parent
    return _artifacts_valid(root, manifest)


def _artifacts_valid(root: Path, manifest: dict[str, Any]) -> bool:
    details = manifest.get("artifact_details")
    if not isinstance(details, dict) or not details:
        return False
    for item in details.values():
        if not isinstance(item, dict) or not item.get("relative_path"):
            return False
        path = root / str(item["relative_path"])
        if item.get("kind") == "parquet_dataset":
            files = item.get("files")
            if not path.is_dir() or not isinstance(files, list) or not files:
                return False
            for detail in files:
                if not isinstance(detail, dict) or not detail.get("relative_path"):
                    return False
                candidate = path / str(detail["relative_path"])
                if (
                    not candidate.is_file()
                    or candidate.stat().st_size != int(detail.get("size_bytes") or -1)
                    or _file_sha256(candidate) != detail.get("sha256")
                ):
                    return False
            continue
        if (
            not path.is_file()
            or path.stat().st_size != int(item.get("size_bytes") or -1)
            or _file_sha256(path) != item.get("sha256")
        ):
            return False
    return True


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    payload["__manifest_path"] = str(path)
    return payload


def _read_latest_manifest(output_root: Path) -> dict[str, Any] | None:
    pointer = _read_json(output_root / "latest.json")
    if not pointer or not pointer.get("manifest"):
        return None
    return _read_json(output_root / str(pointer["manifest"]))


def _handle_from_manifest(path: Path, manifest: dict[str, Any], *, reused: bool) -> ProbeHandle:
    return ProbeHandle(
        source_name=str(manifest["source_name"]),
        manifest_path=path,
        dataset_fingerprint=str(manifest["dataset_fingerprint"]),
        probe_schema_version=str(manifest["probe_schema_version"]),
        capabilities=dict(manifest["capabilities"]),
        artifact_paths={
            key: path.parent / str(value) for key, value in manifest["artifacts"].items()
        },
        reused=reused,
    )


def _validate_source_unchanged(
    files: list[Any], profiles: list[dict[str, Any]], *, project_root: Path
) -> None:
    expected = {item["source_file"]: item for item in profiles}
    for item in files:
        current = _inspect_file(item.path, project_root=project_root)
        previous = expected[current["source_file"]]
        if (
            current["size_bytes"] != previous["size_bytes"]
            or current["footer_fingerprint"] != previous["footer_fingerprint"]
        ):
            raise SmokingDataError(
                "Parquet source changed before Probe publish.",
                code="probe.source_changed",
                context={"source_file": current["source_file"]},
            )


def _hash_json(payload: Any) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _acquire_lock(path: Path) -> int:
    for attempt in range(2):
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.write(
                descriptor,
                json.dumps(
                    {"pid": os.getpid(), "schema_version": PHYSICAL_PROBE_VERSION}
                ).encode(),
            )
            return descriptor
        except FileExistsError as error:
            owner = _read_json(path)
            owner_pid = int((owner or {}).get("pid") or -1)
            if attempt == 0 and not _pid_alive(owner_pid):
                path.unlink(missing_ok=True)
                continue
            raise SmokingDataError(
                "Another Probe process is publishing the same source.",
                code="probe.concurrent_publish",
                context={"lock": path.name, "owner_pid": owner_pid},
            ) from error
    raise AssertionError("unreachable")


def _recover_orphan_staging(temp_root: Path) -> int:
    if not temp_root.is_dir():
        return 0
    removed = 0
    for candidate in temp_root.iterdir():
        marker = _read_json(candidate / ".probe-staging.json") if candidate.is_dir() else None
        if marker is None or marker.get("schema_version") != PHYSICAL_PROBE_VERSION:
            continue
        if _pid_alive(int(marker.get("pid") or -1)):
            continue
        shutil.rmtree(candidate)
        removed += 1
    return removed


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _safe_name(value: str) -> str:
    text = "".join(
        character if character.isalnum() or character in "-_" else "_" for character in value
    )
    return text or "source"


def _cleanup_empty_temp(path: Path) -> None:
    try:
        path.rmdir()
    except OSError:
        pass


def _parse_probe_output(value: Any, *, project_root: Path) -> dict[str, Path]:
    output = _mapping(value, path="output")
    _reject_unknown(output, {"artifact", "logging"}, path="output")
    artifact = _mapping(output.get("artifact"), path="output.artifact")
    _reject_unknown(
        artifact,
        {"type", "root_dir", "layout"},
        path="output.artifact",
    )
    if artifact.get("type") != "probe_sidecar":
        raise ValidationError(
            "output.artifact.type must be probe_sidecar for the internal parquet probe.",
            code="output.invalid_artifact_type",
        )
    if artifact.get("layout") != "partitioned":
        raise ValidationError(
            "Internal parquet probe output.artifact.layout must be partitioned.",
            code="output.invalid_layout",
        )
    roots: dict[str, Path] = {}
    for name, section_value in (
        ("artifact", artifact),
        ("logging", output.get("logging")),
    ):
        section = _mapping(section_value, path=f"output.{name}")
        allowed = {"root_dir", "type", "layout"} if name == "artifact" else {"root_dir"}
        _reject_unknown(section, allowed, path=f"output.{name}")
        root_dir = str(section.get("root_dir") or "").strip()
        if not root_dir:
            raise ValidationError(f"output.{name}.root_dir must be a non-empty string.")
        roots[f"{name}_root"] = resolve_project_path(root_dir, project_root=project_root)
    return roots


def _mapping(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{path} must be a mapping.")
    return value


def _reject_unknown(value: dict[str, Any], allowed: set[str], *, path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValidationError(
            f"Unsupported keys in {path}: {unknown}",
            code="probe.unknown_key",
            context={"path": path, "keys": unknown},
        )
