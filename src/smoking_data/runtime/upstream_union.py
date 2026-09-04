from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import polars as pl

from smoking_data.core.exceptions import SmokingDataError
from smoking_data.core.pipeline import PipelineSpec, SourceSpec
from smoking_data.core.results import to_json_safe, utc_now_iso
from smoking_data.ops.upstream import discover_parquet_files
from smoking_data.runtime.config import RuntimeConfig
from smoking_data.runtime.paths import resolve_project_path
from smoking_data.runtime.transactions import (
    DatasetTransaction,
    refresh_dataset_manifest_provenance,
    validate_committed_dataset,
)

_DATASET_BOUNDARY_MARKER = ".smoking-data-dataset-boundary.json"


def prepare_combined_sources(
    spec: PipelineSpec,
    *,
    config: RuntimeConfig,
) -> tuple[PipelineSpec, dict[str, Any]]:
    updated_sources = dict(spec.sources)
    raw_sources = {key: dict(value) for key, value in spec.raw["sources"].items()}
    profiles: dict[str, Any] = {}
    for name, source in spec.sources.items():
        if not source.combined_members:
            continue
        cache_root, profile = _materialize_combined_source(source, config=config)
        updated_sources[name] = replace(source, paths=(str(cache_root),))
        raw_sources[name] = {**raw_sources[name], "paths": [str(cache_root)]}
        profiles[name] = profile
    if not profiles:
        return spec, {"enabled": False, "sources": {}}
    return (
        replace(
            spec,
            sources=updated_sources,
            raw={
                **dict(spec.raw),
                "sources": raw_sources,
            },
        ),
        {"enabled": True, "sources": profiles},
    )


def _materialize_combined_source(
    source: SourceSpec,
    *,
    config: RuntimeConfig,
) -> tuple[Path, dict[str, Any]]:
    source_column = dict(source.source_column or {})
    column_name = str(source_column.get("name") or "").strip()
    if not column_name:
        raise SmokingDataError(
            "Combined upstream source column is missing.",
            code="combine_upstream.invalid_source_column",
        )
    members: list[dict[str, Any]] = []
    seen_paths: dict[Path, str] = {}
    fingerprint_files: list[dict[str, Any]] = []
    for member in source.combined_members:
        roots = [
            resolve_project_path(path, project_root=config.project_root)
            for path in member.get("paths") or []
        ]
        files = discover_parquet_files(roots, recursive=True)
        if not files:
            raise SmokingDataError(
                "Combined upstream member has no Parquet files.",
                code="combine_upstream.empty_member",
                context={"alias": member.get("alias"), "paths": [str(item) for item in roots]},
            )
        for item in files:
            previous_alias = seen_paths.get(item.path)
            if previous_alias is not None:
                raise SmokingDataError(
                    "Combined upstream selected the same Parquet file more than once.",
                    code="combine_upstream.duplicate_path",
                    context={
                        "path": str(item.path),
                        "first_alias": previous_alias,
                        "second_alias": member.get("alias"),
                    },
                )
            seen_paths[item.path] = str(member.get("alias"))
            schema = pl.read_parquet_schema(item.path)
            if column_name in schema:
                raise SmokingDataError(
                    "Combined upstream source identity column collides with input data.",
                    code="combine_upstream.source_column_collision",
                    context={"column": column_name, "path": str(item.path)},
                )
            fingerprint_files.append(
                {
                    "path": str(item.path),
                    "size_bytes": item.size_bytes,
                    "modified_ns": item.modified_ns,
                    "source_identity": str(member["source_identity"]),
                    "dataset_shard_id": str(item.dataset_id or ""),
                }
            )
        members.append({**dict(member), "files": files})
    fingerprint_document = {
        "source_name": source.name,
        "source_column": source_column,
        "members": [
            {
                key: value
                for key, value in member.items()
                if key not in {"files", "paths"}
            }
            for member in members
        ],
        "files": sorted(fingerprint_files, key=lambda item: item["path"]),
    }
    encoded = json.dumps(
        to_json_safe(fingerprint_document),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    fingerprint = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    cache_root = config.temp_root / "upstream-union" / f"{source.name}-{fingerprint}.dataset"
    if validate_committed_dataset(cache_root):
        return cache_root, _profile(
            source,
            cache_root=cache_root,
            fingerprint=fingerprint,
            members=members,
            reused=True,
        )

    transaction = DatasetTransaction.create(
        cache_root,
        manifest_context={
            "asset_code": "0201",
            "artifact_type": "combined_upstream_cache",
            "logical_plan_hash": fingerprint,
            "source_name": source.name,
        },
    )
    try:
        for member_index, member in enumerate(members):
            identity = str(member["source_identity"])
            dataset_roots: dict[str, Path] = {}
            for file_index, item in enumerate(member["files"]):
                frame = pl.read_parquet(item.path).with_columns(
                    pl.lit(identity, dtype=pl.String).alias(column_name)
                )
                original_shard_id = str(item.dataset_id or f"flat:{member_index}")
                dataset_shard_id = _combined_dataset_shard_id(
                    member_alias=str(member.get("alias") or member_index),
                    source_identity=identity,
                    original_shard_id=original_shard_id,
                )
                shard_token = dataset_shard_id.rsplit(":", 1)[-1]
                dataset_root = (
                    transaction.staging_root
                    / f"source={member_index:04d}"
                    / f"dataset={shard_token}"
                )
                dataset_roots.setdefault(dataset_shard_id, dataset_root)
                output = (
                    dataset_root
                    / f"data_{file_index:06d}.parquet"
                )
                output.parent.mkdir(parents=True, exist_ok=True)
                frame.write_parquet(output, compression="zstd")
            for dataset_shard_id, dataset_root in dataset_roots.items():
                (dataset_root / _DATASET_BOUNDARY_MARKER).write_text(
                    json.dumps(
                        {
                            "schema_version": "smoking-data.dataset-boundary.v1",
                            "dataset_shard_id": dataset_shard_id,
                            "upstream_alias": str(member.get("alias") or ""),
                            "source_identity": identity,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
        metadata = transaction.staging_root / "_smoking_data" / "upstream-union.json"
        metadata.parent.mkdir(parents=True, exist_ok=True)
        metadata.write_text(
            json.dumps(
                {
                    "schema_version": "smoking-data.upstream-union.v1",
                    "created_at": utc_now_iso(),
                    "fingerprint": fingerprint,
                    "source_column": source_column,
                    "members": [
                        {
                            key: to_json_safe(value)
                            for key, value in member.items()
                            if key != "files"
                        }
                        for member in members
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (transaction.staging_root / "_smoking_data" / "metadata.json").write_text(
            json.dumps(
                {
                    "schema_version": "smoking-data.artifact-metadata.v1",
                    "created_at": utc_now_iso(),
                    "asset": {"code": "0201", "kind": "combined_upstream_cache"},
                    "source_name": source.name,
                    "fingerprint": fingerprint,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        transaction.commit()
        refresh_dataset_manifest_provenance(cache_root)
    except BaseException:
        transaction.abort()
        raise
    return cache_root, _profile(
        source,
        cache_root=cache_root,
        fingerprint=fingerprint,
        members=members,
        reused=False,
    )


def _profile(
    source: SourceSpec,
    *,
    cache_root: Path,
    fingerprint: str,
    members: list[dict[str, Any]],
    reused: bool,
) -> dict[str, Any]:
    return {
        "source": source.name,
        "cache_root": str(cache_root),
        "fingerprint": fingerprint,
        "reused": reused,
        "source_column": dict(source.source_column or {}),
        "members": [
            {
                "alias": member.get("alias"),
                "source_identity": member.get("source_identity"),
                "asset_code": member.get("asset_code"),
                "asset_definition": member.get("asset_definition"),
                "asset_definition_hash": member.get("asset_definition_hash"),
                "selection": member.get("selection"),
                "file_count": len(member.get("files") or []),
            }
            for member in members
        ],
    }


def _combined_dataset_shard_id(
    *, member_alias: str, source_identity: str, original_shard_id: str
) -> str:
    document = {
        "member_alias": member_alias,
        "source_identity": source_identity,
        "original_shard_id": original_shard_id,
    }
    digest = hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:20]
    return f"combined:{member_alias}:{digest}"
