from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pyarrow.parquet as pq

from smoking_data.core.exceptions import SmokingDataError
from smoking_data.runtime.paths import ensure_dir, file_sha256

from .backend import ConditionalWriteConflict, ObjectStore, S3ObjectStore
from .config import ObjectStoreTarget, PublicationSpec, load_object_store_target
from .index_builder import BuiltIndex, build_parquet_indexes
from .remote_manifest import (
    PUBLICATION_RECEIPT_VERSION,
    RemoteObject,
    build_pointer,
    build_remote_manifest,
    canonical_json,
    payload_sha256,
    utc_now,
)
from .sbdf_representation import (
    BuiltSbdfRepresentation,
    build_existing_sbdf_representation,
    build_sbdf_representation,
)


@dataclass(frozen=True, slots=True)
class PublicationResult:
    status: str
    target: str
    dataset_uri: str
    generation_id: str
    manifest_key: str | None
    receipt_path: Path
    uploaded_objects: int
    reused_objects: int


def publish_committed_dataset(
    dataset_root: str | Path,
    *,
    project_root: str | Path,
    publication: PublicationSpec,
    asset_code: str,
    job_name: str,
    definition_sha256: str = "",
    target: ObjectStoreTarget | None = None,
    store_factory: Callable[[ObjectStoreTarget], ObjectStore] = S3ObjectStore,
) -> PublicationResult | None:
    if not publication.enabled:
        return None
    root = Path(dataset_root).expanduser().resolve()
    project = Path(project_root).expanduser().resolve()
    resolved_target = target or load_object_store_target(project, publication.target)
    store = store_factory(resolved_target)
    preflight = getattr(store, "preflight", None)
    if callable(preflight):
        preflight()
    receipt_root = ensure_dir(project / ".smoking-data" / "registry" / "publications")
    dataset_id = payload_sha256(
        {"target": publication.target, "dataset_prefix": publication.dataset_prefix}
    )[:24]
    receipt_path = receipt_root / f"{dataset_id}.json"
    try:
        result = _publish(
            root,
            publication=publication,
            asset_code=asset_code,
            job_name=job_name,
            definition_sha256=definition_sha256,
            target=resolved_target,
            store=store,
            dataset_id=dataset_id,
            receipt_path=receipt_path,
        )
    except BaseException as exc:
        _write_receipt(
            receipt_path,
            {
                "schema_version": PUBLICATION_RECEIPT_VERSION,
                "status": "pending" if publication.failure_policy == "warn_and_retry" else "failed",
                "target": publication.target,
                "dataset_prefix": publication.dataset_prefix,
                "asset_code": asset_code,
                "job_name": job_name,
                "local_dataset_root": str(root),
                "publication": publication.to_mapping(),
                "definition_sha256": definition_sha256,
                "error": {
                    "code": getattr(exc, "code", "remote.publication_failed"),
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
                "updated_at": utc_now(),
            },
        )
        if publication.failure_policy == "warn_and_retry":
            return PublicationResult(
                status="pending",
                target=publication.target,
                dataset_uri=_dataset_uri(resolved_target, publication.dataset_prefix),
                generation_id="",
                manifest_key=None,
                receipt_path=receipt_path,
                uploaded_objects=0,
                reused_objects=0,
            )
        raise
    return result


def _publish(
    root: Path,
    *,
    publication: PublicationSpec,
    asset_code: str,
    job_name: str,
    definition_sha256: str,
    target: ObjectStoreTarget,
    store: ObjectStore,
    dataset_id: str,
    receipt_path: Path,
) -> PublicationResult:
    manifest_path = root / "_dataset.manifest.json"
    if not manifest_path.is_file():
        raise SmokingDataError(
            "Remote publication requires a committed dataset manifest.",
            code="remote.local_manifest_missing",
            context={"dataset_root": str(root)},
        )
    raw_local_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    local_manifest, detected_commit_kind = _normalize_local_manifest(
        root, raw_local_manifest, manifest_path=manifest_path
    )
    local_artifact_format = str(
        (local_manifest.get("context") or {}).get("artifact_format") or "parquet"
    )
    if local_artifact_format == "sbdf" and (
        publication.parquet.enabled or not publication.sbdf.enabled
    ):
        raise SmokingDataError(
            "An SBDF artifact can only publish the SBDF representation.",
            code="remote.sbdf_only_publication_required",
        )
    commit_kind = detected_commit_kind or "snapshot_replace"
    generation_id = str(local_manifest.get("generation_id") or local_manifest.get("transaction_id") or "")
    if not generation_id:
        raise SmokingDataError(
            "Committed dataset manifest has no generation identity.",
            code="remote.local_generation_missing",
        )
    parent_generation_id = local_manifest.get("parent_generation_id")
    local_manifest_sha256 = file_sha256(manifest_path)
    prefix = publication.dataset_prefix
    generation_prefix = target.object_key(f"{prefix}/generations/{generation_id}")
    pointer_key = target.object_key(f"{prefix}/catalog/latest.json")
    previous_pointer = store.head(pointer_key)
    if commit_kind == "append_generation" and previous_pointer is not None:
        previous_payload, _ = store.get(pointer_key)
        previous_document = json.loads(previous_payload)
        previous_generation = previous_document.get("generation_id")
        if previous_generation and previous_generation != generation_id:
            parent_generation_id = str(previous_generation)
        elif previous_generation == generation_id:
            # A retry of the same append must reproduce the original
            # immutable manifest, including its parent generation.
            existing_manifest_payload, _ = store.get(previous_document["manifest_key"])
            existing_manifest = json.loads(existing_manifest_payload)
            parent_generation_id = existing_manifest.get("parent_generation_id")

    with tempfile.TemporaryDirectory(prefix=".smoking-data-publication-", dir=root.parent) as temporary:
        snapshot = Path(temporary) / "snapshot"
        ensure_dir(snapshot)
        snapshot_files = _snapshot_committed_dataset(root, snapshot, local_manifest)
        if file_sha256(snapshot / "_dataset.manifest.json") != local_manifest_sha256:
            raise SmokingDataError(
                "Local dataset changed while publication snapshot was being created.",
                code="remote.local_snapshot_changed",
            )
        objects: list[RemoteObject] = []
        uploaded = 0
        reused = 0
        parquet_rows = 0
        parquet_files = 0
        for relative, local_path, role in snapshot_files:
            if role in {"parquet_data", "sbdf_data"} and (
                (role == "parquet_data" and not publication.parquet.enabled)
                or role == "sbdf_data"
            ):
                continue
            remote_relative = (
                f"data/{relative}" if role == "parquet_data" else f"provenance/{relative}"
            )
            key = f"{generation_prefix}/{remote_relative}"
            sha256 = file_sha256(local_path)
            existed = store.head(key) is not None
            metadata = store.put_immutable(key, local_path, sha256=sha256)
            reused += int(existed)
            uploaded += int(not existed)
            objects.append(
                RemoteObject(
                    role=role,
                    object_key=key,
                    size_bytes=local_path.stat().st_size,
                    sha256=sha256,
                    etag=metadata.etag,
                    version_id=metadata.version_id,
                    checksum_sha256=metadata.checksum_sha256,
                )
            )
            if role == "parquet_data":
                parquet_files += 1
                part = next(
                    (item for item in local_manifest.get("parts", []) if item.get("relative_path") == relative),
                    {},
                )
                parquet_rows += int(part.get("rows") or 0)

        built_index: BuiltIndex | None = None
        if publication.parquet.enabled and local_artifact_format == "parquet":
            built_index = build_parquet_indexes(
                snapshot,
                Path(temporary) / "indexes" / "parquet",
                generation_id=generation_id,
                generation_prefix=generation_prefix,
                parts=[item for item in local_manifest.get("parts", []) if isinstance(item, dict)],
                spec=publication.parquet,
                cache_root=receipt_path.parent.parent / "publication-cache" / "parquet",
            )
            for local_path in built_index.artifact_paths:
                relative = local_path.relative_to(built_index.root).as_posix()
                key = f"{generation_prefix}/indexes/parquet/{relative}"
                sha256 = file_sha256(local_path)
                existed = store.head(key) is not None
                metadata = store.put_immutable(key, local_path, sha256=sha256)
                reused += int(existed)
                uploaded += int(not existed)
                objects.append(
                    RemoteObject(
                        role="parquet_index",
                        object_key=key,
                        size_bytes=local_path.stat().st_size,
                        sha256=sha256,
                        etag=metadata.etag,
                        version_id=metadata.version_id,
                        checksum_sha256=metadata.checksum_sha256,
                    )
                )

        built_sbdf: BuiltSbdfRepresentation | None = None
        if publication.sbdf.enabled:
            sbdf_parts = [
                item for item in local_manifest.get("parts", []) if isinstance(item, dict)
            ]
            if local_artifact_format == "sbdf":
                built_sbdf = build_existing_sbdf_representation(
                    snapshot,
                    Path(temporary),
                    generation_id=generation_id,
                    generation_prefix=generation_prefix,
                    parts=sbdf_parts,
                    spec=publication.sbdf,
                )
            else:
                built_sbdf = build_sbdf_representation(
                    snapshot,
                    Path(temporary),
                    receipt_path.parent.parent / "publication-cache" / "sbdf",
                    generation_id=generation_id,
                    generation_prefix=generation_prefix,
                    parts=sbdf_parts,
                    spec=publication.sbdf,
                )
            for item in built_sbdf.objects:
                existed = store.head(item.object_key) is not None
                metadata = store.put_immutable(
                    item.object_key, item.local_path, sha256=item.sha256
                )
                reused += int(existed)
                uploaded += int(not existed)
                objects.append(
                    RemoteObject(
                        role="sbdf_data",
                        object_key=item.object_key,
                        size_bytes=item.local_path.stat().st_size,
                        sha256=item.sha256,
                        etag=metadata.etag,
                        version_id=metadata.version_id,
                        checksum_sha256=metadata.checksum_sha256,
                    )
                )
            for local_path in built_sbdf.index_paths:
                relative = local_path.relative_to(
                    Path(temporary) / "indexes" / "sbdf"
                ).as_posix()
                key = f"{generation_prefix}/indexes/sbdf/{relative}"
                sha256 = file_sha256(local_path)
                existed = store.head(key) is not None
                metadata = store.put_immutable(key, local_path, sha256=sha256)
                reused += int(existed)
                uploaded += int(not existed)
                objects.append(
                    RemoteObject(
                        role="sbdf_index",
                        object_key=key,
                        size_bytes=local_path.stat().st_size,
                        sha256=sha256,
                        etag=metadata.etag,
                        version_id=metadata.version_id,
                        checksum_sha256=metadata.checksum_sha256,
                    )
                )

        schema_fingerprint = payload_sha256(
            sorted({str(item.get("schema") or "") for item in local_manifest.get("parts", [])})
        )
        _verify_uploaded_objects(
            store,
            objects,
            generation_prefix=generation_prefix,
            verify_head=publication.verification.verify_remote_head,
            verify_references=publication.verification.verify_sidecar_references,
        )
        remote_manifest = build_remote_manifest(
            dataset_id=dataset_id,
            asset_code=asset_code,
            job_name=job_name,
            generation_id=generation_id,
            parent_generation_id=str(parent_generation_id) if parent_generation_id else None,
            local_manifest_sha256=local_manifest_sha256,
            definition_sha256=definition_sha256,
            schema_fingerprint=schema_fingerprint,
            objects=objects,
            target_identity=target.safe_identity(),
            representations={
                "parquet": {
                    "enabled": publication.parquet.enabled and local_artifact_format == "parquet",
                    "files": parquet_files,
                    "rows": parquet_rows,
                },
                "sbdf": {
                    "enabled": built_sbdf is not None,
                    "files": len(built_sbdf.objects) if built_sbdf else 0,
                    "rows": built_sbdf.rows if built_sbdf else 0,
                    "key_rows": built_sbdf.key_rows if built_sbdf else 0,
                    "schema_fingerprint": (
                        built_sbdf.schema_fingerprint if built_sbdf else None
                    ),
                    "key_columns": list(publication.sbdf.row_key_columns),
                    "key_types": {},
                    "hash_buckets": publication.sbdf.hash_buckets,
                    "capabilities": {
                        "sbdf_object_range_read": built_sbdf is not None,
                        "sbdf_key_read": built_sbdf is not None,
                    },
                },
            },
            sidecars={
                "parquet": {
                    "requested_level": publication.parquet.index_level,
                    "published": built_index is not None,
                    "schema_versions": {
                        "files": "smoking-data.remote-parquet-files.v1",
                        "row_groups": "smoking-data.remote-parquet-row-groups.v1",
                        "pages": "smoking-data.remote-parquet-pages.v1",
                        "keys": "smoking-data.remote-parquet-keys.v2",
                    },
                    "counts": {
                        "files": built_index.files if built_index else 0,
                        "row_groups": built_index.row_groups if built_index else 0,
                        "pages": built_index.pages if built_index else 0,
                        "key_rows": built_index.key_rows if built_index else 0,
                    },
                    "key_hash": publication.parquet.key_hash,
                    "key_columns": list(publication.parquet.key_columns),
                    "key_types": built_index.key_types if built_index else {},
                    "planning_columns": list(publication.parquet.planning_columns),
                    "planning_types": built_index.planning_types if built_index else {},
                    "hash_buckets": publication.parquet.hash_buckets,
                    "capabilities": {
                        "row_group": built_index is not None,
                        "page_index": bool(built_index and built_index.pages),
                        "key_index": bool(built_index and built_index.key_rows),
                        "object_range_read": built_index is not None,
                    },
                }
            },
            runtime_versions=_runtime_versions(),
            # The generation manifest is immutable.  Use the source part's
            # stable mtime instead of the temporary local manifest mtime,
            # which changes on every retry of the same publication.
            created_at=_stable_generation_created_at(snapshot_files, generation_id),
        )
        manifest_bytes = canonical_json(remote_manifest)
        remote_manifest_path = Path(temporary) / "manifest.json"
        remote_manifest_path.write_bytes(manifest_bytes)
        manifest_key = f"{generation_prefix}/manifest.json"
        manifest_meta = store.put_immutable(
            manifest_key,
            remote_manifest_path,
            sha256=file_sha256(remote_manifest_path),
        )
        if publication.verification.verify_remote_head:
            verified = store.head(manifest_key)
            if verified is None or verified.size_bytes != len(manifest_bytes):
                raise SmokingDataError(
                    "Remote generation manifest verification failed.",
                    code="remote.manifest_verification_failed",
                )
        pointer = build_pointer(
            generation_id=generation_id,
            manifest_key=manifest_key,
            manifest_sha256=hashlib_sha256(manifest_bytes),
        )
        pointer_bytes = canonical_json(pointer)
        if previous_pointer is not None:
            previous_payload, _ = store.get(pointer_key)
            previous_document = json.loads(previous_payload)
            if (
                previous_document.get("generation_id") != generation_id
                or previous_document.get("manifest_sha256") != pointer["manifest_sha256"]
            ):
                try:
                    store.put_conditional(
                        pointer_key,
                        pointer_bytes,
                        previous_etag=previous_pointer.etag,
                        create_only=False,
                    )
                except ConditionalWriteConflict as exc:
                    if commit_kind != "append_generation":
                        raise
                    current_payload, current_meta = store.get(pointer_key)
                    current = json.loads(current_payload)
                    if (
                        current.get("generation_id") == generation_id
                        and current.get("manifest_sha256") == pointer["manifest_sha256"]
                    ):
                        # Another retry already committed the same append run.
                        pass
                    else:
                        raise SmokingDataError(
                            "Concurrent append changed the remote catalog; retry the append publication.",
                            code="remote.append_pointer_cas_conflict",
                            context={
                                "generation_id": generation_id,
                                "current_generation_id": current.get("generation_id"),
                                "current_pointer_etag": current_meta.etag,
                            },
                        ) from exc
        else:
            store.put_conditional(
                pointer_key,
                pointer_bytes,
                previous_etag=None,
                create_only=True,
            )

    receipt = {
        "schema_version": PUBLICATION_RECEIPT_VERSION,
        "status": "committed",
        "target": publication.target,
        "dataset_prefix": publication.dataset_prefix,
        "dataset_uri": _dataset_uri(target, prefix),
        "asset_code": asset_code,
        "job_name": job_name,
        "local_dataset_root": str(root),
        "publication": publication.to_mapping(),
        "definition_sha256": definition_sha256,
        "generation_id": generation_id,
        "manifest_key": manifest_key,
        "manifest_etag": manifest_meta.etag,
        "uploaded_objects": uploaded + 1,
        "reused_objects": reused,
        "credential_resolution": target.credentials.metadata(),
        "updated_at": utc_now(),
    }
    _write_receipt(receipt_path, receipt)
    return PublicationResult(
        status="committed",
        target=publication.target,
        dataset_uri=_dataset_uri(target, prefix),
        generation_id=generation_id,
        manifest_key=manifest_key,
        receipt_path=receipt_path,
        uploaded_objects=uploaded + 1,
        reused_objects=reused,
    )


def _snapshot_committed_dataset(
    source: Path,
    destination: Path,
    manifest: dict[str, Any],
) -> list[tuple[str, Path, str]]:
    selected: list[tuple[str, Path, str]] = []
    entries: list[tuple[str, str]] = [
        (
            str(item["relative_path"]),
            "sbdf_data" if str(item.get("format") or "") == "sbdf" else "parquet_data",
        )
        for item in manifest.get("parts", [])
        if isinstance(item, dict) and item.get("relative_path")
    ]
    entries.extend(
        (str(item["relative_path"]), "provenance")
        for item in manifest.get("provenance", [])
        if isinstance(item, dict) and item.get("relative_path")
    )
    entries.append(("_dataset.manifest.json", "provenance"))
    change_receipt = manifest.get("change_receipt")
    if change_receipt:
        entries.append((str(change_receipt), "provenance"))
    seen: set[str] = set()
    for relative, role in entries:
        if relative in seen:
            continue
        seen.add(relative)
        relative_path = Path(relative)
        local = (source / relative_path).resolve()
        if (
            relative_path.is_absolute()
            or ".." in relative_path.parts
            or not local.is_relative_to(source)
            or not local.is_file()
        ):
            raise SmokingDataError(
                "Committed manifest references a missing or unsafe local object.",
                code="remote.local_reference_invalid",
                context={"relative_path": relative},
            )
        target = destination / relative
        ensure_dir(target.parent)
        try:
            os.link(local, target)
        except OSError:
            shutil.copy2(local, target)
        selected.append((relative, target, role))
    return selected


def _write_receipt(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _runtime_versions() -> dict[str, str]:
    result: dict[str, str] = {}
    for package in ("smoking-data", "smoking-sbdf", "pyarrow"):
        try:
            result[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            result[package] = "development"
    return result


def _dataset_uri(target: ObjectStoreTarget, prefix: str) -> str:
    key = target.object_key(prefix)
    return f"s3://{target.bucket}/{key}"


def _stable_generation_created_at(
    snapshot_files: list[tuple[str, Path, str]], generation_id: str
) -> str:
    data_mtimes = [
        path.stat().st_mtime
        for _, path, role in snapshot_files
        if role in {"parquet_data", "sbdf_data"}
    ]
    if not data_mtimes:
        return generation_id
    return datetime.fromtimestamp(min(data_mtimes), timezone.utc).isoformat()


def hashlib_sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _verify_uploaded_objects(
    store: ObjectStore,
    objects: list[RemoteObject],
    *,
    generation_prefix: str,
    verify_head: bool,
    verify_references: bool,
) -> None:
    keys = [item.object_key for item in objects]
    if verify_references:
        expected_prefix = generation_prefix + "/"
        if len(keys) != len(set(keys)) or any(
            not key.startswith(expected_prefix) for key in keys
        ):
            raise SmokingDataError(
                "Remote bundle contains a duplicate or out-of-generation reference.",
                code="remote.manifest_reference_invalid",
            )
    if not verify_head:
        return
    for item in objects:
        metadata = store.head(item.object_key)
        if (
            metadata is None
            or metadata.size_bytes != item.size_bytes
            or (
                metadata.checksum_sha256 is not None
                and metadata.checksum_sha256 != item.sha256
            )
        ):
            raise SmokingDataError(
                "Remote immutable object verification failed.",
                code="remote.object_verification_failed",
                context={"object_key": item.object_key, "role": item.role},
            )


def _normalize_local_manifest(
    root: Path,
    manifest: dict[str, Any],
    *,
    manifest_path: Path,
) -> tuple[dict[str, Any], str | None]:
    if manifest.get("version") == "smoking-data.dataset-manifest.v1":
        return manifest, None
    if manifest.get("schema_version") != "smoking-data.calculated-fact-manifest.v1":
        return manifest, None
    parts: list[dict[str, Any]] = []
    for generation in manifest.get("generations") or []:
        if not isinstance(generation, dict):
            continue
        for item in generation.get("files") or []:
            if not isinstance(item, dict) or not item.get("path"):
                continue
            relative = str(item["path"])
            path = (root / relative).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise SmokingDataError(
                    "Calculated FACT manifest references a missing part.",
                    code="remote.append_local_reference_invalid",
                    context={"relative_path": relative},
                )
            parquet = pq.ParquetFile(path)
            parts.append(
                {
                    "relative_path": relative,
                    "rows": int(item.get("rows") or parquet.metadata.num_rows),
                    "size_bytes": path.stat().st_size,
                    "sha256": str(item.get("sha256") or file_sha256(path)),
                    "schema": str(parquet.schema_arrow),
                }
            )
    active_seq = int(manifest.get("active_generation_seq") or 0)
    identity = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    return (
        {
            "version": "smoking-data.dataset-manifest.v1",
            "generation_id": f"append-{active_seq:020d}-{identity[:24]}",
            "parent_generation_id": None,
            "rows": sum(int(item["rows"]) for item in parts),
            "parts": parts,
            "provenance": [],
            "context": {
                "asset_code": "0102",
                "active_generation_seq": active_seq,
                "append_run_key": next(
                    (
                        str(item.get("run_key"))
                        for item in reversed(manifest.get("generations") or [])
                        if isinstance(item, dict)
                        and int(item.get("generation_seq") or 0) == active_seq
                    ),
                    "",
                ),
            },
        },
        "append_generation",
    )
