from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any, Mapping, Sequence

from smoking_data.runtime.paths import ensure_dir, reset_path
from smoking_data.runtime.selector_ipc import ipc_file_is_valid

CACHE_VERSION = "smoking-data.0201-cohort-partition-cache.v1"
GLOBAL_CACHE_VERSION = "smoking-data.0201-global-winner-cache.v1"


def cohort_partition_cache_key(
    cohort: Mapping[str, Any],
    *,
    partition_key: str,
    selection_group_keys: Sequence[str],
    sort: Sequence[Mapping[str, Any]],
) -> str:
    sources = []
    for item in cohort.get("slices") or []:
        for source in item.get("sources") or []:
            sources.append(
                {
                    "dataset_shard_id": str(source.get("dataset_shard_id") or ""),
                    "relative_path": str(source.get("relative_path") or source.get("path") or ""),
                    "fingerprint": str(source.get("fingerprint") or ""),
                    "logical_plan_hash": str(source.get("logical_plan_hash") or ""),
                    "selector_contract_hash": str(source.get("selector_contract_hash") or ""),
                }
            )
    payload = {
        "schema_version": CACHE_VERSION,
        "partition_key": partition_key,
        "selection_group_keys": list(selection_group_keys),
        "sort": [dict(item) for item in sort],
        "sources": sorted(
            sources,
            key=lambda item: (
                item["dataset_shard_id"],
                item["relative_path"],
            ),
        ),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def load_cohort_partition_cache(
    root: Path, cache_key: str
) -> tuple[Path, dict[str, Any]] | None:
    entry = root / cache_key[:2] / cache_key
    manifest_path = entry / "manifest.json"
    ipc_path = entry / "local-winner.arrow"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        manifest.get("schema_version") != CACHE_VERSION
        or manifest.get("cache_key") != cache_key
        or manifest.get("ipc_path") != ipc_path.name
        or not ipc_file_is_valid(ipc_path)
        or _file_sha256(ipc_path) != str(manifest.get("ipc_sha256") or "")
    ):
        return None
    return ipc_path, manifest


def publish_cohort_partition_cache(
    root: Path,
    cache_key: str,
    *,
    ipc_path: Path,
    local_manifest: Mapping[str, Any],
) -> tuple[Path, dict[str, Any]]:
    existing = load_cohort_partition_cache(root, cache_key)
    if existing is not None:
        return existing
    parent = ensure_dir(root / cache_key[:2])
    entry = parent / cache_key
    temporary = parent / f".{cache_key}.{os.getpid()}.tmp"
    reset_path(temporary)
    ensure_dir(temporary)
    destination = temporary / "local-winner.arrow"
    try:
        shutil.copy2(ipc_path, destination)
        manifest = {
            **dict(local_manifest),
            "schema_version": CACHE_VERSION,
            "cache_key": cache_key,
            "ipc_path": destination.name,
            "ipc_sha256": _file_sha256(destination),
            "bytes": destination.stat().st_size,
        }
        manifest_path = temporary / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=True, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        if entry.exists():
            reset_path(entry)
        os.replace(temporary, entry)
    finally:
        reset_path(temporary)
    loaded = load_cohort_partition_cache(root, cache_key)
    if loaded is None:
        raise RuntimeError("0201 cohort-local cache failed post-write validation.")
    return loaded


def prune_cohort_partition_cache(
    root: Path, *, protected_keys: set[str], max_entries: int = 512
) -> int:
    entries = [
        path
        for bucket in root.iterdir()
        if bucket.is_dir()
        for path in bucket.iterdir()
        if path.is_dir() and len(path.name) == 64
    ] if root.is_dir() else []
    removable = sorted(
        (path for path in entries if path.name not in protected_keys),
        key=lambda path: path.stat().st_mtime_ns,
    )
    remove_count = max(0, len(entries) - max(1, int(max_entries)))
    for path in removable[:remove_count]:
        reset_path(path)
    return min(remove_count, len(removable))


def global_winner_cache_key(
    *,
    partition_key: str,
    bundle_id: str,
    selection_group_keys: Sequence[str],
    sort: Sequence[Mapping[str, Any]],
    routes: Sequence[Mapping[str, Any]],
) -> str:
    payload = {
        "schema_version": GLOBAL_CACHE_VERSION,
        "partition_key": partition_key,
        "bundle_id": bundle_id,
        "selection_group_keys": list(selection_group_keys),
        "sort": [dict(item) for item in sort],
        "routes": sorted(
            [
                {
                    "local_cache_key": str(item.get("local_cache_key") or ""),
                    "batch_indices": [int(value) for value in item.get("batch_indices") or []],
                }
                for item in routes
            ],
            key=lambda item: (item["local_cache_key"], item["batch_indices"]),
        ),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def load_global_winner_cache(root: Path, cache_key: str) -> tuple[Path, dict[str, Any]] | None:
    return _load_ipc_cache_entry(
        root,
        cache_key,
        schema_version=GLOBAL_CACHE_VERSION,
        ipc_name="winner.arrow",
    )


def publish_global_winner_cache(
    root: Path,
    cache_key: str,
    *,
    ipc_path: Path,
    metadata: Mapping[str, Any],
) -> tuple[Path, dict[str, Any]]:
    existing = load_global_winner_cache(root, cache_key)
    if existing is not None:
        return existing
    parent = ensure_dir(root / cache_key[:2])
    entry = parent / cache_key
    temporary = parent / f".{cache_key}.{os.getpid()}.tmp"
    reset_path(temporary)
    ensure_dir(temporary)
    destination = temporary / "winner.arrow"
    try:
        shutil.copy2(ipc_path, destination)
        manifest = {
            **dict(metadata),
            "schema_version": GLOBAL_CACHE_VERSION,
            "cache_key": cache_key,
            "ipc_path": destination.name,
            "ipc_sha256": _file_sha256(destination),
            "bytes": destination.stat().st_size,
        }
        (temporary / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=True, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        if entry.exists():
            reset_path(entry)
        os.replace(temporary, entry)
    finally:
        reset_path(temporary)
    loaded = load_global_winner_cache(root, cache_key)
    if loaded is None:
        raise RuntimeError("0201 global-winner cache failed post-write validation.")
    return loaded


def prune_global_winner_cache(
    root: Path, *, protected_keys: set[str], max_entries: int = 512
) -> int:
    return prune_cohort_partition_cache(
        root,
        protected_keys=protected_keys,
        max_entries=max_entries,
    )


def _load_ipc_cache_entry(
    root: Path,
    cache_key: str,
    *,
    schema_version: str,
    ipc_name: str,
) -> tuple[Path, dict[str, Any]] | None:
    entry = root / cache_key[:2] / cache_key
    manifest_path = entry / "manifest.json"
    ipc_path = entry / ipc_name
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        manifest.get("schema_version") != schema_version
        or manifest.get("cache_key") != cache_key
        or manifest.get("ipc_path") != ipc_path.name
        or not ipc_file_is_valid(ipc_path)
        or _file_sha256(ipc_path) != str(manifest.get("ipc_sha256") or "")
    ):
        return None
    return ipc_path, manifest


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
