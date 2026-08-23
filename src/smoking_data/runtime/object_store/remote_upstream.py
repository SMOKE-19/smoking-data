from __future__ import annotations

from pathlib import Path

from smoking_data.core.exceptions import SmokingDataError
from smoking_data.core.types import DatasetFile
from smoking_data.runtime.paths import ensure_dir, file_sha256

from .backend import S3ObjectStore
from .config import validate_relative_prefix
from .remote_reader import open_remote_generation


def materialize_remote_parquet_files(
    project_root: str | Path,
    *,
    target_name: str,
    dataset_prefix: str,
    relative_paths: list[str] | tuple[str, ...] | None = None,
    recursive: bool = True,
) -> list[DatasetFile]:
    """Resolve a pinned S3 generation into reusable local Parquet paths.

    The generation pointer and manifest are read once. Objects are downloaded
    through the shared bounded range/multipart backend and stored under a
    generation-addressed cache, so an ETL retry cannot mix generations.
    """
    handle = open_remote_generation(
        project_root,
        target_name=target_name,
        dataset_prefix=dataset_prefix,
    )
    target = handle.target
    store = S3ObjectStore(target)
    store.preflight()
    requested = tuple(
        validate_relative_prefix(value, path="source.upstream.remote.relative_paths")
        for value in (relative_paths or ())
    )
    generation_marker = f"/generations/{handle.generation_id}/data/"
    cache_root = (
        Path(project_root).expanduser().resolve()
        / ".smoking-data"
        / "cache"
        / "remote-upstream"
        / target_name
        / handle.generation_id
    )
    result: list[DatasetFile] = []
    for item in handle.manifest.get("objects") or []:
        if not isinstance(item, dict) or item.get("role") != "parquet_data":
            continue
        object_key = str(item.get("object_key") or "")
        if generation_marker not in object_key:
            raise SmokingDataError(
                "Remote manifest Parquet object is outside the pinned generation.",
                code="remote.manifest_reference_invalid",
            )
        relative = object_key.split(generation_marker, 1)[1]
        if requested and not _matches_relative_path(relative, requested, recursive=recursive):
            continue
        destination = cache_root / relative
        expected_size = int(item.get("size_bytes") or 0)
        expected_sha = str(item.get("sha256") or "")
        if not destination.is_file() or destination.stat().st_size != expected_size:
            ensure_dir(destination.parent)
            store.download_to_path(object_key, destination)
        if expected_sha and file_sha256(destination) != expected_sha:
            destination.unlink(missing_ok=True)
            raise SmokingDataError(
                "Materialized remote Parquet checksum differs from the pinned manifest.",
                code="remote.upstream_checksum_mismatch",
                context={"relative_path": relative, "generation_id": handle.generation_id},
            )
        stat = destination.stat()
        result.append(
            DatasetFile(
                path=destination,
                size_bytes=stat.st_size,
                modified_ns=stat.st_mtime_ns,
            )
        )
    result.sort(key=lambda item: str(item.path))
    if not result:
        raise SmokingDataError(
            "Pinned remote generation has no matching Parquet upstream files.",
            code="remote.upstream_empty",
            context={"dataset_prefix": dataset_prefix, "generation_id": handle.generation_id},
        )
    return result


def _matches_relative_path(relative: str, requested: tuple[str, ...], *, recursive: bool) -> bool:
    for value in requested:
        if relative == value:
            return True
        if recursive and relative.startswith(f"{value}/"):
            return True
    return False
