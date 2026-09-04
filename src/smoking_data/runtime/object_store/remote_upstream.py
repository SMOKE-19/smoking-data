from __future__ import annotations

import hashlib
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from smoking_data.core.exceptions import SmokingDataError
from smoking_data.core.types import DatasetFile
from smoking_data.runtime.paths import ensure_dir, file_sha256

from .backend import S3ObjectStore
from .config import validate_relative_prefix
from .remote_reader import (
    RemoteGenerationHandle,
    open_remote_generation,
    read_remote_parquet_file_index,
    read_remote_parquet_planning_index,
    read_remote_parquet_row_group_index,
    read_remote_parquet_to_ipc,
    remote_parquet_objects,
)


@dataclass(slots=True)
class RemoteSelectorContext:
    handle: RemoteGenerationHandle
    planning_table: pa.Table
    objects_by_file_id: dict[str, dict[str, Any]]
    proxy_path_by_file_id: dict[str, Path]
    file_id_by_proxy_path: dict[str, str]
    profile: dict[str, Any]
    payload_object_ids: set[str] = field(default_factory=set)


def materialize_remote_parquet_files(
    project_root: str | Path,
    *,
    target_name: str,
    dataset_prefix: str,
    relative_paths: list[str] | tuple[str, ...] | None = None,
    recursive: bool = True,
    transfer_profile: dict[str, object] | None = None,
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
    pinned_dataset_prefix = str(getattr(handle, "dataset_prefix", "") or dataset_prefix)
    dataset_cache_id = hashlib.sha256(pinned_dataset_prefix.encode("utf-8")).hexdigest()[:20]
    cache_root = (
        Path(project_root).expanduser().resolve()
        / ".smoking-data"
        / "cache"
        / "remote-upstream"
        / target_name
        / dataset_cache_id
        / handle.generation_id
    )
    object_cache_root = cache_root.parent / "objects"
    result: list[DatasetFile] = []
    downloaded_objects = 0
    downloaded_bytes = 0
    reused_objects = 0
    reused_bytes = 0
    for item in remote_parquet_objects(handle):
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
        object_cache_path = object_cache_root / expected_sha[:2] / expected_sha
        object_was_cached = _valid_cached_object(
            object_cache_path, size=expected_size, sha256=expected_sha
        )
        if not object_was_cached:
            ensure_dir(object_cache_path.parent)
            temporary = object_cache_path.with_name(
                f".{object_cache_path.name}.{os.getpid()}.tmp"
            )
            temporary.unlink(missing_ok=True)
            try:
                store.download_to_path(object_key, temporary)
                if not _valid_cached_object(temporary, size=expected_size, sha256=expected_sha):
                    raise SmokingDataError(
                        "Materialized remote Parquet checksum differs from the pinned manifest.",
                        code="remote.upstream_checksum_mismatch",
                        context={
                            "relative_path": relative,
                            "generation_id": handle.generation_id,
                        },
                    )
                os.replace(temporary, object_cache_path)
            finally:
                temporary.unlink(missing_ok=True)
            downloaded_objects += 1
            downloaded_bytes += expected_size
        else:
            reused_objects += 1
            reused_bytes += expected_size
        if not _valid_cached_object(destination, size=expected_size, sha256=expected_sha):
            ensure_dir(destination.parent)
            destination.unlink(missing_ok=True)
            try:
                os.link(object_cache_path, destination)
            except OSError:
                shutil.copy2(object_cache_path, destination)
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
                dataset_root=cache_root,
                dataset_id=str(item["dataset_id"]),
                source_kind="s3",
                file_id=str(item["file_id"]),
                relative_path=relative,
                content_sha256=str(item.get("content_sha256") or ""),
                object_key=object_key,
            )
        )
    result.sort(key=lambda item: str(item.path))
    if not result:
        raise SmokingDataError(
            "Pinned remote generation has no matching Parquet upstream files.",
            code="remote.upstream_empty",
            context={"dataset_prefix": dataset_prefix, "generation_id": handle.generation_id},
        )
    if transfer_profile is not None:
        transfer_profile.update(
            {
                "mode": "content_addressed_whole_object",
                "generation_id": handle.generation_id,
                "manifest_sha256": str(
                    getattr(handle, "manifest_sha256", "")
                    or getattr(handle, "pointer", {}).get("manifest_sha256")
                    or ""
                ),
                "candidate_objects_touched": len(result),
                "payload_objects_touched": len(result),
                "downloaded_objects": downloaded_objects,
                "reused_objects": reused_objects,
                "bytes_fetched": downloaded_bytes,
                "bytes_reused": reused_bytes,
                "estimated_bytes_avoided": reused_bytes,
                "fallback_reason": (
                    "whole_object_materialization_required" if downloaded_objects else None
                ),
            }
        )
    return result


def _valid_cached_object(path: Path, *, size: int, sha256: str) -> bool:
    if not path.is_file() or path.stat().st_size != size:
        return False
    return not sha256 or file_sha256(path) == sha256


def materialize_remote_selector_proxies(
    project_root: str | Path,
    *,
    target_name: str,
    dataset_prefix: str,
    required_columns: list[str],
    relative_paths: list[str] | tuple[str, ...] | None = None,
    recursive: bool = True,
) -> tuple[list[DatasetFile], RemoteSelectorContext]:
    """Materialize a complete published selector index as thin local Parquet proxies."""
    handle = open_remote_generation(
        project_root,
        target_name=target_name,
        dataset_prefix=dataset_prefix,
    )
    requested = tuple(
        validate_relative_prefix(value, path="source.upstream.remote.relative_paths")
        for value in (relative_paths or ())
    )
    objects = [
        item
        for item in remote_parquet_objects(handle)
        if not requested
        or _matches_relative_path(str(item["relative_path"]), requested, recursive=recursive)
    ]
    selected_file_ids = {str(item["file_id"]) for item in objects}
    planning, planning_profile = read_remote_parquet_planning_index(
        handle,
        required_columns=required_columns,
        file_ids=selected_file_ids,
    )
    file_index, file_profile = read_remote_parquet_file_index(handle)
    file_rows = {
        str(row["file_id"]): int(row["rows"])
        for row in file_index.to_pylist()
        if str(row.get("file_id") or "") in selected_file_ids
    }
    root = Path(project_root).expanduser().resolve()
    dataset_cache_id = hashlib.sha256(handle.dataset_prefix.encode("utf-8")).hexdigest()[:20]
    contract_hash = hashlib.sha256(
        "\0".join(required_columns).encode("utf-8")
    ).hexdigest()[:20]
    proxy_root = (
        root
        / ".smoking-data"
        / "cache"
        / "remote-selector"
        / target_name
        / dataset_cache_id
        / "objects"
        / contract_hash
    )
    proxy_path_by_file_id: dict[str, Path] = {}
    files: list[DatasetFile] = []
    for item in objects:
        file_id = str(item["file_id"])
        rows = planning.filter(pc.equal(planning["file_id"], file_id)).sort_by(
            [("source_row_index", "ascending")]
        )
        expected_rows = file_rows.get(file_id)
        indexes = rows["source_row_index"].to_pylist() if rows.num_rows else []
        if expected_rows is None or rows.num_rows != expected_rows or indexes != list(
            range(expected_rows)
        ):
            raise SmokingDataError(
                "Published Parquet planning index is not a complete row index.",
                code="remote.planning_index_incomplete",
                context={
                    "file_id": file_id,
                    "expected_rows": expected_rows,
                    "indexed_rows": rows.num_rows,
                },
            )
        proxy_path = proxy_root / f"{file_id}.parquet"
        if not proxy_path.is_file():
            ensure_dir(proxy_path.parent)
            temporary = proxy_path.with_name(f".{proxy_path.name}.{os.getpid()}.tmp")
            temporary.unlink(missing_ok=True)
            try:
                pq.write_table(rows.select(required_columns), temporary, compression="zstd")
                os.replace(temporary, proxy_path)
            finally:
                temporary.unlink(missing_ok=True)
        stat = proxy_path.stat()
        proxy_path_by_file_id[file_id] = proxy_path
        files.append(
            DatasetFile(
                path=proxy_path,
                size_bytes=stat.st_size,
                modified_ns=stat.st_mtime_ns,
                dataset_root=proxy_root,
                dataset_id=str(item["dataset_id"]),
                source_kind="s3_selector_proxy",
                file_id=file_id,
                relative_path=str(item["relative_path"]),
                content_sha256=str(item["content_sha256"]),
                object_key=str(item["object_key"]),
            )
        )
    if not files:
        raise SmokingDataError(
            "Pinned remote generation has no matching Parquet upstream files.",
            code="remote.upstream_empty",
        )
    profile = {
        "mode": "published_planning_index",
        "generation_id": handle.generation_id,
        "manifest_sha256": handle.manifest_sha256,
        "index_bytes": int(planning_profile["index_bytes"]) + int(file_profile["index_bytes"]),
        "candidate_objects_touched": 0,
        "payload_objects_touched": 0,
        "row_groups_touched": 0,
        "range_count": 0,
        "requested_range_bytes": 0,
        "bytes_fetched": 0,
        "estimated_bytes_avoided": sum(int(item.get("size_bytes") or 0) for item in objects),
        "fallback_reason": None,
    }
    return sorted(files, key=lambda item: str(item.path)), RemoteSelectorContext(
        handle=handle,
        planning_table=planning,
        objects_by_file_id={str(item["file_id"]): item for item in objects},
        proxy_path_by_file_id=proxy_path_by_file_id,
        file_id_by_proxy_path={str(path.resolve()): file_id for file_id, path in proxy_path_by_file_id.items()},
        profile=profile,
    )


def materialize_remote_projected_selector_proxies(
    project_root: str | Path,
    *,
    target_name: str,
    dataset_prefix: str,
    required_columns: list[str],
    relative_paths: list[str] | tuple[str, ...] | None = None,
    recursive: bool = True,
) -> tuple[list[DatasetFile], RemoteSelectorContext]:
    """Range-read only selector columns when a complete planning index is unavailable."""
    handle = open_remote_generation(
        project_root,
        target_name=target_name,
        dataset_prefix=dataset_prefix,
    )
    requested = tuple(
        validate_relative_prefix(value, path="source.upstream.remote.relative_paths")
        for value in (relative_paths or ())
    )
    objects = [
        item
        for item in remote_parquet_objects(handle)
        if not requested
        or _matches_relative_path(str(item["relative_path"]), requested, recursive=recursive)
    ]
    if not objects:
        raise SmokingDataError(
            "Pinned remote generation has no matching Parquet upstream files.",
            code="remote.upstream_empty",
        )
    selected_file_ids = {str(item["file_id"]) for item in objects}
    row_groups, row_group_profile = read_remote_parquet_row_group_index(
        handle,
        file_ids=selected_file_ids,
    )
    root = Path(project_root).expanduser().resolve()
    dataset_cache_id = hashlib.sha256(handle.dataset_prefix.encode("utf-8")).hexdigest()[:20]
    contract_hash = hashlib.sha256(
        "\0".join(required_columns).encode("utf-8")
    ).hexdigest()[:20]
    proxy_root = (
        root
        / ".smoking-data"
        / "cache"
        / "remote-selector-projected"
        / target_name
        / dataset_cache_id
        / "objects"
        / contract_hash
    )
    files: list[DatasetFile] = []
    proxy_path_by_file_id: dict[str, Path] = {}
    coordinate_tables: list[pa.Table] = []
    fetched_object_ids: set[str] = set()
    profile: dict[str, Any] = {
        "mode": "projected_row_group_range",
        "generation_id": handle.generation_id,
        "manifest_sha256": handle.manifest_sha256,
        "index_bytes": int(row_group_profile["index_bytes"]),
        "candidate_objects_touched": 0,
        "candidate_row_groups_touched": 0,
        "payload_objects_touched": 0,
        "row_groups_touched": 0,
        "range_count": 0,
        "requested_range_bytes": 0,
        "bytes_fetched": 0,
        "estimated_bytes_avoided": sum(int(item.get("size_bytes") or 0) for item in objects),
        "fallback_reason": "published_planning_index_incomplete",
    }
    for item in objects:
        file_id = str(item["file_id"])
        relative_path = str(item["relative_path"])
        physical_rows = (
            row_groups.filter(pc.equal(row_groups["file_id"], file_id))
            .to_pylist()
        )
        unique_groups: dict[int, tuple[int, int]] = {}
        for row in physical_rows:
            row_group_id = int(row["row_group_id"])
            identity = (int(row["first_row_index"]), int(row["row_count"]))
            previous = unique_groups.setdefault(row_group_id, identity)
            if previous != identity:
                raise SmokingDataError(
                    "Published Parquet row-group coordinates are inconsistent.",
                    code="remote.row_group_index_incomplete",
                    context={"file_id": file_id, "row_group_id": row_group_id},
                )
        ordered_groups = sorted(unique_groups.items())
        if not ordered_groups or [group_id for group_id, _ in ordered_groups] != list(
            range(len(ordered_groups))
        ):
            raise SmokingDataError(
                "Published Parquet row-group index does not cover the complete file.",
                code="remote.row_group_index_incomplete",
                context={"file_id": file_id},
            )
        for row_group_id, (first_row_index, row_count) in ordered_groups:
            coordinate_tables.append(
                pa.table(
                    {
                        "file_id": [file_id] * row_count,
                        "row_group_id": pa.array([row_group_id] * row_count, type=pa.int64()),
                        "row_offset_in_group": pa.array(range(row_count), type=pa.int64()),
                        "source_row_index": pa.array(
                            range(first_row_index, first_row_index + row_count), type=pa.int64()
                        ),
                    }
                )
            )
        expected_rows = sum(row_count for _, (_, row_count) in ordered_groups)
        proxy_path = proxy_root / f"{file_id}.parquet"
        reusable = False
        if proxy_path.is_file():
            try:
                parquet = pq.ParquetFile(proxy_path)
                reusable = (
                    parquet.metadata.num_rows == expected_rows
                    and set(required_columns).issubset(parquet.schema_arrow.names)
                )
            except (OSError, pa.ArrowException):
                reusable = False
        if not reusable:
            ensure_dir(proxy_path.parent)
            temporary = proxy_path.with_name(f".{proxy_path.name}.{os.getpid()}.tmp")
            temporary.unlink(missing_ok=True)
            writer: pq.ParquetWriter | None = None
            try:
                for piece_index, (row_group_id, (_, expected_group_rows)) in enumerate(
                    ordered_groups
                ):
                    piece = proxy_root / f".{file_id}.{piece_index:06d}.arrow.tmp"
                    piece.unlink(missing_ok=True)
                    try:
                        try:
                            stats = read_remote_parquet_to_ipc(
                                handle,
                                relative_path=relative_path,
                                output_ipc_path=piece,
                                projection=required_columns,
                                row_groups=[row_group_id],
                            )
                        except SmokingDataError:
                            raise
                        except Exception as exc:
                            raise SmokingDataError(
                                "Projected selector range read failed.",
                                code="remote.projected_selector_range_read_failed",
                                context={"file_id": file_id, "row_group_id": row_group_id},
                            ) from exc
                        with piece.open("rb") as source:
                            table = ipc.open_file(source).read_all()
                    finally:
                        piece.unlink(missing_ok=True)
                    if table.num_rows != expected_group_rows:
                        raise SmokingDataError(
                            "Projected selector row count differs from the row-group index.",
                            code="remote.projected_selector_row_mismatch",
                            context={"file_id": file_id, "row_group_id": row_group_id},
                        )
                    if set(required_columns) - set(table.column_names):
                        raise SmokingDataError(
                            "Projected selector columns are absent from the remote Parquet part.",
                            code="remote.projected_selector_columns_missing",
                            context={"file_id": file_id},
                        )
                    projected = table.select(required_columns)
                    if writer is None:
                        writer = pq.ParquetWriter(temporary, projected.schema, compression="zstd")
                    writer.write_table(projected, row_group_size=65_536)
                    fetched_object_ids.add(file_id)
                    profile["candidate_row_groups_touched"] += 1
                    profile["range_count"] += int(stats.get("range_count") or 0)
                    profile["requested_range_bytes"] += int(
                        stats.get("requested_range_bytes") or 0
                    )
                    profile["bytes_fetched"] += int(stats.get("received_range_bytes") or 0)
                if writer is None:
                    raise SmokingDataError(
                        "Projected selector produced no rows.",
                        code="remote.projected_selector_empty",
                        context={"file_id": file_id},
                    )
                writer.close()
                writer = None
                os.replace(temporary, proxy_path)
            finally:
                if writer is not None:
                    writer.close()
                temporary.unlink(missing_ok=True)
        stat = proxy_path.stat()
        proxy_path_by_file_id[file_id] = proxy_path
        files.append(
            DatasetFile(
                path=proxy_path,
                size_bytes=stat.st_size,
                modified_ns=stat.st_mtime_ns,
                dataset_root=proxy_root,
                dataset_id=str(item["dataset_id"]),
                source_kind="s3_selector_proxy",
                file_id=file_id,
                relative_path=relative_path,
                content_sha256=str(item["content_sha256"]),
                object_key=str(item["object_key"]),
            )
        )
    profile["candidate_objects_touched"] = len(fetched_object_ids)
    planning_table = pa.concat_tables(coordinate_tables) if coordinate_tables else pa.table({})
    return sorted(files, key=lambda file: str(file.path)), RemoteSelectorContext(
        handle=handle,
        planning_table=planning_table,
        objects_by_file_id={str(item["file_id"]): item for item in objects},
        proxy_path_by_file_id=proxy_path_by_file_id,
        file_id_by_proxy_path={
            str(path.resolve()): file_id for file_id, path in proxy_path_by_file_id.items()
        },
        profile=profile,
    )


def materialize_remote_active_payload(
    context: RemoteSelectorContext,
    coordinates: "Any",
    *,
    output_root: Path,
    source_file_column: str,
    source_row_group_column: str,
    source_row_index_column: str,
) -> tuple["Any", list[DatasetFile]]:
    """Range-read active remote rows and remap coordinates to compact local Parquet parts."""
    import polars as pl

    if coordinates.is_empty():
        return coordinates, []
    ensure_dir(output_root)
    updated = coordinates
    materialized: list[DatasetFile] = []
    for source_text in coordinates.get_column(source_file_column).unique().to_list():
        resolved = str(Path(str(source_text)).resolve())
        file_id = context.file_id_by_proxy_path.get(resolved)
        if file_id is None:
            existing = Path(resolved)
            if existing.is_file():
                existing_file_id = existing.stem
                obj = context.objects_by_file_id.get(existing_file_id, {})
                stat = existing.stat()
                materialized.append(
                    DatasetFile(
                        path=existing,
                        size_bytes=stat.st_size,
                        modified_ns=stat.st_mtime_ns,
                        dataset_root=existing.parent,
                        dataset_id=context.handle.dataset_id,
                        source_kind="s3_active_payload",
                        file_id=existing_file_id,
                        relative_path=str(obj.get("relative_path") or existing.name),
                        content_sha256=str(obj.get("content_sha256") or ""),
                        object_key=str(obj.get("object_key") or ""),
                    )
                )
            continue
        selected_indexes = sorted(
            int(value)
            for value in coordinates.filter(pl.col(source_file_column) == source_text)
            .get_column(source_row_index_column)
            .unique()
            .to_list()
        )
        index_rows = context.planning_table.filter(
            pc.and_(
                pc.equal(context.planning_table["file_id"], file_id),
                pc.is_in(
                    context.planning_table["source_row_index"],
                    value_set=pa.array(selected_indexes, type=pa.int64()),
                ),
            )
        ).sort_by([("row_group_id", "ascending"), ("row_offset_in_group", "ascending")])
        if index_rows.num_rows != len(selected_indexes):
            raise SmokingDataError(
                "Active coordinate is absent from the pinned planning index.",
                code="remote.active_coordinate_missing",
                context={"file_id": file_id},
            )
        payload_path = output_root / f"{file_id}.parquet"
        temporary = payload_path.with_name(f".{payload_path.name}.{os.getpid()}.tmp")
        temporary.unlink(missing_ok=True)
        writer: pq.ParquetWriter | None = None
        output_position_by_source_index: dict[int, int] = {}
        output_position = 0
        try:
            rows_by_group: dict[int, list[dict[str, Any]]] = {}
            for row in index_rows.to_pylist():
                rows_by_group.setdefault(int(row["row_group_id"]), []).append(row)
            for piece_index, (row_group_id, rows) in enumerate(sorted(rows_by_group.items())):
                piece = output_root / f".{file_id}.{piece_index:06d}.arrow.tmp"
                ranges = _consecutive_ranges(
                    sorted(int(row["row_offset_in_group"]) for row in rows)
                )
                stats = read_remote_parquet_to_ipc(
                    context.handle,
                    relative_path=str(context.objects_by_file_id[file_id]["relative_path"]),
                    output_ipc_path=piece,
                    row_groups=[row_group_id],
                    row_ranges=ranges,
                )
                context.profile["row_groups_touched"] += 1
                context.profile["range_count"] += int(stats.get("range_count") or 0)
                context.profile["requested_range_bytes"] += int(
                    stats.get("requested_range_bytes") or 0
                )
                context.profile["bytes_fetched"] += int(stats.get("received_range_bytes") or 0)
                ordered_source_indexes = [int(row["source_row_index"]) for row in rows]
                with piece.open("rb") as source:
                    reader = ipc.open_file(source)
                    table = reader.read_all()
                piece.unlink(missing_ok=True)
                if table.num_rows != len(ordered_source_indexes):
                    raise SmokingDataError(
                        "Remote active payload row count differs from its coordinate request.",
                        code="remote.active_payload_row_mismatch",
                    )
                if writer is None:
                    writer = pq.ParquetWriter(temporary, table.schema, compression="zstd")
                writer.write_table(table, row_group_size=65_536)
                for source_index in ordered_source_indexes:
                    output_position_by_source_index[source_index] = output_position
                    output_position += 1
            if writer is None:
                raise SmokingDataError(
                    "Remote active payload selection produced no rows.",
                    code="remote.active_payload_empty",
                )
            writer.close()
            writer = None
            os.replace(temporary, payload_path)
        finally:
            if writer is not None:
                writer.close()
            temporary.unlink(missing_ok=True)
        context.payload_object_ids.add(file_id)
        context.profile["payload_objects_touched"] = len(context.payload_object_ids)
        row_group_starts: list[int] = []
        next_start = 0
        payload_metadata = pq.ParquetFile(payload_path).metadata
        for row_group_id in range(payload_metadata.num_row_groups):
            row_group_starts.append(next_start)
            next_start += int(payload_metadata.row_group(row_group_id).num_rows)
        row_group_by_source_index = {
            source_index: max(
                index
                for index, start in enumerate(row_group_starts)
                if start <= output_position
            )
            for source_index, output_position in output_position_by_source_index.items()
        }
        source_subset = updated.get_column(source_file_column) == source_text
        updated = updated.with_columns(
            pl.when(source_subset)
            .then(
                pl.col(source_row_index_column)
                .replace_strict(
                    output_position_by_source_index,
                    default=pl.col(source_row_index_column),
                    return_dtype=pl.Int64,
                )
            )
            .otherwise(pl.col(source_row_index_column))
            .alias(source_row_index_column),
            pl.when(source_subset)
            .then(pl.lit(str(payload_path)))
            .otherwise(pl.col(source_file_column))
            .alias(source_file_column),
            pl.when(source_subset)
            .then(
                pl.col(source_row_index_column)
                .replace_strict(
                    row_group_by_source_index,
                    default=pl.col(source_row_group_column),
                    return_dtype=pl.Int64,
                )
            )
            .otherwise(pl.col(source_row_group_column))
            .alias(source_row_group_column),
        )
        stat = payload_path.stat()
        materialized.append(
            DatasetFile(
                path=payload_path,
                size_bytes=stat.st_size,
                modified_ns=stat.st_mtime_ns,
                dataset_root=output_root,
                dataset_id=context.handle.dataset_id,
                source_kind="s3_active_payload",
                file_id=file_id,
                relative_path=str(context.objects_by_file_id[file_id]["relative_path"]),
                content_sha256=str(context.objects_by_file_id[file_id]["content_sha256"]),
                object_key=str(context.objects_by_file_id[file_id]["object_key"]),
            )
        )
    return updated, materialized


def _consecutive_ranges(offsets: list[int]) -> list[dict[str, int]]:
    if not offsets:
        return []
    result = []
    start = previous = offsets[0]
    for value in offsets[1:]:
        if value == previous + 1:
            previous = value
            continue
        result.append({"start": start, "end_exclusive": previous + 1})
        start = previous = value
    result.append({"start": start, "end_exclusive": previous + 1})
    return result


def _matches_relative_path(relative: str, requested: tuple[str, ...], *, recursive: bool) -> bool:
    for value in requested:
        if relative == value:
            return True
        if recursive and relative.startswith(f"{value}/"):
            return True
    return False
