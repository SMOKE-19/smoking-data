from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from smoking_data.core.exceptions import SmokingDataError
from smoking_data.runtime.paths import ensure_dir, file_sha256

from .config import ParquetPublicationSpec

FILES_SCHEMA_VERSION = "smoking-data.remote-parquet-files.v1"
ROW_GROUP_SCHEMA_VERSION = "smoking-data.remote-parquet-row-groups.v1"
PAGE_SCHEMA_VERSION = "smoking-data.remote-parquet-pages.v1"
KEY_SCHEMA_VERSION = "smoking-data.remote-parquet-keys.v1"


@dataclass(frozen=True, slots=True)
class BuiltIndex:
    root: Path
    files: int
    row_groups: int
    pages: int
    key_rows: int
    page_index_complete: bool
    artifact_paths: tuple[Path, ...]
    key_types: dict[str, str]


def build_parquet_indexes(
    dataset_root: Path,
    output_root: Path,
    *,
    generation_id: str,
    generation_prefix: str,
    parts: list[dict[str, Any]],
    spec: ParquetPublicationSpec,
    cache_root: Path | None = None,
) -> BuiltIndex:
    ensure_dir(output_root)
    if cache_root is not None:
        ensure_dir(cache_root)
    file_rows: list[dict[str, Any]] = []
    row_group_rows: list[dict[str, Any]] = []
    page_rows: list[dict[str, Any]] = []
    key_piece_root = output_root / ".key-pieces"
    key_rows = 0
    page_complete = True
    profiles: list[tuple[Path, str, str, pq.ParquetFile]] = []
    key_type_contract: dict[str, str] = {}
    for part in sorted(parts, key=lambda item: str(item.get("relative_path") or "")):
        relative = str(part["relative_path"])
        path = (dataset_root / relative).resolve()
        if not path.is_relative_to(dataset_root.resolve()) or not path.is_file():
            raise SmokingDataError(
                "Parquet index source reference is invalid.",
                code="remote.index_source_invalid",
                context={"relative_path": relative},
            )
        content_sha256 = str(part.get("sha256") or file_sha256(path))
        file_id = hashlib.sha256(f"{relative}\0{content_sha256}".encode()).hexdigest()
        cached = _read_physical_shard(cache_root, content_sha256) if cache_root is not None else None
        object_key = f"{generation_prefix}/data/{relative}"
        if cached is not None and not spec.key_columns:
            file_rows.append(_bind_physical_row(cached["file"], generation_id, object_key))
            row_group_rows.extend(
                _bind_physical_row(row, generation_id, object_key) for row in cached["row_groups"]
            )
            page_rows.extend(
                _bind_physical_row(row, generation_id, object_key) for row in cached["pages"]
            )
            page_complete = page_complete and bool(cached["page_index_complete"])
            continue
        parquet = pq.ParquetFile(path)
        if spec.key_columns:
            missing = sorted(set(spec.key_columns) - set(parquet.schema_arrow.names))
            if missing:
                raise SmokingDataError(
                    "Parquet key index columns are missing.",
                    code="remote.key_columns_missing",
                    context={"columns": missing, "file_id": file_id},
                )
            current_key_types = {
                column: str(parquet.schema_arrow.field(column).type)
                for column in spec.key_columns
            }
            if key_type_contract and current_key_types != key_type_contract:
                raise SmokingDataError(
                    "Parquet key columns have incompatible types across parts.",
                    code="remote.key_schema_mismatch",
                    context={"file_id": file_id},
                )
            key_type_contract = current_key_types
        footer_start, footer_length, footer_fingerprint = _footer(path)
        schema_fingerprint = hashlib.sha256(str(parquet.schema_arrow).encode()).hexdigest()
        bucket = file_id[:2]
        file_rows.append(
            {
                "sidecar_schema_version": FILES_SCHEMA_VERSION,
                "generation_id": generation_id,
                "file_id": file_id,
                "relative_path": relative,
                "object_key": object_key,
                "size_bytes": path.stat().st_size,
                "rows": parquet.metadata.num_rows,
                "sha256": content_sha256,
                "footer_start": footer_start,
                "footer_length": footer_length,
                "footer_fingerprint": footer_fingerprint,
                "schema_fingerprint": schema_fingerprint,
                "row_group_count": parquet.metadata.num_row_groups,
                "partition_json": json.dumps(_hive_partitions(relative), sort_keys=True),
                "row_group_index_bucket": bucket,
                "page_index_bucket": bucket,
                "key_index_buckets": list(range(spec.hash_buckets)) if spec.key_columns else [],
            }
        )
        first_row = 0
        for row_group_id in range(parquet.metadata.num_row_groups):
            group = parquet.metadata.row_group(row_group_id)
            for column_index in range(group.num_columns):
                column = group.column(column_index)
                offsets = [
                    value
                    for value in (column.dictionary_page_offset, column.data_page_offset)
                    if value is not None
                ]
                row_group_rows.append(
                    {
                        "sidecar_schema_version": ROW_GROUP_SCHEMA_VERSION,
                        "generation_id": generation_id,
                        "file_id": file_id,
                        "object_key": object_key,
                        "row_group_id": row_group_id,
                        "first_row_index": first_row,
                        "row_count": group.num_rows,
                        "column_path": str(column.path_in_schema),
                        "physical_type": str(column.physical_type),
                        "column_chunk_offset": min(offsets),
                        "data_page_offset": column.data_page_offset,
                        "dictionary_page_offset": column.dictionary_page_offset,
                        "compressed_bytes": column.total_compressed_size,
                        "uncompressed_bytes": column.total_uncompressed_size,
                        "value_count": column.num_values,
                        "compression": str(column.compression),
                        "encodings_json": json.dumps([str(item) for item in column.encodings]),
                        "statistics_json": _statistics_json(column.statistics),
                        "column_index_available": bool(column.has_column_index),
                        "offset_index_available": bool(column.has_offset_index),
                    }
                )
            first_row += group.num_rows
        inspected_pages = _page_rows(path)
        file_page_complete = inspected_pages is not None
        if inspected_pages is None:
            page_complete = False
        else:
            page_rows.extend(
                {
                    "sidecar_schema_version": PAGE_SCHEMA_VERSION,
                    "generation_id": generation_id,
                    "file_id": file_id,
                    "object_key": object_key,
                    **row,
                }
                for row in inspected_pages
            )
        profiles.append((path, relative, file_id, parquet))
        if cache_root is not None and not spec.key_columns:
            _write_physical_shard(
                cache_root,
                content_sha256,
                file_rows[-1],
                row_group_rows,
                page_rows,
                file_page_complete,
                file_id=file_id,
            )

    if spec.index_level == "page_required" and not page_complete:
        raise SmokingDataError(
            "page_required publication needs OffsetIndex in every Parquet part.",
            code="remote.page_index_required",
        )
    artifacts: list[Path] = []
    artifacts.extend(_write_file_bucket_rows(output_root / "files", file_rows))
    artifacts.extend(_write_file_bucket_rows(output_root / "row_groups", row_group_rows))
    if page_rows:
        artifacts.extend(_write_file_bucket_rows(output_root / "pages", page_rows))
    if spec.key_columns:
        for path, _, file_id, parquet in profiles:
            key_rows += _spool_key_pieces(
                path,
                parquet,
                key_piece_root,
                generation_id=generation_id,
                file_id=file_id,
                key_columns=spec.key_columns,
                null_policy=spec.key_null_policy,
                hash_buckets=spec.hash_buckets,
            )
        artifacts.extend(_compact_key_pieces(key_piece_root, output_root / "keys"))
    return BuiltIndex(
        root=output_root,
        files=len(file_rows),
        row_groups=len(row_group_rows),
        pages=len(page_rows),
        key_rows=key_rows,
        page_index_complete=page_complete,
        artifact_paths=tuple(artifacts),
        key_types=key_type_contract,
    )


def _physical_shard_path(cache_root: Path, content_sha256: str) -> Path:
    return cache_root / f"{content_sha256}.physical.json"


def _write_physical_shard(
    cache_root: Path,
    content_sha256: str,
    file_row: dict[str, Any],
    row_group_rows: list[dict[str, Any]],
    page_rows: list[dict[str, Any]],
    page_index_complete: bool,
    *,
    file_id: str,
) -> None:
    payload = {
        "schema_version": "smoking-data.remote-parquet-physical-cache.v1",
        "content_sha256": content_sha256,
        "file_id": file_id,
        "file": _unbind_physical_row(file_row),
        "row_groups": [_unbind_physical_row(row) for row in row_group_rows if row.get("file_id") == file_id],
        "pages": [_unbind_physical_row(row) for row in page_rows if row.get("file_id") == file_id],
        "page_index_complete": page_index_complete,
    }
    destination = _physical_shard_path(cache_root, content_sha256)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    # Preserve the Parquet column order from the first build.  Sorting JSON keys
    # would reorder the row dictionaries on cache reload and produce a
    # byte-different sidecar for the same immutable generation.
    temporary.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    temporary.replace(destination)


def _read_physical_shard(cache_root: Path | None, content_sha256: str) -> dict[str, Any] | None:
    if cache_root is None:
        return None
    path = _physical_shard_path(cache_root, content_sha256)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("schema_version") != "smoking-data.remote-parquet-physical-cache.v1":
        return None
    if payload.get("content_sha256") != content_sha256:
        return None
    return payload


def _unbind_physical_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key not in {"generation_id", "object_key"}}


def _bind_physical_row(row: dict[str, Any], generation_id: str, object_key: str) -> dict[str, Any]:
    bound: dict[str, Any] = {}
    for key, value in row.items():
        if key == "file_id":
            bound["generation_id"] = generation_id
        bound[key] = value
        if key == "relative_path":
            bound["object_key"] = object_key
        elif key == "file_id" and "relative_path" not in row:
            bound["object_key"] = object_key
    return bound


def _write_file_bucket_rows(root: Path, rows: list[dict[str, Any]]) -> list[Path]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row["file_id"])[:2], []).append(row)
    result: list[Path] = []
    for bucket, values in sorted(buckets.items()):
        path = root / f"bucket={bucket}" / "part-000.parquet"
        ensure_dir(path.parent)
        pq.write_table(pa.Table.from_pylist(values), path, compression="zstd", write_statistics=True)
        result.append(path)
    return result


def _spool_key_pieces(
    path: Path,
    parquet: pq.ParquetFile,
    root: Path,
    *,
    generation_id: str,
    file_id: str,
    key_columns: tuple[str, ...],
    null_policy: str,
    hash_buckets: int,
) -> int:
    missing = sorted(set(key_columns) - set(parquet.schema_arrow.names))
    if missing:
        raise SmokingDataError(
            "Parquet key index columns are missing.",
            code="remote.key_columns_missing",
            context={"columns": missing, "file_id": file_id},
        )
    total = 0
    source_start = 0
    for row_group_id in range(parquet.metadata.num_row_groups):
        table = parquet.read_row_group(row_group_id, columns=list(key_columns))
        bucket_rows: dict[int, list[dict[str, Any]]] = {}
        for offset in range(table.num_rows):
            values = [table[column][offset].as_py() for column in key_columns]
            if any(value is None for value in values):
                if null_policy == "skip":
                    continue
                raise SmokingDataError(
                    "Null key encountered while building Parquet key index.",
                    code="remote.key_null",
                    context={"file_id": file_id, "row_group_id": row_group_id, "row_offset": offset},
                )
            key_hash = _typed_key_hash(table.schema, key_columns, values)
            bucket = int(key_hash[:16], 16) & (hash_buckets - 1)
            row = {column: value for column, value in zip(key_columns, values, strict=True)}
            row.update(
                {
                    "sidecar_schema_version": KEY_SCHEMA_VERSION,
                    "generation_id": generation_id,
                    "key_hash": key_hash,
                    "key_bucket": bucket,
                    "file_id": file_id,
                    "row_group_id": row_group_id,
                    "row_offset_in_group": offset,
                    "source_row_index": source_start + offset,
                }
            )
            bucket_rows.setdefault(bucket, []).append(row)
            total += 1
        for bucket, rows in bucket_rows.items():
            piece = root / f"bucket={bucket:05d}" / f"{file_id}-{row_group_id:06d}.parquet"
            ensure_dir(piece.parent)
            pq.write_table(pa.Table.from_pylist(rows), piece, compression="zstd")
        source_start += table.num_rows
    return total


def _compact_key_pieces(source: Path, target: Path) -> list[Path]:
    result: list[Path] = []
    if not source.is_dir():
        return result
    for bucket in sorted(path for path in source.iterdir() if path.is_dir()):
        pieces = sorted(bucket.glob("*.parquet"))
        if not pieces:
            continue
        destination = target / bucket.name / "part-000.parquet"
        ensure_dir(destination.parent)
        writer: pq.ParquetWriter | None = None
        try:
            for piece in pieces:
                table = pq.read_table(piece)
                sort_columns = [
                    ("key_hash", "ascending"),
                    ("file_id", "ascending"),
                    ("source_row_index", "ascending"),
                ]
                table = table.sort_by(sort_columns)
                if writer is None:
                    writer = pq.ParquetWriter(destination, table.schema, compression="zstd")
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()
        result.append(destination)
    return result


def _page_rows(path: Path) -> list[dict[str, Any]] | None:
    try:
        from smoking_data_engine_rs import inspect_parquet_pages
    except ImportError:
        return None
    document = inspect_parquet_pages(str(path))
    if not document.get("page_index_available"):
        return None
    return [dict(row) for row in document.get("pages", [])]


def _footer(path: Path) -> tuple[int, int, str]:
    size = path.stat().st_size
    if size < 12:
        raise SmokingDataError("Invalid Parquet footer.", code="remote.invalid_parquet_footer")
    with path.open("rb") as handle:
        handle.seek(-8, 2)
        trailer = handle.read(8)
        if trailer[4:] != b"PAR1":
            raise SmokingDataError("Invalid Parquet footer magic.", code="remote.invalid_parquet_footer")
        footer_length = struct.unpack("<I", trailer[:4])[0]
        footer_start = size - footer_length - 8
        if footer_start < 4:
            raise SmokingDataError("Invalid Parquet footer length.", code="remote.invalid_parquet_footer")
        handle.seek(footer_start)
        footer = handle.read(footer_length + 8)
    return footer_start, footer_length + 8, hashlib.sha256(footer).hexdigest()


def _statistics_json(statistics: Any) -> str:
    if statistics is None:
        return "null"
    payload = {
        "null_count": statistics.null_count,
        "num_values": statistics.num_values,
        "min": _json_value(statistics.min),
        "max": _json_value(statistics.max),
        "has_min_max": statistics.has_min_max,
    }
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _typed_key_hash(schema: pa.Schema, columns: tuple[str, ...], values: list[Any]) -> str:
    payload = [
        {"name": name, "type": str(schema.field(name).type), "value": _json_value(value)}
        for name, value in zip(columns, values, strict=True)
    ]
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()[:32]


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _hive_partitions(relative: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for part in Path(relative).parts[:-1]:
        if "=" in part:
            key, value = part.split("=", 1)
            if key:
                result[key] = value
    return result
