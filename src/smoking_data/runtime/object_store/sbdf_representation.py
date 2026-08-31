from __future__ import annotations

import hashlib
import json
import os
import shutil
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from smoking_data.backends.streaming_sbdf import SbdfExportRequest, export_sbdf_with_result
from smoking_data.core.exceptions import SmokingDataError
from smoking_data.runtime.paths import ensure_dir, file_sha256

from .config import SbdfPublicationSpec

SBDF_INDEX_SCHEMA_VERSION = "smoking-data.remote-sbdf-keys.v1"


@dataclass(frozen=True, slots=True)
class SbdfObject:
    file_id: str
    source_relative_path: str
    local_path: Path
    object_key: str
    sha256: str
    rows: int


@dataclass(frozen=True, slots=True)
class BuiltSbdfRepresentation:
    objects: tuple[SbdfObject, ...]
    index_paths: tuple[Path, ...]
    rows: int
    key_rows: int
    reused_files: int
    schema_fingerprint: str


def build_sbdf_representation(
    dataset_root: Path,
    output_root: Path,
    cache_root: Path,
    *,
    generation_id: str,
    generation_prefix: str,
    parts: list[dict[str, Any]],
    spec: SbdfPublicationSpec,
) -> BuiltSbdfRepresentation:
    if not spec.row_key_columns:
        raise SmokingDataError(
            "SBDF publication requires explicit row_key_columns.",
            code="remote.sbdf_key_columns_required",
        )
    ensure_dir(output_root / "representations" / "sbdf")
    ensure_dir(cache_root)
    objects: list[SbdfObject] = []
    piece_root = output_root / ".sbdf-key-pieces"
    total_rows = 0
    key_rows = 0
    reused = 0
    schema_contracts: list[dict[str, Any]] = []
    for part in sorted(parts, key=lambda item: str(item.get("relative_path") or "")):
        relative = str(part["relative_path"])
        parquet_path = (dataset_root / relative).resolve()
        content_sha256 = str(part.get("sha256") or file_sha256(parquet_path))
        file_id = hashlib.sha256(f"{relative}\0{content_sha256}".encode()).hexdigest()
        cached_sbdf = cache_root / f"{content_sha256}.sbdf"
        cached_sidecar = cache_root / f"{content_sha256}.keys.parquet"
        cache_hit = cached_sbdf.is_file() and cached_sidecar.is_file()
        if not cache_hit:
            temporary_sbdf = cached_sbdf.with_suffix(".sbdf.tmp")
            temporary_sidecar = cached_sidecar.with_suffix(".parquet.tmp")
            result = export_sbdf_with_result(
                SbdfExportRequest(
                    parquet_files=[parquet_path],
                    sbdf_path=temporary_sbdf,
                    row_key_columns=list(spec.row_key_columns),
                    sidecar_path=temporary_sidecar,
                    table_id=file_id,
                    batch_size=spec.batch_size,
                    encoding_rle=spec.encoding_rle,
                )
            )
            if not result.output_path.is_file() or not temporary_sidecar.is_file():
                raise SmokingDataError(
                    "smoking-sbdf did not produce the requested representation and sidecar.",
                    code="remote.sbdf_build_incomplete",
                    context={"file_id": file_id},
                )
            source_rows = int(part.get("rows") or pq.ParquetFile(parquet_path).metadata.num_rows)
            if result.row_count != source_rows:
                raise SmokingDataError(
                    "Parquet and SBDF conversion row counts differ.",
                    code="remote.sbdf_row_count_mismatch",
                    context={
                        "file_id": file_id,
                        "parquet_rows": source_rows,
                        "sbdf_rows": result.row_count,
                    },
                )
            temporary_sbdf.replace(cached_sbdf)
            temporary_sidecar.replace(cached_sidecar)
        else:
            reused += 1
        target_sbdf = output_root / "representations" / "sbdf" / f"{file_id}.sbdf"
        _link_or_copy(cached_sbdf, target_sbdf)
        sbdf_sha256 = file_sha256(target_sbdf)
        sbdf_object_key = f"{generation_prefix}/representations/sbdf/{file_id}.sbdf"
        source_rows = int(part.get("rows") or pq.ParquetFile(parquet_path).metadata.num_rows)
        source_schema = pq.ParquetFile(parquet_path).schema_arrow
        sbdf_schema = _read_sbdf_schema(target_sbdf)
        expected_sbdf_schema = _expected_sbdf_schema(source_schema)
        if sbdf_schema != expected_sbdf_schema:
            raise SmokingDataError(
                "Parquet and SBDF representation schemas differ.",
                code="remote.sbdf_schema_mismatch",
                context={"file_id": file_id},
            )
        schema_contracts.append(
            {"relative_path": relative, "columns": sbdf_schema}
        )
        sidecar_rows = pq.ParquetFile(cached_sidecar).metadata.num_rows
        if sidecar_rows != source_rows:
            raise SmokingDataError(
                "Parquet and SBDF key sidecar row counts differ.",
                code="remote.sbdf_row_count_mismatch",
                context={"file_id": file_id, "parquet_rows": source_rows, "sidecar_rows": sidecar_rows},
            )
        key_rows += _spool_remote_sidecar(
            cached_sidecar,
            piece_root,
            generation_id=generation_id,
            file_id=file_id,
            sbdf_object_key=sbdf_object_key,
            sbdf_sha256=sbdf_sha256,
            key_columns=spec.row_key_columns,
            hash_buckets=spec.hash_buckets,
        )
        total_rows += source_rows
        objects.append(
            SbdfObject(
                file_id=file_id,
                source_relative_path=relative,
                local_path=target_sbdf,
                object_key=sbdf_object_key,
                sha256=sbdf_sha256,
                rows=source_rows,
            )
        )
    indexes = _compact_pieces(piece_root, output_root / "indexes" / "sbdf" / "keys")
    return BuiltSbdfRepresentation(
        objects=tuple(objects),
        index_paths=tuple(indexes),
        rows=total_rows,
        key_rows=key_rows,
        reused_files=reused,
        schema_fingerprint=hashlib.sha256(
            json.dumps(
                schema_contracts,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
    )


def build_existing_sbdf_representation(
    dataset_root: Path,
    output_root: Path,
    *,
    generation_id: str,
    generation_prefix: str,
    parts: list[dict[str, Any]],
    spec: SbdfPublicationSpec,
) -> BuiltSbdfRepresentation:
    """Package an already committed SBDF dataset without rebuilding it from Parquet."""

    if not spec.row_key_columns:
        raise SmokingDataError(
            "SBDF publication requires explicit row_key_columns.",
            code="remote.sbdf_key_columns_required",
        )
    objects: list[SbdfObject] = []
    piece_root = output_root / ".sbdf-key-pieces"
    total_rows = 0
    key_rows = 0
    schema_contracts: list[dict[str, Any]] = []
    for part in sorted(parts, key=lambda item: str(item.get("relative_path") or "")):
        relative = str(part.get("relative_path") or "")
        sbdf_path = (dataset_root / relative).resolve()
        sidecar_relative = str(part.get("key_sidecar_relative_path") or "")
        sidecar_path = (dataset_root / sidecar_relative).resolve()
        if (
            not relative
            or not sidecar_relative
            or not sbdf_path.is_relative_to(dataset_root)
            or not sidecar_path.is_relative_to(dataset_root)
            or not sbdf_path.is_file()
            or not sidecar_path.is_file()
        ):
            raise SmokingDataError(
                "Committed SBDF artifact is missing its data or key sidecar.",
                code="remote.sbdf_local_reference_invalid",
                context={"relative_path": relative, "key_sidecar": sidecar_relative},
            )
        content_sha256 = str(part.get("sha256") or file_sha256(sbdf_path))
        file_id = hashlib.sha256(f"{relative}\0{content_sha256}".encode()).hexdigest()
        object_key = f"{generation_prefix}/representations/sbdf/{file_id}.sbdf"
        rows = int(part.get("rows") or 0)
        sidecar_rows = int(pq.ParquetFile(sidecar_path).metadata.num_rows)
        if sidecar_rows != rows:
            raise SmokingDataError(
                "SBDF artifact and key sidecar row counts differ.",
                code="remote.sbdf_row_count_mismatch",
                context={"file_id": file_id, "sbdf_rows": rows, "sidecar_rows": sidecar_rows},
            )
        schema_contracts.append(
            {"relative_path": relative, "columns": _read_sbdf_schema(sbdf_path)}
        )
        key_rows += _spool_remote_sidecar(
            sidecar_path,
            piece_root,
            generation_id=generation_id,
            file_id=file_id,
            sbdf_object_key=object_key,
            sbdf_sha256=content_sha256,
            key_columns=spec.row_key_columns,
            hash_buckets=spec.hash_buckets,
        )
        total_rows += rows
        objects.append(
            SbdfObject(
                file_id=file_id,
                source_relative_path=relative,
                local_path=sbdf_path,
                object_key=object_key,
                sha256=content_sha256,
                rows=rows,
            )
        )
    indexes = _compact_pieces(piece_root, output_root / "indexes" / "sbdf" / "keys")
    return BuiltSbdfRepresentation(
        objects=tuple(objects),
        index_paths=tuple(indexes),
        rows=total_rows,
        key_rows=key_rows,
        reused_files=len(objects),
        schema_fingerprint=hashlib.sha256(
            json.dumps(
                schema_contracts,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
    )


def _read_sbdf_schema(path: Path) -> list[dict[str, Any]]:
    with path.open("rb") as handle:
        if handle.read(5) != b"\xdf\x5b\x01\x01\x00":
            raise SmokingDataError("Invalid SBDF file header.", code="remote.sbdf_header_invalid")
        if handle.read(3) != b"\xdf\x5b\x02":
            raise SmokingDataError("Invalid SBDF table metadata.", code="remote.sbdf_header_invalid")
        if _read_i32(handle) != 0:
            raise SmokingDataError(
                "Unsupported SBDF table property metadata.",
                code="remote.sbdf_header_invalid",
            )
        column_count = _read_i32(handle)
        definition_count = _read_i32(handle)
        if column_count < 0 or definition_count < 0:
            raise SmokingDataError("Invalid SBDF metadata count.", code="remote.sbdf_header_invalid")
        definitions: list[tuple[bytes, int]] = []
        for _ in range(definition_count):
            name = _read_length_prefixed(handle)
            type_id = _read_u8(handle)
            if _read_u8(handle) != 0:
                raise SmokingDataError(
                    "Unsupported SBDF metadata default.",
                    code="remote.sbdf_header_invalid",
                )
            definitions.append((name, type_id))
        result: list[dict[str, Any]] = []
        for _ in range(column_count):
            column_name: str | None = None
            column_type: int | None = None
            for definition_name, definition_type in definitions:
                if _read_u8(handle) == 0:
                    continue
                value = _read_length_prefixed(handle)
                if definition_name == b"Name" and definition_type == 0x0A:
                    column_name = value.decode("utf-8")
                elif definition_name == b"DataType" and definition_type == 0x0C:
                    if len(value) != 1:
                        raise SmokingDataError(
                            "Invalid SBDF DataType metadata.",
                            code="remote.sbdf_header_invalid",
                        )
                    column_type = value[0]
            if column_name is None or column_type is None:
                raise SmokingDataError(
                    "SBDF column metadata is incomplete.",
                    code="remote.sbdf_header_invalid",
                )
            result.append({"name": column_name, "type_id": column_type})
    return result


def _expected_sbdf_schema(schema: pa.Schema) -> list[dict[str, Any]]:
    return [
        {"name": field.name, "type_id": _sbdf_type_id(field.type)}
        for field in schema
    ]


def _sbdf_type_id(dtype: pa.DataType) -> int:
    if pa.types.is_boolean(dtype):
        return 0x01
    if pa.types.is_integer(dtype) and getattr(dtype, "bit_width", 64) <= 32:
        return 0x02
    if pa.types.is_integer(dtype):
        return 0x03
    if pa.types.is_float32(dtype):
        return 0x04
    if pa.types.is_floating(dtype):
        return 0x05
    if pa.types.is_timestamp(dtype):
        return 0x06
    if pa.types.is_date(dtype):
        return 0x07
    if pa.types.is_time(dtype):
        return 0x08
    if pa.types.is_duration(dtype):
        return 0x09
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return 0x0A
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return 0x0C
    raise SmokingDataError(
        "Parquet schema contains a type unsupported by SBDF publication.",
        code="remote.sbdf_schema_unsupported",
        context={"arrow_type": str(dtype)},
    )


def _read_i32(handle: Any) -> int:
    payload = handle.read(4)
    if len(payload) != 4:
        raise SmokingDataError("Truncated SBDF metadata.", code="remote.sbdf_header_invalid")
    return struct.unpack("<i", payload)[0]


def _read_u8(handle: Any) -> int:
    payload = handle.read(1)
    if len(payload) != 1:
        raise SmokingDataError("Truncated SBDF metadata.", code="remote.sbdf_header_invalid")
    return payload[0]


def _read_length_prefixed(handle: Any) -> bytes:
    length = _read_i32(handle)
    if length < 0:
        raise SmokingDataError("Invalid SBDF metadata length.", code="remote.sbdf_header_invalid")
    payload = handle.read(length)
    if len(payload) != length:
        raise SmokingDataError("Truncated SBDF metadata.", code="remote.sbdf_header_invalid")
    return payload


def _spool_remote_sidecar(
    sidecar: Path,
    root: Path,
    *,
    generation_id: str,
    file_id: str,
    sbdf_object_key: str,
    sbdf_sha256: str,
    key_columns: tuple[str, ...],
    hash_buckets: int,
) -> int:
    parquet = pq.ParquetFile(sidecar)
    missing = sorted(set(key_columns) - set(parquet.schema_arrow.names))
    if missing:
        raise SmokingDataError(
            "smoking-sbdf sidecar misses requested key columns.",
            code="remote.sbdf_sidecar_key_missing",
            context={"columns": missing, "file_id": file_id},
        )
    metadata = {
        (key.decode() if isinstance(key, bytes) else str(key)): (
            value.decode() if isinstance(value, bytes) else str(value)
        )
        for key, value in (parquet.schema_arrow.metadata or {}).items()
    }
    total = 0
    for batch_index, batch in enumerate(parquet.iter_batches(batch_size=65_536)):
        table = pa.Table.from_batches([batch])
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row_index in range(table.num_rows):
            values = [table[column][row_index].as_py() for column in key_columns]
            key_hash = _key_hash(table.schema, key_columns, values)
            bucket = int(key_hash[:16], 16) & (hash_buckets - 1)
            row = {name: table[name][row_index].as_py() for name in table.column_names}
            row.update(
                {
                    "sidecar_schema_version": SBDF_INDEX_SCHEMA_VERSION,
                    "generation_id": generation_id,
                    "file_id": file_id,
                    "sbdf_object_key": sbdf_object_key,
                    "sbdf_sha256": sbdf_sha256,
                    "key_hash": key_hash,
                    "key_bucket": bucket,
                    "preamble_bytes": _metadata_int(metadata, "preamble"),
                    "end_marker_offset": _metadata_int(metadata, "end_marker"),
                    "file_size": _metadata_int(metadata, "file_size"),
                }
            )
            grouped.setdefault(bucket, []).append(row)
            total += 1
        for bucket, rows in grouped.items():
            path = root / f"bucket={bucket:05d}" / f"{file_id}-{batch_index:06d}.parquet"
            ensure_dir(path.parent)
            pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
    return total


def _compact_pieces(source: Path, target: Path) -> list[Path]:
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
                table = pq.read_table(piece).sort_by(
                    [("key_hash", "ascending"), ("file_id", "ascending"), ("__sbdf_row_index", "ascending")]
                )
                if writer is None:
                    writer = pq.ParquetWriter(destination, table.schema, compression="zstd")
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()
        result.append(destination)
    return result


def _key_hash(schema: pa.Schema, columns: tuple[str, ...], values: list[Any]) -> str:
    payload = [
        {"name": name, "type": str(schema.field(name).type), "value": _json_value(value)}
        for name, value in zip(columns, values, strict=True)
    ]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:32]


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _metadata_int(metadata: dict[str, str], needle: str) -> int | None:
    for key, value in metadata.items():
        if needle in key.lower():
            try:
                return int(value)
            except ValueError:
                continue
    return None


def _link_or_copy(source: Path, target: Path) -> None:
    ensure_dir(target.parent)
    target.unlink(missing_ok=True)
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)
