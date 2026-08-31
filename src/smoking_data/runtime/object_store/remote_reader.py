from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc

from smoking_data.core.exceptions import SmokingDataError

from .backend import S3ObjectStore
from .config import ObjectStoreTarget, load_object_store_target, validate_relative_prefix


@dataclass(frozen=True, slots=True)
class RemoteGenerationHandle:
    target: ObjectStoreTarget
    dataset_prefix: str
    generation_id: str
    pointer: dict[str, Any]
    manifest: dict[str, Any]
    pointer_etag: str | None

    @property
    def dataset_uri(self) -> str:
        return f"s3://{self.target.bucket}/{self.target.object_key(self.dataset_prefix)}"


def open_remote_generation(
    project_root: str | Path,
    *,
    target_name: str,
    dataset_prefix: str,
) -> RemoteGenerationHandle:
    target = load_object_store_target(project_root, target_name)
    prefix = validate_relative_prefix(dataset_prefix, path="dataset_prefix")
    store = S3ObjectStore(target)
    store.preflight()
    pointer_key = target.object_key(f"{prefix}/catalog/latest.json")
    pointer_bytes, pointer_meta = store.get(pointer_key)
    pointer = _json_document(pointer_bytes, kind="pointer")
    manifest_key = str(pointer.get("manifest_key") or "")
    expected_root = target.object_key(f"{prefix}/generations") + "/"
    if not manifest_key.startswith(expected_root):
        raise SmokingDataError(
            "Remote pointer references an object outside its dataset prefix.",
            code="remote.pointer_reference_invalid",
        )
    manifest_bytes, _ = store.get(manifest_key)
    expected_sha = str(pointer.get("manifest_sha256") or "")
    if hashlib.sha256(manifest_bytes).hexdigest() != expected_sha:
        raise SmokingDataError(
            "Remote manifest checksum differs from the pinned pointer.",
            code="remote.manifest_checksum_mismatch",
        )
    manifest = _json_document(manifest_bytes, kind="manifest")
    generation_id = str(pointer.get("generation_id") or "")
    if generation_id != str(manifest.get("generation_id") or ""):
        raise SmokingDataError(
            "Remote pointer and manifest generation identities differ.",
            code="remote.generation_mismatch",
        )
    generation_root = target.object_key(f"{prefix}/generations/{generation_id}") + "/"
    for item in manifest.get("objects") or []:
        if not isinstance(item, dict) or not str(item.get("object_key") or "").startswith(
            generation_root
        ):
            raise SmokingDataError(
                "Remote manifest references an object outside its immutable generation.",
                code="remote.manifest_reference_invalid",
            )
    return RemoteGenerationHandle(
        target=target,
        dataset_prefix=prefix,
        generation_id=generation_id,
        pointer=pointer,
        manifest=manifest,
        pointer_etag=pointer_meta.etag,
    )


def read_remote_parquet_to_ipc(
    handle: RemoteGenerationHandle,
    *,
    relative_path: str,
    output_ipc_path: str | Path,
    projection: list[str] | None = None,
    row_groups: list[int] | None = None,
    row_ranges: list[dict[str, int]] | None = None,
    batch_size: int = 65_536,
) -> dict[str, Any]:
    normalized = validate_relative_prefix(relative_path, path="relative_path")
    expected_suffix = f"/data/{normalized}"
    obj = next(
        (
            item
            for item in handle.manifest.get("objects") or []
            if isinstance(item, dict)
            and item.get("role") == "parquet_data"
            and str(item.get("object_key") or "").endswith(expected_suffix)
        ),
        None,
    )
    if obj is None:
        raise SmokingDataError(
            "Requested Parquet part is absent from the pinned generation.",
            code="remote.parquet_part_missing",
            context={"relative_path": normalized, "generation_id": handle.generation_id},
        )
    try:
        from smoking_data_engine_rs import read_s3_parquet_to_ipc as rust_read
    except ImportError as exc:
        raise SmokingDataError(
            "Rust S3 Parquet range reader is unavailable.",
            code="remote.range_reader_unavailable",
        ) from exc
    store = S3ObjectStore(handle.target)
    store.preflight()
    request = {
        "bucket": handle.target.bucket,
        "region": handle.target.region,
        "endpoint_url": handle.target.endpoint_url,
        "path_style": handle.target.path_style,
        **store.rust_credential_payload(),
        "object_key": obj["object_key"],
        "file_size": int(obj["size_bytes"]),
        "projection": projection,
        "row_groups": row_groups,
        "row_ranges": row_ranges,
        "batch_size": batch_size,
        "output_ipc_path": str(Path(output_ipc_path).expanduser().resolve()),
    }
    try:
        result = rust_read(request)
    finally:
        request.clear()
    result["generation_id"] = handle.generation_id
    result["relative_path"] = normalized
    return result


def lookup_remote_parquet_key_coordinates(
    handle: RemoteGenerationHandle,
    *,
    key_values: dict[str, Any],
    key_types: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    sidecar = dict((handle.manifest.get("sidecars") or {}).get("parquet") or {})
    key_columns = [str(value) for value in sidecar.get("key_columns") or []]
    planning_columns = [str(value) for value in sidecar.get("planning_columns") or []]
    resolved_key_types = {
        str(key): str(value)
        for key, value in (key_types or sidecar.get("key_types") or {}).items()
    }
    if not key_columns:
        raise SmokingDataError(
            "Pinned generation has no Parquet key index.",
            code="remote.key_index_unavailable",
        )
    if set(key_values) != set(key_columns) or set(resolved_key_types) != set(key_columns):
        raise SmokingDataError(
            "Key lookup values and types must exactly match the published key columns.",
            code="remote.key_contract_mismatch",
            context={"key_columns": key_columns},
        )
    key_hash = _typed_key_hash(key_columns, resolved_key_types, key_values)
    hash_buckets = int(sidecar.get("hash_buckets") or 0)
    if hash_buckets < 1 or hash_buckets & (hash_buckets - 1):
        raise SmokingDataError(
            "Pinned generation has an invalid key bucket contract.",
            code="remote.key_bucket_contract_invalid",
        )
    bucket = int(key_hash[:16], 16) & (hash_buckets - 1)
    suffix = f"/indexes/parquet/keys/bucket={bucket:05d}/part-000.parquet"
    index_object = next(
        (
            item
            for item in handle.manifest.get("objects") or []
            if isinstance(item, dict)
            and item.get("role") == "parquet_index"
            and str(item.get("object_key") or "").endswith(suffix)
        ),
        None,
    )
    if index_object is None:
        return []
    table = _read_remote_manifest_parquet_object(handle, index_object)
    data_by_file_id = _parquet_data_by_file_id(handle)
    result: list[dict[str, Any]] = []
    for row in table.to_pylist():
        if row.get("key_hash") != key_hash or any(
            row.get(column) != key_values[column] for column in key_columns
        ):
            continue
        file_id = str(row.get("file_id") or "")
        data = data_by_file_id.get(file_id)
        if data is None:
            raise SmokingDataError(
                "Key index references an unknown Parquet data object.",
                code="remote.key_data_reference_invalid",
                context={"file_id": file_id},
            )
        coordinate = {
            "file_id": file_id,
            "relative_path": data["relative_path"],
            "row_group_id": int(row["row_group_id"]),
            "row_offset_in_group": int(row["row_offset_in_group"]),
            "source_row_index": int(row["source_row_index"]),
        }
        if planning_columns:
            coordinate["planning_values"] = {
                column: row.get(column) for column in planning_columns
            }
        result.append(coordinate)
    result.sort(
        key=lambda row: (
            row["relative_path"],
            row["row_group_id"],
            row["row_offset_in_group"],
        )
    )
    return result


def read_remote_parquet_key_to_ipc(
    handle: RemoteGenerationHandle,
    *,
    key_values: dict[str, Any],
    key_types: dict[str, str] | None = None,
    output_ipc_path: str | Path,
    projection: list[str] | None = None,
    batch_size: int = 65_536,
) -> dict[str, Any]:
    coordinates = lookup_remote_parquet_key_coordinates(
        handle,
        key_values=key_values,
        key_types=key_types,
    )
    if not coordinates:
        raise SmokingDataError(
            "Requested key is absent from the pinned generation.",
            code="remote.key_not_found",
            context={"generation_id": handle.generation_id},
        )
    output_path = Path(output_ipc_path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output_path.with_suffix(output_path.suffix + ".tmp")
    writer: ipc.RecordBatchFileWriter | None = None
    output_schema: pa.Schema | None = None
    rows = 0
    try:
        with tempfile.TemporaryDirectory(prefix=".smoking-data-key-read-") as temporary:
            for index, coordinate in enumerate(coordinates):
                piece = Path(temporary) / f"piece-{index:06d}.arrow"
                read_remote_parquet_to_ipc(
                    handle,
                    relative_path=str(coordinate["relative_path"]),
                    output_ipc_path=piece,
                    projection=projection,
                    row_groups=[int(coordinate["row_group_id"])],
                    row_ranges=[
                        {
                            "start": int(coordinate["row_offset_in_group"]),
                            "end_exclusive": int(coordinate["row_offset_in_group"]) + 1,
                        }
                    ],
                    batch_size=batch_size,
                )
                with piece.open("rb") as source:
                    reader = ipc.open_file(source)
                    if writer is None:
                        output_schema = reader.schema
                        writer = ipc.new_file(temporary_output, reader.schema)
                    elif output_schema != reader.schema:
                        raise SmokingDataError(
                            "Selected remote Parquet parts have incompatible schemas.",
                            code="remote.key_result_schema_mismatch",
                        )
                    for batch_index in range(reader.num_record_batches):
                        batch = reader.get_batch(batch_index)
                        writer.write_batch(batch)
                        rows += batch.num_rows
        if writer is None:
            raise SmokingDataError(
                "Remote key selection produced no Arrow batches.",
                code="remote.key_result_empty",
            )
        writer.close()
        writer = None
        temporary_output.replace(output_path)
    finally:
        if writer is not None:
            writer.close()
        temporary_output.unlink(missing_ok=True)
    return {
        "schema_version": "smoking-data.s3-parquet-key-read.v1",
        "generation_id": handle.generation_id,
        "coordinate_count": len(coordinates),
        "rows": rows,
        "output_ipc_path": str(output_path),
    }


def lookup_remote_sbdf_key_coordinates(
    handle: RemoteGenerationHandle,
    *,
    key_values: dict[str, Any],
    key_types: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    representation = dict((handle.manifest.get("representations") or {}).get("sbdf") or {})
    key_columns = [str(value) for value in representation.get("key_columns") or []]
    resolved_types = {str(k): str(v) for k, v in (key_types or representation.get("key_types") or {}).items()}
    hash_buckets = int(representation.get("hash_buckets") or 0)
    if not key_columns or set(key_values) != set(key_columns) or set(resolved_types) != set(key_columns):
        raise SmokingDataError(
            "Key lookup values and types must exactly match the published SBDF key columns.",
            code="remote.sbdf_key_contract_mismatch",
            context={"key_columns": key_columns},
        )
    if hash_buckets < 1 or hash_buckets & (hash_buckets - 1):
        raise SmokingDataError("Pinned generation has an invalid SBDF key bucket contract.", code="remote.sbdf_key_bucket_contract_invalid")
    key_hash = _typed_key_hash(key_columns, resolved_types, key_values)
    suffix = f"/indexes/sbdf/keys/bucket={int(key_hash[:16], 16) & (hash_buckets - 1):05d}/part-000.parquet"
    index_object = next(
        (item for item in handle.manifest.get("objects") or []
         if isinstance(item, dict) and item.get("role") == "sbdf_index"
         and str(item.get("object_key") or "").endswith(suffix)),
        None,
    )
    if index_object is None:
        return []
    table = _read_remote_manifest_parquet_object(handle, index_object)
    result: list[dict[str, Any]] = []
    for row in table.to_pylist():
        if row.get("key_hash") != key_hash or any(row.get(column) != key_values[column] for column in key_columns):
            continue
        result.append({
            "sbdf_object_key": row["sbdf_object_key"],
            "sbdf_sha256": row.get("sbdf_sha256"),
            "slice_id": int(row["__sbdf_slice_id"]),
            "byte_offset": int(row["__sbdf_byte_offset"]),
            "byte_length": int(row["__sbdf_byte_length"]),
            "preamble_bytes": int(row["preamble_bytes"]),
            "end_marker_offset": int(row["end_marker_offset"]),
            "file_size": int(row["file_size"]),
        })
    return result


def read_remote_sbdf_key_to_ipc(
    handle: RemoteGenerationHandle,
    *,
    key_values: dict[str, Any],
    key_types: dict[str, str] | None = None,
    output_ipc_path: str | Path,
) -> dict[str, Any]:
    """Fetch only the SBDF preamble, selected slices, and end marker from S3."""
    coordinates = lookup_remote_sbdf_key_coordinates(handle, key_values=key_values, key_types=key_types)
    if not coordinates:
        raise SmokingDataError("Requested key is absent from the pinned SBDF generation.", code="remote.key_not_found")
    try:
        from smoking_sbdf import decode_sbdf_to_ipc
    except ImportError as exc:
        raise SmokingDataError("SBDF remote decoder is unavailable.", code="remote.sbdf_decoder_unavailable") from exc
    output_path = Path(output_ipc_path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output_path.with_suffix(output_path.suffix + ".tmp")
    writer: ipc.RecordBatchFileWriter | None = None
    output_schema: pa.Schema | None = None
    rows = 0
    object_groups: dict[str, list[dict[str, Any]]] = {}
    for coordinate in coordinates:
        object_groups.setdefault(str(coordinate["sbdf_object_key"]), []).append(coordinate)
    try:
        with tempfile.TemporaryDirectory(prefix=".smoking-data-sbdf-read-") as temporary:
            for object_index, (object_key, group) in enumerate(object_groups.items()):
                ordered = sorted({(item["slice_id"], item["byte_offset"], item["byte_length"]): item for item in group}.values(), key=lambda item: item["byte_offset"])
                first = ordered[0]
                ranges = [(0, first["preamble_bytes"])]
                ranges.extend((item["byte_offset"], item["byte_offset"] + item["byte_length"]) for item in ordered)
                ranges.append((first["end_marker_offset"], first["file_size"]))
                store = S3ObjectStore(handle.target)
                payloads = store.get_ranges(object_key, ranges)
                if any(len(payload) != end - start for payload, (start, end) in zip(payloads, ranges, strict=True)):
                    raise SmokingDataError("SBDF remote range length mismatch.", code="remote.sbdf_range_length_mismatch")
                sbdf_path = Path(temporary) / f"selected-{object_index:04d}.sbdf"
                sbdf_path.write_bytes(b"".join([payloads[0], *payloads[1:-1], payloads[-1]]))
                piece = Path(temporary) / f"selected-{object_index:04d}.arrow"
                decode_sbdf_to_ipc(sbdf_path, piece)
                with piece.open("rb") as source:
                    reader = ipc.open_file(source)
                    if writer is None:
                        output_schema = reader.schema
                        writer = ipc.new_file(temporary_output, reader.schema)
                    elif output_schema != reader.schema:
                        raise SmokingDataError("Selected SBDF objects have incompatible schemas.", code="remote.sbdf_result_schema_mismatch")
                    for batch_index in range(reader.num_record_batches):
                        batch = reader.get_batch(batch_index)
                        writer.write_batch(batch)
                        rows += batch.num_rows
        if writer is None:
            raise SmokingDataError("Remote SBDF selection produced no rows.", code="remote.key_result_empty")
        writer.close()
        writer = None
        temporary_output.replace(output_path)
    finally:
        if writer is not None:
            writer.close()
        temporary_output.unlink(missing_ok=True)
    return {"schema_version": "smoking-data.s3-sbdf-key-read.v1", "generation_id": handle.generation_id, "coordinate_count": len(coordinates), "rows": rows, "output_ipc_path": str(output_path)}


def _read_remote_manifest_parquet_object(
    handle: RemoteGenerationHandle, obj: dict[str, Any]
) -> pa.Table:
    try:
        from smoking_data_engine_rs import read_s3_parquet_to_ipc as rust_read
    except ImportError as exc:
        raise SmokingDataError(
            "Rust S3 Parquet range reader is unavailable.",
            code="remote.range_reader_unavailable",
        ) from exc
    store = S3ObjectStore(handle.target)
    store.preflight()
    with tempfile.TemporaryDirectory(prefix=".smoking-data-index-read-") as temporary:
        output = Path(temporary) / "index.arrow"
        request = {
            "bucket": handle.target.bucket,
            "region": handle.target.region,
            "endpoint_url": handle.target.endpoint_url,
            "path_style": handle.target.path_style,
            **store.rust_credential_payload(),
            "object_key": obj["object_key"],
            "file_size": int(obj["size_bytes"]),
            "batch_size": 65_536,
            "output_ipc_path": str(output),
        }
        try:
            rust_read(request)
        finally:
            request.clear()
        with output.open("rb") as source:
            return ipc.open_file(source).read_all()


def _parquet_data_by_file_id(handle: RemoteGenerationHandle) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    generation_marker = f"/generations/{handle.generation_id}/data/"
    for item in handle.manifest.get("objects") or []:
        if not isinstance(item, dict) or item.get("role") != "parquet_data":
            continue
        object_key = str(item.get("object_key") or "")
        if generation_marker not in object_key:
            raise SmokingDataError(
                "Parquet data object is outside the pinned generation.",
                code="remote.manifest_reference_invalid",
            )
        relative = object_key.split(generation_marker, 1)[1]
        file_id = hashlib.sha256(
            f"{relative}\0{item.get('sha256') or ''}".encode()
        ).hexdigest()
        result[file_id] = {**item, "relative_path": relative}
    return result


def _typed_key_hash(
    columns: list[str], key_types: dict[str, str], key_values: dict[str, Any]
) -> str:
    payload = [
        {
            "name": column,
            "type": key_types[column],
            "value": _json_key_value(key_values[column]),
        }
        for column in columns
    ]
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode()).hexdigest()[:32]


def _json_key_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _json_document(payload: bytes, *, kind: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SmokingDataError(
            f"Remote {kind} is not valid JSON.",
            code=f"remote.invalid_{kind}",
        ) from exc
    if not isinstance(value, dict):
        raise SmokingDataError(
            f"Remote {kind} must be a JSON object.",
            code=f"remote.invalid_{kind}",
        )
    return value
