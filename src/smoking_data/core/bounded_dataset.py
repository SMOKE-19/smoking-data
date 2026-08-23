from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

BOUNDED_SUMMARY_VERSION = "smoking-data.bounded-dataset-multiset.v1"
MASK_256 = (1 << 256) - 1


def summarize_parquet_dataset_bounded(
    root: str | Path, *, batch_size: int = 4_096
) -> dict[str, Any]:
    """Summarize a Parquet dataset while retaining at most one RecordBatch."""

    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    path = Path(root).expanduser().resolve()
    files = _parquet_files(path)
    if not files:
        raise ValueError(f"No parquet files found: {path}")
    schema: pa.Schema | None = None
    rows = 0
    row_groups = 0
    row_hashes = [0, 0]
    null_hashes = [0, 0]
    file_boundaries: list[dict[str, Any]] = []
    for file_path in files:
        parquet = pq.ParquetFile(file_path)
        current_schema = parquet.schema_arrow
        if schema is None:
            schema = current_schema
        elif current_schema != schema:
            raise ValueError(f"Parquet schema drift: {file_path}")
        metadata = parquet.metadata
        file_boundaries.append(
            {
                "relative_path": _relative_path(file_path, path),
                "rows": int(metadata.num_rows),
                "row_groups": int(metadata.num_row_groups),
            }
        )
        rows += int(metadata.num_rows)
        row_groups += int(metadata.num_row_groups)
        columns = current_schema.names
        for batch in parquet.iter_batches(batch_size=batch_size):
            for row in batch.to_pylist():
                values = [_normalize_value(row.get(column)) for column in columns]
                encoded = json.dumps(
                    values, ensure_ascii=False, separators=(",", ":"), default=str
                ).encode("utf-8")
                nulls = bytes(1 if row.get(column) is None else 0 for column in columns)
                _multiset_add(row_hashes, domain=b"row", payload=encoded)
                _multiset_add(null_hashes, domain=b"null", payload=nulls)
    assert schema is not None
    return {
        "schema_version": BOUNDED_SUMMARY_VERSION,
        "path": str(path),
        "batch_size": batch_size,
        "files": len(files),
        "rows": rows,
        "row_groups": row_groups,
        "columns": [
            {
                "name": field.name,
                "logical_dtype": str(field.type),
                "nullable": field.nullable,
            }
            for field in schema
        ],
        "row_multiset_sha256_sums": [f"{value:064x}" for value in row_hashes],
        "null_multiset_sha256_sums": [f"{value:064x}" for value in null_hashes],
        "file_boundaries": file_boundaries,
    }


def compare_bounded_summaries(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    require_file_boundaries: bool = False,
) -> dict[str, Any]:
    checks = {
        "rows_match": left.get("rows") == right.get("rows"),
        "schema_match": left.get("columns") == right.get("columns"),
        "row_multiset_match": (
            left.get("row_multiset_sha256_sums") == right.get("row_multiset_sha256_sums")
        ),
        "null_multiset_match": (
            left.get("null_multiset_sha256_sums") == right.get("null_multiset_sha256_sums")
        ),
    }
    if require_file_boundaries:
        checks["file_boundaries_match"] = _logical_file_boundaries(left) == _logical_file_boundaries(
            right
        )
    return {"ok": all(checks.values()), **checks, "left": left, "right": right}


def _logical_file_boundaries(summary: dict[str, Any]) -> list[tuple[str, int]]:
    return sorted(
        (str(item.get("relative_path")), int(item.get("rows") or 0))
        for item in summary.get("file_boundaries") or []
        if isinstance(item, dict)
    )


def _parquet_files(path: Path) -> list[Path]:
    if path.is_file() and path.suffix.lower() == ".parquet":
        return [path]
    return sorted(
        item
        for item in path.rglob("*.parquet")
        if item.is_file() and "_smoking_data" not in item.relative_to(path).parts
    )


def _relative_path(file_path: Path, root: Path) -> str:
    return file_path.name if root.is_file() else file_path.relative_to(root).as_posix()


def _normalize_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _multiset_add(accumulators: list[int], *, domain: bytes, payload: bytes) -> None:
    for index in range(len(accumulators)):
        digest = hashlib.sha256(domain + bytes([index]) + b"\0" + payload).digest()
        accumulators[index] = (accumulators[index] + int.from_bytes(digest, "big")) & MASK_256
