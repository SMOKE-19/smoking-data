from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True, slots=True)
class DatasetSummary:
    path: Path
    files: int
    rows: int
    schema: dict[str, str]
    sample_hash: str
    ordered_full_row_hash: str
    unordered_full_row_hash: str
    null_bitmap_hash: str
    duplicate_rows: int

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["path"] = str(self.path)
        return payload


@dataclass(frozen=True, slots=True)
class DatasetComparison:
    left: DatasetSummary
    right: DatasetSummary
    rows_match: bool
    schema_match: bool
    sample_hash_match: bool
    ordered_full_row_hash_match: bool
    unordered_full_row_hash_match: bool
    null_bitmap_match: bool
    duplicate_rows_match: bool

    @property
    def ok(self) -> bool:
        return (
            self.rows_match
            and self.schema_match
            and self.unordered_full_row_hash_match
            and self.null_bitmap_match
            and self.duplicate_rows_match
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "rows_match": self.rows_match,
            "schema_match": self.schema_match,
            "sample_hash_match": self.sample_hash_match,
            "ordered_full_row_hash_match": self.ordered_full_row_hash_match,
            "unordered_full_row_hash_match": self.unordered_full_row_hash_match,
            "null_bitmap_match": self.null_bitmap_match,
            "duplicate_rows_match": self.duplicate_rows_match,
            "left": self.left.to_dict(),
            "right": self.right.to_dict(),
        }


def summarize_parquet_dataset(path: str | Path, *, sample_rows: int = 1000) -> DatasetSummary:
    root = Path(path).expanduser().resolve()
    files = (
        sorted(item for item in root.rglob("*.parquet") if item.is_file())
        if root.is_dir()
        else [root]
    )
    if not files:
        raise ValueError(f"No parquet files found: {root}")
    df = pl.concat([pl.read_parquet(item) for item in files], how="diagonal_relaxed")
    schema = {name: str(dtype) for name, dtype in df.schema.items()}
    canonical_rows = [_canonical_row(row, df.columns) for row in df.iter_rows(named=True)]
    null_rows = [
        json.dumps(
            [
                _canonical_row(row, df.columns),
                [row.get(column) is None for column in df.columns],
            ],
            separators=(",", ":"),
        )
        for row in df.iter_rows(named=True)
    ]
    sample_hash = hashlib.sha256(
        "\n".join(sorted(canonical_rows)[:sample_rows]).encode("utf-8")
    ).hexdigest()
    schema_payload = json.dumps(schema, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return DatasetSummary(
        path=root,
        files=len(files),
        rows=df.height,
        schema=schema,
        sample_hash=sample_hash,
        ordered_full_row_hash=_rows_hash(canonical_rows, schema_payload=schema_payload),
        unordered_full_row_hash=_rows_hash(sorted(canonical_rows), schema_payload=schema_payload),
        null_bitmap_hash=_rows_hash(sorted(null_rows), schema_payload=schema_payload),
        duplicate_rows=len(canonical_rows) - len(set(canonical_rows)),
    )


def _canonical_row(row: dict[str, Any], columns: list[str]) -> str:
    return json.dumps(
        [row.get(column) for column in columns],
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def _rows_hash(rows: list[str], *, schema_payload: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(schema_payload)
    digest.update(b"\0")
    for row in rows:
        digest.update(row.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def compare_parquet_datasets(
    left_path: str | Path,
    right_path: str | Path,
    *,
    sample_rows: int = 1000,
) -> DatasetComparison:
    left = summarize_parquet_dataset(left_path, sample_rows=sample_rows)
    right = summarize_parquet_dataset(right_path, sample_rows=sample_rows)
    return DatasetComparison(
        left=left,
        right=right,
        rows_match=left.rows == right.rows,
        schema_match=left.schema == right.schema,
        sample_hash_match=left.sample_hash == right.sample_hash,
        ordered_full_row_hash_match=(left.ordered_full_row_hash == right.ordered_full_row_hash),
        unordered_full_row_hash_match=(
            left.unordered_full_row_hash == right.unordered_full_row_hash
        ),
        null_bitmap_match=left.null_bitmap_hash == right.null_bitmap_hash,
        duplicate_rows_match=left.duplicate_rows == right.duplicate_rows,
    )
