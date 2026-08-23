from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from smoking_data.runtime.paths import ensure_dir, file_sha256, reset_path

INTERMEDIATE_DATASET_VERSION = "smoking-data.intermediate.v1"
GROUPED_INTERMEDIATE_DATASET_VERSION = "smoking-data.grouped-intermediate.v1"


@dataclass(slots=True)
class IntermediateDataset:
    path: Path
    checksum: str
    rows: int
    size_bytes: int
    cleaned: bool = False

    def profile(self) -> dict[str, Any]:
        return {
            "version": INTERMEDIATE_DATASET_VERSION,
            "path": str(self.path),
            "checksum": self.checksum,
            "rows": self.rows,
            "size_bytes": self.size_bytes,
            "cleaned": self.cleaned,
        }

    def cleanup(self) -> None:
        reset_path(self.path)
        self.cleaned = True

    def __enter__(self) -> IntermediateDataset:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        del exc_type, exc, traceback
        self.cleanup()


@dataclass(slots=True)
class GroupedIntermediateDataset:
    root: Path
    part_paths: tuple[Path, ...]
    checksums: tuple[str, ...]
    rows: int
    group_keys: tuple[str, ...]
    cleaned: bool = False

    def profile(self) -> dict[str, Any]:
        return {
            "version": GROUPED_INTERMEDIATE_DATASET_VERSION,
            "root": str(self.root),
            "part_paths": [str(path) for path in self.part_paths],
            "checksums": list(self.checksums),
            "rows": self.rows,
            "group_keys": list(self.group_keys),
            "cleaned": self.cleaned,
        }

    def cleanup(self) -> None:
        reset_path(self.root)
        self.cleaned = True

    def __enter__(self) -> GroupedIntermediateDataset:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        del exc_type, exc, traceback
        self.cleanup()


def write_sorted_intermediate(
    frame: pl.LazyFrame,
    *,
    root: Path,
    sort_columns: list[str],
    descending: list[bool],
    nulls_last: list[bool] | None = None,
) -> IntermediateDataset:
    ensure_dir(root)
    identifier = uuid.uuid4().hex
    final_path = root / f"sorted-{identifier}.parquet"
    temporary_path = root / f".sorted-{identifier}.tmp.parquet"
    try:
        frame.sort(
            sort_columns,
            descending=descending,
            nulls_last=nulls_last or False,
        ).sink_parquet(temporary_path, compression="uncompressed")
        if not temporary_path.is_file() or temporary_path.stat().st_size == 0:
            raise RuntimeError(f"Intermediate writer produced an invalid file: {temporary_path}")
        os.replace(temporary_path, final_path)
        checksum = file_sha256(final_path)
        rows = int(pq.ParquetFile(final_path).metadata.num_rows)
        return IntermediateDataset(
            path=final_path,
            checksum=checksum,
            rows=rows,
            size_bytes=final_path.stat().st_size,
        )
    except BaseException:
        reset_path(temporary_path)
        reset_path(final_path)
        raise


def write_grouped_intermediate_dataset(
    frame: pl.LazyFrame,
    *,
    root: Path,
    group_keys: list[str],
    sort_columns: list[str] | None = None,
    descending: list[bool] | None = None,
    target_rows_per_part: int = 20_000,
    output_row_group_rows: int = 20_000,
) -> GroupedIntermediateDataset:
    """Write sorted complete groups to atomic dataset parts without splitting a group."""
    if not group_keys:
        raise ValueError("group_keys must not be empty for grouped intermediate output")
    target_rows = max(1, int(target_rows_per_part))
    row_group_rows = max(1, int(output_row_group_rows))
    order_columns = list(dict.fromkeys([*group_keys, *(sort_columns or [])]))
    direction_by_column = dict(zip(sort_columns or [], descending or [], strict=False))
    order_descending = [bool(direction_by_column.get(column, False)) for column in order_columns]
    ensure_dir(root)
    identifier = uuid.uuid4().hex
    final_root = root / f"grouped-{identifier}.dataset"
    staging_root = root / f".grouped-{identifier}.tmp.dataset"
    sorted_spill: IntermediateDataset | None = None
    writer: pq.ParquetWriter | None = None
    current_path: Path | None = None
    current_rows = 0
    total_rows = 0
    part_index = 0
    previous_group: tuple[Any, ...] | None = None
    part_paths: list[Path] = []

    def close_part() -> None:
        nonlocal writer, current_path, current_rows
        if writer is None or current_path is None:
            return
        writer.close()
        writer = None
        part_paths.append(current_path)
        current_path = None
        current_rows = 0

    try:
        ensure_dir(staging_root)
        sorted_spill = write_sorted_intermediate(
            frame,
            root=root,
            sort_columns=order_columns,
            descending=order_descending,
        )
        parquet = pq.ParquetFile(sorted_spill.path)
        missing = [name for name in group_keys if name not in parquet.schema_arrow.names]
        if missing:
            raise ValueError(f"group key columns are missing from intermediate input: {missing}")
        for batch in parquet.iter_batches(batch_size=row_group_rows):
            table = pa.Table.from_batches([batch])
            key_values = [table[name].to_pylist() for name in group_keys]
            start = 0
            while start < table.num_rows:
                group = tuple(values[start] for values in key_values)
                end = start + 1
                while end < table.num_rows and tuple(values[end] for values in key_values) == group:
                    end += 1
                if previous_group != group and current_rows >= target_rows:
                    close_part()
                    part_index += 1
                piece = table.slice(start, end - start)
                if writer is None:
                    current_path = staging_root / f"part-{part_index:05d}.parquet"
                    writer = pq.ParquetWriter(
                        current_path,
                        piece.schema,
                        compression=None,
                    )
                writer.write_table(piece, row_group_size=row_group_rows)
                current_rows += piece.num_rows
                total_rows += piece.num_rows
                previous_group = group
                start = end
        close_part()
        if total_rows != parquet.metadata.num_rows:
            raise RuntimeError(
                "Grouped intermediate row count mismatch: "
                f"expected={parquet.metadata.num_rows}, actual={total_rows}"
            )
        checksums = tuple(file_sha256(path) for path in part_paths)
        manifest = {
            "version": GROUPED_INTERMEDIATE_DATASET_VERSION,
            "rows": total_rows,
            "group_keys": group_keys,
            "parts": [
                {
                    "path": path.name,
                    "rows": int(pq.ParquetFile(path).metadata.num_rows),
                    "checksum": checksum,
                }
                for path, checksum in zip(part_paths, checksums, strict=True)
            ],
        }
        (staging_root / "_intermediate.manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=True, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        os.replace(staging_root, final_root)
        final_paths = tuple(final_root / path.name for path in part_paths)
        return GroupedIntermediateDataset(
            root=final_root,
            part_paths=final_paths,
            checksums=checksums,
            rows=total_rows,
            group_keys=tuple(group_keys),
        )
    except BaseException:
        if writer is not None:
            writer.close()
        reset_path(staging_root)
        reset_path(final_root)
        raise
    finally:
        if sorted_spill is not None:
            sorted_spill.cleanup()
