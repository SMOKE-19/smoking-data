from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from smoking_data.runtime.paths import reset_path


@contextmanager
def open_ipc_file(path: Path) -> Iterator[ipc.RecordBatchFileReader]:
    """Open a completed Arrow IPC File through a read-only memory map."""
    source = pa.memory_map(str(path), "r")
    try:
        yield ipc.open_file(source)
    finally:
        source.close()


def ipc_file_stats(path: Path) -> dict[str, object]:
    with open_ipc_file(path) as reader:
        rows = sum(reader.get_batch(index).num_rows for index in range(reader.num_record_batches))
        return {
            "rows": rows,
            "record_batches": reader.num_record_batches,
            "schema": reader.schema,
            "size_bytes": path.stat().st_size,
        }


def ipc_file_is_valid(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size <= 0:
        return False
    try:
        ipc_file_stats(path)
    except (OSError, ValueError, pa.ArrowException):
        return False
    return True


def read_ipc_frame(path: Path, *, batch_indices: Sequence[int] | None = None) -> pl.DataFrame:
    with open_ipc_file(path) as reader:
        indices = (
            range(reader.num_record_batches)
            if batch_indices is None
            else [int(index) for index in batch_indices]
        )
        batches = [reader.get_batch(index) for index in indices]
        schema = reader.schema
        if not batches:
            return pl.from_arrow(pa.Table.from_batches([], schema=schema))
        return pl.from_arrow(pa.Table.from_batches(batches, schema=schema))


def read_sidecar_frame(path: Path) -> pl.DataFrame:
    return read_ipc_frame(path) if path.suffix == ".arrow" else pl.read_parquet(path)


def scan_sidecar(path: Path) -> pl.LazyFrame:
    return pl.scan_ipc(path) if path.suffix == ".arrow" else pl.scan_parquet(path)


def sidecar_schema(path: Path) -> dict[str, pl.DataType]:
    if path.suffix == ".arrow":
        return read_ipc_frame(path, batch_indices=[]).schema
    return pl.read_parquet_schema(path)


def sidecar_rows(path: Path) -> int:
    if path.suffix == ".arrow":
        return int(ipc_file_stats(path)["rows"])
    return int(pq.ParquetFile(path).metadata.num_rows)


def write_ipc_frame_atomic(
    frame: pl.DataFrame,
    path: Path,
    *,
    max_chunksize: int = 65_536,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    backup = path.with_name(f".{path.name}.{os.getpid()}.backup")
    moved_existing = False
    try:
        table = frame.to_arrow()
        with pa.OSFile(str(temporary), "wb") as sink:
            with ipc.new_file(sink, table.schema) as writer:
                for batch in table.to_batches(max_chunksize=max(1, int(max_chunksize))):
                    writer.write_batch(batch)
        ipc_file_stats(temporary)
        if path.exists():
            os.replace(path, backup)
            moved_existing = True
        try:
            os.replace(temporary, path)
        except BaseException:
            if moved_existing and backup.exists() and not path.exists():
                os.replace(backup, path)
            raise
        reset_path(backup)
        return path
    finally:
        reset_path(temporary)


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
