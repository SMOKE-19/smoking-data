from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from smoking_data.core.types import WriteResult
from smoking_data.runtime.paths import ensure_dir, reset_path


def write_partition_dataset(
    lf: pl.LazyFrame,
    *,
    output_dir: str | Path,
    partition_column: str,
    rows_per_part: int = 20_000,
    overwrite: bool = True,
) -> WriteResult:
    out = Path(output_dir).expanduser().resolve()
    if overwrite:
        reset_path(out)
    ensure_dir(out)

    df = lf.collect()
    if partition_column not in df.columns:
        raise ValueError(f"Partition column not found: {partition_column}")
    output_files: list[Path] = []
    total_rows = df.height
    partitions = 0
    for partition_value in df.get_column(partition_column).unique().sort().to_list():
        partitions += 1
        partition_df = df.filter(pl.col(partition_column) == partition_value)
        partition_dir = ensure_dir(out / _safe_part(str(partition_value)))
        for index, offset in enumerate(range(0, partition_df.height, rows_per_part)):
            part = partition_df.slice(offset, rows_per_part)
            part_path = partition_dir / f"part-{index:05d}.parquet"
            part.write_parquet(part_path)
            output_files.append(part_path)
    return WriteResult(
        output_dir=out,
        output_files=output_files,
        rows=total_rows,
        partitions=partitions,
    )


def _safe_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.=-]+", "_", value.strip())
    return cleaned or "__null__"
