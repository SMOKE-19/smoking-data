from __future__ import annotations

from bisect import bisect_right
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

SOURCE_FILE_COLUMN = "__source_file"
SOURCE_ROW_INDEX_COLUMN = "__source_row_index"
SOURCE_ROW_GROUP_COLUMN = "__source_row_group"
ACTIVE_ORDER_COLUMN = "__active_order"
PART_INDEX_COLUMN = "__part_index"


def attach_row_group_ids(frame: pl.DataFrame, source_file: Path) -> pl.DataFrame:
    starts = row_group_start_offsets(source_file)
    row_group_ids = [
        max(0, bisect_right(starts, int(row_index)) - 1)
        for row_index in frame.get_column(SOURCE_ROW_INDEX_COLUMN)
    ]
    return frame.with_columns(
        pl.Series(SOURCE_ROW_GROUP_COLUMN, row_group_ids, dtype=pl.Int64),
        pl.lit(str(source_file)).alias(SOURCE_FILE_COLUMN),
    )


def row_group_start_offsets(source_file: Path) -> list[int]:
    return _row_group_start_offsets(pq.ParquetFile(source_file).metadata)


def _row_group_start_offsets(metadata) -> list[int]:
    starts: list[int] = []
    offset = 0
    for row_group_id in range(metadata.num_row_groups):
        starts.append(offset)
        offset += int(metadata.row_group(row_group_id).num_rows)
    return starts


def read_source_row_group(source_file: Path, row_group_id: int) -> pl.DataFrame:
    parquet_file = pq.ParquetFile(source_file)
    starts = _row_group_start_offsets(parquet_file.metadata)
    if row_group_id < 0 or row_group_id >= len(starts):
        raise ValueError(f"Invalid row group {row_group_id} for {source_file}")
    table = parquet_file.read_row_group(row_group_id)
    return pl.from_arrow(table).with_row_index(
        SOURCE_ROW_INDEX_COLUMN,
        offset=starts[row_group_id],
    )


def write_rust_coordinate_file(coordinates: pl.DataFrame, output_path: Path) -> Path:
    starts_cache: dict[str, list[int]] = {}
    rows: list[dict[str, object]] = []
    for source_file, row_group_id, row_index, active_order in coordinates.select(
        [SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN, SOURCE_ROW_INDEX_COLUMN, ACTIVE_ORDER_COLUMN]
    ).iter_rows():
        source_text = str(source_file)
        starts = starts_cache.get(source_text)
        if starts is None:
            starts = row_group_start_offsets(Path(source_text))
            starts_cache[source_text] = starts
        group_id = int(row_group_id)
        absolute_index = int(row_index)
        rows.append(
            {
                "source_file": source_text,
                "row_group_id": group_id,
                "row_index": absolute_index,
                "row_offset_in_group": absolute_index - starts[group_id],
                "active_order": int(active_order),
                "planner_chunk_id": 0,
            }
        )
    frame = pl.DataFrame(
        rows,
        schema={
            "source_file": pl.String,
            "row_group_id": pl.Int64,
            "row_index": pl.Int64,
            "row_offset_in_group": pl.Int64,
            "active_order": pl.Int64,
            "planner_chunk_id": pl.Int64,
        },
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = frame.to_arrow()
    with pa.OSFile(str(output_path), "wb") as sink:
        with pa_ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
    return output_path
