from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from smoking_data.ops.coordinates import (
    SOURCE_FILE_COLUMN,
    SOURCE_ROW_GROUP_COLUMN,
    SOURCE_ROW_INDEX_COLUMN,
)
from smoking_data.runtime.selector_ipc import read_sidecar_frame


def plan_coordinate_page_ranges(
    manifest_path: str | Path,
    coordinate_path: str | Path,
    *,
    project_root: Path,
    projected_columns: list[str] | None = None,
    merge_gap_bytes: int = 64 * 1024,
    max_range_bytes: int = 8 * 1024 * 1024,
    max_ranges: int = 512,
    minimum_range_savings_ratio: float = 0.0,
) -> dict[str, Any]:
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifacts = dict(manifest.get("artifacts") or {})
    if not manifest.get("capabilities", {}).get("page_index") or "pages" not in artifacts:
        return {"read_path": "row_group_selected", "reason": "page_index_unavailable"}
    coordinates = read_sidecar_frame(Path(coordinate_path))
    coordinates = coordinates.with_columns(
        pl.col(SOURCE_FILE_COLUMN)
        .map_elements(
            lambda value: _portable_path(Path(str(value)).resolve(), project_root),
            return_dtype=pl.String,
        )
        .alias("source_file"),
        pl.col(SOURCE_ROW_GROUP_COLUMN).cast(pl.Int64).alias("row_group_id"),
    )
    selected_sources = coordinates.get_column("source_file").unique().to_list()
    selected_groups = coordinates.get_column("row_group_id").unique().to_list()
    row_groups = (
        pl.scan_parquet(manifest_path.parent / str(artifacts["row_groups"]))
        .filter(
            pl.col("source_file").is_in(selected_sources)
            & pl.col("row_group_id").is_in(selected_groups)
        )
        .collect()
        .unique(subset=["source_file", "row_group_id", "column_path"])
    )
    pages = (
        pl.scan_parquet(manifest_path.parent / str(artifacts["pages"]))
        .filter(
            pl.col("source_file").is_in(selected_sources)
            & pl.col("row_group_id").is_in(selected_groups)
        )
        .collect()
        .unique(subset=["source_file", "row_group_id", "column_path", "page_ordinal"])
    )
    columns = set(projected_columns or pages.get_column("column_path").unique().to_list())
    starts = row_groups.select(["source_file", "row_group_id", "first_row_index"]).unique()
    selected_coordinates = (
        coordinates.join(starts, on=["source_file", "row_group_id"], how="inner")
        .with_columns(
            (pl.col(SOURCE_ROW_INDEX_COLUMN) - pl.col("first_row_index")).alias("row_offset")
        )
        .select(["source_file", "row_group_id", "row_offset"])
        .unique()
    )
    matching_pages = (
        selected_coordinates.join(
            pages.filter(pl.col("column_path").is_in(columns)),
            on=["source_file", "row_group_id"],
            how="inner",
        )
        .filter(
            (pl.col("first_row_index") <= pl.col("row_offset"))
            & (pl.col("first_row_index") + pl.col("row_count") > pl.col("row_offset"))
        )
        .unique(subset=["source_file", "row_group_id", "column_path", "page_ordinal"])
    )
    selected: set[tuple[str, int, int]] = set()
    for page in matching_pages.iter_rows(named=True):
        source_file = str(page["source_file"])
        selected.add(
            (
                source_file,
                int(page["byte_offset"]),
                int(page["compressed_length"]),
            )
        )
        dictionary_offset = page.get("dictionary_page_offset")
        dictionary_length = page.get("dictionary_page_length")
        if dictionary_offset is not None and dictionary_length:
            selected.add((source_file, int(dictionary_offset), int(dictionary_length)))

    merged = _merge_ranges(
        selected,
        merge_gap_bytes=max(0, merge_gap_bytes),
        max_range_bytes=max(1, max_range_bytes),
    )
    range_bytes = sum(item[2] for item in merged)
    selected_row_groups = set(coordinates.select(["source_file", "row_group_id"]).iter_rows())
    row_group_bytes = sum(
        int(row["compressed_bytes"])
        for row in row_groups.filter(pl.col("column_path").is_in(columns)).iter_rows(named=True)
        if (str(row["source_file"]), int(row["row_group_id"])) in selected_row_groups
    )
    required_maximum = row_group_bytes * (1.0 - minimum_range_savings_ratio)
    use_ranges = bool(merged) and len(merged) <= max_ranges and range_bytes < required_maximum
    return {
        "read_path": "range_indexed" if use_ranges else "row_group_selected",
        "reason": "lower_estimated_bytes" if use_ranges else "range_cost_not_lower",
        "page_count": len(selected),
        "range_count": len(merged),
        "range_bytes": range_bytes,
        "row_group_bytes": row_group_bytes,
        "minimum_range_savings_ratio": minimum_range_savings_ratio,
        "ranges": [
            {"source_file": source_file, "offset": offset, "length": length}
            for source_file, offset, length in merged
        ],
    }


def _merge_ranges(
    ranges: set[tuple[str, int, int]], *, merge_gap_bytes: int, max_range_bytes: int
) -> list[tuple[str, int, int]]:
    merged: list[tuple[str, int, int]] = []
    for source_file, offset, length in sorted(ranges):
        if not merged or merged[-1][0] != source_file:
            merged.append((source_file, offset, length))
            continue
        previous_file, previous_offset, previous_length = merged[-1]
        previous_end = previous_offset + previous_length
        current_end = offset + length
        combined_end = max(previous_end, current_end)
        if (
            offset - previous_end <= merge_gap_bytes
            and combined_end - previous_offset <= max_range_bytes
        ):
            merged[-1] = (previous_file, previous_offset, combined_end - previous_offset)
        else:
            merged.append((source_file, offset, length))
    return merged


def _portable_path(path: Path, project_root: Path) -> str:
    try:
        return path.relative_to(project_root.resolve()).as_posix()
    except ValueError:
        import hashlib

        digest = hashlib.sha256(str(path.parent).encode()).hexdigest()[:12]
        return f"external/{digest}/{path.name}"
