from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq


@dataclass(frozen=True, slots=True)
class ColumnChunkProfile:
    name: str
    compressed_bytes: int
    uncompressed_bytes: int
    value_count: int


@dataclass(frozen=True, slots=True)
class RowGroupProfile:
    row_group_id: int
    rows: int
    compressed_bytes: int
    uncompressed_bytes: int
    columns: tuple[ColumnChunkProfile, ...]


@dataclass(frozen=True, slots=True)
class ParquetFileProfile:
    path: Path
    file_size_bytes: int
    rows: int
    row_groups: tuple[RowGroupProfile, ...]

    def estimated_compressed_bytes(
        self,
        *,
        row_group_ids: tuple[int, ...] | None = None,
        columns: tuple[str, ...] | None = None,
    ) -> int:
        selected_groups = (
            self.row_groups
            if row_group_ids is None
            else tuple(group for group in self.row_groups if group.row_group_id in row_group_ids)
        )
        if columns is None:
            return sum(group.compressed_bytes for group in selected_groups)
        selected = set(columns)
        return sum(
            column.compressed_bytes
            for group in selected_groups
            for column in group.columns
            if column.name in selected
        )

    def estimated_uncompressed_bytes(
        self,
        *,
        row_group_ids: tuple[int, ...] | None = None,
        columns: tuple[str, ...] | None = None,
    ) -> int:
        selected_groups = (
            self.row_groups
            if row_group_ids is None
            else tuple(group for group in self.row_groups if group.row_group_id in row_group_ids)
        )
        if columns is None:
            return sum(group.uncompressed_bytes for group in selected_groups)
        selected = set(columns)
        return sum(
            column.uncompressed_bytes
            for group in selected_groups
            for column in group.columns
            if column.name in selected
        )


def profile_parquet_file(path: str | Path) -> ParquetFileProfile:
    source = Path(path).expanduser().resolve()
    parquet = pq.ParquetFile(source)
    metadata = parquet.metadata
    row_groups: list[RowGroupProfile] = []
    for row_group_id in range(metadata.num_row_groups):
        group = metadata.row_group(row_group_id)
        columns = tuple(
            ColumnChunkProfile(
                name=str(group.column(index).path_in_schema),
                compressed_bytes=int(group.column(index).total_compressed_size),
                uncompressed_bytes=int(group.column(index).total_uncompressed_size),
                value_count=int(group.column(index).num_values),
            )
            for index in range(group.num_columns)
        )
        row_groups.append(
            RowGroupProfile(
                row_group_id=row_group_id,
                rows=int(group.num_rows),
                compressed_bytes=sum(column.compressed_bytes for column in columns),
                uncompressed_bytes=sum(column.uncompressed_bytes for column in columns),
                columns=columns,
            )
        )
    return ParquetFileProfile(
        path=source,
        file_size_bytes=source.stat().st_size,
        rows=int(metadata.num_rows),
        row_groups=tuple(row_groups),
    )


def profile_parquet_files(
    paths: list[str | Path],
) -> dict[str, ParquetFileProfile]:
    return {
        str(profile.path): profile for profile in (profile_parquet_file(path) for path in paths)
    }
