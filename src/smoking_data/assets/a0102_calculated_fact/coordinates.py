from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from smoking_data.core.exceptions import ValidationError

from .planning import CalculatedFactRunPlan


@dataclass(frozen=True, slots=True)
class CoordinateBatch:
    projected_batch: pa.RecordBatch
    coordinates: tuple[SourceCoordinate, ...]
    source_file_count: int
    row_group_count: int
    source_schema_hash: str
    source_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True, order=True)
class SourceCoordinate:
    source_file: str
    row_group_id: int
    row_offset_in_group: int


def load_coordinate_batch(
    coordinate_path: str | Path,
    plan: CalculatedFactRunPlan,
) -> CoordinateBatch:
    path = Path(coordinate_path).expanduser().resolve()
    if not path.is_file():
        _fail("sidecar.missing_coordinate", "Coordinate IPC file does not exist.", path=str(path))
    groups: dict[tuple[str, int], set[int]] = defaultdict(set)
    try:
        with path.open("rb") as handle:
            reader = ipc.open_file(handle)
            for batch_index in range(reader.num_record_batches):
                batch = reader.get_batch(batch_index)
                required = {"source_file", "row_group_id", "row_offset_in_group"}
                missing = sorted(required.difference(batch.schema.names))
                if missing:
                    _fail(
                        "sidecar.invalid_coordinate",
                        "Coordinate IPC is missing required columns.",
                        columns=missing,
                    )
                source_files = _column(batch, "source_file").to_pylist()
                row_groups = _column(batch, "row_group_id").to_pylist()
                offsets = _column(batch, "row_offset_in_group").to_pylist()
                for source_file, row_group, offset in zip(
                    source_files, row_groups, offsets, strict=True
                ):
                    if source_file is None or row_group is None or offset is None:
                        _fail(
                            "sidecar.invalid_coordinate",
                            "Coordinate values must not be null.",
                        )
                    row_group = int(row_group)
                    offset = int(offset)
                    if row_group < 0 or offset < 0:
                        _fail(
                            "sidecar.invalid_coordinate",
                            "Coordinate row group and offset must be non-negative.",
                        )
                    groups[(str(source_file), row_group)].add(offset)
    except ValidationError:
        raise
    except Exception as exc:
        _fail(
            "sidecar.invalid_coordinate",
            "Coordinate IPC could not be read.",
            path=str(path),
            reason=str(exc),
        )
    batches: list[pa.RecordBatch] = []
    execution_coordinates: list[SourceCoordinate] = []
    source_files = {source_file for source_file, _row_group in groups}
    if len(source_files) != 1:
        _fail(
            "sidecar.coordinate_multiple_source_files",
            "0102 coordinate must reference exactly one source segment.",
            source_files=sorted(source_files),
        )
    source_path = Path(next(iter(source_files))).expanduser().resolve()
    if not source_path.is_file():
        _fail(
            "sidecar.source_missing",
            "Coordinate references a missing Parquet file.",
            source_file=str(source_path),
        )
    source_schema = pq.ParquetFile(source_path).schema_arrow
    available_columns = set(source_schema.names)
    required_identity = set(plan.spec.identity_columns).union(
        getattr(plan.spec, "partition_by", ())
    )
    missing_identity = sorted(required_identity.difference(available_columns))
    if missing_identity:
        _fail(
            "incremental.invalid_identity",
            "Source segment is missing required identity or partition columns.",
            columns=missing_identity,
            source_file=str(source_path),
        )
    projection = [
        name for name in plan.binding.source_projection if name in available_columns
    ]
    for (source_file, row_group), offsets in sorted(groups.items()):
        source_path = Path(source_file).expanduser().resolve()
        if not source_path.is_file():
            _fail(
                "sidecar.source_missing",
                "Coordinate references a missing Parquet file.",
                source_file=str(source_path),
            )
        parquet = pq.ParquetFile(source_path)
        if row_group >= parquet.num_row_groups:
            _fail(
                "sidecar.coordinate_out_of_bounds",
                "Coordinate row group exceeds Parquet metadata.",
                source_file=str(source_path),
                row_group_id=row_group,
            )
        row_count = parquet.metadata.row_group(row_group).num_rows
        ordered_offsets = sorted(offsets)
        if ordered_offsets and ordered_offsets[-1] >= row_count:
            _fail(
                "sidecar.coordinate_out_of_bounds",
                "Coordinate row offset exceeds its Parquet row group.",
                source_file=str(source_path),
                row_group_id=row_group,
                row_offset=ordered_offsets[-1],
                row_group_rows=row_count,
            )
        table = parquet.read_row_group(row_group, columns=projection)
        selected = pc.take(table, pa.array(ordered_offsets, type=pa.int64()))
        batches.extend(selected.to_batches(max_chunksize=max(1, len(ordered_offsets))))
        execution_coordinates.extend(
            SourceCoordinate(str(source_path), row_group, offset)
            for offset in ordered_offsets
        )
    if not batches:
        _fail("sidecar.empty_coordinate", "Coordinate IPC selects no source rows.")
    try:
        projected = pa.concat_batches(batches)
    except pa.ArrowInvalid as exc:
        _fail(
            "upstream.schema_mismatch",
            "Projected coordinate batches have incompatible schemas.",
            reason=str(exc),
        )
    return CoordinateBatch(
        projected_batch=projected,
        coordinates=tuple(execution_coordinates),
        source_file_count=len({item[0] for item in groups}),
        row_group_count=len(groups),
        source_schema_hash=hashlib.sha256(str(source_schema).encode()).hexdigest(),
        source_columns=tuple(source_schema.names),
    )


def _column(batch: pa.RecordBatch, name: str) -> pa.Array:
    return batch.column(batch.schema.get_field_index(name))


def _fail(code: str, message: str, **context: object) -> None:
    raise ValidationError(message, code=code, context=context)
