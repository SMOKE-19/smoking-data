from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

from smoking_data.core.exceptions import ValidationError

from .coordinates import CoordinateBatch, SourceCoordinate
from .planner import expression_column_references


@dataclass(frozen=True, slots=True)
class InvalidationTaskGroup:
    expression_names: tuple[str, ...]
    row_indices: tuple[int, ...]
    coordinates: tuple[SourceCoordinate, ...]


def group_all_coordinate_expressions(
    coordinate: CoordinateBatch,
    *,
    expression_order: Sequence[str],
    allow_empty: bool = False,
) -> InvalidationTaskGroup:
    names = tuple(expression_order)
    row_count = coordinate.selected_row_count
    if not names and not allow_empty:
        _fail(
            "incremental.state_mismatch",
            "Coordinate states do not match row × expression cardinality.",
        )
    return InvalidationTaskGroup(
        expression_names=names,
        row_indices=tuple(range(row_count)),
        coordinates=coordinate.coordinates,
    )


def write_coordinate_subset(
    path: str | Path,
    coordinates: Sequence[SourceCoordinate],
) -> Path:
    output = Path(path).expanduser().resolve()
    if not coordinates:
        _fail("sidecar.empty_coordinate", "Invalidation task selects no coordinates.")
    output.parent.mkdir(parents=True, exist_ok=True)
    batch = pa.record_batch(
        [
            pa.array([item.source_file for item in coordinates]),
            pa.array([item.row_group_id for item in coordinates], type=pa.int32()),
            pa.array(
                [item.row_offset_in_group for item in coordinates], type=pa.int32()
            ),
        ],
        names=["source_file", "row_group_id", "row_offset_in_group"],
    )
    staging = output.with_suffix(output.suffix + ".tmp")
    with staging.open("wb") as handle, ipc.new_file(handle, batch.schema) as writer:
        writer.write_batch(batch)
    staging.replace(output)
    return output


def select_projected_rows(
    coordinate: CoordinateBatch,
    group: InvalidationTaskGroup,
) -> pa.RecordBatch:
    if coordinate.projected_batch is None:
        _fail(
            "sidecar.payload_not_loaded",
            "Projected rows are unavailable for a metadata-only coordinate batch.",
        )
    selected = pc.take(
        coordinate.projected_batch,
        pa.array(group.row_indices, type=pa.int64()),
    )
    if not isinstance(selected, pa.RecordBatch):
        _fail("sidecar.invalid_coordinate", "Coordinate row selection changed batch shape.")
    return selected


def subset_expression_ir(
    document: Mapping[str, Any],
    output_names: Sequence[str],
) -> dict[str, Any]:
    expressions = [
        item
        for layer in document.get("layers") or []
        if isinstance(layer, Mapping)
        for item in layer.get("expressions") or []
        if isinstance(item, Mapping)
    ]
    by_name = {str(item.get("name") or ""): item for item in expressions}
    required: set[str] = set()

    def visit(name: str) -> None:
        if name in required:
            return
        expression = by_name.get(name)
        if expression is None:
            _fail(
                "expression.unresolved_dependency",
                "Invalidation output is absent from expression IR.",
                expression=name,
            )
        required.add(name)
        node = expression.get("expr")
        if isinstance(node, Mapping):
            for dependency in expression_column_references(node):
                if dependency in by_name:
                    visit(dependency)

    for output in output_names:
        visit(str(output))
    layers = []
    for layer in document.get("layers") or []:
        if not isinstance(layer, Mapping):
            continue
        selected = [
            dict(item)
            for item in layer.get("expressions") or []
            if isinstance(item, Mapping) and str(item.get("name") or "") in required
        ]
        if selected:
            layers.append({**dict(layer), "expressions": selected})
    return {**dict(document), "layers": layers}


def _fail(code: str, message: str, **context: object) -> None:
    raise ValidationError(message, code=code, context=context)
