from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class OperationKind(StrEnum):
    FILTER = "filter"
    TYPE_CAST = "type_cast"
    ADD_CALC = "add_calc"
    REFERENCE_REPLACE = "reference_replace"
    LIST_RESTORE = "list_restore"
    EXPAND_LIST_ROWS = "expand_list_rows"
    COMPACT_LIST_ROWS = "compact_list_rows"
    BUILD_SIDECAR = "build_sidecar"
    ACTIVE_ROW_SELECTION = "active_row_selection"
    MATERIALIZE = "materialize"
    SORT_FIRST = "sort_first"
    PROJECTION = "projection"
    INCLUDE_COLUMNS = "include_columns"
    EXCLUDE_COLUMNS = "exclude_columns"
    RENAME_COLUMNS = "rename_columns"
    DATA_ASSERTION = "data_assertion"
    UNPIVOT = "unpivot"
    UNNEST = "unnest"
    PIVOT = "pivot"
    JOIN = "join"
    WRITE_DATASET = "write_dataset"
    # Internal aliases retained only while the mature physical kernels are lowered.
    PARTITION_WRITE = "partition_write"


class CardinalityEffect(StrEnum):
    PRESERVE = "preserve"
    REDUCE = "reduce"
    EXPAND_OR_REDUCE = "expand_or_reduce"
    MANY_TO_MANY = "many_to_many"


class MissingColumnPolicy(StrEnum):
    ERROR = "error"
    INSERT_NULL = "insert_null"


class NullSemantics(StrEnum):
    PROPAGATE = "propagate"
    THREE_VALUED_FILTER = "three_valued_filter"
    GROUP_KEY = "group_key"
    OP_DEFINED = "op_defined"


@dataclass(frozen=True, slots=True)
class ColumnContract:
    name: str
    dtype: str | None = None
    nullable: bool | None = None
    missing_policy: MissingColumnPolicy = MissingColumnPolicy.ERROR

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Column contract name must not be empty.")


@dataclass(frozen=True, slots=True)
class OperationProperties:
    row_preserving: bool
    cardinality_effect: CardinalityEffect
    deterministic: bool
    order_sensitive: bool
    schema_changing: bool = False
    requires_complete_group: bool = False
    side_effect: bool = False
    pushdown_safe: bool = False
    total: bool = True
    null_semantics: NullSemantics = NullSemantics.PROPAGATE
    error_semantics: str = "raise"
    backend_support: tuple[str, ...] = ("rust",)
    pushdown_capabilities: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class OperationSpec:
    operation_id: str
    kind: OperationKind
    config: dict[str, Any]
    input_columns: tuple[str, ...] = ()
    input_contracts: tuple[ColumnContract, ...] = ()
    output_columns: tuple[ColumnContract, ...] = ()
    alias_lineage: dict[str, tuple[str, ...]] = field(default_factory=dict)
    group_keys: tuple[str, ...] = ()
    partition_keys: tuple[str, ...] = ()
    ordering: tuple[tuple[str, str], ...] = ()
    properties: OperationProperties = field(
        default_factory=lambda: OperationProperties(
            row_preserving=True,
            cardinality_effect=CardinalityEffect.PRESERVE,
            deterministic=True,
            order_sensitive=False,
        )
    )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


OPERATION_PROPERTIES: dict[OperationKind, OperationProperties] = {
    OperationKind.FILTER: OperationProperties(
        row_preserving=False,
        cardinality_effect=CardinalityEffect.REDUCE,
        deterministic=True,
        order_sensitive=False,
        pushdown_safe=True,
        null_semantics=NullSemantics.THREE_VALUED_FILTER,
        pushdown_capabilities=("source_projection", "predicate"),
    ),
    OperationKind.TYPE_CAST: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        schema_changing=True,
        total=False,
        pushdown_capabilities=("selector_expression",),
    ),
    OperationKind.ADD_CALC: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        schema_changing=True,
        pushdown_capabilities=("selector_expression",),
    ),
    OperationKind.REFERENCE_REPLACE: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        schema_changing=True,
        null_semantics=NullSemantics.OP_DEFINED,
    ),
    OperationKind.LIST_RESTORE: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=True,
        schema_changing=True,
        requires_complete_group=True,
    ),
    OperationKind.EXPAND_LIST_ROWS: OperationProperties(
        row_preserving=False,
        cardinality_effect=CardinalityEffect.EXPAND_OR_REDUCE,
        deterministic=True,
        order_sensitive=True,
        schema_changing=True,
        null_semantics=NullSemantics.OP_DEFINED,
    ),
    OperationKind.COMPACT_LIST_ROWS: OperationProperties(
        row_preserving=False,
        cardinality_effect=CardinalityEffect.EXPAND_OR_REDUCE,
        deterministic=True,
        order_sensitive=True,
        schema_changing=True,
        requires_complete_group=True,
        null_semantics=NullSemantics.OP_DEFINED,
    ),
    OperationKind.BUILD_SIDECAR: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        side_effect=True,
        pushdown_capabilities=("source_projection", "coordinate_index"),
    ),
    OperationKind.SORT_FIRST: OperationProperties(
        row_preserving=False,
        cardinality_effect=CardinalityEffect.REDUCE,
        deterministic=True,
        order_sensitive=True,
        requires_complete_group=True,
        null_semantics=NullSemantics.GROUP_KEY,
    ),
    OperationKind.ACTIVE_ROW_SELECTION: OperationProperties(
        row_preserving=False,
        cardinality_effect=CardinalityEffect.REDUCE,
        deterministic=True,
        order_sensitive=True,
        requires_complete_group=True,
        null_semantics=NullSemantics.GROUP_KEY,
        pushdown_capabilities=("selector_expression", "coordinate_snapshot"),
    ),
    OperationKind.MATERIALIZE: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=True,
        requires_complete_group=True,
        side_effect=True,
        pushdown_capabilities=("coordinate_snapshot", "selective_payload_read"),
    ),
    OperationKind.PROJECTION: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        schema_changing=True,
        pushdown_safe=True,
        pushdown_capabilities=("source_projection",),
    ),
    OperationKind.INCLUDE_COLUMNS: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        schema_changing=True,
        pushdown_safe=True,
        pushdown_capabilities=("source_projection",),
    ),
    OperationKind.EXCLUDE_COLUMNS: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        schema_changing=True,
        pushdown_safe=True,
        pushdown_capabilities=("source_projection",),
    ),
    OperationKind.RENAME_COLUMNS: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        schema_changing=True,
    ),
    OperationKind.DATA_ASSERTION: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        requires_complete_group=True,
        null_semantics=NullSemantics.OP_DEFINED,
    ),
    OperationKind.UNPIVOT: OperationProperties(
        row_preserving=False,
        cardinality_effect=CardinalityEffect.EXPAND_OR_REDUCE,
        deterministic=True,
        order_sensitive=True,
        schema_changing=True,
        requires_complete_group=False,
        null_semantics=NullSemantics.OP_DEFINED,
    ),
    OperationKind.UNNEST: OperationProperties(
        row_preserving=False,
        cardinality_effect=CardinalityEffect.EXPAND_OR_REDUCE,
        deterministic=True,
        order_sensitive=True,
        schema_changing=True,
        null_semantics=NullSemantics.OP_DEFINED,
    ),
    OperationKind.PIVOT: OperationProperties(
        row_preserving=False,
        cardinality_effect=CardinalityEffect.EXPAND_OR_REDUCE,
        deterministic=True,
        order_sensitive=True,
        schema_changing=True,
        requires_complete_group=True,
        null_semantics=NullSemantics.OP_DEFINED,
    ),
    OperationKind.JOIN: OperationProperties(
        row_preserving=False,
        cardinality_effect=CardinalityEffect.MANY_TO_MANY,
        deterministic=True,
        order_sensitive=False,
        schema_changing=True,
        requires_complete_group=True,
        null_semantics=NullSemantics.OP_DEFINED,
    ),
    OperationKind.PARTITION_WRITE: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        side_effect=True,
    ),
    OperationKind.WRITE_DATASET: OperationProperties(
        row_preserving=True,
        cardinality_effect=CardinalityEffect.PRESERVE,
        deterministic=True,
        order_sensitive=False,
        side_effect=True,
    ),
}
