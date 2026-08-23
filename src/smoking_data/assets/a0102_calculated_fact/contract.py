from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from smoking_data.core.exceptions import ValidationError

LONG_FACT_CONTRACT_VERSION = "long_fact_v1"

VALUE_LANES: tuple[tuple[str, str, pa.DataType], ...] = (
    ("boolean", "_sd_value_boolean", pa.bool_()),
    ("int64", "_sd_value_int64", pa.int64()),
    ("float64", "_sd_value_float64", pa.float64()),
    ("decimal", "_sd_value_decimal", pa.decimal128(38, 10)),
    ("string", "_sd_value_string", pa.string()),
    ("datetime", "_sd_value_datetime", pa.timestamp("us")),
    ("duration", "_sd_value_duration", pa.duration("us")),
    ("list_boolean", "_sd_value_list_boolean", pa.list_(pa.bool_())),
    ("list_int64", "_sd_value_list_int64", pa.list_(pa.int64())),
    ("list_float64", "_sd_value_list_float64", pa.list_(pa.float64())),
    ("list_decimal", "_sd_value_list_decimal", pa.list_(pa.decimal128(38, 10))),
    ("list_string", "_sd_value_list_string", pa.list_(pa.string())),
    ("list_datetime", "_sd_value_list_datetime", pa.list_(pa.timestamp("us"))),
    ("list_duration", "_sd_value_list_duration", pa.list_(pa.duration("us"))),
)


def long_fact_schema(identity_fields: Sequence[pa.Field]) -> pa.Schema:
    names = [field.name for field in identity_fields]
    if not names or len(set(names)) != len(names):
        _fail(
            "incremental.invalid_identity",
            "Long Fact identity fields must be non-empty and unique.",
            identity_columns=names,
        )
    reserved = set(reserved_field_names())
    collisions = sorted(reserved.intersection(names))
    if collisions:
        _fail(
            "incremental.invalid_identity",
            "Identity columns must not use Long Fact reserved field names.",
            columns=collisions,
        )
    fields = [*identity_fields]
    fields.extend(
        (
            pa.field("_sd_column_name", pa.string(), nullable=False),
            pa.field("_sd_value_type", pa.string(), nullable=False),
        )
    )
    fields.extend(pa.field(name, dtype) for _, name, dtype in VALUE_LANES)
    fields.extend(
        (
            pa.field("_sd_generation_seq", pa.int64(), nullable=False),
            pa.field("_sd_expression_hash", pa.string(), nullable=False),
            pa.field("_sd_binding_hash", pa.string(), nullable=False),
            pa.field("_sd_source_fingerprint", pa.string(), nullable=False),
            pa.field("_sd_calculated_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("_sd_is_deleted", pa.bool_(), nullable=False),
        )
    )
    return pa.schema(
        fields,
        metadata={b"smoking_data.contract": LONG_FACT_CONTRACT_VERSION.encode("ascii")},
    )


def reserved_field_names() -> tuple[str, ...]:
    return (
        "_sd_column_name",
        "_sd_value_type",
        *(name for _, name, _ in VALUE_LANES),
        "_sd_generation_seq",
        "_sd_expression_hash",
        "_sd_binding_hash",
        "_sd_source_fingerprint",
        "_sd_calculated_at",
        "_sd_is_deleted",
    )


def validate_long_fact_batch(
    batch: pa.RecordBatch,
    *,
    identity_columns: Sequence[str],
) -> None:
    missing_identities = [name for name in identity_columns if name not in batch.schema.names]
    if missing_identities:
        _fail(
            "incremental.invalid_identity",
            "Long Fact batch is missing identity columns.",
            columns=missing_identities,
        )
    expected = long_fact_schema(
        [batch.schema.field(name) for name in identity_columns]
    )
    if batch.schema != expected:
        _fail(
            "long_fact.schema_mismatch",
            "Long Fact batch schema does not match long_fact_v1.",
            expected=str(expected),
            actual=str(batch.schema),
        )
    type_values = _column(batch, "_sd_value_type").to_pylist()
    deleted_values = _column(batch, "_sd_is_deleted").to_pylist()
    lanes = {
        tag: _column(batch, name) for tag, name, _ in VALUE_LANES
    }
    invalid: list[dict[str, object]] = []
    for row_index, (value_type, is_deleted) in enumerate(
        zip(type_values, deleted_values, strict=True)
    ):
        non_null = [tag for tag, array in lanes.items() if array[row_index].is_valid]
        # An active calculated value may itself be null. Its declared type still
        # identifies the lane, while every lane remains null. Tombstones are
        # distinguished by _sd_is_deleted rather than by null payload alone.
        valid = (is_deleted and not non_null) or (
            not is_deleted
            and value_type in lanes
            and non_null in ([], [value_type])
        )
        if not valid:
            invalid.append(
                {
                    "row_index": row_index,
                    "value_type": value_type,
                    "is_deleted": is_deleted,
                    "non_null_lanes": non_null,
                }
            )
            if len(invalid) == 5:
                break
    if invalid:
        _fail(
            "long_fact.invalid_value_lane",
            "Each active Long Fact row must use only its declared typed lane.",
            samples=invalid,
        )


def _column(batch: pa.RecordBatch, name: str) -> pa.Array:
    return batch.column(batch.schema.get_field_index(name))


def _fail(code: str, message: str, **context: object) -> None:
    raise ValidationError(message, code=code, context=context)
