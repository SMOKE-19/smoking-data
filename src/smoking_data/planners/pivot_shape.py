from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any

import polars as pl

PIVOT_SHAPE_PROFILE_VERSION = "smoking-data.pivot-shape-profile.v1"
DEFAULT_SAMPLE_ROWS = 200_000
_HASH_SEED = 30_101
_FIXED_DTYPE_BYTES = {
    "BOOLEAN": 1,
    "INT8": 1,
    "UINT8": 1,
    "INT16": 2,
    "UINT16": 2,
    "INT32": 4,
    "UINT32": 4,
    "FLOAT32": 4,
    "DATE": 4,
    "INT64": 8,
    "UINT64": 8,
    "FLOAT64": 8,
    "DATETIME": 8,
    "DURATION": 8,
    "TIME": 8,
    "DECIMAL": 16,
}


def build_pivot_shape_profile(
    active_snapshot: pl.DataFrame,
    pivot: Mapping[str, Any] | None,
    *,
    sample_limit: int = DEFAULT_SAMPLE_ROWS,
) -> dict[str, Any]:
    """Estimate pivot output shape without changing the physical plan."""

    pivot_config = dict(pivot or {})
    selected_input_rows = active_snapshot.height
    if not bool(pivot_config.get("enabled", False)):
        return {
            "schema_version": PIVOT_SHAPE_PROFILE_VERSION,
            "enabled": False,
            "estimator_mode": "not_applicable",
            "selected_input_rows": selected_input_rows,
        }

    row_keys = _string_items(pivot_config.get("row_keys"))
    column_keys = _string_items(pivot_config.get("column_keys"))
    value_keys = _value_specs(pivot_config.get("value_keys"))
    values_without_column = _value_specs(pivot_config.get("value_keys_without_column"))
    required_columns = list(
        dict.fromkeys(
            [
                *row_keys,
                *column_keys,
                *(item["source_column"] for item in [*value_keys, *values_without_column]),
            ]
        )
    )
    missing_columns = [column for column in required_columns if column not in active_snapshot]
    signature = _pivot_signature(
        pivot_config,
        row_keys=row_keys,
        column_keys=column_keys,
        value_keys=value_keys,
        values_without_column=values_without_column,
    )
    base: dict[str, Any] = {
        "schema_version": PIVOT_SHAPE_PROFILE_VERSION,
        "enabled": True,
        "estimator_mode": "bounded_active_snapshot",
        "pivot_signature": signature,
        "selected_input_rows": selected_input_rows,
        "row_keys": row_keys,
        "column_keys": column_keys,
        "value_keys": value_keys,
        "value_keys_without_column": values_without_column,
        "required_columns": required_columns,
        "missing_columns": missing_columns,
    }
    if missing_columns:
        return {
            **base,
            "sampled_rows": 0,
            "sample_fraction": 0.0,
            "confidence": 0.0,
            "estimate_status": "missing_columns",
            "assumptions": [
                "The estimator is report-only; missing planner columns do not fail the run."
            ],
        }

    effective_sample_limit = max(1, int(sample_limit))
    sample = _bounded_sample(active_snapshot, limit=effective_sample_limit)
    sampled_rows = sample.height
    sample_fraction = sampled_rows / selected_input_rows if selected_input_rows else 1.0
    row_cardinality = _approx_cardinality(active_snapshot, row_keys)
    column_cardinality = _approx_cardinality(active_snapshot, column_keys)
    cell_cardinality = _approx_cardinality(active_snapshot, [*row_keys, *column_keys])
    group_distribution = _group_distribution(
        sample,
        row_keys=row_keys,
        sample_fraction=sample_fraction,
    )

    dtype_widths, used_variable_width_fallback = _dtype_widths(
        sample,
        active_snapshot=active_snapshot,
        required_columns=required_columns,
        value_specs=[*value_keys, *values_without_column],
    )
    row_key_width = sum(dtype_widths[column]["estimated_bytes"] for column in row_keys)
    rows_per_cell = selected_input_rows / cell_cardinality if cell_cardinality else 0.0
    rows_per_group = selected_input_rows / row_cardinality if row_cardinality else 0.0
    value_width = sum(
        _aggregate_output_bytes(
            item,
            dtype_widths[item["source_column"]]["estimated_bytes"],
            rows_per_state=rows_per_cell,
        )
        for item in value_keys
    )
    standalone_value_width = sum(
        _aggregate_output_bytes(
            item,
            dtype_widths[item["source_column"]]["estimated_bytes"],
            rows_per_state=rows_per_group,
        )
        for item in values_without_column
    )
    estimated_output_columns = (
        len(row_keys) + column_cardinality * len(value_keys) + len(values_without_column)
    )
    validity_bytes = math.ceil(estimated_output_columns / 8)
    estimated_wide_row_bytes = math.ceil(
        row_key_width
        + column_cardinality * value_width
        + standalone_value_width
        + validity_bytes
    )
    possible_cells = row_cardinality * column_cardinality
    cell_density = min(1.0, cell_cardinality / possible_cells) if possible_cells else 0.0
    aggregate_state_width = sum(
        _aggregate_state_bytes(
            item,
            dtype_widths[item["source_column"]]["estimated_bytes"],
            rows_per_state=rows_per_cell,
        )
        for item in value_keys
    )
    standalone_state_width = sum(
        _aggregate_state_bytes(
            item,
            dtype_widths[item["source_column"]]["estimated_bytes"],
            rows_per_state=rows_per_group,
        )
        for item in values_without_column
    )
    estimated_pivot_state_bytes = math.ceil(
        1.25
        * (
            row_cardinality * (row_key_width + standalone_state_width + 32)
            + cell_cardinality * (aggregate_state_width + 16)
            + row_cardinality * estimated_wide_row_bytes
        )
    )
    confidence = 1.0 if sample_fraction >= 1.0 else 0.8
    if used_variable_width_fallback:
        confidence = max(0.0, confidence - 0.15)

    return {
        **base,
        "estimate_status": "complete",
        "sampled_rows": sampled_rows,
        "sample_fraction": round(sample_fraction, 6),
        "row_key_group_approx_cardinality": row_cardinality,
        "column_key_approx_cardinality": column_cardinality,
        "observed_cell_approx_cardinality": cell_cardinality,
        "cell_density": round(cell_density, 6),
        "rows_per_pivot_group": group_distribution,
        "dtype_widths": dtype_widths,
        "estimated_output_rows": row_cardinality,
        "estimated_output_columns": estimated_output_columns,
        "estimated_wide_row_bytes": estimated_wide_row_bytes,
        "estimated_output_uncompressed_bytes": row_cardinality * estimated_wide_row_bytes,
        "estimated_pivot_state_bytes": estimated_pivot_state_bytes,
        "confidence": round(confidence, 3),
        "assumptions": [
            "Cardinalities use deterministic hash-based approximate_n_unique.",
            "Group-size quantiles are extrapolated from a deterministic bounded sample.",
            "Wide output bytes are an uncompressed in-memory estimate, not a Parquet size forecast.",
            "Pivot state includes a 1.25 safety factor and hash-table overhead heuristics.",
        ],
    }


def _bounded_sample(frame: pl.DataFrame, *, limit: int) -> pl.DataFrame:
    if frame.height <= limit:
        return frame
    return frame.sample(n=limit, with_replacement=False, shuffle=True, seed=_HASH_SEED)


def _approx_cardinality(frame: pl.DataFrame, columns: list[str]) -> int:
    if frame.is_empty():
        return 0
    if not columns:
        return 1
    value = frame.select(
        pl.struct(columns).hash(seed=_HASH_SEED).approx_n_unique().alias("cardinality")
    ).item()
    return int(value or 0)


def _group_distribution(
    sample: pl.DataFrame,
    *,
    row_keys: list[str],
    sample_fraction: float,
) -> dict[str, int]:
    if sample.is_empty():
        return {"p50": 0, "p95": 0, "p99": 0, "max": 0}
    if not row_keys:
        counts = pl.Series("len", [sample.height])
    else:
        counts = sample.group_by(row_keys).len().get_column("len")
    scale = 1.0 / sample_fraction if sample_fraction > 0 else 0.0
    return {
        "p50": math.ceil(float(counts.quantile(0.50, interpolation="nearest") or 0) * scale),
        "p95": math.ceil(float(counts.quantile(0.95, interpolation="nearest") or 0) * scale),
        "p99": math.ceil(float(counts.quantile(0.99, interpolation="nearest") or 0) * scale),
        "max": math.ceil(float(counts.max() or 0) * scale),
    }


def _dtype_widths(
    sample: pl.DataFrame,
    *,
    active_snapshot: pl.DataFrame,
    required_columns: list[str],
    value_specs: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], bool]:
    output_dtypes = {
        item["source_column"]: str(item.get("output_dtype") or "").upper()
        for item in value_specs
        if item.get("output_dtype")
    }
    widths: dict[str, dict[str, Any]] = {}
    used_fallback = False
    for column in required_columns:
        source_dtype = str(active_snapshot.schema[column])
        requested_dtype = output_dtypes.get(column)
        lookup_dtype = requested_dtype or source_dtype.upper()
        fixed_width = _fixed_width(lookup_dtype)
        if fixed_width is not None:
            widths[column] = {
                "source_dtype": source_dtype,
                "output_dtype": requested_dtype,
                "estimated_bytes": fixed_width,
                "mode": "fixed_dtype",
            }
            continue
        if sample.height:
            estimated_bytes = max(
                1,
                math.ceil(sample.get_column(column).estimated_size("b") / sample.height),
            )
            mode = "sample_estimated_size"
        else:
            estimated_bytes = 32
            mode = "empty_fallback"
            used_fallback = True
        widths[column] = {
            "source_dtype": source_dtype,
            "output_dtype": requested_dtype,
            "estimated_bytes": estimated_bytes,
            "mode": mode,
        }
    return widths, used_fallback


def _fixed_width(dtype_name: str) -> int | None:
    normalized = dtype_name.upper().replace(" ", "")
    for prefix, width in _FIXED_DTYPE_BYTES.items():
        if (
            normalized == prefix
            or normalized.startswith(f"{prefix}(")
            or normalized.startswith(f"{prefix}[")
        ):
            return width
    return None


def _aggregate_output_bytes(
    spec: Mapping[str, Any],
    value_width: int,
    *,
    rows_per_state: float,
) -> int:
    aggregation = str(spec.get("aggregation") or "first").lower()
    if aggregation == "unique_concatenate":
        return max(value_width, math.ceil((value_width + 1) * rows_per_state))
    return value_width


def _aggregate_state_bytes(
    spec: Mapping[str, Any],
    value_width: int,
    *,
    rows_per_state: float,
) -> int:
    aggregation = str(spec.get("aggregation") or "first").lower()
    if aggregation in {"avg", "mean"}:
        return max(16, value_width + 8)
    if aggregation in {"count", "count_distinct"}:
        return 8 if aggregation == "count" else max(64, value_width * 4)
    if aggregation == "unique_concatenate":
        return max(64, math.ceil((value_width + 24) * rows_per_state))
    return max(8, value_width)


def _pivot_signature(
    pivot: Mapping[str, Any],
    *,
    row_keys: list[str],
    column_keys: list[str],
    value_keys: list[dict[str, Any]],
    values_without_column: list[dict[str, Any]],
) -> str:
    semantic_contract = {
        "row_keys": row_keys,
        "column_keys": column_keys,
        "value_keys": value_keys,
        "value_keys_without_column": values_without_column,
        "column_name_rule": pivot.get("column_name_rule"),
        "first_duplicate_policy": str(pivot.get("first_duplicate_policy") or "warn").lower(),
        "null_column_key_policy": str(pivot.get("null_column_key_policy") or "error").lower(),
        "column_key_separator": str(pivot.get("column_key_separator") or "__"),
    }
    canonical = json.dumps(
        semantic_contract,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _string_items(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _value_specs(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        source_column = str(item.get("source_column") or "").strip()
        if not source_column:
            continue
        spec: dict[str, Any] = {
            "name": str(item.get("name") or source_column),
            "source_column": source_column,
            "aggregation": str(item.get("aggregation") or "first").lower(),
        }
        if item.get("output_dtype") is not None:
            spec["output_dtype"] = str(item["output_dtype"]).upper()
        if item.get("column_name_rule") is not None:
            spec["column_name_rule"] = str(item["column_name_rule"])
        normalized.append(spec)
    return normalized
