from __future__ import annotations

import re
from typing import Any

import polars as pl

POLARS_TYPE_MAP: dict[str, pl.DataType] = {
    "TEXT": pl.String,
    "STRING": pl.String,
    "TINYINT": pl.Int8,
    "INT8": pl.Int8,
    "SMALLINT": pl.Int16,
    "INT16": pl.Int16,
    "INT32": pl.Int32,
    "INT64": pl.Int64,
    # INTEGER is the engine's 32-bit integer alias.  Use INT64/BIGINT when
    # the wider representation is intended; keeping this aligned with the
    # Rust payload engine makes repeated INTEGER casts true no-ops.
    "INTEGER": pl.Int32,
    "FLOAT": pl.Float32,
    "FLOAT32": pl.Float32,
    "REAL": pl.Float32,
    "FLOAT64": pl.Float64,
    "DOUBLE": pl.Float64,
    "BOOL": pl.Boolean,
    "BOOLEAN": pl.Boolean,
    "DATE": pl.Date,
    "TIME": pl.Time,
    "TIMESTAMP": pl.Datetime,
    "DATETIME": pl.Datetime,
    "DURATION": pl.Duration("us"),
}


def apply_include_columns(lf: pl.LazyFrame, columns: list[str] | None) -> pl.LazyFrame:
    if not columns:
        return lf
    return lf.select([pl.col(column) for column in columns])


def apply_exclude_columns(lf: pl.LazyFrame, columns: list[str] | None) -> pl.LazyFrame:
    if not columns:
        return lf
    return lf.drop(columns, strict=False)


def apply_type_casts(
    lf: pl.LazyFrame,
    casts: list[dict[str, Any]] | None,
    *,
    stats: dict[str, int] | None = None,
) -> pl.LazyFrame:
    if not casts:
        return lf
    expressions: list[pl.Expr] = []
    source_schema = lf.collect_schema()
    seen_targets: set[tuple[str, str]] = set()
    skipped = 0
    for item in casts:
        name = str(item.get("name") or item.get("column") or "").strip()
        type_name = str(item.get("type") or "").strip().upper()
        if not name or not type_name:
            raise ValueError("Cast item must define name and type.")
        decimal_match = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", type_name.replace(" ", ""))
        dtype = (
            pl.Decimal(int(decimal_match.group(1)), int(decimal_match.group(2)))
            if decimal_match
            else POLARS_TYPE_MAP.get(type_name)
        )
        if dtype is None:
            raise ValueError(f"Unsupported cast type: {type_name}")
        canonical_target = str(dtype)
        target_key = (name, canonical_target)
        if target_key in seen_targets or source_schema.get(name) == dtype:
            skipped += 1
            continue
        seen_targets.add(target_key)
        expressions.append(pl.col(name).cast(dtype).alias(name))
    if stats is not None:
        stats["skipped_same_dtype"] = stats.get("skipped_same_dtype", 0) + skipped
    if not expressions:
        return lf
    return lf.with_columns(expressions)


def apply_filter_sql(lf: pl.LazyFrame, sql: str | None) -> pl.LazyFrame:
    if not sql:
        return lf
    return lf.filter(pl.sql_expr(sql))


def apply_add_calc(lf: pl.LazyFrame, expressions: list[dict[str, Any]] | None) -> pl.LazyFrame:
    if not expressions:
        return lf
    from spotfire_expr_normalizer import normalize_expression

    for index, item in enumerate(expressions):
        name = str(item.get("name") or "").strip()
        dialect, expression = resolve_add_calc_expression(item, index=index)
        if not name:
            raise ValueError(f"Add-calc item {index} must define name.")
        planner_expression = (
            normalize_expression(expression) if dialect == "spotfire_expression" else expression
        )
        # Apply in declaration order so a later selector-local expression can
        # reference a key created immediately before it.
        lf = lf.with_columns(pl.sql_expr(planner_expression).alias(name))
    return lf


def resolve_add_calc_expression(
    item: dict[str, Any],
    *,
    index: int | None = None,
) -> tuple[str, str]:
    """Resolve one non-empty expression while preserving its source dialect."""
    sql = str(item.get("sql") or "").strip()
    spotfire = str(item.get("spotfire_expression") or "").strip()
    label = f"source.payload.add_calc[{index}]" if index is not None else "add-calc item"
    if sql and spotfire:
        raise ValueError(f"{label} must define only one of sql or spotfire_expression.")
    if spotfire:
        return "spotfire_expression", spotfire
    if sql:
        _validate_sql_expression_subset(sql, label=label)
        return "sql", sql
    raise ValueError(f"{label} requires one non-empty value: sql or spotfire_expression.")


def resolve_filter_expression(item: dict[str, Any]) -> tuple[str, str]:
    """Resolve one filter predicate while preserving its source dialect."""
    sql = str(item.get("sql") or "").strip()
    spotfire = str(item.get("spotfire_expression") or "").strip()
    label = "filter operation"
    if sql and spotfire:
        raise ValueError(f"{label} must define only one of sql or spotfire_expression.")
    if spotfire:
        return "spotfire_expression", spotfire
    if sql:
        _validate_sql_expression_subset(sql, label=label)
        return "sql", sql
    raise ValueError(f"{label} requires one non-empty value: sql or spotfire_expression.")


def _validate_sql_expression_subset(expression: str, *, label: str) -> None:
    unsupported_markers = {
        "[": "Spotfire bracket column syntax",
        "]": "Spotfire bracket column syntax",
        "~=": "Spotfire contains operator",
        "//": "Spotfire comment syntax",
    }
    for marker, reason in unsupported_markers.items():
        if marker in expression:
            raise ValueError(f"{label}.sql contains {reason}; use spotfire_expression instead.")


def apply_reference_replace(
    lf: pl.LazyFrame,
    *,
    reference_parquet: str,
    source_column: str,
    reference_input_column: str,
    reference_output_column: str,
    output_column: str | None = None,
) -> pl.LazyFrame:
    target_column = output_column or source_column
    ref = pl.scan_parquet(reference_parquet).select(
        [
            pl.col(reference_input_column).alias("__ref_input"),
            pl.col(reference_output_column).alias("__ref_output"),
        ]
    )
    joined = lf.join(
        ref,
        left_on=source_column,
        right_on="__ref_input",
        how="left",
    )
    return joined.with_columns(
        pl.coalesce([pl.col("__ref_output"), pl.col(source_column)]).alias(target_column)
    ).drop("__ref_output", strict=False)
