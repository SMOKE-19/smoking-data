"""Spotfire expression normalization helpers for DuckDB SQL."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import yaml

from .dependency import build_expression_layers

SQL_KEYWORDS = {
    "and",
    "as",
    "between",
    "by",
    "case",
    "cast",
    "coalesce",
    "date",
    "desc",
    "distinct",
    "else",
    "end",
    "false",
    "filter",
    "first",
    "from",
    "group",
    "having",
    "if",
    "in",
    "is",
    "join",
    "left",
    "like",
    "max",
    "min",
    "not",
    "null",
    "on",
    "or",
    "order",
    "over",
    "partition",
    "select",
    "sum",
    "then",
    "true",
    "varchar",
    "when",
    "where",
}

SUPPORTED_SPOTFIRE_FUNCTIONS = {
    "avg",
    "average",
    "boolean",
    "base64decode",
    "base64encode",
    "cast",
    "charindex",
    "concatenate",
    "count",
    "countbig",
    "covariance",
    "currency",
    "date",
    "dateadd",
    "datediff",
    "datepart",
    "denserank",
    "rownumber",
    "datetime",
    "datetimenow",
    "days",
    "decimal",
    "find",
    "firstvalidafter",
    "fiscalmonth",
    "fiscalquarter",
    "fiscalyear",
    "fromepochmilliseconds",
    "fromepochseconds",
    "geometricmean",
    "hours",
    "if",
    "integer",
    "iqr",
    "isoweek",
    "isoyear",
    "isnull",
    "l95",
    "lastvalidbefore",
    "lastvalueformax",
    "lastvalueformin",
    "lav",
    "len",
    "lif",
    "lof",
    "longinteger",
    "max",
    "meandeviation",
    "median",
    "medianabsolutedeviation",
    "mid",
    "milliseconds",
    "min",
    "minutes",
    "mostcommon",
    "nthlargest",
    "nthsmallest",
    "outliers",
    "p10",
    "p90",
    "parsedate",
    "parsedatetime",
    "parsereal",
    "parsetime",
    "parsetimespan",
    "pctoutliers",
    "percent",
    "percentile",
    "q1",
    "q3",
    "rand",
    "randbetween",
    "rankreal",
    "real",
    "right",
    "rxextract",
    "rxreplace",
    "seconds",
    "single",
    "singlereal",
    "sn",
    "split",
    "stderr",
    "string",
    "substitute",
    "sum",
    "totaldays",
    "totalhours",
    "totalmilliseconds",
    "totalminutes",
    "totalseconds",
    "time",
    "timespan",
    "toepochmilliseconds",
    "toepochseconds",
    "trimmedmean",
    "u95",
    "uif",
    "uof",
    "uav",
    "uniquecount",
    "uniqueconcatenate",
    "valueformax",
    "valueformin",
    "var",
    "variance",
    "weightedaverage",
    "yearandweek",
}

DEFAULT_WINDOW_AGGREGATES = {
    "countbig",
    "covariance",
    "denserank",
    "rownumber",
    "firstvalidafter",
    "geometricmean",
    "iqr",
    "l95",
    "lastvalidbefore",
    "lastvalueformax",
    "lastvalueformin",
    "lif",
    "lof",
    "lav",
    "meandeviation",
    "median",
    "medianabsolutedeviation",
    "mostcommon",
    "nthlargest",
    "nthsmallest",
    "outliers",
    "p10",
    "p90",
    "pctoutliers",
    "percent",
    "percentile",
    "q1",
    "q3",
    "rankreal",
    "stderr",
    "trimmedmean",
    "u95",
    "uif",
    "uof",
    "uav",
    "uniqueconcatenate",
    "valueformax",
    "valueformin",
    "var",
    "weightedaverage",
}

NESTED_INDEX_COLUMN = "__nested_index"
GENERATED_NESTED_INDEX_COLUMN = "__generated_nested_index"
SYNTHETIC_ROW_ID_COLUMN = "__row_id"
_DUCKDB_FUNCTION_NAME_CACHE: set[str] | None = None


@dataclass(slots=True)
class DerivedExpression:
    """Single derived expression and its DuckDB-normalized SQL."""

    name: str
    expression: str
    normalized_expression: str
    dependencies: list[str]


@dataclass(slots=True)
class ExpressionCompatibilityError(ValueError):
    """Spotfire expression을 DuckDB로 변환하기 전에 감지한 호환성 오류다."""

    unsupported_yaml_path: Path
    issues: list[dict[str, Any]]

    def __str__(self) -> str:
        preview = "; ".join(
            f"{item['column_name']}: {', '.join(item['unsupported_functions'])}"
            for item in self.issues[:5]
        )
        suffix = " ..." if len(self.issues) > 5 else ""
        return (
            "Spotfire expression 호환성 검증 실패: "
            f"{len(self.issues)}개 expression에서 미지원 함수가 발견되었습니다. "
            f"unsupported_yaml={self.unsupported_yaml_path}. "
            f"{preview}{suffix}"
        )


@dataclass(slots=True)
class ExpressionCompileResult:
    """Expression file compilation outputs."""

    expressions: list[DerivedExpression]
    layered_yaml_path: Path
    duckdb_layered_yaml_path: Path
    expression_count_before: int
    expression_count_after: int
    expression_rewrite_count: int


@dataclass(slots=True)
class DuckDbLayeredExpressionPrepareResult:
    """Normalized DuckDB layered expression contract for downstream executors."""

    expressions: list[DerivedExpression]
    duckdb_layered_yaml_path: Path
    source_path: Path
    source_format: str
    layered_yaml_path: Path | None
    expression_count_before: int
    expression_count_after: int
    expression_rewrite_count: int
    compiled: bool


@dataclass(slots=True)
class PolarsExpression:
    """Single derived expression and its Polars expression source."""

    name: str
    expression: str
    polars_expression: str
    dependencies: list[str]


@dataclass(slots=True)
class PolarsExpressionCompileResult:
    """Polars expression file compilation outputs."""

    expressions: list[PolarsExpression]
    layered_yaml_path: Path
    polars_layered_yaml_path: Path
    expression_count_before: int
    expression_count_after: int
    expression_rewrite_count: int


@dataclass(slots=True)
class PolarsLayeredExpressionPrepareResult:
    """Normalized Polars layered expression contract for downstream executors."""

    expressions: list[PolarsExpression]
    polars_layered_yaml_path: Path
    source_path: Path
    source_format: str
    layered_yaml_path: Path | None
    expression_count_before: int
    expression_count_after: int
    expression_rewrite_count: int
    compiled: bool


def prepare_duckdb_layered_expression(
    source_path: str | Path,
    *,
    source_format: str | None = None,
    result_name_field: str | None = None,
    sql_expression_field: str | None = None,
) -> DuckDbLayeredExpressionPrepareResult:
    """Prepare CSV, Spotfire layered YAML, or DuckDB layered YAML for execution.

    CSV and Spotfire YAML inputs are compiled into ``*.duckdb.layered.yaml``.
    DuckDB layered YAML inputs are validated and loaded without recompilation.
    """
    path = Path(source_path)
    normalized_format = _infer_expression_source_format(path, source_format)

    if normalized_format == "duckdb_layered_yaml":
        expressions = load_duckdb_layered_expression_yaml(path)
        return DuckDbLayeredExpressionPrepareResult(
            expressions=expressions,
            duckdb_layered_yaml_path=path,
            source_path=path,
            source_format=normalized_format,
            layered_yaml_path=None,
            expression_count_before=len(expressions),
            expression_count_after=len(expressions),
            expression_rewrite_count=0,
            compiled=False,
        )

    compile_result = compile_expression_file(
        path,
        source_format=normalized_format,
        result_name_field=result_name_field,
        sql_expression_field=sql_expression_field,
    )
    return DuckDbLayeredExpressionPrepareResult(
        expressions=compile_result.expressions,
        duckdb_layered_yaml_path=compile_result.duckdb_layered_yaml_path,
        source_path=path,
        source_format=normalized_format,
        layered_yaml_path=compile_result.layered_yaml_path,
        expression_count_before=compile_result.expression_count_before,
        expression_count_after=compile_result.expression_count_after,
        expression_rewrite_count=compile_result.expression_rewrite_count,
        compiled=True,
    )


def prepare_polars_layered_expression(
    source_path: str | Path,
    *,
    source_format: str | None = None,
    result_name_field: str | None = None,
    sql_expression_field: str | None = None,
) -> PolarsLayeredExpressionPrepareResult:
    """Prepare CSV, Spotfire layered YAML, or Polars layered YAML for execution."""
    path = Path(source_path)
    normalized_format = _infer_polars_expression_source_format(path, source_format)

    if normalized_format == "polars_layered_yaml":
        expressions = load_polars_layered_expression_yaml(path)
        return PolarsLayeredExpressionPrepareResult(
            expressions=expressions,
            polars_layered_yaml_path=path,
            source_path=path,
            source_format=normalized_format,
            layered_yaml_path=None,
            expression_count_before=len(expressions),
            expression_count_after=len(expressions),
            expression_rewrite_count=0,
            compiled=False,
        )

    compile_result = compile_polars_expression_file(
        path,
        source_format=normalized_format,
        result_name_field=result_name_field,
        sql_expression_field=sql_expression_field,
    )
    return PolarsLayeredExpressionPrepareResult(
        expressions=compile_result.expressions,
        polars_layered_yaml_path=compile_result.polars_layered_yaml_path,
        source_path=path,
        source_format=normalized_format,
        layered_yaml_path=compile_result.layered_yaml_path,
        expression_count_before=compile_result.expression_count_before,
        expression_count_after=compile_result.expression_count_after,
        expression_rewrite_count=compile_result.expression_rewrite_count,
        compiled=True,
    )


def compile_expression_file(
    source_path: str | Path,
    *,
    source_format: str,
    result_name_field: str | None = None,
    sql_expression_field: str | None = None,
) -> ExpressionCompileResult:
    """Compile Spotfire expression CSV/YAML into human and DuckDB layered YAML files."""
    path = Path(source_path)
    normalized_format = source_format.strip().lower()
    if normalized_format == "csv":
        raw_items = load_expression_items_from_csv(
            path,
            result_name_field=result_name_field,
            sql_expression_field=sql_expression_field,
        )
    elif normalized_format == "yaml":
        raw_items = load_expression_items_from_yaml(path)
    else:
        raise ValueError(f"Unsupported expression source format: {source_format}")

    raw_expressions = build_raw_expressions(raw_items)
    canonicalized, expression_rewrite_count = canonicalize_expressions(raw_expressions)
    layers = build_expression_layers(canonicalized)
    layered_yaml_path = _layered_yaml_path_for_source(path)
    duckdb_layered_yaml_path = _duckdb_layered_yaml_path_for_source(path)
    write_layered_expression_yaml(layered_yaml_path, layers)
    write_duckdb_layered_expression_yaml(duckdb_layered_yaml_path, layers)
    validate_expression_compatibility(
        canonicalized, unsupported_yaml_path=path.with_suffix(".unsupported.yaml")
    )
    return ExpressionCompileResult(
        expressions=load_duckdb_layered_expression_yaml(duckdb_layered_yaml_path),
        layered_yaml_path=layered_yaml_path,
        duckdb_layered_yaml_path=duckdb_layered_yaml_path,
        expression_count_before=len(raw_expressions),
        expression_count_after=len(canonicalized),
        expression_rewrite_count=expression_rewrite_count,
    )


def compile_polars_expression_file(
    source_path: str | Path,
    *,
    source_format: str,
    result_name_field: str | None = None,
    sql_expression_field: str | None = None,
) -> PolarsExpressionCompileResult:
    """Compile Spotfire expression CSV/YAML into human and Polars layered YAML files."""
    path = Path(source_path)
    normalized_format = source_format.strip().lower()
    if normalized_format == "csv":
        raw_items = load_expression_items_from_csv(
            path,
            result_name_field=result_name_field,
            sql_expression_field=sql_expression_field,
        )
    elif normalized_format == "yaml":
        raw_items = load_expression_items_from_yaml(path)
    else:
        raise ValueError(f"Unsupported expression source format: {source_format}")

    raw_expressions = build_raw_expressions(raw_items)
    canonicalized, expression_rewrite_count = canonicalize_expressions(raw_expressions)
    layers = build_expression_layers(canonicalized)
    polars_layers = [
        [
            PolarsExpression(
                name=item.name,
                expression=item.expression,
                polars_expression=normalize_expression_for_polars(item.expression),
                dependencies=list(item.dependencies),
            )
            for item in layer_items
        ]
        for layer_items in layers
    ]
    layered_yaml_path = _layered_yaml_path_for_source(path)
    polars_layered_yaml_path = _polars_layered_yaml_path_for_source(path)
    write_layered_expression_yaml(layered_yaml_path, layers)
    write_polars_layered_expression_yaml(polars_layered_yaml_path, polars_layers)
    validate_expression_compatibility(
        canonicalized, unsupported_yaml_path=path.with_suffix(".unsupported.yaml")
    )
    return PolarsExpressionCompileResult(
        expressions=load_polars_layered_expression_yaml(polars_layered_yaml_path),
        layered_yaml_path=layered_yaml_path,
        polars_layered_yaml_path=polars_layered_yaml_path,
        expression_count_before=len(raw_expressions),
        expression_count_after=len(canonicalized),
        expression_rewrite_count=expression_rewrite_count,
    )


def _infer_expression_source_format(path: Path, source_format: str | None) -> str:
    if source_format is not None:
        normalized = source_format.strip().lower().replace("-", "_")
        if normalized in {"yaml", "yml"} and path.suffix.lower() in {".yaml", ".yml"}:
            return "duckdb_layered_yaml" if _is_duckdb_layered_expression_yaml(path) else "yaml"
        aliases = {
            "csv": "csv",
            "yaml": "yaml",
            "yml": "yaml",
            "layered_yaml": "yaml",
            "spotfire_layered_yaml": "yaml",
            "duckdb": "duckdb_layered_yaml",
            "duckdb_yaml": "duckdb_layered_yaml",
            "duckdb_layered": "duckdb_layered_yaml",
            "duckdb_layered_yaml": "duckdb_layered_yaml",
        }
        try:
            return aliases[normalized]
        except KeyError as exc:
            raise ValueError(f"Unsupported expression source format: {source_format}") from exc

    if path.suffix.lower() == ".csv":
        return "csv"
    if path.suffix.lower() in {".yaml", ".yml"}:
        return "duckdb_layered_yaml" if _is_duckdb_layered_expression_yaml(path) else "yaml"
    raise ValueError(f"Cannot infer expression source format from path: {path}")


def _infer_polars_expression_source_format(path: Path, source_format: str | None) -> str:
    if source_format is not None:
        normalized = source_format.strip().lower().replace("-", "_")
        if normalized in {"yaml", "yml"} and path.suffix.lower() in {".yaml", ".yml"}:
            return "polars_layered_yaml" if _is_polars_layered_expression_yaml(path) else "yaml"
        aliases = {
            "csv": "csv",
            "yaml": "yaml",
            "yml": "yaml",
            "layered_yaml": "yaml",
            "spotfire_layered_yaml": "yaml",
            "polars": "polars_layered_yaml",
            "polars_yaml": "polars_layered_yaml",
            "polars_layered": "polars_layered_yaml",
            "polars_layered_yaml": "polars_layered_yaml",
        }
        try:
            return aliases[normalized]
        except KeyError as exc:
            raise ValueError(f"Unsupported expression source format: {source_format}") from exc

    if path.suffix.lower() == ".csv":
        return "csv"
    if path.suffix.lower() in {".yaml", ".yml"}:
        return "polars_layered_yaml" if _is_polars_layered_expression_yaml(path) else "yaml"
    raise ValueError(f"Cannot infer expression source format from path: {path}")


def _layered_yaml_path_for_source(path: Path) -> Path:
    if path.name.endswith(".layered.yaml"):
        return path
    return path.with_suffix(".layered.yaml")


def _duckdb_layered_yaml_path_for_source(path: Path) -> Path:
    if path.name.endswith(".duckdb.layered.yaml"):
        return path
    if path.name.endswith(".layered.yaml"):
        return path.with_name(path.name.removesuffix(".layered.yaml") + ".duckdb.layered.yaml")
    return path.with_suffix(".duckdb.layered.yaml")


def _polars_layered_yaml_path_for_source(path: Path) -> Path:
    if path.name.endswith(".polars.layered.yaml"):
        return path
    if path.name.endswith(".layered.yaml"):
        return path.with_name(path.name.removesuffix(".layered.yaml") + ".polars.layered.yaml")
    return path.with_suffix(".polars.layered.yaml")


def _is_duckdb_layered_expression_yaml(path: Path) -> bool:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError:
        raise
    except yaml.YAMLError:
        return False
    return isinstance(payload, dict) and payload.get("format") == "duckdb_layered_expression"


def _is_polars_layered_expression_yaml(path: Path) -> bool:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError:
        raise
    except yaml.YAMLError:
        return False
    return isinstance(payload, dict) and payload.get("format") == "polars_layered_expression"


def build_raw_expressions(raw_items: list[tuple[str, str]]) -> list[DerivedExpression]:
    names = {name for name, _ in raw_items}
    if len(names) != len(raw_items):
        raise ValueError("Expression input contains duplicate result column names.")
    return [
        DerivedExpression(
            name=name,
            expression=expr,
            normalized_expression=normalize_expression(expr),
            dependencies=[
                dep for dep in extract_identifier_tokens(expr) if dep in names and dep != name
            ],
        )
        for name, expr in raw_items
    ]


def canonicalize_expressions(
    expressions: list[DerivedExpression],
) -> tuple[list[DerivedExpression], int]:
    """동일 로직을 첫 이름으로 통일하고 변경된 expression 개수를 반환한다."""
    canonical_by_logic: dict[str, DerivedExpression] = {}
    alias_to_canonical: dict[str, str] = {}
    deduped: list[DerivedExpression] = []
    rewrite_count = 0

    for item in expressions:
        logic_key = _normalize_logic_key(item.expression)
        existing = canonical_by_logic.get(logic_key)
        if existing is None:
            canonical_by_logic[logic_key] = item
            alias_to_canonical[item.name] = item.name
            deduped.append(item)
            continue
        alias_to_canonical[item.name] = existing.name

    rewritten: list[DerivedExpression] = []
    for item in deduped:
        new_expression = _rewrite_bracket_identifiers(
            item.expression,
            alias_to_canonical,
            skip_name=item.name,
        )
        if new_expression != item.expression:
            rewrite_count += 1
        rewritten.append(
            DerivedExpression(
                name=item.name,
                expression=new_expression,
                normalized_expression=normalize_expression(new_expression),
                dependencies=[],
            )
        )

    names = {item.name for item in rewritten}
    finalized = [
        DerivedExpression(
            name=item.name,
            expression=item.expression,
            normalized_expression=item.normalized_expression,
            dependencies=[
                dep
                for dep in extract_identifier_tokens(item.expression)
                if dep in names and dep != item.name
            ],
        )
        for item in rewritten
    ]

    return finalized, rewrite_count


def validate_expression_compatibility(
    expressions: list[DerivedExpression],
    *,
    unsupported_yaml_path: Path,
) -> None:
    """DuckDB 실행 전에 Spotfire 함수 호환성을 검증하고 실패 YAML을 저장한다."""
    issues: list[dict[str, Any]] = []
    supported = {item.lower() for item in SUPPORTED_SPOTFIRE_FUNCTIONS} | _duckdb_function_names()
    for item in expressions:
        function_calls = _extract_function_calls(item.expression)
        unsupported_functions = [name for name in function_calls if name.lower() not in supported]
        if not unsupported_functions:
            continue
        issues.append(
            {
                "column_name": item.name,
                "unsupported_functions": unsupported_functions,
                "sql_expression": item.expression,
                "normalized_expression": item.normalized_expression,
            }
        )
    if not issues:
        if unsupported_yaml_path.exists():
            unsupported_yaml_path.unlink()
        return
    write_unsupported_expression_yaml(unsupported_yaml_path, issues)
    raise ExpressionCompatibilityError(unsupported_yaml_path=unsupported_yaml_path, issues=issues)


def _duckdb_function_names() -> set[str]:
    global _DUCKDB_FUNCTION_NAME_CACHE
    if _DUCKDB_FUNCTION_NAME_CACHE is not None:
        return _DUCKDB_FUNCTION_NAME_CACHE
    try:
        with duckdb.connect(database=":memory:") as connection:
            rows = connection.execute("PRAGMA functions").fetchall()
    except duckdb.Error:
        _DUCKDB_FUNCTION_NAME_CACHE = set()
        return _DUCKDB_FUNCTION_NAME_CACHE
    _DUCKDB_FUNCTION_NAME_CACHE = {
        str(row[0]).strip().lower() for row in rows if row and str(row[0]).strip()
    }
    return _DUCKDB_FUNCTION_NAME_CACHE


def write_unsupported_expression_yaml(path: Path, issues: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "unsupported_expressions": issues,
    }
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path


def write_layered_expression_yaml(path: Path, layers: list[list[DerivedExpression]]) -> Path:
    """레이어 순서와 pretty-formatted 식을 담은 YAML을 저장한다."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_dump_layered_expression_yaml(layers), encoding="utf-8")
    return path


def write_duckdb_layered_expression_yaml(path: Path, layers: list[list[DerivedExpression]]) -> Path:
    """Write the execution contract consumed by ETL0202."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "format": "duckdb_layered_expression",
        "layers": [
            {
                "layer_number": layer_index,
                "expressions": [
                    {
                        "column_name": item.name,
                        "dependencies": list(item.dependencies),
                        "duckdb_sql": item.normalized_expression,
                        "source_expression": item.expression,
                    }
                    for item in layer_items
                ],
            }
            for layer_index, layer_items in enumerate(layers, start=1)
        ],
    }
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path


def write_polars_layered_expression_yaml(path: Path, layers: list[list[PolarsExpression]]) -> Path:
    """Write the Polars execution contract consumed by Polars-based executors."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "format": "polars_layered_expression",
        "layers": [
            {
                "layer_number": layer_index,
                "expressions": [
                    {
                        "column_name": item.name,
                        "dependencies": list(item.dependencies),
                        "polars_expr": item.polars_expression,
                        "source_expression": item.expression,
                    }
                    for item in layer_items
                ],
            }
            for layer_index, layer_items in enumerate(layers, start=1)
        ],
    }
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path


def load_duckdb_layered_expression_yaml(path: str | Path) -> list[DerivedExpression]:
    yaml_path = Path(path)
    payload = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("format") != "duckdb_layered_expression":
        raise ValueError(f"DuckDB layered expression YAML expected: {yaml_path}")
    raw_layers = payload.get("layers")
    if not isinstance(raw_layers, list):
        raise ValueError(f"DuckDB layered expression YAML must contain layers: {yaml_path}")
    expressions: list[DerivedExpression] = []
    for raw_layer in raw_layers:
        if not isinstance(raw_layer, dict):
            continue
        raw_items = raw_layer.get("expressions")
        if not isinstance(raw_items, list):
            continue
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                continue
            name = str(raw_item.get("column_name") or "").strip()
            duckdb_sql = str(raw_item.get("duckdb_sql") or "").strip()
            source_expression = str(raw_item.get("source_expression") or duckdb_sql).strip()
            dependencies = [str(item) for item in raw_item.get("dependencies") or []]
            if not name or not duckdb_sql:
                continue
            expressions.append(
                DerivedExpression(
                    name=name,
                    expression=source_expression,
                    normalized_expression=duckdb_sql,
                    dependencies=dependencies,
                )
            )
    if not expressions:
        raise ValueError(f"DuckDB layered expression YAML is empty: {yaml_path}")
    return expressions


def load_polars_layered_expression_yaml(path: str | Path) -> list[PolarsExpression]:
    yaml_path = Path(path)
    payload = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("format") != "polars_layered_expression":
        raise ValueError(f"Polars layered expression YAML expected: {yaml_path}")
    raw_layers = payload.get("layers")
    if not isinstance(raw_layers, list):
        raise ValueError(f"Polars layered expression YAML must contain layers: {yaml_path}")
    expressions: list[PolarsExpression] = []
    for raw_layer in raw_layers:
        if not isinstance(raw_layer, dict):
            continue
        raw_items = raw_layer.get("expressions")
        if not isinstance(raw_items, list):
            continue
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                continue
            name = str(raw_item.get("column_name") or "").strip()
            polars_expr = str(raw_item.get("polars_expr") or "").strip()
            source_expression = str(raw_item.get("source_expression") or polars_expr).strip()
            dependencies = [str(item) for item in raw_item.get("dependencies") or []]
            if not name or not polars_expr:
                continue
            expressions.append(
                PolarsExpression(
                    name=name,
                    expression=source_expression,
                    polars_expression=polars_expr,
                    dependencies=dependencies,
                )
            )
    if not expressions:
        raise ValueError(f"Polars layered expression YAML is empty: {yaml_path}")
    return expressions


def load_expression_items_from_csv(
    csv_path: str | Path,
    *,
    result_name_field: str | None = None,
    sql_expression_field: str | None = None,
) -> list[tuple[str, str]]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"Expression CSV is empty: {path}")
    name_key = result_name_field or _find_header(
        rows[0], {"column_name", "name", "result_column", "column"}
    )
    expr_key = sql_expression_field or _find_header(
        rows[0], {"sql_expression", "expression", "expr", "sql"}
    )
    raw_items: list[tuple[str, str]] = []
    for row in rows:
        name = str(row.get(name_key) or "").strip()
        expr = str(row.get(expr_key) or "").strip()
        if name and expr:
            raw_items.append((name, expr))
    return raw_items


def load_expression_items_from_yaml(yaml_path: str | Path) -> list[tuple[str, str]]:
    path = Path(yaml_path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        if isinstance(payload.get("expressions"), list):
            items = payload["expressions"]
        elif isinstance(payload.get("layers"), list):
            items = []
            for layer in payload["layers"]:
                if isinstance(layer, dict) and isinstance(layer.get("expressions"), list):
                    items.extend(layer["expressions"])
        else:
            raise ValueError(f"Expression YAML must contain expressions or layers: {path}")
    else:
        raise ValueError(f"Expression YAML must be a list or dict: {path}")
    raw_items: list[tuple[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(
            item.get("column_name")
            or item.get("name")
            or item.get("result_column")
            or item.get("column")
            or ""
        ).strip()
        expr = str(
            item.get("sql_expression")
            or item.get("expression")
            or item.get("expr")
            or item.get("sql")
            or ""
        ).strip()
        if name and expr:
            raw_items.append((name, expr))
    if not raw_items:
        raise ValueError(f"Expression YAML is empty: {path}")
    return raw_items


def _dump_layered_expression_yaml(layers: list[list[DerivedExpression]]) -> str:
    lines = ["layers:"]
    for layer_index, layer_items in enumerate(layers, start=1):
        lines.append(f"- layer_number: {layer_index}")
        lines.append("  expressions:")
        for item in layer_items:
            lines.extend(_render_expression_yaml_item(item, indent=4, include_dependencies=True))
    return "\n".join(lines) + "\n"


def _render_expression_yaml_item(
    item: DerivedExpression,
    *,
    indent: int,
    include_dependencies: bool,
) -> list[str]:
    prefix = " " * indent
    lines = [f"{prefix}- column_name: {_yaml_inline_string(item.name)}"]
    if include_dependencies:
        lines.append(f"{prefix}  dependencies: {_yaml_inline_list(item.dependencies)}")
    expression, comments = _split_spotfire_line_comments(item.expression)
    for comment in comments:
        lines.append(f"{prefix}  # {comment}")
    lines.extend(
        _render_yaml_scalar(
            "sql_expression", pretty_format_expression(expression), indent=indent + 2
        )
    )
    return lines


def _render_yaml_scalar(key: str, value: str, *, indent: int) -> list[str]:
    prefix = " " * indent
    if "\n" not in value:
        return [f"{prefix}{key}: {_yaml_inline_string(value)}"]
    lines = [f"{prefix}{key}: |-"]
    value_indent = " " * (indent + 2)
    lines.extend(f"{value_indent}{line}" if line else value_indent for line in value.splitlines())
    return lines


def _yaml_inline_string(value: str) -> str:
    return (
        yaml.safe_dump(
            value,
            allow_unicode=True,
            default_flow_style=True,
            width=10_000,
        )
        .strip()
        .removesuffix("\n...")
        .strip()
    )


def _yaml_inline_list(values: list[str]) -> str:
    if not values:
        return "[]"
    return "[" + ", ".join(_yaml_inline_string(value) for value in values) + "]"


def _layer_alias(layer_index: int) -> str:
    return f"layer_{layer_index:03d}"


def _find_header(row: dict[str, Any], candidates: set[str]) -> str:
    for key in row:
        normalized = re.sub(r"[\s-]+", "_", str(key).strip().lower())
        if normalized in candidates:
            return str(key)
    raise ValueError(f"Required CSV header not found. candidates={sorted(candidates)}")


def extract_identifier_tokens(expression: str) -> list[str]:
    expression = _strip_spotfire_line_comments(expression)
    bracket_tokens, without_brackets = _extract_bracket_identifiers(expression)
    stripped = _strip_single_quoted_literals(without_brackets)
    tokens: list[str] = []
    tokens.extend(bracket_tokens)
    for match in re.finditer(r'"([^"]+)"|\b([A-Za-z_][A-Za-z0-9_]*)\b', stripped):
        quoted, bare = match.groups()
        token = quoted if quoted is not None else bare
        if token is None:
            continue
        normalized = token.strip()
        if not normalized:
            continue
        if normalized.lower() in SQL_KEYWORDS:
            continue
        tokens.append(normalized)
    return list(dict.fromkeys(tokens))


def _extract_function_calls(expression: str) -> list[str]:
    expression = _strip_spotfire_line_comments(expression)
    calls: list[str] = []
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "'":
            _literal, index = _read_single_quoted_literal(expression, index)
            continue
        if char == '"':
            _quoted, index = _read_double_quoted_segment(expression, index)
            continue
        if char == "[":
            _identifier, index = _read_bracket_identifier(expression, index)
            continue
        if char.isalpha() or char == "_":
            identifier, next_index = _read_identifier(expression, index)
            cursor = next_index
            while cursor < len(expression) and expression[cursor].isspace():
                cursor += 1
            if (
                cursor < len(expression)
                and expression[cursor] == "("
                and identifier.lower() not in SQL_KEYWORDS
            ):
                calls.append(identifier)
            index = next_index
            continue
        index += 1
    return list(dict.fromkeys(calls))


def normalize_expression(expression: str) -> str:
    return _rewrite_expression_for_duckdb(_strip_spotfire_line_comments(expression))


def normalize_spotfire_expression_for_duckdb(expression: str) -> str:
    """Normalize a Spotfire-style expression into DuckDB-compatible SQL."""
    return normalize_expression(expression)


def normalize_expression_for_polars(expression: str) -> str:
    """Normalize a Spotfire-style expression into a Polars expression source string."""
    return _rewrite_expression_for_polars(_strip_spotfire_line_comments(expression))


def normalize_spotfire_expression_for_polars(expression: str) -> str:
    """Normalize a Spotfire-style expression into Polars-compatible expression source."""
    return normalize_expression_for_polars(expression)


def _normalize_logic_key(expression: str) -> str:
    expression = _strip_spotfire_line_comments(expression)
    return re.sub(r"\s+", " ", expression).strip().lower()


def _rewrite_expression_for_polars(expression: str) -> str:
    expr = _strip_outer_parentheses(expression.strip())
    if not expr:
        return "pl.lit(None)"
    if expr.startswith("'") and expr.endswith("'"):
        return f"pl.lit({_python_string_literal(_strip_sql_string_literal(expr) or '')})"
    if expr.startswith('"') and expr.endswith('"'):
        inner_expr = expr[1:-1].replace('""', '"')
        return f"pl.lit({_python_string_literal(inner_expr)})"
    if expr.lower() == "null":
        return "pl.lit(None)"
    if expr.lower() == "true":
        return "pl.lit(True)"
    if expr.lower() == "false":
        return "pl.lit(False)"
    if _parse_numeric_literal(expr) is not None:
        return expr
    if expr[:4].lower() == "case" and _word_at_end(expr, "end"):
        return _rewrite_case_expression_for_polars(expr)

    split = _split_top_level_word_operator(expr, "or")
    if split is not None:
        left, right = split
        return f"({_rewrite_expression_for_polars(left)} | {_rewrite_expression_for_polars(right)})"
    split = _split_top_level_word_operator(expr, "and")
    if split is not None:
        left, right = split
        return f"({_rewrite_expression_for_polars(left)} & {_rewrite_expression_for_polars(right)})"
    not_tail = _strip_leading_word(expr, "not")
    if not_tail is not None:
        return f"(~{_rewrite_expression_for_polars(not_tail)})"

    split = _split_top_level_word_operator(expr, "in")
    if split is not None:
        left, right = split
        return (
            f"{_rewrite_expression_for_polars(left)}.is_in({_rewrite_polars_list_literal(right)})"
        )
    split = _split_top_level_symbol_operator(expr, ["~=", "<=", ">=", "<>", "=", "<", ">"])
    if split is not None:
        left, operator, right = split
        if operator == "~=":
            return f"{_rewrite_expression_for_polars(left)}.str.contains({_polars_plain_literal(right)}, literal=True)"
        polars_operator = "==" if operator == "=" else "!=" if operator == "<>" else operator
        return f"({_rewrite_expression_for_polars(left)} {polars_operator} {_rewrite_expression_for_polars(right)})"

    concat_parts = _split_top_level_symbol_chain(expr, "&")
    if concat_parts is not None:
        return (
            "pl.concat_str(["
            + ", ".join(_rewrite_expression_for_polars(part) for part in concat_parts)
            + "])"
        )
    split = _split_top_level_symbol_operator(expr, ["+", "-"], scan_right=True)
    if split is not None:
        left, operator, right = split
        return f"({_rewrite_expression_for_polars(left)} {operator} {_rewrite_expression_for_polars(right)})"
    split = _split_top_level_symbol_operator(expr, ["*", "/"], scan_right=True)
    if split is not None:
        left, operator, right = split
        return f"({_rewrite_expression_for_polars(left)} {operator} {_rewrite_expression_for_polars(right)})"

    function_result = _rewrite_polars_function_expression(expr)
    if function_result is not None:
        return function_result

    if expr.startswith("["):
        identifier, end_index = _read_bracket_identifier(expr, 0)
        if end_index == len(expr):
            return f"pl.col({_python_string_literal(identifier)})"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", expr) and expr.lower() not in SQL_KEYWORDS:
        return f"pl.col({_python_string_literal(expr)})"
    return expr


def _rewrite_polars_function_expression(expression: str) -> str | None:
    if not expression or not (expression[0].isalpha() or expression[0] == "_"):
        return None
    function_name, cursor = _read_identifier(expression, 0)
    while cursor < len(expression) and expression[cursor].isspace():
        cursor += 1
    if cursor >= len(expression) or expression[cursor] != "(":
        return None
    inner_text, end_index = _read_parenthesized(expression, cursor)
    over_clause, final_index = _read_window_over_clause(expression, end_index)
    if final_index != len(expression):
        return None
    raw_args = _split_top_level_arguments(inner_text)
    lowered = function_name.lower()
    if lowered == "cast" and len(raw_args) == 1:
        cast_match = re.match(
            r"^(?P<value>.+?)\s+as\s+(?P<type>[A-Za-z][A-Za-z0-9_(), ]*)$",
            raw_args[0],
            flags=re.IGNORECASE,
        )
        if cast_match:
            row_expr = (
                f"{_rewrite_expression_for_polars(cast_match.group('value').strip())}"
                f".cast({_normalize_polars_cast_type(cast_match.group('type'))})"
            )
            return (
                _apply_polars_over(row_expr, over_clause) if over_clause is not None else row_expr
            )
    args = [_rewrite_expression_for_polars(arg) for arg in raw_args]
    row_expr = _rewrite_polars_function_call(lowered, args, raw_args)
    if over_clause is not None:
        return _apply_polars_over(row_expr, over_clause)
    if lowered in DEFAULT_WINDOW_AGGREGATES:
        return row_expr
    return row_expr


def _rewrite_polars_function_call(lowered: str, args: list[str], raw_args: list[str]) -> str:
    cast_target = _polars_function_cast_target(lowered)
    if cast_target is not None and len(args) == 1:
        return f"{args[0]}.cast({cast_target})"
    if lowered == "if" and len(args) == 3:
        return f"pl.when({args[0]}).then({args[1]}).otherwise({args[2]})"
    if lowered == "abs" and len(args) == 1:
        return f"{args[0]}.abs()"
    if lowered in {"sum", "avg", "average", "min", "max", "median"} and len(args) == 1:
        method = "mean" if lowered in {"avg", "average"} else lowered
        return f"{args[0]}.{method}()"
    if lowered == "firstvalidafter" and len(args) == 1:
        return f"{args[0]}.backward_fill()"
    if lowered == "lastvalidbefore" and len(args) == 1:
        return f"{args[0]}.forward_fill()"
    if lowered == "count" and len(args) == 1:
        return f"{args[0]}.count()"
    if lowered == "countbig" and len(args) == 1:
        return f"{args[0]}.count()"
    if lowered == "uniquecount" and len(args) == 1:
        return f"{args[0]}.n_unique()"
    if lowered == "percentile" and len(args) == 2:
        return f"{args[0]}.quantile({_normalize_percentile_argument(raw_args[1])}, interpolation='linear')"
    if lowered == "p10" and len(args) == 1:
        return f"{args[0]}.quantile(0.1, interpolation='linear')"
    if lowered == "p90" and len(args) == 1:
        return f"{args[0]}.quantile(0.9, interpolation='linear')"
    if lowered == "q1" and len(args) == 1:
        return f"{args[0]}.quantile(0.25, interpolation='linear')"
    if lowered == "q3" and len(args) == 1:
        return f"{args[0]}.quantile(0.75, interpolation='linear')"
    if lowered == "iqr" and len(args) == 1:
        return f"({args[0]}.quantile(0.75, interpolation='linear') - {args[0]}.quantile(0.25, interpolation='linear'))"
    if lowered == "lif" and len(args) == 1:
        return (
            f"({args[0]}.quantile(0.25, interpolation='linear') - 1.5 * "
            f"({args[0]}.quantile(0.75, interpolation='linear') - {args[0]}.quantile(0.25, interpolation='linear')))"
        )
    if lowered == "uif" and len(args) == 1:
        return (
            f"({args[0]}.quantile(0.75, interpolation='linear') + 1.5 * "
            f"({args[0]}.quantile(0.75, interpolation='linear') - {args[0]}.quantile(0.25, interpolation='linear')))"
        )
    if lowered == "lof" and len(args) == 1:
        return (
            f"({args[0]}.quantile(0.25, interpolation='linear') - 3.0 * "
            f"({args[0]}.quantile(0.75, interpolation='linear') - {args[0]}.quantile(0.25, interpolation='linear')))"
        )
    if lowered == "uof" and len(args) == 1:
        return (
            f"({args[0]}.quantile(0.75, interpolation='linear') + 3.0 * "
            f"({args[0]}.quantile(0.75, interpolation='linear') - {args[0]}.quantile(0.25, interpolation='linear')))"
        )
    if lowered == "lav" and len(args) == 1:
        return f"{args[0]}.filter({args[0]} >= {_polars_lif_expr(args[0])}).min()"
    if lowered == "uav" and len(args) == 1:
        return f"{args[0]}.filter({args[0]} <= {_polars_uif_expr(args[0])}).max()"
    if lowered == "outliers" and len(args) == 1:
        return f"{args[0]}.filter({_polars_outlier_predicate(args[0])}).count()"
    if lowered == "pctoutliers" and len(args) == 1:
        return (
            f"({args[0]}.filter({_polars_outlier_predicate(args[0])}).count() / {args[0]}.count())"
        )
    if lowered == "meandeviation" and len(args) == 1:
        return f"({args[0]} - {args[0]}.mean()).abs().mean()"
    if lowered == "medianabsolutedeviation" and len(args) == 1:
        return f"({args[0]} - {args[0]}.median()).abs().median()"
    if lowered == "trimmedmean" and len(args) == 2:
        lower = _normalize_trim_tail_fraction(raw_args[1])
        upper = f"1 - {lower}"
        return (
            f"{args[0]}.filter(({args[0]} >= {args[0]}.quantile({lower}, interpolation='linear')) & "
            f"({args[0]} <= {args[0]}.quantile({upper}, interpolation='linear'))).mean()"
        )
    if lowered == "var" and len(args) == 1:
        return f"{args[0]}.var()"
    if lowered == "covariance" and len(args) == 2:
        return f"pl.cov({args[0]}, {args[1]})"
    if lowered == "weightedaverage" and len(args) == 2:
        return f"(({args[0]} * {args[1]}).sum() / {args[1]}.sum())"
    if lowered == "mostcommon" and len(args) == 1:
        return f"{args[0]}.mode().first()"
    if lowered == "uniqueconcatenate" and len(args) == 1:
        return f"{args[0]}.cast(pl.String).unique().implode().list.join(',')"
    if lowered in {"valueformax", "lastvalueformax"} and len(args) == 2:
        return f"{args[0]}.sort_by({args[1]}).last()"
    if lowered in {"valueformin", "lastvalueformin"} and len(args) == 2:
        return f"{args[0]}.sort_by({args[1]}).first()"
    if lowered == "percent" and len(args) == 2:
        return f"pl.when(({args[1]}) == 0).then(pl.lit(None)).otherwise(({args[0]}) / ({args[1]}))"
    if lowered == "geometricmean" and len(args) == 1:
        return f"{args[0]}.log().mean().exp()"
    if lowered == "stderr" and len(args) == 1:
        return f"({args[0]}.std() / {args[0]}.count().sqrt())"
    if lowered == "l95" and len(args) == 1:
        return f"({args[0]}.mean() - 1.96 * {args[0]}.std() / {args[0]}.count().sqrt())"
    if lowered == "u95" and len(args) == 1:
        return f"({args[0]}.mean() + 1.96 * {args[0]}.std() / {args[0]}.count().sqrt())"
    if lowered == "nthlargest" and len(args) == 2:
        return f"{args[0]}.sort(descending=True).gather(({args[1]}) - 1).first()"
    if lowered == "nthsmallest" and len(args) == 2:
        return f"{args[0]}.sort().gather(({args[1]}) - 1).first()"
    if lowered == "isnull" and len(args) == 1:
        return f"{args[0]}.is_null()"
    if lowered == "sn" and len(args) == 2:
        return f"{args[0]}.fill_null({args[1]})"
    if lowered == "parsereal" and len(args) == 1:
        return f"{args[0]}.cast(pl.Float64)"
    if lowered == "parsedate" and len(args) == 1:
        return f"{args[0]}.cast(pl.Date)"
    if lowered == "parsedate" and len(args) >= 2:
        return f"{args[0]}.str.strptime(pl.Date, format={_polars_plain_literal(raw_args[1])}, strict=False)"
    if lowered == "parsedatetime" and len(args) == 1:
        return f"{args[0]}.cast(pl.Datetime)"
    if lowered == "parsedatetime" and len(args) >= 2:
        return f"{args[0]}.str.strptime(pl.Datetime, format={_polars_plain_literal(raw_args[1])}, strict=False)"
    if lowered == "parsetime" and len(args) == 1:
        return f"{args[0]}.cast(pl.Time)"
    if lowered == "parsetime" and len(args) >= 2:
        return f"{args[0]}.str.strptime(pl.Time, format={_polars_plain_literal(raw_args[1])}, strict=False)"
    if lowered == "dateadd" and len(args) == 3:
        unit = _normalize_polars_duration_unit(raw_args[0])
        return f"({args[2]} + {_polars_duration_expression(unit, args[1])})"
    if lowered == "datediff" and len(args) == 3:
        unit = _normalize_polars_duration_unit(raw_args[0])
        return f"({args[2]} - {args[1]}).dt.{_polars_total_duration_method(unit)}()"
    if lowered == "datepart" and len(args) == 2:
        return _rewrite_polars_date_part(raw_args[0], args[1])
    if lowered == "isoweek" and len(args) == 1:
        return f"{args[0]}.dt.week()"
    if lowered == "isoyear" and len(args) == 1:
        return f"{args[0]}.dt.iso_year()"
    if lowered == "yearandweek" and len(args) == 1:
        return f"{args[0]}.dt.strftime('%G-%V')"
    if lowered == "toepochseconds" and len(args) == 1:
        return f"{args[0]}.dt.epoch('s')"
    if lowered == "toepochmilliseconds" and len(args) == 1:
        return f"{args[0]}.dt.epoch('ms')"
    if lowered == "fromepochseconds" and len(args) == 1:
        return f"pl.from_epoch({args[0]}, time_unit='s')"
    if lowered == "fromepochmilliseconds" and len(args) == 1:
        return f"pl.from_epoch({args[0]}, time_unit='ms')"
    if lowered in {"days", "hours", "minutes", "seconds", "milliseconds"} and len(args) == 1:
        return _polars_duration_expression(
            lowered[:-1] if lowered != "milliseconds" else "millisecond", args[0]
        )
    if (
        lowered in {"totaldays", "totalhours", "totalminutes", "totalseconds", "totalmilliseconds"}
        and len(args) == 1
    ):
        return f"{args[0]}.dt.{_polars_total_function_method(lowered)}()"
    if lowered == "rand" and len(args) <= 1:
        return "(pl.int_range(0, pl.len()).shuffle().cast(pl.Float64) / pl.len())"
    if lowered == "randbetween" and len(args) >= 2:
        return f"((pl.int_range(0, pl.len()).shuffle() % (({args[1]}) - ({args[0]}) + 1)) + ({args[0]}))"
    if lowered == "denserank" and args:
        return _rewrite_polars_rank_function("dense", args, raw_args)
    if lowered == "rankreal" and args:
        return _rewrite_polars_rank_function("average", args, raw_args)
    if (
        lowered in {"abs", "acos", "asin", "atan", "cos", "exp", "floor", "sin", "sqrt", "tan"}
        and len(args) == 1
    ):
        method = {"acos": "arccos", "asin": "arcsin", "atan": "arctan"}.get(lowered, lowered)
        return f"{args[0]}.{method}()"
    if lowered == "ceiling" and len(args) == 1:
        return f"{args[0]}.ceil()"
    if lowered in {"ln", "log"} and len(args) == 1:
        return f"{args[0]}.log()"
    if lowered == "log10" and len(args) == 1:
        return f"{args[0]}.log10()"
    if lowered == "lower" and len(args) == 1:
        return f"{args[0]}.str.to_lowercase()"
    if lowered == "upper" and len(args) == 1:
        return f"{args[0]}.str.to_uppercase()"
    if lowered == "trim" and len(args) == 1:
        return f"{args[0]}.str.strip_chars()"
    if lowered == "left" and len(args) == 2:
        return f"{args[0]}.str.slice(0, {args[1]})"
    if lowered == "substring" and len(args) in {2, 3}:
        length = f", {args[2]}" if len(args) == 3 else ""
        return f"{args[0]}.str.slice(({args[1]}) - 1{length})"
    if lowered == "len" and len(args) == 1:
        return f"{args[0]}.str.len_chars()"
    if lowered == "right" and len(args) == 2:
        return f"{args[0]}.str.slice(-({args[1]}))"
    if lowered == "mid" and len(args) == 3:
        return f"{args[0]}.str.slice(({args[1]}) - 1, {args[2]})"
    if lowered == "substitute" and len(args) == 3:
        return f"{args[0]}.str.replace_all({_polars_plain_literal(raw_args[1])}, {_polars_plain_literal(raw_args[2])}, literal=True)"
    if lowered in {"find", "charindex"} and len(args) == 2:
        position = f"{args[1]}.str.find({_polars_plain_literal(raw_args[0])})"
        return f"pl.when({position}.is_null()).then(pl.lit(0)).otherwise({position} + 1)"
    if lowered in {"find", "charindex"} and len(args) == 3:
        sliced = f"{args[1]}.str.slice(({args[2]}) - 1)"
        relative = f"{sliced}.str.find({_polars_plain_literal(raw_args[0])})"
        return f"pl.when({relative}.is_null()).then(pl.lit(0)).otherwise({relative} + {args[2]})"
    if lowered == "rxextract" and len(args) in {2, 3}:
        group_index = args[2] if len(args) == 3 else "1"
        return f"{args[0]}.str.extract({_polars_plain_literal(raw_args[1])}, group_index={group_index})"
    if lowered == "rxreplace" and len(args) >= 3:
        return f"{args[0]}.str.replace_all({_polars_plain_literal(raw_args[1])}, {_polars_plain_literal(raw_args[2])})"
    if lowered == "split" and len(args) == 2:
        return f"{args[0]}.str.split({_polars_plain_literal(raw_args[1])})"
    if lowered == "split" and len(args) == 3:
        index = _parse_numeric_literal(raw_args[2].strip())
        polars_index = repr(int(index) - 1) if index is not None else f"({args[2]} - 1)"
        return f"{args[0]}.str.split({_polars_plain_literal(raw_args[1])}).list.get({polars_index})"
    if lowered == "concatenate" and args:
        return "pl.concat_str([" + ", ".join(args) + "])"
    return f"{lowered}({', '.join(args)})"


def _normalize_polars_cast_type(value: str) -> str:
    normalized = re.sub(r"\s+", "", value.strip().lower())
    type_map = {
        "int": "pl.Int64",
        "integer": "pl.Int64",
        "long": "pl.Int64",
        "longinteger": "pl.Int64",
        "real": "pl.Float64",
        "double": "pl.Float64",
        "single": "pl.Float32",
        "singlereal": "pl.Float32",
        "decimal": "pl.Decimal(38, 10)",
        "currency": "pl.Decimal(38, 10)",
        "string": "pl.String",
        "varchar": "pl.String",
        "date": "pl.Date",
        "datetime": "pl.Datetime",
        "timestamp": "pl.Datetime",
        "time": "pl.Time",
        "boolean": "pl.Boolean",
        "bool": "pl.Boolean",
    }
    return type_map.get(normalized, value.strip())


def _polars_function_cast_target(function_name: str) -> str | None:
    return {
        "integer": "pl.Int64",
        "longinteger": "pl.Int64",
        "real": "pl.Float64",
        "single": "pl.Float32",
        "singlereal": "pl.Float32",
        "decimal": "pl.Decimal(38, 10)",
        "currency": "pl.Decimal(38, 10)",
        "string": "pl.String",
        "date": "pl.Date",
        "datetime": "pl.Datetime",
        "time": "pl.Time",
        "boolean": "pl.Boolean",
    }.get(function_name)


def _rewrite_polars_date_part(raw_unit: str, value_expr: str) -> str:
    unit = _normalize_date_part_literal(_polars_plain_literal(raw_unit)).strip("'\"").lower()
    methods = {
        "year": "year",
        "yyyy": "year",
        "month": "month",
        "mm": "month",
        "day": "day",
        "dd": "day",
        "hour": "hour",
        "hh": "hour",
        "minute": "minute",
        "mi": "minute",
        "second": "second",
        "ss": "second",
        "week": "week",
        "quarter": "quarter",
    }
    method = methods.get(unit)
    if method is None:
        return f"{value_expr}.dt.{unit}()"
    return f"{value_expr}.dt.{method}()"


def _normalize_polars_duration_unit(raw_unit: str) -> str:
    unit = _normalize_date_part_literal(_polars_plain_literal(raw_unit)).strip("'\"").lower()
    unit_map = {
        "day": "day",
        "days": "day",
        "dd": "day",
        "hour": "hour",
        "hours": "hour",
        "hh": "hour",
        "minute": "minute",
        "minutes": "minute",
        "mi": "minute",
        "second": "second",
        "seconds": "second",
        "ss": "second",
        "millisecond": "millisecond",
        "milliseconds": "millisecond",
        "ms": "millisecond",
    }
    try:
        return unit_map[unit]
    except KeyError as exc:
        raise ValueError(
            "Polars backend supports DateAdd/DateDiff only for "
            "day, hour, minute, second, and millisecond units"
        ) from exc


def _polars_duration_expression(unit: str, amount_expr: str) -> str:
    keyword = {
        "day": "days",
        "hour": "hours",
        "minute": "minutes",
        "second": "seconds",
        "millisecond": "milliseconds",
    }[unit]
    return f"pl.duration({keyword}={amount_expr})"


def _polars_total_duration_method(unit: str) -> str:
    return {
        "day": "total_days",
        "hour": "total_hours",
        "minute": "total_minutes",
        "second": "total_seconds",
        "millisecond": "total_milliseconds",
    }[unit]


def _polars_total_function_method(function_name: str) -> str:
    return {
        "totaldays": "total_days",
        "totalhours": "total_hours",
        "totalminutes": "total_minutes",
        "totalseconds": "total_seconds",
        "totalmilliseconds": "total_milliseconds",
    }[function_name]


def _polars_iqr_expr(value_expr: str) -> str:
    return (
        f"({value_expr}.quantile(0.75, interpolation='linear') - "
        f"{value_expr}.quantile(0.25, interpolation='linear'))"
    )


def _polars_lif_expr(value_expr: str) -> str:
    return f"({value_expr}.quantile(0.25, interpolation='linear') - 1.5 * {_polars_iqr_expr(value_expr)})"


def _polars_uif_expr(value_expr: str) -> str:
    return f"({value_expr}.quantile(0.75, interpolation='linear') + 1.5 * {_polars_iqr_expr(value_expr)})"


def _polars_outlier_predicate(value_expr: str) -> str:
    return f"(({value_expr} < {_polars_lif_expr(value_expr)}) | ({value_expr} > {_polars_uif_expr(value_expr)}))"


def _rewrite_polars_rank_function(method: str, args: list[str], raw_args: list[str]) -> str:
    direction = "False"
    partition_args: list[str] = []
    for raw_arg, arg in zip(raw_args[1:], args[1:]):
        literal = _strip_spotfire_literal(raw_arg)
        if literal is None:
            partition_args.append(_polars_partition_argument(raw_arg))
            continue
        normalized = literal.strip().lower()
        if normalized in {"asc", "ascending"}:
            direction = "False"
        elif normalized in {"desc", "descending"}:
            direction = "True"
        elif not normalized.startswith("ties.method="):
            partition_args.append(arg)
    expression = f"{args[0]}.rank(method='{method}', descending={direction})"
    if partition_args:
        expression = f"{expression}.over({', '.join(partition_args)})"
    return expression


def _strip_spotfire_literal(value: str) -> str | None:
    stripped = value.strip()
    if stripped.startswith("'") and stripped.endswith("'"):
        return _strip_sql_string_literal(stripped)
    if stripped.startswith('"') and stripped.endswith('"'):
        return stripped[1:-1].replace('""', '"')
    return None


def _apply_polars_over(expression: str, partition_args: list[str]) -> str:
    if not partition_args:
        return expression
    return (
        f"{expression}.over({', '.join(_polars_partition_argument(arg) for arg in partition_args)})"
    )


def _polars_partition_argument(argument: str) -> str:
    stripped = argument.strip()
    if stripped.startswith('"') and stripped.endswith('"'):
        return _python_string_literal(stripped[1:-1].replace('""', '"'))
    if stripped.startswith("["):
        identifier, end_index = _read_bracket_identifier(stripped, 0)
        if end_index == len(stripped):
            return _python_string_literal(identifier)
    return _rewrite_expression_for_polars(stripped)


def _rewrite_case_expression_for_polars(expression: str) -> str:
    body = expression.strip()[4:].strip()
    if _word_at_end(body, "end"):
        body = body[:-3].strip()
    cases: list[tuple[str, str]] = []
    else_expr = "pl.lit(None)"
    cursor = 0
    while True:
        when_start = _find_top_level_keyword(body, "when", cursor)
        if when_start is None:
            else_start = _find_top_level_keyword(body, "else", cursor)
            if else_start is not None:
                else_expr = _rewrite_expression_for_polars(body[else_start + 4 :].strip())
            break
        then_start = _find_top_level_keyword(body, "then", when_start + 4)
        if then_start is None:
            raise ValueError(f"CASE expression missing THEN: {expression}")
        next_when = _find_top_level_keyword(body, "when", then_start + 4)
        next_else = _find_top_level_keyword(body, "else", then_start + 4)
        value_end_candidates = [item for item in [next_when, next_else] if item is not None]
        value_end = min(value_end_candidates) if value_end_candidates else len(body)
        condition = body[when_start + 4 : then_start].strip()
        value = body[then_start + 4 : value_end].strip()
        cases.append(
            (_rewrite_expression_for_polars(condition), _rewrite_expression_for_polars(value))
        )
        if next_else is not None and (next_when is None or next_else < next_when):
            else_expr = _rewrite_expression_for_polars(body[next_else + 4 :].strip())
            break
        cursor = value_end
    if not cases:
        raise ValueError(f"CASE expression has no WHEN clauses: {expression}")
    result = f"pl.when({cases[0][0]}).then({cases[0][1]})"
    for condition, value in cases[1:]:
        result += f".when({condition}).then({value})"
    return result + f".otherwise({else_expr})"


def _strip_outer_parentheses(expression: str) -> str:
    stripped = expression.strip()
    while stripped.startswith("(") and stripped.endswith(")"):
        try:
            _inner, end_index = _read_parenthesized(stripped, 0)
        except ValueError:
            break
        if end_index != len(stripped):
            break
        stripped = stripped[1:-1].strip()
    return stripped


def _word_at_end(expression: str, word: str) -> bool:
    return re.search(rf"\b{re.escape(word)}\s*$", expression, flags=re.IGNORECASE) is not None


def _strip_leading_word(expression: str, word: str) -> str | None:
    match = re.match(
        rf"\s*{re.escape(word)}\b(?P<tail>.*)$", expression, flags=re.IGNORECASE | re.DOTALL
    )
    return match.group("tail").strip() if match else None


def _split_top_level_word_operator(expression: str, operator: str) -> tuple[str, str] | None:
    index = _find_top_level_keyword(expression, operator)
    if index is None:
        return None
    return expression[:index].strip(), expression[index + len(operator) :].strip()


def _split_top_level_symbol_operator(
    expression: str,
    operators: list[str],
    *,
    scan_right: bool = False,
) -> tuple[str, str, str] | None:
    ranges = range(len(expression) - 1, -1, -1) if scan_right else range(len(expression))
    for index in ranges:
        if not _is_top_level_position(expression, index):
            continue
        for operator in sorted(operators, key=len, reverse=True):
            if expression.startswith(operator, index):
                if operator in {"+", "-"} and _is_unary_numeric_sign(expression, index):
                    continue
                return (
                    expression[:index].strip(),
                    operator,
                    expression[index + len(operator) :].strip(),
                )
    return None


def _split_top_level_symbol_chain(expression: str, operator: str) -> list[str] | None:
    parts: list[str] = []
    cursor = 0
    index = 0
    while index < len(expression):
        if expression.startswith(operator, index) and _is_top_level_position(expression, index):
            parts.append(expression[cursor:index].strip())
            cursor = index + len(operator)
            index = cursor
            continue
        index += 1
    if not parts:
        return None
    parts.append(expression[cursor:].strip())
    return parts


def _find_top_level_keyword(expression: str, keyword: str, start: int = 0) -> int | None:
    lowered = keyword.lower()
    index = start
    while index < len(expression):
        if not _is_top_level_position(expression, index):
            index += 1
            continue
        if expression[index : index + len(keyword)].lower() == lowered:
            before = expression[index - 1] if index > 0 else " "
            after_index = index + len(keyword)
            after = expression[after_index] if after_index < len(expression) else " "
            if not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_"):
                return index
        index += 1
    return None


def _is_top_level_position(expression: str, target_index: int) -> bool:
    depth = 0
    index = 0
    while index < target_index:
        char = expression[index]
        if char == "'":
            _literal, next_index = _read_single_quoted_literal(expression, index)
            if next_index > target_index:
                return False
            index = next_index
            continue
        if char == '"':
            _quoted, next_index = _read_double_quoted_segment(expression, index)
            if next_index > target_index:
                return False
            index = next_index
            continue
        if char == "[":
            _identifier, next_index = _read_bracket_identifier(expression, index)
            if next_index > target_index:
                return False
            index = next_index
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(depth - 1, 0)
        index += 1
    return depth == 0


def _is_unary_numeric_sign(expression: str, index: int) -> bool:
    if expression[index] not in {"+", "-"}:
        return False
    previous = expression[:index].rstrip()
    if not previous:
        return True
    return previous[-1] in "([,+-*/<>=~"


def _rewrite_polars_list_literal(expression: str) -> str:
    stripped = expression.strip()
    if stripped.startswith("(") and stripped.endswith(")"):
        inner, end_index = _read_parenthesized(stripped, 0)
        if end_index == len(stripped):
            values = [_polars_plain_literal(item) for item in _split_top_level_arguments(inner)]
            return "[" + ", ".join(values) + "]"
    return _rewrite_expression_for_polars(stripped)


def _polars_plain_literal(expression: str) -> str:
    stripped = expression.strip()
    if stripped.startswith("'") and stripped.endswith("'"):
        return _python_string_literal(_strip_sql_string_literal(stripped) or "")
    if stripped.startswith('"') and stripped.endswith('"'):
        return _python_string_literal(stripped[1:-1].replace('""', '"'))
    if stripped.lower() == "null":
        return "None"
    if stripped.lower() == "true":
        return "True"
    if stripped.lower() == "false":
        return "False"
    return stripped


def _python_string_literal(value: str) -> str:
    return repr(value)


def _rewrite_expression_for_duckdb(expression: str) -> str:
    result: list[str] = []
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "'":
            literal, index = _read_single_quoted_literal(expression, index)
            result.append(literal)
            continue
        if char == '"':
            literal, index = _read_double_quoted_segment(expression, index)
            result.append(_double_quoted_to_sql_string(literal))
            continue
        if char == "[":
            identifier, index = _read_bracket_identifier(expression, index)
            escaped = identifier.replace('"', '""')
            result.append(f'"{escaped}"')
            continue
        if expression.startswith("~=", index):
            index += 2
            while result and result[-1].isspace():
                result.pop()
            while index < len(expression) and expression[index].isspace():
                index += 1
            result.append(" LIKE ")
            if index < len(expression) and expression[index] == "'":
                literal, index = _read_single_quoted_literal(expression, index)
                result.append(_wrap_spotfire_like_literal(literal))
                continue
            if index < len(expression) and expression[index] == '"':
                literal, index = _read_double_quoted_segment(expression, index)
                result.append(_wrap_spotfire_like_literal(_double_quoted_to_sql_string(literal)))
                continue
            continue
        if char == "&":
            result.append("||")
            index += 1
            continue
        if char.isalpha() or char == "_":
            identifier, next_index = _read_identifier(expression, index)
            cursor = next_index
            while cursor < len(expression) and expression[cursor].isspace():
                cursor += 1
            if cursor < len(expression) and expression[cursor] == "(":
                inner_text, end_index = _read_parenthesized(expression, cursor)
                args = [
                    _rewrite_expression_for_duckdb(arg)
                    for arg in _split_top_level_arguments(inner_text)
                ]
                rewritten_call = _rewrite_function_call_for_duckdb(identifier, args)
                over_clause, final_index = _read_window_over_clause(expression, end_index)
                if over_clause is not None:
                    rewritten_call = _rewrite_window_function_call_for_duckdb(
                        function_name=identifier,
                        args=args,
                        row_expression=rewritten_call,
                        partition_args=over_clause,
                    )
                elif identifier.strip().lower() in DEFAULT_WINDOW_AGGREGATES:
                    rewritten_call = _rewrite_window_function_call_for_duckdb(
                        function_name=identifier,
                        args=args,
                        row_expression=rewritten_call,
                        partition_args=[],
                    )
                result.append(rewritten_call)
                index = final_index
                continue
            result.append(identifier)
            index = next_index
            continue
        result.append(char)
        index += 1
    return "".join(result)


def _rewrite_function_call_for_duckdb(function_name: str, args: list[str]) -> str:
    lowered = function_name.strip().lower()

    if lowered == "average" and args:
        return f"avg({', '.join(args)})"
    if lowered in {"sum", "avg", "count", "max", "min"}:
        return f"{lowered}({', '.join(args)})"
    if lowered == "uniquecount" and len(args) == 1:
        return f"count(DISTINCT {args[0]})"
    if lowered == "dateadd" and len(args) == 3:
        unit = _normalize_date_part_literal(args[0])
        return f"date_add({args[2]}, ({args[1]}) * INTERVAL 1 {unit})"
    if lowered == "datediff" and len(args) == 3:
        unit = _normalize_date_part_literal(args[0])
        return f"date_diff('{unit}', {args[1]}, {args[2]})"
    if lowered == "datepart" and len(args) == 2:
        unit = _normalize_date_part_literal(args[0])
        return f"date_part('{unit}', {args[1]})"
    if lowered == "datetimenow" and not args:
        return "current_timestamp"
    if lowered in {"fiscalmonth", "fiscalquarter", "fiscalyear"} and args:
        offset = args[1] if len(args) >= 2 else "0"
        fiscal_date = f"date_add({args[0]}, -(({offset}) * INTERVAL 1 MONTH))"
        if lowered == "fiscalmonth":
            return f"date_part('month', {fiscal_date})"
        if lowered == "fiscalquarter":
            return f"date_part('quarter', {fiscal_date})"
        return f"date_part('year', {fiscal_date})"
    if lowered == "cast" and len(args) == 1:
        cast_match = re.match(
            r"^(?P<value>.+?)\s+as\s+(?P<type>[A-Za-z][A-Za-z0-9_(), ]*)$",
            args[0],
            flags=re.IGNORECASE,
        )
        if cast_match:
            target = _normalize_spotfire_cast_type(cast_match.group("type"))
            return f"CAST({cast_match.group('value').strip()} AS {target})"
    if lowered == "base64encode" and len(args) == 1:
        return f"base64(encode({args[0]}))"
    if lowered == "base64decode" and len(args) == 1:
        return f"decode(from_base64({args[0]}))"
    if lowered == "charindex" and len(args) == 2:
        return f"position({args[0]} IN {args[1]})"
    if lowered == "charindex" and len(args) == 3:
        remainder = f"substr({args[1]}, {args[2]})"
        relative = f"position({args[0]} IN {remainder})"
        return (
            "CASE "
            f"WHEN {args[2]} <= 1 THEN position({args[0]} IN {args[1]}) "
            f"WHEN {relative} = 0 THEN 0 "
            f"ELSE {relative} + {args[2]} - 1 "
            "END"
        )
    if lowered == "find" and len(args) == 2:
        return f"position({args[0]} IN {args[1]})"
    if lowered == "find" and len(args) == 3:
        remainder = f"substr({args[1]}, {args[2]})"
        relative = f"position({args[0]} IN {remainder})"
        return (
            "CASE "
            f"WHEN {args[2]} <= 1 THEN position({args[0]} IN {args[1]}) "
            f"WHEN {relative} = 0 THEN 0 "
            f"ELSE {relative} + {args[2]} - 1 "
            "END"
        )
    if lowered == "isnull" and len(args) == 1:
        return f"({args[0]} IS NULL)"
    if lowered == "if" and len(args) == 3:
        return f"(CASE WHEN {args[0]} THEN {args[1]} ELSE {args[2]} END)"
    if lowered == "rxextract" and len(args) in {2, 3}:
        return f"regexp_extract({', '.join(args)})"
    if lowered == "rxreplace" and len(args) == 3:
        return f"regexp_replace({', '.join(args)}, 'g')"
    if lowered == "rxreplace" and len(args) == 4:
        return f"regexp_replace({', '.join(args)})"
    if lowered == "concatenate" and args:
        return f"concat({', '.join(args)})"
    if lowered == "split" and len(args) == 2:
        return f"string_split({args[0]}, {args[1]})"
    if lowered == "split" and len(args) == 3:
        return f"list_extract(string_split({args[0]}, {args[1]}), {args[2]})"
    if lowered == "len" and len(args) == 1:
        return f"length({args[0]})"
    if lowered == "right" and len(args) == 2:
        return f"right({args[0]}, {args[1]})"
    if lowered == "mid" and len(args) == 3:
        return f"substr({args[0]}, {args[1]}, {args[2]})"
    if lowered == "substitute" and len(args) == 3:
        return f"replace({args[0]}, {args[1]}, {args[2]})"
    if lowered == "fromepochseconds" and len(args) == 1:
        return f"to_timestamp({args[0]})"
    if lowered == "fromepochmilliseconds" and len(args) == 1:
        return f"to_timestamp(({args[0]})::DOUBLE / 1000.0)"
    if lowered == "timespan" and len(args) == 1:
        return f"CAST({args[0]} AS INTERVAL)"
    if lowered == "timespan" and len(args) == 5:
        return (
            f"(({args[0]}) * INTERVAL 1 DAY + "
            f"({args[1]}) * INTERVAL 1 HOUR + "
            f"({args[2]}) * INTERVAL 1 MINUTE + "
            f"({args[3]}) * INTERVAL 1 SECOND + "
            f"({args[4]}) * INTERVAL 1 MILLISECOND)"
        )
    if lowered == "parsetimespan" and len(args) == 1:
        return f"CAST({args[0]} AS INTERVAL)"
    if lowered == "days" and len(args) == 1:
        return f"(({args[0]}) * INTERVAL 1 DAY)"
    if lowered == "hours" and len(args) == 1:
        return f"(({args[0]}) * INTERVAL 1 HOUR)"
    if lowered == "minutes" and len(args) == 1:
        return f"(({args[0]}) * INTERVAL 1 MINUTE)"
    if lowered == "seconds" and len(args) == 1:
        return f"(({args[0]}) * INTERVAL 1 SECOND)"
    if lowered == "milliseconds" and len(args) == 1:
        return f"(({args[0]}) * INTERVAL 1 MILLISECOND)"
    if lowered == "totaldays" and len(args) == 1:
        return f"(epoch({args[0]}) / 86400.0)"
    if lowered == "totalhours" and len(args) == 1:
        return f"(epoch({args[0]}) / 3600.0)"
    if lowered == "totalminutes" and len(args) == 1:
        return f"(epoch({args[0]}) / 60.0)"
    if lowered == "totalseconds" and len(args) == 1:
        return f"epoch({args[0]})"
    if lowered == "totalmilliseconds" and len(args) == 1:
        return f"epoch_ms({args[0]})"
    if lowered == "toepochseconds" and len(args) == 1:
        return f"epoch({args[0]})"
    if lowered == "toepochmilliseconds" and len(args) == 1:
        return f"epoch_ms({args[0]})"
    if lowered == "isoweek" and len(args) == 1:
        return f"week({args[0]})"
    if lowered == "isoyear" and len(args) == 1:
        return f"isoyear({args[0]})"
    if lowered == "yearandweek" and len(args) == 1:
        return f"strftime({args[0]}, '%G-%V')"
    if lowered == "percentile" and len(args) == 2:
        return f"quantile_cont({args[0]}, {_normalize_percentile_argument(args[1])})"
    if lowered == "p10" and len(args) == 1:
        return f"quantile_cont({args[0]}, 0.1)"
    if lowered == "p90" and len(args) == 1:
        return f"quantile_cont({args[0]}, 0.9)"
    if lowered == "q1" and len(args) == 1:
        return f"quantile_cont({args[0]}, 0.25)"
    if lowered == "q3" and len(args) == 1:
        return f"quantile_cont({args[0]}, 0.75)"
    if lowered == "iqr" and len(args) == 1:
        return f"(quantile_cont({args[0]}, 0.75) - quantile_cont({args[0]}, 0.25))"
    if lowered == "lif" and len(args) == 1:
        return f"(quantile_cont({args[0]}, 0.25) - 1.5 * (quantile_cont({args[0]}, 0.75) - quantile_cont({args[0]}, 0.25)))"
    if lowered == "uif" and len(args) == 1:
        return f"(quantile_cont({args[0]}, 0.75) + 1.5 * (quantile_cont({args[0]}, 0.75) - quantile_cont({args[0]}, 0.25)))"
    if lowered == "lof" and len(args) == 1:
        return f"(quantile_cont({args[0]}, 0.25) - 3.0 * (quantile_cont({args[0]}, 0.75) - quantile_cont({args[0]}, 0.25)))"
    if lowered == "uof" and len(args) == 1:
        return f"(quantile_cont({args[0]}, 0.75) + 3.0 * (quantile_cont({args[0]}, 0.75) - quantile_cont({args[0]}, 0.25)))"
    if lowered == "median" and len(args) == 1:
        return f"median({args[0]})"
    if lowered == "meandeviation" and len(args) > 1:
        arg_list = "[" + ", ".join(args) + "]"
        return f"list_aggr(list_transform({arg_list}, x -> abs(x - list_aggr({arg_list}, 'avg'))), 'avg')"
    if lowered == "medianabsolutedeviation" and len(args) == 1:
        return f"mad({args[0]})"
    if lowered == "medianabsolutedeviation" and len(args) > 1:
        return f"list_aggr(list_transform([{', '.join(args)}], x -> abs(x - list_aggr([{', '.join(args)}], 'median'))), 'median')"
    if lowered == "geometricmean" and len(args) == 1:
        return f"exp(avg(ln({args[0]})))"
    if lowered == "stderr" and len(args) == 1:
        return f"(stddev_samp({args[0]}) / sqrt(count({args[0]})))"
    if lowered == "l95" and len(args) == 1:
        return f"(avg({args[0]}) - 1.96 * stddev_samp({args[0]}) / sqrt(count({args[0]})))"
    if lowered == "u95" and len(args) == 1:
        return f"(avg({args[0]}) + 1.96 * stddev_samp({args[0]}) / sqrt(count({args[0]})))"
    if lowered == "nthlargest" and len(args) == 2:
        return f"list_extract(list_sort(list({args[0]}), 'DESC'), {args[1]})"
    if lowered == "nthsmallest" and len(args) == 2:
        return f"list_extract(list_sort(list({args[0]}), 'ASC'), {args[1]})"
    if lowered == "percent" and len(args) == 2:
        return f"(({args[0]})::DOUBLE / nullif({args[1]}, 0))"
    if lowered == "var" and len(args) == 1:
        return f"var_samp({args[0]})"
    if lowered == "countbig" and len(args) == 1:
        return f"count({args[0]})"
    if lowered == "covariance" and len(args) == 2:
        return f"covar_samp({args[0]}, {args[1]})"
    if lowered == "weightedaverage" and len(args) == 2:
        return f"(sum(({args[0]}) * ({args[1]})) / nullif(sum({args[1]}), 0))"
    if lowered == "mostcommon" and len(args) == 1:
        return f"mode({args[0]})"
    if lowered == "uniqueconcatenate" and len(args) == 1:
        return f"string_agg(DISTINCT CAST({args[0]} AS VARCHAR), ',')"
    if lowered == "valueformax" and len(args) == 2:
        return f"arg_max({args[0]}, {args[1]})"
    if lowered == "valueformin" and len(args) == 2:
        return f"arg_min({args[0]}, {args[1]})"
    if lowered == "lastvalueformax" and len(args) == 2:
        return f"arg_max({args[0]}, {args[1]})"
    if lowered == "lastvalueformin" and len(args) == 2:
        return f"arg_min({args[0]}, {args[1]})"
    if lowered == "parsereal" and args:
        return f"CAST({args[0]} AS DOUBLE)"
    if lowered == "parsedate" and len(args) == 1:
        return f"CAST({args[0]} AS DATE)"
    if lowered == "parsedate" and len(args) >= 2:
        return f"strptime({args[0]}, {args[1]})::DATE"
    if lowered == "parsedatetime" and len(args) == 1:
        return f"CAST({args[0]} AS TIMESTAMP)"
    if lowered == "parsedatetime" and len(args) >= 2:
        return f"strptime({args[0]}, {args[1]})"
    if lowered == "parsetime" and len(args) == 1:
        return f"CAST({args[0]} AS TIME)"
    if lowered == "parsetime" and len(args) >= 2:
        return f"strptime({args[0]}, {args[1]})::TIME"
    if lowered == "sn" and len(args) == 2:
        return f"coalesce({args[0]}, {args[1]})"
    if lowered == "rand" and len(args) <= 1:
        return "random()"
    if lowered == "randbetween" and len(args) >= 2:
        return f"floor(random() * (({args[1]}) - ({args[0]}) + 1) + ({args[0]}))"
    if lowered == "denserank" and args:
        return _rewrite_rank_function("dense_rank", args)
    if lowered == "rankreal" and args:
        return _rewrite_rank_function("rank", args)

    cast_targets = {
        "integer": "INTEGER",
        "longinteger": "BIGINT",
        "real": "DOUBLE",
        "single": "REAL",
        "singlereal": "REAL",
        "decimal": "DECIMAL",
        "currency": "DECIMAL",
        "string": "VARCHAR",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "time": "TIME",
        "boolean": "BOOLEAN",
    }
    cast_target = cast_targets.get(lowered)
    if cast_target is not None and len(args) == 1:
        cast_arg = _normalize_cast_argument(args[0])
        return f"CAST({cast_arg} AS {cast_target})"

    return f"{function_name}({', '.join(args)})"


def _normalize_date_part_literal(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == "'" and stripped[-1] == "'":
        stripped = stripped[1:-1].replace("''", "'")
    return stripped.strip().lower()


def _normalize_cast_argument(value: str) -> str:
    return value


def _normalize_spotfire_cast_type(value: str) -> str:
    normalized = re.sub(r"\s+", "", value.strip().lower())
    type_map = {
        "int": "INTEGER",
        "integer": "INTEGER",
        "longinteger": "BIGINT",
        "long": "BIGINT",
        "real": "DOUBLE",
        "single": "REAL",
        "singlereal": "REAL",
        "decimal": "DECIMAL",
        "currency": "DECIMAL",
        "string": "VARCHAR",
        "varchar": "VARCHAR",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "time": "TIME",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
    }
    return type_map.get(normalized, value.strip().upper())


def _normalize_percentile_argument(value: str) -> str:
    stripped = value.strip()
    numeric = _parse_numeric_literal(stripped)
    if numeric is not None:
        return repr(numeric / 100.0 if abs(numeric) > 1 else numeric)
    return f"(({stripped})::DOUBLE / 100.0)"


def _normalize_trim_tail_fraction(value: str) -> str:
    stripped = value.strip()
    numeric = _parse_numeric_literal(stripped)
    if numeric is not None:
        percent = numeric / 100.0 if abs(numeric) > 1 else numeric
        return repr(percent / 2.0)
    return f"(({stripped})::DOUBLE / 200.0)"


def _parse_numeric_literal(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def _strip_sql_string_literal(value: str) -> str | None:
    stripped = value.strip()
    if len(stripped) < 2 or stripped[0] != "'" or stripped[-1] != "'":
        return None
    return stripped[1:-1].replace("''", "'")


def _rewrite_rank_function(rank_function: str, args: list[str]) -> str:
    value_arg = args[0]
    direction = "ASC"
    ties_method = "minimum"
    partition_args: list[str] = []
    for arg in args[1:]:
        literal = _strip_sql_string_literal(arg)
        if literal is None:
            partition_args.append(arg)
            continue
        normalized = literal.strip().lower()
        if normalized in {"asc", "ascending"}:
            direction = "ASC"
        elif normalized in {"desc", "descending"}:
            direction = "DESC"
        elif normalized.startswith("ties.method="):
            ties_method = normalized.split("=", 1)[1].strip()
    partition_sql = ""
    if partition_args:
        partition_sql = "PARTITION BY " + ", ".join(partition_args) + " "
    order_sql = f"ORDER BY {value_arg} {direction}"
    peer_partition_args = [*partition_args, value_arg]
    peer_partition_sql = "PARTITION BY " + ", ".join(peer_partition_args)
    if rank_function == "dense_rank":
        return f"dense_rank() OVER ({partition_sql}{order_sql})"
    if ties_method == "first":
        return f"row_number() OVER ({partition_sql}{order_sql})"
    if ties_method == "maximum":
        return (
            f"(rank() OVER ({partition_sql}{order_sql}) + count(*) OVER ({peer_partition_sql}) - 1)"
        )
    if ties_method == "average":
        return (
            f"(rank() OVER ({partition_sql}{order_sql}) + "
            f"rank() OVER ({partition_sql}{order_sql}) + "
            f"count(*) OVER ({peer_partition_sql}) - 1)::DOUBLE / 2.0"
        )
    return f"rank() OVER ({partition_sql}{order_sql})"


def _double_quoted_to_sql_string(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == '"' and stripped[-1] == '"':
        inner = stripped[1:-1].replace('""', '"')
        return "'" + inner.replace("'", "''") + "'"
    return value


def _wrap_spotfire_like_literal(value: str) -> str:
    stripped = value.strip()
    if len(stripped) < 2 or stripped[0] != "'" or stripped[-1] != "'":
        return value
    inner = stripped[1:-1]
    if "%" in inner or "_" in inner:
        return stripped
    return f"'%{inner}%'"


def _rewrite_window_function_call_for_duckdb(
    *,
    function_name: str,
    args: list[str],
    row_expression: str,
    partition_args: list[str],
) -> str:
    lowered = function_name.strip().lower()
    if lowered in {"meandeviation", "medianabsolutedeviation"} and len(args) > 1:
        return row_expression
    partition_sql = _build_partition_clause(partition_args)
    over_sql = f"OVER ({partition_sql})"

    def list_over(value: str) -> str:
        return f"list({value}) {over_sql}"

    def q(value: str, percentile: str) -> str:
        return f"quantile_cont({value}, {percentile}) {over_sql}"

    def iqr_sql(value: str) -> str:
        return f"({q(value, '0.75')} - {q(value, '0.25')})"

    def lif_sql(value: str) -> str:
        return f"({q(value, '0.25')} - 1.5 * {iqr_sql(value)})"

    def uif_sql(value: str) -> str:
        return f"({q(value, '0.75')} + 1.5 * {iqr_sql(value)})"

    if lowered == "sum":
        return f"sum({args[0]}) {over_sql}" if args else f"{row_expression} {over_sql}"
    if lowered in {"avg", "average"}:
        return f"avg({args[0]}) {over_sql}" if args else f"{row_expression} {over_sql}"
    if lowered == "count":
        return f"count({args[0]}) {over_sql}" if args else f"count(*) {over_sql}"
    if lowered == "countbig":
        return f"count({args[0]}) {over_sql}" if args else f"{row_expression} {over_sql}"
    if lowered == "uniquecount":
        return (
            f"count(DISTINCT {args[0]}) {over_sql}"
            if args
            else f"count(DISTINCT {row_expression}) {over_sql}"
        )
    if lowered == "firstvalidafter" and len(args) == 1:
        return (
            f"coalesce({args[0]}, first_value({args[0]} IGNORE NULLS) "
            f"OVER (ORDER BY {SYNTHETIC_ROW_ID_COLUMN} ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING))"
        )
    if lowered == "lastvalidbefore" and len(args) == 1:
        return (
            f"coalesce({args[0]}, last_value({args[0]} IGNORE NULLS) "
            f"OVER (ORDER BY {SYNTHETIC_ROW_ID_COLUMN} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))"
        )
    if lowered == "denserank" and args:
        return _rewrite_rank_function("dense_rank", args)
    if lowered == "rankreal" and args:
        return _rewrite_rank_function("rank", args)
    if lowered == "max":
        return f"max({args[0]}) {over_sql}" if args else f"{row_expression} {over_sql}"
    if lowered == "min":
        return f"min({args[0]}) {over_sql}" if args else f"{row_expression} {over_sql}"
    if lowered == "percentile" and len(args) == 2:
        return f"quantile_cont({args[0]}, {_normalize_percentile_argument(args[1])}) {over_sql}"
    if lowered == "p10" and len(args) == 1:
        return f"quantile_cont({args[0]}, 0.1) {over_sql}"
    if lowered == "p90" and len(args) == 1:
        return f"quantile_cont({args[0]}, 0.9) {over_sql}"
    if lowered == "q1" and len(args) == 1:
        return f"quantile_cont({args[0]}, 0.25) {over_sql}"
    if lowered == "q3" and len(args) == 1:
        return f"quantile_cont({args[0]}, 0.75) {over_sql}"
    if lowered == "iqr" and len(args) == 1:
        return iqr_sql(args[0])
    if lowered == "meandeviation" and len(args) == 1:
        values = list_over(args[0])
        return (
            f"list_aggr(list_transform({values}, x -> abs(x - list_aggr({values}, 'avg'))), 'avg')"
        )
    if lowered == "lif" and len(args) == 1:
        return lif_sql(args[0])
    if lowered == "uif" and len(args) == 1:
        return uif_sql(args[0])
    if lowered == "lof" and len(args) == 1:
        return f"({q(args[0], '0.25')} - 3.0 * {iqr_sql(args[0])})"
    if lowered == "uof" and len(args) == 1:
        return f"({q(args[0], '0.75')} + 3.0 * {iqr_sql(args[0])})"
    if lowered == "lav" and len(args) == 1:
        return (
            f"list_aggr(list_filter({list_over(args[0])}, x -> x >= ({lif_sql(args[0])})), 'min')"
        )
    if lowered == "uav" and len(args) == 1:
        return (
            f"list_aggr(list_filter({list_over(args[0])}, x -> x <= ({uif_sql(args[0])})), 'max')"
        )
    if lowered == "outliers" and len(args) == 1:
        return f"array_length(list_filter({list_over(args[0])}, x -> x < ({lif_sql(args[0])}) OR x > ({uif_sql(args[0])})))"
    if lowered == "pctoutliers" and len(args) == 1:
        outlier_count = f"array_length(list_filter({list_over(args[0])}, x -> x < ({lif_sql(args[0])}) OR x > ({uif_sql(args[0])})))"
        return f"({outlier_count})::DOUBLE / nullif(array_length({list_over(args[0])}), 0)"
    if lowered == "median" and len(args) == 1:
        return f"median({args[0]}) {over_sql}"
    if lowered == "medianabsolutedeviation" and len(args) == 1:
        return f"mad({args[0]}) {over_sql}"
    if lowered == "trimmedmean" and len(args) == 2:
        lower = _normalize_trim_tail_fraction(args[1])
        upper = f"1 - {lower}"
        return f"list_aggr(list_filter({list_over(args[0])}, x -> x BETWEEN {q(args[0], lower)} AND {q(args[0], upper)}), 'avg')"
    if lowered == "geometricmean" and len(args) == 1:
        return f"exp(avg(ln({args[0]})) {over_sql})"
    if lowered == "stderr" and len(args) == 1:
        return f"(stddev_samp({args[0]}) {over_sql} / sqrt(count({args[0]}) {over_sql}))"
    if lowered == "l95" and len(args) == 1:
        return (
            f"(avg({args[0]}) {over_sql} - 1.96 * "
            f"stddev_samp({args[0]}) {over_sql} / sqrt(count({args[0]}) {over_sql}))"
        )
    if lowered == "u95" and len(args) == 1:
        return (
            f"(avg({args[0]}) {over_sql} + 1.96 * "
            f"stddev_samp({args[0]}) {over_sql} / sqrt(count({args[0]}) {over_sql}))"
        )
    if lowered == "nthlargest" and len(args) == 2:
        return f"list_extract(list_sort(list({args[0]}) {over_sql}, 'DESC'), {args[1]})"
    if lowered == "nthsmallest" and len(args) == 2:
        return f"list_extract(list_sort(list({args[0]}) {over_sql}, 'ASC'), {args[1]})"
    if lowered == "percent" and len(args) == 2:
        return f"(({args[0]})::DOUBLE / nullif(sum({args[1]}) {over_sql}, 0))"
    if lowered == "var" and len(args) == 1:
        return f"var_samp({args[0]}) {over_sql}"
    if lowered == "covariance" and len(args) == 2:
        return f"covar_samp({args[0]}, {args[1]}) {over_sql}"
    if lowered == "weightedaverage" and len(args) == 2:
        return f"(sum(({args[0]}) * ({args[1]})) {over_sql} / nullif(sum({args[1]}) {over_sql}, 0))"
    if lowered == "mostcommon" and len(args) == 1:
        return f"mode({args[0]}) {over_sql}"
    if lowered == "uniqueconcatenate" and len(args) == 1:
        return f"string_agg(DISTINCT CAST({args[0]} AS VARCHAR), ',') {over_sql}"
    if lowered in {"valueformax", "lastvalueformax"} and len(args) == 2:
        return f"arg_max({args[0]}, {args[1]}) {over_sql}"
    if lowered in {"valueformin", "lastvalueformin"} and len(args) == 2:
        return f"arg_min({args[0]}, {args[1]}) {over_sql}"

    return f"{row_expression} {over_sql}"


def _build_partition_clause(partition_args: list[str]) -> str:
    if not partition_args:
        return ""
    return "PARTITION BY " + ", ".join(partition_args)


def pretty_format_expression(expression: str) -> str:
    """SQL-like expression을 CASE 단위 중심으로 보기 좋게 정리한다."""
    expression, _comments = _split_spotfire_line_comments(expression)
    tokens = _tokenize_expression(expression)
    lines: list[str] = []
    current: list[str] = []
    indent = 0

    def flush_line() -> None:
        text = _normalize_sql_token_spacing(current).strip()
        if not text:
            current.clear()
            return
        lines.append(("  " * indent) + text)
        current.clear()

    for token in tokens:
        upper = token.upper()
        if upper == "CASE":
            flush_line()
            current.append("CASE")
            flush_line()
            indent += 1
            continue
        if upper == "WHEN":
            flush_line()
            current.append("WHEN")
            continue
        if upper == "THEN":
            current.append("THEN")
            continue
        if upper == "ELSE":
            flush_line()
            current.append("ELSE")
            continue
        if upper == "END":
            flush_line()
            indent = max(indent - 1, 0)
            current.append("END")
            flush_line()
            continue
        current.append(token)

    flush_line()
    return "\n".join(lines)


def _normalize_sql_token_spacing(tokens: list[str]) -> str:
    result: list[str] = []
    no_space_before = {")", ","}
    no_space_after = {"("}
    binary_operators = {"+", "-", "*", "/", "||", "&", "=", "~=", "<", ">", "<=", ">=", "<>"}
    for token in tokens:
        if not result:
            result.append(token)
            continue
        previous = result[-1]
        if token in no_space_before or previous in no_space_after or token == "(" and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", previous):
            result.append(token)
        elif token in binary_operators or previous in binary_operators:
            result.append(" " + token)
        else:
            result.append(" " + token)
    return "".join(result)


def _tokenize_expression(expression: str) -> list[str]:
    tokens: list[str] = []
    current: list[str] = []
    index = 0
    in_single_quote = False
    while index < len(expression):
        char = expression[index]
        if in_single_quote:
            current.append(char)
            if char == "'" and not (index + 1 < len(expression) and expression[index + 1] == "'"):
                in_single_quote = False
            elif char == "'" and index + 1 < len(expression) and expression[index + 1] == "'":
                current.append(expression[index + 1])
                index += 1
            index += 1
            continue
        if char == "'":
            if current:
                tokens.append("".join(current))
                current.clear()
            current.append(char)
            in_single_quote = True
            index += 1
            continue
        if char.isspace():
            if current:
                tokens.append("".join(current))
                current.clear()
            index += 1
            continue
        if expression.startswith("//", index):
            if current:
                tokens.append("".join(current))
                current.clear()
            end_index = expression.find("\n", index)
            if end_index == -1:
                tokens.append(expression[index:])
                break
            tokens.append(expression[index:end_index])
            index = end_index
            continue
        if expression.startswith("||", index) or expression.startswith("~=", index):
            if current:
                tokens.append("".join(current))
                current.clear()
            tokens.append(expression[index : index + 2])
            index += 2
            continue
        if char == "&":
            if current:
                tokens.append("".join(current))
                current.clear()
            tokens.append(char)
            index += 1
            continue
        if char in {"(", ")", ",", "+", "-", "*", "/"}:
            if current:
                tokens.append("".join(current))
                current.clear()
            tokens.append(char)
            index += 1
            continue
        current.append(char)
        index += 1
    if current:
        tokens.append("".join(current))
    return tokens


def _strip_spotfire_line_comments(expression: str) -> str:
    """Remove Spotfire-style // comments outside string and bracket identifiers."""
    return _split_spotfire_line_comments(expression)[0]


def _split_spotfire_line_comments(expression: str) -> tuple[str, list[str]]:
    """Split Spotfire-style // comments from expression text."""
    result: list[str] = []
    comments: list[str] = []
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "'":
            literal, index = _read_single_quoted_literal(expression, index)
            result.append(literal)
            continue
        if char == '"':
            quoted, index = _read_double_quoted_segment(expression, index)
            result.append(quoted)
            continue
        if char == "[":
            identifier, index = _read_bracket_identifier(expression, index)
            result.append(_to_bracket_identifier(identifier))
            continue
        if expression.startswith("//", index):
            end_index = expression.find("\n", index)
            if end_index == -1:
                comment_text = expression[index + 2 :].strip()
                if comment_text:
                    comments.append(comment_text)
                break
            comment_text = expression[index + 2 : end_index].strip()
            if comment_text:
                comments.append(comment_text)
            result.append("\n")
            index = end_index + 1
            continue
        result.append(char)
        index += 1
    return "".join(result).strip(), comments


def _read_identifier(expression: str, start: int) -> tuple[str, int]:
    index = start
    while index < len(expression) and (expression[index].isalnum() or expression[index] == "_"):
        index += 1
    return expression[start:index], index


def _read_single_quoted_literal(expression: str, start: int) -> tuple[str, int]:
    chars = ["'"]
    index = start + 1
    while index < len(expression):
        char = expression[index]
        chars.append(char)
        if char == "'" and index + 1 < len(expression) and expression[index + 1] == "'":
            chars.append(expression[index + 1])
            index += 2
            continue
        if char == "'":
            return "".join(chars), index + 1
        index += 1
    raise ValueError(f"Unclosed single-quoted literal in expression: {expression}")


def _read_double_quoted_segment(expression: str, start: int) -> tuple[str, int]:
    chars = ['"']
    index = start + 1
    while index < len(expression):
        char = expression[index]
        chars.append(char)
        if char == '"' and index + 1 < len(expression) and expression[index + 1] == '"':
            chars.append(expression[index + 1])
            index += 2
            continue
        if char == '"':
            return "".join(chars), index + 1
        index += 1
    raise ValueError(f"Unclosed double-quoted identifier in expression: {expression}")


def _read_parenthesized(expression: str, start: int) -> tuple[str, int]:
    if expression[start] != "(":
        raise ValueError("Parenthesized segment must start with '('.")

    depth = 1
    index = start + 1
    chars: list[str] = []
    while index < len(expression):
        char = expression[index]
        if char == "'":
            literal, index = _read_single_quoted_literal(expression, index)
            chars.append(literal)
            continue
        if char == '"':
            quoted, index = _read_double_quoted_segment(expression, index)
            chars.append(quoted)
            continue
        if char == "[":
            identifier, index = _read_bracket_identifier(expression, index)
            chars.append(_to_bracket_identifier(identifier))
            continue
        if char == "(":
            depth += 1
            chars.append(char)
            index += 1
            continue
        if char == ")":
            depth -= 1
            if depth == 0:
                return "".join(chars), index + 1
            chars.append(char)
            index += 1
            continue
        chars.append(char)
        index += 1
    raise ValueError(f"Unclosed parenthesis in expression: {expression}")


def _split_top_level_arguments(inner_text: str) -> list[str]:
    if not inner_text.strip():
        return []

    args: list[str] = []
    current: list[str] = []
    index = 0
    depth = 0
    while index < len(inner_text):
        char = inner_text[index]
        if char == "'":
            literal, index = _read_single_quoted_literal(inner_text, index)
            current.append(literal)
            continue
        if char == '"':
            quoted, index = _read_double_quoted_segment(inner_text, index)
            current.append(quoted)
            continue
        if char == "[":
            identifier, index = _read_bracket_identifier(inner_text, index)
            current.append(_to_bracket_identifier(identifier))
            continue
        if char == "(":
            depth += 1
            current.append(char)
            index += 1
            continue
        if char == ")":
            depth = max(depth - 1, 0)
            current.append(char)
            index += 1
            continue
        if char == "," and depth == 0:
            args.append("".join(current).strip())
            current.clear()
            index += 1
            continue
        current.append(char)
        index += 1

    tail = "".join(current).strip()
    if tail:
        args.append(tail)
    return args


def _read_window_over_clause(expression: str, start: int) -> tuple[list[str] | None, int]:
    cursor = start
    while cursor < len(expression) and expression[cursor].isspace():
        cursor += 1
    if cursor >= len(expression) or not expression[cursor].isalpha():
        return None, start

    keyword, next_index = _read_identifier(expression, cursor)
    if keyword.lower() != "over":
        return None, start

    cursor = next_index
    while cursor < len(expression) and expression[cursor].isspace():
        cursor += 1
    if cursor >= len(expression) or expression[cursor] != "(":
        return None, start

    inner_text, end_index = _read_parenthesized(expression, cursor)
    partition_args = [
        _rewrite_expression_for_duckdb(arg) for arg in _split_top_level_arguments(inner_text)
    ]
    return partition_args, end_index


def _strip_single_quoted_literals(expression: str) -> str:
    result: list[str] = []
    in_string = False
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "'":
            if in_string and index + 1 < len(expression) and expression[index + 1] == "'":
                index += 2
                continue
            in_string = not in_string
            result.append(" ")
        elif in_string:
            result.append(" ")
        else:
            result.append(char)
        index += 1
    return "".join(result)


def _rewrite_bracket_identifiers(
    expression: str,
    replacements: dict[str, str],
    *,
    skip_name: str | None = None,
) -> str:
    result: list[str] = []
    index = 0
    while index < len(expression):
        char = expression[index]
        if char != "[":
            result.append(char)
            index += 1
            continue
        identifier, next_index = _read_bracket_identifier(expression, index)
        replacement = replacements.get(identifier, identifier)
        if skip_name is not None and identifier == skip_name:
            replacement = identifier
        result.append(_to_bracket_identifier(replacement))
        index = next_index
    return "".join(result)


def _to_bracket_identifier(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"


def _extract_bracket_identifiers(expression: str) -> tuple[list[str], str]:
    tokens: list[str] = []
    result: list[str] = []
    index = 0
    while index < len(expression):
        char = expression[index]
        if char != "[":
            result.append(char)
            index += 1
            continue
        identifier, next_index = _read_bracket_identifier(expression, index)
        tokens.append(identifier)
        result.append(" ")
        index = next_index
    return tokens, "".join(result)


def _read_bracket_identifier(expression: str, start: int) -> tuple[str, int]:
    if expression[start] != "[":
        raise ValueError("Bracket identifier must start with '['.")

    chars: list[str] = []
    index = start + 1
    while index < len(expression):
        char = expression[index]
        if char == "]":
            if index + 1 < len(expression) and expression[index + 1] == "]":
                chars.append("]")
                index += 2
                continue
            return "".join(chars), index + 1
        chars.append(char)
        index += 1
    raise ValueError(f"Unclosed bracket identifier in expression: {expression}")
