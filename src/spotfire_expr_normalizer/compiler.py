from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from .ast import (
    AliasNode,
    BinaryNode,
    CallNode,
    CaseNode,
    CastNode,
    ExpressionNode,
    LiteralNode,
    UnaryNode,
    WindowNode,
    WindowOrder,
)
from .ir import ExpressionIrDocument, IrExpression, alias_expression
from .normalizer import (
    DerivedExpression,
    build_expression_layers,
    build_raw_expressions,
    canonicalize_expressions,
    load_duckdb_layered_expression_yaml,
    load_expression_items_from_csv,
    load_expression_items_from_yaml,
    load_polars_layered_expression_yaml,
)
from .parser import parse_expression

_WINDOW_FUNCTIONS = {
    "sum",
    "avg",
    "average",
    "min",
    "max",
    "median",
    "count",
    "uniquecount",
    "percentile",
    "p10",
    "p90",
    "q1",
    "q3",
    "iqr",
    "var",
    "covariance",
    "weightedaverage",
    "valueformax",
    "valueformin",
    "nthlargest",
    "nthsmallest",
    "medianabsolutedeviation",
    "trimmedmean",
    "lav",
    "uav",
    "outliers",
    "pctoutliers",
    "firstvalidafter",
    "lastvalidbefore",
    "variance",
    "product",
    "range",
    "stddev",
    "stderr",
    "geometricmean",
    "l95",
    "u95",
    "lif",
    "uif",
    "lof",
    "uof",
    "mostcommon",
    "uniqueconcatenate",
    "lastvalueformax",
    "lastvalueformin",
    "countbig",
    "first",
    "last",
    "lag",
    "lead",
}


@dataclass(frozen=True, slots=True)
class ExpressionIrPrepareResult:
    document: ExpressionIrDocument
    source_path: Path
    source_format: str
    ir_path: Path


def prepare_expression_ir(
    source_path: str | Path,
    *,
    source_format: str | None = None,
    result_name_field: str | None = None,
    sql_expression_field: str | None = None,
    output_path: str | Path | None = None,
) -> ExpressionIrPrepareResult:
    path = Path(source_path)
    normalized_format = _infer_ir_source_format(path, source_format)
    if normalized_format == "csv":
        items = load_expression_items_from_csv(
            path,
            result_name_field=result_name_field,
            sql_expression_field=sql_expression_field,
        )
        expressions, _ = canonicalize_expressions(build_raw_expressions(items))
    elif normalized_format == "spotfire_layered_yaml":
        expressions, _ = canonicalize_expressions(
            build_raw_expressions(load_expression_items_from_yaml(path))
        )
    elif normalized_format == "duckdb_layered_yaml":
        expressions = load_duckdb_layered_expression_yaml(path)
    elif normalized_format == "polars_layered_yaml":
        expressions = [
            DerivedExpression(
                name=item.name,
                expression=item.expression,
                normalized_expression=item.expression,
                dependencies=list(item.dependencies),
            )
            for item in load_polars_layered_expression_yaml(path)
        ]
    else:
        raise ValueError(f"Unsupported IR source format: {normalized_format}")

    document = compile_expressions_to_ir(expressions)
    ir_path = Path(output_path) if output_path else _ir_path_for_source(path)
    ir_path.write_text(
        json.dumps(document.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return ExpressionIrPrepareResult(document, path, normalized_format, ir_path)


def compile_expressions_to_ir(
    expressions: Iterable[DerivedExpression],
) -> ExpressionIrDocument:
    layers = build_expression_layers(list(expressions))
    return ExpressionIrDocument(
        layers=tuple(
            tuple(
                IrExpression(
                    name=item.name,
                    source=item.expression,
                    dependencies=tuple(item.dependencies),
                    node=alias_expression(
                        item.name,
                        _canonicalize_windows(parse_expression(item.expression)),
                    ),
                )
                for item in layer
            )
            for layer in layers
        )
    )


def _canonicalize_windows(node: ExpressionNode) -> ExpressionNode:
    if isinstance(node, WindowNode):
        expression = node.expression
        if isinstance(expression, CallNode):
            expression = CallNode(
                expression.function,
                tuple(_canonicalize_windows(item) for item in expression.arguments),
            )
        else:
            expression = _canonicalize_windows(expression)
        return WindowNode(
            expression,
            tuple(_canonicalize_windows(item) for item in node.partition_by),
            tuple(
                WindowOrder(
                    _canonicalize_windows(item.expression),
                    item.direction,
                    item.nulls,
                )
                for item in node.order_by
            ),
        )
    if isinstance(node, CallNode):
        arguments = tuple(_canonicalize_windows(item) for item in node.arguments)
        function = {"variance": "var", "countbig": "count"}.get(node.function, node.function)
        if function == "rownumber":
            if len(arguments) < 2 or not isinstance(arguments[1], LiteralNode):
                raise ValueError(
                    "RowNumber requires RowNumber(order_column, direction, [nulls], partition...)."
                )
            direction = str(arguments[1].value).lower()
            if direction not in {"asc", "desc"}:
                raise ValueError("RowNumber direction must be asc or desc.")
            option_end = 2
            nulls = "last"
            if len(arguments) > 2 and isinstance(arguments[2], LiteralNode):
                nulls = str(arguments[2].value).lower().replace("nulls_", "")
                if nulls not in {"first", "last"}:
                    raise ValueError("RowNumber null ordering must be nulls_first or nulls_last.")
                option_end = 3
            return WindowNode(
                CallNode("rownumber", ()),
                tuple(arguments[option_end:]),
                (WindowOrder(arguments[0], direction, nulls),),
            )
        if function in {"denserank", "rankreal"}:
            value_and_options: list[ExpressionNode] = [arguments[0]]
            partitions: list[ExpressionNode] = []
            for argument in arguments[1:]:
                if isinstance(argument, LiteralNode):
                    value_and_options.append(argument)
                else:
                    partitions.append(argument)
            return WindowNode(CallNode(function, tuple(value_and_options)), tuple(partitions))
        if function == "meandeviation" and len(arguments) == 1:
            return WindowNode(CallNode(function, arguments), ())
        if function in _WINDOW_FUNCTIONS:
            return WindowNode(CallNode(function, arguments), ())
        return CallNode(function, arguments)
    if isinstance(node, UnaryNode):
        return UnaryNode(node.operator, _canonicalize_windows(node.operand))
    if isinstance(node, BinaryNode):
        return BinaryNode(
            node.operator,
            _canonicalize_windows(node.left),
            _canonicalize_windows(node.right),
        )
    if isinstance(node, CaseNode):
        return CaseNode(
            tuple(
                (_canonicalize_windows(condition), _canonicalize_windows(value))
                for condition, value in node.branches
            ),
            _canonicalize_windows(node.otherwise),
        )
    if isinstance(node, CastNode):
        return CastNode(_canonicalize_windows(node.expression), node.target_dtype)
    if isinstance(node, AliasNode):
        return AliasNode(node.name, _canonicalize_windows(node.expression))
    return node


def _infer_ir_source_format(path: Path, source_format: str | None) -> str:
    if source_format:
        normalized = source_format.lower().replace("-", "_")
        aliases = {
            "csv": "csv",
            "yaml": "spotfire_layered_yaml",
            "layered_yaml": "spotfire_layered_yaml",
            "spotfire_layered_yaml": "spotfire_layered_yaml",
            "duckdb": "duckdb_layered_yaml",
            "duckdb_layered_yaml": "duckdb_layered_yaml",
            "polars": "polars_layered_yaml",
            "polars_layered_yaml": "polars_layered_yaml",
        }
        if normalized not in aliases:
            raise ValueError(f"Unsupported IR source format: {source_format}")
        return aliases[normalized]
    if path.suffix.lower() == ".csv":
        return "csv"
    if path.name.endswith(".duckdb.layered.yaml"):
        return "duckdb_layered_yaml"
    if path.name.endswith(".polars.layered.yaml"):
        return "polars_layered_yaml"
    if path.suffix.lower() in {".yaml", ".yml"}:
        return "spotfire_layered_yaml"
    raise ValueError(f"Cannot infer IR source format from path: {path}")


def _ir_path_for_source(path: Path) -> Path:
    for suffix in (".duckdb.layered.yaml", ".polars.layered.yaml", ".layered.yaml"):
        if path.name.endswith(suffix):
            return path.with_name(path.name.removesuffix(suffix) + ".rust-ir.json")
    return path.with_suffix(".rust-ir.json")
