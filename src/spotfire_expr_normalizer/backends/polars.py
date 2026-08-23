from __future__ import annotations

from ..ast import ExpressionNode, render_spotfire
from ..normalizer import normalize_expression_for_polars


def emit_polars(node: ExpressionNode) -> str:
    return normalize_expression_for_polars(render_spotfire(node))
