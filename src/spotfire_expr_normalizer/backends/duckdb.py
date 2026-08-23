from __future__ import annotations

from ..ast import ExpressionNode, render_spotfire
from ..normalizer import normalize_expression


def emit_duckdb(node: ExpressionNode) -> str:
    return normalize_expression(render_spotfire(node))
