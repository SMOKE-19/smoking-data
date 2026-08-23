from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class ExpressionNode:
    def to_dict(self) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class ColumnNode(ExpressionNode):
    name: str

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "column", "name": self.name}


@dataclass(frozen=True, slots=True)
class LiteralNode(ExpressionNode):
    dtype: str
    value: Any

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "literal", "dtype": self.dtype, "value": self.value}


@dataclass(frozen=True, slots=True)
class UnaryNode(ExpressionNode):
    operator: str
    operand: ExpressionNode

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "unary", "operator": self.operator, "operand": self.operand.to_dict()}


@dataclass(frozen=True, slots=True)
class BinaryNode(ExpressionNode):
    operator: str
    left: ExpressionNode
    right: ExpressionNode

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "binary",
            "operator": self.operator,
            "left": self.left.to_dict(),
            "right": self.right.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class CallNode(ExpressionNode):
    function: str
    arguments: tuple[ExpressionNode, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "call",
            "function": self.function,
            "arguments": [argument.to_dict() for argument in self.arguments],
        }


@dataclass(frozen=True, slots=True)
class CaseNode(ExpressionNode):
    branches: tuple[tuple[ExpressionNode, ExpressionNode], ...]
    otherwise: ExpressionNode

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "case",
            "branches": [
                {"when": condition.to_dict(), "then": value.to_dict()}
                for condition, value in self.branches
            ],
            "otherwise": self.otherwise.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class CastNode(ExpressionNode):
    expression: ExpressionNode
    target_dtype: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "cast",
            "expression": self.expression.to_dict(),
            "target_dtype": self.target_dtype,
        }


@dataclass(frozen=True, slots=True)
class WindowOrder:
    expression: ExpressionNode
    direction: str = "asc"
    nulls: str = "last"

    def to_dict(self) -> dict[str, Any]:
        return {
            "expression": self.expression.to_dict(),
            "direction": self.direction,
            "nulls": self.nulls,
        }


@dataclass(frozen=True, slots=True)
class WindowNode(ExpressionNode):
    expression: ExpressionNode
    partition_by: tuple[ExpressionNode, ...]
    order_by: tuple[WindowOrder, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "window",
            "expression": self.expression.to_dict(),
            "partition_by": [item.to_dict() for item in self.partition_by],
            "order_by": [item.to_dict() for item in self.order_by],
            "frame": None,
        }


@dataclass(frozen=True, slots=True)
class AliasNode(ExpressionNode):
    name: str
    expression: ExpressionNode

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "alias", "name": self.name, "expression": self.expression.to_dict()}


def render_spotfire(node: ExpressionNode) -> str:
    if isinstance(node, ColumnNode):
        return f"[{node.name}]"
    if isinstance(node, LiteralNode):
        if node.value is None:
            return "Null"
        if node.dtype == "bool":
            return "True" if node.value else "False"
        if node.dtype == "string":
            return '"' + str(node.value).replace('"', '""') + '"'
        return str(node.value)
    if isinstance(node, UnaryNode):
        operator = {"negate": "-", "positive": "+", "not": "Not "}[node.operator]
        return f"{operator}({render_spotfire(node.operand)})"
    if isinstance(node, BinaryNode):
        operator = {"contains": "~=", "concat": "&"}.get(node.operator, node.operator)
        return f"({render_spotfire(node.left)} {operator} {render_spotfire(node.right)})"
    if isinstance(node, CallNode):
        arguments = ", ".join(render_spotfire(item) for item in node.arguments)
        return f"{node.function}({arguments})"
    if isinstance(node, CaseNode):
        if len(node.branches) == 1:
            condition, value = node.branches[0]
            return (
                f"If({render_spotfire(condition)}, {render_spotfire(value)}, "
                f"{render_spotfire(node.otherwise)})"
            )
        parts = ["CASE"]
        for condition, value in node.branches:
            parts.append(f"WHEN {render_spotfire(condition)} THEN {render_spotfire(value)}")
        parts.append(f"ELSE {render_spotfire(node.otherwise)} END")
        return " ".join(parts)
    if isinstance(node, CastNode):
        function = {
            "bool": "Boolean",
            "int32": "Integer",
            "int64": "LongInteger",
            "float32": "SingleReal",
            "float64": "Real",
            "string": "String",
        }.get(node.target_dtype, "Cast")
        return f"{function}({render_spotfire(node.expression)})"
    if isinstance(node, WindowNode):
        partitions = ", ".join(render_spotfire(item) for item in node.partition_by)
        return f"{render_spotfire(node.expression)} OVER ({partitions})"
    if isinstance(node, AliasNode):
        return render_spotfire(node.expression)
    raise TypeError(f"Unsupported AST node: {type(node).__name__}")
