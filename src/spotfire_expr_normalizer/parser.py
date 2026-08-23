from __future__ import annotations

from .ast import (
    BinaryNode,
    CallNode,
    CaseNode,
    CastNode,
    ColumnNode,
    ExpressionNode,
    LiteralNode,
    UnaryNode,
    WindowNode,
)
from .tokenizer import Token, tokenize_expression

_BINARY_PRECEDENCE = {
    "or": 10,
    "and": 20,
    "=": 30,
    "!=": 30,
    "<>": 30,
    "<": 30,
    "<=": 30,
    ">": 30,
    ">=": 30,
    "~=": 30,
    "+": 40,
    "-": 40,
    "&": 40,
    "*": 50,
    "/": 50,
    "%": 50,
}

_CANONICAL_BINARY = {"<>": "!=", "~=": "contains", "&": "concat"}
_CAST_FUNCTIONS = {
    "boolean": "bool",
    "integer": "int32",
    "longinteger": "int64",
    "single": "float32",
    "singlereal": "float32",
    "real": "float64",
    "decimal": "decimal128_38_10",
    "currency": "decimal128_38_10",
    "string": "string",
    "date": "date32",
    "datetime": "timestamp_us",
    "time": "time64_us",
    "timespan": "duration_us",
}
_CAST_TYPE_NAMES = {
    "boolean": "bool",
    "integer": "int32",
    "bigint": "int64",
    "longinteger": "int64",
    "real": "float64",
    "double": "float64",
    "single": "float32",
    "singlereal": "float32",
    "string": "string",
    "varchar": "string",
    "date": "date32",
    "datetime": "timestamp_us",
    "timestamp": "timestamp_us",
    "time": "time64_us",
    "timespan": "duration_us",
    "decimal": "decimal128_38_10",
    "currency": "decimal128_38_10",
}


class ExpressionParser:
    def __init__(self, expression: str) -> None:
        self.expression = expression
        self.tokens = tokenize_expression(expression)
        self.index = 0

    @property
    def current(self) -> Token:
        return self.tokens[self.index]

    def advance(self) -> Token:
        token = self.current
        self.index += 1
        return token

    def match(self, value: str) -> bool:
        if self.current.value.lower() != value.lower():
            return False
        self.advance()
        return True

    def expect(self, value: str) -> None:
        if not self.match(value):
            raise ValueError(
                f"Expected {value!r} at position {self.current.position}, "
                f"got {self.current.value!r}."
            )

    def parse(self) -> ExpressionNode:
        node = self.parse_expression()
        if self.current.kind != "eof":
            raise ValueError(
                f"Unexpected token {self.current.value!r} at position {self.current.position}."
            )
        return node

    def parse_expression(self, minimum_precedence: int = 0) -> ExpressionNode:
        left = self.parse_prefix()
        while True:
            operator = self.current.value.lower()
            precedence = _BINARY_PRECEDENCE.get(operator)
            if precedence is None or precedence < minimum_precedence:
                break
            self.advance()
            right = self.parse_expression(precedence + 1)
            left = BinaryNode(_CANONICAL_BINARY.get(operator, operator), left, right)
        return left

    def parse_prefix(self) -> ExpressionNode:
        token = self.advance()
        lowered = token.value.lower()
        if token.kind == "operator" and token.value in {"+", "-"}:
            return UnaryNode(
                "positive" if token.value == "+" else "negate", self.parse_expression(60)
            )
        if token.kind == "identifier" and lowered == "not":
            return UnaryNode("not", self.parse_expression(60))
        if token.kind == "column":
            return ColumnNode(token.value)
        if token.kind == "string":
            return LiteralNode("string", token.value)
        if token.kind == "number":
            return LiteralNode(
                "float64" if any(char in token.value.lower() for char in ".e") else "int64",
                float(token.value)
                if any(char in token.value.lower() for char in ".e")
                else int(token.value),
            )
        if token.kind == "identifier":
            if lowered in {"null", "true", "false"}:
                return LiteralNode(
                    "null" if lowered == "null" else "bool",
                    None if lowered == "null" else lowered == "true",
                )
            if lowered == "case":
                branches: list[tuple[ExpressionNode, ExpressionNode]] = []
                while self.match("when"):
                    condition = self.parse_expression()
                    self.expect("then")
                    value = self.parse_expression()
                    branches.append((condition, value))
                otherwise: ExpressionNode = LiteralNode("null", None)
                if self.match("else"):
                    otherwise = self.parse_expression()
                self.expect("end")
                if not branches:
                    raise ValueError("CASE requires at least one WHEN branch.")
                return CaseNode(tuple(branches), otherwise)
            if self.match("("):
                if lowered == "cast":
                    expression = self.parse_expression()
                    if self.current.kind == "identifier" and self.current.value.lower() == "as":
                        self.advance()
                        target = self.advance().value.lower()
                        self.expect(")")
                        try:
                            return CastNode(expression, _CAST_TYPE_NAMES[target])
                        except KeyError as exc:
                            raise ValueError(f"Unsupported Cast target: {target}") from exc
                    self.expect(",")
                    target_node = self.parse_expression()
                    self.expect(")")
                    if not isinstance(target_node, LiteralNode) or target_node.dtype != "string":
                        raise ValueError("Cast target must be a type name literal.")
                    target = str(target_node.value).lower()
                    try:
                        return CastNode(expression, _CAST_TYPE_NAMES[target])
                    except KeyError as exc:
                        raise ValueError(f"Unsupported Cast target: {target}") from exc
                arguments: list[ExpressionNode] = []
                if not self.match(")"):
                    while True:
                        arguments.append(self.parse_expression())
                        if self.match(")"):
                            break
                        self.expect(",")
                if lowered == "if" and len(arguments) == 3:
                    node: ExpressionNode = CaseNode(
                        branches=((arguments[0], arguments[1]),),
                        otherwise=arguments[2],
                    )
                elif lowered in _CAST_FUNCTIONS and len(arguments) == 1:
                    node = CastNode(arguments[0], _CAST_FUNCTIONS[lowered])
                else:
                    node = CallNode(lowered, tuple(arguments))
                if self.current.kind == "identifier" and self.current.value.lower() == "over":
                    self.advance()
                    self.expect("(")
                    partitions: list[ExpressionNode] = []
                    if (
                        self.current.kind == "identifier"
                        and self.current.value.lower() == "partition"
                    ):
                        self.advance()
                        self.expect("by")
                    if not self.match(")"):
                        while True:
                            partitions.append(self.parse_expression())
                            if self.match(")"):
                                break
                            self.expect(",")
                    node = WindowNode(node, tuple(partitions))
                return node
            return ColumnNode(token.value)
        if token.value == "(":
            node = self.parse_expression()
            self.expect(")")
            return node
        raise ValueError(f"Unexpected token {token.value!r} at position {token.position}.")


def parse_expression(expression: str) -> ExpressionNode:
    return ExpressionParser(expression).parse()
