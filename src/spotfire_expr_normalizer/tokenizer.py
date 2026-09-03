from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Token:
    kind: str
    value: str
    position: int


def tokenize_expression(expression: str) -> list[Token]:
    tokens: list[Token] = []
    index = 0
    while index < len(expression):
        char = expression[index]
        if char.isspace():
            index += 1
            continue
        if expression.startswith("//", index):
            newline = expression.find("\n", index)
            index = len(expression) if newline < 0 else newline + 1
            continue
        if char == "[":
            value, index = _read_bracket_identifier(expression, index)
            tokens.append(Token("column", value, index - len(value) - 2))
            continue
        if char in {"'", '"'}:
            value, index = _read_string_literal(expression, index)
            tokens.append(Token("string", value, index - len(value) - 2))
            continue
        number = re.match(r"(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?", expression[index:])
        if number:
            value = number.group(0)
            tokens.append(Token("number", value, index))
            index += len(value)
            continue
        identifier = re.match(r"[A-Za-z_][A-Za-z0-9_]*", expression[index:])
        if identifier:
            value = identifier.group(0)
            tokens.append(Token("identifier", value, index))
            index += len(value)
            continue
        matched = next(
            (
                operator
                for operator in (">=", "<=", "!=", "<>", "~=")
                if expression.startswith(operator, index)
            ),
            None,
        )
        if matched:
            tokens.append(Token("operator", matched, index))
            index += len(matched)
            continue
        if char in "+-*/%=<>&(),":
            tokens.append(Token("punctuation" if char in "()," else "operator", char, index))
            index += 1
            continue
        raise ValueError(f"Unsupported token {char!r} at position {index}.")
    tokens.append(Token("eof", "", len(expression)))
    return tokens


def _read_bracket_identifier(expression: str, start: int) -> tuple[str, int]:
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
    raise ValueError(f"Unclosed bracket column at position {start}.")


def _read_string_literal(expression: str, start: int) -> tuple[str, int]:
    quote = expression[start]
    cursor = start + 1
    value: list[str] = []
    while cursor < len(expression):
        if expression[cursor] == quote:
            if cursor + 1 < len(expression) and expression[cursor + 1] == quote:
                value.append(quote)
                cursor += 2
                continue
            return "".join(value), cursor + 1
        value.append(expression[cursor])
        cursor += 1
    raise ValueError(f"Unclosed string literal at position {start}.")
