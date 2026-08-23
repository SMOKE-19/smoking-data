from __future__ import annotations

from typing import Protocol, TypeVar


class DependencyExpression(Protocol):
    name: str
    dependencies: list[str]


ExpressionT = TypeVar("ExpressionT", bound=DependencyExpression)


def build_expression_layers(expressions: list[ExpressionT]) -> list[list[ExpressionT]]:
    pending = {item.name: item for item in expressions}
    resolved: set[str] = set()
    layers: list[list[ExpressionT]] = []
    while pending:
        layer = [item for item in pending.values() if set(item.dependencies).issubset(resolved)]
        if not layer:
            remaining = ", ".join(sorted(pending))
            raise ValueError(
                "Spotfire derived expressions contain a cycle or unresolved dependency: "
                + remaining
            )
        layer = sorted(layer, key=lambda item: item.name)
        layers.append(layer)
        for item in layer:
            resolved.add(item.name)
            pending.pop(item.name, None)
    return layers
