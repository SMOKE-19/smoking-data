from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .ast import AliasNode, ExpressionNode

IR_VERSION = "spotfire-expression-ir.v1"


@dataclass(frozen=True, slots=True)
class IrExpression:
    name: str
    source: str
    dependencies: tuple[str, ...]
    node: AliasNode
    dtype: str = "unknown"
    nullable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "dependencies": list(self.dependencies),
            "dtype": self.dtype,
            "nullable": self.nullable,
            "expr": self.node.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class ExpressionIrDocument:
    layers: tuple[tuple[IrExpression, ...], ...]
    version: str = IR_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "layers": [
                {"expressions": [expression.to_dict() for expression in layer]}
                for layer in self.layers
            ],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.to_json().encode("utf-8")).hexdigest()


def alias_expression(name: str, node: ExpressionNode) -> AliasNode:
    return AliasNode(name=name, expression=node)
