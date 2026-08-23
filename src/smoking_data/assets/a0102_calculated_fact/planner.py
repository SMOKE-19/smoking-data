from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping

from smoking_data.core.exceptions import ValidationError


class ListExecutionStrategy(StrEnum):
    SCALAR = "scalar"
    LIST_NATIVE_MAP = "list_native_map"
    LIST_NATIVE_REDUCE = "list_native_reduce"
    EXPAND_CALCULATE_COMPACT = "expand_calculate_compact"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True, slots=True)
class ExpressionExecutionPlan:
    name: str
    strategy: ListExecutionStrategy
    dependencies: tuple[str, ...]
    list_dependencies: tuple[str, ...]
    output_dtype: str
    output_cardinality: str


_NATIVE_NODE_KINDS = frozenset({"column", "literal", "alias", "binary", "case"})
_NATIVE_BINARY_OPERATORS = frozenset(
    {"+", "-", "*", "/", "%", "=", "!=", "<", "<=", ">", ">=", "and", "or"}
)
_EXPLICIT_LIST_REDUCTIONS = frozenset({"listsum", "listavg", "listmin", "listmax", "listcount"})


def plan_expression_execution(
    ir_document: Mapping[str, Any],
    *,
    source_dtypes: Mapping[str, str],
    expansion_available: bool,
) -> tuple[ExpressionExecutionPlan, ...]:
    """Classify typed expressions without changing their Spotfire semantics."""

    cardinality = {
        name: ("list" if _is_list_dtype(dtype) else "scalar")
        for name, dtype in source_dtypes.items()
    }
    result: list[ExpressionExecutionPlan] = []
    for layer in ir_document.get("layers") or []:
        expressions = layer.get("expressions") if isinstance(layer, Mapping) else None
        if not isinstance(expressions, list):
            _fail("expression.invalid_ir", "IR layer expressions must be a list.")
        for expression in expressions:
            if not isinstance(expression, Mapping):
                _fail("expression.invalid_ir", "IR expression must be a mapping.")
            name = str(expression.get("name") or "").strip()
            node = expression.get("expr")
            if not isinstance(node, Mapping):
                _fail("expression.invalid_ir", "IR expression node must be a mapping.", expression=name)
            dependencies = expression_column_references(node)
            unresolved = [item for item in dependencies if item not in cardinality]
            if unresolved:
                _fail(
                    "expression.unresolved_dependency",
                    "Expression dependencies are not bound to the source or a prior expression.",
                    expression=name,
                    dependencies=unresolved,
                )
            list_dependencies = tuple(
                dependency for dependency in dependencies if cardinality[dependency] == "list"
            )
            strategy, output_cardinality = _strategy(
                node,
                has_list_dependencies=bool(list_dependencies),
                expansion_available=expansion_available,
            )
            output_dtype = str(expression.get("dtype") or "unknown")
            if output_cardinality == "list":
                output_dtype = f"list<{output_dtype}>"
            plan = ExpressionExecutionPlan(
                name=name,
                strategy=strategy,
                dependencies=dependencies,
                list_dependencies=list_dependencies,
                output_dtype=output_dtype,
                output_cardinality=output_cardinality,
            )
            result.append(plan)
            cardinality[name] = output_cardinality
    return tuple(result)


def _strategy(
    node: Mapping[str, Any],
    *,
    has_list_dependencies: bool,
    expansion_available: bool,
) -> tuple[ListExecutionStrategy, str]:
    reduction = _explicit_list_reduction(node)
    if reduction:
        if not has_list_dependencies:
            _fail(
                "list.expression_unsupported",
                "Explicit List reduction requires at least one List dependency.",
                function=reduction,
            )
        return ListExecutionStrategy.LIST_NATIVE_REDUCE, "scalar"
    if not has_list_dependencies:
        return ListExecutionStrategy.SCALAR, "scalar"
    if _is_native_map_node(node):
        return ListExecutionStrategy.LIST_NATIVE_MAP, "list"
    if _contains_kind(node, "window"):
        _fail(
            "list.expression_unsupported",
            "Window expressions over List dependencies require an explicit complete-group contract.",
        )
    if expansion_available and _is_shape_preserving_node(node):
        return ListExecutionStrategy.EXPAND_CALCULATE_COMPACT, "list"
    _fail(
        "list.expression_unsupported",
        "List expression cannot prove shape-preserving native or expand/compact execution.",
    )


def _is_native_map_node(node: Mapping[str, Any]) -> bool:
    kind = str(node.get("kind") or "")
    if kind not in _NATIVE_NODE_KINDS:
        return False
    if kind in {"column", "literal"}:
        return True
    if kind == "alias":
        return _child_native(node.get("expression"))
    if kind == "binary":
        return (
            str(node.get("operator") or "") in _NATIVE_BINARY_OPERATORS
            and _child_native(node.get("left"))
            and _child_native(node.get("right"))
        )
    if kind == "case":
        branches = node.get("branches")
        return (
            isinstance(branches, list)
            and all(
                isinstance(branch, Mapping)
                and _child_native(branch.get("when"))
                and _child_native(branch.get("then"))
                for branch in branches
            )
            and _child_native(node.get("otherwise"))
        )
    return False


def _is_shape_preserving_node(node: Mapping[str, Any]) -> bool:
    if _contains_kind(node, "window"):
        return False
    kind = str(node.get("kind") or "")
    if kind in {"column", "literal"}:
        return True
    if kind == "call":
        function = str(node.get("function") or "").lower()
        return function not in _EXPLICIT_LIST_REDUCTIONS and all(
            isinstance(child, Mapping) and _is_shape_preserving_node(child)
            for child in _node_children(node)
        )
    if kind not in {"alias", "unary", "binary", "case", "cast"}:
        return False
    return all(
        isinstance(child, Mapping) and _is_shape_preserving_node(child)
        for child in _node_children(node)
    )


def _explicit_list_reduction(node: Mapping[str, Any]) -> str | None:
    for current in _walk_nodes(node):
        if current.get("kind") == "call":
            function = str(current.get("function") or "").lower()
            if function in _EXPLICIT_LIST_REDUCTIONS:
                return function
    return None


def _contains_kind(node: Mapping[str, Any], kind: str) -> bool:
    return any(str(current.get("kind") or "") == kind for current in _walk_nodes(node))


def _walk_nodes(node: Mapping[str, Any]):
    yield node
    for child in _node_children(node):
        if isinstance(child, Mapping):
            yield from _walk_nodes(child)


def expression_column_references(node: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            str(current.get("name") or "")
            for current in _walk_nodes(node)
            if current.get("kind") == "column" and str(current.get("name") or "")
        )
    )


def _node_children(node: Mapping[str, Any]) -> list[Any]:
    kind = str(node.get("kind") or "")
    if kind == "alias":
        return [node.get("expression")]
    if kind == "unary":
        return [node.get("operand")]
    if kind == "binary":
        return [node.get("left"), node.get("right")]
    if kind == "case":
        branches = node.get("branches") or []
        return [
            *(value for branch in branches if isinstance(branch, Mapping) for value in (branch.get("when"), branch.get("then"))),
            node.get("otherwise"),
        ]
    if kind == "call":
        return list(node.get("arguments") or [])
    if kind == "cast":
        return [node.get("expression")]
    if kind == "window":
        return [
            node.get("expression"),
            *list(node.get("partition_by") or []),
            *(item.get("expression") for item in node.get("order_by") or [] if isinstance(item, Mapping)),
        ]
    return []


def _child_native(value: Any) -> bool:
    return isinstance(value, Mapping) and _is_native_map_node(value)


def _is_list_dtype(dtype: str) -> bool:
    normalized = str(dtype).strip().lower().replace(" ", "")
    return normalized.startswith("list<") or normalized.startswith("large_list<")


def _fail(code: str, message: str, **context: Any) -> None:
    raise ValidationError(message, code=code, context=context)
