from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from smoking_data.core.exceptions import ValidationError

_LITERAL_PATTERN = re.compile(r"^(?P<prefix>[fr])(?P<quote>['\"])(?P<body>.*)(?P=quote)$")
_TEMPLATE_PATTERN = re.compile(r"\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}")


def resolve_contract_templates(
    payload: dict[str, Any],
    *,
    scope: dict[str, Any],
    source: str | Path,
) -> dict[str, Any]:
    normalized_scope = {str(key): str(value) for key, value in scope.items()}
    resolved = _resolve_node(payload, scope=normalized_scope, source=source)
    if not isinstance(resolved, dict):
        raise ValidationError("Resolved YAML contract must be a mapping.")
    return resolved


def template_path_value(path: Path, *, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _resolve_node(node: Any, *, scope: dict[str, str], source: str | Path) -> Any:
    if isinstance(node, dict):
        return {
            key: _resolve_node(value, scope=scope, source=source)
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [_resolve_node(value, scope=scope, source=source) for value in node]
    if not isinstance(node, str):
        return node
    literal = _LITERAL_PATTERN.match(node)
    value = literal.group("body") if literal else node
    unresolved: set[str] = set()

    def replace(match: re.Match[str]) -> str:
        name = match.group("name")
        if name not in scope:
            unresolved.add(name)
            return match.group(0)
        return scope[name]

    result = _TEMPLATE_PATTERN.sub(replace, value)
    if unresolved:
        raise ValidationError(
            "Output contract contains unresolved template variables.",
            code="output.unresolved_template",
            context={"source": str(source), "variables": sorted(unresolved)},
        )
    return result
