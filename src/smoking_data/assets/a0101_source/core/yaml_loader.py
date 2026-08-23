"""프로젝트 전용 YAML 후처리와 문자열 규약 해석 모듈."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_LITERAL_PATTERN = re.compile(r"^(?P<prefix>[fr])(?P<quote>['\"])(?P<body>.*)(?P=quote)$")
_TEMPLATE_PATTERN = re.compile(r"\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}")
_UNRESOLVED_TEMPLATE_PATTERN = re.compile(r"\{[A-Za-z_][A-Za-z0-9_]*\}")


@dataclass(slots=True)
class LoadedYaml:
    path: Path
    raw: dict[str, Any]
    resolved: dict[str, Any]


def _active_yaml_module() -> Any:
    return sys.modules.get("yaml", yaml)


def load_yaml_file(path: str | Path) -> LoadedYaml:
    yaml_path = Path(path).resolve()
    raw = _active_yaml_module().safe_load(yaml_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("YAML 최상위 구조는 dict여야 합니다.")
    resolved = resolve_yaml_literals(raw, yaml_path=yaml_path)
    return LoadedYaml(path=yaml_path, raw=raw, resolved=resolved)


def resolve_yaml_literals(
    payload: dict[str, Any],
    *,
    extra_scope: dict[str, Any] | None = None,
    yaml_path: str | Path | None = None,
) -> dict[str, Any]:
    current: dict[str, Any] = payload
    for _ in range(5):
        root_scope = _build_scope(current)
        if extra_scope:
            root_scope.update(
                {str(key): _scalarize_scope_value(value) for key, value in extra_scope.items()}
            )
        resolved = _resolve_node(current, scope=root_scope)
        if not isinstance(resolved, dict):
            raise ValueError("해석 결과 최상위 구조는 dict여야 합니다.")
        if resolved == current:
            _assert_no_unresolved(resolved, yaml_path=yaml_path)
            return resolved
        current = resolved
    _assert_no_unresolved(current, yaml_path=yaml_path)
    return current


def _assert_no_unresolved(node: Any, *, yaml_path: str | Path | None) -> None:
    if isinstance(node, dict):
        for value in node.values():
            _assert_no_unresolved(value, yaml_path=yaml_path)
        return
    if isinstance(node, list):
        for item in node:
            _assert_no_unresolved(item, yaml_path=yaml_path)
        return
    if isinstance(node, str) and _UNRESOLVED_TEMPLATE_PATTERN.search(node):
        location = str(Path(yaml_path).resolve()) if yaml_path is not None else "<unknown yaml>"
        raise ValueError(f"{location}: unresolved template variable remains: {node!r}")


def _resolve_node(node: Any, *, scope: dict[str, Any]) -> Any:
    if isinstance(node, dict):
        return {key: _resolve_node(value, scope=scope) for key, value in node.items()}
    if isinstance(node, list):
        return [_resolve_node(item, scope=scope) for item in node]
    if isinstance(node, str):
        return _resolve_string(node, scope=scope)
    return node


def _resolve_string(value: str, *, scope: dict[str, Any]) -> str:
    match = _LITERAL_PATTERN.match(value)
    if not match:
        if "{" not in value:
            return value
        return _TEMPLATE_PATTERN.sub(lambda m: _replace_template(m, scope=scope), value)

    prefix = match.group("prefix")
    body = match.group("body")
    if prefix == "r":
        return body
    return _TEMPLATE_PATTERN.sub(lambda m: _replace_template(m, scope=scope), body)


def _replace_template(match: re.Match[str], *, scope: dict[str, Any]) -> str:
    name = match.group("name")
    if name not in scope:
        return match.group(0)
    return str(scope[name])


def _build_scope(payload: dict[str, Any]) -> dict[str, Any]:
    entries: list[tuple[str, Any]] = []
    _collect_scope_entries(payload, entries=entries)
    counts: dict[str, int] = {}
    for key, _ in entries:
        counts[key] = counts.get(key, 0) + 1
    scope: dict[str, Any] = {}
    for key, value in entries:
        if counts[key] == 1:
            scope[key] = _scalarize_scope_value(value)
    return scope


def _collect_scope_entries(node: Any, *, entries: list[tuple[str, Any]], parents: tuple[str, ...] = ()) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            current_path = parents + (key,)
            if isinstance(value, (dict, list)):
                _collect_scope_entries(value, entries=entries, parents=current_path)
                continue
            entries.append((".".join(current_path), value))
            entries.append((key, value))
            if len(current_path) >= 2:
                entries.append((f"{current_path[-2]}_{current_path[-1]}", value))
            if len(current_path) >= 3:
                entries.append((f"{current_path[-3]}_{current_path[-2]}_{current_path[-1]}", value))
        return
    if isinstance(node, list):
        for item in node:
            _collect_scope_entries(item, entries=entries, parents=parents)


def _scalarize_scope_value(value: Any) -> Any:
    if isinstance(value, str):
        match = _LITERAL_PATTERN.match(value)
        if match:
            prefix = match.group("prefix")
            body = match.group("body")
            return body if prefix in {"f", "r"} else value
    return value



def load_yaml_document(yaml_path: str | Path) -> dict[str, Any]:
    raw = _active_yaml_module().safe_load(Path(yaml_path).resolve().read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("YAML 최상위 구조는 dict여야 합니다.")
    return raw
