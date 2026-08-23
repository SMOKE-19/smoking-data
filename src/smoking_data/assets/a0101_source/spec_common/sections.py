"""Common value access helpers for Asset spec parsing."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def get_value(mapping: dict[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def require_dict(mapping: dict[str, Any], *keys: str) -> dict[str, Any]:
    value = get_value(mapping, *keys)
    if not isinstance(value, dict):
        raise ValueError(f"필수 dict 경로가 없습니다: {'.'.join(keys)}")
    return value


def require_str(mapping: dict[str, Any], *keys: str) -> str:
    value = get_value(mapping, *keys)
    if not isinstance(value, str):
        raise ValueError(f"필수 문자열 경로가 없습니다: {'.'.join(keys)}")
    return value


def optional_str(mapping: dict[str, Any], *keys: str) -> str | None:
    value = get_value(mapping, *keys)
    if value is None:
        return None
    return str(value)


def optional_str_list(mapping: dict[str, Any], *keys: str) -> list[str] | None:
    value = get_value(mapping, *keys)
    if value is None:
        return None
    if not isinstance(value, list):
        return None
    return [str(item) for item in value]


def parse_relative_window(
    value: Any,
    *,
    label: str = "date_window",
) -> tuple[int, int] | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not (text.startswith("(") and text.endswith(")")):
            raise ValueError(f"{label} 튜플 문자열은 '(-90, 0)' 형식이어야 합니다.")
        parts = [item.strip() for item in text[1:-1].split(",")]
        if len(parts) != 2:
            raise ValueError(f"{label} 튜플 문자열은 값 2개가 필요합니다.")
        return (int(parts[0]), int(parts[1]))
    if isinstance(value, (list, tuple)):
        if len(value) != 2:
            raise ValueError(f"{label} 튜플은 값 2개가 필요합니다.")
        return (int(value[0]), int(value[1]))
    raise ValueError(f"{label} 는 '(-90, 0)' 문자열 또는 길이 2인 리스트/튜플이어야 합니다.")


def with_yaml_error_context(exc: Exception, yaml_path: str | Path) -> ValueError:
    path = Path(yaml_path)
    message = str(exc)
    if message.startswith(f"[{path.name}]"):
        return ValueError(message)
    return ValueError(f"[{path.name}] {message}")


_TYPE_PREFIX_PATTERN = re.compile(r"^(?:pl|polars)\.", re.IGNORECASE)
_LIST_PATTERN = re.compile(r"^(?:list|array)\s*[\(\[]\s*(.+?)\s*[\)\]]$", re.IGNORECASE)
_DECIMAL_PATTERN = re.compile(r"^(decimal|numeric)\s*\(\s*\d+\s*,\s*\d+\s*\)$", re.IGNORECASE)

_TYPE_ALIASES = {
    "text": "TEXT",
    "string": "TEXT",
    "utf8": "TEXT",
    "varchar": "TEXT",
    "char": "TEXT",
    "str": "TEXT",
    "tinyint": "TINYINT",
    "int8": "TINYINT",
    "smallint": "SMALLINT",
    "int16": "SMALLINT",
    "integer": "INTEGER",
    "int": "INTEGER",
    "int32": "INTEGER",
    "bigint": "BIGINT",
    "long": "BIGINT",
    "int64": "BIGINT",
    "utinyint": "UTINYINT",
    "uint8": "UTINYINT",
    "usmallint": "USMALLINT",
    "uint16": "USMALLINT",
    "uinteger": "UINTEGER",
    "uint32": "UINTEGER",
    "ubigint": "UBIGINT",
    "uint64": "UBIGINT",
    "float": "FLOAT",
    "float32": "FLOAT",
    "real": "FLOAT",
    "double": "DOUBLE",
    "float64": "DOUBLE",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "time": "TIME",
}


def normalize_type_name(value: Any, *, label: str = "type") -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{label} 값이 비어 있습니다.")
    text = _TYPE_PREFIX_PATTERN.sub("", text)
    if text.endswith("[]"):
        inner = normalize_type_name(text[:-2], label=label)
        return f"{inner}[]"
    if _DECIMAL_PATTERN.match(text):
        return re.sub(r"\s+", "", text).upper()
    list_match = _LIST_PATTERN.match(text)
    if list_match:
        inner = normalize_type_name(list_match.group(1), label=label)
        return f"{inner}[]"
    key = text.lower()
    if key in _TYPE_ALIASES:
        return _TYPE_ALIASES[key]
    return text.upper()
