from __future__ import annotations

import shutil
import unicodedata
from collections.abc import Iterable, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from smoking_data.core.exceptions import ValidationError
from smoking_data.ops.upstream import discover_parquet_files
from smoking_data.runtime.paths import resolve_project_path

PREVIEW_SCHEMA_VERSION = "smoking-data.parquet-preview.v1"
DEFAULT_PREVIEW_ROWS = 10
DEFAULT_TERMINAL_WIDTH = 120
MAX_CELL_WIDTH = 32
_COLUMN_SEPARATOR = " | "


def preview_parquet(
    path: str | Path,
    *,
    project_root: str | Path | None = None,
    row_limit: int = DEFAULT_PREVIEW_ROWS,
    repeat_columns: Sequence[str] = (),
) -> dict[str, Any]:
    root = Path(project_root or Path.cwd()).expanduser().resolve()
    resolved = resolve_project_path(path, project_root=root)
    files = discover_parquet_files([resolved], recursive=True)
    if not files:
        raise ValidationError(
            "Parquet 파일을 찾을 수 없습니다.",
            code="preview.source_empty",
            context={"path": str(resolved)},
        )

    source = files[0].path
    parquet = pq.ParquetFile(source)
    batch = next(parquet.iter_batches(batch_size=row_limit), None)
    table = (
        pa.Table.from_batches([batch])
        if batch is not None
        else pa.Table.from_batches([], schema=parquet.schema_arrow)
    )
    columns = list(table.column_names)
    repeated = _normalize_repeat_columns(repeat_columns)
    missing = [column for column in repeated if column not in columns]
    if missing:
        raise ValidationError(
            "반복 출력할 칼럼이 첫 번째 Parquet 파일에 없습니다.",
            code="preview.repeat_column_missing",
            context={"source_file": str(source), "missing_columns": missing},
        )

    rows = [
        {column: _json_safe_value(value) for column, value in row.items()}
        for row in table.to_pylist()
    ]
    return {
        "ok": True,
        "schema_version": PREVIEW_SCHEMA_VERSION,
        "source_path": str(resolved),
        "source_file": str(source),
        "file_count": len(files),
        "row_limit": row_limit,
        "rows_read": len(rows),
        "columns": columns,
        "column_types": [str(field.type) for field in table.schema],
        "repeat_columns": repeated,
        "rows": rows,
    }


def render_parquet_preview(
    payload: dict[str, Any],
    *,
    terminal_width: int | None = None,
) -> str:
    width = terminal_width or shutil.get_terminal_size(
        fallback=(DEFAULT_TERMINAL_WIDTH, 24)
    ).columns
    width = max(20, width)
    columns = [str(column) for column in payload.get("columns") or []]
    repeated = [str(column) for column in payload.get("repeat_columns") or []]
    rows = list(payload.get("rows") or [])
    display_values = {
        column: [_format_value(row.get(column)) for row in rows]
        for column in columns
    }
    preferred_widths = {
        column: min(
            MAX_CELL_WIDTH,
            max([_display_width(column), *(_display_width(value) for value in display_values[column])]),
        )
        for column in columns
    }
    payload_columns = [column for column in columns if column not in repeated]
    blocks = _column_blocks(
        payload_columns,
        repeat_columns=repeated,
        preferred_widths=preferred_widths,
        terminal_width=width,
    )

    lines = [
        f"[parquet {payload.get('source_file')}]",
        f"files={payload.get('file_count')} rows={payload.get('rows_read')}/{payload.get('row_limit')} "
        f"columns={len(columns)} width={width}",
    ]
    for index, block in enumerate(blocks, start=1):
        selected = [*repeated, *block]
        fitted = _fit_widths(selected, preferred_widths, terminal_width=width)
        first_payload = columns.index(block[0]) + 1 if block else 0
        last_payload = columns.index(block[-1]) + 1 if block else 0
        lines.extend(
            [
                "",
                f"[column block {index}/{len(blocks)}: {first_payload}-{last_payload}/{len(columns)}]",
                _render_row(selected, fitted, selected),
                _render_separator(selected, fitted),
            ]
        )
        lines.extend(
            _render_row(selected, fitted, [display_values[column][row_index] for column in selected])
            for row_index in range(len(rows))
        )
    return "\n".join(lines)


def _normalize_repeat_columns(columns: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    for raw in columns:
        for item in str(raw).split(","):
            name = item.strip()
            if name and name not in normalized:
                normalized.append(name)
    return normalized


def _column_blocks(
    columns: Sequence[str],
    *,
    repeat_columns: Sequence[str],
    preferred_widths: dict[str, int],
    terminal_width: int,
) -> list[list[str]]:
    if not columns:
        return [[]]
    blocks: list[list[str]] = []
    current: list[str] = []
    for column in columns:
        candidate = [*repeat_columns, *current, column]
        if current and _row_width(candidate, preferred_widths) > terminal_width:
            blocks.append(current)
            current = []
        current.append(column)
    if current:
        blocks.append(current)
    return blocks


def _fit_widths(
    columns: Sequence[str],
    preferred_widths: dict[str, int],
    *,
    terminal_width: int,
) -> dict[str, int]:
    widths = {column: max(1, preferred_widths[column]) for column in columns}
    while columns and _row_width(columns, widths) > terminal_width:
        widest = max(columns, key=lambda column: widths[column])
        if widths[widest] <= 1:
            break
        widths[widest] -= 1
    return widths


def _row_width(columns: Sequence[str], widths: dict[str, int]) -> int:
    return sum(widths[column] for column in columns) + len(_COLUMN_SEPARATOR) * max(
        0, len(columns) - 1
    )


def _render_row(columns: Sequence[str], widths: dict[str, int], values: Iterable[str]) -> str:
    return _COLUMN_SEPARATOR.join(
        _pad_or_truncate(str(value), widths[column])
        for column, value in zip(columns, values, strict=True)
    )


def _render_separator(columns: Sequence[str], widths: dict[str, int]) -> str:
    return "-+-".join("-" * widths[column] for column in columns)


def _format_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str):
        return value.replace("\r", "\\r").replace("\n", "\\n")
    if isinstance(value, (list, dict)):
        import json

        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def _pad_or_truncate(value: str, width: int) -> str:
    if _display_width(value) <= width:
        return value + " " * (width - _display_width(value))
    if width == 1:
        return "…"
    target = width - 1
    rendered = ""
    used = 0
    for character in value:
        character_width = _character_width(character)
        if used + character_width > target:
            break
        rendered += character
        used += character_width
    return rendered + " " * (target - used) + "…"


def _display_width(value: str) -> int:
    return sum(_character_width(character) for character in value)


def _character_width(character: str) -> int:
    if unicodedata.combining(character):
        return 0
    return 2 if unicodedata.east_asian_width(character) in {"W", "F"} else 1


def _json_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    return str(value)
