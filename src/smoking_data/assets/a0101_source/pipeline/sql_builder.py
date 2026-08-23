"""Polars SOURCE SQL 빌드 유틸."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from pathlib import Path

from .models import DateWindowItemSpec, SourceSpec
from .spec import load_source_spec

_TEMPLATE_PATTERN = re.compile(r"<(?P<name>[A-Za-z_][A-Za-z0-9_]*)>")
_SUBSTRING_EQUALS_PATTERN = re.compile(
    r"^\s*substring\(\s*(?P<column>[A-Za-z_][A-Za-z0-9_.]*)\s*,\s*(?P<start>\d+)\s*,\s*(?P<length>\d+)\s*\)\s*=\s*'(?P<value>[^']*)'\s*$",
    re.IGNORECASE,
)
_SPLIT_PART_EQUALS_PATTERN = re.compile(
    r"^\s*split_part\(\s*(?P<column>[A-Za-z_][A-Za-z0-9_.]*)\s*,\s*'(?P<delimiter>[^']*)'\s*,\s*(?P<index>\d+)\s*\)\s*=\s*'(?P<value>[^']*)'\s*$",
    re.IGNORECASE,
)
_SUBSTRING_IN_PATTERN = re.compile(
    r"^\s*substring\(\s*(?P<column>[A-Za-z_][A-Za-z0-9_.]*)\s*,\s*(?P<start>\d+)\s*,\s*(?P<length>\d+)\s*\)\s+in\s*\((?P<values>.+)\)\s*$",
    re.IGNORECASE,
)
_SPLIT_PART_IN_PATTERN = re.compile(
    r"^\s*split_part\(\s*(?P<column>[A-Za-z_][A-Za-z0-9_.]*)\s*,\s*'(?P<delimiter>[^']*)'\s*,\s*(?P<index>\d+)\s*\)\s+in\s*\((?P<values>.+)\)\s*$",
    re.IGNORECASE,
)
_SMALL_IN_MAX_VALUES = 8
_LINE_COMMENT_PATTERN = re.compile(r"--.*?$", re.MULTILINE)
_WHERE_CLAUSE_PATTERN = re.compile(
    r"\bwhere\b(?P<body>.*?)(?=\bgroup\s+by\b|\border\s+by\b|\bhaving\b|\blimit\b|\bqualify\b|\bunion\b|$)",
    re.IGNORECASE | re.DOTALL,
)
_INCLUSIVE_DATE_TO_BETWEEN_PATTERN = re.compile(
    r"\bbetween\b[\s\S]{0,160}@dateFrom[\s\S]{0,160}\band\b[\s\S]{0,160}@dateTo\b",
    re.IGNORECASE,
)
_SECONDS_PER_DAY = 24 * 60 * 60


@dataclass(slots=True)
class SourceWindow:
    date_from: date | datetime
    date_to: date | datetime

    @property
    def start_at(self) -> datetime:
        return _as_datetime(self.date_from)

    @property
    def end_at(self) -> datetime:
        if isinstance(self.date_to, datetime):
            return self.date_to
        return datetime.combine(self.date_to + timedelta(days=1), datetime.min.time())


def build_source_sql_map(
    spec_or_path: SourceSpec | str | Path,
    *,
    reference_date: date | datetime | str | None = None,
    date_window: object | None = None,
    step: int | float | None = None,
    output_rule: str = "raw_dataset",
) -> dict[str, str]:
    spec = _ensure_spec(spec_or_path)
    template_sql = build_source_template_sql(spec)
    windows = build_source_windows(
        spec,
        reference_date=reference_date,
        date_window=date_window,
        step=step,
    )
    output_map: dict[str, str] = {}
    for window in windows:
        sql_text = render_source_sql(template_sql, date_from=window.date_from, date_to=window.date_to)
        file_name = render_source_output_name(
            spec,
            output_rule=output_rule,
            date_from=window.start_at,
            date_to=window.end_at,
        )
        output_map[file_name] = sql_text
    return output_map


def build_source_windows(
    spec_or_path: SourceSpec | str | Path,
    *,
    reference_date: date | datetime | str | None = None,
    date_window: object | None = None,
    step: int | float | None = None,
) -> list[SourceWindow]:
    spec = _ensure_spec(spec_or_path)
    window_spec = spec.request.date_window
    resolved_step = step if step is not None else window_spec.step
    _step_seconds(resolved_step)
    window_items = list(window_spec.windows)
    if date_window is not None and _uses_explicit_absolute_window_schedule(window_items):
        base_windows = _expand_window_items(
            window_items,
            step=window_spec.step,
            reference_date=reference_date,
            anchor_date=spec.project.partition_grid_anchor_date,
        )
        selector_items = _parse_runtime_date_window_items(date_window)
        selector_ranges = [
            _resolve_window_item(item, reference_date=reference_date)
            for item in selector_items
        ]
        return [
            window
            for window in base_windows
            if any(_window_overlaps_range(window, start, end) for start, end in selector_ranges)
        ]
    if date_window is not None:
        window_items = _parse_runtime_date_window_items(date_window)
    if not window_items:
        raise ValueError("date window 를 해석할 수 없습니다.")
    return _expand_window_items(
        window_items,
        step=resolved_step,
        reference_date=reference_date,
        anchor_date=spec.project.partition_grid_anchor_date,
    )


def _expand_window_items(
    window_items: list[DateWindowItemSpec],
    *,
    step: int | float,
    reference_date: date | datetime | str | None,
    anchor_date: date,
) -> list[SourceWindow]:
    resolved_ranges: list[tuple[date, date]] = []
    for window_item in window_items:
        start, end = _resolve_window_item(window_item, reference_date=reference_date)
        if start > end:
            raise ValueError("date_window 의 시작일은 종료일보다 늦을 수 없습니다.")
        resolved_ranges.append((start, end))

    step_seconds = _step_seconds(step)
    if step_seconds % _SECONDS_PER_DAY == 0:
        return _expand_whole_day_windows(
            resolved_ranges,
            step_days=step_seconds // _SECONDS_PER_DAY,
            anchor_date=anchor_date,
        )
    return _expand_subday_windows(
        resolved_ranges,
        step_seconds=step_seconds,
        anchor_date=anchor_date,
    )


def _expand_whole_day_windows(
    resolved_ranges: list[tuple[date, date]],
    *,
    step_days: int,
    anchor_date: date,
) -> list[SourceWindow]:
    minimum_date = min(start for start, _ in resolved_ranges)
    maximum_date = max(end for _, end in resolved_ranges)
    first_bucket_start = _window_bucket_start(
        minimum_date,
        step_days=step_days,
        anchor_date=anchor_date,
    )
    last_bucket_start = _window_bucket_start(
        maximum_date,
        step_days=step_days,
        anchor_date=anchor_date,
    )

    windows: list[SourceWindow] = []
    cursor = first_bucket_start
    while cursor <= last_bucket_start:
        windows.append(
            SourceWindow(
                date_from=cursor,
                date_to=cursor + timedelta(days=step_days - 1),
            )
        )
        cursor += timedelta(days=step_days)
    return windows


def _expand_subday_windows(
    resolved_ranges: list[tuple[date, date]],
    *,
    step_seconds: int,
    anchor_date: date,
) -> list[SourceWindow]:
    anchor = _as_datetime(anchor_date)
    minimum_at = _as_datetime(min(start for start, _ in resolved_ranges))
    maximum_end_at = _as_datetime(max(end for _, end in resolved_ranges) + timedelta(days=1))
    first_bucket_start = _timestamp_bucket_start(
        minimum_at,
        step_seconds=step_seconds,
        anchor=anchor,
    )
    last_bucket_start = _timestamp_bucket_start(
        maximum_end_at - timedelta(seconds=1),
        step_seconds=step_seconds,
        anchor=anchor,
    )
    step_delta = timedelta(seconds=step_seconds)
    windows: list[SourceWindow] = []
    cursor = first_bucket_start
    while cursor <= last_bucket_start:
        end_at = cursor + step_delta
        windows.append(SourceWindow(date_from=cursor, date_to=end_at))
        cursor = end_at
    return windows


def _step_seconds(step: int | float) -> int:
    if isinstance(step, bool):
        raise ValueError("step 은 1초 이상의 양수 일수여야 합니다.")
    try:
        seconds = int(
            (Decimal(str(step)) * Decimal(_SECONDS_PER_DAY)).to_integral_value(
                rounding=ROUND_DOWN
            )
        )
    except (InvalidOperation, OverflowError, ValueError) as exc:
        raise ValueError("step 은 1초 이상의 양수 일수여야 합니다.") from exc
    if seconds < 1:
        raise ValueError("step 은 1초 이상의 양수 일수여야 합니다.")
    return seconds


def _window_bucket_start(value: date, *, step_days: int, anchor_date: date) -> date:
    offset_days = (value - anchor_date).days
    return anchor_date + timedelta(days=(offset_days // step_days) * step_days)


def _timestamp_bucket_start(
    value: datetime,
    *,
    step_seconds: int,
    anchor: datetime,
) -> datetime:
    offset_seconds = int((value - anchor).total_seconds())
    return anchor + timedelta(seconds=(offset_seconds // step_seconds) * step_seconds)


def _uses_explicit_absolute_window_schedule(window_items: list[DateWindowItemSpec]) -> bool:
    return bool(window_items) and all(item.date_from is not None and item.date_to is not None for item in window_items)


def _window_overlaps_range(window: SourceWindow, right_start: date, right_end: date) -> bool:
    range_start = _as_datetime(right_start)
    range_end = _as_datetime(right_end + timedelta(days=1))
    return window.start_at < range_end and range_start < window.end_at


def build_source_sql(
    spec_or_path: SourceSpec | str | Path,
    *,
    date_from: date | datetime | str,
    date_to: date | datetime | str,
) -> str:
    spec = _ensure_spec(spec_or_path)
    template_sql = build_source_template_sql(spec)
    return render_source_sql(template_sql, date_from=date_from, date_to=date_to)


def build_source_template_sql(
    spec_or_path: SourceSpec | str | Path,
) -> str:
    spec = _ensure_spec(spec_or_path)
    if spec.request.query_mode == "structured":
        template_sql = build_structured_template_sql(spec)
    elif spec.request.query_mode == "sql_file":
        template_sql = load_sql_file_template(spec)
    else:
        raise ValueError(f"지원하지 않는 query_mode 입니다: {spec.request.query_mode}")
    return template_sql


def render_source_sql(template_sql: str, *, date_from: date | datetime | str, date_to: date | datetime | str) -> str:
    start = _coerce_datetime(date_from)
    end_exclusive = (
        _coerce_datetime(date_to)
        if _has_explicit_time(date_to)
        else _coerce_datetime(date_to) + timedelta(days=1)
    )
    return (
        template_sql.replace("@dateFrom", _format_sql_timestamp(start))
        .replace("@dateTo", _format_sql_timestamp(end_exclusive))
        .strip()
    )


def render_source_output_name(
    spec_or_path: SourceSpec | str | Path,
    *,
    output_rule: str,
    date_from: date | datetime | str,
    date_to: date | datetime | str,
    **extra_context: str,
) -> str:
    spec = _ensure_spec(spec_or_path)
    rules = _get_source_output_file_name_rules(spec)
    if not isinstance(rules, dict) or output_rule not in rules:
        raise ValueError(f"file_name_rule 에 없는 output_rule 입니다: {output_rule}")
    template = str(rules[output_rule])
    context = _build_template_context(spec, date_from=date_from, date_to=date_to)
    context.update(_build_sub_job_template_context(extra_context.get("sub_job_name")))
    context.update({key: str(value) for key, value in extra_context.items()})
    return _render_template(template, context=context)


def build_structured_template_sql(spec: SourceSpec, *, filters: list[str] | None = None) -> str:
    select_lines: list[str] = []
    for column in spec.request.columns:
        if column.expr == column.name:
            select_lines.append(f"  {column.name}")
        else:
            select_lines.append(f"  {column.expr} AS {column.name}")
    effective_filters = spec.request.filters if filters is None else filters
    where_clauses = [f"({item})" for item in _optimize_structured_filters(effective_filters)]
    where_clauses.append(
        f"{spec.request.date_window.column} >= CAST(@dateFrom AS TIMESTAMP) AND {spec.request.date_window.column} < CAST(@dateTo AS TIMESTAMP)"
    )
    return "\n".join(["SELECT", ",\n".join(select_lines), f"FROM {spec.request.table_id}", "WHERE", "  " + "\n  AND ".join(where_clauses)])


def _optimize_structured_filters(filters: list[str]) -> list[str]:
    optimized = list(filters)
    substring_groups: dict[str, list[tuple[int, int, str, int]]] = {}
    substring_in_groups: dict[str, list[tuple[int, int, list[str], int]]] = {}
    consumed_indexes: set[int] = set()

    for index, item in enumerate(filters):
        parsed = _parse_substring_equals_filter(item)
        if parsed is None:
            parsed_in = _parse_substring_in_filter(item)
            if parsed_in is None:
                continue
            column, start, length, values = parsed_in
            if not values:
                continue
            if any(len(value) != length for value in values):
                continue
            substring_in_groups.setdefault(column, []).append((start, length, values, index))
        else:
            column, start, length, value = parsed
            if len(value) != length:
                continue
            substring_groups.setdefault(column, []).append((start, length, value, index))

    replacement_by_index: dict[int, str] = {}
    for column, parts in substring_groups.items():
        prefix_like = _build_substring_prefix_like(column, parts)
        if prefix_like is None:
            continue
        first_index = min(item[3] for item in parts)
        replacement_by_index[first_index] = prefix_like
        consumed_indexes.update(item[3] for item in parts)
        consumed_indexes.discard(first_index)

    for column, parts in substring_in_groups.items():
        prefix_like = _build_substring_in_prefix_like(column, parts)
        first_index = min(item[3] for item in parts)
        if prefix_like is None:
            merged_predicate = _build_substring_in_merged_predicate(column, parts)
            if merged_predicate is None:
                continue
            replacement_by_index[first_index] = merged_predicate
        else:
            replacement_by_index[first_index] = prefix_like
        consumed_indexes.update(item[3] for item in parts)
        consumed_indexes.discard(first_index)

    split_part_replacements: dict[int, str] = {}
    for index, item in enumerate(filters):
        if index in consumed_indexes:
            continue
        parsed = _parse_split_part_equals_filter(item)
        if parsed is not None:
            column, delimiter, part_index, value = parsed
            if part_index != 1 or not delimiter:
                continue
            split_part_replacements[index] = f"({column} = '{_sql_escape_literal(value)}' OR {column} LIKE '{_sql_escape_literal(value + delimiter)}%')"
            continue
        parsed_in = _parse_split_part_in_filter(item)
        if parsed_in is None:
            continue
        column, delimiter, part_index, values = parsed_in
        if part_index != 1 or not delimiter or not values or len(values) > _SMALL_IN_MAX_VALUES:
            continue
        split_part_replacements[index] = _build_split_part_in_predicate(column, delimiter, values)

    result: list[str] = []
    for index, item in enumerate(optimized):
        if index in consumed_indexes:
            continue
        if index in replacement_by_index:
            result.append(replacement_by_index[index])
            continue
        if index in split_part_replacements:
            result.append(split_part_replacements[index])
            continue
        result.append(item)
    return result


def _parse_substring_equals_filter(value: str) -> tuple[str, int, int, str] | None:
    matched = _SUBSTRING_EQUALS_PATTERN.match(value)
    if matched is None:
        return None
    return (
        matched.group("column"),
        int(matched.group("start")),
        int(matched.group("length")),
        matched.group("value"),
    )


def _parse_split_part_equals_filter(value: str) -> tuple[str, str, int, str] | None:
    matched = _SPLIT_PART_EQUALS_PATTERN.match(value)
    if matched is None:
        return None
    return (
        matched.group("column"),
        matched.group("delimiter"),
        int(matched.group("index")),
        matched.group("value"),
    )


def _parse_substring_in_filter(value: str) -> tuple[str, int, int, list[str]] | None:
    matched = _SUBSTRING_IN_PATTERN.match(value)
    if matched is None:
        return None
    return (
        matched.group("column"),
        int(matched.group("start")),
        int(matched.group("length")),
        _parse_sql_string_list(matched.group("values")),
    )


def _parse_split_part_in_filter(value: str) -> tuple[str, str, int, list[str]] | None:
    matched = _SPLIT_PART_IN_PATTERN.match(value)
    if matched is None:
        return None
    return (
        matched.group("column"),
        matched.group("delimiter"),
        int(matched.group("index")),
        _parse_sql_string_list(matched.group("values")),
    )


def _build_substring_prefix_like(
    column: str,
    parts: list[tuple[int, int, str, int]],
) -> str | None:
    ordered = sorted(parts, key=lambda item: item[0])
    expected_start = 1
    prefix_parts: list[str] = []

    for start, length, value, _ in ordered:
        if start != expected_start:
            return None
        prefix_parts.append(value)
        expected_start = start + length

    prefix = "".join(prefix_parts)
    return f"{column} LIKE '{_sql_escape_literal(prefix)}%'"


def _build_substring_in_prefix_like(
    column: str,
    parts: list[tuple[int, int, list[str], int]],
) -> str | None:
    ordered = sorted(parts, key=lambda item: item[0])
    expected_start = 1
    prefix_options: list[list[str]] = []

    for start, length, values, _ in ordered:
        if start != expected_start:
            return None
        prefix_options.append(values)
        expected_start = start + length

    combinations = [""]
    for values in prefix_options:
        next_combinations: list[str] = []
        for prefix in combinations:
            for value in values:
                next_combinations.append(prefix + value)
                if len(next_combinations) > _SMALL_IN_MAX_VALUES:
                    return None
        combinations = next_combinations

    if not combinations:
        return None
    predicates = [f"{column} LIKE '{_sql_escape_literal(prefix)}%'" for prefix in combinations]
    if len(predicates) == 1:
        return predicates[0]
    return "(" + " OR ".join(predicates) + ")"


def _build_substring_in_merged_predicate(
    column: str,
    parts: list[tuple[int, int, list[str], int]],
) -> str | None:
    ordered = sorted(parts, key=lambda item: item[0])
    expected_start = 1
    value_options: list[list[str]] = []

    for start, length, values, _ in ordered:
        if start != expected_start:
            return None
        value_options.append(values)
        expected_start = start + length

    combinations = [""]
    for values in value_options:
        next_combinations: list[str] = []
        for prefix in combinations:
            for value in values:
                next_combinations.append(prefix + value)
        combinations = next_combinations

    total_length = expected_start - 1
    escaped_values = [f"'{_sql_escape_literal(item)}'" for item in combinations]
    if len(escaped_values) == 1:
        return f"substring({column}, 1, {total_length}) = {escaped_values[0]}"
    return f"substring({column}, 1, {total_length}) IN ({', '.join(escaped_values)})"


def _build_split_part_in_predicate(column: str, delimiter: str, values: list[str]) -> str:
    predicates = [
        f"({column} = '{_sql_escape_literal(value)}' OR {column} LIKE '{_sql_escape_literal(value + delimiter)}%')"
        for value in values
    ]
    if len(predicates) == 1:
        return predicates[0]
    return "(" + " OR ".join(predicates) + ")"


def _sql_escape_literal(value: str) -> str:
    return value.replace("'", "''")


def _parse_sql_string_list(value: str) -> list[str]:
    return [item.replace("''", "'") for item in re.findall(r"'((?:''|[^'])*)'", value)]




def load_sql_file_template(spec: SourceSpec) -> str:
    if spec.request.sql_file_path is None:
        raise ValueError("sql_file 모드에서는 sql_file_path 가 필요합니다.")
    sql_path = Path(spec.request.sql_file_path)
    template_sql = sql_path.read_text(encoding="utf-8").strip()
    _validate_sql_file_date_window_template(template_sql, sql_path=sql_path)
    return template_sql


def _validate_sql_file_date_window_template(template_sql: str, *, sql_path: Path) -> None:
    if _INCLUSIVE_DATE_TO_BETWEEN_PATTERN.search(template_sql) is None:
        return
    raise ValueError(
        "ETL0101 sql_file 템플릿에서 @dateTo 는 exclusive 종료일입니다. "
        "BETWEEN @dateFrom AND @dateTo 는 윈도우 경계일 오버랩을 만들 수 있으므로 "
        f"'{sql_path}'의 날짜 조건을 '>= @dateFrom AND < @dateTo' 형태로 변경하세요."
    )


def _ensure_spec(spec_or_path: SourceSpec | str | Path) -> SourceSpec:
    if isinstance(spec_or_path, SourceSpec):
        return spec_or_path
    return load_source_spec(spec_or_path)


def _build_template_context(
    spec: SourceSpec,
    *,
    date_from: date | datetime | str,
    date_to: date | datetime | str,
) -> dict[str, str]:
    context = {
        "job_name": spec.job.name,
        "table_id": spec.request.table_id,
        "root_dir": spec.storage.root_dir,
        "date_from": _format_output_boundary(date_from),
        "date_to": _format_output_boundary(date_to),
        "sub_job_name": "",
        "sub_job_suffix": "",
        "task_job_name": spec.job.name,
    }
    return context


def _build_sub_job_template_context(sub_job_name: str | None) -> dict[str, str]:
    normalized = str(sub_job_name or "").strip()
    if not normalized:
        return {}
    return {
        "sub_job_name": normalized,
        "sub_job_suffix": normalized,
    }


def _get_source_output_file_name_rules(spec: SourceSpec) -> dict[str, object]:
    output_block = spec.resolved.get("output")
    if isinstance(output_block, dict):
        artifact = output_block.get("artifact")
        if isinstance(artifact, dict):
            rules = artifact.get("file_name_rule")
            if isinstance(rules, dict):
                return rules
    return {}


def _render_template(template: str, *, context: dict[str, str]) -> str:
    return _TEMPLATE_PATTERN.sub(lambda m: context.get(m.group("name"), m.group(0)), template)


def _coerce_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _coerce_datetime(value: date | datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = datetime.combine(date.fromisoformat(text), datetime.min.time())
    return parsed.replace(microsecond=0)


def _as_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    return datetime.combine(value, datetime.min.time())


def _has_explicit_time(value: date | datetime | str) -> bool:
    if isinstance(value, datetime):
        return True
    if isinstance(value, date):
        return False
    text = str(value).strip()
    return "T" in text or " " in text


def _format_output_boundary(value: date | datetime | str) -> str:
    return _coerce_datetime(value).strftime("%Y%m%dT%H%M%S")


def _format_sql_timestamp(value: date | datetime) -> str:
    return f"'{_as_datetime(value).strftime('%Y-%m-%d %H:%M:%S')}'"


def _coerce_relative_window(
    value: tuple[int, int] | list[int] | str | None,
) -> tuple[int, int] | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not (text.startswith("(") and text.endswith(")")):
            raise ValueError("date_window 문자열은 '(-90, 0)' 형식이어야 합니다.")
        parts = [item.strip() for item in text[1:-1].split(",")]
        if len(parts) != 2:
            raise ValueError("date_window 문자열은 값 2개가 필요합니다.")
        return (int(parts[0]), int(parts[1]))
    if len(value) != 2:
        raise ValueError("date_window 는 값 2개가 필요합니다.")
    return (int(value[0]), int(value[1]))


def _parse_runtime_date_window_items(value: object) -> list[DateWindowItemSpec]:
    items = _coerce_runtime_window_items(value)
    windows = [_parse_runtime_window_item(item) for item in items]
    if not windows:
        raise ValueError("date_window 는 비어 있을 수 없습니다.")
    return windows


def _coerce_runtime_window_items(value: object) -> list[object]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, tuple):
        return [list(value)]
    if isinstance(value, list):
        if not value:
            return []
        if _looks_like_single_runtime_window(value):
            return [value]
        return list(value)
    return [value]


def _looks_like_single_runtime_window(value: list[object]) -> bool:
    if len(value) != 2:
        return False
    left, right = value
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return True
    if (_is_date_like_value(left) and _is_int_like_runtime_offset(right)) or (
        _is_int_like_runtime_offset(left) and _is_date_like_value(right)
    ):
        return True
    return _is_date_like_value(left) and _is_date_like_value(right)


def _parse_runtime_window_item(value: object) -> DateWindowItemSpec:
    absolute_window = _coerce_absolute_window(value)
    if absolute_window is not None:
        date_from, date_to = absolute_window
        return DateWindowItemSpec(date_from=date_from.isoformat(), date_to=date_to.isoformat())
    mixed_window = _coerce_mixed_window(value)
    if mixed_window is not None:
        return DateWindowItemSpec(mixed_window=mixed_window)
    relative_window = _coerce_relative_window(value)  # type: ignore[arg-type]
    if relative_window is not None:
        return DateWindowItemSpec(relative_window=relative_window)
    raise ValueError(
        "date_window 값은 '(-5, 0)', '(2026-05-01, 2026-05-08)', '(2026-05-01, -10)', '(-10, 2026-05-08)' 형식이어야 합니다."
    )


def _coerce_absolute_window(value: object) -> tuple[date, date] | None:
    if isinstance(value, str):
        text = value.strip()
        if not (text.startswith("(") and text.endswith(")")):
            return None
        parts = [part.strip() for part in text[1:-1].split(",")]
        if len(parts) != 2:
            raise ValueError("date_window 문자열은 값 2개가 필요합니다.")
        return _coerce_absolute_window_pair(parts[0], parts[1])
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return _coerce_absolute_window_pair(value[0], value[1])
    return None


def _coerce_absolute_window_pair(left: object, right: object) -> tuple[date, date] | None:
    if not (_is_date_like_value(left) and _is_date_like_value(right)):
        return None
    return _coerce_date(left), _coerce_date(right)


def _coerce_mixed_window(value: object) -> tuple[str | int, str | int] | None:
    if isinstance(value, str):
        text = value.strip()
        if not (text.startswith("(") and text.endswith(")")):
            return None
        parts = [part.strip() for part in text[1:-1].split(",")]
        if len(parts) != 2:
            raise ValueError("date_window 문자열은 값 2개가 필요합니다.")
        return _coerce_mixed_window_pair(parts[0], parts[1])
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return _coerce_mixed_window_pair(value[0], value[1])
    return None


def _coerce_mixed_window_pair(left: object, right: object) -> tuple[str | int, str | int] | None:
    left_is_date = _is_date_like_value(left)
    right_is_date = _is_date_like_value(right)
    left_is_offset = _is_int_like_runtime_offset(left)
    right_is_offset = _is_int_like_runtime_offset(right)
    if left_is_date and right_is_offset:
        return (_coerce_date(left).isoformat(), int(right))
    if left_is_offset and right_is_date:
        return (int(left), _coerce_date(right).isoformat())
    return None


def _is_date_like_value(value: object) -> bool:
    if isinstance(value, (date, datetime)):
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    try:
        _coerce_date(text)
    except ValueError:
        return False
    return True


def _resolve_window_item(
    value: DateWindowItemSpec,
    *,
    reference_date: date | datetime | str | None = None,
) -> tuple[date, date]:
    if value.relative_window is not None:
        return _resolve_relative_window(value.relative_window, reference_date=reference_date)
    if value.mixed_window is not None:
        return _resolve_mixed_window(value.mixed_window, reference_date=reference_date)
    if value.date_from is not None and value.date_to is not None:
        return _coerce_date(value.date_from), _coerce_date(value.date_to)
    raise ValueError("date window 를 해석할 수 없습니다.")


def _resolve_relative_window(
    relative_window: tuple[int, int],
    *,
    reference_date: date | datetime | str | None,
) -> tuple[date, date]:
    anchor = _coerce_date(reference_date) if reference_date is not None else date.today()
    start_offset, end_offset = relative_window
    start = anchor + timedelta(days=start_offset)
    end = anchor + timedelta(days=end_offset)
    return start, end


def _resolve_mixed_window(
    mixed_window: tuple[str | int, str | int],
    *,
    reference_date: date | datetime | str | None,
) -> tuple[date, date]:
    anchor = _coerce_date(reference_date) if reference_date is not None else date.today()
    left, right = mixed_window
    if isinstance(left, str) and isinstance(right, int):
        return _coerce_date(left), anchor + timedelta(days=right)
    if isinstance(left, int) and isinstance(right, str):
        return anchor + timedelta(days=left), _coerce_date(right)
    raise ValueError("mixed date window 를 해석할 수 없습니다.")


def _is_int_like_runtime_offset(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    try:
        int(text)
    except ValueError:
        return False
    return True
