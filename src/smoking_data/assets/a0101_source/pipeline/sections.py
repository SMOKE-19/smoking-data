"""SOURCE YAML 하위 블록 해석기."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from smoking_data.assets.a0101_source.spec_common.sections import (
    get_value,
    parse_relative_window,
    require_dict,
    require_str,
)

from .models import (
    ColumnSpec,
    DateWindowItemSpec,
    DateWindowSpec,
    SourceRequestSpec,
    SourceSubJobSpec,
    SpiPrepareSpec,
)


def parse_request(
    resolved: dict[str, Any],
    *,
    path_resolver: Callable[[str], str] | None = None,
) -> SourceRequestSpec:
    query_mode = require_str(resolved, "source", "api_request", "query_mode")
    if query_mode == "structured":
        return parse_structured_request(resolved, path_resolver=path_resolver)
    if query_mode == "sql_file":
        return parse_sql_file_request(resolved, path_resolver=path_resolver)
    if query_mode in {"http_json", "http_ndjson", "http_xml"}:
        return parse_http_request(resolved, query_mode=query_mode)
    raise ValueError(f"지원하지 않는 query_mode 입니다: {query_mode}")


def parse_structured_request(
    resolved: dict[str, Any],
    *,
    path_resolver: Callable[[str], str] | None = None,
) -> SourceRequestSpec:
    table_id = require_str(resolved, "source", "table_id")
    payload = require_dict(resolved, "source", "api_request", "payload")
    columns: list[ColumnSpec] = []
    for item in payload.get("select", []):
        if not isinstance(item, dict):
            raise ValueError("structured payload.select 항목은 dict여야 합니다.")
        name = str(item["name"])
        expr = str(item.get("expr", name))
        columns.append(ColumnSpec(name=name, expr=expr))
    filters, sub_jobs = _parse_payload_filters(payload.get("filters"))
    return SourceRequestSpec(
        table_id=table_id,
        query_mode="structured",
        date_window=parse_date_window(resolved),
        columns=columns,
        filters=filters,
        sub_jobs=sub_jobs,
        spi_prepare=_parse_spi_prepare(resolved, path_resolver=path_resolver),
    )


def parse_sql_file_request(
    resolved: dict[str, Any],
    *,
    path_resolver: Callable[[str], str] | None = None,
) -> SourceRequestSpec:
    table_id = require_str(resolved, "source", "table_id")
    payload = require_dict(resolved, "source", "api_request", "payload")
    columns: list[ColumnSpec] = []
    for item in payload.get("select", []):
        if not isinstance(item, dict):
            raise ValueError("sql_file payload.select 항목은 dict여야 합니다.")
        name = str(item["name"])
        expr = str(item.get("expr", name))
        columns.append(ColumnSpec(name=name, expr=expr))
    return SourceRequestSpec(
        table_id=table_id,
        query_mode="sql_file",
        date_window=parse_date_window(resolved),
        columns=columns,
        filters=[],
        sub_jobs=[],
        sql_file_path=(
            path_resolver(require_str(resolved, "source", "api_request", "sql_file_path"))
            if path_resolver is not None
            else require_str(resolved, "source", "api_request", "sql_file_path")
        ),
        spi_prepare=_parse_spi_prepare(resolved, path_resolver=path_resolver),
    )


def _parse_spi_prepare(
    resolved: dict[str, Any],
    *,
    path_resolver: Callable[[str], str] | None,
) -> SpiPrepareSpec | None:
    api_request = require_dict(resolved, "source", "api_request")
    raw = api_request.get("spi")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("source.api_request.spi 는 dict여야 합니다.")
    unknown = sorted(set(raw) - {"pre_query_script", "execution", "timeout_sec", "lock_timeout_sec"})
    if unknown:
        raise ValueError(f"source.api_request.spi의 알 수 없는 키입니다: {unknown}")
    script_value = str(raw.get("pre_query_script") or "").strip()
    if not script_value:
        raise ValueError("source.api_request.spi.pre_query_script 값이 필요합니다.")
    script_path = path_resolver(script_value) if path_resolver is not None else script_value
    execution = str(raw.get("execution") or "once_per_run")
    if execution != "once_per_run":
        raise ValueError("source.api_request.spi.execution은 once_per_run만 지원합니다.")
    timeout_sec = float(raw.get("timeout_sec") or 60.0)
    lock_timeout_sec = float(raw.get("lock_timeout_sec") or 60.0)
    if timeout_sec <= 0 or lock_timeout_sec <= 0:
        raise ValueError("SOURCE 0101 SPI timeout 설정은 양수여야 합니다.")
    return SpiPrepareSpec(
        script_path=script_path,
        execution="once_per_run",
        timeout_sec=timeout_sec,
        lock_timeout_sec=lock_timeout_sec,
    )


def _parse_payload_filters(value: Any) -> tuple[list[str], list[SourceSubJobSpec]]:
    if value is None:
        return [], []
    if not isinstance(value, dict):
        raise ValueError("source.api_request.payload.filters 는 dict여야 합니다.")
    unknown = sorted(set(value) - {"common", "sub_job"})
    if unknown:
        raise ValueError(f"source.api_request.payload.filters의 알 수 없는 키입니다: {unknown}")
    common = [str(item) for item in (value.get("common", []) or [])]
    raw_sub_jobs = value.get("sub_job")
    if raw_sub_jobs is None:
        return common, []
    if not isinstance(raw_sub_jobs, list):
        raise ValueError("source.api_request.payload.filters.sub_job 은 list여야 합니다.")
    sub_jobs: list[SourceSubJobSpec] = []
    for index, item in enumerate(raw_sub_jobs, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"filters.sub_job[{index}] 는 dict 여야 합니다.")
        unknown = sorted(set(item) - {"sub_job_name", "sub_job_filtering"})
        if unknown:
            raise ValueError(f"filters.sub_job[{index}]의 알 수 없는 키입니다: {unknown}")
        name = str(item.get("sub_job_name") or "").strip()
        if not name:
            raise ValueError(f"filters.sub_job[{index}].sub_job_name 값이 필요합니다.")
        raw_filters = item.get("sub_job_filtering", [])
        if not isinstance(raw_filters, list):
            raise ValueError(f"filters.sub_job[{index}].sub_job_filtering 은 list여야 합니다.")
        filters = [str(filter_value) for filter_value in raw_filters]
        sub_jobs.append(SourceSubJobSpec(name=name, filters=filters))
    return common, sub_jobs


def parse_date_window(resolved: dict[str, Any], *, require_column: bool = True) -> DateWindowSpec:
    window_payload = require_dict(resolved, "source", "api_request", "date_window")
    windows = _parse_date_window_items(get_value(window_payload, "date_window"))
    return DateWindowSpec(
        column=(require_str(window_payload, "column") if require_column else str(window_payload.get("column") or "")),
        step=_parse_window_step(get_value(window_payload, "step")),
        windows=windows,
    )


def parse_http_request(resolved: dict[str, Any], *, query_mode: str) -> SourceRequestSpec:
    table_id = require_str(resolved, "source", "table_id")
    api_request = require_dict(resolved, "source", "api_request")
    if api_request.get("spi") is not None:
        raise ValueError("source.api_request.spi는 structured 또는 sql_file에서만 사용할 수 있습니다.")
    http = require_dict(api_request, "http")
    unknown = sorted(
        set(http)
        - {
            "url",
            "query",
            "headers",
            "timeout_sec",
            "max_response_bytes",
            "batch_rows",
            "pagination",
            "json",
            "xml",
        }
    )
    if unknown:
        raise ValueError(f"source.api_request.http의 알 수 없는 키입니다: {unknown}")
    url = str(http.get("url") or "").strip()
    if not url.startswith(("http://", "https://")):
        raise ValueError("source.api_request.http.url은 http 또는 https URL이어야 합니다.")
    query = _parse_scalar_mapping(http.get("query"), path="source.api_request.http.query")
    headers = _parse_scalar_mapping(http.get("headers"), path="source.api_request.http.headers")
    timeout_sec = float(http.get("timeout_sec") or 60)
    max_response_bytes = int(http.get("max_response_bytes") or 67_108_864)
    batch_rows = int(http.get("batch_rows") or 10_000)
    if timeout_sec <= 0 or max_response_bytes < 1 or batch_rows < 1:
        raise ValueError("HTTP timeout_sec, max_response_bytes와 batch_rows는 양수여야 합니다.")
    pagination = _parse_http_pagination(http.get("pagination"))
    json_response = _parse_http_json(http.get("json"), required=query_mode == "http_json")
    xml = _parse_http_xml(http.get("xml"), required=query_mode == "http_xml")
    return SourceRequestSpec(
        table_id=table_id,
        query_mode=query_mode,  # type: ignore[arg-type]
        date_window=parse_date_window(resolved, require_column=False),
        columns=[],
        filters=[],
        sub_jobs=[],
        http_request={
            "url": url,
            "query": query,
            "headers": headers,
            "timeout_sec": timeout_sec,
            "max_response_bytes": max_response_bytes,
            "batch_rows": batch_rows,
            "pagination": pagination,
            "json": json_response,
            "xml": xml,
        },
    )


def _parse_scalar_mapping(value: Any, *, path: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{path}는 dict여야 합니다.")
    result: dict[str, str] = {}
    for key, item in value.items():
        if isinstance(item, (dict, list)):
            raise ValueError(f"{path} 값은 scalar여야 합니다.")
        result[str(key)] = str(item)
    return result


def _parse_http_pagination(value: Any) -> dict[str, Any]:
    raw = {} if value is None else value
    if not isinstance(raw, dict):
        raise ValueError("source.api_request.http.pagination은 dict여야 합니다.")
    unknown = sorted(set(raw) - {"strategy", "page_parameter", "size_parameter", "page_size", "start_page", "max_pages"})
    if unknown:
        raise ValueError(f"HTTP pagination의 알 수 없는 키입니다: {unknown}")
    strategy = str(raw.get("strategy") or "none")
    if strategy not in {"none", "page_number"}:
        raise ValueError("HTTP pagination.strategy는 none 또는 page_number여야 합니다.")
    result = {
        "strategy": strategy,
        "page_parameter": str(raw.get("page_parameter") or "pageNo"),
        "size_parameter": str(raw.get("size_parameter") or "numOfRows"),
        "page_size": int(raw.get("page_size") or 1000),
        "start_page": int(raw.get("start_page") or 1),
        "max_pages": int(raw.get("max_pages") or 10_000),
    }
    if result["page_size"] < 1 or result["start_page"] < 1 or result["max_pages"] < 1:
        raise ValueError("HTTP pagination 숫자 설정은 양수여야 합니다.")
    return result


def _parse_http_xml(value: Any, *, required: bool) -> dict[str, Any] | None:
    if value is None:
        if required:
            raise ValueError("http_xml에는 source.api_request.http.xml 설정이 필요합니다.")
        return None
    if not isinstance(value, dict):
        raise ValueError("source.api_request.http.xml은 dict여야 합니다.")
    unknown = sorted(set(value) - {"item_path", "total_count_path", "result_code_path", "result_message_path", "success_codes"})
    if unknown:
        raise ValueError(f"HTTP XML 설정의 알 수 없는 키입니다: {unknown}")
    item_path = str(value.get("item_path") or "").strip()
    if not item_path:
        raise ValueError("HTTP XML item_path 값이 필요합니다.")
    return {
        "item_path": item_path,
        "total_count_path": str(value.get("total_count_path") or "") or None,
        "result_code_path": str(value.get("result_code_path") or "") or None,
        "result_message_path": str(value.get("result_message_path") or "") or None,
        "success_codes": [str(item) for item in (value.get("success_codes") or ["00"])],
    }


def _parse_http_json(value: Any, *, required: bool) -> dict[str, Any] | None:
    if value is None:
        if required:
            raise ValueError("http_json에는 source.api_request.http.json 설정이 필요합니다.")
        return None
    if not isinstance(value, dict):
        raise ValueError("source.api_request.http.json은 dict여야 합니다.")
    unknown = sorted(set(value) - {"item_path", "total_count_path"})
    if unknown:
        raise ValueError(f"HTTP JSON 설정의 알 수 없는 키입니다: {unknown}")
    item_path = str(value.get("item_path") or "").strip()
    if not item_path:
        raise ValueError("HTTP JSON item_path 값이 필요합니다.")
    return {
        "item_path": item_path,
        "total_count_path": str(value.get("total_count_path") or "") or None,
    }


def _parse_window_step(value: Any) -> int | float:
    if value is None:
        return 1
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("source.api_request.date_window.step 은 양수여야 합니다.")
    if value <= 0:
        raise ValueError("source.api_request.date_window.step 은 양수여야 합니다.")
    return value


def _parse_date_window_items(value: Any) -> list[DateWindowItemSpec]:
    if value is None:
        raise ValueError("source.api_request.date_window.date_window 값이 필요합니다.")
    items = _coerce_window_items(value)
    windows = [_parse_single_date_window_item(item) for item in items]
    if not windows:
        raise ValueError("source.api_request.date_window.date_window 는 비어 있을 수 없습니다.")
    return windows


def _coerce_window_items(value: Any) -> list[Any]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, tuple):
        return [list(value)]
    if isinstance(value, list):
        if not value:
            return []
        if _looks_like_single_window_pair(value):
            return [value]
        return list(value)
    return [value]


def _looks_like_single_window_pair(value: list[Any]) -> bool:
    if len(value) != 2:
        return False
    left, right = value
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return True
    return _is_date_like_value(left) and _is_date_like_value(right)


def _is_date_like_value(value: Any) -> bool:
    if isinstance(value, (date, datetime)):
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    try:
        date.fromisoformat(text[:10])
    except ValueError:
        return False
    return True


def _parse_single_date_window_item(value: Any) -> DateWindowItemSpec:
    absolute_window = _parse_absolute_window(value)
    if absolute_window is not None:
        date_from, date_to = absolute_window
        return DateWindowItemSpec(date_from=date_from, date_to=date_to)
    mixed_window = _parse_mixed_window(value)
    if mixed_window is not None:
        return DateWindowItemSpec(mixed_window=mixed_window)
    relative_window = parse_relative_window(value)
    if relative_window is not None:
        return DateWindowItemSpec(relative_window=relative_window)
    raise ValueError(
        "source.api_request.date_window.date_window 값은 "
        "'(-5, 0)', '(2026-05-01, 2026-05-08)', '(2026-05-01, -10)', '(-10, 2026-05-08)' 형식이어야 합니다."
    )


def _parse_absolute_window(value: Any) -> tuple[str, str] | None:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("(") and text.endswith(")"):
            body = text[1:-1]
            parts = [part.strip() for part in body.split(",")]
            if len(parts) != 2:
                raise ValueError("date_window 문자열은 값 2개가 필요합니다.")
            return _coerce_absolute_window_pair(parts[0], parts[1])
        return None
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return _coerce_absolute_window_pair(value[0], value[1])
    return None


def _coerce_absolute_window_pair(left: Any, right: Any) -> tuple[str, str] | None:
    if not (_is_date_like_value(left) and _is_date_like_value(right)):
        return None
    left_date = str(_coerce_date_like_value(left))
    right_date = str(_coerce_date_like_value(right))
    return left_date, right_date


def _parse_mixed_window(value: Any) -> tuple[str | int, str | int] | None:
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


def _coerce_mixed_window_pair(left: Any, right: Any) -> tuple[str | int, str | int] | None:
    left_is_date = _is_date_like_value(left)
    right_is_date = _is_date_like_value(right)
    left_is_offset = _is_int_like_value(left)
    right_is_offset = _is_int_like_value(right)
    if left_is_date and right_is_offset:
        return (str(_coerce_date_like_value(left)), int(right))
    if left_is_offset and right_is_date:
        return (int(left), str(_coerce_date_like_value(right)))
    return None


def _is_int_like_value(value: Any) -> bool:
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


def _coerce_date_like_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value.strip()[:10])
    raise ValueError(f"지원하지 않는 날짜 값입니다: {value!r}")
