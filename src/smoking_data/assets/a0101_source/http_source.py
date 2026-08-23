from __future__ import annotations

import json
import os
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

import pandas as pd
import pyarrow.parquet as pq

from .pipeline.task import SourceTask


def call_http_source(*, output_dir: str | Path, task: SourceTask) -> list[tuple[str, int]]:
    request_spec = dict(task.http_request or {})
    if task.query_mode not in {"http_json", "http_ndjson", "http_xml"} or not request_spec:
        raise ValueError("SOURCE HTTP task contract is missing.")
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    pagination = dict(request_spec.get("pagination") or {})
    strategy = str(pagination.get("strategy") or "none")
    page = int(pagination.get("start_page") or 1)
    max_pages = int(pagination.get("max_pages") or 1)
    page_size = int(pagination.get("page_size") or 1000)
    batch_rows = int(request_spec.get("batch_rows") or 10_000)
    writer_options = dict(task.parquet_writer_options or {})
    outputs: list[tuple[str, int]] = []
    part_index = 0

    for page_offset in range(max_pages):
        current_page = page + page_offset
        payload = _fetch_page(request_spec, task=task, page=current_page)
        if task.query_mode == "http_json":
            records, total_count = _parse_json(
                payload, dict(request_spec.get("json") or {})
            )
        elif task.query_mode == "http_ndjson":
            records, total_count = _parse_ndjson(payload)
        else:
            records, total_count = _parse_xml(
                payload, dict(request_spec.get("xml") or {})
            )
        page_rows = len(records)
        for batch in _batches(records, batch_rows):
            part_index += 1
            path = output_root / f"data_{part_index:04d}_{task.sql_revision}.parquet"
            frame = pd.DataFrame.from_records(batch).astype("string")
            frame.to_parquet(path, **writer_options)
            outputs.append((str(path), pq.ParquetFile(path).metadata.num_rows))
        if strategy == "none":
            break
        if total_count is not None and sum(rows for _, rows in outputs) >= total_count:
            break
        if page_rows < page_size:
            break
    else:
        raise RuntimeError("SOURCE HTTP pagination reached max_pages before a terminal page.")

    if not outputs:
        path = output_root / f"data_0001_{task.sql_revision}.parquet"
        pd.DataFrame().to_parquet(path, **writer_options)
        outputs.append((str(path), 0))
    return outputs


def _fetch_page(request_spec: dict[str, Any], *, task: SourceTask, page: int) -> bytes:
    query = {
        str(name): _render_value(str(value), task=task)
        for name, value in dict(request_spec.get("query") or {}).items()
    }
    pagination = dict(request_spec.get("pagination") or {})
    if pagination.get("strategy") == "page_number":
        query[str(pagination["page_parameter"])] = str(page)
        query[str(pagination["size_parameter"])] = str(pagination["page_size"])
    headers = {
        str(name): _render_value(str(value), task=task)
        for name, value in dict(request_spec.get("headers") or {}).items()
    }
    url = _url_with_query(str(request_spec["url"]), query)
    try:
        with urlopen(Request(url, headers=headers, method="GET"), timeout=float(request_spec["timeout_sec"])) as response:  # noqa: S310 - parser validates HTTP(S).
            limit = int(request_spec.get("max_response_bytes") or 67_108_864)
            content_length = response.headers.get("Content-Length")
            if content_length is not None and int(content_length) > limit:
                raise RuntimeError("SOURCE HTTP response exceeds max_response_bytes.")
            chunks: list[bytes] = []
            received = 0
            for chunk in iter(lambda: response.read(min(1024 * 1024, limit + 1)), b""):
                received += len(chunk)
                if received > limit:
                    raise RuntimeError("SOURCE HTTP response exceeds max_response_bytes.")
                chunks.append(chunk)
            return b"".join(chunks)
    except HTTPError as exc:
        safe = _safe_url(str(request_spec["url"]))
        raise RuntimeError(
            f"SOURCE HTTP GET failed for {safe}: status={exc.code}"
        ) from exc
    except RuntimeError:
        raise
    except Exception as exc:  # noqa: BLE001 - do not expose query/header values.
        safe = _safe_url(str(request_spec["url"]))
        raise RuntimeError(f"SOURCE HTTP GET failed for {safe}: {type(exc).__name__}") from exc


def _render_value(value: str, *, task: SourceTask) -> str:
    expanded = os.path.expandvars(value)
    if "${" in expanded:
        raise RuntimeError("SOURCE HTTP setting references an unavailable environment variable.")
    dates = {"date_from": task.date_from, "date_to": task.date_to}

    def replace(match: re.Match[str]) -> str:
        name = match.group("name")
        format_text = match.group("format")
        raw = dates[name]
        if not format_text:
            return raw
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"SOURCE HTTP {name} is not an ISO datetime.") from exc
        return parsed.strftime(format_text)

    return re.sub(
        r"\{(?P<name>date_from|date_to)(?::(?P<format>[^{}]+))?\}",
        replace,
        expanded,
    )


def _url_with_query(base_url: str, query: dict[str, str]) -> str:
    parts = urlsplit(base_url)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError("SOURCE HTTP URL must use http or https.")
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    pairs.extend(query.items())
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(pairs), parts.fragment))


def _parse_ndjson(payload: bytes) -> tuple[list[dict[str, str | None]], int | None]:
    records: list[dict[str, str | None]] = []
    for line_number, raw_line in enumerate(payload.decode("utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"SOURCE NDJSON line {line_number} is malformed.") from exc
        if not isinstance(value, dict):
            raise ValueError(f"SOURCE NDJSON line {line_number} must be a JSON object.")
        records.append(_flatten_mapping(value))
    return records, None


def _parse_json(
    payload: bytes, spec: dict[str, Any]
) -> tuple[list[dict[str, str | None]], int | None]:
    try:
        root = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("SOURCE JSON response is malformed.") from exc
    items = _json_path(root, str(spec["item_path"]))
    if not isinstance(items, list):
        raise ValueError("SOURCE JSON item_path must resolve to an array.")
    records: list[dict[str, str | None]] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"SOURCE JSON item_path item {index} must be an object.")
        records.append(_flatten_mapping(item))
    total_path = spec.get("total_count_path")
    total_value = _json_path(root, str(total_path)) if total_path else None
    try:
        total_count = int(total_value) if total_value is not None else None
    except (TypeError, ValueError) as exc:
        raise ValueError("SOURCE JSON total_count_path is not an integer.") from exc
    return records, total_count


def _json_path(root: Any, path: str) -> Any:
    current = root
    for part in [item for item in path.split(".") if item]:
        if not isinstance(current, dict) or part not in current:
            raise ValueError(f"SOURCE JSON path does not exist: {path}")
        current = current[part]
    return current


def _parse_xml(payload: bytes, spec: dict[str, Any]) -> tuple[list[dict[str, str | None]], int | None]:
    records: list[dict[str, str | None]] = []
    values: dict[str, str | None] = {}
    stack: list[str] = []
    tracked_paths = {
        str(spec.get(name) or "")
        for name in ("total_count_path", "result_code_path", "result_message_path")
        if spec.get(name)
    }
    item_path = str(spec["item_path"])
    try:
        for event, element in ET.iterparse(BytesIO(payload), events=("start", "end")):
            element.tag = element.tag.rsplit("}", 1)[-1]
            if event == "start":
                stack.append(element.tag)
                continue
            current_path = ".".join(stack)
            if current_path == item_path:
                records.append(_flatten_xml_element(element))
                element.clear()
            elif current_path in tracked_paths:
                values[current_path] = (element.text or "").strip() or None
            stack.pop()
    except ET.ParseError as exc:
        raise ValueError("SOURCE XML response is malformed.") from exc
    code_path = spec.get("result_code_path")
    if code_path:
        code = values.get(str(code_path))
        if code not in set(spec.get("success_codes") or []):
            message = values.get(str(spec.get("result_message_path") or ""))
            raise RuntimeError(f"SOURCE XML API returned result code {code!r}: {message or 'no message'}")
    total_text = values.get(str(spec.get("total_count_path") or ""))
    try:
        total_count = int(total_text) if total_text is not None else None
    except ValueError as exc:
        raise ValueError("SOURCE XML total_count_path is not an integer.") from exc
    return records, total_count


def _flatten_mapping(value: dict[str, Any], *, prefix: str = "") -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for key, item in value.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            result.update(_flatten_mapping(item, prefix=name))
        elif isinstance(item, (list, tuple)):
            result[name] = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        elif item is None:
            result[name] = None
        elif isinstance(item, bool):
            result[name] = "true" if item else "false"
        else:
            result[name] = str(item)
    return result


def _flatten_xml_element(element: ET.Element, *, prefix: str = "") -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    children = list(element)
    if not children:
        result[prefix or element.tag] = (element.text or "").strip() or None
        return result
    for child in children:
        name = f"{prefix}.{child.tag}" if prefix else child.tag
        result.update(_flatten_xml_element(child, prefix=name))
    return result


def _batches(records: list[dict[str, str | None]], size: int) -> Iterable[list[dict[str, str | None]]]:
    for offset in range(0, len(records), size):
        yield records[offset : offset + size]


def _safe_url(value: str) -> str:
    parts = urlsplit(value)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
