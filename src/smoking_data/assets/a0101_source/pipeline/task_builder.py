"""Polars SOURCE task 평탄화 유틸."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from .models import SourceSpec
from .spec import load_source_spec
from .sql_builder import (
    build_source_template_sql,
    build_source_windows,
    build_structured_template_sql,
    render_source_output_name,
    render_source_sql,
)
from .task import SourceTask


def build_source_tasks(
    spec_or_path: SourceSpec | str | Path,
    *,
    reference_date: date | datetime | str | None = None,
    date_window: object | None = None,
    step: int | float | None = None,
) -> list[SourceTask]:
    spec = spec_or_path if isinstance(spec_or_path, SourceSpec) else load_source_spec(spec_or_path)
    project_root = spec.project.project_root
    is_http = spec.request.query_mode in {"http_json", "http_ndjson", "http_xml"}
    template_sql = (
        _http_provenance_text(spec)
        if is_http
        else build_source_template_sql(spec)
    )
    windows = build_source_windows(
        spec,
        reference_date=reference_date,
        date_window=date_window,
        step=step,
    )
    tasks: list[SourceTask] = []
    sub_jobs = list(spec.request.sub_jobs or [])
    if spec.request.query_mode == "sql_file" and sub_jobs:
        raise ValueError("SOURCE 0101 sql_file query_mode에서는 filters.sub_job을 사용할 수 없습니다.")
    sub_job_items = sub_jobs or [None]
    for window in windows:
        for sub_job in sub_job_items:
            sub_job_name = None if sub_job is None else sub_job.name
            task_job_name = spec.job.name if sub_job_name is None else f"{spec.job.name}_{sub_job_name}"
            task_template_sql = template_sql
            if sub_job is not None:
                task_template_sql = build_structured_template_sql(
                    spec,
                    filters=[*spec.request.filters, *sub_job.filters],
                )
            revision_document = (
                json.dumps(spec.request.http_request, sort_keys=True, separators=(",", ":"))
                if is_http
                else task_template_sql.strip()
            )
            sql_revision_hash = hashlib.sha256(revision_document.encode("utf-8")).hexdigest()
            file_stem = render_source_output_name(
                spec,
                output_rule="raw_dataset",
                date_from=window.start_at,
                date_to=window.end_at,
                sub_job_name=sub_job_name or "",
                task_job_name=task_job_name,
            )
            tasks.append(
                SourceTask(
                    job_name=spec.job.name,
                    table_id=spec.request.table_id,
                    date_from=window.start_at.isoformat(),
                    date_to=window.end_at.isoformat(),
                    file_stem=file_stem,
                    sql_text=(
                        task_template_sql
                        if is_http
                        else render_source_sql(task_template_sql, date_from=window.date_from, date_to=window.date_to)
                    ),
                    sql_template=task_template_sql,
                    sql_parameters={
                        "dateFrom": window.start_at.strftime("%Y-%m-%d %H:%M:%S"),
                        "dateTo": window.end_at.strftime("%Y-%m-%d %H:%M:%S"),
                    },
                    sql_revision=sql_revision_hash[:16],
                    sql_revision_hash=sql_revision_hash,
                    sub_job_name=sub_job_name,
                    task_job_name=task_job_name,
                    parquet_writer_options=dict(spec.storage.parquet_writer_options),
                    query_mode=spec.request.query_mode,
                    http_request=(dict(spec.request.http_request or {}) if is_http else None),
                    adapter=spec.request.adapter,
                    adapter_options=dict(spec.request.adapter_options),
                )
            )
    return tasks


def _http_provenance_text(spec: SourceSpec) -> str:
    request = dict(spec.request.http_request or {})
    parts = urlsplit(str(request.get("url") or ""))
    safe_url = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    document = {
        "query_mode": spec.request.query_mode,
        "url": safe_url,
        "query_parameter_names": sorted(dict(request.get("query") or {})),
        "header_names": sorted(dict(request.get("headers") or {})),
        "pagination": request.get("pagination"),
        "json": request.get("json"),
        "xml": request.get("xml"),
    }
    return "-- SOURCE 0101 HTTP request contract\n" + json.dumps(
        document, ensure_ascii=False, sort_keys=True
    )
