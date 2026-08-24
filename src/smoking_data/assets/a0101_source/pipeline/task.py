"""Polars SOURCE task 모델."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import SourceSpec


@dataclass(slots=True)
class SourceTask:
    job_name: str
    table_id: str
    date_from: str
    date_to: str
    file_stem: str
    sql_text: str
    sql_template: str = ""
    sql_parameters: dict[str, str] | None = None
    sql_renderer_version: str = "source0101-sql-renderer-v1"
    sql_revision: str = ""
    sql_revision_hash: str = ""
    sub_job_name: str | None = None
    task_job_name: str | None = None
    parquet_writer_options: dict[str, Any] | None = None
    query_mode: str = "structured"
    http_request: dict[str, Any] | None = None
    adapter: str = "spi"
    adapter_options: dict[str, Any] | None = None


@dataclass(slots=True)
class SourceYamlTaskQueue:
    yaml_path: str
    spec: SourceSpec
    tasks: list[SourceTask]
