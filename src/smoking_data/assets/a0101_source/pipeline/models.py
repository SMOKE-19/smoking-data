"""SOURCE 정규화 결과의 내부 모델."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from smoking_data.assets.a0101_source.spec_common.project import ProjectPaths

QueryMode = Literal["structured", "sql_file", "http_json", "http_ndjson", "http_xml"]


@dataclass(slots=True)
class DateWindowItemSpec:
    date_from: str | None = None
    date_to: str | None = None
    relative_window: tuple[int, int] | None = None
    mixed_window: tuple[str | int, str | int] | None = None


@dataclass(slots=True)
class DateWindowSpec:
    column: str
    step: int | float
    windows: list[DateWindowItemSpec]


@dataclass(slots=True)
class ColumnSpec:
    name: str
    expr: str
    data_type: str = "TEXT"


@dataclass(slots=True)
class SourceJobSpec:
    name: str
    description: str


@dataclass(slots=True)
class SourceStorageSpec:
    temp_root: str
    root_dir: str
    raw_dataset_file_name: str
    raw_dir: str = ""
    parquet_writer_options: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SourceLoggingSpec:
    path: str


@dataclass(slots=True)
class SourceExecutionSpec:
    reset_before_run: bool
    write_source_profile_json: bool
    retryable_error_substrings: list[str]
    data_api_print_capture_rules: list[SourceApiPrintCaptureRule]
    workers: int
    warmup_first_task: bool
    worker_start_delay_sec: float
    max_retries: int
    retry_backoff_sec: float
    test_run_final_task_limit: int | None = None


@dataclass(slots=True)
class SourceApiPrintCaptureRule:
    field: str
    enabled: bool = True
    capture: str = "regex"
    regex: str | None = None


@dataclass(slots=True)
class SourceRequestSpec:
    table_id: str
    query_mode: QueryMode
    date_window: DateWindowSpec
    columns: list[ColumnSpec]
    filters: list[str]
    sub_jobs: list[SourceSubJobSpec] | None = None
    sql_file_path: str | None = None
    http_request: dict[str, Any] | None = None


@dataclass(slots=True)
class SourceSubJobSpec:
    name: str
    filters: list[str]


@dataclass(slots=True)
class SourceSpec:
    schema_version: str
    path: Path
    project: ProjectPaths
    raw: dict[str, Any]
    resolved: dict[str, Any]
    job: SourceJobSpec
    request: SourceRequestSpec
    storage: SourceStorageSpec
    logging: SourceLoggingSpec
    execution: SourceExecutionSpec
