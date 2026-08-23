"""Polars 기반 SOURCE 실행 레이어."""

from .api_runner import DataApiResponse, call_data_api
from .io import (
    SourcePathSet,
    build_source_paths,
    dataset_footer_fingerprint,
    normalize_dataset_part_names,
    write_source_dataset_manifest,
)
from .log import (
    SourceLogRecord,
    build_source_log_path,
    emit_source_log_record,
    get_source_logger,
    log_source_event,
)
from .metadata import (
    SourceMetadataRecord,
    read_source_metadata,
    write_source_artifact_provenance,
)
from .orchestrator import (
    SourceOrchestrationPlan,
    SourceRawStageResult,
    build_source_orchestration_plan,
    build_source_yaml_task_queue,
    execute_source_raw_stage,
)
from .sql_builder import (
    SourceWindow,
    build_source_sql,
    build_source_sql_map,
    build_source_template_sql,
    build_source_windows,
    render_source_output_name,
    render_source_sql,
)
from .task import SourceTask, SourceYamlTaskQueue
from .task_builder import build_source_tasks

__all__ = [
    "DataApiResponse",
    "SourceLogRecord",
    "SourceMetadataRecord",
    "SourceOrchestrationPlan",
    "SourcePathSet",
    "SourceRawStageResult",
    "SourceTask",
    "SourceWindow",
    "SourceYamlTaskQueue",
    "build_source_log_path",
    "build_source_orchestration_plan",
    "build_source_paths",
    "build_source_sql",
    "build_source_sql_map",
    "build_source_tasks",
    "build_source_template_sql",
    "build_source_windows",
    "build_source_yaml_task_queue",
    "call_data_api",
    "dataset_footer_fingerprint",
    "emit_source_log_record",
    "execute_source_raw_stage",
    "get_source_logger",
    "log_source_event",
    "normalize_dataset_part_names",
    "read_source_metadata",
    "render_source_output_name",
    "render_source_sql",
    "write_source_artifact_provenance",
    "write_source_dataset_manifest",
]
