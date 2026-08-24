"""0101 Source Asset orchestration."""

from __future__ import annotations

import io
import multiprocessing
import os
import re
import shutil
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from smoking_data.runtime.test_run import select_final_tasks

from ..spi_hook import run_spi_prepare_hook
from .api_runner import DataApiResponse, call_data_api
from .io import (
    SourcePathSet,
    build_source_paths,
    cleanup_staging_dataset,
    commit_staged_dataset,
    dataset_footer_fingerprint,
    normalize_dataset_part_names,
    profile_source_written_dataset_result,
    reset_staging_dataset,
    write_source_dataset_manifest,
)
from .log import (
    SourceLogRecord,
    build_source_log_path,
    emit_source_log_record,
    get_source_logger,
)
from .metadata import (
    SourceMetadataRecord,
    write_source_artifact_provenance,
    write_source_dataset_catalog,
)
from .models import SourceSpec
from .spec import load_source_spec
from .sql_builder import build_source_template_sql
from .task import SourceTask, SourceYamlTaskQueue
from .task_builder import build_source_tasks


@dataclass(slots=True)
class SourceOrchestrationPlan:
    spec: SourceSpec
    template_sql: str
    tasks: list[SourceTask]
    test_run: dict[str, object]


@dataclass(slots=True)
class SourceRawStageResult:
    plan: SourceOrchestrationPlan
    responses: list[DataApiResponse]
    path_sets: list[SourcePathSet]
    metadata_records: list[SourceMetadataRecord]
    metadata_paths: list[Path]
    log_records: list[SourceLogRecord]
    log_path: Path


@dataclass(slots=True)
class _SourceRawTaskExecutionResult:
    response: DataApiResponse
    path_set: SourcePathSet
    metadata_record: SourceMetadataRecord
    log_record: SourceLogRecord


@dataclass(slots=True)
class _SourceRawTaskWorkerArgs:
    spec: SourceSpec
    task: SourceTask
    should_write_source_profile: bool
    startup_delay_sec: float = 0.0
    task_index: int = 1
    total_tasks: int = 1


class _CapturedDataApiError(RuntimeError):
    def __init__(self, original: BaseException, *, stdout_text: str, stderr_text: str) -> None:
        super().__init__(str(original))
        self.original = original
        self.stdout_text = stdout_text
        self.stderr_text = stderr_text


def build_source_yaml_task_queue(
    yaml_path: str | Path,
    *,
    reference_date: date | datetime | str | None = None,
    date_window: object | None = None,
    step: int | float | None = None,
) -> SourceYamlTaskQueue:
    spec = load_source_spec(yaml_path)
    tasks = build_source_tasks(
        spec,
        reference_date=reference_date,
        date_window=date_window,
        step=step,
    )
    tasks = sorted(tasks, key=lambda item: (item.date_from, item.date_to, item.file_stem))
    return SourceYamlTaskQueue(
        yaml_path=str(Path(yaml_path)),
        spec=spec,
        tasks=tasks,
    )


def build_source_orchestration_plan(
    yaml_path: str | Path,
    *,
    reference_date: date | datetime | str | None = None,
    date_window: object | None = None,
    step: int | float | None = None,
) -> SourceOrchestrationPlan:
    yaml_queue = build_source_yaml_task_queue(
        yaml_path,
        reference_date=reference_date,
        date_window=date_window,
        step=step,
    )
    template_sql = (
        yaml_queue.tasks[0].sql_template
        if yaml_queue.spec.request.query_mode in {"http_json", "http_ndjson", "http_xml"}
        else build_source_template_sql(yaml_queue.spec)
    )
    selected_tasks, test_run = select_final_tasks(
        yaml_queue.tasks,
        limit=yaml_queue.spec.execution.test_run_final_task_limit,
        task_id=lambda task: task.file_stem,
    )
    return SourceOrchestrationPlan(
        spec=yaml_queue.spec,
        template_sql=template_sql,
        tasks=selected_tasks,
        test_run=test_run,
    )


def execute_source_raw_stage(
    yaml_path: str | Path,
    *,
    transport: Callable[..., object] | None = None,
    reference_date: date | datetime | str | None = None,
    date_window: object | None = None,
    step: int | float | None = None,
) -> SourceRawStageResult:
    plan = build_source_orchestration_plan(
        yaml_path,
        reference_date=reference_date,
        date_window=date_window,
        step=step,
    )
    if plan.spec.execution.reset_before_run:
        _reset_source_run_artifacts(plan.spec)
    log_path = build_source_log_path(log_path=plan.spec.logging.path)
    logger = get_source_logger(log_path=log_path, job_name=plan.spec.job.name)
    _emit_progress(
        f"🧩 [SOURCE] 시작: yaml={Path(yaml_path).resolve()}, job={plan.spec.job.name}, "
        f"tasks={len(plan.tasks)}/{plan.test_run['global_planned_tasks']}, "
        f"workers={max(1, int(plan.spec.execution.workers or 1))}"
    )

    responses: list[DataApiResponse] = []
    path_sets: list[SourcePathSet] = []
    metadata_records: list[SourceMetadataRecord] = []
    log_records: list[SourceLogRecord] = []
    should_write_source_profile = bool(plan.spec.execution.write_source_profile_json)
    if plan.spec.request.spi_prepare is not None:
        run_spi_prepare_hook(
            plan.spec.request.spi_prepare,
            project_root=plan.spec.project.project_root,
            temp_root=plan.spec.project.temp_root,
        )
    task_results = _execute_source_raw_tasks(
        plan,
        should_write_source_profile=should_write_source_profile,
        transport=transport,
    )
    for item in task_results:
        path_sets.append(item.path_set)
        responses.append(item.response)
        metadata_records.append(item.metadata_record)
        log_records.append(item.log_record)
        emit_source_log_record(logger, item.log_record, job_name=plan.spec.job.name)

    metadata_paths = [
        item.raw_json_path / "_smoking_data" / "metadata.json"
        for item, response in zip(path_sets, responses, strict=True)
        if response.status == "success"
    ]
    success_tasks = sum(1 for item in task_results if item.response.status == "success")
    error_tasks = sum(1 for item in task_results if item.response.status != "success")
    write_source_dataset_catalog(plan.spec.storage.raw_dir)
    _emit_progress(
        f"🏁 [SOURCE] 완료: job={plan.spec.job.name}, raw_datasets={len(path_sets)}, success_tasks={success_tasks}, error_tasks={error_tasks}"
    )
    return SourceRawStageResult(
        plan=plan,
        responses=responses,
        path_sets=path_sets,
        metadata_records=metadata_records,
        metadata_paths=metadata_paths,
        log_records=log_records,
        log_path=log_path,
    )


def _reset_source_run_artifacts(spec: SourceSpec) -> None:
    output_dir = Path(spec.storage.raw_dir).resolve()
    artifact_paths = [("log", Path(spec.logging.path).resolve())]
    forbidden = {
        Path(output_dir.anchor).resolve(),
        Path(spec.project.project_root).resolve(),
        Path(spec.project.data_root).resolve(),
        Path.home().resolve(),
    }
    if output_dir in forbidden:
        raise ValueError(
            "execution.reset_before_run refuses to reset a broad output directory: "
            f"{output_dir}"
        )
    for label, path in artifact_paths:
        if path.exists() and path.is_dir():
            raise ValueError(
                f"execution.reset_before_run expected a {label} file, got directory: {path}"
            )
    if not output_dir.exists():
        pass
    elif output_dir.is_dir():
        shutil.rmtree(output_dir)
    else:
        output_dir.unlink()

    for _, path in artifact_paths:
        if not path.exists():
            continue
        path.unlink()


def _invoke_call_data_api(
    task: SourceTask,
    *,
    transport: Callable[..., object] | None = None,
    output_dir: str | Path,
) -> object:
    return call_data_api(task, transport=transport, output_dir=output_dir)


def _execute_source_raw_tasks(
    plan: SourceOrchestrationPlan,
    *,
    should_write_source_profile: bool,
    transport: Callable[..., object] | None,
) -> list[_SourceRawTaskExecutionResult]:
    worker_count = max(1, int(plan.spec.execution.workers or 1))
    if transport is not None:
        return [
            _execute_single_source_raw_task(
                plan.spec,
                task,
                should_write_source_profile=should_write_source_profile,
                transport=transport,
            )
            for task in plan.tasks
        ]

    results: list[_SourceRawTaskExecutionResult] = []
    remaining_tasks = list(plan.tasks)
    if plan.spec.execution.warmup_first_task and remaining_tasks:
        first_task = remaining_tasks.pop(0)
        results.append(
            _execute_source_raw_task_in_fresh_process(
                _SourceRawTaskWorkerArgs(
                    spec=plan.spec,
                    task=first_task,
                    should_write_source_profile=should_write_source_profile,
                    task_index=1,
                    total_tasks=len(plan.tasks),
                )
            )
        )
    if not remaining_tasks:
        return results

    startup_delay = float(plan.spec.execution.worker_start_delay_sec or 0.0)
    worker_args = [
        _SourceRawTaskWorkerArgs(
            spec=plan.spec,
            task=task,
            should_write_source_profile=should_write_source_profile,
            startup_delay_sec=(startup_delay * index) if startup_delay > 0 else 0.0,
            task_index=index + 1,
            total_tasks=len(remaining_tasks),
        )
        for index, task in enumerate(remaining_tasks)
    ]
    _emit_progress(
        f"🧵 [SOURCE] 태스크 프로세스 실행 시작: tasks={len(worker_args)}, workers={worker_count}, lifetime=task"
    )
    if worker_count == 1:
        for args in worker_args:
            results.append(_execute_source_raw_task_in_fresh_process(args))
        return results

    indexed_results: dict[int, _SourceRawTaskExecutionResult] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_index = {
            executor.submit(_execute_source_raw_task_in_fresh_process, args): index
            for index, args in enumerate(worker_args)
        }
        for future in as_completed(future_to_index):
            indexed_results[future_to_index[future]] = future.result()
    results.extend(indexed_results[index] for index in sorted(indexed_results))
    return results


def _execute_source_raw_task_in_fresh_process(args: _SourceRawTaskWorkerArgs) -> _SourceRawTaskExecutionResult:
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as executor:
        return next(executor.map(_execute_source_raw_task_worker, [args]))


def _execute_source_raw_task_worker(args: _SourceRawTaskWorkerArgs) -> _SourceRawTaskExecutionResult:
    from smoking_data.assets.a0101_source.source_api import call_data_api as transport

    return _execute_single_source_raw_task(
        args.spec,
        args.task,
        should_write_source_profile=args.should_write_source_profile,
        transport=transport,
        startup_delay_sec=args.startup_delay_sec,
        task_index=args.task_index,
        total_tasks=args.total_tasks,
    )


def _execute_single_source_raw_task(
    spec: SourceSpec,
    task: SourceTask,
    *,
    should_write_source_profile: bool,
    transport: Callable[..., object],
    startup_delay_sec: float = 0.0,
    task_index: int = 1,
    total_tasks: int = 1,
) -> _SourceRawTaskExecutionResult:
    if startup_delay_sec > 0:
        time.sleep(startup_delay_sec)
    path_set = build_source_paths(spec, task)
    task_job_fields = _task_job_fields(task)
    _emit_progress(
        f"⚙️ [SOURCE] 태스크 시작: {task_index}/{total_tasks}, pid={os.getpid()}, file_stem={task.file_stem}"
    )
    max_retries = max(0, int(spec.execution.max_retries))
    retry_backoff_sec = max(0.0, float(spec.execution.retry_backoff_sec))
    max_attempts = max_retries + 1
    attempt = 0
    while True:
        attempt += 1
        reset_staging_dataset(path_set.staging_dataset_path)
        try:
            payload, api_stdout, api_stderr = _invoke_call_data_api_with_capture(
                spec,
                task,
                transport=transport,
                output_dir=path_set.staging_dataset_path,
            )
            captured_fields, capture_status, capture_match_count = _collect_data_api_print_fields(
                spec,
                stdout_text=api_stdout,
                stderr_text=api_stderr,
            )
            profile = profile_source_written_dataset_result(
                spec,
                path_set.staging_dataset_path,
                payload,
            )
            sql_revision_hash = task.sql_revision_hash
            sql_revision = task.sql_revision
            if not sql_revision_hash or not sql_revision:
                raise ValueError("SOURCE task SQL revision fields must be populated.")
            normalized_parts = normalize_dataset_part_names(
                path_set.staging_dataset_path,
                sql_revision=sql_revision,
            )
            _normalize_source_write_profile(
                profile,
                normalized_parts=normalized_parts,
                final_dataset_path=path_set.raw_json_path,
            )
            dataset_fingerprint = dataset_footer_fingerprint(path_set.staging_dataset_path)
            metadata_record = SourceMetadataRecord(
                raw_dataset_path=str(path_set.raw_json_path),
                status="success",
                job_name=task_job_fields["job_name"],
                sub_job_name=task_job_fields["sub_job_name"],
                task_job_name=task_job_fields["task_job_name"],
                date_from=task.date_from,
                date_to=task.date_to,
                sql_text=task.sql_text,
                sql_template=task.sql_template or None,
                sql_parameters=task.sql_parameters,
                sql_renderer_version=task.sql_renderer_version,
                sql_revision=sql_revision,
                sql_revision_hash=sql_revision_hash,
                raw_dataset_fingerprint=dataset_fingerprint,
                source_write_profile_path=(
                    str(path_set.raw_json_path / "_smoking_data" / "source-write-profile.json")
                    if should_write_source_profile
                    else None
                ),
                data_api_captured_fields=captured_fields or None,
                data_api_capture_status=capture_status,
                data_api_capture_match_count=capture_match_count,
                attempts=attempt,
                test_run=_source_test_run_metadata(spec),
            )
            write_source_artifact_provenance(
                path_set.staging_dataset_path,
                record=metadata_record,
                definition_path=spec.path,
                query_sql=task.sql_text,
                source_write_profile=(
                    {"source_write_profile": profile}
                    if should_write_source_profile
                    else None
                ),
            )
            write_source_dataset_manifest(path_set.staging_dataset_path)
            commit_staged_dataset(path_set.staging_dataset_path, path_set.raw_json_path)
            response = DataApiResponse(
                task=task,
                status="success",
                raw_json_path=str(path_set.raw_json_path),
                error_message=None,
                attempts=attempt,
            )
            log_record = SourceLogRecord(
                stage="raw_json",
                status="success",
                message=(
                    "DATA API response saved as parquet dataset. "
                    f"rows_written={profile.get('rows_written') or 0}"
                ),
                file_stem=task.file_stem,
                sub_job_name=task_job_fields["sub_job_name"],
                task_job_name=task_job_fields["task_job_name"],
                attempt=attempt,
            )
            _emit_progress(
                f"✅ [SOURCE] 태스크 완료: {task_index}/{total_tasks}, pid={os.getpid()}, file_stem={task.file_stem}, attempt={attempt}/{max_attempts}, raw_dataset={path_set.raw_json_path.name}{_format_data_api_capture_summary(captured_fields, capture_status)}"
            )
            return _SourceRawTaskExecutionResult(
                response=response,
                path_set=path_set,
                metadata_record=metadata_record,
                log_record=log_record,
            )
        except Exception as exc:
            api_stdout = getattr(exc, "stdout_text", locals().get("api_stdout", ""))
            api_stderr = getattr(exc, "stderr_text", locals().get("api_stderr", ""))
            display_exc = getattr(exc, "original", exc)
            if attempt < max_attempts and _is_retryable_source_error(
                spec,
                exc,
                stdout_text=api_stdout,
                stderr_text=api_stderr,
            ):
                _emit_progress(
                    f"🔁 [SOURCE] 태스크 재시도: {task_index}/{total_tasks}, pid={os.getpid()}, file_stem={task.file_stem}, attempt={attempt}/{max_attempts}, delay_sec={retry_backoff_sec}, error={exc}"
                )
                if retry_backoff_sec > 0:
                    time.sleep(retry_backoff_sec)
                continue
            captured_fields, capture_status, capture_match_count = _collect_data_api_print_fields(
                spec,
                stdout_text=api_stdout,
                stderr_text=api_stderr,
            )
            response = DataApiResponse(
                task=task,
                status="error",
                raw_json_path=None,
                error_message=str(display_exc),
                attempts=attempt,
            )
            metadata_record = SourceMetadataRecord(
                raw_dataset_path=str(path_set.raw_json_path),
                status="error",
                job_name=task_job_fields["job_name"],
                sub_job_name=task_job_fields["sub_job_name"],
                task_job_name=task_job_fields["task_job_name"],
                date_from=task.date_from,
                date_to=task.date_to,
                sql_text=task.sql_text,
                sql_template=(task.sql_template or None),
                sql_parameters=task.sql_parameters,
                sql_renderer_version=task.sql_renderer_version,
                sql_revision=(task.sql_revision or None),
                sql_revision_hash=(task.sql_revision_hash or None),
                raw_dataset_fingerprint=None,
                source_write_profile_path=None,
                data_api_captured_fields=captured_fields or None,
                data_api_capture_status=capture_status,
                data_api_capture_match_count=capture_match_count,
                attempts=attempt,
                last_error_message=str(display_exc),
                last_error_stage="api_call",
                last_error_code="api_call_failed",
                last_error_stdout=api_stdout or None,
                last_error_stderr=api_stderr or None,
                test_run=_source_test_run_metadata(spec),
            )
            log_record = SourceLogRecord(
                stage="raw_json",
                status="error",
                message=response.error_message or "DATA API call failed.",
                file_stem=task.file_stem,
                sub_job_name=task_job_fields["sub_job_name"],
                task_job_name=task_job_fields["task_job_name"],
                attempt=attempt,
                stdout=api_stdout or None,
                stderr=api_stderr or None,
            )
            _emit_progress(
                f"❌ [SOURCE] 태스크 실패: {task_index}/{total_tasks}, pid={os.getpid()}, file_stem={task.file_stem}, attempts={attempt}/{max_attempts}, error={response.error_message or 'DATA API call failed.'}"
            )
            return _SourceRawTaskExecutionResult(
                response=response,
                path_set=path_set,
                metadata_record=metadata_record,
                log_record=log_record,
            )
        finally:
            cleanup_staging_dataset(
                path_set.staging_dataset_path,
                staging_root=path_set.staging_root_path,
            )


def _task_job_fields(task: SourceTask) -> dict[str, str | None]:
    return {
        "job_name": task.job_name,
        "sub_job_name": task.sub_job_name,
        "task_job_name": task.task_job_name or task.job_name,
    }


def _source_test_run_metadata(spec: SourceSpec) -> dict[str, object] | None:
    limit = spec.execution.test_run_final_task_limit
    if limit is None:
        return None
    return {
        "enabled": True,
        "final_task_limit": limit,
        "sidecar_scope": "published_dataset_root",
        "output_scope": "partial_dataset",
    }


def _normalize_source_write_profile(
    profile: dict[str, object],
    *,
    normalized_parts: list[Path],
    final_dataset_path: Path,
) -> None:
    profile["output_file"] = str(final_dataset_path)
    profile["dataset_dir"] = str(final_dataset_path)
    profile["chunks_written"] = len(normalized_parts)
    if "api_written_files" in profile:
        profile["api_written_files"] = [
            str(final_dataset_path / part.name) for part in normalized_parts
        ]


def _is_retryable_source_error(
    spec: SourceSpec,
    exc: BaseException,
    *,
    stdout_text: str,
    stderr_text: str,
) -> bool:
    substrings = [
        str(item).strip().lower()
        for item in spec.execution.retryable_error_substrings
        if str(item).strip()
    ]
    if not substrings:
        return False
    haystack = "\n".join(
        item
        for item in (
            _flatten_error_text(exc),
            stdout_text,
            stderr_text,
        )
        if item
    ).lower()
    return any(item in haystack for item in substrings)


def _flatten_error_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, BaseException):
        parts = [type(value).__name__, str(value)]
        parts.extend(_flatten_error_text(item) for item in getattr(value, "args", ()) or ())
        parts.append(_flatten_error_text(getattr(value, "original", None)))
        parts.append(_flatten_error_text(value.__cause__))
        parts.append(_flatten_error_text(value.__context__))
        return "\n".join(part for part in parts if part)
    if isinstance(value, dict):
        parts: list[str] = []
        for key, item in value.items():
            parts.append(str(key))
            parts.append(_flatten_error_text(item))
        return "\n".join(part for part in parts if part)
    if isinstance(value, (list, tuple, set, frozenset)):
        return "\n".join(_flatten_error_text(item) for item in value)
    return str(value)


def _emit_progress(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def _format_data_api_capture_summary(
    captured_fields: dict[str, object],
    capture_status: str,
) -> str:
    if capture_status != "matched":
        return ""
    execution_time = captured_fields.get("api_execution_time_sec")
    rows = captured_fields.get("api_rows")
    dataset_parts = captured_fields.get("api_dataset_parts")
    if execution_time is None or rows is None or dataset_parts is None:
        return ""
    return (
        f", api_time_sec={execution_time}, api_rows={rows}, api_dataset_parts={dataset_parts}"
    )


def _invoke_call_data_api_with_capture(
    spec: SourceSpec,
    task: SourceTask,
    *,
    transport: Callable[..., object] | None,
    output_dir: str | Path | None,
) -> tuple[object, str, str]:
    del spec
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    try:
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            payload = _invoke_call_data_api(
                task,
                transport=transport,
                output_dir=output_dir,
            )
    except Exception as exc:
        raise _CapturedDataApiError(
            exc,
            stdout_text=stdout_buffer.getvalue(),
            stderr_text=stderr_buffer.getvalue(),
        ) from exc
    return payload, stdout_buffer.getvalue(), stderr_buffer.getvalue()


def _collect_data_api_print_fields(
    spec: SourceSpec,
    *,
    stdout_text: str,
    stderr_text: str,
) -> tuple[dict[str, object], str, int]:
    result: dict[str, object] = {}
    total_matches = 0
    if not spec.execution.data_api_print_capture_rules:
        return result, "disabled", 0
    combined_text = "\n".join(part for part in (stdout_text, stderr_text) if part)
    enabled_rules = [rule for rule in spec.execution.data_api_print_capture_rules if rule.enabled]
    if not enabled_rules:
        return result, "disabled", 0
    for rule in enabled_rules:
        if rule.capture == "full_text":
            if combined_text:
                result[rule.field] = combined_text
                total_matches += 1
            continue
        if not rule.regex:
            continue
        pattern = re.compile(rule.regex, flags=re.MULTILINE)
        matches = list(pattern.finditer(combined_text))
        if not matches:
            continue
        total_matches += len(matches)
        match = matches[-1]
        result[rule.field] = _coerce_captured_print_value(rule.field, match)
    if result:
        return result, "matched", total_matches
    return result, "no_match", 0


def _coerce_captured_print_value(field: str, match: re.Match[str]) -> str:
    if field.endswith("_sec") and match.lastindex == 2:
        minute_text = match.group(1) or "0"
        second_text = match.group(2)
        total_seconds = (int(minute_text) * 60.0) + float(second_text)
        normalized = f"{total_seconds:.6f}".rstrip("0").rstrip(".")
        return normalized
    if match.lastindex:
        return str(match.group(1))
    return str(match.group(0))
