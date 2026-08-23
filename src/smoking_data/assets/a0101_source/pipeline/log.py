"""Polars SOURCE 로그 유틸."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class SourceLogRecord:
    stage: str
    status: str
    message: str
    file_stem: str | None = None
    sub_job_name: str | None = None
    task_job_name: str | None = None
    error_code: str | None = None
    attempt: int | None = None
    retry_delay_sec: float | None = None
    stdout: str | None = None
    stderr: str | None = None


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "logger": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for key in (
            "stage",
            "status",
            "file_stem",
            "job_name",
            "sub_job_name",
            "task_job_name",
            "error_code",
            "attempt",
            "retry_delay_sec",
        ):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        for key in ("stdout", "stderr"):
            value = getattr(record, key, None)
            if value:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def build_source_log_path(*, log_path: str | Path) -> Path:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_source_logger(*, log_path: str | Path, job_name: str) -> logging.Logger:
    resolved_path = Path(log_path).resolve()
    logger_name = f"smoking_data.source.{job_name}.{resolved_path}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers and not resolved_path.exists():
        for existing_handler in list(logger.handlers):
            existing_handler.close()
            logger.removeHandler(existing_handler)
    if not logger.handlers:
        handler = logging.FileHandler(resolved_path, encoding="utf-8")
        handler.setFormatter(JsonLineFormatter())
        logger.addHandler(handler)
    return logger


def log_source_event(
    logger: logging.Logger,
    *,
    stage: str,
    status: str,
    message: str,
    file_stem: str | None = None,
    job_name: str | None = None,
    error_code: str | None = None,
    sub_job_name: str | None = None,
    task_job_name: str | None = None,
    attempt: int | None = None,
    retry_delay_sec: float | None = None,
    stdout: str | None = None,
    stderr: str | None = None,
) -> SourceLogRecord:
    level = logging.INFO if status == "success" else logging.ERROR
    logger.log(
        level,
        message,
        extra={
            "stage": stage,
            "status": status,
            "file_stem": file_stem,
            "job_name": job_name,
            "sub_job_name": sub_job_name,
            "task_job_name": task_job_name,
            "error_code": error_code,
            "attempt": attempt,
            "retry_delay_sec": retry_delay_sec,
            "stdout": stdout,
            "stderr": stderr,
        },
    )
    return SourceLogRecord(
        stage=stage,
        status=status,
        message=message,
        file_stem=file_stem,
        sub_job_name=sub_job_name,
        task_job_name=task_job_name,
        error_code=error_code,
        attempt=attempt,
        retry_delay_sec=retry_delay_sec,
        stdout=stdout,
        stderr=stderr,
    )


def emit_source_log_record(
    logger: logging.Logger,
    record: SourceLogRecord,
    *,
    job_name: str | None = None,
) -> None:
    level = logging.INFO if record.status == "success" else logging.ERROR
    logger.log(
        level,
        record.message,
        extra={
            "stage": record.stage,
            "status": record.status,
            "file_stem": record.file_stem,
            "job_name": job_name,
            "sub_job_name": record.sub_job_name,
            "task_job_name": record.task_job_name,
            "error_code": record.error_code,
            "attempt": record.attempt,
            "retry_delay_sec": record.retry_delay_sec,
            "stdout": record.stdout,
            "stderr": record.stderr,
        },
    )
