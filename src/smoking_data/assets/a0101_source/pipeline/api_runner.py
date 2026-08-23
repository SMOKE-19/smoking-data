"""Strict Source backend invocation contract."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .task import SourceTask


@dataclass(slots=True)
class DataApiResponse:
    task: SourceTask
    status: str = "success"
    raw_json_path: str | None = None
    error_message: str | None = None
    attempts: int = 1


def call_data_api(
    task: SourceTask,
    *,
    transport: Callable[..., Any],
    output_dir: str | Path,
) -> Any:
    return _invoke_api_callable(transport, task, output_dir=output_dir)


def _invoke_api_callable(
    func: Callable[..., Any],
    task: SourceTask,
    *,
    output_dir: str | Path,
) -> Any:
    return func(task.sql_text, output_dir=str(Path(output_dir)), task=task)
