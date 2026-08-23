from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

TASK_RESULT_SCHEMA_VERSION = "smoking-data.task-result.v1"


@dataclass(frozen=True, slots=True)
class TaskSpec:
    task_id: str
    partition_value: str | None = None
    part_index: int | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TaskResult:
    task_id: str
    ok: bool
    pid: int
    partition_value: str | None = None
    part_index: int | None = None
    output_paths: list[Path] = field(default_factory=list)
    counters: dict[str, int | float | str] = field(default_factory=dict)
    error_type: str | None = None
    error_message: str | None = None
    traceback_tail: str | None = None
    schema_version: str = TASK_RESULT_SCHEMA_VERSION
