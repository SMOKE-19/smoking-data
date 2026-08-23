from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, TypeVar

T = TypeVar("T")


def final_task_limit(execution: Mapping[str, Any] | None) -> int | None:
    if not isinstance(execution, Mapping):
        return None
    test_run = execution.get("test_run")
    if not isinstance(test_run, Mapping):
        return None
    value = test_run.get("final_task_limit")
    return int(value) if value is not None else None


def select_final_tasks(
    tasks: Sequence[T],
    *,
    limit: int | None,
    task_id: Callable[[T], object],
) -> tuple[list[T], dict[str, Any]]:
    planned = list(tasks)
    ordered = sorted(planned, key=lambda item: str(task_id(item)))
    selected = ordered[:limit] if limit is not None else planned
    return selected, {
        "enabled": limit is not None,
        "final_task_limit": limit,
        "sidecar_scope": "global",
        "global_planned_tasks": len(planned),
        "selected_tasks": len(selected),
        "selected_task_ids": [str(task_id(item)) for item in selected],
        "forces_execution": limit is not None,
        "output_scope": (
            "partial_dataset"
            if limit is not None and len(selected) < len(planned)
            else "complete_dataset"
        ),
    }
