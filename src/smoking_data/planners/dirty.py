from __future__ import annotations

from dataclasses import dataclass

from smoking_data.core.tasks import TaskSpec


@dataclass(frozen=True, slots=True)
class DirtyPlan:
    dirty_tasks: list[TaskSpec]
    skipped_tasks: list[TaskSpec]


def select_dirty_tasks(
    tasks: list[TaskSpec],
    *,
    previous_fingerprints: dict[str, str],
    current_fingerprints: dict[str, str],
) -> DirtyPlan:
    dirty: list[TaskSpec] = []
    skipped: list[TaskSpec] = []
    for task in tasks:
        previous = previous_fingerprints.get(task.task_id)
        current = current_fingerprints.get(task.task_id)
        if not current or previous != current:
            dirty.append(task)
        else:
            skipped.append(task)
    return DirtyPlan(dirty_tasks=dirty, skipped_tasks=skipped)
