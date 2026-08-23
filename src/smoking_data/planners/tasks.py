from __future__ import annotations

from collections.abc import Iterable

from smoking_data.core.tasks import TaskSpec


def plan_partition_part_tasks(
    *,
    partition_values: Iterable[str],
    parts_per_partition: int = 1,
    payload: dict | None = None,
) -> list[TaskSpec]:
    tasks: list[TaskSpec] = []
    base_payload = payload or {}
    for partition_value in sorted(str(value) for value in partition_values):
        for part_index in range(parts_per_partition):
            tasks.append(
                TaskSpec(
                    task_id=f"{partition_value}__part-{part_index:05d}",
                    partition_value=partition_value,
                    part_index=part_index,
                    payload=dict(base_payload),
                )
            )
    return tasks
