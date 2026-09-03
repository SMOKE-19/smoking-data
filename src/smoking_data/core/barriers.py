from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from smoking_data.core.exceptions import ValidationError


class BarrierState(StrEnum):
    SORT_FIRST = "sort_first"
    WINDOW = "window"
    PIVOT = "pivot"
    JOIN_BUILD = "join_build"


@dataclass(frozen=True, slots=True)
class BarrierPolicy:
    state: BarrierState
    spill_supported: bool
    mergeable_aggregations: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


BARRIER_POLICIES: dict[BarrierState, BarrierPolicy] = {
    BarrierState.SORT_FIRST: BarrierPolicy(BarrierState.SORT_FIRST, False),
    BarrierState.WINDOW: BarrierPolicy(BarrierState.WINDOW, False),
    BarrierState.PIVOT: BarrierPolicy(
        BarrierState.PIVOT,
        True,
        ("count", "sum", "min", "max"),
    ),
    BarrierState.JOIN_BUILD: BarrierPolicy(BarrierState.JOIN_BUILD, False),
}


def ensure_complete_group_within_budget(
    *,
    state: BarrierState,
    group_key: dict[str, Any],
    estimated_bytes: int,
    budget_bytes: int,
    rows: int,
) -> None:
    if estimated_bytes <= budget_bytes:
        return
    policy = BARRIER_POLICIES[state]
    raise ValidationError(
        "A complete group exceeds the task memory budget and cannot be split safely.",
        code="physical_plan.oversized_group",
        context={
            "state": state.value,
            "group_key": group_key,
            "rows": rows,
            "estimated_bytes": estimated_bytes,
            "memory_budget_bytes": budget_bytes,
            "spill_supported": policy.spill_supported,
            "suggested_next": [
                "use the automatic spill path when the pivot aggregation is mergeable",
                "increase .smoking-data/config.yaml execution.memory.hard_limit_mb",
                "reduce upstream payload width",
                "partition the complete group earlier",
            ],
        },
    )
