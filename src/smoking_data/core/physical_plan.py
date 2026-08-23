from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from typing import Any

PHYSICAL_PLAN_VERSION = "smoking-data.physical-plan.v1"
DEFAULT_OUTPUT_ROW_GROUP_TARGET_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class SourceSpan:
    source_name: str
    path: str
    size_bytes: int
    estimated_read_bytes: int | None = None
    estimated_uncompressed_bytes: int | None = None
    row_groups: tuple[int, ...] = ()
    selected_rows: int | None = None
    row_index_min: int | None = None
    row_index_max: int | None = None


@dataclass(frozen=True, slots=True)
class PhysicalTask:
    task_id: str
    partition_value: str
    batch_index: int | None
    part_index: int
    single_partition_guaranteed: bool
    source_spans: tuple[SourceSpan, ...]
    expected_input_rows: int | None
    expected_output_rows: int | None
    expected_payload_bytes: int
    file_fanout: int
    row_group_fanout: int
    state_estimate_bytes: int | None = None
    expected_spawn_overhead_units: int = 0
    execution_group_hint: int = 1
    risk: str = "bounded"


@dataclass(frozen=True, slots=True)
class PhysicalTaskPlan:
    logical_plan_hash: str
    tasks: tuple[PhysicalTask, ...]
    decisions: tuple[dict[str, Any], ...] = ()
    rejected_candidates: tuple[dict[str, Any], ...] = ()
    version: str = PHYSICAL_PLAN_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def plan_hash(self) -> str:
        payload = json.dumps(
            self.to_dict(),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class PhysicalPlanCost:
    feasible: bool
    peak_state_bytes: int | None
    payload_bytes: int
    file_fanout: int
    process_count: int
    small_file_penalty: int
    conservative_penalty: int
    estimated_io_bytes: int
    estimated_cpu_units: int
    estimated_elapsed_units: int
    estimated_spawn_overhead_units: int
    estimated_writer_appends: int
    estimated_output_files: int
    estimated_partition_switches: int

    @property
    def score(self) -> tuple[int, ...]:
        return (
            0 if self.feasible else 1,
            self.conservative_penalty,
            self.peak_state_bytes or 2**63 - 1,
            self.file_fanout,
            self.small_file_penalty,
            self.estimated_writer_appends,
            self.estimated_output_files,
            self.estimated_partition_switches,
            self.estimated_elapsed_units,
            self.estimated_spawn_overhead_units,
            self.estimated_cpu_units,
            self.estimated_io_bytes,
            self.process_count,
        )


def choose_output_row_group_rows(
    task: PhysicalTask,
    *,
    override_rows: int | None = None,
    target_bytes: int = DEFAULT_OUTPUT_ROW_GROUP_TARGET_BYTES,
    minimum_rows: int = 1_000,
    maximum_rows: int = 128_000,
) -> int:
    """Choose a bounded writer row-group size from the planned payload width."""
    if override_rows is not None:
        return max(1, int(override_rows))
    rows = task.expected_input_rows or task.expected_output_rows
    if not rows or task.expected_payload_bytes <= 0:
        return min(maximum_rows, max(minimum_rows, 20_000))
    estimated_row_width = max(1, task.expected_payload_bytes // rows)
    selected = max(1, target_bytes // estimated_row_width)
    return min(maximum_rows, max(minimum_rows, selected))


def build_physical_plan(
    *,
    logical_plan_hash: str,
    tasks: list[PhysicalTask],
    decisions: list[dict[str, Any]] | None = None,
    rejected_candidates: list[dict[str, Any]] | None = None,
) -> PhysicalTaskPlan:
    task_ids = [task.task_id for task in tasks]
    if len(task_ids) != len(set(task_ids)):
        raise ValueError(f"Physical task IDs must be unique: {task_ids}")
    return PhysicalTaskPlan(
        logical_plan_hash=logical_plan_hash,
        tasks=tuple(tasks),
        decisions=tuple(decisions or ()),
        rejected_candidates=tuple(rejected_candidates or ()),
    )


def build_logical_physical_candidates(
    template: PhysicalTaskPlan,
    *,
    logical_plan_hashes: list[str],
) -> list[PhysicalTaskPlan]:
    """Bind one executable task shape to each validated semantic-equivalent plan."""
    unique_hashes = list(dict.fromkeys(logical_plan_hashes))
    if not unique_hashes:
        raise ValueError("At least one logical plan hash is required.")
    return [
        replace(
            template,
            logical_plan_hash=logical_hash,
            decisions=(
                *template.decisions,
                {
                    "decision": "logical_rewrite_candidate",
                    "logical_plan_hash": logical_hash,
                    "task_shape": "semantic_equivalent",
                },
            ),
        )
        for logical_hash in unique_hashes
    ]


def reconcile_task_memory(
    plan: PhysicalTaskPlan,
    task_results: list[Any],
) -> dict[str, Any]:
    result_by_id = {str(result.task_id): result for result in task_results}
    tasks: list[dict[str, Any]] = []
    for task in plan.tasks:
        result = result_by_id.get(task.task_id)
        counters = dict(getattr(result, "counters", {}) or {})
        rss_start = _optional_float(counters.get("rss_start_mb"))
        rss_peak = _optional_float(counters.get("rss_peak_mb"))
        actual_delta_bytes = (
            max(0.0, rss_peak - rss_start) * 1024 * 1024
            if rss_start is not None and rss_peak is not None
            else None
        )
        estimate = task.state_estimate_bytes
        tasks.append(
            {
                "task_id": task.task_id,
                "risk": task.risk,
                "planned_source_files": task.file_fanout,
                "planned_row_groups": task.row_group_fanout,
                "planned_input_rows": task.expected_input_rows,
                "planned_payload_bytes": task.expected_payload_bytes,
                "actual_source_files_touched": counters.get("source_files_touched"),
                "actual_row_groups_touched": counters.get("row_groups_touched"),
                "actual_selected_rows": counters.get("rust_input_rows", counters.get("input_rows")),
                "actual_projected_array_bytes": counters.get(
                    "rust_projected_input_array_bytes",
                    counters.get("projected_input_array_bytes"),
                ),
                "estimated_state_bytes": estimate,
                "rss_start_mb": rss_start,
                "rss_peak_mb": rss_peak,
                "actual_peak_growth_bytes": (
                    int(actual_delta_bytes) if actual_delta_bytes is not None else None
                ),
                "estimate_error_bytes": (
                    int(actual_delta_bytes - estimate)
                    if actual_delta_bytes is not None and estimate is not None
                    else None
                ),
            }
        )
    comparable = [item for item in tasks if item["estimate_error_bytes"] is not None]
    return {
        "tasks": tasks,
        "comparable_tasks": len(comparable),
        "max_absolute_error_bytes": max(
            (abs(int(item["estimate_error_bytes"])) for item in comparable),
            default=None,
        ),
    }


def admitted_worker_count(
    plan: PhysicalTaskPlan,
    *,
    requested_workers: int,
    memory_budget_bytes: int,
) -> int:
    requested = max(1, int(requested_workers))
    estimates = [
        int(task.state_estimate_bytes)
        for task in plan.tasks
        if task.state_estimate_bytes is not None and task.state_estimate_bytes > 0
    ]
    if not estimates:
        return 1
    conservative_task_bytes = max(estimates)
    memory_limited = max(1, memory_budget_bytes // conservative_task_bytes)
    return min(requested, len(plan.tasks) or 1, memory_limited)


def estimate_plan_cost(
    plan: PhysicalTaskPlan,
    *,
    memory_budget_bytes: int,
    target_rows_per_part: int,
) -> PhysicalPlanCost:
    states = [task.state_estimate_bytes for task in plan.tasks]
    unknown = sum(value is None for value in states)
    known_states = [int(value) for value in states if value is not None]
    peak = max(known_states, default=None)
    feasible = unknown == 0 and (peak or 0) <= memory_budget_bytes
    small_files = sum(
        1
        for task in plan.tasks
        if task.expected_output_rows is not None
        and task.expected_output_rows < max(1, target_rows_per_part // 4)
    )
    estimated_io_bytes = sum(
        int(span.estimated_read_bytes if span.estimated_read_bytes is not None else span.size_bytes)
        for task in plan.tasks
        for span in task.source_spans
    )
    estimated_cpu_units = sum(
        max(0, int(task.expected_input_rows or 0)) + max(0, int(task.expected_output_rows or 0))
        for task in plan.tasks
    )
    estimated_writer_appends = sum(max(1, task.row_group_fanout) for task in plan.tasks)
    estimated_spawn_overhead_units = sum(
        max(0, int(task.expected_spawn_overhead_units or 0)) for task in plan.tasks
    )
    estimated_output_files = len(plan.tasks)
    estimated_partition_switches = sum(
        0 if task.single_partition_guaranteed else max(0, task.file_fanout - 1)
        for task in plan.tasks
    )
    # This is a ranking unit, not a wall-clock prediction. It keeps memory as
    # the primary gate and compares bounded candidates by their slowest task.
    estimated_elapsed_units = max(
        (
            max(0, int(task.expected_input_rows or 0))
            + max(0, int(task.expected_output_rows or 0))
            + max(1, task.row_group_fanout) * 128
            + (0 if task.single_partition_guaranteed else max(0, task.file_fanout - 1) * 256)
            + max(0, int(task.expected_spawn_overhead_units or 0))
            + sum(
                int(
                    span.estimated_read_bytes
                    if span.estimated_read_bytes is not None
                    else span.size_bytes
                )
                // 4096
                for span in task.source_spans
            )
            for task in plan.tasks
        ),
        default=0,
    )
    return PhysicalPlanCost(
        feasible=feasible,
        peak_state_bytes=peak,
        payload_bytes=sum(task.expected_payload_bytes for task in plan.tasks),
        file_fanout=max((task.file_fanout for task in plan.tasks), default=0),
        process_count=len(plan.tasks),
        small_file_penalty=small_files,
        conservative_penalty=unknown,
        estimated_io_bytes=estimated_io_bytes,
        estimated_cpu_units=estimated_cpu_units,
        estimated_elapsed_units=estimated_elapsed_units,
        estimated_spawn_overhead_units=estimated_spawn_overhead_units,
        estimated_writer_appends=estimated_writer_appends,
        estimated_output_files=estimated_output_files,
        estimated_partition_switches=estimated_partition_switches,
    )


def choose_physical_plan(
    candidates: list[PhysicalTaskPlan],
    *,
    memory_budget_bytes: int,
    target_rows_per_part: int,
) -> tuple[PhysicalTaskPlan, list[dict[str, Any]]]:
    if not candidates:
        raise ValueError("At least one physical plan candidate is required.")
    ranked = sorted(
        (
            (
                estimate_plan_cost(
                    candidate,
                    memory_budget_bytes=memory_budget_bytes,
                    target_rows_per_part=target_rows_per_part,
                ),
                candidate,
            )
            for candidate in candidates
        ),
        key=lambda item: item[0].score,
    )
    chosen_cost, chosen = ranked[0]
    trace = [
        {
            "physical_plan_hash": candidate.plan_hash,
            "chosen": candidate.plan_hash == chosen.plan_hash,
            "cost": asdict(cost),
            "score": cost.score,
            "rejected_reason": (
                None if candidate.plan_hash == chosen.plan_hash else "higher_cost_or_memory_risk"
            ),
        }
        for cost, candidate in ranked
    ]
    if not chosen_cost.feasible:
        trace[0]["selection_reason"] = "no_fully_bounded_candidate; conservative fallback"
    return chosen, trace


def _optional_float(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None
