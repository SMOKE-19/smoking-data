from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

PIVOT_SIZING_RECONCILIATION_VERSION = "smoking-data.pivot-sizing-reconciliation.v1"
ADAPTIVE_SIZING_DECISION_VERSION = "smoking-data.adaptive-sizing-decision.v1"
MATERIALIZE_CALIBRATION_PLAN_VERSION = "smoking-data.materialize-calibration-plan.v1"
CALIBRATED_WORKER_ADMISSION_VERSION = "smoking-data.calibrated-worker-admission.v1"
ADAPTIVE_TASK_BOUNDARY_DECISION_VERSION = (
    "smoking-data.adaptive-task-boundary-decision.v1"
)
CALIBRATED_COMPLETE_GROUP_GUARD_VERSION = (
    "smoking-data.calibrated-complete-group-guard.v1"
)
DEFAULT_TARGET_ROW_GROUP_BYTES = 64 * 1024 * 1024
DEFAULT_TARGET_FILE_BYTES = 256 * 1024 * 1024
DEFAULT_COMPRESSION_RATIO = 0.35
MIB = 1024 * 1024


def build_pivot_sizing_reconciliation(
    shape_profile: Mapping[str, Any],
    output_files: Sequence[str | Path],
    task_results: Sequence[Any],
) -> dict[str, Any]:
    """Compare report-only pivot estimates with committed Parquet and task counters."""

    if not bool(shape_profile.get("enabled", False)):
        return {
            "schema_version": PIVOT_SIZING_RECONCILIATION_VERSION,
            "status": "not_applicable",
            "reason": "pivot_disabled",
        }

    parquet_paths = [
        Path(path)
        for path in output_files
        if Path(path).is_file() and Path(path).suffix.lower() == ".parquet"
    ]
    if not parquet_paths:
        return {
            "schema_version": PIVOT_SIZING_RECONCILIATION_VERSION,
            "status": "unavailable",
            "reason": "no_committed_parquet_files",
        }

    output_rows = 0
    physical_file_bytes = 0
    compressed_column_bytes = 0
    uncompressed_column_bytes = 0
    row_group_rows: list[int] = []
    column_names: list[str] = []
    seen_columns: set[str] = set()
    try:
        for path in parquet_paths:
            parquet_file = pq.ParquetFile(path)
            metadata = parquet_file.metadata
            output_rows += metadata.num_rows
            physical_file_bytes += path.stat().st_size
            for name in parquet_file.schema_arrow.names:
                if name not in seen_columns:
                    seen_columns.add(name)
                    column_names.append(name)
            for row_group_index in range(metadata.num_row_groups):
                row_group = metadata.row_group(row_group_index)
                row_group_rows.append(row_group.num_rows)
                for column_index in range(row_group.num_columns):
                    column = row_group.column(column_index)
                    compressed_column_bytes += int(column.total_compressed_size)
                    uncompressed_column_bytes += int(column.total_uncompressed_size)
    except Exception as exc:
        return {
            "schema_version": PIVOT_SIZING_RECONCILIATION_VERSION,
            "status": "report_failed",
            "reason": f"{type(exc).__name__}: {exc}",
        }

    actual_wide_row_bytes = (
        uncompressed_column_bytes / output_rows if output_rows > 0 else None
    )
    compression_ratio = (
        compressed_column_bytes / uncompressed_column_bytes
        if uncompressed_column_bytes > 0
        else None
    )
    actual = {
        "output_files": len(parquet_paths),
        "output_rows": output_rows,
        "output_columns": len(column_names),
        "output_column_names": column_names,
        "physical_file_bytes": physical_file_bytes,
        "compressed_column_bytes": compressed_column_bytes,
        "uncompressed_column_bytes": uncompressed_column_bytes,
        "uncompressed_wide_row_bytes": _rounded(actual_wide_row_bytes),
        "compression_ratio": _rounded(compression_ratio),
        "row_group_rows": _distribution(row_group_rows),
        "file_sizes": _file_size_profile(
            parquet_paths,
            target_file_bytes=DEFAULT_TARGET_FILE_BYTES,
        ),
    }
    estimate_comparisons = {
        "output_rows": _comparison(shape_profile.get("estimated_output_rows"), output_rows),
        "output_columns": _comparison(
            shape_profile.get("estimated_output_columns"), len(column_names)
        ),
        "wide_row_bytes": _comparison(
            shape_profile.get("estimated_wide_row_bytes"), actual_wide_row_bytes
        ),
        "output_uncompressed_bytes": _comparison(
            shape_profile.get("estimated_output_uncompressed_bytes"),
            uncompressed_column_bytes,
        ),
    }

    return {
        "schema_version": PIVOT_SIZING_RECONCILIATION_VERSION,
        "status": "complete",
        "actual": actual,
        "estimate_comparisons": estimate_comparisons,
        "task_peak_memory_model": build_task_peak_memory_model(task_results),
        "limitations": [
            "Parquet column metadata excludes process, allocator, and writer working memory.",
            "The fitted task model is same-run diagnostic evidence, not an admission limit.",
            "estimated_pivot_state_bytes must not be treated as total task peak memory.",
        ],
    }


def build_adaptive_sizing_shadow_decision(
    shape_profile: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    *,
    rows_per_part: int,
    output_row_group_rows_override: int | None,
    memory_budget_mb: int,
    admitted_workers: int,
    history_compression_ratio: float | None = None,
    target_row_group_bytes: int = DEFAULT_TARGET_ROW_GROUP_BYTES,
    target_file_bytes: int = DEFAULT_TARGET_FILE_BYTES,
) -> dict[str, Any]:
    """Recommend next-run writer sizing without mutating the current physical plan."""

    if not bool(shape_profile.get("enabled", False)):
        return {
            "schema_version": ADAPTIVE_SIZING_DECISION_VERSION,
            "status": "not_applicable",
            "reason": "pivot_disabled",
            "mode": "shadow_only",
            "applied": False,
        }

    actual = dict(reconciliation.get("actual") or {})
    actual_width = _positive_float(actual.get("uncompressed_wide_row_bytes"))
    estimated_width = _positive_float(shape_profile.get("estimated_wide_row_bytes"))
    row_width = actual_width or estimated_width
    width_source = "same_run_observed" if actual_width is not None else "shape_estimate"
    historical_compression = _positive_float(history_compression_ratio)
    actual_compression = _positive_float(actual.get("compression_ratio"))
    compression_ratio = (
        historical_compression or actual_compression or DEFAULT_COMPRESSION_RATIO
    )
    compression_source = (
        "matching_history"
        if historical_compression is not None
        else "same_run_observed"
        if actual_compression is not None
        else "dtype_default"
    )
    if row_width is None:
        return {
            "schema_version": ADAPTIVE_SIZING_DECISION_VERSION,
            "status": "unavailable",
            "reason": "wide_row_width_unavailable",
            "mode": "shadow_only",
            "applied": False,
        }

    desired_row_group_rows = _clamp(
        math.floor(target_row_group_bytes / row_width), minimum=1, maximum=1_000_000
    )
    desired_rows_per_file = _clamp(
        math.floor(target_file_bytes / (row_width * compression_ratio)),
        minimum=desired_row_group_rows,
        maximum=10_000_000,
    )
    selected_input_rows = int(shape_profile.get("selected_input_rows") or 0)
    estimated_output_rows = int(shape_profile.get("estimated_output_rows") or 0)
    estimated_output_rows_per_part = (
        max(1, math.ceil(rows_per_part * estimated_output_rows / selected_input_rows))
        if selected_input_rows > 0 and estimated_output_rows > 0
        else None
    )
    bounded_row_group_rows = desired_row_group_rows
    if estimated_output_rows_per_part is not None:
        bounded_row_group_rows = max(
            1, min(desired_row_group_rows, estimated_output_rows_per_part)
        )
    manual_override_present = output_row_group_rows_override is not None

    return {
        "schema_version": ADAPTIVE_SIZING_DECISION_VERSION,
        "status": "advisory",
        "mode": "shadow_only",
        "applied": False,
        "model_source": width_source,
        "application_blocked_by": (
            "manual_override" if manual_override_present else "shadow_only_mode"
        ),
        "manual_override_present": manual_override_present,
        "current_output_row_group_rows_override": output_row_group_rows_override,
        "current_input_rows_per_part": int(rows_per_part),
        "current_plan_estimated_output_rows_per_part": estimated_output_rows_per_part,
        "writer_model": {
            "wide_row_bytes": _rounded(row_width),
            "wide_row_bytes_source": width_source,
            "compression_ratio": _rounded(compression_ratio),
            "compression_ratio_source": compression_source,
            "target_row_group_uncompressed_bytes": int(target_row_group_bytes),
            "target_file_compressed_bytes": int(target_file_bytes),
        },
        "recommendation": {
            "desired_output_row_group_rows": desired_row_group_rows,
            "bounded_output_row_group_rows": bounded_row_group_rows,
            "desired_output_rows_per_file": desired_rows_per_file,
            "file_boundary_status": "next_changed_run_task_boundary",
            "current_file_size_validation": actual.get("file_sizes"),
        },
        "resource_context": {
            "memory_budget_mb": int(memory_budget_mb),
            "admitted_workers": int(admitted_workers),
            "task_peak_memory_model": reconciliation.get("task_peak_memory_model"),
        },
        "next_step": (
            "Validate the recommendation on a future run before enabling plan mutation."
        ),
    }


def build_materialize_calibration_plan(
    physical_plan: Mapping[str, Any],
    shape_profile: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    *,
    memory_budget_mb: int,
    admitted_workers: int,
    calibration_target_count: int | None = None,
) -> dict[str, Any]:
    """Select next-run calibration tasks and report a bounded memory envelope."""

    if not bool(shape_profile.get("enabled", False)):
        return {
            "schema_version": MATERIALIZE_CALIBRATION_PLAN_VERSION,
            "status": "not_applicable",
            "reason": "pivot_disabled",
            "mode": "shadow_only",
            "applied": False,
        }
    tasks = [dict(item) for item in physical_plan.get("tasks") or [] if isinstance(item, dict)]
    if not tasks:
        return {
            "schema_version": MATERIALIZE_CALIBRATION_PLAN_VERSION,
            "status": "unavailable",
            "reason": "physical_tasks_unavailable",
            "mode": "shadow_only",
            "applied": False,
        }

    total_budget_bytes = max(1, int(memory_budget_mb)) * MIB
    workers = max(1, int(admitted_workers))
    parent_reserve_bytes = min(
        total_budget_bytes // 2,
        max(256 * MIB, math.ceil(total_budget_bytes * 0.10)),
    )
    worker_pool_bytes = max(1, total_budget_bytes - parent_reserve_bytes)
    worker_slot_bytes = max(1, worker_pool_bytes // workers)
    pivot_state_budget_bytes = math.floor(worker_slot_bytes * 0.65)
    calibrated_peak_limit_bytes = math.floor(worker_slot_bytes * 0.80)

    memory_model = dict(reconciliation.get("task_peak_memory_model") or {})
    observations = [
        dict(item) for item in memory_model.get("observations") or [] if isinstance(item, dict)
    ]
    fit = dict(memory_model.get("fit") or {})
    fitted = fit.get("status") == "fitted"
    slope = _positive_float(fit.get("peak_growth_bytes_per_selected_row")) if fitted else None
    fixed_growth = _positive_or_zero_float(fit.get("fixed_peak_growth_bytes")) if fitted else None
    baseline_rss_bytes = max(
        (
            float(item["rss_start_mb"]) * MIB
            for item in observations
            if _positive_float(item.get("rss_start_mb")) is not None
        ),
        default=0.0,
    )
    model_source = "same_run_observed_fit" if slope is not None else "physical_estimate"

    candidates: list[dict[str, Any]] = []
    for task in tasks:
        selected_rows = _optional_int(task.get("expected_input_rows")) or 0
        predicted_peak = None
        if slope is not None and fixed_growth is not None and baseline_rss_bytes > 0:
            predicted_peak = int(
                math.ceil(baseline_rss_bytes + fixed_growth + slope * selected_rows)
            )
        state_estimate = _optional_int(task.get("state_estimate_bytes")) or 0
        candidates.append(
            {
                "task_id": str(task.get("task_id") or ""),
                "expected_input_rows": selected_rows,
                "state_estimate_bytes": state_estimate,
                "file_fanout": _optional_int(task.get("file_fanout")) or 0,
                "row_group_fanout": _optional_int(task.get("row_group_fanout")) or 0,
                "predicted_total_peak_bytes": predicted_peak,
                "predicted_worker_slot_ratio": (
                    _rounded(predicted_peak / worker_slot_bytes)
                    if predicted_peak is not None
                    else None
                ),
            }
        )

    default_target_count = min(
        len(candidates), min(max(3, math.ceil(math.sqrt(len(candidates)))), 8)
    )
    target_count = (
        min(len(candidates), max(1, int(calibration_target_count)))
        if calibration_target_count is not None
        else default_target_count
    )
    selected = _select_calibration_candidates(candidates, target_count=target_count)
    model_capacity_rows = None
    recommended_max_rows = None
    observed_max_rows = max(
        (
            int(item["selected_input_rows"])
            for item in observations
            if _positive_float(item.get("selected_input_rows")) is not None
        ),
        default=None,
    )
    if slope is not None and fixed_growth is not None and baseline_rss_bytes > 0:
        available_variable_bytes = (
            calibrated_peak_limit_bytes - baseline_rss_bytes - fixed_growth
        )
        model_capacity_rows = max(1, math.floor(available_variable_bytes / slope))
        if observed_max_rows is not None:
            recommended_max_rows = min(model_capacity_rows, observed_max_rows * 2)

    max_predicted_peak = max(
        (
            int(item["predicted_total_peak_bytes"])
            for item in candidates
            if item["predicted_total_peak_bytes"] is not None
        ),
        default=None,
    )
    return {
        "schema_version": MATERIALIZE_CALIBRATION_PLAN_VERSION,
        "status": "advisory",
        "mode": "shadow_only",
        "applied": False,
        "planning_scope": "next_run",
        "complete_group_contract": {
            "status": "preserved_by_current_coordinate_parts",
            "split_allowed": False,
        },
        "memory_envelope": {
            "total_memory_budget_bytes": total_budget_bytes,
            "parent_reserve_bytes": parent_reserve_bytes,
            "worker_pool_bytes": worker_pool_bytes,
            "admitted_workers": workers,
            "worker_slot_bytes": worker_slot_bytes,
            "pivot_state_budget_ratio": 0.65,
            "pivot_state_budget_bytes": pivot_state_budget_bytes,
            "calibrated_total_peak_limit_ratio": 0.80,
            "calibrated_total_peak_limit_bytes": calibrated_peak_limit_bytes,
        },
        "memory_model": {
            "source": model_source,
            "baseline_rss_bytes": int(math.ceil(baseline_rss_bytes)) or None,
            "fixed_peak_growth_bytes": (
                int(round(fixed_growth)) if fixed_growth is not None else None
            ),
            "peak_growth_bytes_per_selected_row": _rounded(slope),
            "r_squared": fit.get("r_squared") if fitted else None,
            "usage": "next_run_advisory_only",
            "estimated_pivot_state_is_total_peak": False,
        },
        "current_plan": {
            "tasks": len(candidates),
            "max_expected_input_rows": max(
                (item["expected_input_rows"] for item in candidates), default=0
            ),
            "max_predicted_total_peak_bytes": max_predicted_peak,
            "max_predicted_worker_slot_ratio": (
                _rounded(max_predicted_peak / worker_slot_bytes)
                if max_predicted_peak is not None
                else None
            ),
        },
        "recommendation": {
            "recommended_max_selected_rows_per_task": recommended_max_rows,
            "model_capacity_selected_rows_per_task": model_capacity_rows,
            "observed_max_selected_rows_per_task": observed_max_rows,
            "extrapolation_cap_multiplier": 2.0,
            "extrapolation_cap_applied": (
                recommended_max_rows is not None
                and model_capacity_rows is not None
                and recommended_max_rows < model_capacity_rows
            ),
            "plan_mutation_status": "deferred",
            "replan_limit": 1,
            "replan_scope": "not_started_tasks_only",
        },
        "calibration": {
            "selection_rule": "risk_strata_then_axes_then_deterministic_fill.v2",
            "target_count": target_count,
            "target_count_source": (
                "learned_registry" if calibration_target_count is not None else "first_run_default"
            ),
            "selected_tasks": selected,
            "output_reuse_policy": "required_before_activation",
            "outputs_reused_this_run": False,
        },
        "limitations": [
            "The same-run fit contains only observed input-row sizes and is not history yet.",
            "First-run row recommendations do not exceed twice the largest observed task.",
            "Pivot state and total process peak are separate budgets.",
            "No task split, worker change, or output rewrite is applied in shadow mode.",
        ],
    }


def build_task_peak_memory_model(task_results: Sequence[Any]) -> dict[str, Any]:
    """Build the task peak model used by calibration and final reconciliation."""

    return _task_peak_memory_model(task_results)


def build_adaptive_task_boundary_decision(
    previous_metadata: Mapping[str, Any] | None,
    *,
    logical_plan_hash: str,
    configured_rows_per_part: int,
    pivot_enabled: bool,
    minimum_r_squared: float = 0.80,
) -> dict[str, Any]:
    """Resolve a bounded next-run coordinate boundary from completed history."""

    configured = max(1, int(configured_rows_per_part))
    base = {
        "schema_version": ADAPTIVE_TASK_BOUNDARY_DECISION_VERSION,
        "configured_rows_per_part": configured,
        "effective_rows_per_part": configured,
        "applied": False,
        "task_boundary_changed": False,
        "movement_limit": {"minimum_multiplier": 0.5, "maximum_multiplier": 2.0},
    }
    if not pivot_enabled:
        return {**base, "status": "not_applicable", "reason": "pivot_disabled"}

    root = dict(previous_metadata or {})
    result = root.get("result") if isinstance(root.get("result"), Mapping) else root
    details = (
        result.get("details")
        if isinstance(result, Mapping) and isinstance(result.get("details"), Mapping)
        else result
    )
    if not isinstance(details, Mapping):
        return {**base, "status": "history_unavailable", "reason": "metadata_missing"}

    previous_hash = str(details.get("logical_plan_hash") or "")
    if not previous_hash:
        return {
            **base,
            "status": "history_unavailable",
            "reason": "logical_plan_hash_missing",
        }
    if previous_hash != logical_plan_hash:
        return {
            **base,
            "status": "history_rejected",
            "reason": "logical_plan_hash_mismatch",
            "history_logical_plan_hash": previous_hash,
        }

    plan = details.get("materialize_calibration_plan")
    if not isinstance(plan, Mapping) or plan.get("applied") is not True:
        return {
            **base,
            "status": "history_unavailable",
            "reason": "applied_calibration_plan_missing",
            "history_logical_plan_hash": previous_hash,
        }
    recommendation = plan.get("recommendation")
    memory_model = plan.get("memory_model")
    recommendation = recommendation if isinstance(recommendation, Mapping) else {}
    memory_model = memory_model if isinstance(memory_model, Mapping) else {}
    recommended = _optional_int(
        recommendation.get("recommended_max_selected_rows_per_task")
    )
    observed_max = _optional_int(
        recommendation.get("observed_max_selected_rows_per_task")
    )
    r_squared = _positive_or_zero_float(memory_model.get("r_squared"))
    if recommended is None or recommended <= 0 or observed_max is None or observed_max <= 0:
        return {
            **base,
            "status": "history_unavailable",
            "reason": "task_row_recommendation_missing",
            "history_logical_plan_hash": previous_hash,
        }
    if r_squared is None or r_squared < minimum_r_squared:
        return {
            **base,
            "status": "history_rejected",
            "reason": "memory_model_fit_below_threshold",
            "history_logical_plan_hash": previous_hash,
            "model_r_squared": _rounded(r_squared),
            "minimum_r_squared": minimum_r_squared,
        }

    file_input_rows = None
    writer_decision = details.get("adaptive_sizing_decision")
    shape_profile = details.get("pivot_shape_profile")
    if isinstance(writer_decision, Mapping) and isinstance(shape_profile, Mapping):
        writer_recommendation = writer_decision.get("recommendation")
        writer_recommendation = (
            writer_recommendation
            if isinstance(writer_recommendation, Mapping)
            else {}
        )
        output_rows_per_file = _optional_int(
            writer_recommendation.get("desired_output_rows_per_file")
        )
        selected_input_rows = _optional_int(shape_profile.get("selected_input_rows"))
        estimated_output_rows = _optional_int(shape_profile.get("estimated_output_rows"))
        if (
            output_rows_per_file is not None
            and output_rows_per_file > 0
            and selected_input_rows is not None
            and selected_input_rows > 0
            and estimated_output_rows is not None
            and estimated_output_rows > 0
        ):
            file_input_rows = max(
                1,
                math.floor(
                    output_rows_per_file
                    * selected_input_rows
                    / estimated_output_rows
                ),
            )
    effective_recommendation = min(
        value for value in (recommended, file_input_rows) if value is not None
    )
    minimum = max(1, math.ceil(configured * 0.5))
    maximum = max(minimum, math.ceil(configured * 2.0))
    effective = _clamp(effective_recommendation, minimum=minimum, maximum=maximum)
    return {
        **base,
        "status": "applied_from_history",
        "reason": "validated_previous_calibration",
        "applied": True,
        "task_boundary_changed": effective != configured,
        "effective_rows_per_part": effective,
        "history_logical_plan_hash": previous_hash,
        "history_recommendation_rows": recommended,
        "history_file_target_input_rows": file_input_rows,
        "selected_recommendation_rows": effective_recommendation,
        "selection_rule": "minimum_of_memory_and_file_target",
        "history_observed_max_rows": observed_max,
        "model_r_squared": _rounded(r_squared),
        "minimum_r_squared": minimum_r_squared,
        "movement_bounds_rows": {"minimum": minimum, "maximum": maximum},
        "movement_clamped": effective != effective_recommendation,
    }


def matching_history_compression_ratio(
    previous_metadata: Mapping[str, Any] | None,
    *,
    logical_plan_hash: str,
) -> float | None:
    """Return compression history only for the same logical plan."""

    root = dict(previous_metadata or {})
    result = root.get("result") if isinstance(root.get("result"), Mapping) else root
    details = (
        result.get("details")
        if isinstance(result, Mapping) and isinstance(result.get("details"), Mapping)
        else result
    )
    if not isinstance(details, Mapping):
        return None
    if str(details.get("logical_plan_hash") or "") != logical_plan_hash:
        return None
    reconciliation = details.get("pivot_sizing_reconciliation")
    reconciliation = reconciliation if isinstance(reconciliation, Mapping) else {}
    actual = reconciliation.get("actual")
    actual = actual if isinstance(actual, Mapping) else {}
    return _positive_float(actual.get("compression_ratio"))


def build_calibrated_complete_group_guard(
    shape_profile: Mapping[str, Any],
    memory_model: Mapping[str, Any],
    *,
    memory_budget_mb: int,
    admitted_workers: int,
    minimum_r_squared: float = 0.80,
) -> dict[str, Any]:
    """Check the largest complete pivot group against calibrated total peak."""

    base = {
        "schema_version": CALIBRATED_COMPLETE_GROUP_GUARD_VERSION,
        "applied": False,
    }
    if not bool(shape_profile.get("enabled", False)):
        return {**base, "status": "not_applicable", "reason": "pivot_disabled"}

    distribution = shape_profile.get("rows_per_pivot_group")
    distribution = distribution if isinstance(distribution, Mapping) else {}
    complete_group_rows = _optional_int(distribution.get("max"))
    fit = memory_model.get("fit")
    fit = fit if isinstance(fit, Mapping) else {}
    observations = [
        item
        for item in memory_model.get("observations") or []
        if isinstance(item, Mapping)
    ]
    slope = (
        _positive_float(fit.get("peak_growth_bytes_per_selected_row"))
        if fit.get("status") == "fitted"
        else None
    )
    fixed_growth = (
        _positive_or_zero_float(fit.get("fixed_peak_growth_bytes"))
        if fit.get("status") == "fitted"
        else None
    )
    r_squared = _positive_or_zero_float(fit.get("r_squared"))
    baseline_rss_bytes = max(
        (
            float(item["rss_start_mb"]) * MIB
            for item in observations
            if _positive_float(item.get("rss_start_mb")) is not None
        ),
        default=0.0,
    )
    if complete_group_rows is None or complete_group_rows <= 0:
        return {**base, "status": "unavailable", "reason": "group_size_unavailable"}
    if (
        slope is None
        or fixed_growth is None
        or baseline_rss_bytes <= 0
        or r_squared is None
        or r_squared < minimum_r_squared
    ):
        return {
            **base,
            "status": "unavailable",
            "reason": "reliable_calibrated_memory_model_unavailable",
            "complete_group_rows": complete_group_rows,
            "model_r_squared": _rounded(r_squared),
            "minimum_r_squared": minimum_r_squared,
        }

    total_budget_bytes = max(1, int(memory_budget_mb)) * MIB
    workers = max(1, int(admitted_workers))
    parent_reserve_bytes = min(
        total_budget_bytes // 2,
        max(256 * MIB, math.ceil(total_budget_bytes * 0.10)),
    )
    worker_pool_bytes = max(1, total_budget_bytes - parent_reserve_bytes)
    worker_slot_bytes = max(1, worker_pool_bytes // workers)
    calibrated_peak_limit_bytes = math.floor(worker_slot_bytes * 0.80)
    predicted_total_peak_bytes = math.ceil(
        baseline_rss_bytes + fixed_growth + slope * complete_group_rows
    )
    over_budget = predicted_total_peak_bytes > calibrated_peak_limit_bytes
    return {
        **base,
        "status": "over_budget" if over_budget else "safe",
        "reason": (
            "predicted_complete_group_peak_exceeds_limit"
            if over_budget
            else "predicted_complete_group_peak_within_limit"
        ),
        "applied": True,
        "complete_group_rows": complete_group_rows,
        "group_size_source": "pivot_shape_profile.rows_per_pivot_group.max",
        "group_size_is_sample_estimate": float(
            shape_profile.get("sample_fraction") or 1.0
        ) < 1.0,
        "baseline_rss_bytes": int(math.ceil(baseline_rss_bytes)),
        "fixed_peak_growth_bytes": int(round(fixed_growth)),
        "peak_growth_bytes_per_selected_row": _rounded(slope),
        "model_r_squared": _rounded(r_squared),
        "minimum_r_squared": minimum_r_squared,
        "memory_envelope": {
            "total_memory_budget_bytes": total_budget_bytes,
            "parent_reserve_bytes": parent_reserve_bytes,
            "worker_pool_bytes": worker_pool_bytes,
            "admitted_workers": workers,
            "worker_slot_bytes": worker_slot_bytes,
            "calibrated_total_peak_limit_ratio": 0.80,
            "calibrated_total_peak_limit_bytes": calibrated_peak_limit_bytes,
        },
        "predicted_complete_group_total_peak_bytes": predicted_total_peak_bytes,
        "predicted_limit_ratio": _rounded(
            predicted_total_peak_bytes / calibrated_peak_limit_bytes
        ),
    }


def build_calibrated_worker_admission(
    task_results: Sequence[Any],
    *,
    memory_budget_mb: int,
    initial_admitted_workers: int,
    remaining_tasks: int,
) -> dict[str, Any]:
    """Allow calibration to reduce, but never increase, current-run concurrency."""

    initial = max(1, int(initial_admitted_workers))
    if remaining_tasks <= 0:
        return {
            "schema_version": CALIBRATED_WORKER_ADMISSION_VERSION,
            "status": "not_required",
            "initial_admitted_workers": initial,
            "admitted_workers": 0,
            "remaining_tasks": 0,
        }
    total_budget_bytes = max(1, int(memory_budget_mb)) * MIB
    parent_reserve_bytes = min(
        total_budget_bytes // 2,
        max(256 * MIB, math.ceil(total_budget_bytes * 0.10)),
    )
    worker_pool_bytes = max(1, total_budget_bytes - parent_reserve_bytes)
    observed_peaks = [
        float(counters["rss_peak_mb"]) * MIB
        for result in task_results
        for counters in [dict(getattr(result, "counters", {}) or {})]
        if _positive_float(counters.get("rss_peak_mb")) is not None
    ]
    if not observed_peaks:
        return {
            "schema_version": CALIBRATED_WORKER_ADMISSION_VERSION,
            "status": "estimate_fallback",
            "initial_admitted_workers": initial,
            "admitted_workers": min(initial, remaining_tasks),
            "remaining_tasks": remaining_tasks,
            "reason": "calibration_peak_unavailable",
        }
    max_observed_peak_bytes = math.ceil(max(observed_peaks))
    reserved_peak_per_worker_bytes = math.ceil(max_observed_peak_bytes / 0.80)
    memory_limited_workers = max(1, worker_pool_bytes // reserved_peak_per_worker_bytes)
    admitted = min(initial, remaining_tasks, memory_limited_workers)
    return {
        "schema_version": CALIBRATED_WORKER_ADMISSION_VERSION,
        "status": "calibrated",
        "initial_admitted_workers": initial,
        "admitted_workers": admitted,
        "remaining_tasks": remaining_tasks,
        "total_memory_budget_bytes": total_budget_bytes,
        "parent_reserve_bytes": parent_reserve_bytes,
        "worker_pool_bytes": worker_pool_bytes,
        "max_observed_task_peak_bytes": max_observed_peak_bytes,
        "reserved_peak_per_worker_bytes": reserved_peak_per_worker_bytes,
        "memory_limited_workers": memory_limited_workers,
        "concurrency_increase_allowed": False,
    }


def _select_calibration_candidates(
    tasks: list[dict[str, Any]], *, target_count: int
) -> list[dict[str, Any]]:
    ordered = sorted(tasks, key=lambda item: item["task_id"])
    selected: dict[str, dict[str, Any]] = {}

    def add(task: dict[str, Any], reason: str) -> None:
        task_id = str(task["task_id"])
        if task_id not in selected:
            selected[task_id] = {**task, "selection_reasons": []}
        reasons = selected[task_id]["selection_reasons"]
        if reason not in reasons:
            reasons.append(reason)

    def risk_value(task: dict[str, Any]) -> int:
        predicted = task.get("predicted_total_peak_bytes")
        return int(predicted if predicted is not None else task["state_estimate_bytes"])

    by_risk = sorted(ordered, key=lambda item: (risk_value(item), item["task_id"]))
    add(by_risk[-1], "largest_memory_risk")
    if target_count > 1 and risk_value(by_risk[0]) != risk_value(by_risk[-1]):
        add(by_risk[0], "smallest_memory_risk_stratum")
    if target_count > 2:
        add(by_risk[len(by_risk) // 2], "median_memory_risk")

    axes = [
        (lambda item: int(item["file_fanout"]), "largest_file_fanout"),
        (lambda item: int(item["row_group_fanout"]), "largest_row_group_fanout"),
        (lambda item: int(item["expected_input_rows"]), "largest_input_rows"),
    ]
    for key, reason in axes:
        winner = max(ordered, key=lambda item: (key(item), item["task_id"]))
        if winner["task_id"] in selected or len(selected) < target_count:
            add(winner, reason)

    for task in sorted(
        ordered,
        key=lambda item: hashlib.sha256(item["task_id"].encode("utf-8")).hexdigest(),
    ):
        if len(selected) >= target_count:
            break
        add(task, "deterministic_fill")
    return list(selected.values())[:target_count]


def _task_peak_memory_model(task_results: Sequence[Any]) -> dict[str, Any]:
    observations: list[dict[str, Any]] = []
    for result in task_results:
        counters = dict(getattr(result, "counters", {}) or {})
        selected_rows = _positive_float(
            counters.get("rust_input_rows", counters.get("input_rows"))
        )
        rss_start_mb = _positive_float(counters.get("rss_start_mb"))
        rss_peak_mb = _positive_float(counters.get("rss_peak_mb"))
        if selected_rows is None or rss_start_mb is None or rss_peak_mb is None:
            continue
        peak_growth_bytes = max(0.0, rss_peak_mb - rss_start_mb) * 1024 * 1024
        observations.append(
            {
                "task_id": str(getattr(result, "task_id", "")),
                "selected_input_rows": int(selected_rows),
                "output_rows": _optional_int(counters.get("output_rows")),
                "projected_input_array_bytes": _optional_int(
                    counters.get(
                        "rust_projected_input_array_bytes",
                        counters.get("projected_input_array_bytes"),
                    )
                ),
                "rss_start_mb": _rounded(rss_start_mb),
                "rss_peak_mb": _rounded(rss_peak_mb),
                "peak_growth_bytes": int(peak_growth_bytes),
            }
        )
    fit_points = [
        (float(item["selected_input_rows"]), float(item["peak_growth_bytes"]))
        for item in observations
    ]
    fit = _linear_fit(fit_points)
    return {
        "status": "complete" if observations else "unavailable",
        "observations": observations,
        "fit": fit,
        "usage": "diagnostic_only",
    }


def _linear_fit(points: list[tuple[float, float]]) -> dict[str, Any]:
    distinct_x = {x for x, _ in points}
    if len(points) < 2 or len(distinct_x) < 2:
        return {
            "status": "insufficient_distinct_input_rows",
            "sample_count": len(points),
        }
    x_mean = sum(x for x, _ in points) / len(points)
    y_mean = sum(y for _, y in points) / len(points)
    denominator = sum((x - x_mean) ** 2 for x, _ in points)
    slope = sum((x - x_mean) * (y - y_mean) for x, y in points) / denominator
    intercept = y_mean - slope * x_mean
    residual = sum((y - (intercept + slope * x)) ** 2 for x, y in points)
    total = sum((y - y_mean) ** 2 for _, y in points)
    r_squared = 1.0 - residual / total if total > 0 else 1.0
    return {
        "status": "fitted",
        "sample_count": len(points),
        "distinct_input_row_counts": len(distinct_x),
        "fixed_peak_growth_bytes": int(round(intercept)),
        "peak_growth_bytes_per_selected_row": _rounded(slope),
        "r_squared": _rounded(r_squared),
    }


def _comparison(estimated: Any, actual: Any) -> dict[str, Any]:
    estimated_value = _positive_or_zero_float(estimated)
    actual_value = _positive_or_zero_float(actual)
    if estimated_value is None or actual_value is None:
        return {"status": "unavailable"}
    error = estimated_value - actual_value
    return {
        "status": "complete",
        "estimated": _rounded(estimated_value),
        "actual": _rounded(actual_value),
        "error": _rounded(error),
        "relative_error": _rounded(error / actual_value) if actual_value > 0 else None,
    }


def _distribution(values: list[int]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min": None, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": _nearest_quantile(ordered, 0.50),
        "p95": _nearest_quantile(ordered, 0.95),
        "max": ordered[-1],
    }


def _file_size_profile(
    paths: Sequence[Path],
    *,
    target_file_bytes: int,
) -> dict[str, Any]:
    by_parent: dict[Path, list[Path]] = {}
    for path in paths:
        by_parent.setdefault(path.parent, []).append(path)
    tail_paths = {
        max(partition_paths, key=lambda item: item.name)
        for partition_paths in by_parent.values()
        if partition_paths
    }
    all_sizes = [int(path.stat().st_size) for path in paths]
    non_tail_sizes = [
        int(path.stat().st_size) for path in paths if path not in tail_paths
    ]
    minimum = math.floor(target_file_bytes * 0.5)
    maximum = math.ceil(target_file_bytes * 1.5)
    within = sum(minimum <= value <= maximum for value in non_tail_sizes)
    return {
        "target_compressed_bytes": int(target_file_bytes),
        "accepted_range_bytes": {"minimum": minimum, "maximum": maximum},
        "all": _distribution(all_sizes),
        "non_tail": _distribution(non_tail_sizes),
        "validation": {
            "status": "complete" if non_tail_sizes else "insufficient_non_tail_files",
            "files_checked": len(non_tail_sizes),
            "files_within_target_range": within,
            "within_target_ratio": (
                _rounded(within / len(non_tail_sizes)) if non_tail_sizes else None
            ),
        },
    }


def _nearest_quantile(values: list[int], quantile: float) -> int:
    return values[min(len(values) - 1, max(0, round((len(values) - 1) * quantile)))]


def _clamp(value: int, *, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, value))


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _positive_or_zero_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _rounded(value: float | None) -> float | None:
    return round(value, 6) if value is not None and math.isfinite(value) else None
