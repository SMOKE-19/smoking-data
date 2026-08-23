from __future__ import annotations

from typing import Any

from smoking_data.core.tasks import TaskResult

TELEMETRY_RECONCILIATION_VERSION = "smoking-data.task-telemetry-reconciliation.v1"
DIAGNOSTIC_TOLERANCES = {
    "peak_rss_mb": {"absolute": 1.0, "relative": 0.05},
    "cpu_sec": {"absolute": 0.05, "relative": 0.25},
    "io_read_bytes": {"absolute": 64 * 1024, "relative": 0.10},
    "io_write_bytes": {"absolute": 64 * 1024, "relative": 0.10},
}


def reconcile_task_telemetry(
    task_results: list[TaskResult],
    telemetry: dict[str, Any] | None,
) -> dict[str, Any]:
    """Compare child self-counters with external supervisor counters.

    The report is diagnostic only. It deliberately does not reject a task or alter
    sizing because short-task CPU and I/O counters have platform-specific resolution.
    """

    if not isinstance(telemetry, dict) or telemetry.get("status") != "completed":
        return {
            "schema_version": TELEMETRY_RECONCILIATION_VERSION,
            "status": "not_available",
            "platform": (telemetry or {}).get("platform"),
            "reason": (telemetry or {}).get("status") or "telemetry_missing",
            "tasks_expected": 0,
            "tasks_matched": 0,
        }

    external_profiles = {
        str(item.get("task_id")): item
        for item in telemetry.get("task_profiles") or []
        if isinstance(item, dict) and item.get("task_id")
    }
    child_results = {item.task_id: item for item in task_results if item.pid > 0}
    task_comparisons: list[dict[str, Any]] = []
    for task_id, external in sorted(external_profiles.items()):
        child = child_results.get(task_id)
        if child is None:
            task_comparisons.append(
                {
                    "task_id": task_id,
                    "status": "child_result_missing",
                    "pid": external.get("pid"),
                    "metrics": {},
                }
            )
            continue
        external_read_key = (
            "requested_read_bytes"
            if external.get("requested_read_bytes") is not None
            else "read_bytes"
        )
        external_write_key = (
            "requested_write_bytes"
            if external.get("requested_write_bytes") is not None
            else "write_bytes"
        )
        metric_pairs = {
            "peak_rss_mb": (
                child.counters.get("rss_peak_mb"),
                external.get("max_peak_rss_mb"),
            ),
            "cpu_sec": (child.counters.get("cpu_sec"), external.get("cpu_sec")),
            "io_read_bytes": (
                child.counters.get("io_read_bytes"),
                external.get(external_read_key),
            ),
            "io_write_bytes": (
                child.counters.get("io_write_bytes"),
                external.get(external_write_key),
            ),
        }
        task_comparisons.append(
            {
                "task_id": task_id,
                "status": "matched",
                "pid": child.pid,
                "process_creation_time": external.get("process_creation_time"),
                "io_comparison_basis": {
                    "read": external_read_key,
                    "write": external_write_key,
                },
                "metrics": {
                    name: _compare_values(
                        child_value,
                        external_value,
                        absolute_tolerance=DIAGNOSTIC_TOLERANCES[name]["absolute"],
                        relative_tolerance=DIAGNOSTIC_TOLERANCES[name]["relative"],
                    )
                    for name, (child_value, external_value) in metric_pairs.items()
                },
            }
        )

    matched = [item for item in task_comparisons if item["status"] == "matched"]
    metric_summaries = {
        metric: _summarize_metric(matched, metric)
        for metric in ("peak_rss_mb", "cpu_sec", "io_read_bytes", "io_write_bytes")
    }
    return {
        "schema_version": TELEMETRY_RECONCILIATION_VERSION,
        "status": "complete" if len(matched) == len(external_profiles) else "partial",
        "mode": "report_only",
        "platform": telemetry.get("platform"),
        "tasks_expected": len(external_profiles),
        "tasks_matched": len(matched),
        "diagnostic_tolerances": DIAGNOSTIC_TOLERANCES,
        "metric_summaries": metric_summaries,
        "task_comparisons": task_comparisons,
        "notes": [
            "Linux I/O compares child rchar/wchar with supervisor requested byte counters.",
            "Windows I/O compares GetProcessIoCounters transfer byte counters on both sides.",
            "CPU deltas may differ for short tasks because OS counters have coarser resolution.",
            "This reconciliation cannot fail ETL or change physical sizing.",
        ],
    }


def _compare_values(
    child_value: Any,
    external_value: Any,
    *,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> dict[str, Any]:
    child = _number(child_value)
    external = _number(external_value)
    if child is None or external is None:
        return {
            "status": "not_comparable",
            "child": child,
            "supervisor": external,
            "signed_delta": None,
            "absolute_error": None,
            "relative_error": None,
            "within_diagnostic_tolerance": None,
        }
    signed_delta = external - child
    absolute_error = abs(signed_delta)
    relative_error = absolute_error / abs(child) if child else (0.0 if external == 0 else None)
    return {
        "status": "compared",
        "child": child,
        "supervisor": external,
        "signed_delta": signed_delta,
        "absolute_error": absolute_error,
        "relative_error": relative_error,
        "within_diagnostic_tolerance": (
            absolute_error <= absolute_tolerance
            or (relative_error is not None and relative_error <= relative_tolerance)
        ),
    }


def _summarize_metric(tasks: list[dict[str, Any]], metric: str) -> dict[str, Any]:
    comparisons = [
        item["metrics"][metric]
        for item in tasks
        if item["metrics"][metric]["status"] == "compared"
    ]
    absolute_errors = [float(item["absolute_error"]) for item in comparisons]
    relative_errors = [
        float(item["relative_error"])
        for item in comparisons
        if item["relative_error"] is not None
    ]
    within_tolerance = [
        item for item in comparisons if item["within_diagnostic_tolerance"] is True
    ]
    return {
        "count": len(comparisons),
        "within_diagnostic_tolerance": len(within_tolerance),
        "outside_diagnostic_tolerance": len(comparisons) - len(within_tolerance),
        "max_absolute_error": max(absolute_errors, default=None),
        "avg_absolute_error": (
            sum(absolute_errors) / len(absolute_errors) if absolute_errors else None
        ),
        "max_relative_error": max(relative_errors, default=None),
        "avg_relative_error": (
            sum(relative_errors) / len(relative_errors) if relative_errors else None
        ),
    }


def _number(value: Any) -> float | int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value
