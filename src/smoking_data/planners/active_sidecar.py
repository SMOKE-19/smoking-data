from __future__ import annotations

import math
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

MIB = 1024 * 1024
DECISION_SCHEMA_VERSION = "smoking-data.active-sidecar-decision.v1"
MAX_SAMPLE_FILES = 16
MAX_SAMPLE_ROWS = 65_536
DEFAULT_ON_TARGET_BYTES = 128 * MIB
HYSTERESIS_RATIO = 0.75
MIN_TARGET_BYTES = MIB
HARD_FILE_FANOUT = 256
HARD_ROW_GROUP_FANOUT = 1_024
BOUNDARY_OVERHEAD_RATIO = 1.15


def profile_selector_shape(
    candidate_paths: Sequence[Path],
    *,
    selection_group_keys: Sequence[str],
    candidate_rows: int,
) -> dict[str, Any]:
    """Estimate active snapshot size from a deterministic, bounded candidate sample."""
    started = time.perf_counter()
    paths = [Path(path) for path in candidate_paths]
    if not paths or candidate_rows <= 0:
        result = _fallback_shape(candidate_rows=max(0, int(candidate_rows)), row_groups=0)
        result["sampling_elapsed_sec"] = time.perf_counter() - started
        return result
    sample_paths = _evenly_spaced_paths(paths, limit=MAX_SAMPLE_FILES)
    rows_per_file = max(1, MAX_SAMPLE_ROWS // len(sample_paths))
    row_groups = 0
    for path in paths:
        row_groups += int(pq.ParquetFile(path).metadata.num_row_groups)
    try:
        sample = pl.concat(
            [pl.scan_parquet(path).head(rows_per_file) for path in sample_paths],
            how="diagonal_relaxed",
        ).collect(engine="streaming")
        if sample.is_empty():
            result = _fallback_shape(candidate_rows=candidate_rows, row_groups=row_groups)
            result["sampling_elapsed_sec"] = time.perf_counter() - started
            return result
        group_keys = list(dict.fromkeys(str(key) for key in selection_group_keys))
        groups = sample.group_by(group_keys).len()
        active_ratio = min(1.0, max(0.0, groups.height / sample.height))
        bytes_per_row = max(1.0, sample.estimated_size() / sample.height)
        estimated_active_rows = min(
            int(candidate_rows),
            max(1, math.ceil(int(candidate_rows) * active_ratio)),
        )
        estimated_active_bytes = math.ceil(
            estimated_active_rows * bytes_per_row * BOUNDARY_OVERHEAD_RATIO
        )
        return {
            "status": "sampled",
            "sample_files": len(sample_paths),
            "sample_rows": sample.height,
            "selector_key_count": len(group_keys),
            "sample_unique_groups": groups.height,
            "sample_active_ratio": active_ratio,
            "sample_max_rows_per_group": int(groups.get_column("len").max() or 0),
            "estimated_selector_row_bytes": bytes_per_row,
            "boundary_overhead_ratio": BOUNDARY_OVERHEAD_RATIO,
            "estimated_active_rows": estimated_active_rows,
            "estimated_active_bytes": estimated_active_bytes,
            "candidate_row_groups": row_groups,
            "sampling_elapsed_sec": time.perf_counter() - started,
        }
    except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
        fallback = _fallback_shape(candidate_rows=candidate_rows, row_groups=row_groups)
        fallback["status"] = "fallback"
        fallback["fallback_reason"] = type(exc).__name__
        fallback["sampling_elapsed_sec"] = time.perf_counter() - started
        return fallback


def build_active_sidecar_decision(
    *,
    candidate_files: int,
    candidate_bytes: int,
    candidate_rows: int,
    memory_budget_mb: int,
    selector_shape: Mapping[str, Any],
    previous_decision: Mapping[str, Any] | None = None,
    force_enabled: bool = False,
    force_disabled: bool = False,
) -> dict[str, Any]:
    """Choose direct, parent-bounded, or disposable active-sidecar execution."""
    budget_bytes = max(1, int(memory_budget_mb)) * MIB
    on_target_bytes = max(
        MIN_TARGET_BYTES,
        min(DEFAULT_ON_TARGET_BYTES, budget_bytes // 8),
    )
    off_target_bytes = max(MIN_TARGET_BYTES, math.floor(on_target_bytes * HYSTERESIS_RATIO))
    previous_mode = str((previous_decision or {}).get("selected_mode") or "")
    previous_plan = previous_mode == "active_sidecar_plan"
    effective_target_bytes = off_target_bytes if previous_plan else on_target_bytes
    estimated_active_bytes = max(0, int(selector_shape.get("estimated_active_bytes") or 0))
    candidate_row_groups = max(0, int(selector_shape.get("candidate_row_groups") or 0))
    direct_contract = (
        int(candidate_bytes) <= budget_bytes // 4
        and int(candidate_files) <= 16
        and estimated_active_bytes < effective_target_bytes
    )

    if force_disabled:
        selected_mode = "parent_bounded"
        reason = "internal_benchmark_force_disabled"
    elif force_enabled:
        selected_mode = "active_sidecar_plan"
        reason = "internal_benchmark_force_enabled"
    elif direct_contract:
        selected_mode = "direct"
        reason = "direct_contract_within_target"
    elif estimated_active_bytes >= effective_target_bytes:
        selected_mode = "active_sidecar_plan"
        reason = (
            "previous_plan_hysteresis_retained"
            if previous_plan and estimated_active_bytes < on_target_bytes
            else "estimated_active_snapshot_over_target"
        )
    elif int(candidate_files) >= HARD_FILE_FANOUT:
        selected_mode = "active_sidecar_plan"
        reason = "candidate_file_fanout_over_limit"
    elif candidate_row_groups >= HARD_ROW_GROUP_FANOUT:
        selected_mode = "active_sidecar_plan"
        reason = "candidate_row_group_fanout_over_limit"
    else:
        selected_mode = "parent_bounded"
        reason = "estimated_active_snapshot_below_target"

    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "selected_mode": selected_mode,
        "active_sidecar_plan_enabled": selected_mode == "active_sidecar_plan",
        "reason": reason,
        "candidate_files": int(candidate_files),
        "candidate_bytes": int(candidate_bytes),
        "candidate_rows": int(candidate_rows),
        "candidate_row_groups": candidate_row_groups,
        "memory_budget_mb": int(memory_budget_mb),
        "on_target_bytes": on_target_bytes,
        "off_target_bytes": off_target_bytes,
        "effective_target_bytes": effective_target_bytes,
        "previous_mode": previous_mode or None,
        "hysteresis_applied": previous_plan,
        "direct_contract": direct_contract,
        "selector_shape": dict(selector_shape),
        "override": (
            "force_disabled" if force_disabled else "force_enabled" if force_enabled else None
        ),
    }


def _evenly_spaced_paths(paths: Sequence[Path], *, limit: int) -> list[Path]:
    if len(paths) <= limit:
        return list(paths)
    indices = sorted({round(index * (len(paths) - 1) / (limit - 1)) for index in range(limit)})
    return [paths[index] for index in indices]


def _fallback_shape(*, candidate_rows: int, row_groups: int) -> dict[str, Any]:
    estimated_active_rows = max(0, int(candidate_rows))
    estimated_active_bytes = math.ceil(estimated_active_rows * 256 * BOUNDARY_OVERHEAD_RATIO)
    return {
        "status": "conservative_fallback",
        "sample_files": 0,
        "sample_rows": 0,
        "selector_key_count": None,
        "sample_unique_groups": None,
        "sample_active_ratio": 1.0 if candidate_rows else 0.0,
        "sample_max_rows_per_group": None,
        "estimated_selector_row_bytes": 256.0,
        "boundary_overhead_ratio": BOUNDARY_OVERHEAD_RATIO,
        "estimated_active_rows": estimated_active_rows,
        "estimated_active_bytes": estimated_active_bytes,
        "candidate_row_groups": int(row_groups),
    }
