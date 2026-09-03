from __future__ import annotations

import math
from typing import Any

from smoking_data.runtime.config import PhaseMemoryPolicy


def build_phase_memory_admission(
    *,
    phase: str,
    hard_limit_mb: int,
    safety_ratio: float,
    policy: PhaseMemoryPolicy,
    requested_workers: int,
    historical_worker_peak_p95_mb: float | None,
    fallback_worker_peak_mb: float,
) -> dict[str, Any]:
    hard_limit_mb = int(hard_limit_mb)
    safe_envelope_mb = max(1.0, hard_limit_mb * float(safety_ratio))
    parent_reserve_mb = min(512.0, max(128.0, safe_envelope_mb * 0.10))
    worker_pool_mb = max(1.0, safe_envelope_mb - parent_reserve_mb)
    worker_peak_mb = max(
        1.0,
        float(historical_worker_peak_p95_mb or fallback_worker_peak_mb or 1.0),
    )
    capacity = max(1, math.floor(worker_pool_mb / worker_peak_mb))
    requested = min(max(1, int(requested_workers)), int(policy.max_workers))
    admitted = max(int(policy.min_workers), min(requested, capacity))
    admitted = min(admitted, requested)
    admitted_peak_mb = worker_peak_mb * admitted
    if admitted_peak_mb >= hard_limit_mb * 0.95:
        pressure = "hard_limit_near"
    elif admitted_peak_mb > safe_envelope_mb * 0.80:
        pressure = "safe_envelope_near"
    else:
        pressure = "within_envelope"
    return {
        "schema_version": "smoking-data.phase-memory-admission.v2",
        "phase": phase,
        "hard_limit_mb": hard_limit_mb,
        "safety_ratio": float(safety_ratio),
        "safe_envelope_mb": safe_envelope_mb,
        "parent_reserve_mb": parent_reserve_mb,
        "worker_pool_mb": worker_pool_mb,
        "worker_peak_mb": worker_peak_mb,
        "worker_peak_source": (
            "historical_p95" if historical_worker_peak_p95_mb is not None else "fallback_estimate"
        ),
        "requested_workers": requested_workers,
        "bounded_requested_workers": requested,
        "admitted_workers": admitted,
        "pressure": pressure,
    }
