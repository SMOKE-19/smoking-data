from __future__ import annotations

import json
import os
import platform
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from smoking_data.runtime.operation_registry import registry_path
from smoking_data.runtime.paths import ensure_dir

ADAPTIVE_SIZING_KEY_VERSION = "smoking-data.adaptive-sizing-key.v1"
ADAPTIVE_SIZING_REGISTRY_VERSION = "smoking-data.adaptive-sizing-registry.v1"


def build_adaptive_sizing_key(
    files: Sequence[Path],
    *,
    canonical_op_hash: str,
    pivot: Mapping[str, Any] | None,
    compression: str,
    engine: Mapping[str, Any],
) -> dict[str, Any]:
    canonical = {
        "schema_version": ADAPTIVE_SIZING_KEY_VERSION,
        "canonical_op_hash": canonical_op_hash,
        "source_schema_hash": _source_schema_hash(files),
        "pivot_contract": _canonical_pivot_contract(pivot),
        "engine": {
            str(key): value for key, value in sorted(engine.items()) if key != "expression_ir_hash"
        },
        "compression": str(compression).lower(),
        "machine_profile": _machine_profile(),
    }
    canonical_json = _json(canonical)
    return {
        "schema_version": ADAPTIVE_SIZING_KEY_VERSION,
        "model_key": sha256(canonical_json.encode()).hexdigest(),
        "canonical_json": canonical_json,
        "canonical": canonical,
    }


def load_adaptive_sizing_model(
    *,
    project_root: Path,
    model_key: str,
) -> dict[str, Any] | None:
    connection = _connect(project_root)
    try:
        row = connection.execute(
            """
            SELECT model_json, observation_count, last_observed_at, last_alias, last_job_name
            FROM adaptive_sizing_models
            WHERE model_key = ?
            """,
            (model_key,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return None
    model = json.loads(str(row["model_json"]))
    if not isinstance(model, dict):
        return None
    return {
        "schema_version": ADAPTIVE_SIZING_REGISTRY_VERSION,
        "model_key": model_key,
        "observation_count": int(row["observation_count"]),
        "last_observed_at": row["last_observed_at"],
        "last_alias": row["last_alias"],
        "last_job_name": row["last_job_name"],
        "model": model,
    }


def record_adaptive_sizing_model(
    *,
    project_root: Path,
    key: Mapping[str, Any],
    alias: str,
    job_name: str,
    details: Mapping[str, Any],
    counters: Mapping[str, Any],
) -> dict[str, Any]:
    model_key = str(key["model_key"])
    canonical_json = str(key["canonical_json"])
    observed_at = datetime.now(timezone.utc).isoformat()
    model = {
        "result": {
            "details": {
                name: details[name]
                for name in (
                    "logical_plan_hash",
                    "pivot_shape_profile",
                    "pivot_sizing_reconciliation",
                    "adaptive_sizing_decision",
                    "materialize_calibration_plan",
                    "adaptive_materialize_execution",
                    "phase_telemetry",
                )
                if name in details
            }
        }
    }
    candidate_sidecar = _mapping(details.get("candidate_sidecar"))
    active_sidecar_decision = _mapping(candidate_sidecar.get("active_sidecar_decision"))
    if active_sidecar_decision:
        model["result"]["details"]["active_sidecar_decision"] = active_sidecar_decision
    task_memory = _mapping(details.get("task_memory"))
    reconciliation = _mapping(details.get("pivot_sizing_reconciliation"))
    actual = _mapping(reconciliation.get("actual"))
    phase_profile = _mapping(details.get("phase_profile"))
    elapsed_sec = _positive_float(phase_profile.get("total_elapsed_sec"))
    output_rows = int(counters.get("output_rows") or 0)
    metrics = {
        "tasks": int(counters.get("tasks") or 0),
        "dirty_tasks": int(counters.get("dirty_tasks") or 0),
        "output_rows": output_rows,
        "actual_peak_rss_mb": task_memory.get("max_peak_rss_mb"),
        "compression_ratio": actual.get("compression_ratio"),
        "throughput_output_rows_per_sec": (
            round(output_rows / elapsed_sec, 6) if elapsed_sec is not None else None
        ),
        "estimate_errors": reconciliation.get("estimate_comparisons"),
        "task_memory": task_memory,
        "pivot_sizing_reconciliation": reconciliation,
        "phase_telemetry": _mapping(details.get("phase_telemetry")),
        "active_sidecar_decision": active_sidecar_decision,
    }
    connection = _connect(project_root)
    try:
        with connection:
            connection.execute(
                """
                INSERT INTO adaptive_sizing_models (
                    model_key, canonical_json, model_json, first_observed_at,
                    last_observed_at, last_alias, last_job_name, observation_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(model_key) DO UPDATE SET
                    canonical_json = excluded.canonical_json,
                    model_json = excluded.model_json,
                    last_observed_at = excluded.last_observed_at,
                    last_alias = excluded.last_alias,
                    last_job_name = excluded.last_job_name,
                    observation_count = adaptive_sizing_models.observation_count + 1
                """,
                (
                    model_key,
                    canonical_json,
                    _json(model),
                    observed_at,
                    observed_at,
                    alias,
                    job_name,
                ),
            )
            connection.execute(
                """
                INSERT INTO adaptive_sizing_observations (
                    model_key, observed_at, alias, job_name, metrics_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (model_key, observed_at, alias, job_name, _json(metrics)),
            )
            count = connection.execute(
                "SELECT observation_count FROM adaptive_sizing_models WHERE model_key = ?",
                (model_key,),
            ).fetchone()[0]
    finally:
        connection.close()
    return {
        "schema_version": ADAPTIVE_SIZING_REGISTRY_VERSION,
        "status": "recorded",
        "path": str(registry_path(project_root)),
        "model_key": model_key,
        "observation_count": int(count),
        "last_alias": alias,
        "last_job_name": job_name,
        "observed_at": observed_at,
        "execution_count_contract": (
            "Model observations are separate from manual/scheduled execution counts."
        ),
    }


def phase_memory_history(
    model: Mapping[str, Any] | None,
    *,
    phase_name: str,
    admission_limit_mb: int,
) -> dict[str, Any]:
    details = _mapping(_mapping(_mapping(model).get("model")).get("result"))
    details = _mapping(details.get("details"))
    telemetry = _mapping(details.get("phase_telemetry"))
    statistics = _mapping(_mapping(telemetry.get("phase_statistics")).get(phase_name))
    rss = _mapping(_mapping(statistics.get("metrics")).get("max_rss_mb"))
    p95 = _positive_float(rss.get("p95"))
    if p95 is None:
        p95 = _positive_float(rss.get("max"))
    pressure = "unobserved"
    if p95 is not None:
        ratio = p95 / max(1, int(admission_limit_mb))
        pressure = (
            "hard_limit_near"
            if ratio >= 0.95
            else ("safe_envelope_near" if ratio > 0.80 else "within_envelope")
        )
    return {
        "schema_version": "smoking-data.phase-memory-history.v2",
        "phase_name": phase_name,
        "peak_rss_p95_mb": p95,
        "pressure": pressure,
        "observations": int(statistics.get("instances") or 0),
    }


def load_phase_memory_history(
    *,
    project_root: Path,
    model_key: str,
    phase_name: str,
    admission_limit_mb: int,
    limit: int = 20,
) -> dict[str, Any]:
    connection = _connect(project_root)
    try:
        rows = connection.execute(
            """
            SELECT metrics_json
            FROM adaptive_sizing_observations
            WHERE model_key = ?
            ORDER BY observation_id DESC
            LIMIT ?
            """,
            (model_key, max(1, int(limit))),
        ).fetchall()
    finally:
        connection.close()
    peaks: list[float] = []
    recent_pressures: list[str] = []
    for row in rows:
        try:
            metrics = json.loads(str(row["metrics_json"]))
        except (json.JSONDecodeError, TypeError):
            continue
        telemetry = _mapping(_mapping(metrics).get("phase_telemetry"))
        statistics = _mapping(_mapping(telemetry.get("phase_statistics")).get(phase_name))
        rss = _mapping(_mapping(statistics.get("metrics")).get("max_rss_mb"))
        value = _positive_float(rss.get("p95")) or _positive_float(rss.get("max"))
        if value is None:
            continue
        peaks.append(value)
        ratio = value / max(1, int(admission_limit_mb))
        recent_pressures.append(
            "hard_limit_near"
            if ratio >= 0.95
            else ("safe_envelope_near" if ratio > 0.80 else "within_envelope")
        )
    consecutive_exceeds = 0
    for pressure in recent_pressures:
        if pressure not in {"safe_envelope_near", "hard_limit_near"}:
            break
        consecutive_exceeds += 1
    p95 = _percentile(peaks, 0.95)
    return {
        "schema_version": "smoking-data.phase-memory-history.v2",
        "phase_name": phase_name,
        "peak_rss_p95_mb": p95,
        "pressure": recent_pressures[0] if recent_pressures else "unobserved",
        "observations": len(peaks),
        "consecutive_envelope_exceeds": consecutive_exceeds,
        "recalibration_required": consecutive_exceeds >= 3,
    }


def _connect(project_root: Path) -> sqlite3.Connection:
    path = registry_path(project_root)
    ensure_dir(path.parent)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        PRAGMA journal_mode = WAL;
        CREATE TABLE IF NOT EXISTS adaptive_sizing_models (
            model_key TEXT PRIMARY KEY,
            canonical_json TEXT NOT NULL,
            model_json TEXT NOT NULL,
            first_observed_at TEXT NOT NULL,
            last_observed_at TEXT NOT NULL,
            last_alias TEXT NOT NULL,
            last_job_name TEXT NOT NULL,
            observation_count INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS adaptive_sizing_observations (
            observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_key TEXT NOT NULL REFERENCES adaptive_sizing_models(model_key),
            observed_at TEXT NOT NULL,
            alias TEXT NOT NULL,
            job_name TEXT NOT NULL,
            metrics_json TEXT NOT NULL
        );
        """
    )
    return connection


def _source_schema_hash(files: Sequence[Path]) -> str:
    schemas: set[str] = set()
    for path in sorted(Path(item).resolve() for item in files):
        schema = pq.ParquetFile(path).schema_arrow
        schemas.add(
            _json(
                [
                    {
                        "name": field.name,
                        "type": str(field.type),
                        "nullable": field.nullable,
                    }
                    for field in schema
                ]
            )
        )
    return sha256(_json(sorted(schemas)).encode()).hexdigest()


def _canonical_pivot_contract(pivot: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = dict(pivot or {})
    return {key: raw[key] for key in sorted(raw) if key not in {"alias", "description", "name"}}


def _machine_profile() -> dict[str, Any]:
    total_memory_bytes = None
    try:
        total_memory_bytes = int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, TypeError, ValueError):
        pass
    return {
        "system": platform.system().lower(),
        "machine": platform.machine().lower(),
        "total_memory_bytes": total_memory_bytes,
    }


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _positive_float(value: Any) -> float | None:
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return None
    return resolved if resolved > 0 else None


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
