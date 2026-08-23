from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from smoking_data.core.exceptions import ValidationError
from smoking_data.core.pipeline import SourceSpec
from smoking_data.core.results import to_json_safe
from smoking_data.runtime.asset_config import asset_code_from_definition_path
from smoking_data.runtime.config import RuntimeConfig, load_config
from smoking_data.runtime.parquet_probe import ensure_pipeline_probes, ensure_probe
from smoking_data.runtime.paths import ensure_dir, resolve_project_path
from smoking_data.runtime.yaml_loader import load_pipeline_spec, load_preset_spec

PWQ_SCHEMA_VERSION = "smoking-data.pwq-recommendation.v1"
CANDIDATE_SCHEMA_VERSION = "smoking-data.pwq-candidates.v1"
VALIDATION_SCHEMA_VERSION = "smoking-data.pwq-validation.v1"

BASELINE = {
    "range_merge_gap_bytes": 64 * 1024,
    "max_range_bytes": 8 * 1024 * 1024,
    "max_ranges_per_task": 512,
    "minimum_range_savings_ratio": 0.0,
}


@dataclass(frozen=True, slots=True)
class PwqHandle:
    recommendation_path: Path
    candidate_scores_path: Path
    validation_path: Path
    pipeline_fingerprint: str
    dataset_fingerprint: str
    reused: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "recommendation_path": str(self.recommendation_path),
            "candidate_scores_path": str(self.candidate_scores_path),
            "validation_path": str(self.validation_path),
            "pipeline_fingerprint": self.pipeline_fingerprint,
            "dataset_fingerprint": self.dataset_fingerprint,
            "reused": self.reused,
        }


def advise_pipeline(
    yaml_path: str | Path,
    *,
    metadata_path: str | Path | None = None,
    config_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> PwqHandle:
    """Create a deterministic recommendation without executing or rewriting the pipeline."""

    config = load_config(
        config_path=config_path,
        project_root=project_root,
        asset_code=asset_code_from_definition_path(yaml_path),
    )
    pipeline_path = resolve_project_path(yaml_path, project_root=config.project_root)
    raw = yaml.safe_load(pipeline_path.read_text(encoding="utf-8")) or {}
    if isinstance(raw, dict) and isinstance(raw.get("yaml"), dict):
        spec = load_pipeline_spec(pipeline_path, config=config)
        probes = ensure_pipeline_probes(spec, config=config)
        pipeline_fingerprint = spec.logical_plan.plan_hash
        yaml_hash = spec.yaml_hash
        execution = dict(spec.execution)
    else:
        preset = load_preset_spec(pipeline_path, config=config)
        probes = _preset_probes(preset.raw, yaml_hash=preset.yaml_hash, config=config)
        pipeline_fingerprint = preset.yaml_hash
        yaml_hash = preset.yaml_hash
        execution = dict(preset.raw.get("execution") or {})
    if not probes:
        raise ValidationError("PWQ requires at least one Parquet source.", code="pwq.no_source")

    dataset_fingerprint = _hash_json(
        sorted(handle.dataset_fingerprint for handle in probes.values())
    )
    output_root = config.metadata_root / "pwq" / pipeline_fingerprint / dataset_fingerprint
    recommendation_path = output_root / "recommendation.json"
    candidate_scores_path = output_root / "candidate_scores.parquet"
    validation_path = output_root / "validation.json"
    physical = _physical_evidence(probes)
    receipt = _receipt_evidence(
        metadata_path,
        project_root=config.project_root,
        expected_yaml_hash=yaml_hash,
        expected_dataset_fingerprint=dataset_fingerprint,
    )
    evidence_fingerprint = _hash_json(
        {"physical": physical, "receipt": receipt, "execution": execution}
    )
    existing = _read_json(recommendation_path)
    if (
        existing
        and existing.get("schema_version") == PWQ_SCHEMA_VERSION
        and existing.get("pipeline_fingerprint") == pipeline_fingerprint
        and existing.get("dataset_fingerprint") == dataset_fingerprint
        and existing.get("evidence_fingerprint") == evidence_fingerprint
        and _artifacts_valid(output_root, existing)
    ):
        return PwqHandle(
            recommendation_path,
            candidate_scores_path,
            validation_path,
            pipeline_fingerprint,
            dataset_fingerprint,
            reused=True,
        )

    recommendation, candidates = _recommend(
        physical=physical,
        receipt=receipt,
        execution=execution,
    )
    staging = output_root.parent / ".temp" / uuid.uuid4().hex
    ensure_dir(staging)
    try:
        pq.write_table(
            pa.Table.from_pylist(candidates),
            staging / "candidate_scores.parquet",
            compression=None,
        )
        validation = {
            "schema_version": VALIDATION_SCHEMA_VERSION,
            "pipeline_fingerprint": pipeline_fingerprint,
            "dataset_fingerprint": dataset_fingerprint,
            "evidence_fingerprint": evidence_fingerprint,
            "probe_manifests": sorted(
                _portable_path(handle.manifest_path, config.project_root)
                for handle in probes.values()
            ),
            "receipt_status": receipt["status"],
            "result_parity_checked": False,
            "result_parity_reason": "advisory_does_not_execute_candidate_pipelines",
            "source_dataset_modified": False,
        }
        _write_json(staging / "validation.json", validation)
        artifact_details = {
            "candidate_scores": {
                "relative_path": "candidate_scores.parquet",
                "sha256": _file_sha256(staging / "candidate_scores.parquet"),
            },
            "validation": {
                "relative_path": "validation.json",
                "sha256": _file_sha256(staging / "validation.json"),
            },
        }
        document = {
            "schema_version": PWQ_SCHEMA_VERSION,
            "pipeline_fingerprint": pipeline_fingerprint,
            "dataset_fingerprint": dataset_fingerprint,
            "evidence_fingerprint": evidence_fingerprint,
            "baseline": BASELINE,
            "recommendation": recommendation,
            "evidence": {"physical": physical, "receipt": receipt},
            "confidence": recommendation["confidence"],
            "requires_dataset_rewrite": bool(recommendation["writer"]),
            "applied": False,
            "artifact_paths": {
                "candidate_scores": "candidate_scores.parquet",
                "validation": "validation.json",
            },
            "artifact_details": artifact_details,
        }
        _write_json(staging / "recommendation.json", document)
        ensure_dir(output_root.parent)
        if output_root.exists():
            shutil.rmtree(output_root)
        os.replace(staging, output_root)
        _cleanup_empty(staging.parent)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        _cleanup_empty(staging.parent)
        raise
    return PwqHandle(
        recommendation_path,
        candidate_scores_path,
        validation_path,
        pipeline_fingerprint,
        dataset_fingerprint,
        reused=False,
    )


def _preset_probes(raw: dict[str, Any], *, yaml_hash: str, config: RuntimeConfig) -> dict[str, Any]:
    preset = str(raw.get("preset") or "")
    if "03.01" not in preset and "0201" not in preset:
        raise ValidationError(
            "PWQ preset adapter currently supports only 0201 fixtures.",
            code="pwq.unsupported_preset",
            context={"preset": preset},
        )
    source = dict(raw.get("source") or {})
    upstream = dict(source.get("upstream") or {})
    paths = upstream.get("paths")
    if not isinstance(paths, list) or not paths:
        raise ValidationError("PWQ 0201 fixture requires source.upstream.paths.")
    payload = dict(source.get("payload") or {})
    selection = dict(raw.get("row_selection") or {})
    sort_first = dict(selection.get("sort_first") or {})
    required = {str(value) for value in payload.get("include_columns") or []}
    required.update(
        str(item.get("name"))
        for item in payload.get("type_casts") or []
        if isinstance(item, dict) and item.get("name")
    )
    selector = {str(value) for value in sort_first.get("group_keys") or []}
    selector.update(
        str(item.get("column"))
        for item in sort_first.get("sort") or []
        if isinstance(item, dict) and item.get("column")
    )
    required.update(selector)
    source_spec = SourceSpec(
        name="main",
        kind="parquet_dataset",
        paths=tuple(str(value) for value in paths),
        union_by_name=True,
        missing_columns="insert_null",
        incompatible_dtypes="error",
    )
    handle = ensure_probe(
        source_name="main",
        source=source_spec,
        access_profile={
            "source_name": "main",
            "required_columns": sorted(required),
            "selector_columns": sorted(selector),
            "partition_and_group_columns": sorted(selector),
            "payload_columns": sorted(required - selector),
            "projection_width": len(required),
        },
        downstream_plan_fingerprints=[yaml_hash],
        config=config,
    )
    return {"main": handle}


def _physical_evidence(probes: dict[str, Any]) -> dict[str, Any]:
    file_count = 0
    row_group_rows = 0
    page_rows = 0
    compressed_bytes = 0
    uncompressed_bytes = 0
    row_counts: list[int] = []
    page_index_sources = 0
    for handle in probes.values():
        manifest = _read_json(handle.manifest_path) or {}
        file_count += int(manifest.get("file_count") or 0)
        row_group_rows += int(manifest.get("row_group_rows") or 0)
        page_rows += int(manifest.get("page_rows") or 0)
        page_index_sources += int(bool((manifest.get("capabilities") or {}).get("page_index")))
        table = (
            pl.read_parquet(handle.artifact_paths["row_groups"])
            .unique(subset=["source_file", "row_group_id", "column_path"])
        )
        compressed_bytes += int(table["compressed_bytes"].sum())
        uncompressed_bytes += int(table["uncompressed_bytes"].sum())
        row_counts.extend(int(value) for value in table["row_count"].to_list())
    return {
        "source_count": len(probes),
        "file_count": file_count,
        "column_chunk_rows": row_group_rows,
        "page_rows": page_rows,
        "page_index_source_count": page_index_sources,
        "compressed_bytes": compressed_bytes,
        "uncompressed_bytes": uncompressed_bytes,
        "median_row_group_rows": int(median(row_counts)) if row_counts else 0,
    }


def _receipt_evidence(
    metadata_path: str | Path | None,
    *,
    project_root: Path,
    expected_yaml_hash: str,
    expected_dataset_fingerprint: str,
) -> dict[str, Any]:
    if metadata_path is None:
        return {"status": "unavailable", "task_count": 0}
    path = resolve_project_path(metadata_path, project_root=project_root)
    metadata = _read_json(path)
    if not metadata:
        raise ValidationError(
            "PWQ metadata file is missing or invalid.",
            code="pwq.invalid_metadata",
            context={"path": _portable_path(path, project_root)},
        )
    if str(metadata.get("yaml_hash") or "") != expected_yaml_hash:
        return {"status": "stale_pipeline", "task_count": 0}
    result = dict(metadata.get("result") or {})
    details = dict(result.get("details") or {})
    artifact = details.get("details_artifact")
    if isinstance(artifact, dict) and artifact.get("path"):
        external = _read_json(Path(str(artifact["path"]))) or {}
        details.update(external)
    bound_fingerprints: list[str] = []
    physical_probes = details.get("physical_probes")
    if isinstance(physical_probes, dict):
        bound_fingerprints.extend(
            str(item.get("dataset_fingerprint"))
            for item in physical_probes.values()
            if isinstance(item, dict) and item.get("dataset_fingerprint")
        )
    physical_probe = details.get("physical_probe")
    if isinstance(physical_probe, dict) and physical_probe.get("dataset_fingerprint"):
        bound_fingerprints.append(str(physical_probe["dataset_fingerprint"]))
    if (
        bound_fingerprints
        and _hash_json(sorted(bound_fingerprints)) != expected_dataset_fingerprint
    ):
        return {"status": "stale_dataset", "task_count": 0}
    dependency = details.get("dependency_graph")
    upstream = dependency.get("upstream") if isinstance(dependency, dict) else None
    if isinstance(upstream, dict) and upstream.get("fingerprint") and upstream.get("files"):
        current_fingerprint = _stat_fingerprint([Path(str(value)) for value in upstream["files"]])
        if current_fingerprint != upstream["fingerprint"]:
            return {"status": "stale_dataset", "task_count": 0}
    tasks = details.get("task_results") or []
    counters = [
        dict(task.get("counters") or {})
        for task in tasks
        if isinstance(task, dict) and isinstance(task.get("counters"), dict)
    ]
    planned_range = sum(float(item.get("planned_range_bytes") or 0) for item in counters)
    planned_row_group = sum(float(item.get("planned_row_group_bytes") or 0) for item in counters)
    actual_read = sum(float(item.get("actual_source_bytes_read") or 0) for item in counters)
    peak_rss = [float(item.get("rss_peak_mb") or 0) for item in counters if item.get("rss_peak_mb")]
    return {
        "status": (
            ("available" if bound_fingerprints else "available_unbound")
            if counters
            else "no_task_receipts"
        ),
        "task_count": len(counters),
        "planned_range_bytes": int(planned_range),
        "planned_row_group_bytes": int(planned_row_group),
        "actual_source_bytes_read": int(actual_read),
        "range_to_row_group_ratio": (
            planned_range / planned_row_group if planned_row_group > 0 else None
        ),
        "peak_rss_mb_max": max(peak_rss) if peak_rss else None,
    }


def _recommend(
    *,
    physical: dict[str, Any],
    receipt: dict[str, Any],
    execution: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ratio = receipt.get("range_to_row_group_ratio")
    confidence = "medium" if ratio is not None else "low"
    if ratio is None:
        chosen = "baseline"
        reason = "task_receipts_unavailable"
    elif ratio <= 0.50:
        chosen = "sparse"
        reason = "planned_page_ranges_are_sparse"
    elif ratio >= 0.85:
        chosen = "dense"
        reason = "planned_page_ranges_approach_row_group_cost"
    else:
        chosen = "baseline"
        reason = "observed_ratio_is_near_baseline_boundary"

    candidate_values = {
        "sparse": {**BASELINE, "max_ranges_per_task": 256},
        "baseline": dict(BASELINE),
        "dense": {
            **BASELINE,
            "max_ranges_per_task": 128,
            "minimum_range_savings_ratio": 0.20,
        },
    }
    candidates = [
        {
            "schema_version": CANDIDATE_SCHEMA_VERSION,
            "candidate": name,
            **values,
            "estimated_ratio": float(ratio) if ratio is not None else None,
            "eligible": name == chosen,
            "reason": reason if name == chosen else "not_selected",
        }
        for name, values in candidate_values.items()
    ]
    peak_rss = receipt.get("peak_rss_mb_max")
    memory_budget = int(execution.get("memory_budget_mb") or 4096)
    current_workers = max(1, int(execution.get("workers") or 1))
    safe_workers = current_workers
    if peak_rss:
        safe_workers = max(1, min(current_workers, int(memory_budget // float(peak_rss))))
    writer: dict[str, Any] = {}
    median_rows = int(physical.get("median_row_group_rows") or 0)
    if median_rows > 0 and median_rows < 1_000:
        writer = {
            "review_row_group_size": True,
            "reason": "median_row_group_is_small",
            "observed_median_rows": median_rows,
        }
    return (
        {
            "read_planner": {"profile": chosen, **candidate_values[chosen], "reason": reason},
            "execution": {
                "workers": safe_workers,
                "max_tasks_per_child": 1,
                "reason": "bounded_by_observed_peak_rss" if peak_rss else "keep_safe_default",
            },
            "writer": writer,
            "confidence": confidence,
        },
        candidates,
    )


def _hash_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stat_fingerprint(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        if not path.is_file():
            return "missing"
        stat = path.stat()
        digest.update(f"{path}|{stat.st_size}|{stat.st_mtime_ns}".encode())
        digest.update(b"\n")
    return digest.hexdigest()


def _portable_path(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _artifacts_valid(root: Path, recommendation: dict[str, Any]) -> bool:
    details = recommendation.get("artifact_details")
    if not isinstance(details, dict) or not details:
        return False
    return all(
        isinstance(item, dict)
        and item.get("relative_path")
        and (root / str(item["relative_path"])).is_file()
        and _file_sha256(root / str(item["relative_path"])) == item.get("sha256")
        for item in details.values()
    )


def _file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(to_json_safe(value), ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )


def _cleanup_empty(path: Path) -> None:
    try:
        path.rmdir()
    except OSError:
        pass
