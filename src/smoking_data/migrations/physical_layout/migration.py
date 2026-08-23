from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import platform
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from smoking_data.advisors.physical_layout.advisor import RECOMMENDATION_SCHEMA_VERSION
from smoking_data.core.bounded_dataset import (
    compare_bounded_summaries,
    summarize_parquet_dataset_bounded,
)
from smoking_data.core.exceptions import SmokingDataError
from smoking_data.core.pipeline import SourceSpec
from smoking_data.runtime.config import load_config
from smoking_data.runtime.memory import peak_rss_mb, process_io_bytes
from smoking_data.runtime.parquet_probe import ensure_source_probe
from smoking_data.runtime.paths import file_sha256
from smoking_data.runtime.transactions import (
    DATASET_MANIFEST_VERSION,
    refresh_dataset_manifest_provenance,
    validate_committed_dataset,
)

MIGRATION_SCHEMA_VERSION = "smoking-data.layout-migration.v1"
RECEIPT_SCHEMA_VERSION = "smoking-data.layout-migration-receipt.v1"
ALLOWED_WRITER_KEYS = frozenset(
    {
        "compression",
        "row_group_size",
        "write_page_index",
        "write_statistics",
        "data_page_size",
        "max_rows_per_page",
        "use_dictionary",
    }
)


class LayoutMigrationError(SmokingDataError):
    code = "layout_migration.error"


@dataclass(frozen=True, slots=True)
class LayoutMigrationResult:
    status: str
    dataset_path: Path
    migration_id: str
    dry_run: bool
    plan: dict[str, Any]
    receipt_path: Path | None = None
    physical_probe: dict[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.status in {
            "dry_run",
            "completed",
            "completed_with_probe_error",
            "no_change",
            "already_applied",
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "schema_version": MIGRATION_SCHEMA_VERSION,
            "status": self.status,
            "dataset_path": str(self.dataset_path),
            "migration_id": self.migration_id,
            "dry_run": self.dry_run,
            "plan": self.plan,
            "receipt_path": str(self.receipt_path) if self.receipt_path else None,
            "physical_probe": self.physical_probe,
        }


def migrate_layout_yaml(
    yaml_path: str | Path,
    *,
    project_root: str | Path | None = None,
) -> LayoutMigrationResult:
    root = Path(project_root or Path.cwd()).expanduser().resolve()
    definition_path = _resolve(yaml_path, root)
    try:
        payload = yaml.safe_load(definition_path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise LayoutMigrationError(
            f"Invalid layout migration YAML: {definition_path}",
            code="layout_migration.invalid_definition",
        ) from exc
    if not isinstance(payload, dict):
        raise LayoutMigrationError(
            "Layout migration YAML root must be a mapping.",
            code="layout_migration.invalid_definition",
        )
    header = payload.get("yaml") or {}
    if header != {"schema_version": MIGRATION_SCHEMA_VERSION, "asset_code": "0101"}:
        raise LayoutMigrationError(
            "Unsupported layout migration YAML header.",
            code="layout_migration.invalid_definition",
        )
    job = payload.get("job") or {}
    upstream_items = payload.get("define_upstream") or []
    if not isinstance(upstream_items, list) or len(upstream_items) != 1:
        raise LayoutMigrationError(
            "define_upstream must contain exactly one dataset.",
            code="layout_migration.invalid_definition",
        )
    upstream = upstream_items[0] if isinstance(upstream_items[0], dict) else {}
    migration = payload.get("migrate_layout") or {}
    execution = payload.get("execution") or {}
    if not all(isinstance(item, dict) for item in (job, migration, execution)):
        raise LayoutMigrationError(
            "job, migrate_layout, and execution must be mappings.",
            code="layout_migration.invalid_definition",
        )
    if not isinstance(job.get("name"), str) or not job["name"].strip():
        raise LayoutMigrationError(
            "job.name is required.", code="layout_migration.invalid_definition"
        )
    if (
        upstream.get("op") != "define_dataset"
        or not upstream.get("alias")
        or not upstream.get("path")
    ):
        raise LayoutMigrationError(
            "define_upstream must define one dataset path.",
            code="layout_migration.invalid_definition",
        )
    if (
        migration.get("op") != "rewrite_parquet_layout"
        or migration.get("source") != upstream.get("alias")
        or not migration.get("recommendation")
    ):
        raise LayoutMigrationError(
            "migrate_layout must reference define_upstream and a recommendation YAML.",
            code="layout_migration.invalid_definition",
        )
    allowed_root = {"yaml", "job", "define_upstream", "migrate_layout", "execution"}
    allowed_execution = {
        "mode",
        "workers",
        "memory_budget_mb",
        "batch_size",
        "max_files_per_worker",
    }
    if (
        set(payload) - allowed_root
        or set(execution) - allowed_execution
        or set(upstream) - {"op", "alias", "path"}
        or set(migration) - {"op", "source", "recommendation"}
        or set(job) - {"name"}
    ):
        raise LayoutMigrationError(
            "Layout migration YAML contains unsupported fields.",
            code="layout_migration.invalid_definition",
        )
    mode = str(execution.get("mode") or "dry_run")
    if mode not in {"dry_run", "in_place"}:
        raise LayoutMigrationError(
            "execution.mode must be dry_run or in_place.",
            code="layout_migration.invalid_definition",
        )
    return migrate_0101_dataset(
        upstream["path"],
        migration["recommendation"],
        in_place=mode == "in_place",
        project_root=root,
        memory_budget_mb=int(execution.get("memory_budget_mb") or 4_096),
        batch_size=int(execution.get("batch_size") or 4_096),
        max_files_per_worker=int(execution.get("max_files_per_worker") or 8),
        workers=int(execution.get("workers") or 1),
        migration_definition_path=definition_path,
    )

def migrate_0101_dataset(
    dataset_path: str | Path,
    recommendation_path: str | Path,
    *,
    in_place: bool = False,
    project_root: str | Path | None = None,
    memory_budget_mb: int = 4_096,
    batch_size: int = 4_096,
    max_files_per_worker: int = 8,
    workers: int = 1,
    migration_definition_path: str | Path | None = None,
) -> LayoutMigrationResult:
    if memory_budget_mb < 1 or batch_size < 1 or max_files_per_worker < 1 or workers < 1:
        raise LayoutMigrationError(
            "memory_budget_mb, batch_size, and max_files_per_worker must be positive.",
            code="layout_migration.invalid_recommendation",
        )
    root = Path(project_root or Path.cwd()).expanduser().resolve()
    dataset = _resolve(dataset_path, root)
    recommendation_file = _resolve(recommendation_path, root)
    recommendation = _load_recommendation(recommendation_file)
    migration_id = str(recommendation["recommendation_id"])
    plan = _build_plan(
        dataset,
        recommendation,
        recommendation_file=recommendation_file,
        memory_budget_mb=memory_budget_mb,
        batch_size=batch_size,
        max_files_per_worker=max_files_per_worker,
        workers=workers,
        migration_definition_path=(
            _resolve(migration_definition_path, root)
            if migration_definition_path is not None
            else None
        ),
    )
    if plan["already_applied"] and not plan["blocking_reasons"]:
        return LayoutMigrationResult("already_applied", dataset, migration_id, not in_place, plan)
    if not plan["changes"] and not plan["blocking_reasons"]:
        return LayoutMigrationResult("no_change", dataset, migration_id, not in_place, plan)
    if not in_place:
        return LayoutMigrationResult("dry_run", dataset, migration_id, True, plan)
    if plan["blocking_reasons"]:
        primary = plan["blocking_reasons"][0]
        raise LayoutMigrationError(
            "Layout migration is blocked by preflight validation.",
            code=str(primary["code"]),
            context={
                "blocking_reasons": plan["blocking_reasons"],
                "dataset": str(dataset),
                "migration_id": migration_id,
                "safe_recovery_state": "source_dataset_untouched",
            },
        )
    try:
        return _execute_migration(
            dataset,
            recommendation,
            recommendation_file=recommendation_file,
            plan=plan,
            batch_size=batch_size,
            memory_budget_mb=memory_budget_mb,
            max_files_per_worker=max_files_per_worker,
            workers=workers,
            migration_definition_path=(
                _resolve(migration_definition_path, root)
                if migration_definition_path is not None
                else None
            ),
        )
    except LayoutMigrationError:
        raise
    except Exception as exc:
        raise LayoutMigrationError(
            "Layout migration failed before a verified commit was completed.",
            code="layout_migration.commit_failed",
            context={
                "dataset": str(dataset),
                "migration_id": migration_id,
                "safe_recovery_state": (
                    "source_dataset_or_verified_backup_preserved"
                ),
                "cause_type": type(exc).__name__,
                "cause_message": str(exc),
            },
        ) from exc


def _load_recommendation(path: Path) -> dict[str, Any]:
    if path.suffix.lower() not in {".yaml", ".yml"}:
        raise LayoutMigrationError(
            "Physical-layout recommendation must be YAML.",
            code="layout_migration.invalid_recommendation",
        )
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise LayoutMigrationError(
            f"Invalid recommendation: {path}",
            code="layout_migration.invalid_recommendation",
        ) from exc
    if (
        not isinstance(payload, dict)
        or (payload.get("yaml") or {}).get("schema_version") != RECOMMENDATION_SCHEMA_VERSION
    ):
        raise LayoutMigrationError(
            "Unsupported physical-layout recommendation schema.",
            code="layout_migration.invalid_recommendation",
        )
    expected = str(payload.get("canonical_hash") or "")
    canonical = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "canonical_hash",
            "recommendation_id",
            "generated_at",
            "history_sources",
            "yaml_patch",
        }
    }
    encoded = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    actual = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    if expected != actual or payload.get("recommendation_id") != f"layout-{actual[:16]}":
        raise LayoutMigrationError(
            "Recommendation canonical hash is invalid.",
            code="layout_migration.invalid_recommendation",
        )
    if payload.get("edge") != {"upstream": "0101", "downstream": "0201"}:
        raise LayoutMigrationError(
            "Only 0101 -> 0201 recommendations can migrate a 0101 dataset.",
            code="layout_migration.unsupported_asset",
        )
    settings = payload.get("recommendation")
    if not isinstance(settings, dict) or not settings:
        raise LayoutMigrationError(
            "Recommendation settings are missing.", code="layout_migration.invalid_recommendation"
        )
    unknown = sorted(set(settings) - ALLOWED_WRITER_KEYS)
    if unknown:
        raise LayoutMigrationError(
            "Recommendation contains unsupported writer options.",
            code="layout_migration.unsupported_writer_option",
            context={"unknown": unknown},
        )
    if int(settings.get("row_group_size") or 0) < 1:
        raise LayoutMigrationError(
            "row_group_size must be positive.", code="layout_migration.invalid_recommendation"
        )
    experiment = payload.get("candidate_experiment")
    if payload.get("recommendation_source") == "candidate_experiment" and (
        not isinstance(experiment, dict)
        or not (experiment.get("logical_parity") or {}).get("all_ok")
    ):
        raise LayoutMigrationError(
            "Candidate recommendation is missing parity evidence.",
            code="layout_migration.invalid_recommendation",
        )
    return payload


def _build_plan(
    dataset: Path,
    recommendation: dict[str, Any],
    *,
    recommendation_file: Path,
    memory_budget_mb: int,
    batch_size: int,
    max_files_per_worker: int,
    workers: int,
    migration_definition_path: Path | None,
) -> dict[str, Any]:
    blocking: list[dict[str, Any]] = []
    if not dataset.is_dir():
        _block(blocking, "layout_migration.invalid_dataset_manifest", "dataset is not a directory")
    manifest_path = dataset / "_dataset.manifest.json"
    metadata_path = dataset / "_smoking_data" / "metadata.json"
    definition_path = dataset / "_smoking_data" / "definition.yaml"
    manifest = _read_json(manifest_path)
    metadata = _read_json(metadata_path)
    if manifest is None or metadata is None or not validate_committed_dataset(dataset):
        _block(blocking, "layout_migration.invalid_dataset_manifest", "manifest/provenance invalid")
    if not isinstance(metadata, dict) or (metadata.get("asset") or {}).get("code") != "0101":
        _block(blocking, "layout_migration.unsupported_asset", "metadata asset code is not 0101")
    if not definition_path.is_file():
        _block(blocking, "layout_migration.invalid_dataset_manifest", "definition.yaml missing")
    else:
        expected_hash = str((recommendation.get("upstream") or {}).get("yaml_sha256") or "")
        if file_sha256(definition_path) != expected_hash:
            _block(blocking, "layout_migration.stale_recommendation", "upstream YAML hash mismatch")
    parts = _manifest_parts(dataset, manifest or {}, blocking) if dataset.is_dir() else []
    if not parts:
        _block(blocking, "layout_migration.invalid_dataset_manifest", "dataset has no valid parts")
    current = recommendation.get("current") or {}
    definition_current = _definition_writer_settings(definition_path)
    mismatched = {
        key: {"recommendation_current": value, "definition_current": definition_current.get(key)}
        for key, value in current.items()
        if definition_current.get(key) != value
    }
    if mismatched:
        _block(
            blocking,
            "layout_migration.stale_recommendation",
            "definition writer settings changed",
            mismatched=mismatched,
        )
    settings = dict(recommendation["recommendation"])
    migrated_current = (
        (metadata.get("physical_layout_migration") or {}).get("after")
        if isinstance(metadata, dict)
        else None
    )
    effective_current = (
        dict(migrated_current) if isinstance(migrated_current, dict) else dict(current)
    )
    changes = {
        key: {"before": effective_current.get(key), "after": value}
        for key, value in settings.items()
        if effective_current.get(key) != value
    }
    source_bytes = sum(int(item["size_bytes"]) for item in parts)
    free_bytes = shutil.disk_usage(dataset.parent).free if dataset.parent.exists() else 0
    # During cutover the source, staging, and backup names coexist. Rename does not
    # duplicate source bytes, so only staging plus a safety margin needs free space.
    required_bytes = int(source_bytes * 1.25)
    if free_bytes < required_bytes:
        _block(
            blocking,
            "layout_migration.insufficient_disk",
            "insufficient free disk for staging",
            free_bytes=free_bytes,
            required_bytes=required_bytes,
        )
    max_file_uncompressed = max((int(item["uncompressed_bytes"]) for item in parts), default=0)
    estimated_peak_per_worker_bytes = min(
        max_file_uncompressed,
        max(1, int(settings["row_group_size"])) * max(
            1, max((int(item["estimated_row_bytes"]) for item in parts), default=1)
        ),
    )
    parent_reserve_bytes = min(512 * 1024 * 1024, max(128 * 1024 * 1024, source_bytes // 20))
    estimated_peak_bytes = parent_reserve_bytes + estimated_peak_per_worker_bytes * workers
    if estimated_peak_bytes > memory_budget_mb * 1024 * 1024:
        _block(
            blocking,
            "layout_migration.memory_budget_exceeded",
            "estimated row-group buffer exceeds memory budget",
            estimated_peak_bytes=estimated_peak_bytes,
            memory_budget_mb=memory_budget_mb,
        )
    migration_id = str(recommendation["recommendation_id"])
    receipt = dataset / "_smoking_data" / "migrations" / migration_id / "receipt.json"
    return {
        "schema_version": MIGRATION_SCHEMA_VERSION,
        "dataset": str(dataset),
        "recommendation_path": str(recommendation_file),
        "migration_definition_path": (
            str(migration_definition_path) if migration_definition_path else None
        ),
        "recommendation_hash": recommendation["canonical_hash"],
        "migration_id": migration_id,
        "already_applied": receipt.is_file(),
        "changes": changes,
        "effective_current": effective_current,
        "settings": settings,
        "parts": parts,
        "file_count": len(parts),
        "rows": sum(int(item["rows"]) for item in parts),
        "row_groups_before": sum(int(item["row_groups"]) for item in parts),
        "row_groups_after_estimated": sum(
            (int(item["rows"]) + int(settings["row_group_size"]) - 1)
            // int(settings["row_group_size"])
            for item in parts
        ),
        "file_row_count_contract": "preserve_per_relative_path",
        "source_manifest_sha256": file_sha256(manifest_path) if manifest_path.is_file() else None,
        "disk": {
            "source_bytes": source_bytes,
            "staging_bytes_estimated": source_bytes,
            "backup_bytes_logical": source_bytes,
            "peak_dataset_bytes_estimated": source_bytes * 2,
            "required_free_bytes": required_bytes,
            "available_free_bytes": free_bytes,
        },
        "memory": {
            "batch_size": batch_size,
            "estimated_peak_bytes": estimated_peak_bytes,
            "estimated_peak_per_worker_bytes": estimated_peak_per_worker_bytes,
            "parent_reserve_bytes": parent_reserve_bytes,
            "budget_mb": memory_budget_mb,
            "workers": workers,
            "worker_start_method": "spawn",
            "max_files_per_worker": max_files_per_worker,
        },
        "recommendation_policy": {
            "source": recommendation.get("recommendation_source"),
            "confidence": recommendation.get("confidence"),
            "in_place_allowed": True,
            "explicit_in_place_required": True,
        },
        "stale_artifacts": ["_smoking_data/physical_probe", "downstream physical probes/sidecars"],
        "blocking_reasons": blocking,
    }


def _manifest_parts(
    dataset: Path, manifest: dict[str, Any], blocking: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    declared = manifest.get("parts")
    if not isinstance(declared, list):
        return parts
    actual_paths = {
        path.relative_to(dataset).as_posix()
        for path in dataset.rglob("*.parquet")
        if path.is_file() and "_smoking_data" not in path.relative_to(dataset).parts
    }
    declared_paths: set[str] = set()
    for item in declared:
        relative = str((item or {}).get("relative_path") or "")
        path = (dataset / relative).resolve()
        try:
            path.relative_to(dataset)
        except ValueError:
            _block(blocking, "layout_migration.invalid_dataset_manifest", "part path escapes root")
            continue
        if not relative or not path.is_file():
            continue
        declared_paths.add(relative)
        try:
            parquet = pq.ParquetFile(path)
        except (OSError, pa.ArrowException) as exc:
            _block(
                blocking,
                "layout_migration.invalid_dataset_manifest",
                "part footer is unreadable",
                relative_path=relative,
                error=str(exc),
            )
            continue
        metadata = parquet.metadata
        if path.stat().st_size == 0 or metadata.num_rows == 0:
            _block(
                blocking,
                "layout_migration.invalid_dataset_manifest",
                "empty parquet parts are not supported",
                relative_path=relative,
            )
            continue
        uncompressed = sum(
            int(metadata.row_group(rg).column(col).total_uncompressed_size or 0)
            for rg in range(metadata.num_row_groups)
            for col in range(metadata.row_group(rg).num_columns)
        )
        rows = int(metadata.num_rows)
        parts.append(
            {
                "relative_path": relative,
                "rows": rows,
                "row_groups": int(metadata.num_row_groups),
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
                "schema": str(parquet.schema_arrow),
                "uncompressed_bytes": uncompressed,
                "estimated_row_bytes": (uncompressed // rows if rows else 0),
            }
        )
    if actual_paths != declared_paths:
        _block(
            blocking,
            "layout_migration.invalid_dataset_manifest",
            "manifest parts do not match dataset files",
        )
    return parts


def _execute_migration(
    dataset: Path,
    recommendation: dict[str, Any],
    *,
    recommendation_file: Path,
    plan: dict[str, Any],
    batch_size: int,
    memory_budget_mb: int,
    max_files_per_worker: int,
    workers: int,
    migration_definition_path: Path | None,
) -> LayoutMigrationResult:
    migration_id = str(recommendation["recommendation_id"])
    staging = dataset.parent / f".{dataset.name}.layout-{migration_id}.staging"
    backup = dataset.parent / f".{dataset.name}.layout-{migration_id}.backup"
    staging.mkdir(parents=True, exist_ok=True)
    resume_path = staging / ".migration-resume.json"
    resume = _read_json(resume_path) or {"schema_version": MIGRATION_SCHEMA_VERSION, "parts": {}}
    source_manifest_hash = str(plan["source_manifest_sha256"])
    started = time.perf_counter()
    io_started = process_io_bytes()
    cpu_started = time.process_time()
    part_receipts: list[dict[str, Any]] = []
    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=multiprocessing.get_context("spawn"),
        max_tasks_per_child=max_files_per_worker,
    ) as executor:
        pending: list[tuple[dict[str, Any], Any]] = []
        for part in plan["parts"]:
            relative = str(part["relative_path"])
            source = dataset / relative
            target = staging / relative
            completed = (resume.get("parts") or {}).get(relative)
            if (
                isinstance(completed, dict)
                and completed.get("source_sha256") == part["sha256"]
                and target.is_file()
                and completed.get("destination_sha256") == file_sha256(target)
            ):
                part_receipts.append(completed)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            future = executor.submit(
                _rewrite_part_worker,
                (source, target, relative, plan["settings"], batch_size),
            )
            pending.append((part, future))
            if len(pending) >= workers:
                _collect_rewrite_batch(pending, resume=resume, resume_path=resume_path, receipts=part_receipts)
                pending = []
        _collect_rewrite_batch(
            pending, resume=resume, resume_path=resume_path, receipts=part_receipts
        )
    _copy_provenance(dataset, staging, migration_id=migration_id)
    source_summary = summarize_parquet_dataset_bounded(dataset, batch_size=batch_size)
    staging_summary = summarize_parquet_dataset_bounded(staging, batch_size=batch_size)
    observed_peak_rss_mb = max(
        [peak_rss_mb() or 0.0]
        + [float(item.get("peak_rss_mb") or 0.0) for item in part_receipts]
    )
    if observed_peak_rss_mb > memory_budget_mb:
        raise LayoutMigrationError(
            "Observed migration RSS exceeded the configured memory budget.",
            code="layout_migration.memory_budget_exceeded",
            context={
                "observed_peak_rss_mb": observed_peak_rss_mb,
                "memory_budget_mb": memory_budget_mb,
                "safe_recovery_state": "source_dataset_untouched",
            },
        )
    parity = compare_bounded_summaries(
        source_summary, staging_summary, require_file_boundaries=True
    )
    if not parity["ok"]:
        raise LayoutMigrationError(
            "Migrated dataset failed logical parity.",
            code="layout_migration.logical_parity_failed",
            context={key: value for key, value in parity.items() if key not in {"left", "right"}},
        )
    if file_sha256(dataset / "_dataset.manifest.json") != source_manifest_hash:
        raise LayoutMigrationError(
            "Source dataset changed during migration.",
            code="layout_migration.source_changed_during_migration",
        )
    migration_root = staging / "_smoking_data" / "migrations" / migration_id
    migration_root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(recommendation_file, migration_root / "recommendation.yaml")
    if migration_definition_path is not None:
        shutil.copy2(migration_definition_path, migration_root / "migration.yaml")
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "status": "completed",
        "migration_id": migration_id,
        "migrated_at": datetime.now(timezone.utc).isoformat(),
        "source_manifest_sha256": source_manifest_hash,
        "recommendation_sha256": file_sha256(recommendation_file),
        "recommendation_canonical_hash": recommendation["canonical_hash"],
        "migration_definition_sha256": (
            file_sha256(migration_definition_path) if migration_definition_path else None
        ),
        "before": plan["effective_current"],
        "after": recommendation["recommendation"],
        "parts": part_receipts,
        "summary": {
            "files": source_summary["files"],
            "rows": source_summary["rows"],
            "row_groups_before": source_summary["row_groups"],
            "row_groups_after": staging_summary["row_groups"],
            "elapsed_sec": time.perf_counter() - started,
            "cpu_sec": (time.process_time() - cpu_started)
            + sum(float(item.get("cpu_sec") or 0.0) for item in part_receipts),
            "peak_rss_mb": observed_peak_rss_mb,
            "process_io": _sum_process_io(
                _io_delta(io_started, process_io_bytes()), part_receipts
            ),
        },
        "logical_parity": parity,
        "environment": {
            "python": sys.version.split()[0],
            "pyarrow": pyarrow.__version__,
            "os": platform.system().lower(),
        },
        "execution": {
            "workers": workers,
            "max_files_per_worker": max_files_per_worker,
            "worker_pid_count": len(
                {item.get("worker_pid") for item in part_receipts if item.get("worker_pid")}
            ),
        },
    }
    _write_json_atomic(migration_root / "receipt.json", receipt)
    _update_migrated_metadata(staging, receipt)
    _write_dataset_manifest(staging, migration_id=migration_id)
    resume_path.unlink(missing_ok=True)
    if staging.stat().st_dev != dataset.parent.stat().st_dev:
        raise LayoutMigrationError(
            "Staging and dataset must share a filesystem.",
            code="layout_migration.commit_failed",
        )
    if backup.exists():
        shutil.rmtree(backup)
    moved = False
    try:
        os.replace(dataset, backup)
        moved = True
        os.replace(staging, dataset)
        if not validate_committed_dataset(dataset):
            raise LayoutMigrationError(
                "Committed dataset checksum validation failed.",
                code="layout_migration.commit_failed",
            )
    except BaseException:
        if moved and backup.exists():
            if dataset.exists():
                failed = dataset.parent / f".{dataset.name}.layout-{migration_id}.failed"
                os.replace(dataset, failed)
            os.replace(backup, dataset)
        raise
    else:
        shutil.rmtree(backup)
    probe = _refresh_physical_probe(dataset, recommendation=recommendation)
    receipt_path = dataset / "_smoking_data" / "migrations" / migration_id / "receipt.json"
    committed_receipt = _read_json(receipt_path) or receipt
    committed_receipt["physical_probe"] = probe
    committed_receipt["status"] = "completed" if probe["ok"] else "completed_with_probe_error"
    _write_json_atomic(receipt_path, committed_receipt)
    refresh_dataset_manifest_provenance(dataset)
    if not validate_committed_dataset(dataset):
        raise LayoutMigrationError(
            "Dataset became invalid while recording probe provenance.",
            code="layout_migration.commit_failed",
            context={"dataset": str(dataset), "migration_id": migration_id},
        )
    return LayoutMigrationResult(
        str(committed_receipt["status"]),
        dataset,
        migration_id,
        False,
        plan,
        receipt_path,
        probe,
    )


def _rewrite_part(
    source: Path,
    target: Path,
    *,
    relative_path: str,
    settings: dict[str, Any],
    batch_size: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    cpu_started = time.process_time()
    io_started = process_io_bytes()
    parquet = pq.ParquetFile(source)
    schema = parquet.schema_arrow
    row_group_size = int(settings["row_group_size"])
    effective = min(row_group_size, max(1, int(parquet.metadata.num_rows)))
    temporary = target.with_suffix(target.suffix + ".incomplete")
    temporary.unlink(missing_ok=True)
    writer = pq.ParquetWriter(
        temporary,
        schema,
        compression=settings.get("compression", "zstd"),
        use_dictionary=settings.get("use_dictionary", False),
        write_statistics=settings.get("write_statistics", True),
        data_page_size=settings.get("data_page_size"),
        write_page_index=settings.get("write_page_index", True),
        max_rows_per_page=settings.get("max_rows_per_page"),
    )
    pending: list[pa.RecordBatch] = []
    pending_rows = 0
    try:
        for batch in parquet.iter_batches(batch_size=min(batch_size, effective)):
            offset = 0
            while offset < batch.num_rows:
                take = min(effective - pending_rows, batch.num_rows - offset)
                pending.append(batch.slice(offset, take))
                pending_rows += take
                offset += take
                if pending_rows == effective:
                    writer.write_table(pa.Table.from_batches(pending, schema=schema))
                    pending = []
                    pending_rows = 0
        if pending:
            writer.write_table(pa.Table.from_batches(pending, schema=schema))
    finally:
        writer.close()
    os.replace(temporary, target)
    after = pq.ParquetFile(target)
    if after.schema_arrow != schema:
        raise LayoutMigrationError(
            f"Schema changed while rewriting {relative_path}",
            code="layout_migration.schema_mismatch",
        )
    if after.metadata.num_rows != parquet.metadata.num_rows:
        raise LayoutMigrationError(
            f"File row count changed while rewriting {relative_path}",
            code="layout_migration.file_boundary_changed",
        )
    return {
        "relative_path": relative_path,
        "source_sha256": file_sha256(source),
        "destination_sha256": file_sha256(target),
        "rows": int(after.metadata.num_rows),
        "row_groups_before": int(parquet.metadata.num_row_groups),
        "row_groups_after": int(after.metadata.num_row_groups),
        "bytes_before": source.stat().st_size,
        "bytes_after": target.stat().st_size,
        "effective_row_group_rows": effective,
        "effective_writer_options": {
            **settings,
            "row_group_size": effective,
        },
        "compression_before": _compression_codecs(parquet),
        "compression_after": _compression_codecs(after),
        "peak_rss_mb": peak_rss_mb(),
        "cpu_sec": time.process_time() - cpu_started,
        "process_io": _io_delta(io_started, process_io_bytes()),
        "worker_pid": os.getpid(),
        "elapsed_sec": time.perf_counter() - started,
    }


def _rewrite_part_worker(
    args: tuple[Path, Path, str, dict[str, Any], int]
) -> dict[str, Any]:
    source, target, relative_path, settings, batch_size = args
    return _rewrite_part(
        source,
        target,
        relative_path=relative_path,
        settings=settings,
        batch_size=batch_size,
    )


def _collect_rewrite_batch(
    pending: list[tuple[dict[str, Any], Any]],
    *,
    resume: dict[str, Any],
    resume_path: Path,
    receipts: list[dict[str, Any]],
) -> None:
    for part, future in pending:
        receipt = future.result()
        relative = str(part["relative_path"])
        resume.setdefault("parts", {})[relative] = receipt
        _write_json_atomic(resume_path, resume)
        receipts.append(receipt)


def _refresh_physical_probe(
    dataset: Path, *, recommendation: dict[str, Any]
) -> dict[str, Any]:
    upstream = recommendation.get("upstream") or {}
    source_name = str(upstream.get("job_name") or dataset.name)
    definition = dataset / "_smoking_data" / "definition.yaml"
    project_root = _common_parent(dataset, Path(str(upstream.get("yaml_path") or definition)))
    source = SourceSpec(
        name=source_name,
        kind="parquet_dataset",
        paths=(str(dataset),),
        union_by_name=True,
        missing_columns="insert_null",
        incompatible_dtypes="error",
        asset_definition=str(definition),
        asset_definition_hash=file_sha256(definition) if definition.is_file() else None,
        asset_code="0101",
    )
    try:
        handle = ensure_source_probe(
            source_name=source_name,
            source=source,
            config=load_config(project_root=project_root, asset_code="0101"),
        )
    except Exception as exc:  # noqa: BLE001 - match the 0101 post-publish contract.
        return {
            "ok": False,
            "error_type": type(exc).__name__,
            "error_code": getattr(exc, "code", "layout_migration.probe_refresh_failed"),
            "error_message": str(exc),
        }
    return {"ok": True, **handle.to_dict()}


def _common_parent(left: Path, right: Path) -> Path:
    try:
        return Path(os.path.commonpath((left.resolve(), right.resolve())))
    except ValueError:
        return left.parent.resolve()


def _compression_codecs(parquet: pq.ParquetFile) -> list[str]:
    metadata = parquet.metadata
    return sorted(
        {
            str(metadata.row_group(row_group).column(column).compression)
            for row_group in range(metadata.num_row_groups)
            for column in range(metadata.row_group(row_group).num_columns)
        }
    )


def _io_delta(
    before: tuple[int, int] | None, after: tuple[int, int] | None
) -> dict[str, int] | None:
    if before is None or after is None:
        return None
    return {
        "read_bytes": max(0, after[0] - before[0]),
        "write_bytes": max(0, after[1] - before[1]),
    }


def _sum_process_io(
    parent: dict[str, int] | None, parts: list[dict[str, Any]]
) -> dict[str, int] | None:
    records = [
        item
        for item in [parent, *(part.get("process_io") for part in parts)]
        if isinstance(item, dict)
    ]
    if not records:
        return None
    return {
        "read_bytes": sum(int(item.get("read_bytes") or 0) for item in records),
        "write_bytes": sum(int(item.get("write_bytes") or 0) for item in records),
    }


def _copy_provenance(dataset: Path, staging: Path, *, migration_id: str) -> None:
    source_root = dataset / "_smoking_data"
    target_root = staging / "_smoking_data"
    target_root.mkdir(parents=True, exist_ok=True)
    for path in source_root.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(source_root)
        if relative.parts and relative.parts[0] in {"physical_probe", "migrations"}:
            continue
        target = target_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    migration_root = target_root / "migrations" / migration_id
    migration_root.mkdir(parents=True, exist_ok=True)
    metadata = source_root / "metadata.json"
    if metadata.is_file():
        shutil.copy2(metadata, migration_root / "before-metadata.json")


def _update_migrated_metadata(staging: Path, receipt: dict[str, Any]) -> None:
    path = staging / "_smoking_data" / "metadata.json"
    payload = _read_json(path) or {}
    payload["source_created_at"] = payload.get("created_at")
    payload["migrated_at"] = receipt["migrated_at"]
    payload["migration_id"] = receipt["migration_id"]
    payload["physical_layout_migration"] = {
        "receipt": (
            f"_smoking_data/migrations/{receipt['migration_id']}/receipt.json"
        ),
        "before": receipt["before"],
        "after": receipt["after"],
    }
    result = payload.get("result")
    if isinstance(result, dict):
        result["raw_dataset_fingerprint"] = None
        result["raw_dataset_fingerprint_status"] = "invalidated_by_layout_migration"
    _write_json_atomic(path, payload)


def _write_dataset_manifest(staging: Path, *, migration_id: str) -> None:
    parts = []
    rows = 0
    for path in sorted(
        item
        for item in staging.rglob("*.parquet")
        if item.is_file() and "_smoking_data" not in item.relative_to(staging).parts
    ):
        parquet = pq.ParquetFile(path)
        count = int(parquet.metadata.num_rows)
        rows += count
        parts.append(
            {
                "relative_path": path.relative_to(staging).as_posix(),
                "rows": count,
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
                "schema": str(parquet.schema_arrow),
            }
        )
    provenance = [
        {
            "relative_path": path.relative_to(staging).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
        for path in sorted((staging / "_smoking_data").rglob("*"))
        if path.is_file()
    ]
    _write_json_atomic(
        staging / "_dataset.manifest.json",
        {
            "version": DATASET_MANIFEST_VERSION,
            "transaction_id": migration_id,
            "rows": rows,
            "parts": parts,
            "provenance": provenance,
            "context": {"asset_code": "0101", "change_reason": "physical_layout_migration"},
        },
    )


def _definition_writer_settings(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return {}
    return dict((((payload.get("output") or {}).get("artifact") or {}).get("parquet_writer") or {}))


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _block(reasons: list[dict[str, Any]], code: str, message: str, **context: Any) -> None:
    reasons.append({"code": code, "message": message, "context": context})


def _resolve(value: str | Path, root: Path) -> Path:
    path = Path(value).expanduser()
    return (root / path).resolve() if not path.is_absolute() else path.resolve()
