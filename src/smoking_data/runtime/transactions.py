from __future__ import annotations

import errno
import json
import os
import shutil
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from smoking_data.core.results import to_json_safe
from smoking_data.runtime.change_receipt import (
    CHANGE_RECEIPT_RELATIVE_PATH,
    build_dataset_change_receipt,
    normalize_manifest_parts,
    read_dataset_change_receipt,
    validate_dataset_change_receipt,
    write_dataset_change_receipt,
)
from smoking_data.runtime.paths import ensure_dir, file_sha256, reset_path

DATASET_MANIFEST_VERSION = "smoking-data.dataset-manifest.v1"


@dataclass(slots=True)
class DatasetTransaction:
    final_root: Path
    staging_root: Path
    backup_root: Path
    transaction_id: str
    manifest_context: dict[str, Any] | None = None

    @classmethod
    def create(
        cls,
        final_root: Path,
        *,
        manifest_context: dict[str, Any] | None = None,
    ) -> DatasetTransaction:
        final_root = final_root.resolve()
        ensure_dir(final_root.parent)
        transaction_id = uuid.uuid4().hex
        staging_root = final_root.parent / f".{final_root.name}.txn-{transaction_id}"
        backup_root = final_root.parent / f".{final_root.name}.backup-{transaction_id}"
        reset_path(staging_root)
        ensure_dir(staging_root)
        return cls(
            final_root,
            staging_root,
            backup_root,
            transaction_id,
            manifest_context,
        )

    def commit(self) -> tuple[list[Path], dict[str, Any]]:
        previous_manifest = _read_manifest(self.final_root)
        manifest, manifest_profile = self._validate_and_write_manifest(previous_manifest)
        moved_existing = False
        commit_mode = "same_filesystem_atomic_replace"
        try:
            if self.final_root.exists():
                os.replace(self.final_root, self.backup_root)
                moved_existing = True
            try:
                os.replace(self.staging_root, self.final_root)
            except OSError as exc:
                if exc.errno != errno.EXDEV:
                    raise
                commit_mode = "cross_filesystem_copy_verify_swap"
                _copy_verify_swap(self.staging_root, self.final_root, manifest)
        except BaseException:
            if moved_existing and self.backup_root.exists() and not self.final_root.exists():
                os.replace(self.backup_root, self.final_root)
            raise
        reset_path(self.backup_root)
        output_paths = [self.final_root / str(item["relative_path"]) for item in manifest["parts"]]
        return output_paths, {
            "transaction_id": self.transaction_id,
            "manifest_path": str(self.final_root / "_dataset.manifest.json"),
            "parts": len(output_paths),
            "rows": manifest["rows"],
            "checksum_verified": True,
            "commit_mode": commit_mode,
            "stale_parts_removed": _stale_parts(previous_manifest, manifest),
            "change_reason": (self.manifest_context or {}).get("change_reason"),
            **manifest_profile,
        }

    def abort(self) -> None:
        reset_path(self.staging_root)
        if self.backup_root.exists() and not self.final_root.exists():
            os.replace(self.backup_root, self.final_root)
        else:
            reset_path(self.backup_root)

    def _validate_and_write_manifest(
        self, previous_manifest: dict[str, Any] | None
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        parts: list[dict[str, Any]] = []
        total_rows = 0
        paths = [
            path
            for path in sorted(self.staging_root.rglob("*.parquet"))
            if path.is_file()
        ]
        checksum_workers = min(4, len(paths))
        checksum_started = time.perf_counter()
        if checksum_workers > 1:
            with ThreadPoolExecutor(max_workers=checksum_workers) as executor:
                checksums = dict(zip(paths, executor.map(file_sha256, paths), strict=True))
        else:
            checksums = {path: file_sha256(path) for path in paths}
        checksum_sec = time.perf_counter() - checksum_started
        metadata_started = time.perf_counter()
        for path in paths:
            if not path.is_file() or path.stat().st_size == 0:
                raise RuntimeError(f"Dataset transaction contains an invalid part: {path}")
            parquet = pq.ParquetFile(path)
            rows = int(parquet.metadata.num_rows)
            total_rows += rows
            parts.append(
                {
                    "relative_path": path.relative_to(self.staging_root).as_posix(),
                    "rows": rows,
                    "size_bytes": path.stat().st_size,
                    "sha256": checksums[path],
                    "schema": str(parquet.schema_arrow),
                }
            )
        parquet_metadata_sec = time.perf_counter() - metadata_started
        receipt_started = time.perf_counter()
        parts = normalize_manifest_parts({"parts": parts})
        receipt, change_counts = build_dataset_change_receipt(
            previous_manifest=previous_manifest,
            current_parts=parts,
            manifest_context=self.manifest_context,
        )
        write_dataset_change_receipt(self.staging_root, receipt)
        receipt_build_sec = time.perf_counter() - receipt_started
        manifest = {
            "version": DATASET_MANIFEST_VERSION,
            "transaction_id": self.transaction_id,
            "generation_id": receipt["generation_id"],
            "parent_generation_id": receipt["parent_generation_id"],
            "change_receipt": CHANGE_RECEIPT_RELATIVE_PATH.as_posix(),
            "rows": total_rows,
            "parts": parts,
            "context": to_json_safe(self.manifest_context or {}),
        }
        manifest_path = self.staging_root / "_dataset.manifest.json"
        manifest_write_started = time.perf_counter()
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return manifest, {
            "checksum_workers": checksum_workers,
            "checksum_sec": checksum_sec,
            "parquet_metadata_sec": parquet_metadata_sec,
            "manifest_write_sec": time.perf_counter() - manifest_write_started,
            "change_receipt_build_sec": receipt_build_sec,
            "change_receipt_counts": change_counts,
        }


def recover_orphan_transactions(final_root: Path) -> dict[str, int]:
    final_root = final_root.resolve()
    parent = final_root.parent
    removed_staging = 0
    restored_backup = 0
    for staging in parent.glob(f".{final_root.name}.txn-*"):
        reset_path(staging)
        removed_staging += 1
    backups = sorted(parent.glob(f".{final_root.name}.backup-*"))
    if not final_root.exists() and backups:
        os.replace(backups[-1], final_root)
        restored_backup += 1
        backups = backups[:-1]
    for backup in backups:
        shutil.rmtree(backup)
    return {
        "removed_staging": removed_staging,
        "restored_backup": restored_backup,
    }


def validate_committed_dataset(final_root: Path) -> bool:
    manifest_path = final_root / "_dataset.manifest.json"
    if not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(manifest, dict) or manifest.get("version") != DATASET_MANIFEST_VERSION:
        return False
    if manifest.get("change_receipt"):
        receipt = read_dataset_change_receipt(final_root)
        if receipt is None or not validate_dataset_change_receipt(
            receipt, manifest=manifest
        ):
            return False
    parts = manifest.get("parts")
    if not isinstance(parts, list):
        return False
    for item in parts:
        if not isinstance(item, dict) or not item.get("relative_path"):
            return False
        path = final_root / str(item["relative_path"])
        if (
            not path.is_file()
            or path.stat().st_size == 0
            or path.stat().st_size != int(item.get("size_bytes") or -1)
            or file_sha256(path) != item.get("sha256")
        ):
            return False
    provenance = manifest.get("provenance")
    if not isinstance(provenance, list) or not provenance:
        return False
    for item in provenance:
        if not isinstance(item, dict) or not item.get("relative_path"):
            return False
        path = final_root / str(item["relative_path"])
        if (
            not path.is_file()
            or path.stat().st_size != int(item.get("size_bytes") or -1)
            or file_sha256(path) != item.get("sha256")
        ):
            return False
    if "_smoking_data/metadata.json" not in {
        str(item.get("relative_path")) for item in provenance
    }:
        return False
    return True


def refresh_dataset_manifest_provenance(final_root: Path) -> None:
    manifest_path = final_root / "_dataset.manifest.json"
    provenance_root = final_root / "_smoking_data"
    if not manifest_path.is_file() or not provenance_root.is_dir():
        return
    manifest = _read_manifest(final_root)
    if manifest is None:
        return
    manifest["provenance"] = [
        {
            "relative_path": path.relative_to(final_root).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
        for path in sorted(provenance_root.rglob("*"))
        if path.is_file()
    ]
    staging = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
    staging.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    staging.replace(manifest_path)


def _read_manifest(root: Path) -> dict[str, Any] | None:
    path = root / "_dataset.manifest.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _stale_parts(previous: dict[str, Any] | None, current: dict[str, Any]) -> list[str]:
    previous_paths = {
        str(item.get("relative_path"))
        for item in (previous or {}).get("parts", [])
        if isinstance(item, dict) and item.get("relative_path")
    }
    current_paths = {
        str(item.get("relative_path"))
        for item in current.get("parts", [])
        if isinstance(item, dict) and item.get("relative_path")
    }
    return sorted(previous_paths - current_paths)


def _copy_verify_swap(source: Path, final_root: Path, manifest: dict[str, Any]) -> None:
    swap = final_root.parent / f".{final_root.name}.copy-{uuid.uuid4().hex}"
    reset_path(swap)
    try:
        shutil.copytree(source, swap, copy_function=shutil.copy2)
        for item in manifest["parts"]:
            copied = swap / str(item["relative_path"])
            if not copied.is_file() or file_sha256(copied) != item["sha256"]:
                raise RuntimeError(f"Cross-filesystem copy verification failed: {copied}")
        os.replace(swap, final_root)
        reset_path(source)
    except BaseException:
        reset_path(swap)
        raise
