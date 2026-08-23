"""0101 staging, atomic dataset publication, and footer fingerprinting."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow.parquet as pq

from smoking_data.runtime.change_receipt import manifest_generation_id
from smoking_data.runtime.paths import file_sha256
from smoking_data.runtime.transactions import DATASET_MANIFEST_VERSION

from .models import SourceSpec
from .task import SourceTask


@dataclass(slots=True)
class SourcePathSet:
    raw_json_path: Path
    staging_root_path: Path
    staging_dataset_path: Path


def build_source_paths(spec: SourceSpec, task: SourceTask) -> SourcePathSet:
    raw_output_dir = Path(spec.storage.raw_dir)
    raw_dataset_dir = raw_output_dir / f"{task.file_stem}.dataset"
    staging_root = raw_output_dir / ".temp"
    staging_dataset_dir = (
        staging_root
        / "source_0101"
        / "staging"
        / spec.job.name
        / f"{task.file_stem}.{uuid4().hex}.dataset"
    )
    return SourcePathSet(
        raw_json_path=raw_dataset_dir,
        staging_root_path=staging_root,
        staging_dataset_path=staging_dataset_dir,
    )


def reset_staging_dataset(path: str | Path) -> None:
    staging_path = Path(path)
    if staging_path.exists():
        shutil.rmtree(staging_path)


def cleanup_staging_dataset(path: str | Path, *, staging_root: str | Path) -> None:
    staging_path = Path(path)
    reset_staging_dataset(staging_path)
    cleanup_boundary = Path(staging_root).parent
    current = staging_path.parent
    while current != cleanup_boundary:
        try:
            current.rmdir()
        except (FileNotFoundError, OSError):
            break
        current = current.parent


def commit_staged_dataset(staging_path: str | Path, output_path: str | Path) -> Path:
    staging = Path(staging_path)
    output = Path(output_path)
    if not dataset_has_parquet_files(staging):
        raise ValueError(f"SOURCE staging dataset has no parquet parts: {staging}")
    output.parent.mkdir(parents=True, exist_ok=True)
    if staging.stat().st_dev != output.parent.stat().st_dev:
        raise OSError(
            "SOURCE staging and output must share a filesystem for atomic publication: "
            f"staging={staging}, output={output}"
        )

    backup = staging.parent / f"{output.name}.previous.{uuid4().hex}"
    moved_existing = False
    try:
        if output.exists():
            os.replace(output, backup)
            moved_existing = True
        os.replace(staging, output)
    except BaseException:
        if moved_existing and backup.exists() and not output.exists():
            os.replace(backup, output)
        raise
    else:
        if backup.exists():
            shutil.rmtree(backup)
    return output


def write_source_dataset_manifest(dataset_dir: str | Path) -> Path:
    root = Path(dataset_dir)
    parts = []
    total_rows = 0
    for path in _iter_dataset_part_files(root):
        parquet = pq.ParquetFile(path)
        rows = int(parquet.metadata.num_rows)
        total_rows += rows
        parts.append(
            {
                "relative_path": path.relative_to(root).as_posix(),
                "rows": rows,
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
                "schema": str(parquet.schema_arrow),
            }
        )
    provenance_root = root / "_smoking_data"
    provenance = [
        {
            "relative_path": path.relative_to(root).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
        for path in sorted(provenance_root.rglob("*"))
        if path.is_file()
    ]
    manifest_path = root / "_dataset.manifest.json"
    manifest_context = {"asset_code": "0101"}
    generation_id = manifest_generation_id(parts, manifest_context=manifest_context)
    manifest_path.write_text(
        json.dumps(
            {
                "version": DATASET_MANIFEST_VERSION,
                "transaction_id": "source-0101",
                "generation_id": generation_id,
                "parent_generation_id": None,
                "rows": total_rows,
                "parts": parts,
                "provenance": provenance,
                "context": manifest_context,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return manifest_path


def normalize_dataset_part_names(
    dataset_dir: str | Path,
    *,
    sql_revision: str,
) -> list[Path]:
    resolved_dir = Path(dataset_dir)
    parts = _iter_dataset_part_files(resolved_dir)
    if not parts:
        raise ValueError(f"SOURCE dataset has no parquet parts: {resolved_dir}")
    revision = sql_revision.strip().lower()
    if not revision or any(character not in "0123456789abcdef" for character in revision):
        raise ValueError(f"SOURCE sql_revision must be hexadecimal: {sql_revision!r}")

    temporary_parts: list[Path] = []
    for part in parts:
        temporary = resolved_dir / f".rename.{uuid4().hex}.parquet"
        os.replace(part, temporary)
        temporary_parts.append(temporary)
    normalized: list[Path] = []
    for sequence, temporary in enumerate(temporary_parts, start=1):
        target = resolved_dir / f"data_{sequence:04d}_{revision}.parquet"
        os.replace(temporary, target)
        normalized.append(target)
    return normalized


def dataset_footer_fingerprint(path: str | Path) -> str:
    dataset_dir = Path(path)
    parts = _iter_dataset_part_files(dataset_dir)
    if not parts:
        raise ValueError(f"SOURCE dataset has no parquet parts: {dataset_dir}")

    part_profiles: list[dict[str, Any]] = []
    for part in parts:
        metadata = pq.ParquetFile(part).metadata
        row_groups: list[dict[str, Any]] = []
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            row_groups.append(
                {
                    "rows": row_group.num_rows,
                    "total_bytes": row_group.total_byte_size,
                    "columns": [
                        {
                            "path": row_group.column(column_index).path_in_schema,
                            "physical_type": row_group.column(column_index).physical_type,
                            "compression": row_group.column(column_index).compression,
                            "encodings": list(row_group.column(column_index).encodings),
                            "num_values": row_group.column(column_index).num_values,
                            "compressed_bytes": row_group.column(column_index).total_compressed_size,
                            "uncompressed_bytes": row_group.column(column_index).total_uncompressed_size,
                        }
                        for column_index in range(row_group.num_columns)
                    ],
                }
            )
        part_profiles.append(
            {
                "path": part.relative_to(dataset_dir).as_posix(),
                "file_bytes": part.stat().st_size,
                "modified_ns": part.stat().st_mtime_ns,
                "rows": metadata.num_rows,
                "created_by": metadata.created_by,
                "schema": metadata.schema.to_arrow_schema().to_string(
                    truncate_metadata=False,
                    show_field_metadata=True,
                    show_schema_metadata=True,
                    element_size_limit=1_000_000,
                ),
                "row_groups": row_groups,
            }
        )
    canonical = json.dumps(
        {"version": "parquet-footer-v1", "parts": part_profiles},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"parquet-footer-v1:{hashlib.sha256(canonical).hexdigest()}"


def profile_source_written_dataset_result(
    spec: SourceSpec,
    dataset_dir: str | Path,
    payload: Any,
) -> dict[str, Any]:
    artifacts = _require_api_written_artifacts(payload)
    resolved_dir = Path(dataset_dir).resolve()
    if not resolved_dir.is_dir():
        raise ValueError(f"SOURCE backend did not create dataset directory: {resolved_dir}")

    files: list[Path] = []
    total_rows = 0
    total_bytes = 0
    for raw_path, declared_rows in artifacts:
        path = Path(raw_path).expanduser()
        file_path = path.resolve() if path.is_absolute() else (resolved_dir / path).resolve()
        try:
            file_path.relative_to(resolved_dir)
        except ValueError as exc:
            raise ValueError(f"SOURCE backend reported a file outside staging: {file_path}") from exc
        if not file_path.is_file() or file_path.suffix.lower() != ".parquet":
            raise ValueError(f"SOURCE backend reported an invalid parquet file: {file_path}")
        actual_rows = int(pq.ParquetFile(file_path).metadata.num_rows)
        if declared_rows != actual_rows:
            raise ValueError(
                "SOURCE backend row count does not match parquet footer: "
                f"file={file_path}, declared={declared_rows}, actual={actual_rows}"
            )
        files.append(file_path)
        total_rows += actual_rows
        total_bytes += file_path.stat().st_size
    if not files or total_rows <= 0:
        raise ValueError("SOURCE backend must report at least one non-empty parquet file.")

    profile: dict[str, Any] = {
        "convert_sec": 0.0,
        "convert_engine": "backend_declared_dataset",
        "source_yaml_path": str(spec.path),
        "chunks_written": len(files),
        "rows_written": total_rows,
        "bytes_written": total_bytes,
        "dataset_dir": str(resolved_dir),
        "api_written_files": [str(path) for path in files],
    }
    return profile


def dataset_has_parquet_files(dataset_dir: str | Path) -> bool:
    return bool(_iter_dataset_part_files(Path(dataset_dir)))


def _iter_dataset_part_files(dataset_dir: Path) -> list[Path]:
    return sorted(path for path in dataset_dir.glob("*.parquet") if path.is_file())


def _require_api_written_artifacts(payload: Any) -> list[tuple[str, int]]:
    if not isinstance(payload, (list, tuple)):
        raise TypeError("SOURCE backend must return a list of (path, row_count) pairs.")
    artifacts: list[tuple[str, int]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise TypeError(f"SOURCE backend manifest item {index} must be a two-item pair.")
        raw_path, raw_rows = item
        if not isinstance(raw_path, (str, Path)):
            raise TypeError(f"SOURCE backend manifest path {index} must be text or Path.")
        if isinstance(raw_rows, bool) or not isinstance(raw_rows, int) or raw_rows < 0:
            raise TypeError(f"SOURCE backend manifest row_count {index} must be a non-negative int.")
        artifacts.append((str(raw_path), raw_rows))
    return artifacts
