from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl

from smoking_data.core.types import DatasetFile

PARQUET_GLOB = "*.parquet"
_NON_DATASET_PARTS = frozenset({"_smoking_data", ".temp"})
_DATASET_BOUNDARY_MARKER = ".smoking-data-dataset-boundary.json"


def discover_parquet_files(paths: list[str | Path], *, recursive: bool = True) -> list[DatasetFile]:
    files: dict[Path, tuple[Path, str]] = {}
    for raw in paths:
        path = Path(raw).expanduser().resolve()
        if path.is_file() and path.suffix.lower() == ".parquet":
            dataset_root = _nearest_dataset_root(path, boundary=path.parent)
            files[path] = (dataset_root, _dataset_identity(dataset_root))
        elif path.is_dir():
            iterator = path.rglob(PARQUET_GLOB) if recursive else path.glob(PARQUET_GLOB)
            for item in iterator:
                if not item.is_file() or _NON_DATASET_PARTS.intersection(
                    item.relative_to(path).parts
                ):
                    continue
                resolved = item.resolve()
                dataset_root = _nearest_dataset_root(resolved, boundary=path)
                previous = files.get(resolved)
                if previous is None or len(dataset_root.parts) > len(previous[0].parts):
                    files[resolved] = (dataset_root, _dataset_identity(dataset_root))
    result: list[DatasetFile] = []
    for file_path in sorted(files):
        stat = file_path.stat()
        dataset_root, dataset_id = files[file_path]
        result.append(
            DatasetFile(
                path=file_path,
                size_bytes=stat.st_size,
                modified_ns=stat.st_mtime_ns,
                dataset_root=dataset_root,
                dataset_id=dataset_id,
            )
        )
    return result


def _nearest_dataset_root(path: Path, *, boundary: Path) -> Path:
    resolved_boundary = boundary.resolve()
    current = path.parent.resolve()
    while current == resolved_boundary or resolved_boundary in current.parents:
        if (current / _DATASET_BOUNDARY_MARKER).is_file() or (
            current / "_dataset.manifest.json"
        ).is_file():
            return current
        if current == resolved_boundary:
            break
        current = current.parent
    return resolved_boundary


def _dataset_identity(root: Path) -> str:
    boundary_path = root / _DATASET_BOUNDARY_MARKER
    if boundary_path.is_file():
        try:
            boundary = json.loads(boundary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            boundary = {}
        dataset_shard_id = str(boundary.get("dataset_shard_id") or "").strip()
        if dataset_shard_id:
            return dataset_shard_id
    manifest_path = root / "_dataset.manifest.json"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            manifest = {}
        generation_id = str(manifest.get("generation_id") or "").strip()
        if generation_id:
            path_digest = hashlib.sha256(root.as_posix().encode("utf-8")).hexdigest()[:12]
            return f"generation:{root.name}:{generation_id}:{path_digest}"
    digest = hashlib.sha256(root.as_posix().encode("utf-8")).hexdigest()[:16]
    return f"path:{root.name}:{digest}"


def scan_parquet_files(files: list[DatasetFile]) -> pl.LazyFrame:
    if not files:
        raise ValueError("No parquet files discovered.")
    return pl.scan_parquet([str(item.path) for item in files])


def scan_parquet_files_union_by_name(files: list[DatasetFile]) -> pl.LazyFrame:
    if not files:
        raise ValueError("No parquet files discovered.")
    union_schema: dict[str, pl.DataType] = {}
    for item in files:
        schema = pl.read_parquet_schema(item.path)
        for name, dtype in schema.items():
            existing = union_schema.get(name)
            if existing is not None and existing != dtype:
                raise ValueError(
                    "Incompatible parquet schema drift for "
                    f"{name!r}: {existing} vs {dtype} ({item.path})"
                )
            union_schema.setdefault(name, dtype)
    return pl.scan_parquet(
        [str(item.path) for item in files],
        schema=union_schema,
        missing_columns="insert",
        extra_columns="ignore",
        low_memory=True,
    )
