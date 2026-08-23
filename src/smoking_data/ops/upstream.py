from __future__ import annotations

from pathlib import Path

import polars as pl

from smoking_data.core.types import DatasetFile

PARQUET_GLOB = "*.parquet"
_NON_DATASET_PARTS = frozenset({"_smoking_data", ".temp"})


def discover_parquet_files(paths: list[str | Path], *, recursive: bool = True) -> list[DatasetFile]:
    files: list[Path] = []
    for raw in paths:
        path = Path(raw).expanduser()
        if path.is_file() and path.suffix.lower() == ".parquet":
            files.append(path)
        elif path.is_dir():
            iterator = path.rglob(PARQUET_GLOB) if recursive else path.glob(PARQUET_GLOB)
            files.extend(
                item
                for item in iterator
                if item.is_file()
                and not _NON_DATASET_PARTS.intersection(item.relative_to(path).parts)
            )
    result: list[DatasetFile] = []
    for file_path in sorted({item.resolve() for item in files}):
        stat = file_path.stat()
        result.append(
            DatasetFile(
                path=file_path,
                size_bytes=stat.st_size,
                modified_ns=stat.st_mtime_ns,
            )
        )
    return result


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
