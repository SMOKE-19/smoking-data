from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DatasetFile:
    path: Path
    size_bytes: int
    modified_ns: int
    dataset_root: Path | None = None
    dataset_id: str | None = None
    source_kind: str = "local"
    file_id: str | None = None
    relative_path: str | None = None
    content_sha256: str | None = None
    object_key: str | None = None


@dataclass(frozen=True, slots=True)
class WriteResult:
    output_dir: Path
    output_files: list[Path]
    rows: int
    partitions: int
