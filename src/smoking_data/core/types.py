from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DatasetFile:
    path: Path
    size_bytes: int
    modified_ns: int


@dataclass(frozen=True, slots=True)
class WriteResult:
    output_dir: Path
    output_files: list[Path]
    rows: int
    partitions: int
