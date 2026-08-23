from __future__ import annotations

import re

NAMING_POLICY_VERSION = "smoking-data.naming.v1"


def partition_dir_name(value: object) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(value).strip())
    return cleaned or "__null__"


def part_file_name(part_index: int, *, suffix: str = ".parquet") -> str:
    if part_index < 0:
        raise ValueError("part_index must be >= 0")
    return f"part-{part_index:05d}{suffix}"


def task_id(partition_value: object, part_index: int, *, batch_index: int | None = None) -> str:
    pieces = [partition_dir_name(partition_value)]
    if batch_index is not None:
        pieces.append(f"batch-{batch_index:05d}")
    pieces.append(f"part-{part_index:05d}")
    return "__".join(pieces)
