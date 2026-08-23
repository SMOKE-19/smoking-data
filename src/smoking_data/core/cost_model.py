from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

UNKNOWN_RISK = "unknown"


@dataclass(frozen=True, slots=True)
class GroupCost:
    key: tuple[Any, ...]
    rows: int
    payload_bytes: int | None
    state_bytes: int | None
    source_files: frozenset[str] = frozenset()

    @property
    def risk(self) -> str:
        return (
            "bounded"
            if self.payload_bytes is not None and self.state_bytes is not None
            else UNKNOWN_RISK
        )


@dataclass(frozen=True, slots=True)
class TaskCost:
    input_rows: int
    output_rows: int | None
    payload_bytes: int | None
    state_bytes: int | None
    source_files: int
    small_file_penalty: int
    risk: str


def estimate_hash_state_bytes(
    *, rows: int, key_width_bytes: int | None, payload_width_bytes: int | None
) -> int | None:
    if key_width_bytes is None or payload_width_bytes is None:
        return None
    # Hash table control bytes, key/value payload and allocator slack.
    return max(0, int(rows)) * (24 + max(1, key_width_bytes) + max(1, payload_width_bytes))


def estimate_window_state_bytes(
    *, rows: int, row_width_bytes: int | None, bounded_frame_rows: int | None = None
) -> int | None:
    if row_width_bytes is None:
        return None
    retained_rows = min(rows, bounded_frame_rows) if bounded_frame_rows else rows
    return max(0, int(retained_rows)) * (16 + max(1, row_width_bytes))


def estimate_join_state_bytes(
    *, right_rows: int | None, right_row_width_bytes: int | None, distinct_keys: int | None
) -> int | None:
    if right_rows is None or right_row_width_bytes is None or distinct_keys is None:
        return None
    return max(0, right_rows) * (16 + max(1, right_row_width_bytes)) + max(0, distinct_keys) * 24


def pack_group_costs(
    groups: Iterable[GroupCost],
    *,
    max_rows: int,
    max_bytes: int,
    max_source_files: int,
    max_groups: int | None = None,
) -> list[list[GroupCost]]:
    """Pack complete groups without splitting any group across task boundaries."""
    packed: list[list[GroupCost]] = []
    current: list[GroupCost] = []
    current_rows = 0
    current_bytes = 0
    current_files: set[str] = set()
    for group in groups:
        group_bytes = group.payload_bytes if group.payload_bytes is not None else max_bytes
        candidate_files = current_files | set(group.source_files)
        exceeds = bool(current) and (
            current_rows + group.rows > max_rows
            or current_bytes + group_bytes > max_bytes
            or len(candidate_files) > max_source_files
            or (max_groups is not None and len(current) >= max_groups)
        )
        if exceeds:
            packed.append(current)
            current = []
            current_rows = 0
            current_bytes = 0
            current_files = set()
        current.append(group)
        current_rows += group.rows
        current_bytes += group_bytes
        current_files.update(group.source_files)
    if current:
        packed.append(current)
    return packed


def summarize_task_cost(groups: Iterable[GroupCost], *, target_rows: int) -> TaskCost:
    items = list(groups)
    payloads = [item.payload_bytes for item in items]
    states = [item.state_bytes for item in items]
    rows = sum(item.rows for item in items)
    files = set().union(*(item.source_files for item in items)) if items else set()
    known = all(value is not None for value in [*payloads, *states])
    return TaskCost(
        input_rows=rows,
        output_rows=None,
        payload_bytes=sum(int(value) for value in payloads)
        if all(value is not None for value in payloads)
        else None,
        state_bytes=sum(int(value) for value in states)
        if all(value is not None for value in states)
        else None,
        source_files=len(files),
        small_file_penalty=1 if 0 < rows < max(1, target_rows // 4) else 0,
        risk="bounded" if known else UNKNOWN_RISK,
    )
