from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

_WINDOW_PATTERN = re.compile(
    r"(?P<date_from>\d{8}T\d{6})-(?P<date_to>\d{8}T\d{6})"
)
UNASSIGNED_PARTITION = "unassigned"


@dataclass(frozen=True, slots=True)
class ProbePartitionGrid:
    anchor_date: date
    step_days: int

    def __post_init__(self) -> None:
        if self.step_days < 1:
            raise ValueError("parquet probe partition_grid.step_days는 1 이상이어야 합니다.")

    def keys_for_source_file(self, source_file: str) -> tuple[str, ...]:
        window = source_window_from_path(source_file)
        if window is None:
            return (UNASSIGNED_PARTITION,)
        date_from, date_to = window
        first = self.bucket_start(date_from)
        last = self.bucket_start(date_to)
        keys: list[str] = []
        cursor = first
        while cursor <= last:
            keys.append(cursor.isoformat())
            cursor += timedelta(days=self.step_days)
        return tuple(keys)

    def bucket_start(self, value: date) -> date:
        offset_days = (value - self.anchor_date).days
        return self.anchor_date + timedelta(
            days=(offset_days // self.step_days) * self.step_days
        )


def source_window_from_path(source_file: str) -> tuple[date, date] | None:
    matches = list(_WINDOW_PATTERN.finditer(str(source_file)))
    if not matches:
        return None
    match = matches[-1]
    start_at = datetime.strptime(match.group("date_from"), "%Y%m%dT%H%M%S")
    end_at = datetime.strptime(match.group("date_to"), "%Y%m%dT%H%M%S")
    if start_at >= end_at:
        raise ValueError(f"0101 source window 시작일이 종료일보다 늦습니다: {source_file}")
    return start_at.date(), (end_at - timedelta(seconds=1)).date()


def partition_directory_name(partition_key: str) -> str:
    return f"partition_start={partition_key}"
