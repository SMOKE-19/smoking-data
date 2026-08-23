from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq


@dataclass(frozen=True, slots=True)
class PresetParityFixture:
    preset: str
    yaml_path: Path
    expected_rows: int
    expected_columns: list[str]


def write_0201_curated_parity_fixture(root: str | Path) -> PresetParityFixture:
    base = Path(root).expanduser().resolve()
    source_root = base / "DATA" / "0101" / "demo_source"
    source_dataset = source_root / "raw_demo__2025-01-01-2025-01-02.dataset"
    source_dataset_extra = source_root / "raw_demo__2025-01-02-2025-01-03.dataset"
    source_dataset.mkdir(parents=True, exist_ok=True)
    source_dataset_extra.mkdir(parents=True, exist_ok=True)
    _write_probe_ready_parquet(
        pl.DataFrame(
            {
                "id": ["A", "A", "B"],
                "part": ["KOR", "KOR", "USA"],
                "updated_at": [1, 3, 2],
                "amount": [10, 30, 20],
                "coords": [[1, 2], [3, 4], [5, 6]],
            }
        ),
        source_dataset / "part-000.parquet",
    )
    _write_probe_ready_parquet(
        pl.DataFrame(
            {
                "id": ["B", "C"],
                "part": ["USA", "KOR"],
                "updated_at": [5, 4],
                "amount": [50, 40],
                "coords": [[7, 8], [9, 10]],
            }
        ),
        source_dataset / "part-001.parquet",
    )
    _write_probe_ready_parquet(
        pl.DataFrame(
            {
                "id": ["D"],
                "part": ["KOR"],
                "updated_at": [6],
                "amount": [60],
                "coords": [[11, 12]],
            }
        ),
        source_dataset_extra / "part-000.parquet",
    )

    yaml_path = base / "parity_0201_curated.yaml"
    yaml_path.write_text(
        f"""
preset: "0201"
job:
  name: parity_0201_curated
source:
  upstream:
    paths:
    - {source_root.as_posix()}
  payload:
    type_casts:
    - name: updated_at
      type: INT32
    - name: amount
      type: INT32
    include_columns:
    - id
    - part
    - updated_at
    - amount
    - coords
row_selection:
  sort_first:
    enabled: true
    group_keys:
    - id
    - part
    sort:
    - column: updated_at
      direction: desc
output:
  output_dir: {base / "DATA" / "new_output" / "0201"!s}
  partition_column: part
execution:
  target_rows_per_part: 2
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return PresetParityFixture(
        preset="0201",
        yaml_path=yaml_path,
        expected_rows=4,
        expected_columns=["id", "part", "updated_at", "amount", "coords"],
    )


def _write_probe_ready_parquet(frame: pl.DataFrame, path: Path) -> None:
    pq.write_table(
        frame.to_arrow(),
        path,
        row_group_size=2,
        write_page_index=True,
        write_statistics=True,
        data_page_size=256,
        use_dictionary=False,
    )


def write_0301_join_parity_fixture(root: str | Path) -> PresetParityFixture:
    base = Path(root).expanduser().resolve()
    left_dir = base / "DATA" / "left_pivot"
    right_dir = base / "DATA" / "right_a"
    left_dir.mkdir(parents=True, exist_ok=True)
    right_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "biz_date": ["A", "A", "A"],
            "order_id": [1, 2, 3],
            "left_value": ["x1", "x2", "x3"],
        }
    ).write_parquet(left_dir / "A.left_pivot.parquet")
    pl.DataFrame(
        {
            "biz_date": ["B"],
            "order_id": [4],
            "left_value": ["y4"],
        }
    ).write_parquet(left_dir / "B.left_pivot.parquet")
    pl.DataFrame(
        {
            "biz_date_r": ["A", "A", "B"],
            "order_id_r": [1, 3, 4],
            "amount": [10, 30, 40],
            "country": ["KR", "US", "JP"],
        }
    ).write_parquet(right_dir / "A.right_a.parquet")
    pl.DataFrame(
        {
            "biz_date_r": ["B"],
            "order_id_r": [4],
            "amount": [40],
            "country": ["JP"],
        }
    ).write_parquet(right_dir / "B.right_a.parquet")

    yaml_path = base / "parity_0301_join.yaml"
    yaml_path.write_text(
        f"""
preset: "0301"
job:
  name: parity_0301_join
left:
  upstream:
    paths:
    - {left_dir.as_posix()}
right:
  upstream:
    paths:
    - {right_dir.as_posix()}
join:
  how: inner
  left_on:
  - biz_date
  - order_id
  right_on:
  - biz_date_r
  - order_id_r
execution:
  target_key_groups_per_part: 2
output:
  output_dir: {base / "DATA" / "new_output" / "0301"!s}
  partition_column: biz_date
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return PresetParityFixture(
        preset="0301",
        yaml_path=yaml_path,
        expected_rows=3,
        expected_columns=["biz_date", "order_id", "left_value", "amount", "country"],
    )


def write_0201_pivot_parity_fixture(root: str | Path) -> PresetParityFixture:
    base = Path(root).expanduser().resolve()
    source_dir = base / "DATA" / "parity" / "0201" / "input"
    source_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "partition": ["KOR", "KOR", "KOR", "KOR", "USA"],
            "row_key": ["A", "A", "A", "B", "C"],
            "metric": ["X", "X", "Y", "X", None],
            "amount": [1, 3, 2, 4, 5],
            "updated_at": [10, 12, 20, 30, 40],
        }
    ).write_parquet(source_dir / "part-000.parquet", row_group_size=2)

    yaml_path = base / "parity_0201_pivot.yaml"
    yaml_path.write_text(
        f"""
preset: "0201"
job:
  name: parity_0201_pivot
source:
  upstream:
    paths:
    - {source_dir.as_posix()}
  payload:
    include_columns: [partition, row_key, metric, amount, updated_at]
pivot:
  enabled: true
  row_keys: [partition, row_key]
  column_keys: [metric]
  value_keys:
  - name: amount_total
    source_column: amount
    aggregation: sum
    output_dtype: INT64
    column_name_rule: "<column_key_value>__<value_key_name>"
  value_keys_without_column:
  - name: latest_updated_at
    source_column: updated_at
    aggregation: max
    output_dtype: INT64
    column_name_rule: "<value_key_name>__<agg>"
  first_duplicate_policy: error
  null_column_key_policy: label
output:
  output_dir: {(base / "DATA" / "parity" / "0201" / "output").as_posix()}
  partition_column: partition
execution:
  target_rows_per_part: 2
  reset_before_run: true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return PresetParityFixture(
        preset="0201-pivot",
        yaml_path=yaml_path,
        expected_rows=3,
        expected_columns=[
            "partition",
            "row_key",
            "X__amount_total",
            "Y__amount_total",
            "__null____amount_total",
            "latest_updated_at__max",
        ],
    )


def write_0301_multi_right_full_parity_fixture(
    root: str | Path,
) -> PresetParityFixture:
    base = Path(root).expanduser().resolve()
    data_root = base / "DATA" / "parity" / "0301"
    left_dir = data_root / "left"
    profile_dir = data_root / "right_profile"
    category_dir = data_root / "right_category"
    for directory in (left_dir, profile_dir, category_dir):
        directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "partition": ["KOR", "KOR", "USA"],
            "join_key": ["A", "B", "D"],
            "left_value": [1, 2, 4],
        }
    ).write_parquet(left_dir / "part-000.parquet")
    pl.DataFrame(
        {
            "right_partition": ["KOR", "KOR", "USA", "EU"],
            "right_key": ["A", "C", "D", "E"],
            "profile_code": ["P1", "P3", "P4", "P5"],
            "amount": [10, 30, 40, 50],
            "unused": [101, 103, 104, 105],
        }
    ).write_parquet(profile_dir / "part-000.parquet")
    pl.DataFrame(
        {
            "right_partition": ["KOR", "KOR", "USA", "EU"],
            "code_key": ["P1", "P3", "P4", "P5"],
            "label": ["alpha", "gamma", "delta", "epsilon"],
            "debug": ["drop"] * 4,
        }
    ).write_parquet(category_dir / "part-000.parquet")

    yaml_path = base / "parity_0301_multi_right_full.yaml"
    yaml_path.write_text(
        f"""
preset: "0301"
job:
  name: parity_0301_multi_right_full
left:
  upstream:
    paths:
    - {left_dir.as_posix()}
right_sources:
- name: profile
  upstream:
    paths:
    - {profile_dir.as_posix()}
  columns:
    include: [profile_code, amount]
    exclude: [unused]
  suffix: _profile
- name: category
  upstream:
    paths:
    - {category_dir.as_posix()}
  columns:
    include: [label]
    exclude: [debug]
  join:
    left_on: [profile_code]
    right_on: [code_key]
    how: left
  suffix: _category
join:
  left_on: [join_key]
  right_on: [right_key]
  how: full
  left_partition_key_column: partition
  right_partition_key_column: right_partition
output:
  output_dir: {(data_root / "output").as_posix()}
  partition_column: partition
execution:
  target_key_groups_per_part: 2
  reset_before_run: true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return PresetParityFixture(
        preset="0301-multi-right-full",
        yaml_path=yaml_path,
        expected_rows=5,
        expected_columns=[
            "partition",
            "join_key",
            "left_value",
            "profile_code",
            "amount",
            "label",
        ],
    )
