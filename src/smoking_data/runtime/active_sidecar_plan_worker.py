from __future__ import annotations

import argparse
import gc
import json
import os
import time
import traceback
from pathlib import Path
from typing import Any

import polars as pl

from smoking_data.core.barriers import BarrierState, ensure_complete_group_within_budget
from smoking_data.ops.coordinates import (
    ACTIVE_ORDER_COLUMN,
    PART_INDEX_COLUMN,
    SOURCE_FILE_COLUMN,
    SOURCE_ROW_GROUP_COLUMN,
    SOURCE_ROW_INDEX_COLUMN,
    write_rust_coordinate_file,
)
from smoking_data.planners.pivot_shape import build_pivot_shape_profile
from smoking_data.runtime.active_sidecar_plan import (
    PLAN_SCHEMA_VERSION,
    REQUEST_SCHEMA_VERSION,
    RESULT_SCHEMA_VERSION,
)
from smoking_data.runtime.memory import current_rss_mb, peak_rss_mb, process_io_bytes
from smoking_data.runtime.naming import part_file_name, partition_dir_name, task_id
from smoking_data.runtime.paths import ensure_dir, reset_path
from smoking_data.runtime.selector_ipc import (
    read_sidecar_frame,
    scan_sidecar,
    write_ipc_frame_atomic,
)
from smoking_data.runtime.task_telemetry import task_telemetry_phase

ESTIMATED_PAYLOAD_BYTES_COLUMN = "__estimated_payload_bytes"
SPILL_REQUIRED_COLUMN = "__spill_required"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build one bounded 0201 active-sidecar plan.")
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    result_path = args.result.expanduser().resolve()
    try:
        request = json.loads(args.request.expanduser().resolve().read_text(encoding="utf-8"))
        if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
            raise ValueError("Unsupported active-sidecar-plan request schema.")
        result = _execute(request)
        _write_json_atomic(
            result_path,
            {"schema_version": RESULT_SCHEMA_VERSION, "status": "completed", **result},
        )
        return 0
    except Exception as exc:  # pragma: no cover - validated through the parent boundary.
        _write_json_atomic(
            result_path,
            {
                "schema_version": RESULT_SCHEMA_VERSION,
                "status": "failed",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "traceback_tail": "\n".join(traceback.format_exception(exc)[-20:]),
            },
        )
        return 1


def _execute(request: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    io_started = process_io_bytes()
    telemetry_endpoint = request.get("telemetry_endpoint")
    with task_telemetry_phase(telemetry_endpoint, "build_sidecar.active_boundary"):
        plan = _build_plan(request)
    io_finished = process_io_bytes()
    result = {
        "pid": os.getpid(),
        "elapsed_sec": time.perf_counter() - started,
        "rss_mb": current_rss_mb(),
        "peak_rss_mb": peak_rss_mb(),
        "io_read_bytes": (
            max(0, io_finished[0] - io_started[0])
            if io_started is not None and io_finished is not None
            else None
        ),
        "io_write_bytes": (
            max(0, io_finished[1] - io_started[1])
            if io_started is not None and io_finished is not None
            else None
        ),
        "plan_path": str(request["plan_path"]),
        "active_rows": plan["active_snapshot_stats"]["rows"],
        "tasks": len(plan["coordinate_tasks"]),
    }
    selector_result_path = request.get("selector_result_path")
    if selector_result_path:
        result["upstream_selector"] = json.loads(
            Path(str(selector_result_path)).read_text(encoding="utf-8")
        )
    return result


def _build_plan(request: dict[str, Any]) -> dict[str, Any]:
    piece_paths = [Path(str(item)).resolve() for item in request["active_piece_paths"]]
    if not piece_paths or any(not path.is_file() for path in piece_paths):
        raise ValueError("Active selector piece paths are missing.")
    partition_column = str(request["partition_column"])
    group_keys = [str(item) for item in request.get("group_keys") or []]
    window_partitions = [
        tuple(str(column) for column in item) for item in request.get("window_partitions") or []
    ]
    pivot = dict(request.get("pivot") or {})
    pivot_enabled = bool(pivot.get("enabled", False))
    pivot_row_keys = [str(item) for item in pivot.get("row_keys") or []]
    active_lf = pl.concat(
        [scan_sidecar(path) for path in piece_paths], how="diagonal_relaxed"
    ).drop(SPILL_REQUIRED_COLUMN, strict=False)
    active_schema = active_lf.collect_schema()
    if partition_column not in active_schema:
        raise ValueError(f"0201 partition column is missing: {partition_column}")
    partition_values = (
        active_lf.select(partition_column)
        .unique()
        .sort(partition_column)
        .collect(engine="streaming")
        .get_column(partition_column)
        .to_list()
    )
    if any(value is None for value in partition_values):
        raise ValueError(f"0201 partition column contains null values: {partition_column}")
    window_keys = _validate_window_boundaries_lazy(
        active_lf,
        columns=set(active_schema.names()),
        partition_column=partition_column,
        window_partitions=window_partitions,
    )
    task_group_keys = _complete_group_keys_lazy(
        active_lf,
        columns=set(active_schema.names()),
        partition_column=partition_column,
        window_keys=window_keys,
        pivot_row_keys=pivot_row_keys if pivot_enabled else None,
    )
    spill_aggregation = request.get("spill_aggregation")
    spill_allowed = spill_aggregation is not None and window_keys is None
    snapshot_path = Path(str(request["active_snapshot_path"])).resolve()
    plan_path = Path(str(request["plan_path"])).resolve()
    coordinate_root = snapshot_path.parent / "parts"
    identifier = f"{os.getpid()}-{time.time_ns()}"
    snapshot_staging = snapshot_path.with_name(f".{snapshot_path.name}.{identifier}.tmp")
    coordinate_staging = snapshot_path.parent / f".parts.{identifier}.tmp"
    plan_staging = plan_path.with_name(f".{plan_path.name}.{identifier}.tmp")
    ensure_dir(snapshot_path.parent)
    reset_path(snapshot_staging)
    ensure_dir(snapshot_staging)
    reset_path(coordinate_staging)
    ensure_dir(coordinate_staging)
    coordinate_tasks: list[dict[str, Any]] = []
    snapshot_rows = snapshot_estimated_size = 0
    snapshot_source_files: set[str] = set()
    snapshot_source_row_groups: set[tuple[str, int]] = set()
    window_max_group_rows = 0
    pivot_samples: list[pl.DataFrame] = []
    pivot_required = _pivot_required_columns(pivot)
    sample_per_partition = max(1, 200_000 // max(1, len(partition_values)))
    try:
        for partition_index, raw_partition_value in enumerate(partition_values):
            resolved = (
                active_lf.filter(pl.col(partition_column) == pl.lit(raw_partition_value))
                .sort(
                    list(dict.fromkeys([*group_keys, SOURCE_FILE_COLUMN, SOURCE_ROW_INDEX_COLUMN]))
                )
                .collect(engine="streaming")
            )
            resolved = _assign_part_indices(
                resolved.with_row_index(ACTIVE_ORDER_COLUMN),
                group_keys=task_group_keys,
                barrier_state=(
                    BarrierState.WINDOW
                    if window_keys is not None
                    else BarrierState.PIVOT
                    if pivot_enabled
                    else BarrierState.SORT_FIRST
                ),
                rows_per_part=int(request["rows_per_part"]),
                max_payload_bytes=int(request["memory_budget_bytes"]),
                max_source_files=int(request["max_source_files_per_task"]),
                max_source_row_groups=int(request["max_source_row_groups_per_task"]),
                allow_oversized_group_spill=spill_allowed,
            )
            snapshot_rows += resolved.height
            snapshot_estimated_size += resolved.estimated_size()
            snapshot_source_files.update(
                str(item) for item in resolved.get_column(SOURCE_FILE_COLUMN).unique().to_list()
            )
            snapshot_source_row_groups.update(
                (str(item[SOURCE_FILE_COLUMN]), int(item[SOURCE_ROW_GROUP_COLUMN]))
                for item in resolved.select([SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN])
                .unique()
                .iter_rows(named=True)
            )
            window_max_group_rows = max(
                window_max_group_rows,
                _max_window_group_rows(resolved, window_partitions),
            )
            if pivot_required and all(column in resolved for column in pivot_required):
                pivot_samples.append(resolved.select(pivot_required).head(sample_per_partition))
            snapshot_shard = snapshot_staging / f"part-{partition_index:05d}.arrow"
            write_ipc_frame_atomic(resolved, snapshot_shard)
            for values in (
                resolved.select(PART_INDEX_COLUMN)
                .unique()
                .sort(PART_INDEX_COLUMN)
                .iter_rows(named=True)
            ):
                partition_value = str(raw_partition_value)
                part_index = int(values[PART_INDEX_COLUMN])
                task_frame = resolved.filter(pl.col(PART_INDEX_COLUMN) == part_index)
                coordinates = task_frame.select(
                    [
                        SOURCE_FILE_COLUMN,
                        SOURCE_ROW_GROUP_COLUMN,
                        SOURCE_ROW_INDEX_COLUMN,
                        ACTIVE_ORDER_COLUMN,
                    ]
                )
                coordinate_path = ensure_dir(
                    coordinate_staging / partition_dir_name(partition_value)
                ) / part_file_name(part_index, suffix=".coordinates.arrow")
                write_ipc_frame_atomic(coordinates, coordinate_path)
                arrow_path = coordinate_path.with_name(f"{coordinate_path.stem}.rust.arrow")
                write_rust_coordinate_file(coordinates, arrow_path)
                relative = coordinate_path.relative_to(coordinate_staging)
                coordinate_tasks.append(
                    {
                        "task_id": task_id(partition_value, part_index),
                        "partition_value": partition_value,
                        "part_index": part_index,
                        "coordinate_path": str(Path("parts") / relative),
                        "rust_coordinate_path": str(
                            Path("parts") / arrow_path.relative_to(coordinate_staging)
                        ),
                        "spill_required": bool(task_frame.get_column(SPILL_REQUIRED_COLUMN).any()),
                    }
                )
            del resolved
            gc.collect()
        if pivot_enabled:
            pivot_input = (
                pl.concat(pivot_samples, how="diagonal_relaxed")
                if pivot_samples
                else pl.DataFrame(
                    schema={column: active_schema[column] for column in pivot_required}
                )
            )
            pivot_profile = build_pivot_shape_profile(pivot_input, pivot)
            pivot_profile["estimator_mode"] = "bounded_partition_sample"
            pivot_profile["selected_input_rows"] = snapshot_rows
        else:
            pivot_profile = build_pivot_shape_profile(
                pl.DataFrame({"__rows": [None] * min(snapshot_rows, 1)}), pivot
            )
            pivot_profile["selected_input_rows"] = snapshot_rows
        window_profile = _window_profile_from_stats(
            window_partitions,
            rows=snapshot_rows,
            estimated_size_bytes=snapshot_estimated_size,
            max_group_rows=window_max_group_rows,
        )
        plan = {
            "schema_version": PLAN_SCHEMA_VERSION,
            "path_contract": "plan_parent_relative",
            "active_snapshot_path": snapshot_path.name,
            "active_snapshot_stats": {
                "rows": snapshot_rows,
                "partitions": len(partition_values),
                "source_files": len(snapshot_source_files),
                "source_row_groups": len(snapshot_source_row_groups),
                "estimated_size_bytes": snapshot_estimated_size,
                "layout": "partition_sharded_arrow_ipc_dataset",
                "storage": "arrow_ipc_file",
                "shards": len(partition_values),
            },
            "coordinate_storage": {
                "planner": "arrow_ipc_file",
                "rust_materialize": "arrow_ipc_file",
            },
            "coordinate_tasks": coordinate_tasks,
            "coordinate_boundary_fanout": _fanout_profile_from_tasks(
                coordinate_tasks,
                coordinate_staging=coordinate_staging,
                partition_column=partition_column,
                max_source_files=int(request["max_source_files_per_task"]),
                max_source_row_groups=int(request["max_source_row_groups_per_task"]),
            ),
            "pivot_shape_profile": pivot_profile,
            "window_planner": window_profile,
            "pivot_spill_fallback": {
                "eligible": spill_allowed,
                "merge_aggregation": spill_aggregation,
                "window_barrier_present": window_keys is not None,
            },
        }
        _write_json_atomic(plan_staging, plan)
        _commit_outputs(
            snapshot_staging=snapshot_staging,
            snapshot_path=snapshot_path,
            coordinate_staging=coordinate_staging,
            coordinate_root=coordinate_root,
            plan_staging=plan_staging,
            plan_path=plan_path,
        )
        return plan
    finally:
        reset_path(snapshot_staging)
        reset_path(coordinate_staging)
        reset_path(plan_staging)


def _commit_outputs(
    *,
    snapshot_staging: Path,
    snapshot_path: Path,
    coordinate_staging: Path,
    coordinate_root: Path,
    plan_staging: Path,
    plan_path: Path,
) -> None:
    identifier = f"{os.getpid()}-{time.time_ns()}"
    snapshot_backup = snapshot_path.with_name(f".{snapshot_path.name}.{identifier}.backup")
    coordinate_backup = coordinate_root.with_name(f".{coordinate_root.name}.{identifier}.backup")
    plan_backup = plan_path.with_name(f".{plan_path.name}.{identifier}.backup")
    snapshot_moved = coordinate_moved = plan_moved = False
    try:
        if snapshot_path.exists():
            os.replace(snapshot_path, snapshot_backup)
            snapshot_moved = True
        if coordinate_root.exists():
            os.replace(coordinate_root, coordinate_backup)
            coordinate_moved = True
        if plan_path.exists():
            os.replace(plan_path, plan_backup)
            plan_moved = True
        os.replace(snapshot_staging, snapshot_path)
        os.replace(coordinate_staging, coordinate_root)
        os.replace(plan_staging, plan_path)
    except BaseException:
        reset_path(snapshot_path)
        reset_path(coordinate_root)
        reset_path(plan_path)
        if snapshot_moved and snapshot_backup.exists():
            os.replace(snapshot_backup, snapshot_path)
        if coordinate_moved and coordinate_backup.exists():
            os.replace(coordinate_backup, coordinate_root)
        if plan_moved and plan_backup.exists():
            os.replace(plan_backup, plan_path)
        raise
    reset_path(snapshot_backup)
    reset_path(coordinate_backup)
    reset_path(plan_backup)


def _validate_window_boundaries_lazy(
    active: pl.LazyFrame,
    *,
    columns: set[str],
    partition_column: str,
    window_partitions: list[tuple[str, ...]],
) -> list[str] | None:
    if not window_partitions:
        return None
    if len(window_partitions) > 1:
        raise ValueError("Window expressions require one shared partition-key contract.")
    keys = list(window_partitions[0])
    if not keys:
        partitions = active.select(pl.col(partition_column).n_unique()).collect().item()
        if int(partitions or 0) > 1:
            raise ValueError("Global OVER() crosses output partitions.")
        return []
    missing = [column for column in keys if column not in columns]
    if missing:
        raise ValueError(f"Window partition columns are missing: {missing}")
    crossing = (
        active.group_by(keys)
        .agg(pl.col(partition_column).n_unique().alias("__partition_count"))
        .filter(pl.col("__partition_count") > 1)
        .limit(1)
        .collect(engine="streaming")
    )
    if crossing.height:
        raise ValueError("Window group crosses output partitions.")
    return keys


def _complete_group_keys_lazy(
    active: pl.LazyFrame,
    *,
    columns: set[str],
    partition_column: str,
    window_keys: list[str] | None,
    pivot_row_keys: list[str] | None,
) -> list[str] | None:
    contracts = [
        list(dict.fromkeys(keys)) for keys in (window_keys, pivot_row_keys) if keys is not None
    ]
    if not contracts:
        return None
    for keys in contracts:
        if not keys:
            partitions = active.select(pl.col(partition_column).n_unique()).collect().item()
            if int(partitions or 0) > 1:
                raise ValueError("A global complete-group operation crosses partitions.")
            return []
        missing = [column for column in keys if column not in columns]
        if missing:
            raise ValueError(f"Complete-group key columns are missing: {missing}")
        crossing = (
            active.group_by(keys)
            .agg(pl.col(partition_column).n_unique().alias("__partition_count"))
            .filter(pl.col("__partition_count") > 1)
            .limit(1)
            .collect(engine="streaming")
        )
        if crossing.height:
            raise ValueError("Complete-group keys cross output partitions.")
    chosen = min(contracts, key=len)
    if any(not set(chosen).issubset(keys) for keys in contracts):
        raise ValueError("Window and pivot complete-group keys are not nested.")
    return chosen


def _pivot_required_columns(pivot: dict[str, Any]) -> list[str]:
    if not bool(pivot.get("enabled", False)):
        return []
    values = [
        item
        for item in [
            *(pivot.get("value_keys") or []),
            *(pivot.get("value_keys_without_column") or []),
        ]
        if isinstance(item, dict)
    ]
    return list(
        dict.fromkeys(
            [
                *[str(item) for item in (pivot.get("row_keys") or [])],
                *[str(item) for item in (pivot.get("column_keys") or [])],
                *[str(item.get("source_column") or "") for item in values],
            ]
        )
    )


def _max_window_group_rows(frame: pl.DataFrame, window_partitions: list[tuple[str, ...]]) -> int:
    if not window_partitions:
        return 0
    keys = list(window_partitions[0])
    if not keys:
        return frame.height
    return int(frame.group_by(keys).len().get_column("len").max() or 0)


def _window_profile_from_stats(
    window_partitions: list[tuple[str, ...]],
    *,
    rows: int,
    estimated_size_bytes: int,
    max_group_rows: int,
) -> dict[str, Any]:
    if not window_partitions:
        return {"enabled": False}
    keys = list(window_partitions[0])
    bytes_per_row = estimated_size_bytes / rows if rows else 0.0
    return {
        "enabled": True,
        "partition_keys": keys,
        "max_group_rows": max_group_rows,
        "estimated_max_group_sidecar_bytes": int(max_group_rows * bytes_per_row),
        "stateful_sort_functions": _window_profile(
            pl.DataFrame({key: [] for key in keys}), window_partitions
        )["stateful_sort_functions"],
        "spill_supported": False,
    }


def _fanout_profile_from_tasks(
    tasks: list[dict[str, Any]],
    *,
    coordinate_staging: Path,
    partition_column: str,
    max_source_files: int,
    max_source_row_groups: int,
) -> dict[str, Any]:
    del partition_column
    files: list[int] = []
    row_groups: list[int] = []
    for task in tasks:
        relative = Path(str(task["coordinate_path"]))
        coordinate_path = coordinate_staging / Path(*relative.parts[1:])
        coordinates = read_sidecar_frame(coordinate_path).select(
            [SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN]
        )
        files.append(coordinates.get_column(SOURCE_FILE_COLUMN).n_unique())
        row_groups.append(
            coordinates.select([SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN]).n_unique()
        )
    return {
        "schema_version": "smoking-data.coordinate-boundary-fanout.v1",
        "tasks": len(tasks),
        "limits": {
            "max_source_files_per_task": max_source_files,
            "max_source_row_groups_per_task": max_source_row_groups,
        },
        "source_files_per_task": _distribution(files),
        "source_row_groups_per_task": _distribution(row_groups),
        "unsplittable_complete_group_tasks": {
            "source_files_over_limit": sum(value > max_source_files for value in files),
            "source_row_groups_over_limit": sum(
                value > max_source_row_groups for value in row_groups
            ),
        },
        "complete_group_split_allowed": False,
    }


def _validate_window_boundaries(
    active: pl.DataFrame,
    *,
    partition_column: str,
    window_partitions: list[tuple[str, ...]],
) -> list[str] | None:
    if not window_partitions:
        return None
    if len(window_partitions) > 1:
        raise ValueError("Window expressions require one shared partition-key contract.")
    keys = list(window_partitions[0])
    if not keys:
        if active.get_column(partition_column).n_unique() > 1:
            raise ValueError("Global OVER() crosses output partitions.")
        return []
    crossing = (
        active.group_by(keys)
        .agg(pl.col(partition_column).n_unique().alias("__partition_count"))
        .filter(pl.col("__partition_count") > 1)
        .limit(1)
    )
    if crossing.height:
        raise ValueError("Window group crosses output partitions.")
    return keys


def _complete_group_keys(
    active: pl.DataFrame,
    *,
    partition_column: str,
    window_keys: list[str] | None,
    pivot_row_keys: list[str] | None,
) -> list[str] | None:
    contracts = [
        list(dict.fromkeys(keys)) for keys in (window_keys, pivot_row_keys) if keys is not None
    ]
    if not contracts:
        return None
    for keys in contracts:
        if not keys:
            if active.get_column(partition_column).n_unique() > 1:
                raise ValueError("A global complete-group operation crosses partitions.")
            return []
        missing = [column for column in keys if column not in active.columns]
        if missing:
            raise ValueError(f"Complete-group key columns are missing: {missing}")
        crossing = (
            active.group_by(keys)
            .agg(pl.col(partition_column).n_unique().alias("__partition_count"))
            .filter(pl.col("__partition_count") > 1)
            .limit(1)
        )
        if crossing.height:
            raise ValueError("Complete-group keys cross output partitions.")
    chosen = min(contracts, key=len)
    if any(not set(chosen).issubset(keys) for keys in contracts):
        raise ValueError("Window and pivot complete-group keys are not nested.")
    return chosen


def _assign_part_indices(
    frame: pl.DataFrame,
    *,
    group_keys: list[str] | None,
    barrier_state: BarrierState,
    rows_per_part: int,
    max_payload_bytes: int,
    max_source_files: int,
    max_source_row_groups: int,
    allow_oversized_group_spill: bool,
) -> pl.DataFrame:
    if group_keys == []:
        payload_bytes = int(frame.get_column(ESTIMATED_PAYLOAD_BYTES_COLUMN).sum() or 0)
        spill_required = payload_bytes > max_payload_bytes and allow_oversized_group_spill
        if not spill_required:
            ensure_complete_group_within_budget(
                state=barrier_state,
                group_key={"global": True},
                estimated_bytes=payload_bytes,
                budget_bytes=max_payload_bytes,
                rows=frame.height,
            )
        return frame.with_columns(
            pl.lit(0, dtype=pl.Int64).alias(PART_INDEX_COLUMN),
            pl.lit(spill_required).alias(SPILL_REQUIRED_COLUMN),
        )
    if group_keys is None:
        return _assign_rows(
            frame,
            rows_per_part=rows_per_part,
            max_payload_bytes=max_payload_bytes,
            max_source_files=max_source_files,
            max_source_row_groups=max_source_row_groups,
        )
    groups = frame.group_by(group_keys, maintain_order=True).agg(
        pl.len().alias("__group_rows"),
        pl.col(ESTIMATED_PAYLOAD_BYTES_COLUMN).sum().alias("__group_payload_bytes"),
        pl.col(SOURCE_FILE_COLUMN).unique().alias("__group_source_files"),
        pl.struct([SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN])
        .unique()
        .alias("__group_source_row_groups"),
    )
    part_indices, spill_flags = _group_part_indices(
        groups,
        group_keys=group_keys,
        barrier_state=barrier_state,
        rows_per_part=rows_per_part,
        max_payload_bytes=max_payload_bytes,
        max_source_files=max_source_files,
        max_source_row_groups=max_source_row_groups,
        allow_oversized_group_spill=allow_oversized_group_spill,
    )
    mapping = groups.with_columns(
        pl.Series(PART_INDEX_COLUMN, part_indices, dtype=pl.Int64),
        pl.Series(SPILL_REQUIRED_COLUMN, spill_flags, dtype=pl.Boolean),
    )
    return frame.join(
        mapping.select([*group_keys, PART_INDEX_COLUMN, SPILL_REQUIRED_COLUMN]),
        on=group_keys,
        how="left",
        nulls_equal=True,
    )


def _group_part_indices(
    groups: pl.DataFrame,
    *,
    group_keys: list[str],
    barrier_state: BarrierState,
    rows_per_part: int,
    max_payload_bytes: int,
    max_source_files: int,
    max_source_row_groups: int,
    allow_oversized_group_spill: bool,
) -> tuple[list[int], list[bool]]:
    part_indices: list[int] = []
    spill_flags: list[bool] = []
    part_index = current_rows = current_bytes = 0
    current_files: set[str] = set()
    current_row_groups: set[tuple[str, int]] = set()
    for group in groups.iter_rows(named=True):
        size = int(group["__group_rows"])
        payload_bytes = int(group["__group_payload_bytes"] or 0)
        spill_required = payload_bytes > max_payload_bytes and allow_oversized_group_spill
        if not spill_required:
            ensure_complete_group_within_budget(
                state=barrier_state,
                group_key={key: group[key] for key in group_keys},
                estimated_bytes=payload_bytes,
                budget_bytes=max_payload_bytes,
                rows=size,
            )
        group_files = {str(item) for item in group["__group_source_files"]}
        group_row_groups = {
            (str(item[SOURCE_FILE_COLUMN]), int(item[SOURCE_ROW_GROUP_COLUMN]))
            for item in group["__group_source_row_groups"]
        }
        exceeds = (
            current_rows + size > rows_per_part
            or current_bytes + payload_bytes > max_payload_bytes
            or len(current_files | group_files) > max_source_files
            or len(current_row_groups | group_row_groups) > max_source_row_groups
        )
        if current_rows and exceeds:
            part_index += 1
            current_rows = current_bytes = 0
            current_files.clear()
            current_row_groups.clear()
        part_indices.append(part_index)
        spill_flags.append(spill_required)
        current_rows += size
        current_bytes += payload_bytes
        current_files.update(group_files)
        current_row_groups.update(group_row_groups)
    return part_indices, spill_flags


def _assign_rows(
    frame: pl.DataFrame,
    *,
    rows_per_part: int,
    max_payload_bytes: int,
    max_source_files: int,
    max_source_row_groups: int,
) -> pl.DataFrame:
    part_indices: list[int] = []
    part_index = current_rows = current_bytes = 0
    current_files: set[str] = set()
    current_row_groups: set[tuple[str, int]] = set()
    columns = frame.select(
        [ESTIMATED_PAYLOAD_BYTES_COLUMN, SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN]
    )
    for payload_bytes, source_file, source_row_group in columns.iter_rows():
        row_bytes = int(payload_bytes or 0)
        source = str(source_file)
        row_group = (source, int(source_row_group))
        exceeds = (
            current_rows + 1 > rows_per_part
            or current_bytes + row_bytes > max_payload_bytes
            or (source not in current_files and len(current_files) >= max_source_files)
            or (
                row_group not in current_row_groups
                and len(current_row_groups) >= max_source_row_groups
            )
        )
        if current_rows and exceeds:
            part_index += 1
            current_rows = current_bytes = 0
            current_files.clear()
            current_row_groups.clear()
        part_indices.append(part_index)
        current_rows += 1
        current_bytes += row_bytes
        current_files.add(source)
        current_row_groups.add(row_group)
    return frame.with_columns(
        pl.Series(PART_INDEX_COLUMN, part_indices, dtype=pl.Int64),
        pl.lit(False).alias(SPILL_REQUIRED_COLUMN),
    )


def _assign_partitioned_rows(
    frame: pl.DataFrame,
    *,
    partition_column: str,
    rows_per_part: int,
    max_payload_bytes: int,
    max_source_files: int,
    max_source_row_groups: int,
) -> pl.DataFrame:
    active_orders: list[int] = []
    part_indices: list[int] = []
    current_partition: Any = object()
    active_order = part_index = current_rows = current_bytes = 0
    current_files: set[str] = set()
    current_row_groups: set[tuple[str, int]] = set()
    columns = frame.select(
        [
            partition_column,
            ESTIMATED_PAYLOAD_BYTES_COLUMN,
            SOURCE_FILE_COLUMN,
            SOURCE_ROW_GROUP_COLUMN,
        ]
    )
    for partition, payload_bytes, source_file, source_row_group in columns.iter_rows():
        if partition != current_partition:
            current_partition = partition
            active_order = part_index = current_rows = current_bytes = 0
            current_files.clear()
            current_row_groups.clear()
        row_bytes = int(payload_bytes or 0)
        source = str(source_file)
        row_group = (source, int(source_row_group))
        exceeds = (
            current_rows + 1 > rows_per_part
            or current_bytes + row_bytes > max_payload_bytes
            or (source not in current_files and len(current_files) >= max_source_files)
            or (
                row_group not in current_row_groups
                and len(current_row_groups) >= max_source_row_groups
            )
        )
        if current_rows and exceeds:
            part_index += 1
            current_rows = current_bytes = 0
            current_files.clear()
            current_row_groups.clear()
        active_orders.append(active_order)
        part_indices.append(part_index)
        active_order += 1
        current_rows += 1
        current_bytes += row_bytes
        current_files.add(source)
        current_row_groups.add(row_group)
    return frame.with_columns(
        pl.Series(ACTIVE_ORDER_COLUMN, active_orders, dtype=pl.UInt32),
        pl.Series(PART_INDEX_COLUMN, part_indices, dtype=pl.Int64),
        pl.lit(False).alias(SPILL_REQUIRED_COLUMN),
    )


def _window_profile(
    active: pl.DataFrame, window_partitions: list[tuple[str, ...]]
) -> dict[str, Any]:
    if not window_partitions:
        return {"enabled": False}
    keys = list(window_partitions[0])
    max_group_rows = (
        active.height if not keys else int(active.group_by(keys).len()["len"].max() or 0)
    )
    bytes_per_row = active.estimated_size() / active.height if active.height else 0.0
    return {
        "enabled": True,
        "partition_keys": keys,
        "max_group_rows": max_group_rows,
        "estimated_max_group_sidecar_bytes": int(max_group_rows * bytes_per_row),
        "stateful_sort_functions": [
            "median",
            "percentile",
            "p10",
            "p90",
            "q1",
            "q3",
            "iqr",
            "dense_rank",
            "rank_real",
            "nth_largest",
            "nth_smallest",
            "trimmed_mean",
            "lav",
            "uav",
            "outliers",
            "pct_outliers",
        ],
        "spill_supported": False,
    }


def _fanout_profile(
    frame: pl.DataFrame,
    *,
    partition_column: str,
    max_source_files: int,
    max_source_row_groups: int,
) -> dict[str, Any]:
    values = frame.group_by([partition_column, PART_INDEX_COLUMN]).agg(
        pl.col(SOURCE_FILE_COLUMN).n_unique().alias("source_files"),
        pl.struct([SOURCE_FILE_COLUMN, SOURCE_ROW_GROUP_COLUMN])
        .n_unique()
        .alias("source_row_groups"),
    )
    files = [int(value) for value in values["source_files"].to_list()]
    row_groups = [int(value) for value in values["source_row_groups"].to_list()]
    return {
        "schema_version": "smoking-data.coordinate-boundary-fanout.v1",
        "tasks": values.height,
        "limits": {
            "max_source_files_per_task": max_source_files,
            "max_source_row_groups_per_task": max_source_row_groups,
        },
        "source_files_per_task": _distribution(files),
        "source_row_groups_per_task": _distribution(row_groups),
        "unsplittable_complete_group_tasks": {
            "source_files_over_limit": sum(value > max_source_files for value in files),
            "source_row_groups_over_limit": sum(
                value > max_source_row_groups for value in row_groups
            ),
        },
        "complete_group_split_allowed": False,
    }


def _distribution(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {"count": 0, "min": None, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": ordered[round((len(ordered) - 1) * 0.50)],
        "p95": ordered[round((len(ordered) - 1) * 0.95)],
        "max": ordered[-1],
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


if __name__ == "__main__":
    raise SystemExit(main())
