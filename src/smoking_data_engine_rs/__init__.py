"""Bounded-memory Rust execution engine for smoking-data pipelines."""

from __future__ import annotations

import concurrent.futures
import json
from collections.abc import Mapping
from pathlib import Path

from .smoking_data_engine_rs import (
    execute_curated_task as _execute_curated_task_impl,
)
from .smoking_data_engine_rs import (
    execute_join_task as _execute_join_task_impl,
)
from .smoking_data_engine_rs import (
    inspect_parquet_pages as _inspect_parquet_pages_impl,
)

try:
    from .smoking_data_engine_rs import (
        read_s3_parquet_to_ipc as _read_s3_parquet_to_ipc_impl,
    )
    from .smoking_data_engine_rs import s3_get_range as _s3_get_range_impl
except ImportError:  # Editable trees may still contain an older native build.
    _read_s3_parquet_to_ipc_impl = None
    _s3_get_range_impl = None
from .smoking_data_engine_rs import (
    join_backend_capabilities as _join_backend_capabilities_impl,
)
from .smoking_data_engine_rs import (
    plan_coordinates as _plan_coordinates_impl,
)
from .smoking_data_engine_rs import (
    restore_parquet_to_parquet as _restore_parquet_to_parquet_impl,
)
from .smoking_data_engine_rs import (
    restore_parquet_to_parquet_profiled as _restore_parquet_to_parquet_profiled_impl,
)
from .smoking_data_engine_rs import (
    supported_expression_functions as _supported_expression_functions_impl,
)
from .smoking_data_engine_rs import (
    validate_dataset as _validate_dataset_impl,
)
from .smoking_data_engine_rs import (
    validate_expression_ir as _validate_expression_ir_impl,
)

__all__ = [
    "__version__",
    "execute_curated_task",
    "execute_join_task",
    "inspect_parquet_pages",
    "join_backend_capabilities",
    "plan_coordinates",
    "read_s3_parquet_to_ipc",
    "restore_dataset_to_dataset",
    "restore_dataset_to_dataset_profiled",
    "restore_parquet_to_parquet",
    "restore_parquet_to_parquet_profiled",
    "supported_expression_functions",
    "s3_get_range",
    "validate_dataset",
    "validate_expression_ir",
]

__version__ = "2.1.0"

RestoreWorkerArgs = tuple[
    str,
    str,
    str,
    dict[str, str],
    dict[str, object],
    int | None,
    bool,
    bool,
]
ProfiledRestoreWorkerArgs = tuple[
    str,
    str,
    str,
    dict[str, str],
    dict[str, object],
    int | None,
    bool,
]


def validate_expression_ir(ir_json: str) -> str:
    return _validate_expression_ir_impl(ir_json)


def supported_expression_functions() -> list[str]:
    return _supported_expression_functions_impl()


def inspect_parquet_pages(path: str | Path) -> dict[str, object]:
    return json.loads(_inspect_parquet_pages_impl(str(path)))


def s3_get_range(request: Mapping[str, object]) -> bytes:
    if _s3_get_range_impl is None:
        raise RuntimeError("native S3 range reader is not built")
    return bytes(_s3_get_range_impl(json.dumps(dict(request), ensure_ascii=True)))


def read_s3_parquet_to_ipc(request: Mapping[str, object]) -> dict[str, object]:
    if _read_s3_parquet_to_ipc_impl is None:
        raise RuntimeError("native S3 Parquet reader is not built")
    return json.loads(
        _read_s3_parquet_to_ipc_impl(json.dumps(dict(request), ensure_ascii=True))
    )


def execute_join_task(task_json: str) -> dict[str, float]:
    return _execute_join_task_impl(task_json)


def join_backend_capabilities() -> list[str]:
    return _join_backend_capabilities_impl()


def validate_dataset(parquet_paths: list[str], assertion_config_json: str, spill_dir: str) -> str:
    return _validate_dataset_impl(parquet_paths, assertion_config_json, spill_dir)


def plan_coordinates(
    input_parquet_paths: list[str],
    coord_output_dir: str,
    filter_config: Mapping[str, object] | None = None,
    planner_config: Mapping[str, object] | None = None,
) -> dict[str, float]:
    return _plan_coordinates_impl(
        [str(item) for item in input_parquet_paths],
        coord_output_dir,
        json.dumps(dict(filter_config or {}), ensure_ascii=True),
        json.dumps(dict(planner_config or {}), ensure_ascii=True),
    )


def _restore_parquet_to_parquet_worker(args: RestoreWorkerArgs) -> dict[str, float]:
    (
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size,
        drop_cache_hint,
        print_timing,
    ) = args
    return _restore_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size=batch_size,
        drop_cache_hint=drop_cache_hint,
        print_timing=print_timing,
        profiled=False,
    )


def _restore_parquet_to_parquet_profiled_worker(
    args: ProfiledRestoreWorkerArgs,
) -> dict[str, float]:
    (
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size,
        drop_cache_hint,
    ) = args
    return _restore_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size=batch_size,
        drop_cache_hint=drop_cache_hint,
        print_timing=False,
        profiled=True,
    )


def _restore_parquet_impl(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    *,
    batch_size: int | None,
    drop_cache_hint: bool,
    print_timing: bool,
    profiled: bool,
) -> dict[str, float]:
    config_json = json.dumps(config, ensure_ascii=True)
    if profiled:
        return _restore_parquet_to_parquet_profiled_impl(
            input_parquet_path,
            output_parquet_path,
            lookup_path,
            schema,
            config_json,
            batch_size,
            drop_cache_hint,
        )
    return _restore_parquet_to_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
        print_timing,
    )


def restore_parquet_to_parquet(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = None,
    drop_cache_hint: bool = False,
    print_timing: bool = False,
) -> dict[str, float]:
    return _restore_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size=batch_size,
        drop_cache_hint=drop_cache_hint,
        print_timing=print_timing,
        profiled=False,
    )


def execute_curated_task(
    coord_path: str,
    output_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    writer_config: Mapping[str, object] | None = None,
    batch_size: int | None = None,
    drop_cache_hint: bool = False,
    print_timing: bool = False,
) -> dict[str, float]:
    return _execute_curated_task_impl(
        coord_path,
        output_dir,
        lookup_path,
        schema,
        json.dumps(config, ensure_ascii=True),
        json.dumps(dict(writer_config or {}), ensure_ascii=True),
        batch_size,
        drop_cache_hint,
        print_timing,
    )


def restore_dataset_to_dataset(
    input_dataset_dir: str,
    output_dataset_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = None,
    max_workers: int = 1,
    drop_cache_hint: bool = False,
    print_timing: bool = False,
) -> dict[str, float]:
    input_dir = Path(input_dataset_dir)
    output_dir = Path(output_dataset_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    part_files = sorted(path for path in input_dir.glob("*.parquet") if path.is_file())
    if not part_files:
        raise ValueError(f"restore input dataset has no parquet files: {input_dir}")

    totals: dict[str, float] = {
        "files_written": 0.0,
        "rows_written": 0.0,
        "reference_load_sec": 0.0,
        "restore_sec": 0.0,
        "parquet_write_sec": 0.0,
        "total_sec": 0.0,
    }
    worker_args = [
        (
            str(input_path),
            str(output_dir / input_path.name),
            lookup_path,
            schema,
            dict(config),
            batch_size,
            drop_cache_hint,
            print_timing,
        )
        for input_path in part_files
    ]
    if max_workers <= 1:
        results = [_restore_parquet_to_parquet_worker(args) for args in worker_args]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(_restore_parquet_to_parquet_worker, worker_args))
    for stats in results:
        totals["files_written"] += 1.0
        for key, value in stats.items():
            totals[key] = totals.get(key, 0.0) + value
    return totals


def restore_parquet_to_parquet_profiled(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = None,
    drop_cache_hint: bool = False,
) -> dict[str, float]:
    return _restore_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size=batch_size,
        drop_cache_hint=drop_cache_hint,
        print_timing=False,
        profiled=True,
    )


def restore_dataset_to_dataset_profiled(
    input_dataset_dir: str,
    output_dataset_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = None,
    max_workers: int = 1,
    drop_cache_hint: bool = False,
) -> dict[str, object]:
    input_dir = Path(input_dataset_dir)
    output_dir = Path(output_dataset_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    part_files = sorted(path for path in input_dir.glob("*.parquet") if path.is_file())
    if not part_files:
        raise ValueError(f"restore input dataset has no parquet files: {input_dir}")

    summary: dict[str, float] = {
        "files_written": 0.0,
        "rows_written": 0.0,
        "reference_load_sec": 0.0,
        "restore_sec": 0.0,
        "parquet_write_sec": 0.0,
        "total_sec": 0.0,
        "batches_processed": 0.0,
        "input_rows": 0.0,
        "source_extract_sec": 0.0,
        "dense_restore_sec": 0.0,
        "record_batch_build_sec": 0.0,
        "writer_write_sec": 0.0,
        "cache_hint_sec": 0.0,
        "cache_hint_calls": 0.0,
    }
    avg_accumulators: dict[str, float] = {"avg_restored_batch_array_bytes": 0.0}
    max_keys = {
        "max_batch_rows",
        "max_dense_len",
        "value_column_count",
        "output_file_size_bytes",
        "max_restored_batch_array_bytes",
    }
    files: list[dict[str, object]] = []
    worker_args = [
        (
            str(input_path),
            str(output_dir / input_path.name),
            lookup_path,
            schema,
            dict(config),
            batch_size,
            drop_cache_hint,
        )
        for input_path in part_files
    ]
    if max_workers <= 1:
        results = [_restore_parquet_to_parquet_profiled_worker(args) for args in worker_args]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(_restore_parquet_to_parquet_profiled_worker, worker_args))
    for input_path, output_path, stats in zip(
        [Path(args[0]) for args in worker_args],
        [Path(args[1]) for args in worker_args],
        results,
        strict=False,
    ):
        files.append(
            {
                "input_path": str(input_path),
                "output_path": str(output_path),
                **stats,
            }
        )
        summary["files_written"] += 1.0
        batches_processed = float(stats.get("batches_processed", 0.0))
        for key, value in stats.items():
            numeric_value = float(value)
            if key in max_keys:
                summary[key] = max(summary.get(key, 0.0), numeric_value)
            elif key == "avg_restored_batch_array_bytes":
                avg_accumulators[key] += numeric_value * batches_processed
            else:
                summary[key] = summary.get(key, 0.0) + numeric_value
    total_batches = summary.get("batches_processed", 0.0)
    if total_batches > 0:
        summary["avg_restored_batch_array_bytes"] = (
            avg_accumulators["avg_restored_batch_array_bytes"] / total_batches
        )
    else:
        summary["avg_restored_batch_array_bytes"] = 0.0
    return {"summary": summary, "files": files}
