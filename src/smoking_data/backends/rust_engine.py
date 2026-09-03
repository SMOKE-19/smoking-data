from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from smoking_data.runtime.paths import ensure_dir


@dataclass(frozen=True, slots=True)
class RestoreListRequest:
    input_dataset_dir: Path
    output_dataset_dir: Path
    lookup_path: Path
    schema: dict[str, str]
    config: dict[str, Any]
    batch_size: int | None = None
    max_workers: int = 1
    drop_cache_hint: bool = False
    print_timing: bool = False


@dataclass(frozen=True, slots=True)
class RestoreParquetRequest:
    input_parquet_path: Path
    output_parquet_path: Path
    lookup_path: Path
    schema: dict[str, str]
    config: dict[str, Any]
    batch_size: int | None = None
    drop_cache_hint: bool = False
    print_timing: bool = False


@dataclass(frozen=True, slots=True)
class CuratedTaskRequest:
    coordinate_path: Path
    output_dir: Path
    output_file_name: str
    single_partition_guaranteed: bool
    writer_input_contract: dict[str, Any] | None
    projection_columns: list[dict[str, str]]
    schema: dict[str, str]
    expression_ir: dict[str, Any] | None = None
    lookup_enrich: list[dict[str, Any]] | None = None
    long_fact: dict[str, Any] | None = None
    output_columns: list[str] | None = None
    output_projection_columns: list[dict[str, Any]] | None = None
    partition_columns: list[str] | None = None
    lookup_path: Path | None = None
    restore_config: dict[str, Any] | None = None
    reference_replace: list[dict[str, Any]] | None = None
    pivot: dict[str, Any] | None = None
    pre_pivot_operations: list[dict[str, Any]] | None = None
    post_operations: list[dict[str, Any]] | None = None
    ordered_operations: list[dict[str, str]] | None = None
    compression: str = "zstd"
    output_row_group_rows: int | None = None
    batch_size: int | None = None
    drop_cache_hint: bool = False
    print_timing: bool = False


def execute_join_task(task: dict[str, Any]) -> dict[str, float]:
    from smoking_data_engine_rs import execute_join_task as execute

    return execute(json.dumps(task, ensure_ascii=True, default=str))


def validate_dataset_assertions(
    parquet_paths: list[Path],
    *,
    assertion_config: dict[str, Any],
    spill_dir: Path,
) -> dict[str, Any]:
    from smoking_data_engine_rs import validate_dataset

    result = validate_dataset(
        parquet_paths=[str(path) for path in parquet_paths],
        assertion_config_json=json.dumps(assertion_config, ensure_ascii=True, default=str),
        spill_dir=str(spill_dir),
    )
    return json.loads(result)


def restore_list_parquet(request: RestoreParquetRequest) -> dict[str, float]:
    from smoking_data_engine_rs import restore_parquet_to_parquet

    ensure_dir(request.output_parquet_path.parent)
    return restore_parquet_to_parquet(
        input_parquet_path=str(request.input_parquet_path),
        output_parquet_path=str(request.output_parquet_path),
        lookup_path=str(request.lookup_path),
        schema=request.schema,
        config=request.config,
        batch_size=request.batch_size,
        drop_cache_hint=request.drop_cache_hint,
        print_timing=request.print_timing,
    )


def restore_list_dataset(request: RestoreListRequest) -> dict[str, float]:
    from smoking_data_engine_rs import restore_dataset_to_dataset

    ensure_dir(request.output_dataset_dir)
    return restore_dataset_to_dataset(
        input_dataset_dir=str(request.input_dataset_dir),
        output_dataset_dir=str(request.output_dataset_dir),
        lookup_path=str(request.lookup_path),
        schema=request.schema,
        config=request.config,
        batch_size=request.batch_size,
        max_workers=request.max_workers,
        drop_cache_hint=request.drop_cache_hint,
        print_timing=request.print_timing,
    )


def execute_curated_task(request: CuratedTaskRequest) -> dict[str, float]:
    from smoking_data_engine_rs import execute_curated_task as execute

    ensure_dir(request.output_dir)
    return execute(
        coord_path=str(request.coordinate_path),
        output_dir=str(request.output_dir),
        lookup_path=str(request.lookup_path) if request.lookup_path else "",
        schema=request.schema,
        config=request.restore_config or {"enabled": False},
        writer_config={
            "output_file_name": request.output_file_name,
            "single_partition_guaranteed": request.single_partition_guaranteed,
            "writer_input_contract": request.writer_input_contract or None,
            "projection_columns": request.projection_columns,
            "expression_ir": request.expression_ir,
            "lookup_enrich": request.lookup_enrich or [],
            "long_fact": request.long_fact,
            "output_columns": request.output_columns or [],
            "output_projection_columns": request.output_projection_columns or [],
            "partition_columns": request.partition_columns or [],
            "reference_replace": request.reference_replace or None,
            "pivot": request.pivot or None,
            "pre_pivot_operations": request.pre_pivot_operations or [],
            "post_operations": request.post_operations or [],
            "ordered_operations": request.ordered_operations or [],
            "compression": request.compression,
            "output_row_group_rows": request.output_row_group_rows,
        },
        batch_size=request.batch_size,
        drop_cache_hint=request.drop_cache_hint,
        print_timing=request.print_timing,
    )
