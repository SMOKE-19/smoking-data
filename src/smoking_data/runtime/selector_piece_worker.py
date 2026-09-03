from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from smoking_data.runtime.intermediate import write_sorted_intermediate
from smoking_data.runtime.memory import current_rss_mb, peak_rss_mb, process_io_bytes
from smoking_data.runtime.naming import partition_dir_name
from smoking_data.runtime.selector_piece import REQUEST_SCHEMA_VERSION, RESULT_SCHEMA_VERSION
from smoking_data.runtime.task_telemetry import emit_task_telemetry_event, task_telemetry_phase

GROUP_SIZE_COLUMN = "__smoking_data_selector_group_size"


def main() -> int:
    parser = argparse.ArgumentParser(description="Write bounded 0201 selector winner pieces.")
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    result_path = args.result.expanduser().resolve()
    try:
        request = json.loads(args.request.expanduser().resolve().read_text(encoding="utf-8"))
        if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
            raise ValueError("Unsupported selector-piece request schema.")
        result = _execute(request)
        continuation = request.get("active_sidecar_plan_continuation")
        if isinstance(continuation, dict):
            _continue_with_active_sidecar_plan(
                request=request,
                selector_result=result,
                continuation=continuation,
                final_result_path=result_path,
            )
            raise AssertionError("os.execv returned unexpectedly")
        _write_json_atomic(
            result_path,
            {"schema_version": RESULT_SCHEMA_VERSION, "status": "completed", **result},
        )
        return 0
    except Exception as exc:  # pragma: no cover - parent boundary reports structured failure.
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


def _continue_with_active_sidecar_plan(
    *,
    request: dict[str, Any],
    selector_result: dict[str, Any],
    continuation: dict[str, Any],
    final_result_path: Path,
) -> None:
    staging = Path(str(request["staging"])).resolve()
    active_request = dict(continuation.get("request") or {})
    selector_result_path = staging / "_selector-piece.completed.json"
    active_request_path = staging / "_active-sidecar-plan.request.json"
    active_request["active_piece_paths"] = [
        str(staging / str(item["path"])) for item in selector_result.get("active_entries") or []
    ]
    active_request["selector_result_path"] = str(selector_result_path)
    _write_json_atomic(
        selector_result_path,
        {
            "schema_version": RESULT_SCHEMA_VERSION,
            "status": "completed",
            **selector_result,
        },
    )
    _write_json_atomic(active_request_path, active_request)
    os.execv(
        sys.executable,
        [
            sys.executable,
            "-m",
            "smoking_data.runtime.active_sidecar_plan_worker",
            "--request",
            str(active_request_path),
            "--result",
            str(final_result_path),
        ],
    )


def _execute(request: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    io_started = process_io_bytes()
    staging = Path(str(request["staging"])).resolve()
    candidate_paths = [Path(str(item)).resolve() for item in request["candidate_paths"]]
    partition_column = str(request["partition_column"])
    group_keys = [str(item) for item in request["selection_group_keys"]]
    sort = list(request.get("sort") or [])
    bucket_count = max(1, int(request["bucket_count"]))
    memory_budget_bytes = int(request["memory_budget_bytes"])
    endpoint = request.get("telemetry_endpoint")
    bucket_started = time.perf_counter()
    with task_telemetry_phase(
        endpoint,
        "build_sidecar.bucketize",
        task_id="selector-bucketize",
    ):
        buckets, piece_count = _bucketize(
            candidate_paths,
            staging=staging,
            partition_column=partition_column,
            group_keys=group_keys,
            bucket_count=bucket_count,
            telemetry_endpoint=endpoint,
        )
    bucket_elapsed = time.perf_counter() - bucket_started
    selector_started = time.perf_counter()
    active_entries: list[dict[str, Any]] = []
    total_rows = total_groups = max_group_rows = 0
    with task_telemetry_phase(
        endpoint,
        "build_sidecar.selector_bucket",
        task_id="selector-buckets",
    ):
        for partition_value, bucket_id in sorted(buckets):
            paths = buckets[(partition_value, bucket_id)]
            output_path = paths[0].parent / "active.parquet"
            counters = _select_bucket(
                paths,
                output_path=output_path,
                group_keys=group_keys,
                sort=sort,
                memory_budget_bytes=memory_budget_bytes,
            )
            total_rows += counters["selector_input_rows"]
            total_groups += counters["selector_groups"]
            max_group_rows = max(max_group_rows, counters["max_rows_per_selector_group"])
            active_entries.append(
                {
                    "partition_value": partition_value,
                    "bucket_id": bucket_id,
                    "path": str(output_path.relative_to(staging)),
                }
            )
    selector_elapsed = time.perf_counter() - selector_started
    _write_json_atomic(
        staging / "_bucket-plan.json",
        {
            "schema_version": "smoking-data.selector-bucket-plan.v1",
            "path_contract": "staging_relative",
            "bucket_count": bucket_count,
            "candidate_pieces": piece_count,
            "buckets": [
                {
                    "partition_value": partition,
                    "bucket_id": bucket,
                    "paths": [str(path.relative_to(staging)) for path in paths],
                }
                for (partition, bucket), paths in sorted(buckets.items())
            ],
        },
    )
    io_finished = process_io_bytes()
    return {
        "pid": os.getpid(),
        "elapsed_sec": time.perf_counter() - started,
        "bucketize_elapsed_sec": bucket_elapsed,
        "selector_elapsed_sec": selector_elapsed,
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
        "candidate_pieces": piece_count,
        "bucket_count": bucket_count,
        "bucket_tasks": len(active_entries),
        "selector_input_rows": total_rows,
        "selector_groups": total_groups,
        "max_rows_per_selector_group": max_group_rows,
        "active_entries": active_entries,
    }


def _bucketize(
    candidate_paths: list[Path],
    *,
    staging: Path,
    partition_column: str,
    group_keys: list[str],
    bucket_count: int,
    telemetry_endpoint: dict[str, Any] | None,
) -> tuple[dict[tuple[str, int], list[Path]], int]:
    buckets: dict[tuple[str, int], list[Path]] = {}
    piece_index = 0
    total_row_groups = sum(
        pq.ParquetFile(candidate_path).metadata.num_row_groups
        for candidate_path in candidate_paths
    )
    completed_row_groups = 0
    emit_task_telemetry_event(
        telemetry_endpoint,
        "phase_planned",
        task_id="selector-bucketize",
        details={
            "phase_name": "build_sidecar.bucketize",
            "total": total_row_groups,
            "unit": "row_groups",
            "replace_total": True,
        },
    )
    for candidate_path in candidate_paths:
        parquet = pq.ParquetFile(candidate_path)
        for row_group in range(parquet.metadata.num_row_groups):
            for batch in parquet.iter_batches(batch_size=65_536, row_groups=[row_group]):
                frame = pl.from_arrow(batch)
                if frame.is_empty():
                    continue
                frame = frame.with_columns(
                    (frame.select(group_keys).hash_rows(seed=0) % bucket_count)
                    .cast(pl.UInt32)
                    .alias("__selector_bucket")
                )
                for key, piece in frame.partition_by(
                    [partition_column, "__selector_bucket"],
                    as_dict=True,
                    maintain_order=False,
                ).items():
                    partition_value, bucket_value = key
                    bucket_key = (str(partition_value), int(bucket_value))
                    bucket_dir = (
                        staging
                        / partition_dir_name(partition_value)
                        / f"bucket-{int(bucket_value):05d}"
                    )
                    bucket_dir.mkdir(parents=True, exist_ok=True)
                    path = bucket_dir / f"candidate-{piece_index:08d}.parquet"
                    piece.drop("__selector_bucket").write_parquet(
                        path, compression="uncompressed"
                    )
                    buckets.setdefault(bucket_key, []).append(path)
                    piece_index += 1
            completed_row_groups += 1
            emit_task_telemetry_event(
                telemetry_endpoint,
                "phase_progress",
                task_id="selector-bucketize",
                details={
                    "phase_name": "build_sidecar.bucketize",
                    "completed": completed_row_groups,
                    "total": total_row_groups,
                    "unit": "row_groups",
                },
            )
    return buckets, piece_index


def _select_bucket(
    paths: list[Path],
    *,
    output_path: Path,
    group_keys: list[str],
    sort: list[dict[str, Any]],
    memory_budget_bytes: int,
) -> dict[str, int]:
    sort_columns = [str(item.get("column") or "") for item in sort]
    descending = [str(item.get("direction") or "asc").lower() == "desc" for item in sort]
    nulls_last = [str(item.get("nulls") or "last").lower() == "last" for item in sort]
    ordered_columns = [
        *sort_columns,
        "__source_file",
        "__source_row_group",
        "__source_row_index",
    ]
    ordered_descending = [*descending, False, False, False]
    ordered_nulls_last = [*nulls_last, False, False, False]
    frame = pl.concat([pl.scan_parquet(path) for path in paths], how="diagonal_relaxed")
    if sum(path.stat().st_size for path in paths) > memory_budget_bytes:
        spill = write_sorted_intermediate(
            frame,
            root=output_path.parent / "spill",
            sort_columns=ordered_columns,
            descending=ordered_descending,
            nulls_last=ordered_nulls_last,
        )
        with spill:
            selected = (
                pl.scan_parquet(spill.path)
                .group_by(group_keys, maintain_order=True)
                .agg(pl.all().first(), pl.len().alias(GROUP_SIZE_COLUMN))
                .collect(engine="streaming")
            )
    else:
        selected = (
            frame.sort(
                ordered_columns,
                descending=ordered_descending,
                nulls_last=ordered_nulls_last,
            )
            .group_by(group_keys, maintain_order=True)
            .agg(pl.all().first(), pl.len().alias(GROUP_SIZE_COLUMN))
            .collect(engine="streaming")
        )
    sizes = selected.get_column(GROUP_SIZE_COLUMN)
    counters = {
        "selector_input_rows": int(sizes.sum() or 0),
        "selector_groups": selected.height,
        "max_rows_per_selector_group": int(sizes.max() or 0),
    }
    selected.drop(GROUP_SIZE_COLUMN).write_parquet(output_path, compression="uncompressed")
    return counters


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


if __name__ == "__main__":
    raise SystemExit(main())
