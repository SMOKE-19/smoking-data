from __future__ import annotations

import multiprocessing as mp
import os
import time
import traceback
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from smoking_data.core.tasks import TaskResult, TaskSpec
from smoking_data.runtime.memory import current_rss_mb, peak_rss_mb, process_io_bytes
from smoking_data.runtime.task_telemetry import (
    emit_task_telemetry_event,
    start_task_telemetry_supervisor,
    task_telemetry_phase,
)

TaskCallable = Callable[[TaskSpec], TaskResult]


@dataclass(frozen=True, slots=True)
class _TaskBatchResult:
    results: list[TaskResult]
    remaining_tasks: list[TaskSpec]
    recycle_reason: str | None = None
    recycle_rss_mb: float | None = None


def run_tasks_in_subprocesses(
    tasks: list[TaskSpec],
    *,
    worker: TaskCallable,
    workers: int = 1,
    max_tasks_per_child: int | None = None,
    max_child_rss_mb: float | None = None,
    task_batches: list[list[TaskSpec]] | None = None,
    return_profile: bool = False,
    telemetry_log_path: Path | None = None,
    telemetry_endpoint: dict[str, Any] | None = None,
    telemetry_sample_interval_sec: float = 0.5,
) -> list[TaskResult] | tuple[list[TaskResult], dict[str, Any]]:
    if not tasks:
        empty_profile = _build_empty_runner_profile(
            task_count=0,
            requested_workers=workers,
            max_tasks_per_child=max_tasks_per_child,
            max_child_rss_mb=max_child_rss_mb,
            telemetry_log_path=telemetry_log_path,
            telemetry_sample_interval_sec=telemetry_sample_interval_sec,
        )
        return ([], empty_profile) if return_profile else []
    max_workers = max(1, int(workers or 1))
    results: list[TaskResult] = []
    context = mp.get_context("spawn")
    explicit_batches = bool(task_batches)
    submission_mode = (
        "explicit_task_batches"
        if explicit_batches
        else ("single_task_futures" if max_tasks_per_child is None else "batched_generation")
    )
    runner_started = time.perf_counter()
    telemetry_handle = (
        start_task_telemetry_supervisor(
            log_path=telemetry_log_path,
            sample_interval_sec=telemetry_sample_interval_sec,
        )
        if telemetry_log_path is not None
        else None
    )
    effective_telemetry_endpoint = (
        telemetry_handle.endpoint if telemetry_handle is not None else telemetry_endpoint
    )
    indexed_tasks = [
        replace(
            task,
            payload={
                **task.payload,
                "__task_ordinal": ordinal,
                "__task_telemetry": effective_telemetry_endpoint,
            },
        )
        for ordinal, task in enumerate(tasks, start=1)
    ]
    explicit_indexed_batches: list[list[TaskSpec]] | None = None
    if task_batches is not None:
        task_id_map = {task.task_id: task for task in indexed_tasks}
        explicit_indexed_batches = [
            [task_id_map[task.task_id] for task in batch if task.task_id in task_id_map]
            for batch in task_batches
        ]
        explicit_indexed_batches = [batch for batch in explicit_indexed_batches if batch]
    generation_profiles: list[dict[str, Any]] = []
    total_submission_sec = 0.0
    total_wait_sec = 0.0
    total_executor_shutdown_sec = 0.0
    completion_ready_times: list[float] = []
    submitted_futures = 0
    recycle_events: list[dict[str, Any]] = []
    if explicit_indexed_batches is not None:
        pending_batches = list(explicit_indexed_batches)
        generation_index = 0
        batch_ordinal = 0
        while pending_batches:
            generation = pending_batches[:max_workers]
            pending_batches = pending_batches[max_workers:]
            submitted_generation: list[list[TaskSpec]] = []
            generation_submit_started = time.perf_counter()
            with ProcessPoolExecutor(
                max_workers=len(generation),
                mp_context=context,
            ) as executor:
                future_to_batch = {
                    executor.submit(
                        _run_task_batch,
                        worker,
                        [
                            replace(
                                task,
                                payload={
                                    **task.payload,
                                    "__submitted_at_ns": time.time_ns(),
                                    "__generation_index": generation_index,
                                    "__batch_index": batch_ordinal + batch_index,
                                    "__batch_size": len(batch),
                                    "__child_task_index": task_index,
                                },
                            )
                            for task_index, task in enumerate(batch, start=1)
                        ],
                        max_child_rss_mb,
                    ): batch
                    for batch_index, batch in enumerate(generation)
                }
                submitted_futures += len(future_to_batch)
                total_submission_sec += time.perf_counter() - generation_submit_started
                submitted_generation.extend(future_to_batch.values())
                generation_wait_started = time.perf_counter()
                for future in as_completed(future_to_batch):
                    completion_ready_times.append(time.perf_counter() - generation_wait_started)
                    batch = future_to_batch[future]
                    try:
                        batch_result = future.result()
                        results.extend(batch_result.results)
                        if batch_result.remaining_tasks:
                            pending_batches.append(batch_result.remaining_tasks)
                            recycle_events.append(
                                {
                                    "generation_index": generation_index,
                                    "completed_tasks": len(batch_result.results),
                                    "deferred_tasks": len(batch_result.remaining_tasks),
                                    "reason": batch_result.recycle_reason,
                                    "rss_mb": batch_result.recycle_rss_mb,
                                }
                            )
                    except BrokenProcessPool as exc:
                        results.extend(_abnormal_child_result(task, exc) for task in batch)
                total_wait_sec += time.perf_counter() - generation_wait_started
                executor_shutdown_started = time.perf_counter()
            total_executor_shutdown_sec += time.perf_counter() - executor_shutdown_started
            generation_profiles.append(
                {
                    "generation_index": generation_index,
                    "mode": submission_mode,
                    "submitted_batches": len(submitted_generation),
                    "submitted_tasks": sum(len(batch) for batch in submitted_generation),
                    "workers": len(generation),
                }
            )
            batch_ordinal += len(generation)
            generation_index += 1
    elif max_tasks_per_child is None:
        generation_submit_started = time.perf_counter()
        with ProcessPoolExecutor(max_workers=max_workers, mp_context=context) as executor:
            future_to_task = {
                executor.submit(
                    _run_task_guarded,
                    worker,
                    replace(
                        task,
                        payload={
                            **task.payload,
                            "__submitted_at_ns": time.time_ns(),
                            "__generation_index": 0,
                            "__batch_index": int(task.payload.get("__task_ordinal") or 0) - 1,
                            "__batch_size": 1,
                            "__child_task_index": 1,
                        },
                    ),
                ): task
                for task in indexed_tasks
            }
            submitted_futures = len(future_to_task)
            total_submission_sec += time.perf_counter() - generation_submit_started
            generation_wait_started = time.perf_counter()
            for future in as_completed(future_to_task):
                completion_ready_times.append(time.perf_counter() - generation_wait_started)
                task = future_to_task[future]
                try:
                    results.append(future.result())
                except BrokenProcessPool as exc:
                    results.append(_abnormal_child_result(task, exc))
            total_wait_sec += time.perf_counter() - generation_wait_started
            executor_shutdown_started = time.perf_counter()
        total_executor_shutdown_sec += time.perf_counter() - executor_shutdown_started
        generation_profiles.append(
            {
                "generation_index": 0,
                "mode": submission_mode,
                "submitted_batches": len(indexed_tasks),
                "submitted_tasks": len(indexed_tasks),
                "workers": max_workers,
            }
        )
    else:
        batches = [
            indexed_tasks[index : index + max_tasks_per_child]
            for index in range(0, len(indexed_tasks), max_tasks_per_child)
        ]
        for generation_start in range(0, len(batches), max_workers):
            generation = batches[generation_start : generation_start + max_workers]
            generation_index = generation_start // max_workers
            submitted_generation: list[list[TaskSpec]] = []
            generation_submit_started = time.perf_counter()
            # Each submitted batch must get one fresh child and exit after at most N
            # logical tasks. Merely matching futures to max_workers is insufficient:
            # a fast child can otherwise dequeue a second batch before its peer starts.
            with ProcessPoolExecutor(
                max_workers=len(generation),
                mp_context=context,
                max_tasks_per_child=1,
            ) as executor:
                future_to_batch = {
                    executor.submit(
                        _run_task_batch,
                        worker,
                        [
                            replace(
                                task,
                                payload={
                                    **task.payload,
                                    "__submitted_at_ns": time.time_ns(),
                                    "__generation_index": generation_index,
                                    "__batch_index": generation_start + batch_index,
                                    "__batch_size": len(batch),
                                    "__child_task_index": task_index,
                                },
                            )
                            for task_index, task in enumerate(batch, start=1)
                        ],
                        None,
                    ): batch
                    for batch_index, batch in enumerate(generation)
                }
                submitted_futures += len(future_to_batch)
                total_submission_sec += time.perf_counter() - generation_submit_started
                submitted_generation.extend(future_to_batch.values())
                generation_wait_started = time.perf_counter()
                for future in as_completed(future_to_batch):
                    completion_ready_times.append(time.perf_counter() - generation_wait_started)
                    batch = future_to_batch[future]
                    try:
                        batch_result = future.result()
                        results.extend(batch_result.results)
                        if batch_result.remaining_tasks:
                            results.extend(
                                _abnormal_child_result(
                                    task,
                                    RuntimeError(
                                        "worker RSS recycle deferred a task outside adaptive batches"
                                    ),
                                )
                                for task in batch_result.remaining_tasks
                            )
                    except BrokenProcessPool as exc:
                        results.extend(_abnormal_child_result(task, exc) for task in batch)
                total_wait_sec += time.perf_counter() - generation_wait_started
                executor_shutdown_started = time.perf_counter()
            total_executor_shutdown_sec += time.perf_counter() - executor_shutdown_started
            generation_profiles.append(
                {
                    "generation_index": generation_index,
                    "mode": submission_mode,
                    "submitted_batches": len(submitted_generation),
                    "submitted_tasks": sum(len(batch) for batch in submitted_generation),
                    "workers": len(generation),
                }
            )
    results = sorted(results, key=lambda item: item.task_id)
    task_telemetry_profile = telemetry_handle.stop() if telemetry_handle is not None else None
    runner_profile = _build_runner_profile(
        task_count=len(tasks),
        requested_workers=workers,
        admitted_workers=max_workers,
        max_tasks_per_child=max_tasks_per_child,
        submission_mode=submission_mode,
        generation_profiles=generation_profiles,
        submission_sec=total_submission_sec,
        wait_sec=total_wait_sec,
        executor_shutdown_sec=total_executor_shutdown_sec,
        completion_ready_times=completion_ready_times,
        total_elapsed_sec=time.perf_counter() - runner_started,
        task_results=results,
        submitted_futures=submitted_futures,
        max_child_rss_mb=max_child_rss_mb,
        recycle_events=recycle_events,
        task_telemetry=task_telemetry_profile,
    )
    return (results, runner_profile) if return_profile else results


def _run_task_batch(
    worker: TaskCallable,
    tasks: list[TaskSpec],
    max_child_rss_mb: float | None = None,
) -> _TaskBatchResult:
    batch_started_ns = time.time_ns()
    results: list[TaskResult] = []
    for task_index, task in enumerate(tasks, start=1):
        result = _run_task_guarded(
            worker,
            task,
            batch_started_ns=batch_started_ns,
            batch_size=len(tasks),
            child_task_index=task_index,
        )
        results.append(result)
        rss_mb = current_rss_mb()
        if (
            task_index < len(tasks)
            and max_child_rss_mb is not None
            and rss_mb is not None
            and rss_mb >= max_child_rss_mb
        ):
            results[-1] = replace(
                result,
                counters={
                    **result.counters,
                    "child_recycle_requested": 1,
                    "child_recycle_rss_mb": rss_mb,
                },
            )
            return _TaskBatchResult(
                results=results,
                remaining_tasks=tasks[task_index:],
                recycle_reason="rss_limit",
                recycle_rss_mb=rss_mb,
            )
    return _TaskBatchResult(results=results, remaining_tasks=[])


def _run_task_guarded(
    worker: TaskCallable,
    task: TaskSpec,
    *,
    batch_started_ns: int | None = None,
    batch_size: int | None = None,
    child_task_index: int | None = None,
) -> TaskResult:
    pid = os.getpid()
    started_ns = time.time_ns()
    started = time.perf_counter()
    cpu_started = time.process_time()
    io_started = process_io_bytes()
    rss_start = current_rss_mb()
    telemetry_endpoint = task.payload.get("__task_telemetry")
    telemetry_phase_only = bool(task.payload.get("__telemetry_phase_only", False))
    telemetry_details = {
        "task_ordinal": int(task.payload.get("__task_ordinal") or 0),
        "generation_index": int(task.payload.get("__generation_index") or 0),
        "batch_index": int(task.payload.get("__batch_index") or 0),
    }
    if not telemetry_phase_only:
        emit_task_telemetry_event(
            telemetry_endpoint,
            "worker_registered",
            task_id=task.task_id,
            details=telemetry_details,
        )
        emit_task_telemetry_event(
            telemetry_endpoint,
            "task_started",
            task_id=task.task_id,
            details=telemetry_details,
        )
    print(f"[task pid={pid}] start task={task.task_id}", flush=True)
    submitted_at_ns = int(task.payload.get("__submitted_at_ns") or 0)
    child_start_latency_sec = None
    queue_wait_sec = None
    if submitted_at_ns:
        queue_wait_sec = max(0.0, (started_ns - submitted_at_ns) / 1_000_000_000)
        if batch_started_ns is not None:
            child_start_latency_sec = max(
                0.0,
                (batch_started_ns - submitted_at_ns) / 1_000_000_000,
            )
    try:
        phase_name = str(task.payload.get("__telemetry_phase_name") or "").strip()
        if phase_name:
            with task_telemetry_phase(
                telemetry_endpoint,
                phase_name,
                task_id=task.task_id,
            ):
                result = worker(task)
        else:
            result = worker(task)
        counters = dict(result.counters)
        counters.update(
            {
                "task_ordinal": int(task.payload.get("__task_ordinal") or 0),
                "elapsed_sec": time.perf_counter() - started,
                "cpu_sec": time.process_time() - cpu_started,
                "generation_index": int(task.payload.get("__generation_index") or 0),
                "batch_index": int(task.payload.get("__batch_index") or 0),
                "child_batch_size": int(batch_size or task.payload.get("__batch_size") or 1),
                "child_task_index": int(
                    child_task_index or task.payload.get("__child_task_index") or 1
                ),
            }
        )
        if queue_wait_sec is not None:
            counters["queue_wait_sec"] = queue_wait_sec
        if child_start_latency_sec is not None:
            counters["child_start_latency_sec"] = child_start_latency_sec
        _add_io_counters(counters, io_started)
        for name, value in (
            ("rss_start_mb", rss_start),
            ("rss_end_mb", current_rss_mb()),
            ("rss_peak_mb", peak_rss_mb()),
        ):
            if value is not None:
                counters[name] = value
        result = replace(result, counters=counters)
        if not telemetry_phase_only:
            emit_task_telemetry_event(
                telemetry_endpoint,
                "task_finished",
                task_id=task.task_id,
                details={**telemetry_details, "ok": result.ok},
            )
        print(f"[task pid={pid}] finish task={task.task_id} ok={result.ok}", flush=True)
        return result
    except Exception as exc:  # pragma: no cover - exercised through subprocess boundary.
        print(f"[task pid={pid}] finish task={task.task_id} ok=False", flush=True)
        counters: dict[str, int | float] = {
            "task_ordinal": int(task.payload.get("__task_ordinal") or 0),
            "elapsed_sec": time.perf_counter() - started,
            "cpu_sec": time.process_time() - cpu_started,
            "generation_index": int(task.payload.get("__generation_index") or 0),
            "batch_index": int(task.payload.get("__batch_index") or 0),
            "child_batch_size": int(batch_size or task.payload.get("__batch_size") or 1),
            "child_task_index": int(
                child_task_index or task.payload.get("__child_task_index") or 1
            ),
        }
        if queue_wait_sec is not None:
            counters["queue_wait_sec"] = queue_wait_sec
        if child_start_latency_sec is not None:
            counters["child_start_latency_sec"] = child_start_latency_sec
        _add_io_counters(counters, io_started)
        for name, value in (
            ("rss_start_mb", rss_start),
            ("rss_end_mb", current_rss_mb()),
            ("rss_peak_mb", peak_rss_mb()),
        ):
            if value is not None:
                counters[name] = value
        if not telemetry_phase_only:
            emit_task_telemetry_event(
                telemetry_endpoint,
                "task_finished",
                task_id=task.task_id,
                details={**telemetry_details, "ok": False, "error_type": type(exc).__name__},
            )
        return TaskResult(
            task_id=task.task_id,
            ok=False,
            pid=pid,
            partition_value=task.partition_value,
            part_index=task.part_index,
            error_type=type(exc).__name__,
            error_message=str(exc),
            traceback_tail="\n".join(traceback.format_exception(exc)[-20:]),
            counters=counters,
        )


def _abnormal_child_result(task: TaskSpec, exc: BaseException) -> TaskResult:
    return TaskResult(
        task_id=task.task_id,
        ok=False,
        pid=0,
        partition_value=task.partition_value,
        part_index=task.part_index,
        error_type="ChildProcessAbnormalExit",
        error_message=str(exc),
        traceback_tail=None,
        counters={
            "task_ordinal": int(task.payload.get("__task_ordinal") or 0),
            "child_abnormal_exit": 1,
        },
    )


def _add_io_counters(
    counters: dict[str, int | float],
    started: tuple[int, int] | None,
) -> None:
    finished = process_io_bytes()
    if started is None or finished is None:
        return
    counters["io_read_bytes"] = max(0, finished[0] - started[0])
    counters["io_write_bytes"] = max(0, finished[1] - started[1])


def echo_task_worker(task: TaskSpec) -> TaskResult:
    return TaskResult(
        task_id=task.task_id,
        ok=True,
        pid=os.getpid(),
        partition_value=task.partition_value,
        part_index=task.part_index,
        counters={"payload_keys": len(task.payload)},
    )


def _build_empty_runner_profile(
    *,
    task_count: int,
    requested_workers: int,
    max_tasks_per_child: int | None,
    max_child_rss_mb: float | None,
    telemetry_log_path: Path | None,
    telemetry_sample_interval_sec: float,
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "task_count": task_count,
        "requested_workers": max(1, int(requested_workers or 1)),
        "admitted_workers": 0,
        "max_tasks_per_child": max_tasks_per_child,
        "max_child_rss_mb": max_child_rss_mb,
        "rss_recycle_count": 0,
        "rss_recycle_events": [],
        "submission_mode": (
            "single_task_futures" if max_tasks_per_child is None else "batched_generation"
        ),
        "submitted_futures": 0,
        "generation_count": 0,
        "generation_profiles": [],
        "submission_sec": 0.0,
        "wait_sec": 0.0,
        "executor_shutdown_sec": 0.0,
        "completion_skew_sec": 0.0,
        "first_completion_ready_sec": 0.0,
        "total_elapsed_sec": 0.0,
    }
    if telemetry_log_path is not None:
        profile["task_telemetry"] = {
            "schema_version": "smoking-data.task-telemetry.v1",
            "status": "not_applicable",
            "sample_interval_sec": telemetry_sample_interval_sec,
            "log_path": str(telemetry_log_path),
            "workers_observed": 0,
            "tasks_observed": 0,
        }
    return profile


def _build_runner_profile(
    *,
    task_count: int,
    requested_workers: int,
    admitted_workers: int,
    max_tasks_per_child: int | None,
    submission_mode: str,
    generation_profiles: list[dict[str, Any]],
    submission_sec: float,
    wait_sec: float,
    executor_shutdown_sec: float,
    completion_ready_times: list[float],
    total_elapsed_sec: float,
    task_results: list[TaskResult],
    submitted_futures: int,
    max_child_rss_mb: float | None,
    recycle_events: list[dict[str, Any]],
    task_telemetry: dict[str, Any] | None,
) -> dict[str, Any]:
    queue_wait_values = [
        float(result.counters["queue_wait_sec"])
        for result in task_results
        if "queue_wait_sec" in result.counters
    ]
    child_start_values = [
        float(result.counters["child_start_latency_sec"])
        for result in task_results
        if "child_start_latency_sec" in result.counters
    ]
    profile = {
        "task_count": task_count,
        "requested_workers": max(1, int(requested_workers or 1)),
        "admitted_workers": admitted_workers,
        "max_tasks_per_child": max_tasks_per_child,
        "max_child_rss_mb": max_child_rss_mb,
        "rss_recycle_count": len(recycle_events),
        "rss_recycle_events": recycle_events,
        "submission_mode": submission_mode,
        "submitted_futures": submitted_futures,
        "generation_count": len(generation_profiles),
        "generation_profiles": generation_profiles,
        "submission_sec": submission_sec,
        "wait_sec": wait_sec,
        "executor_shutdown_sec": executor_shutdown_sec,
        "completion_skew_sec": _completion_skew_sec(completion_ready_times),
        "first_completion_ready_sec": _first_completion_ready_sec(completion_ready_times),
        "total_elapsed_sec": total_elapsed_sec,
        "queue_wait_sec": _summary_stats(queue_wait_values),
        "child_start_latency_sec": _summary_stats(child_start_values),
    }
    if task_telemetry is not None:
        profile["task_telemetry"] = task_telemetry
    return profile


def _summary_stats(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {"count": 0, "min": 0.0, "avg": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "min": min(values),
        "avg": sum(values) / len(values),
        "max": max(values),
    }


def _completion_skew_sec(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return max(values) - min(values)


def _first_completion_ready_sec(values: list[float]) -> float:
    if not values:
        return 0.0
    return min(values)
