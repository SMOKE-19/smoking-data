from __future__ import annotations

import json
import os
import platform
import secrets
import socket
import subprocess
import sys
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from smoking_data.runtime.paths import ensure_dir
from smoking_data.runtime.process_metrics import read_process_metrics

TASK_TELEMETRY_SCHEMA_VERSION = "smoking-data.task-telemetry.v1"
TASK_TELEMETRY_EVENT_VERSION = "smoking-data.task-telemetry-event.v1"
DEFAULT_SAMPLE_INTERVAL_SEC = 0.5


@dataclass(slots=True)
class TaskTelemetryHandle:
    process: subprocess.Popen[str] | None
    endpoint: dict[str, Any] | None
    log_path: Path | None
    ready_path: Path | None
    summary_path: Path | None
    start_profile: dict[str, Any]

    def stop(self) -> dict[str, Any]:
        if self.process is None or self.endpoint is None or self.summary_path is None:
            return dict(self.start_profile)
        profile: dict[str, Any] | None = None
        try:
            emit_task_telemetry_event(self.endpoint, "supervisor_stop", task_id=None)
            self.process.wait(timeout=10.0)
            if self.summary_path.is_file():
                received = json.loads(self.summary_path.read_text(encoding="utf-8"))
                if isinstance(received, dict):
                    profile = received
        except (json.JSONDecodeError, OSError, subprocess.TimeoutExpired):
            profile = None
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2.0)
        _unlink_control_file(self.ready_path)
        _unlink_control_file(self.summary_path)
        if profile is not None:
            return {**self.start_profile, **profile}
        return {
            **self.start_profile,
            "status": "report_failed",
            "failure_reason": "supervisor_summary_unavailable",
        }


def start_task_telemetry_supervisor(
    *,
    log_path: Path,
    sample_interval_sec: float = DEFAULT_SAMPLE_INTERVAL_SEC,
) -> TaskTelemetryHandle:
    sample_interval = min(5.0, max(0.05, float(sample_interval_sec)))
    try:
        ensure_dir(log_path.parent)
        token = secrets.token_hex(16)
        control_stem = f".{log_path.name}.{token[:12]}"
        ready_path = log_path.parent / f"{control_stem}.ready.json"
        summary_path = log_path.parent / f"{control_stem}.summary.json"
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "smoking_data.runtime.task_telemetry_worker",
                "--token",
                token,
                "--log-path",
                str(log_path),
                "--ready-path",
                str(ready_path),
                "--summary-path",
                str(summary_path),
                "--sample-interval-sec",
                str(sample_interval),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        deadline = time.monotonic() + 10.0
        while not ready_path.is_file() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        if not ready_path.is_file():
            process.terminate()
            process.wait(timeout=2.0)
            return _disabled_handle(log_path, "supervisor_start_timeout")
        ready = json.loads(ready_path.read_text(encoding="utf-8"))
        if not isinstance(ready, dict) or ready.get("status") != "ready":
            process.terminate()
            process.wait(timeout=2.0)
            return _disabled_handle(log_path, str(ready.get("failure_reason") or "start_failed"))
        endpoint = {
            "host": str(ready["host"]),
            "port": int(ready["port"]),
            "token": token,
        }
        return TaskTelemetryHandle(
            process=process,
            endpoint=endpoint,
            log_path=log_path,
            ready_path=ready_path,
            summary_path=summary_path,
            start_profile={
                "schema_version": TASK_TELEMETRY_SCHEMA_VERSION,
                "status": "running",
                "platform": platform.system().lower(),
                "sample_interval_sec": sample_interval,
                "log_path": str(log_path),
                "supervisor_start_method": "minimal_subprocess",
            },
        )
    except (json.JSONDecodeError, OSError, RuntimeError, ValueError) as exc:
        return _disabled_handle(log_path, f"{type(exc).__name__}: {exc}")


def emit_task_telemetry_event(
    endpoint: dict[str, Any] | None,
    event: str,
    *,
    task_id: str | None,
    details: dict[str, Any] | None = None,
) -> None:
    if not endpoint:
        return
    payload = {
        "schema_version": TASK_TELEMETRY_EVENT_VERSION,
        "token": endpoint.get("token"),
        "event": event,
        "timestamp_ns": time.time_ns(),
        "pid": os.getpid(),
        "task_id": task_id,
        "details": details or {},
    }
    try:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.sendto(encoded, (str(endpoint["host"]), int(endpoint["port"])))
    except (KeyError, OSError, TypeError, ValueError):
        return


@contextmanager
def task_telemetry_phase(
    endpoint: dict[str, Any] | None,
    phase_name: str,
    *,
    task_id: str | None = None,
    phase_id: str | None = None,
):
    """Emit a real execution boundary without making telemetry an ETL dependency."""
    resolved_phase_id = phase_id or f"{phase_name}:{task_id or os.getpid()}:{time.time_ns()}"
    details = {"phase_name": phase_name, "phase_id": resolved_phase_id}
    emit_task_telemetry_event(endpoint, "phase_started", task_id=task_id, details=details)
    try:
        yield
    except BaseException:
        emit_task_telemetry_event(
            endpoint,
            "phase_finished",
            task_id=task_id,
            details={**details, "ok": False},
        )
        raise
    else:
        emit_task_telemetry_event(
            endpoint,
            "phase_finished",
            task_id=task_id,
            details={**details, "ok": True},
        )


def _supervisor_loop(
    receiver: socket.socket,
    *,
    token: str,
    log_path: Path,
    sample_interval_sec: float,
) -> dict[str, Any]:
    event_counts: Counter[str] = Counter()
    workers: dict[tuple[int, str], dict[str, Any]] = {}
    pid_identities: dict[int, tuple[int, str]] = {}
    tasks: dict[str, dict[str, Any]] = {}
    phases: dict[str, dict[str, Any]] = {}
    samples_written = 0
    max_concurrent_tasks = 0
    metrics_available = False
    stopped = False
    stop_requested = False
    started_ns = time.time_ns()
    next_sample_at = time.monotonic() + sample_interval_sec
    log_stream = _open_jsonl(log_path)
    while not stopped:
        receiver.settimeout(
            0.02 if stop_requested else max(0.001, next_sample_at - time.monotonic())
        )
        try:
            encoded, _address = receiver.recvfrom(65_535)
            event = json.loads(encoded.decode("utf-8"))
        except socket.timeout:
            event = None
            if stop_requested:
                stopped = True
        except (UnicodeDecodeError, json.JSONDecodeError, OSError):
            event = None
        if isinstance(event, dict) and secrets.compare_digest(str(event.get("token") or ""), token):
            event_name = str(event.get("event") or "unknown")
            event_counts[event_name] += 1
            if event_name == "supervisor_stop":
                stop_requested = True
            else:
                metrics_available = (
                    _apply_task_event(event, workers, pid_identities, tasks, phases, log_stream)
                    or metrics_available
                )
                active_count = sum(
                    1 for task in tasks.values() if task.get("finished_at_ns") is None
                )
                max_concurrent_tasks = max(max_concurrent_tasks, active_count)
        if time.monotonic() >= next_sample_at:
            sample_count, sample_available, exited_count = _sample_workers(
                workers,
                pid_identities,
                tasks,
                phases,
                log_stream,
            )
            samples_written += sample_count
            event_counts["worker_exited"] += exited_count
            metrics_available = metrics_available or sample_available
            next_sample_at = time.monotonic() + sample_interval_sec

    sample_count, sample_available, exited_count = _sample_workers(
        workers,
        pid_identities,
        tasks,
        phases,
        log_stream,
    )
    samples_written += sample_count
    event_counts["worker_exited"] += exited_count
    metrics_available = metrics_available or sample_available

    for identity, worker in list(workers.items()):
        if worker.get("exited"):
            continue
        _write_jsonl(
            log_stream,
            {
                "schema_version": TASK_TELEMETRY_EVENT_VERSION,
                "event": "worker_exited",
                "timestamp_ns": time.time_ns(),
                "pid": identity[0],
                "process_creation_time": identity[1],
                "details": {"reason": "supervisor_stop", "last_metrics": worker.get("last")},
            },
        )
        event_counts["worker_exited"] += 1
    if log_stream is not None:
        try:
            log_stream.close()
        except OSError:
            pass
    return _build_summary(
        log_path=log_path,
        sample_interval_sec=sample_interval_sec,
        started_ns=started_ns,
        event_counts=event_counts,
        workers=workers,
        tasks=tasks,
        phases=phases,
        samples_written=samples_written,
        max_concurrent_tasks=max_concurrent_tasks,
        metrics_available=metrics_available,
        log_available=log_stream is not None,
    )


def _apply_task_event(
    event: dict[str, Any],
    workers: dict[tuple[int, str], dict[str, Any]],
    pid_identities: dict[int, tuple[int, str]],
    tasks: dict[str, dict[str, Any]],
    phases: dict[str, dict[str, Any]],
    log_stream: TextIO | None,
) -> bool:
    pid = int(event.get("pid") or 0)
    task_id = str(event.get("task_id") or "")
    metrics = read_process_metrics(pid)
    identity = _resolve_identity(pid, metrics, workers, pid_identities)
    worker = workers.setdefault(
        identity,
        {
            "first": metrics,
            "last": metrics,
            "samples": 0,
            "max_rss_mb": None,
            "max_peak_rss_mb": None,
            "exited": False,
        },
    )
    if metrics is not None:
        worker["last"] = metrics
        _update_worker_peaks(worker, metrics)
    event_name = str(event.get("event") or "unknown")
    timestamp_ns = int(event.get("timestamp_ns") or time.time_ns())
    details = event.get("details") or {}
    if event_name in {"phase_started", "phase_finished"}:
        phase_id = str(details.get("phase_id") or "")
        phase_name = str(details.get("phase_name") or "")
        if phase_id and phase_name:
            phase = phases.setdefault(
                phase_id,
                {
                    "phase_id": phase_id,
                    "phase_name": phase_name,
                    "task_id": task_id or None,
                    "pid": pid,
                    "process_creation_time": identity[1],
                    "started_at_ns": None,
                    "finished_at_ns": None,
                    "ok": None,
                    "first_metrics": None,
                    "last_metrics": None,
                    "max_rss_mb": None,
                    "max_peak_rss_mb": None,
                    "samples": 0,
                },
            )
            if event_name == "phase_started":
                phase["started_at_ns"] = timestamp_ns
                phase["first_metrics"] = metrics
            else:
                phase["finished_at_ns"] = timestamp_ns
                phase["last_metrics"] = metrics
                phase["ok"] = details.get("ok")
            _update_task_peaks(phase, metrics)
    if task_id and event_name in {"worker_registered", "task_started", "task_finished"}:
        task = tasks.setdefault(
            task_id,
            {
                "task_id": task_id,
                "pid": pid,
                "process_creation_time": identity[1],
                "started_at_ns": None,
                "finished_at_ns": None,
                "ok": None,
                "first_metrics": None,
                "last_metrics": None,
                "max_rss_mb": None,
                "max_peak_rss_mb": None,
                "samples": 0,
            },
        )
        if event_name == "task_started":
            task["started_at_ns"] = timestamp_ns
            task["first_metrics"] = metrics
        elif event_name == "task_finished":
            task["finished_at_ns"] = timestamp_ns
            task["last_metrics"] = metrics
            task["ok"] = (event.get("details") or {}).get("ok")
        _update_task_peaks(task, metrics)
    _write_jsonl(
        log_stream,
        {
            **{key: value for key, value in event.items() if key != "token"},
            "process_creation_time": identity[1],
            "metrics": metrics,
        },
    )
    return metrics is not None


def _sample_workers(
    workers: dict[tuple[int, str], dict[str, Any]],
    pid_identities: dict[int, tuple[int, str]],
    tasks: dict[str, dict[str, Any]],
    phases: dict[str, dict[str, Any]],
    log_stream: TextIO | None,
) -> tuple[int, bool, int]:
    count = 0
    available = False
    exited = 0
    for identity, worker in list(workers.items()):
        if worker.get("exited"):
            continue
        metrics = read_process_metrics(identity[0])
        if metrics is None or metrics.get("process_creation_time") != identity[1]:
            _write_jsonl(
                log_stream,
                {
                    "schema_version": TASK_TELEMETRY_EVENT_VERSION,
                    "event": "worker_exited",
                    "timestamp_ns": time.time_ns(),
                    "pid": identity[0],
                    "process_creation_time": identity[1],
                    "details": {"reason": "process_unavailable"},
                },
            )
            worker["exited"] = True
            exited += 1
            if pid_identities.get(identity[0]) == identity:
                pid_identities.pop(identity[0], None)
            continue
        available = True
        worker["last"] = metrics
        _update_worker_peaks(worker, metrics)
        worker["samples"] = int(worker.get("samples") or 0) + 1
        active_task_ids: list[str] = []
        for task_id, task in tasks.items():
            if task["pid"] != identity[0] or task.get("finished_at_ns") is not None:
                continue
            task["samples"] = int(task.get("samples") or 0) + 1
            task["last_metrics"] = metrics
            _update_task_peaks(task, metrics)
            active_task_ids.append(task_id)
        active_phase_ids: list[str] = []
        for phase_id, phase in phases.items():
            if phase["pid"] != identity[0] or phase.get("finished_at_ns") is not None:
                continue
            phase["samples"] = int(phase.get("samples") or 0) + 1
            phase["last_metrics"] = metrics
            _update_task_peaks(phase, metrics)
            active_phase_ids.append(phase_id)
        _write_jsonl(
            log_stream,
            {
                "schema_version": TASK_TELEMETRY_EVENT_VERSION,
                "event": "process_sample",
                "timestamp_ns": time.time_ns(),
                "pid": identity[0],
                "process_creation_time": identity[1],
                "task_ids": active_task_ids,
                "phase_ids": active_phase_ids,
                "metrics": metrics,
            },
        )
        count += 1
    return count, available, exited


def _resolve_identity(
    pid: int,
    metrics: dict[str, Any] | None,
    workers: dict[tuple[int, str], dict[str, Any]],
    pid_identities: dict[int, tuple[int, str]],
) -> tuple[int, str]:
    creation_time = str((metrics or {}).get("process_creation_time") or "unavailable")
    identity = (pid, creation_time)
    previous = pid_identities.get(pid)
    if previous is not None and previous != identity:
        workers.pop(previous, None)
    pid_identities[pid] = identity
    return identity


def _update_task_peaks(task: dict[str, Any], metrics: dict[str, Any] | None) -> None:
    if metrics is None:
        return
    for target, source in (("max_rss_mb", "rss_mb"), ("max_peak_rss_mb", "peak_rss_mb")):
        value = metrics.get(source)
        if value is not None:
            task[target] = max(float(task.get(target) or 0.0), float(value))


def _update_worker_peaks(worker: dict[str, Any], metrics: dict[str, Any]) -> None:
    for target, source in (("max_rss_mb", "rss_mb"), ("max_peak_rss_mb", "peak_rss_mb")):
        value = metrics.get(source)
        if value is not None:
            worker[target] = max(float(worker.get(target) or 0.0), float(value))


def _build_summary(
    *,
    log_path: Path,
    sample_interval_sec: float,
    started_ns: int,
    event_counts: Counter[str],
    workers: dict[tuple[int, str], dict[str, Any]],
    tasks: dict[str, dict[str, Any]],
    phases: dict[str, dict[str, Any]],
    samples_written: int,
    max_concurrent_tasks: int,
    metrics_available: bool,
    log_available: bool,
) -> dict[str, Any]:
    task_profiles = []
    for task in sorted(tasks.values(), key=lambda item: item["task_id"]):
        first = task.pop("first_metrics", None)
        last = task.pop("last_metrics", None)
        task_profiles.append(
            {
                **task,
                "elapsed_sec": _elapsed_sec(task.get("started_at_ns"), task.get("finished_at_ns")),
                "cpu_sec": _counter_delta(first, last, "cpu_sec"),
                "read_bytes": _counter_delta(first, last, "read_bytes"),
                "write_bytes": _counter_delta(first, last, "write_bytes"),
                "requested_read_bytes": _counter_delta(first, last, "requested_read_bytes"),
                "requested_write_bytes": _counter_delta(first, last, "requested_write_bytes"),
                "read_operation_count": _counter_delta(first, last, "read_operation_count"),
                "write_operation_count": _counter_delta(first, last, "write_operation_count"),
            }
        )
    worker_profiles = []
    for (pid, creation_time), worker in sorted(workers.items()):
        first = worker.get("first")
        last = worker.get("last")
        worker_profiles.append(
            {
                "pid": pid,
                "process_creation_time": creation_time,
                "samples": int(worker.get("samples") or 0),
                "max_rss_mb": worker.get("max_rss_mb"),
                "max_peak_rss_mb": worker.get("max_peak_rss_mb"),
                "cpu_sec": _counter_delta(first, last, "cpu_sec"),
                "read_bytes": _counter_delta(first, last, "read_bytes"),
                "write_bytes": _counter_delta(first, last, "write_bytes"),
                "requested_read_bytes": _counter_delta(first, last, "requested_read_bytes"),
                "requested_write_bytes": _counter_delta(first, last, "requested_write_bytes"),
                "read_operation_count": _counter_delta(first, last, "read_operation_count"),
                "write_operation_count": _counter_delta(first, last, "write_operation_count"),
            }
        )
    phase_profiles = []
    for phase in sorted(phases.values(), key=lambda item: (item["phase_name"], item["phase_id"])):
        first = phase.pop("first_metrics", None)
        last = phase.pop("last_metrics", None)
        phase_profiles.append(
            {
                **phase,
                "status": "completed" if phase.get("finished_at_ns") is not None else "incomplete",
                "elapsed_sec": _elapsed_sec(
                    phase.get("started_at_ns"), phase.get("finished_at_ns")
                ),
                "cpu_sec": _counter_delta(first, last, "cpu_sec"),
                "read_bytes": _counter_delta(first, last, "read_bytes"),
                "write_bytes": _counter_delta(first, last, "write_bytes"),
                "requested_read_bytes": _counter_delta(first, last, "requested_read_bytes"),
                "requested_write_bytes": _counter_delta(first, last, "requested_write_bytes"),
            }
        )
    phase_names = sorted({str(item["phase_name"]) for item in phase_profiles})
    return {
        "schema_version": TASK_TELEMETRY_SCHEMA_VERSION,
        "status": "completed" if metrics_available else "report_unavailable",
        "platform": platform.system().lower(),
        "sample_interval_sec": sample_interval_sec,
        "log_path": str(log_path),
        "log_status": "written" if log_available else "report_unavailable",
        "elapsed_sec": round((time.time_ns() - started_ns) / 1_000_000_000, 6),
        "events_received": dict(sorted(event_counts.items())),
        "samples_written": samples_written,
        "workers_observed": len(worker_profiles),
        "tasks_observed": len(task_profiles),
        "max_concurrent_tasks": max_concurrent_tasks,
        "worker_profiles": worker_profiles,
        "task_profiles": task_profiles,
        "phases_observed": len(phase_profiles),
        "phase_names": phase_names,
        "phase_profiles": phase_profiles,
    }


def _counter_delta(
    first: dict[str, Any] | None,
    last: dict[str, Any] | None,
    key: str,
) -> float | int | None:
    if first is None or last is None or first.get(key) is None or last.get(key) is None:
        return None
    return max(0, last[key] - first[key])


def _elapsed_sec(started_ns: Any, finished_ns: Any) -> float | None:
    if not isinstance(started_ns, int) or not isinstance(finished_ns, int):
        return None
    return round(max(0, finished_ns - started_ns) / 1_000_000_000, 6)


def _open_jsonl(path: Path) -> TextIO | None:
    try:
        return path.open("a", encoding="utf-8", buffering=1024 * 1024)
    except OSError:
        return None


def _write_jsonl(stream: TextIO | None, payload: dict[str, Any]) -> None:
    if stream is None:
        return
    try:
        stream.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    except OSError:
        return


def _disabled_handle(log_path: Path, reason: str) -> TaskTelemetryHandle:
    return TaskTelemetryHandle(
        process=None,
        endpoint=None,
        log_path=log_path,
        ready_path=None,
        summary_path=None,
        start_profile={
            "schema_version": TASK_TELEMETRY_SCHEMA_VERSION,
            "status": "report_unavailable",
            "platform": platform.system().lower(),
            "sample_interval_sec": DEFAULT_SAMPLE_INTERVAL_SEC,
            "log_path": str(log_path),
            "failure_reason": reason,
        },
    )


def _unlink_control_file(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        return
