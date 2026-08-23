from __future__ import annotations

import ctypes
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from smoking_data.runtime.process_metrics import read_process_metrics

RUN_TELEMETRY_SCHEMA_VERSION = "smoking-data.run-telemetry.v1"
RUN_TELEMETRY_EVENT_SCHEMA_VERSION = "smoking-data.run-telemetry-event.v1"


@dataclass(slots=True)
class RunTelemetryHandle:
    root_pid: int
    log_path: Path
    stop_event: threading.Event
    thread: threading.Thread
    state: dict[str, Any]

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        self.thread.join(timeout=5.0)
        if self.thread.is_alive():
            return {
                "schema_version": RUN_TELEMETRY_SCHEMA_VERSION,
                "status": "report_failed",
                "platform": _platform_name(),
                "failure_reason": "sampler_stop_timeout",
                "log_path": str(self.log_path),
            }
        return _build_summary(
            root_pid=self.root_pid,
            log_path=self.log_path,
            state=self.state,
        )


def start_run_telemetry(
    *,
    log_path: Path,
    root_pid: int | None = None,
    sample_interval_sec: float = 0.5,
) -> RunTelemetryHandle:
    if sample_interval_sec <= 0:
        raise ValueError("sample_interval_sec must be positive")
    root_pid = int(root_pid or os.getpid())
    log_path.parent.mkdir(parents=True, exist_ok=True)
    stop_event = threading.Event()
    state: dict[str, Any] = {
        "started_ns": time.time_ns(),
        "finished_ns": None,
        "sample_interval_sec": float(sample_interval_sec),
        "samples": 0,
        "peak_tree_rss_mb": None,
        "peak_process_count": 0,
        "processes": {},
        "error": None,
    }
    thread = threading.Thread(
        target=_sample_loop,
        kwargs={
            "root_pid": root_pid,
            "log_path": log_path,
            "sample_interval_sec": sample_interval_sec,
            "stop_event": stop_event,
            "state": state,
        },
        name="smoking-data-run-telemetry",
        daemon=True,
    )
    thread.start()
    return RunTelemetryHandle(root_pid, log_path, stop_event, thread, state)


def _sample_loop(
    *,
    root_pid: int,
    log_path: Path,
    sample_interval_sec: float,
    stop_event: threading.Event,
    state: dict[str, Any],
) -> None:
    try:
        with log_path.open("w", encoding="utf-8") as stream:
            while True:
                _sample_once(root_pid=root_pid, stream=stream, state=state)
                if stop_event.wait(sample_interval_sec):
                    _sample_once(root_pid=root_pid, stream=stream, state=state)
                    break
    except Exception as exc:  # noqa: BLE001 - report-only telemetry must not fail ETL.
        state["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        state["finished_ns"] = time.time_ns()


def _sample_once(*, root_pid: int, stream: Any, state: dict[str, Any]) -> None:
    timestamp_ns = time.time_ns()
    processes: list[dict[str, Any]] = []
    for pid in _process_tree_pids(root_pid):
        metrics = read_process_metrics(pid)
        if metrics is None:
            continue
        processes.append(metrics)
        identity = (int(metrics["pid"]), str(metrics["process_creation_time"]))
        record = state["processes"].setdefault(
            identity,
            {
                "pid": identity[0],
                "process_creation_time": identity[1],
                "first": metrics,
                "last": metrics,
                "samples": 0,
                "max_rss_mb": None,
                "max_peak_rss_mb": None,
            },
        )
        record["last"] = metrics
        record["samples"] += 1
        record["max_rss_mb"] = _max_optional(record["max_rss_mb"], metrics.get("rss_mb"))
        record["max_peak_rss_mb"] = _max_optional(
            record["max_peak_rss_mb"], metrics.get("peak_rss_mb")
        )
    tree_rss_mb = sum(float(item.get("rss_mb") or 0.0) for item in processes)
    state["samples"] += 1
    state["peak_tree_rss_mb"] = _max_optional(state["peak_tree_rss_mb"], tree_rss_mb)
    state["peak_process_count"] = max(state["peak_process_count"], len(processes))
    stream.write(
        json.dumps(
            {
                "schema_version": RUN_TELEMETRY_EVENT_SCHEMA_VERSION,
                "timestamp_ns": timestamp_ns,
                "root_pid": root_pid,
                "tree_rss_mb": round(tree_rss_mb, 3),
                "processes": processes,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    )
    stream.flush()


def _build_summary(
    *, root_pid: int, log_path: Path, state: dict[str, Any]
) -> dict[str, Any]:
    process_profiles = []
    for _, record in sorted(state["processes"].items()):
        first = record["first"]
        last = record["last"]
        process_profiles.append(
            {
                "pid": record["pid"],
                "process_creation_time": record["process_creation_time"],
                "samples": record["samples"],
                "max_rss_mb": record["max_rss_mb"],
                "max_peak_rss_mb": record["max_peak_rss_mb"],
                "cpu_sec": _counter_delta(first, last, "cpu_sec"),
                "read_bytes": _io_counter_delta(first, last, "read"),
                "write_bytes": _io_counter_delta(first, last, "write"),
                "read_operation_count": _counter_delta(
                    first, last, "read_operation_count"
                ),
                "write_operation_count": _counter_delta(
                    first, last, "write_operation_count"
                ),
            }
        )
    status = "completed" if process_profiles and state.get("error") is None else "report_unavailable"
    return {
        "schema_version": RUN_TELEMETRY_SCHEMA_VERSION,
        "status": status,
        "platform": _platform_name(),
        "root_pid": root_pid,
        "sample_interval_sec": state["sample_interval_sec"],
        "samples_written": int(state["samples"]),
        "elapsed_sec": round(
            (int(state.get("finished_ns") or time.time_ns()) - int(state["started_ns"]))
            / 1_000_000_000,
            6,
        ),
        "peak_tree_rss_mb": state["peak_tree_rss_mb"],
        "peak_process_count": int(state["peak_process_count"]),
        "processes_observed": len(process_profiles),
        "process_profiles": process_profiles,
        "aggregate_process_deltas": {
            key: sum(float(item.get(key) or 0.0) for item in process_profiles)
            for key in (
                "cpu_sec",
                "read_bytes",
                "write_bytes",
                "read_operation_count",
                "write_operation_count",
            )
        },
        "io_semantics": (
            "Linux requested rchar/wchar; Windows GetProcessIoCounters transfer bytes. "
            "Values describe process I/O and are not physical SSD utilization."
        ),
        "log_path": str(log_path),
        "failure_reason": state.get("error"),
    }


def _process_tree_pids(root_pid: int) -> list[int]:
    if sys.platform.startswith("win"):
        parent_by_pid = _windows_parent_by_pid()
    elif sys.platform.startswith("linux"):
        parent_by_pid = _linux_parent_by_pid()
    else:
        return [root_pid]
    children: dict[int, list[int]] = {}
    for pid, parent_pid in parent_by_pid.items():
        children.setdefault(parent_pid, []).append(pid)
    pending = [root_pid]
    seen: set[int] = set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        pending.extend(children.get(pid, []))
    return sorted(seen)


def _linux_parent_by_pid() -> dict[int, int]:
    result: dict[int, int] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            raw = (entry / "stat").read_text(encoding="utf-8")
            close = raw.rindex(")")
            fields = raw[close + 2 :].split()
            result[int(entry.name)] = int(fields[1])
        except (OSError, ValueError, IndexError):
            continue
    return result


def _windows_parent_by_pid() -> dict[int, int]:
    from ctypes import wintypes

    class ProcessEntry32W(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_size_t),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", wintypes.WCHAR * 260),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W)]
    kernel32.Process32FirstW.restype = wintypes.BOOL
    kernel32.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W)]
    kernel32.Process32NextW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    snapshot = kernel32.CreateToolhelp32Snapshot(0x00000002, 0)
    invalid_handle_value = ctypes.c_void_p(-1).value
    if not snapshot or snapshot == invalid_handle_value:
        return {}
    result: dict[int, int] = {}
    try:
        entry = ProcessEntry32W()
        entry.dwSize = ctypes.sizeof(entry)
        ok = kernel32.Process32FirstW(snapshot, ctypes.byref(entry))
        while ok:
            result[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
            ok = kernel32.Process32NextW(snapshot, ctypes.byref(entry))
    finally:
        kernel32.CloseHandle(snapshot)
    return result


def _counter_delta(first: dict[str, Any], last: dict[str, Any], key: str) -> float | int | None:
    before = first.get(key)
    after = last.get(key)
    if before is None or after is None:
        return None
    return max(0, after - before)


def _io_counter_delta(
    first: dict[str, Any], last: dict[str, Any], direction: str
) -> float | int | None:
    requested = f"requested_{direction}_bytes"
    transferred = f"{direction}_bytes"
    has_requested = first.get(requested) is not None and last.get(requested) is not None
    key = requested if has_requested else transferred
    return _counter_delta(first, last, key)


def _max_optional(left: Any, right: Any) -> float | None:
    values = [float(value) for value in (left, right) if value is not None]
    return max(values) if values else None


def _platform_name() -> str:
    if sys.platform.startswith("win"):
        return "windows"
    if sys.platform.startswith("linux"):
        return "linux"
    return sys.platform
