from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path
from typing import Any


def read_process_metrics(pid: int) -> dict[str, Any] | None:
    """Read one process without mutating or controlling it."""

    if pid <= 0:
        return None
    if sys.platform.startswith("win"):
        return _read_windows_process_metrics(pid)
    if sys.platform.startswith("linux"):
        return _read_linux_process_metrics(pid)
    return None


def _read_linux_process_metrics(pid: int) -> dict[str, Any] | None:
    root = Path("/proc") / str(pid)
    try:
        stat_text = (root / "stat").read_text(encoding="utf-8")
        status_lines = (root / "status").read_text(encoding="utf-8").splitlines()
        io_lines = (root / "io").read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
        return None

    close_paren = stat_text.rfind(")")
    if close_paren < 0:
        return None
    stat_fields = stat_text[close_paren + 2 :].split()
    if len(stat_fields) < 20:
        return None
    status = _colon_int_values(status_lines)
    io = _colon_int_values(io_lines)
    try:
        clock_ticks = float(os.sysconf("SC_CLK_TCK"))
        cpu_sec = (int(stat_fields[11]) + int(stat_fields[12])) / clock_ticks
        process_creation_time = f"linux-start-ticks:{int(stat_fields[19])}"
    except (OSError, ValueError, IndexError):
        return None
    return {
        "platform": "linux",
        "pid": pid,
        "process_creation_time": process_creation_time,
        "rss_mb": _kb_to_mb(status.get("VmRSS")),
        "peak_rss_mb": _kb_to_mb(status.get("VmHWM")),
        "cpu_sec": round(cpu_sec, 6),
        "read_bytes": int(io.get("read_bytes", 0)),
        "write_bytes": int(io.get("write_bytes", 0)),
        "read_operation_count": int(io.get("syscr", 0)),
        "write_operation_count": int(io.get("syscw", 0)),
        "requested_read_bytes": int(io.get("rchar", 0)),
        "requested_write_bytes": int(io.get("wchar", 0)),
    }


def _read_windows_process_metrics(pid: int) -> dict[str, Any] | None:
    from ctypes import wintypes

    class FileTime(ctypes.Structure):
        _fields_ = [("low", wintypes.DWORD), ("high", wintypes.DWORD)]

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    class IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    process_query_limited_information = 0x1000
    process_vm_read = 0x0010
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(
            process_query_limited_information | process_vm_read,
            False,
            pid,
        )
        if not handle:
            return None
        try:
            memory = ProcessMemoryCounters()
            memory.cb = ctypes.sizeof(memory)
            psapi.GetProcessMemoryInfo.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(ProcessMemoryCounters),
                wintypes.DWORD,
            ]
            psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
            if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(memory), memory.cb):
                return None

            creation = FileTime()
            exit_time = FileTime()
            kernel_time = FileTime()
            user_time = FileTime()
            kernel32.GetProcessTimes.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(FileTime),
                ctypes.POINTER(FileTime),
                ctypes.POINTER(FileTime),
                ctypes.POINTER(FileTime),
            ]
            kernel32.GetProcessTimes.restype = wintypes.BOOL
            if not kernel32.GetProcessTimes(
                handle,
                ctypes.byref(creation),
                ctypes.byref(exit_time),
                ctypes.byref(kernel_time),
                ctypes.byref(user_time),
            ):
                return None

            io = IoCounters()
            kernel32.GetProcessIoCounters.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(IoCounters),
            ]
            kernel32.GetProcessIoCounters.restype = wintypes.BOOL
            if not kernel32.GetProcessIoCounters(handle, ctypes.byref(io)):
                return None
            creation_ticks = _filetime_ticks(creation)
            cpu_sec = (_filetime_ticks(kernel_time) + _filetime_ticks(user_time)) / 10_000_000
            return {
                "platform": "windows",
                "pid": pid,
                "process_creation_time": f"windows-filetime:{creation_ticks}",
                "rss_mb": round(memory.WorkingSetSize / (1024 * 1024), 3),
                "peak_rss_mb": round(memory.PeakWorkingSetSize / (1024 * 1024), 3),
                "cpu_sec": round(cpu_sec, 6),
                "read_bytes": int(io.ReadTransferCount),
                "write_bytes": int(io.WriteTransferCount),
                "read_operation_count": int(io.ReadOperationCount),
                "write_operation_count": int(io.WriteOperationCount),
            }
        finally:
            kernel32.CloseHandle(handle)
    except (AttributeError, OSError, ValueError):
        return None


def _filetime_ticks(value: ctypes.Structure) -> int:
    return (int(value.high) << 32) | int(value.low)  # type: ignore[attr-defined]


def _colon_int_values(lines: list[str]) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in lines:
        name, separator, raw_value = line.partition(":")
        if not separator:
            continue
        try:
            token = raw_value.strip().split(maxsplit=1)[0]
            values[name] = int(token)
        except (ValueError, IndexError):
            continue
    return values


def _kb_to_mb(value: int | None) -> float | None:
    if value is None:
        return None
    return round(value / 1024.0, 3)
