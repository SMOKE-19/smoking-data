from __future__ import annotations

import ctypes
import sys
from pathlib import Path


def current_rss_mb() -> float | None:
    if sys.platform.startswith("win"):
        return _windows_memory_mb(peak=False)
    return _linux_status_mb("VmRSS:")


def peak_rss_mb() -> float | None:
    if sys.platform.startswith("win"):
        return _windows_memory_mb(peak=True)
    return _linux_status_mb("VmHWM:")


def process_io_bytes() -> tuple[int, int] | None:
    if sys.platform.startswith("win"):
        return _windows_io_bytes()
    path = Path("/proc/self/io")
    if not path.is_file():
        return None
    try:
        values = {
            name.rstrip(":"): int(value)
            for name, value in (
                line.split(maxsplit=1) for line in path.read_text(encoding="utf-8").splitlines()
            )
        }
        # rchar/wchar include page-cache traffic and therefore describe the
        # bytes the task asked the OS to move, not only physical disk misses.
        return values.get("rchar", 0), values.get("wchar", 0)
    except (OSError, ValueError):
        return None


def _linux_status_mb(prefix: str) -> float | None:
    path = Path("/proc/self/status")
    if not path.is_file():
        return None
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith(prefix):
                return round(int(line.split()[1]) / 1024.0, 3)
    except (OSError, ValueError, IndexError):
        return None
    return None


def _windows_memory_mb(*, peak: bool) -> float | None:
    from ctypes import wintypes

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

    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        ok = psapi.GetProcessMemoryInfo(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        )
        if not ok:
            return None
        value = counters.PeakWorkingSetSize if peak else counters.WorkingSetSize
        return round(float(value) / (1024.0 * 1024.0), 3) if value else None
    except (AttributeError, OSError):
        return None


def _windows_io_bytes() -> tuple[int, int] | None:
    from ctypes import wintypes

    class IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        kernel32.GetProcessIoCounters.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(IoCounters),
        ]
        kernel32.GetProcessIoCounters.restype = wintypes.BOOL
        counters = IoCounters()
        ok = kernel32.GetProcessIoCounters(kernel32.GetCurrentProcess(), ctypes.byref(counters))
        if not ok:
            return None
        return int(counters.ReadTransferCount), int(counters.WriteTransferCount)
    except (AttributeError, OSError):
        return None
