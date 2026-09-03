from __future__ import annotations

import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from typing import Any, TextIO

PROGRESS_MODE_ENV = "SMOKING_DATA_CONSOLE_PROGRESS"
_TTY_REFRESH_SEC = 0.2


def configured_progress_mode() -> str:
    value = os.environ.get(PROGRESS_MODE_ENV)
    if value in {"tty", "plain", "off"}:
        return value
    return "off"


def worker_console_enabled() -> bool:
    return PROGRESS_MODE_ENV not in os.environ


@dataclass(slots=True)
class _Phase:
    name: str
    total: int | None = None
    completed: int = 0
    unit: str = "tasks"
    status: str = "pending"
    started_at: float | None = None
    elapsed_sec: float | None = None
    last_plain_percent: int = -1
    failed: bool = False


@dataclass(slots=True)
class _Task:
    task_id: str
    pid: int
    phase: str
    started_at: float
    rss_mb: float | None = None


@dataclass(slots=True)
class _Resources:
    process_count: int = 0
    cpu_percent: float | None = None
    cpu_cores: float | None = None
    host_cpu_percent: float | None = None
    rss_mb: float | None = None
    phase_peak_rss_mb: float | None = None
    execution_peak_rss_mb: float | None = None
    read_mib_sec: float | None = None
    write_mib_sec: float | None = None
    filesystem_used_bytes: int | None = None
    filesystem_free_bytes: int | None = None
    filesystem_total_bytes: int | None = None
    previous_timestamp_ns: int | None = None
    previous_cpu_sec: float | None = None
    previous_read_bytes: int | None = None
    previous_write_bytes: int | None = None


@dataclass(slots=True)
class ConsoleProgressRenderer:
    mode: str
    title: str = "smoking-data"
    stream: TextIO = field(default_factory=lambda: sys.stdout)
    phases: dict[str, _Phase] = field(default_factory=dict)
    active_tasks: dict[str, _Task] = field(default_factory=dict)
    resources: _Resources = field(default_factory=_Resources)
    current_phase_name: str | None = None
    started_at: float = field(default_factory=time.monotonic)
    _last_rendered_at: float = 0.0
    _rendered_lines: int = 0

    def handle(self, event: dict[str, Any]) -> None:
        if self.mode == "off":
            return
        event_name = str(event.get("event") or "")
        details = event.get("details") if isinstance(event.get("details"), dict) else {}
        phase_name = str(details.get("phase_name") or "")
        task_id = str(event.get("task_id") or "")
        pid = int(event.get("pid") or 0)

        if event_name == "phase_planned" and phase_name:
            phase = self._phase(phase_name)
            self._activate_phase(phase_name)
            total = max(0, int(details.get("total") or 0))
            if details.get("replace_total"):
                phase.total = total
                phase.completed = min(phase.completed, total)
            else:
                phase.total = (phase.total or 0) + total
            phase.unit = str(details.get("unit") or phase.unit)
            if total == 0 and details.get("skipped"):
                phase.status = "skipped"
            elif phase.total > phase.completed and phase.status in {"completed", "skipped"}:
                phase.status = "pending"
            self._plain_event(event_name, phase, force=True)
        elif event_name == "phase_progress" and phase_name:
            phase = self._phase(phase_name)
            self._activate_phase(phase_name)
            if details.get("total") is not None:
                phase.total = max(0, int(details["total"]))
            phase.completed = max(phase.completed, int(details.get("completed") or 0))
            phase.unit = str(details.get("unit") or phase.unit)
            phase.status = "running"
            self._plain_event(event_name, phase)
        elif event_name == "phase_started" and phase_name:
            phase = self._phase(phase_name)
            self._activate_phase(phase_name)
            phase.status = "running"
            phase.started_at = phase.started_at or time.monotonic()
            if task_id:
                self.active_tasks[task_id] = _Task(
                    task_id=task_id,
                    pid=pid,
                    phase=phase_name,
                    started_at=time.monotonic(),
                )
            if bool(details.get("counts_completion", True)):
                self._plain_event(event_name, phase, task_id=task_id, pid=pid, force=True)
        elif event_name == "phase_finished" and phase_name:
            phase = self._phase(phase_name)
            counts_completion = bool(details.get("counts_completion", True))
            phase.failed = phase.failed or details.get("ok") is False
            if phase.total is not None and counts_completion:
                phase.completed = min(phase.total, phase.completed + 1)
                if phase.completed >= phase.total:
                    phase.status = "failed" if phase.failed else "completed"
            elif not task_id:
                phase.completed = 1
                phase.total = 1
                phase.status = "failed" if phase.failed else "completed"
            phase.elapsed_sec = (
                time.monotonic() - phase.started_at if phase.started_at is not None else None
            )
            if task_id and counts_completion:
                self.active_tasks.pop(task_id, None)
            if counts_completion:
                self._plain_event(event_name, phase, task_id=task_id, pid=pid, force=True)
        elif event_name == "task_started" and task_id:
            phase = self._phase(phase_name or "materialize.01_payload")
            self._activate_phase(phase.name)
            phase.status = "running"
            phase.started_at = phase.started_at or time.monotonic()
            self.active_tasks[task_id] = _Task(
                task_id=task_id,
                pid=pid,
                phase=phase.name,
                started_at=time.monotonic(),
            )
            self._plain_task(event_name, task_id=task_id, pid=pid, phase=phase.name)
        elif event_name == "task_finished" and task_id:
            task = self.active_tasks.pop(task_id, None)
            phase = self._phase(
                phase_name or (task.phase if task else "materialize.01_payload")
            )
            phase.completed += 1
            if details.get("ok") is False:
                phase.failed = True
                phase.status = "failed"
            elif phase.total is not None and phase.completed >= phase.total:
                phase.completed = phase.total
                phase.status = "failed" if phase.failed else "completed"
            self._plain_task(
                event_name,
                task_id=task_id,
                pid=pid,
                phase=phase.name,
                ok=details.get("ok"),
            )
        elif event_name == "process_sample":
            metrics = event.get("metrics") if isinstance(event.get("metrics"), dict) else {}
            for active_id in event.get("task_ids") or []:
                task = self.active_tasks.get(str(active_id))
                if task is not None and metrics.get("rss_mb") is not None:
                    task.rss_mb = float(metrics["rss_mb"])
        elif event_name == "resource_sample":
            self._handle_resource_sample(event)

        if self.mode == "tty":
            self.render(force=event_name in {"phase_finished", "task_finished"})

    def render(self, *, force: bool = False) -> None:
        if self.mode != "tty":
            return
        now = time.monotonic()
        if not force and now - self._last_rendered_at < _TTY_REFRESH_SEC:
            return
        lines = self._render_lines()
        if self._rendered_lines:
            self.stream.write(f"\x1b[{self._rendered_lines}F\x1b[J")
        self.stream.write("\n".join(lines) + "\n")
        self.stream.flush()
        self._rendered_lines = len(lines)
        self._last_rendered_at = now

    def finish(self) -> None:
        if self.mode == "tty":
            self.render(force=True)

    def _phase(self, name: str) -> _Phase:
        return self.phases.setdefault(name, _Phase(name=name))

    def _activate_phase(self, name: str) -> None:
        if self.current_phase_name == name:
            return
        self.current_phase_name = name
        self.resources.phase_peak_rss_mb = self.resources.rss_mb

    def _handle_resource_sample(self, event: dict[str, Any]) -> None:
        metrics = event.get("metrics") if isinstance(event.get("metrics"), dict) else {}
        timestamp_ns = int(event.get("timestamp_ns") or 0)
        current_cpu = _optional_float(metrics.get("cpu_sec"))
        current_read = _optional_int(metrics.get("read_bytes"))
        current_write = _optional_int(metrics.get("write_bytes"))
        previous_ns = self.resources.previous_timestamp_ns
        elapsed_sec = (
            (timestamp_ns - previous_ns) / 1_000_000_000
            if timestamp_ns > 0 and previous_ns is not None and timestamp_ns > previous_ns
            else None
        )
        if elapsed_sec is not None and current_cpu is not None:
            previous_cpu = self.resources.previous_cpu_sec
            if previous_cpu is not None:
                cores = max(0.0, current_cpu - previous_cpu) / elapsed_sec
                self.resources.cpu_cores = cores
                self.resources.cpu_percent = cores * 100.0
                cpu_count = max(1, os.cpu_count() or 1)
                self.resources.host_cpu_percent = min(100.0, cores * 100.0 / cpu_count)
        if elapsed_sec is not None and current_read is not None:
            previous_read = self.resources.previous_read_bytes
            if previous_read is not None:
                self.resources.read_mib_sec = (
                    max(0, current_read - previous_read) / elapsed_sec / (1024 * 1024)
                )
        if elapsed_sec is not None and current_write is not None:
            previous_write = self.resources.previous_write_bytes
            if previous_write is not None:
                self.resources.write_mib_sec = (
                    max(0, current_write - previous_write) / elapsed_sec / (1024 * 1024)
                )
        self.resources.process_count = max(0, int(metrics.get("process_count") or 0))
        self.resources.rss_mb = _optional_float(metrics.get("rss_mb"))
        if self.resources.rss_mb is not None:
            self.resources.phase_peak_rss_mb = max(
                self.resources.phase_peak_rss_mb or 0.0,
                self.resources.rss_mb,
            )
            self.resources.execution_peak_rss_mb = max(
                self.resources.execution_peak_rss_mb or 0.0,
                self.resources.rss_mb,
            )
        filesystem = (
            metrics.get("filesystem") if isinstance(metrics.get("filesystem"), dict) else {}
        )
        self.resources.filesystem_used_bytes = _optional_int(filesystem.get("used_bytes"))
        self.resources.filesystem_free_bytes = _optional_int(filesystem.get("free_bytes"))
        self.resources.filesystem_total_bytes = _optional_int(filesystem.get("total_bytes"))
        self.resources.previous_timestamp_ns = timestamp_ns or previous_ns
        self.resources.previous_cpu_sec = current_cpu
        self.resources.previous_read_bytes = current_read
        self.resources.previous_write_bytes = current_write

    def _render_lines(self) -> list[str]:
        terminal_width = max(60, shutil.get_terminal_size(fallback=(100, 24)).columns)
        elapsed = _duration(time.monotonic() - self.started_at)
        current = (
            self.phases.get(self.current_phase_name) if self.current_phase_name is not None else None
        )
        current_percent = _phase_percent(current) if current is not None else 0.0
        bar = _progress_bar(current_percent, width=min(32, max(12, terminal_width - 58)))
        lines = [
            _truncate(self.title, terminal_width),
            "─" * terminal_width,
            f"Elapsed {elapsed}  Phase Progress {bar} {current_percent:5.1f}%"
            + (f"  · {current.name}" if current else ""),
            "",
            "Resources (1s aggregate)",
            _truncate(self._resource_cpu_line(), terminal_width),
            _truncate(self._resource_memory_line(), terminal_width),
            _truncate(self._resource_storage_line(), terminal_width),
            "",
            "Phases",
        ]
        for phase in sorted(self.phases.values(), key=lambda item: item.name):
            icon = {"completed": "✓", "running": "▶", "failed": "!", "skipped": "-"}.get(
                phase.status, "○"
            )
            percent = _phase_percent(phase)
            count = (
                f"{phase.completed:,}/{phase.total:,} {phase.unit}"
                if phase.total is not None
                else "calculating…"
            )
            lines.append(
                _truncate(
                    f"  {icon}  {phase.name:<34} {percent:6.1f}%  {count}",
                    terminal_width,
                )
            )
        lines.extend(["", "Active tasks", "  PID      Task                              Phase"])
        if not self.active_tasks:
            lines.append("  -        none")
        else:
            for task in sorted(self.active_tasks.values(), key=lambda item: (item.pid, item.task_id)):
                memory = f" {task.rss_mb:.0f} MiB" if task.rss_mb is not None else ""
                lines.append(
                    _truncate(
                        f"  {task.pid:<8} {task.task_id:<33} {task.phase}{memory}",
                        terminal_width,
                    )
                )
        return lines

    def _resource_cpu_line(self) -> str:
        resources = self.resources
        if resources.cpu_percent is None or resources.cpu_cores is None:
            return f"  CPU  calculating…  · processes {resources.process_count}"
        return (
            f"  CPU  {resources.cpu_percent:.0f}% · {resources.cpu_cores:.2f} cores"
            f" · host share {resources.host_cpu_percent or 0.0:.1f}%"
            f" · processes {resources.process_count}"
        )

    def _resource_memory_line(self) -> str:
        resources = self.resources
        if resources.rss_mb is None:
            return "  MEM  unavailable"
        return (
            f"  MEM  RSS {_format_mib(resources.rss_mb)}"
            f" · phase peak {_format_mib(resources.phase_peak_rss_mb)}"
            f" · run peak {_format_mib(resources.execution_peak_rss_mb)}"
        )

    def _resource_storage_line(self) -> str:
        resources = self.resources
        rates = "I/O calculating…"
        if resources.read_mib_sec is not None and resources.write_mib_sec is not None:
            rates = (
                f"read {resources.read_mib_sec:.1f} MiB/s"
                f" · write {resources.write_mib_sec:.1f} MiB/s"
            )
        capacity = "filesystem unavailable"
        if resources.filesystem_used_bytes is not None:
            capacity = (
                f"used {_format_bytes(resources.filesystem_used_bytes)}"
                f" · free {_format_bytes(resources.filesystem_free_bytes)}"
            )
        return f"  SSD  {rates} · {capacity}"

    def _plain_event(
        self,
        event_name: str,
        phase: _Phase,
        *,
        task_id: str = "",
        pid: int = 0,
        force: bool = False,
    ) -> None:
        if self.mode != "plain":
            return
        percent = int(_phase_percent(phase))
        if event_name == "phase_progress" and not force:
            bucket = percent // 10
            if bucket <= phase.last_plain_percent:
                return
            phase.last_plain_percent = bucket
        count = (
            f" completed={phase.completed} total={phase.total} unit={phase.unit}"
            if phase.total is not None
            else ""
        )
        task = f" task={task_id} pid={pid}" if task_id else ""
        print(
            f"[smoking-data] {event_name} phase={phase.name}{count} "
            f"percent={_phase_percent(phase):.1f}{task}",
            file=self.stream,
            flush=True,
        )

    def _plain_task(
        self,
        event_name: str,
        *,
        task_id: str,
        pid: int,
        phase: str,
        ok: Any = None,
    ) -> None:
        if self.mode != "plain":
            return
        outcome = f" ok={str(bool(ok)).lower()}" if ok is not None else ""
        print(
            f"[smoking-data] {event_name} phase={phase} task={task_id} pid={pid}{outcome}",
            file=self.stream,
            flush=True,
        )


def _phase_percent(phase: _Phase) -> float:
    if phase.status in {"completed", "skipped"}:
        return 100.0
    if phase.total is None or phase.total <= 0:
        return 0.0
    return min(100.0, 100.0 * phase.completed / phase.total)


def _progress_bar(percent: float, *, width: int) -> str:
    completed = min(width, max(0, round(width * percent / 100)))
    return "[" + "█" * completed + "░" * (width - completed) + "]"


def _duration(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _truncate(value: str, width: int) -> str:
    return value if len(value) <= width else value[: max(0, width - 1)] + "…"


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _format_mib(value: float | None) -> str:
    return "unavailable" if value is None else f"{value:,.0f} MiB"


def _format_bytes(value: int | None) -> str:
    if value is None:
        return "unavailable"
    scaled = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(scaled) < 1024.0 or unit == "TiB":
            return f"{scaled:,.1f} {unit}"
        scaled /= 1024.0
    return f"{scaled:,.1f} TiB"
