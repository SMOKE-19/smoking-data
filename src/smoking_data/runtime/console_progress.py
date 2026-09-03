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
class ConsoleProgressRenderer:
    mode: str
    title: str = "smoking-data"
    stream: TextIO = field(default_factory=lambda: sys.stdout)
    phases: dict[str, _Phase] = field(default_factory=dict)
    active_tasks: dict[str, _Task] = field(default_factory=dict)
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
            if details.get("total") is not None:
                phase.total = max(0, int(details["total"]))
            phase.completed = max(phase.completed, int(details.get("completed") or 0))
            phase.unit = str(details.get("unit") or phase.unit)
            phase.status = "running"
            self._plain_event(event_name, phase)
        elif event_name == "phase_started" and phase_name:
            phase = self._phase(phase_name)
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
            phase = self._phase(phase_name or "materialize.fused")
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
            phase = self._phase(phase_name or (task.phase if task else "materialize.fused"))
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

    def _render_lines(self) -> list[str]:
        terminal_width = max(60, shutil.get_terminal_size(fallback=(100, 24)).columns)
        elapsed = _duration(time.monotonic() - self.started_at)
        current = next(
            (phase for phase in reversed(list(self.phases.values())) if phase.status == "running"),
            None,
        )
        overall = _overall_percent(list(self.phases.values()))
        bar = _progress_bar(overall, width=min(32, max(12, terminal_width - 52)))
        lines = [
            _truncate(self.title, terminal_width),
            "─" * terminal_width,
            f"Elapsed {elapsed}  Progress {bar} {overall:5.1f}% (estimate)"
            + (f"  · {current.name}" if current else ""),
            "",
            "Phases",
        ]
        for phase in self.phases.values():
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


def _overall_percent(phases: list[_Phase]) -> float:
    if not phases:
        return 0.0
    return sum(_phase_percent(phase) for phase in phases) / len(phases)


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
