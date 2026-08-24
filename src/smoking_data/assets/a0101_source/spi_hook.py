"""Trusted local pre-query hook for SOURCE 0101 SPI adapters."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from .pipeline.models import SpiPrepareSpec


def run_spi_prepare_hook(
    spec: SpiPrepareSpec,
    *,
    project_root: str | Path,
    temp_root: str | Path,
) -> None:
    """Run the configured token/file preparation script without reading its output.

    The hook is intentionally a subprocess: the script may write a token to the
    location expected by the external SPI library, while Smoking Data never reads,
    transports, logs, or persists the token value.
    """

    root = Path(project_root).resolve()
    script = Path(spec.script_path).resolve()
    _require_project_local_script(script, root)
    lock_path = Path(temp_root).resolve() / "spi-pre-query.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with _acquire_run_lock(lock_path, timeout_sec=spec.lock_timeout_sec):
        try:
            subprocess.run(
                [sys.executable, str(script)],
                cwd=str(root),
                env=os.environ.copy(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
                timeout=spec.timeout_sec,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"SOURCE 0101 SPI pre-query script timed out after {spec.timeout_sec:g}s: {script}"
            ) from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"SOURCE 0101 SPI pre-query script failed with exit code {exc.returncode}: {script}"
            ) from exc


def _require_project_local_script(script: Path, project_root: Path) -> None:
    if script.suffix.lower() != ".py":
        raise ValueError("SOURCE 0101 SPI pre-query script must be a .py file.")
    try:
        script.relative_to(project_root)
    except ValueError as exc:
        raise ValueError(
            "SOURCE 0101 SPI pre-query script must resolve inside the project root."
        ) from exc
    if not script.is_file():
        raise FileNotFoundError(f"SOURCE 0101 SPI pre-query script does not exist: {script}")


class _RunLock:
    def __init__(self, path: Path) -> None:
        self.path = path

    def __enter__(self) -> "_RunLock":
        return self

    def __exit__(self, *_: object) -> None:
        self.path.rmdir()


def _acquire_run_lock(path: Path, *, timeout_sec: float) -> _RunLock:
    deadline = time.monotonic() + timeout_sec
    while True:
        try:
            path.mkdir()
            return _RunLock(path)
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"SOURCE 0101 SPI pre-query hook lock timed out: {path}"
                ) from None
            time.sleep(0.1)
