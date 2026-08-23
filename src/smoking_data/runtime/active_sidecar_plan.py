from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from smoking_data.core.exceptions import TaskExecutionError

PLAN_SCHEMA_VERSION = "smoking-data.active-sidecar-plan.v2"
REQUEST_SCHEMA_VERSION = "smoking-data.active-sidecar-plan-request.v1"
RESULT_SCHEMA_VERSION = "smoking-data.active-sidecar-plan-result.v1"


def run_active_sidecar_plan_subprocess(
    request_path: Path,
    result_path: Path,
    *,
    timeout_sec: float = 3600.0,
) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "smoking_data.runtime.active_sidecar_plan_worker",
                "--request",
                str(request_path),
                "--result",
                str(result_path),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as exc:
        raise TaskExecutionError(
            "0201 active-sidecar-plan subprocess timed out.",
            context={"request_path": str(request_path), "timeout_sec": timeout_sec},
        ) from exc
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise TaskExecutionError(
            "0201 active-sidecar-plan subprocess returned no valid result.",
            context={"exit_code": completed.returncode, "result_path": str(result_path)},
        ) from exc
    if (
        completed.returncode != 0
        or payload.get("schema_version") != RESULT_SCHEMA_VERSION
        or payload.get("status") != "completed"
    ):
        raise TaskExecutionError(
            "0201 active-sidecar-plan subprocess failed.",
            context={
                "exit_code": completed.returncode,
                "error_type": payload.get("error_type"),
                "error_message": payload.get("error_message"),
                "traceback_tail": payload.get("traceback_tail"),
            },
        )
    return payload


def run_active_sidecar_pipeline_subprocess(
    request_path: Path,
    result_path: Path,
    *,
    timeout_sec: float = 3600.0,
) -> dict[str, Any]:
    """Run selector and replace its process image with the boundary planner."""
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "smoking_data.runtime.selector_piece_worker",
                "--request",
                str(request_path),
                "--result",
                str(result_path),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as exc:
        raise TaskExecutionError(
            "0201 active-sidecar pipeline timed out.",
            context={"request_path": str(request_path), "timeout_sec": timeout_sec},
        ) from exc
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise TaskExecutionError(
            "0201 active-sidecar pipeline returned no valid result.",
            context={"exit_code": completed.returncode, "result_path": str(result_path)},
        ) from exc
    if (
        completed.returncode != 0
        or payload.get("schema_version") != RESULT_SCHEMA_VERSION
        or payload.get("status") != "completed"
    ):
        raise TaskExecutionError(
            "0201 active-sidecar pipeline failed.",
            context={
                "exit_code": completed.returncode,
                "error_type": payload.get("error_type"),
                "error_message": payload.get("error_message"),
                "traceback_tail": payload.get("traceback_tail"),
            },
        )
    return payload


def load_active_sidecar_plan(path: Path) -> dict[str, Any]:
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TaskExecutionError(
            "0201 active-sidecar-plan manifest is unreadable.",
            context={"path": str(path)},
        ) from exc
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise TaskExecutionError(
            "0201 active-sidecar-plan manifest schema is unsupported.",
            context={"path": str(path), "schema_version": plan.get("schema_version")},
        )
    return plan
