from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from smoking_data.core.exceptions import TaskExecutionError

REQUEST_SCHEMA_VERSION = "smoking-data.selector-piece-request.v1"
RESULT_SCHEMA_VERSION = "smoking-data.selector-piece-result.v1"


def run_selector_piece_subprocess(
    request_path: Path, result_path: Path, *, timeout_sec: float = 3600.0
) -> dict[str, Any]:
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
        raise TaskExecutionError("0201 selector-piece subprocess timed out.") from exc
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise TaskExecutionError(
            "0201 selector-piece subprocess returned no valid result.",
            context={"exit_code": completed.returncode},
        ) from exc
    if (
        completed.returncode != 0
        or payload.get("schema_version") != RESULT_SCHEMA_VERSION
        or payload.get("status") != "completed"
    ):
        raise TaskExecutionError(
            "0201 selector-piece subprocess failed.",
            context={
                "exit_code": completed.returncode,
                "error_type": payload.get("error_type"),
                "error_message": payload.get("error_message"),
                "traceback_tail": payload.get("traceback_tail"),
            },
        )
    return payload
