from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from smoking_data.core.results import to_json_safe, utc_now_iso
from smoking_data.runtime.paths import ensure_dir

LOG_EVENT_SCHEMA_VERSION = "smoking-data.log-event.v1"


def append_stage_event(
    path: Path,
    *,
    event: str,
    preset: str,
    job_name: str,
    details: dict[str, Any] | None = None,
) -> None:
    ensure_dir(path.parent)
    payload = {
        "schema_version": LOG_EVENT_SCHEMA_VERSION,
        "timestamp": utc_now_iso(),
        "event": event,
        "preset": preset,
        "job_name": job_name,
        "pid": os.getpid(),
        "details": details or {},
    }
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(to_json_safe(payload), ensure_ascii=False) + "\n")
