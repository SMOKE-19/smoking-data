from __future__ import annotations

import traceback
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STAGE_RESULT_SCHEMA_VERSION = "smoking-data.stage-result.v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class StageResult:
    ok: bool
    preset: str
    job_name: str
    yaml_path: Path | None = None
    metadata_path: Path | None = None
    output_paths: list[Path] = field(default_factory=list)
    counters: dict[str, int | float] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    error_type: str | None = None
    error_message: str | None = None
    traceback_tail: str | None = None
    started_at: str = field(default_factory=utc_now_iso)
    finished_at: str | None = None
    schema_version: str = STAGE_RESULT_SCHEMA_VERSION

    @classmethod
    def success(
        cls,
        *,
        preset: str,
        job_name: str,
        yaml_path: Path | None = None,
        metadata_path: Path | None = None,
        output_paths: list[Path] | None = None,
        counters: dict[str, int | float] | None = None,
        details: dict[str, Any] | None = None,
    ) -> StageResult:
        return cls(
            ok=True,
            preset=preset,
            job_name=job_name,
            yaml_path=yaml_path,
            metadata_path=metadata_path,
            output_paths=output_paths or [],
            counters=counters or {},
            details=details or {},
            finished_at=utc_now_iso(),
        )

    @classmethod
    def failure(
        cls,
        *,
        preset: str,
        job_name: str,
        exc: BaseException,
        yaml_path: Path | None = None,
        details: dict[str, Any] | None = None,
    ) -> StageResult:
        failure_details = dict(details or {})
        error_code = getattr(exc, "code", None)
        error_context = getattr(exc, "context", None)
        if error_code:
            failure_details["error_code"] = str(error_code)
        if isinstance(error_context, dict) and error_context:
            failure_details["error_context"] = error_context
        return cls(
            ok=False,
            preset=preset,
            job_name=job_name,
            yaml_path=yaml_path,
            details=failure_details,
            error_type=type(exc).__name__,
            error_message=str(exc),
            traceback_tail="\n".join(traceback.format_exception(exc)[-20:]),
            finished_at=utc_now_iso(),
        )

    def to_dict(self) -> dict[str, Any]:
        # Avoid dataclasses.asdict() here: it recursively deep-copies the full
        # physical plan and task receipts before to_json_safe() traverses them
        # a second time. Large ETL results only need one serialization pass.
        return to_json_safe(
            {
                "ok": self.ok,
                "preset": self.preset,
                "job_name": self.job_name,
                "yaml_path": self.yaml_path,
                "metadata_path": self.metadata_path,
                "output_paths": self.output_paths,
                "counters": self.counters,
                "details": self.details,
                "error_type": self.error_type,
                "error_message": self.error_message,
                "traceback_tail": self.traceback_tail,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "schema_version": self.schema_version,
            }
        )


def to_json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(to_json_safe(item) for item in value)
    if is_dataclass(value):
        return to_json_safe(asdict(value))
    if isinstance(value, dict):
        return {str(key): to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_safe(item) for item in value]
    return value
