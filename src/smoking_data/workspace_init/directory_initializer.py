from __future__ import annotations

from pathlib import Path
from typing import Any

from smoking_data.runtime.config import load_config

RUNTIME_DIRECTORY_KEYS = (
    "data_root",
    "temp_root",
    "metadata_root",
    "log_root",
    "schedule_root",
)


def initialize_runtime_directories(target: str | Path) -> dict[str, Any]:
    """Create configured runtime roots that resolve inside the workspace."""
    workspace_root = Path(target).expanduser().resolve()
    config = load_config(project_root=workspace_root)
    created: list[str] = []
    preserved: list[str] = []
    skipped_outside_workspace: list[dict[str, str]] = []

    for key in RUNTIME_DIRECTORY_KEYS:
        path = getattr(config, key).resolve()
        try:
            relative = path.relative_to(workspace_root).as_posix()
        except ValueError:
            skipped_outside_workspace.append({"key": key, "path": str(path)})
            continue
        if path.exists():
            if not path.is_dir():
                raise ValueError(f"paths.{key}가 디렉터리가 아닙니다: {path}")
            preserved.append(relative)
            continue
        path.mkdir(parents=True, exist_ok=False)
        created.append(relative)

    return {
        "ok": True,
        "workspace_root": str(workspace_root),
        "created": created,
        "preserved": preserved,
        "skipped_outside_workspace": skipped_outside_workspace,
    }
