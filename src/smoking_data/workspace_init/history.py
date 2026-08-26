from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

INIT_MANAGED_OUTPUTS = (
    ".vscode",
    ".smoking-data",
    ".agent",
    "for_agents",
    "schedules",
    "templates",
    "AGENTS.md",
)


def backup_init_outputs(
    target: str | Path,
) -> dict[str, Any]:
    """Snapshot existing init-managed outputs into one uncompressed history set."""

    return backup_paths(target, names=INIT_MANAGED_OUTPUTS)


def backup_paths(
    target: str | Path,
    *,
    names: tuple[str, ...],
) -> dict[str, Any]:
    """Snapshot selected workspace paths into one uncompressed history set."""

    workspace_root = Path(target).expanduser().resolve()
    existing = [workspace_root / name for name in names if (workspace_root / name).exists()]
    if not existing:
        return {"history_root": None, "backed_up": []}

    stamp = datetime.now().astimezone().strftime("%y%m%d_%H%M%S")
    history_root = workspace_root / ".history" / stamp
    collision = 1
    while history_root.exists():
        collision += 1
        history_root = workspace_root / ".history" / f"{stamp}_{collision:02d}"
    history_root.mkdir(parents=True, exist_ok=False)

    backed_up: list[str] = []
    for source in existing:
        destination = history_root / source.name
        if source.is_dir():
            shutil.copytree(source, destination)
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        backed_up.append(source.relative_to(workspace_root).as_posix())
    return {
        "history_root": history_root.relative_to(workspace_root).as_posix(),
        "backed_up": backed_up,
    }
