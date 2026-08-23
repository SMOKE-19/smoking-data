from __future__ import annotations

from pathlib import Path
from typing import Any

from smoking_data.runtime.config import load_config

from .template_resources import copy_template_tree


def initialize_schedule_examples(target: str | Path) -> dict[str, Any]:
    """Create the managed schedule starter without overwriting user files."""
    workspace_root = Path(target).expanduser().resolve()
    schedule_root = load_config(project_root=workspace_root).schedule_root.resolve()
    try:
        schedule_root.relative_to(workspace_root)
    except ValueError:
        return {
            "ok": True,
            "created": [],
            "preserved": [],
            "skipped_outside_workspace": str(schedule_root),
        }
    result = copy_template_tree(
        "schedules",
        schedule_root,
        workspace_root=workspace_root,
    )
    return {"ok": True, "workspace_root": str(workspace_root), **result}
