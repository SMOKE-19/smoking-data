from __future__ import annotations

from pathlib import Path
from typing import Any

from .template_resources import copy_template_tree


def initialize_workspace_templates(
    target: str | Path,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Create user-facing Definition templates."""
    workspace_root = Path(target).expanduser().resolve()
    templates_root = workspace_root / "templates"
    result = copy_template_tree(
        "templates",
        templates_root,
        workspace_root=workspace_root,
        force=force,
    )
    return {
        "ok": True,
        "workspace_root": str(workspace_root),
        **result,
    }
