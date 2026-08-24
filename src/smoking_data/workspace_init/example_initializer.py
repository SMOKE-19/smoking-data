from __future__ import annotations

from pathlib import Path
from typing import Any

from .template_resources import copy_template_tree


def initialize_workspace_examples(target: str | Path, *, force: bool = False) -> dict[str, Any]:
    """Create user-facing Definition examples without overwriting user files."""
    workspace_root = Path(target).expanduser().resolve()
    result = copy_template_tree(
        "examples",
        workspace_root / "examples",
        workspace_root=workspace_root,
        force=force,
    )
    return {"ok": True, "workspace_root": str(workspace_root), **result}
