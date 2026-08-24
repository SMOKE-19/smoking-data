from __future__ import annotations

from pathlib import Path
from typing import Any

from .template_resources import template_text


def initialize_help(target: str | Path, *, force: bool = False) -> dict[str, Any]:
    workspace_root = Path(target).expanduser().resolve()
    help_path = workspace_root / ".smoking-data" / "HELP.md"
    help_path.parent.mkdir(parents=True, exist_ok=True)
    if help_path.exists() and not force:
        return {
            "ok": True,
            "created": False,
            "workspace_root": str(workspace_root),
            "help_path": str(help_path),
        }
    temporary = help_path.with_suffix(".md.tmp")
    temporary.write_text(template_text("smoking_data", "HELP.md"), encoding="utf-8")
    temporary.replace(help_path)
    return {
        "ok": True,
        "created": True,
        "workspace_root": str(workspace_root),
        "help_path": str(help_path),
    }
