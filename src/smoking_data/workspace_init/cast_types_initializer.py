from __future__ import annotations

from pathlib import Path
from typing import Any

from .template_resources import template_text


def initialize_cast_types(target: str | Path, *, force: bool = False) -> dict[str, Any]:
    """Install the cast-type contract in the init-managed runtime directory."""

    workspace_root = Path(target).expanduser().resolve()
    cast_types_path = workspace_root / ".smoking-data" / "CAST_TYPES.md"
    cast_types_path.parent.mkdir(parents=True, exist_ok=True)
    if cast_types_path.exists() and not force:
        return {
            "ok": True,
            "created": False,
            "workspace_root": str(workspace_root),
            "cast_types_path": str(cast_types_path),
        }
    temporary = cast_types_path.with_suffix(".md.tmp")
    temporary.write_text(template_text("smoking_data", "CAST_TYPES.md"), encoding="utf-8")
    temporary.replace(cast_types_path)
    return {
        "ok": True,
        "created": True,
        "workspace_root": str(workspace_root),
        "cast_types_path": str(cast_types_path),
    }
