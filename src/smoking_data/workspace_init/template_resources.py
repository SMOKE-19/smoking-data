from __future__ import annotations

from pathlib import Path
from typing import Any

from smoking_data.workspace_resources import workspace_resource, workspace_text


def template_resource(*parts: str):
    return workspace_resource(*parts)


def template_text(*parts: str) -> str:
    return workspace_text(*parts)


def copy_template_tree(
    section: str,
    target_root: Path,
    *,
    workspace_root: Path,
    force: bool = False,
) -> dict[str, Any]:
    created: list[str] = []
    preserved: list[str] = []

    def visit(node, relative: Path) -> None:
        for child in sorted(node.iterdir(), key=lambda item: item.name):
            child_relative = relative / child.name
            if child.is_dir():
                visit(child, child_relative)
                continue
            target = target_root / child_relative
            target.parent.mkdir(parents=True, exist_ok=True)
            display = target.relative_to(workspace_root).as_posix()
            was_existing = target.exists()
            if was_existing and not force:
                preserved.append(display)
                continue
            temporary = target.with_suffix(target.suffix + ".tmp")
            temporary.write_bytes(child.read_bytes())
            temporary.replace(target)
            (preserved if was_existing else created).append(display)

    visit(template_resource(section), Path())
    return {"created": created, "preserved": preserved}
