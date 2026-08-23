from __future__ import annotations

from importlib import resources
from pathlib import Path

PACKAGED_WORKSPACE_RESOURCE = "_workspace"


def workspace_resource(*parts: str):
    """Return the editable product workspace or its wheel-packaged copy."""
    development_root = _development_workspace_root()
    if development_root is not None:
        node = development_root
    else:
        node = resources.files("smoking_data").joinpath(PACKAGED_WORKSPACE_RESOURCE)
    for part in parts:
        node = node.joinpath(part)
    return node


def workspace_text(*parts: str) -> str:
    return workspace_resource(*parts).read_text(encoding="utf-8")


def _development_workspace_root() -> Path | None:
    repository_root = Path(__file__).resolve().parents[2]
    candidate = repository_root / "workspace"
    if (repository_root / "pyproject.toml").is_file() and (
        candidate / "README.md"
    ).is_file():
        return candidate
    return None
