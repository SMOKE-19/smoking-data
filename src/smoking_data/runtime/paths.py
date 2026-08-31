from __future__ import annotations

import hashlib
import shutil
from pathlib import Path


def resolve_project_path(value: str | Path, *, project_root: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def infer_project_root(path: str | Path) -> Path:
    """Find the workspace root used for relative config and data paths.

    An explicit ``--project-root`` remains authoritative.  Otherwise an Asset
    or Chain YAML is resolved against the nearest initialized workspace, which
    keeps CLI execution independent of the caller's current directory.
    """

    resolved = Path(path).expanduser().resolve()
    for parent in (resolved.parent, *resolved.parents):
        if (parent / ".smoking-data" / "config.yaml").is_file():
            return parent
        if parent.name == "settings":
            return parent.parent
        if (parent / "pyproject.toml").exists():
            return parent
    return resolved.parent


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def reset_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()
