from __future__ import annotations

from pathlib import Path
from typing import Any

from .template_resources import template_text

OBJECT_STORE_CONFIG_TEMPLATE = template_text("smoking_data", "object-stores.yaml")
SMOKING_DATA_GITIGNORE_TEMPLATE = template_text("smoking_data", "gitignore")


def initialize_runtime_config(target: str | Path) -> dict[str, Any]:
    workspace_root = Path(target).expanduser().resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    runtime_root = workspace_root / ".smoking-data"
    ignore_path = runtime_root / ".gitignore"
    runtime_root.mkdir(parents=True, exist_ok=True)
    ignored = (
        ignore_path.read_text(encoding="utf-8").splitlines() if ignore_path.exists() else []
    )
    for entry in SMOKING_DATA_GITIGNORE_TEMPLATE.splitlines():
        entry = entry.strip()
        if not entry:
            continue
        if entry not in ignored:
            ignored.append(entry)
    ignore_path.write_text("\n".join(ignored) + "\n", encoding="utf-8")
    object_store_path = runtime_root / "object-stores.yaml"
    object_store_created = _write_private_template_if_missing(
        object_store_path, OBJECT_STORE_CONFIG_TEMPLATE
    )
    return {
        "ok": True,
        "created": False,
        "workspace_root": str(workspace_root),
        "config_path": None,
        "managed_by": "smoking-data-spi entry point",
        "object_store_config_path": str(object_store_path),
        "object_store_created": object_store_created,
    }


def _write_private_template_if_missing(path: Path, text: str) -> bool:
    if path.exists():
        return False
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)
    path.chmod(0o600)
    return True
