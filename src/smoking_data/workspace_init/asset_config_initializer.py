from __future__ import annotations

from pathlib import Path
from typing import Any

from smoking_data.runtime.asset_config import (
    ASSET_CONFIG_RESOURCES,
    bundled_asset_config_text,
    bundled_common_config_text,
    workspace_asset_config_path,
    workspace_common_config_path,
)


def initialize_asset_configs(target: str | Path) -> dict[str, Any]:
    workspace_root = Path(target).expanduser().resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    created: list[str] = []
    preserved: list[str] = []
    config_paths: dict[str, str] = {}
    documents = [
        ("common", workspace_common_config_path(workspace_root), bundled_common_config_text()),
        *[
            (
                asset_code,
                workspace_asset_config_path(workspace_root, asset_code),
                bundled_asset_config_text(asset_code),
            )
            for asset_code in ASSET_CONFIG_RESOURCES
        ],
    ]
    for key, config_path, text in documents:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        relative = config_path.relative_to(workspace_root).as_posix()
        config_paths[key] = str(config_path)
        if config_path.exists():
            preserved.append(relative)
            continue
        temporary = config_path.with_suffix(".yaml.tmp")
        temporary.write_text(text, encoding="utf-8")
        temporary.replace(config_path)
        created.append(relative)
    return {
        "ok": True,
        "workspace_root": str(workspace_root),
        "created": created,
        "preserved": preserved,
        "config_paths": config_paths,
    }
