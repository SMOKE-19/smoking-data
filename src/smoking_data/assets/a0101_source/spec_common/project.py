"""0101 bundled config를 사용하는 Asset 경로 해석기."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from smoking_data.runtime.asset_config import load_effective_asset_config
from smoking_data.runtime.asset_contract import (
    load_effective_asset_contract,
    partition_grid_anchor,
)
from smoking_data.runtime.config import resolve_config_paths

_WINDOWS_DRIVE_PATH = re.compile(r"^[A-Za-z]:[\\/]")


@dataclass(slots=True)
class ProjectPaths:
    project_root: Path
    package_root: Path
    config_path: Path
    config_payload: dict[str, Any]
    contract_payload: dict[str, Any]
    contract_definition: dict[str, Any]
    partition_grid_anchor_date: date
    data_root: Path
    temp_root: Path
    metadata_root: Path
    log_root: Path
    path_values: dict[str, Path]

    def template_scope(
        self,
        *,
        job_name: str = "",
        asset_code: str = "",
        extra_scope: dict[str, str] | None = None,
    ) -> dict[str, str]:
        scope = {
            "project_root": str(self.project_root),
            "package_root": str(self.package_root),
            **{key: str(value) for key, value in self.path_values.items()},
            "job_name": job_name,
            "asset_code": asset_code,
        }
        if extra_scope:
            scope.update({str(key): str(value) for key, value in extra_scope.items()})
        return scope

    def resolve_path(self, path_str: str, *, base_dir: Path | None = None) -> Path:
        return _resolve_path(path_str, base_dir=base_dir or self.project_root)


def load_project_paths(yaml_path: str | Path) -> ProjectPaths:
    asset_yaml_path = Path(yaml_path).resolve()
    project_root = infer_project_root(asset_yaml_path)
    package_root = Path(__file__).resolve().parents[1]
    bundled_config_path = package_root / "config" / "config.yaml"
    effective_config = load_effective_asset_config(project_root, "0101")
    effective_contract = load_effective_asset_contract(project_root, "0101")
    config_path = effective_config.effective_path or bundled_config_path
    payload = effective_config.payload
    raw_paths = payload.get("paths", {})
    if not isinstance(raw_paths, dict):
        raise ValueError("0101 Asset config의 paths는 dict여야 합니다.")
    path_values = resolve_config_paths(
        raw_paths,
        project_root=project_root,
        asset_code="0101",
    )
    return ProjectPaths(
        project_root=project_root,
        package_root=package_root,
        config_path=config_path,
        config_payload=payload,
        contract_payload=effective_contract.payload,
        contract_definition=effective_contract.definition,
        partition_grid_anchor_date=date.fromisoformat(
            partition_grid_anchor(effective_contract)
        ),
        data_root=path_values["data_root"],
        temp_root=path_values["temp_root"],
        metadata_root=path_values["metadata_root"],
        log_root=path_values["log_root"],
        path_values=path_values,
    )


def infer_project_root(path: Path) -> Path:
    resolved = path.resolve()
    for parent in (resolved.parent, *resolved.parents):
        if (parent / ".smoking-data" / "config.yaml").is_file():
            return parent
        if parent.name == "settings":
            return parent.parent
        if (parent / "pyproject.toml").exists():
            return parent
    return resolved.parent


def _resolve_path(path_str: str, *, base_dir: Path) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(path_str))
    if _WINDOWS_DRIVE_PATH.match(expanded):
        if os.name != "nt":
            raise ValueError(
                "비 Windows 환경에서는 D:\\ 경로 대신 /mnt/d 같은 마운트 경로를 사용해야 합니다."
            )
        return Path(expanded)
    path = Path(expanded)
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _get_nested(mapping: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current
