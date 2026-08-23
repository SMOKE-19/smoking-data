from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from smoking_data.runtime.asset_config import load_effective_asset_config


@dataclass(frozen=True, slots=True)
class EffectiveAssetContract:
    asset_code: str
    payload: dict[str, Any]
    definition: dict[str, Any]
    constants: dict[str, Any]
    workspace_common_path: Path
    workspace_asset_path: Path
    effective_paths: tuple[Path, ...]


def load_effective_asset_contract(
    project_root: str | Path,
    asset_code: str,
) -> EffectiveAssetContract:
    config = load_effective_asset_config(project_root, asset_code)
    contract = config.payload.get("contract") or {}
    if not isinstance(contract, dict):
        raise ValueError("Asset config contract는 mapping이어야 합니다.")
    engine_only = {"config", "paths", "execution", "contract"}
    definition = {
        key: value
        for key, value in config.payload.items()
        if key not in engine_only
    }
    return EffectiveAssetContract(
        asset_code=config.asset_code,
        payload=contract,
        definition=definition,
        constants=contract,
        workspace_common_path=config.workspace_common_path,
        workspace_asset_path=config.workspace_path,
        effective_paths=config.effective_paths,
    )


def asset_contract_fingerprint(project_root: str | Path, asset_code: str) -> str:
    payload = load_effective_asset_contract(project_root, asset_code).payload
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def partition_grid_anchor(contract: EffectiveAssetContract) -> str:
    grid = contract.constants.get("partition_grid")
    if not isinstance(grid, dict):
        raise ValueError("contract.constants.partition_grid는 mapping이어야 합니다.")
    value = str(grid.get("anchor_date") or "").strip()
    if not value:
        raise ValueError("contract.constants.partition_grid.anchor_date가 필요합니다.")
    return value


def partition_grid_step_days(contract: EffectiveAssetContract) -> int:
    grid = contract.constants.get("partition_grid")
    if not isinstance(grid, dict):
        raise ValueError("contract.constants.partition_grid는 mapping이어야 합니다.")
    value = int(grid.get("step_days") or 0)
    if value < 1:
        raise ValueError("contract.constants.partition_grid.step_days는 1 이상이어야 합니다.")
    return value
