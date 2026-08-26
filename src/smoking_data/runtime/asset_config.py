from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from smoking_data.workspace_resources import workspace_text

ASSET_CONFIG_SCHEMA_VERSION = "smoking-data.asset-config.v3"
COMMON_CONFIG_RESOURCE = "smoking_data/config.yaml"
ASSET_CONFIG_RESOURCES = {
    "0101": "smoking_data/assets/0101/config.yaml",
    "0102": "smoking_data/assets/0102/config.yaml",
    "0103": "smoking_data/assets/0103/config.yaml",
    "0201": "smoking_data/assets/0201/config.yaml",
    "0301": "smoking_data/assets/0301/config.yaml",
    "0401": "smoking_data/assets/0401/config.yaml",
}


@dataclass(frozen=True, slots=True)
class EffectiveAssetConfig:
    asset_code: str
    payload: dict[str, Any]
    bundled_resource: str
    workspace_common_path: Path
    workspace_path: Path
    effective_paths: tuple[Path, ...]
    effective_path: Path | None


def asset_code_from_definition_path(path: str | Path) -> str | None:
    definition_path = Path(path).expanduser()
    name = definition_path.name
    parts = name.split(".")
    if len(parts) >= 3 and parts[-1].lower() in {"yaml", "yml"}:
        candidate = parts[-2]
        if candidate in ASSET_CONFIG_RESOURCES:
            return candidate

    # Migration output is commonly named `converted.yaml`, so the filename
    # cannot always carry the asset code. Prefer the explicit current-contract
    # header when the path does not identify an asset by name.
    if definition_path.is_file():
        try:
            payload = yaml.safe_load(definition_path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            return None
        if isinstance(payload, dict):
            header = payload.get("yaml")
            if isinstance(header, dict):
                candidate = str(header.get("asset_code") or "").strip()
                if candidate in ASSET_CONFIG_RESOURCES:
                    return candidate
    return None


def workspace_common_config_path(project_root: str | Path) -> Path:
    return Path(project_root).expanduser().resolve() / ".smoking-data" / "config.yaml"


def workspace_asset_config_path(project_root: str | Path, asset_code: str) -> Path:
    _require_asset_code(asset_code)
    return (
        Path(project_root).expanduser().resolve()
        / ".smoking-data"
        / "assets"
        / asset_code
        / "config.yaml"
    )


def bundled_common_config_text() -> str:
    return _resource_text(COMMON_CONFIG_RESOURCE)


def bundled_asset_config_text(asset_code: str) -> str:
    return _resource_text(ASSET_CONFIG_RESOURCES[_require_asset_code(asset_code)])


def load_effective_asset_config(
    project_root: str | Path,
    asset_code: str,
) -> EffectiveAssetConfig:
    code = _require_asset_code(asset_code)
    root = Path(project_root).expanduser().resolve()
    _reject_stale_contract_files(root, code)
    common_path = workspace_common_config_path(root)
    asset_path = workspace_asset_config_path(root, code)
    relative = ASSET_CONFIG_RESOURCES[code]
    layers = [
        _read_config_text(
            bundled_common_config_text(),
            source=f"smoking_data:{COMMON_CONFIG_RESOURCE}",
            expected_scope="common",
        ),
        _read_config_text(
            bundled_asset_config_text(code),
            source=f"smoking_data:{relative}",
            expected_scope=code,
        ),
    ]
    effective_paths: list[Path] = []
    if common_path.is_file():
        layers.append(
            _read_config_text(
                common_path.read_text(encoding="utf-8"),
                source=str(common_path),
                expected_scope="common",
            )
        )
        effective_paths.append(common_path)
    if asset_path.is_file():
        layers.append(
            _read_config_text(
                asset_path.read_text(encoding="utf-8"),
                source=str(asset_path),
                expected_scope=code,
            )
        )
        effective_paths.append(asset_path)

    payload: dict[str, Any] = {}
    for layer in layers:
        payload = deep_merge(payload, layer)
    payload["config"] = {
        "schema_version": ASSET_CONFIG_SCHEMA_VERSION,
        "scope": code,
    }
    return EffectiveAssetConfig(
        asset_code=code,
        payload=payload,
        bundled_resource=f"smoking_data:{relative}",
        workspace_common_path=common_path,
        workspace_path=asset_path,
        effective_paths=tuple(effective_paths),
        effective_path=effective_paths[-1] if effective_paths else None,
    )


def load_effective_common_config(project_root: str | Path) -> dict[str, Any]:
    root = Path(project_root).expanduser().resolve()
    _reject_stale_contract_files(root, None)
    payload = _read_config_text(
        bundled_common_config_text(),
        source=f"smoking_data:{COMMON_CONFIG_RESOURCE}",
        expected_scope="common",
    )
    workspace_path = workspace_common_config_path(root)
    if workspace_path.is_file():
        payload = deep_merge(
            payload,
            _read_config_text(
                workspace_path.read_text(encoding="utf-8"),
                source=str(workspace_path),
                expected_scope="common",
            ),
        )
    return payload


def asset_config_fingerprint(project_root: str | Path, asset_code: str) -> str:
    payload = load_effective_asset_config(project_root, asset_code).payload
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _read_config_text(
    text: str,
    *,
    source: str,
    expected_scope: str,
) -> dict[str, Any]:
    payload = yaml.safe_load(text) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Asset config root는 mapping이어야 합니다: {source}")
    allowed = {
        "common": {"config", "paths", "execution", "contract"},
        "0101": {"config", "paths", "execution", "contract", "output"},
        "0102": {"config", "paths", "execution", "contract", "output"},
        "0103": {"config", "paths", "execution", "contract", "output"},
        "0201": {"config", "paths", "execution", "contract", "output"},
        "0301": {"config", "paths", "execution", "contract", "output"},
        "0401": {"config", "paths", "execution", "contract", "output"},
    }[expected_scope]
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise ValueError(f"Asset config의 지원하지 않는 키입니다: {unknown} ({source})")
    header = payload.get("config")
    if not isinstance(header, dict):
        raise ValueError(f"Asset config.config가 필요합니다: {source}")
    header_unknown = sorted(set(header) - {"schema_version", "scope"})
    if header_unknown:
        raise ValueError(f"Asset config header의 지원하지 않는 키입니다: {header_unknown} ({source})")
    if header.get("schema_version") != ASSET_CONFIG_SCHEMA_VERSION:
        raise ValueError(
            f"Asset config schema_version은 {ASSET_CONFIG_SCHEMA_VERSION}이어야 합니다: {source}"
        )
    if str(header.get("scope") or "") != expected_scope:
        raise ValueError(f"Asset config scope는 {expected_scope!r}이어야 합니다: {source}")
    for key in allowed - {"config"}:
        value = payload.get(key)
        if value is not None and not isinstance(value, dict):
            raise ValueError(f"Asset config {key}는 mapping이어야 합니다: {source}")
    contract = payload.get("contract") or {}
    contract_unknown = sorted(set(contract) - {"partition_grid"})
    if contract_unknown:
        raise ValueError(
            f"Asset config contract의 지원하지 않는 키입니다: {contract_unknown} ({source})"
        )
    return payload


def _resource_text(relative: str) -> str:
    return workspace_text(*relative.split("/"))


def _require_asset_code(asset_code: str) -> str:
    code = str(asset_code).strip()
    if code not in ASSET_CONFIG_RESOURCES:
        raise ValueError(f"지원하지 않는 Asset config 코드입니다: {code!r}")
    return code


def _reject_stale_contract_files(root: Path, asset_code: str | None) -> None:
    candidates = [root / ".smoking-data" / "contract.yaml"]
    if asset_code is not None:
        candidates.append(
            root / ".smoking-data" / "assets" / asset_code / "contract.yaml"
        )
    stale = [path for path in candidates if path.is_file()]
    if stale:
        raise ValueError(
            "분리된 contract.yaml은 지원하지 않습니다. 내용을 같은 범위의 config.yaml "
            f"contract 키로 옮기세요: {[str(path) for path in stale]}"
        )
