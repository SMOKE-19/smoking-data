"""SOURCE 공통 defaults 레이어."""

from __future__ import annotations

from typing import Any

from smoking_data.assets.a0101_source.spec_common.defaults import load_yaml_dict
from smoking_data.runtime.asset_config import deep_merge

__all__ = ["apply_asset_defaults", "load_yaml_dict"]


def apply_asset_defaults(
    *, raw: dict[str, Any], defaults: dict[str, Any]
) -> dict[str, Any]:
    # ``paths`` configures ProjectPaths and is not part of the 0101 Asset YAML
    # contract. Keep it out of SourceSpec.raw while merging contract defaults.
    contract_defaults = {key: value for key, value in defaults.items() if key != "paths"}
    return deep_merge(contract_defaults, raw)
