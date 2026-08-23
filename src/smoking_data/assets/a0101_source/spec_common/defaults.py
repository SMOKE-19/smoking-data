"""Common YAML loading helper for the 0101 Asset spec."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_yaml_dict(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"YAML 최상위 구조는 dict여야 합니다: {path}")
    return payload
