from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_ENVIRONMENT_REFERENCE = re.compile(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}")
_PLACEHOLDER_PREFIX = "REPLACE_WITH_"


@dataclass(frozen=True, slots=True)
class AdapterConfig:
    name: str
    module: str
    query_function: str
    decorator_function: str
    call_options: dict[str, Any]


def load_adapter_config(path: str | Path, *, adapter_name: str | None = None) -> AdapterConfig:
    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise RuntimeError(
            f"Adapter config does not exist: {config_path}. "
            "Run 'smoking-data init .' and edit the generated file."
        )
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"Adapter config root must be an object: {config_path}")
    _reject_unknown_keys(payload, {"version", "default_adapter", "adapters"}, label="root")
    if payload.get("version") != 1:
        raise ValueError("Adapter config version must be 1.")
    selected = str(adapter_name or payload.get("default_adapter") or "").strip()
    if not selected:
        raise ValueError("Adapter config requires default_adapter.")
    adapters = payload.get("adapters")
    if not isinstance(adapters, dict) or selected not in adapters:
        raise ValueError(f"Configured adapter is missing: {selected!r}")
    raw = adapters[selected]
    if not isinstance(raw, dict):
        raise TypeError(f"Adapter definition must be an object: {selected!r}")
    _reject_unknown_keys(
        raw,
        {"module", "query_function", "decorator_function", "call_options"},
        label=f"adapters.{selected}",
    )
    module = _required_configured_string(raw, "module", adapter_name=selected)
    query_function = _required_configured_string(raw, "query_function", adapter_name=selected)
    decorator_function = _required_configured_string(
        raw, "decorator_function", adapter_name=selected
    )
    raw_options = raw.get("call_options", {})
    if not isinstance(raw_options, dict):
        raise TypeError(f"adapters.{selected}.call_options must be an object.")
    call_options = _expand_environment(raw_options)
    return AdapterConfig(
        name=selected,
        module=module,
        query_function=query_function,
        decorator_function=decorator_function,
        call_options=call_options,
    )


def _required_configured_string(
    payload: dict[str, Any], key: str, *, adapter_name: str
) -> str:
    value = str(payload.get(key) or "").strip()
    if not value or value.startswith(_PLACEHOLDER_PREFIX):
        raise ValueError(
            f"adapters.{adapter_name}.{key} must be edited after 'smoking-data init'."
        )
    return value


def _expand_environment(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _expand_environment(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if not isinstance(value, str):
        return value
    expanded = os.path.expandvars(value)
    unresolved = _ENVIRONMENT_REFERENCE.search(expanded)
    if unresolved:
        raise ValueError(f"Adapter environment variable is not set: {unresolved.group(0)}")
    return expanded


def _reject_unknown_keys(payload: dict[str, Any], allowed: set[str], *, label: str) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise ValueError(f"Unknown adapter config keys at {label}: {unknown}")
