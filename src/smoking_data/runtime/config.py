from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from smoking_data.core.exceptions import ConfigError
from smoking_data.runtime.asset_config import (
    deep_merge,
    load_effective_asset_config,
    load_effective_common_config,
    workspace_common_config_path,
)

_TEMPLATE_PATTERN = re.compile(r"\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}")
_LITERAL_PATTERN = re.compile(r"^(?P<prefix>[fr])(?P<quote>['\"])(?P<body>.*)(?P=quote)$")


@dataclass(frozen=True, slots=True)
class PhaseMemoryPolicy:
    min_workers: int = 1
    max_workers: int = 1


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    project_root: Path
    config_path: Path | None
    data_root: Path
    temp_root: Path
    metadata_root: Path
    log_root: Path
    schedule_root: Path
    path_values: dict[str, Path] = field(default_factory=dict)
    workers: int = 1
    max_tasks_per_child: int | None = 1
    target_rows_per_part: int = 20_000
    target_key_groups_per_part: int = 500
    memory_budget_mb: int = 4096
    memory_safety_ratio: float = 0.8
    phase_memory: dict[str, PhaseMemoryPolicy] = field(default_factory=dict)
    max_source_files_per_task: int = 40
    max_source_row_groups_per_task: int = 512
    sidecar_target_bytes_mb: int = 128
    sidecar_workers: int = 1
    sidecar_worker_recycle_mode: str = "adaptive"
    sidecar_max_source_files: int = 16
    sidecar_max_projected_bytes_mb: int = 512
    output_row_group_rows: int | None = None
    optimizer_enabled: bool = True
    reset_before_run: bool = False
    range_merge_gap_bytes: int = 64 * 1024
    max_range_bytes: int = 8 * 1024 * 1024
    max_ranges_per_task: int = 512
    minimum_range_savings_ratio: float = 0.0

    def phase_memory_policy(self, phase: str, *, requested_workers: int) -> PhaseMemoryPolicy:
        configured = self.phase_memory.get(phase)
        if configured is not None:
            return configured
        return PhaseMemoryPolicy(
            min_workers=1,
            max_workers=max(1, int(requested_workers)),
        )

    def template_scope(self) -> dict[str, str]:
        paths = self.path_values or {
            "data_root": self.data_root,
            "temp_root": self.temp_root,
            "metadata_root": self.metadata_root,
            "log_root": self.log_root,
            "schedule_root": self.schedule_root,
        }
        return {
            key: _portable_path_value(value, project_root=self.project_root)
            for key, value in paths.items()
        }


def load_config(
    config_path: str | Path | None = None,
    *,
    project_root: str | Path | None = None,
    asset_code: str | None = None,
) -> RuntimeConfig:
    root = Path(project_root or os.getcwd()).expanduser().resolve()
    payload = load_effective_common_config(root)
    common_workspace = workspace_common_config_path(root)
    effective_path: Path | None = common_workspace if common_workspace.is_file() else None
    if asset_code is not None:
        asset_config = load_effective_asset_config(root, asset_code)
        payload = asset_config.payload
        effective_path = asset_config.effective_path
    candidate = _resolve_override_config_path(config_path=config_path, project_root=root)
    if candidate is not None:
        payload = deep_merge(payload, _read_yaml(candidate))
        effective_path = candidate

    paths = resolve_config_paths(
        _mapping(payload.get("paths"), section="paths"),
        project_root=root,
        asset_code=asset_code,
    )
    execution = _mapping(payload.get("execution"), section="execution")
    memory = _mapping(execution.get("memory"), section="execution.memory")
    hard_limit_mb = _positive_int(
        memory.get("hard_limit_mb", execution.get("memory_budget_mb", 4096)),
        path="execution.memory.hard_limit_mb",
    )
    phase_memory = _phase_memory_policies(memory, hard_limit_mb=hard_limit_mb)

    data_root = paths["data_root"]
    temp_root = paths["temp_root"]
    metadata_root = paths["metadata_root"]
    log_root = paths["log_root"]
    schedule_root = paths["schedule_root"]

    return RuntimeConfig(
        project_root=root,
        config_path=effective_path,
        data_root=data_root,
        temp_root=temp_root,
        metadata_root=metadata_root,
        log_root=log_root,
        schedule_root=schedule_root,
        path_values=paths,
        workers=max(1, int(execution.get("workers", 1) or 1)),
        max_tasks_per_child=parse_max_tasks_per_child(execution.get("max_tasks_per_child", 1)),
        target_rows_per_part=_positive_int(
            execution.get("target_rows_per_part", 20_000),
            path="execution.target_rows_per_part",
        ),
        target_key_groups_per_part=_positive_int(
            execution.get("target_key_groups_per_part", 500),
            path="execution.target_key_groups_per_part",
        ),
        memory_budget_mb=hard_limit_mb,
        memory_safety_ratio=_positive_ratio(
            memory.get("safety_ratio", 0.8), path="execution.memory.safety_ratio"
        ),
        phase_memory=phase_memory,
        max_source_files_per_task=_positive_int(
            execution.get("max_source_files_per_task", 40),
            path="execution.max_source_files_per_task",
        ),
        max_source_row_groups_per_task=_positive_int(
            execution.get("max_source_row_groups_per_task", 512),
            path="execution.max_source_row_groups_per_task",
        ),
        sidecar_target_bytes_mb=_positive_int(
            execution.get("sidecar_target_bytes_mb", 128),
            path="execution.sidecar_target_bytes_mb",
        ),
        sidecar_workers=_positive_int(
            execution.get("sidecar_workers", 1), path="execution.sidecar_workers"
        ),
        sidecar_worker_recycle_mode=_sidecar_worker_recycle_mode(
            execution.get("sidecar_worker_recycle_mode", "adaptive")
        ),
        sidecar_max_source_files=_positive_int(
            execution.get("sidecar_max_source_files", 16),
            path="execution.sidecar_max_source_files",
        ),
        sidecar_max_projected_bytes_mb=_positive_int(
            execution.get("sidecar_max_projected_bytes_mb", 512),
            path="execution.sidecar_max_projected_bytes_mb",
        ),
        output_row_group_rows=_optional_positive_int(
            execution.get("output_row_group_rows"),
            path="execution.output_row_group_rows",
        ),
        optimizer_enabled=bool(execution.get("optimizer_enabled", True)),
        reset_before_run=bool(execution.get("reset_before_run", False)),
        range_merge_gap_bytes=_non_negative_int(
            execution.get("range_merge_gap_bytes", 64 * 1024),
            path="execution.range_merge_gap_bytes",
        ),
        max_range_bytes=_positive_int(
            execution.get("max_range_bytes", 8 * 1024 * 1024),
            path="execution.max_range_bytes",
        ),
        max_ranges_per_task=_positive_int(
            execution.get("max_ranges_per_task", 512),
            path="execution.max_ranges_per_task",
        ),
        minimum_range_savings_ratio=_ratio(
            execution.get("minimum_range_savings_ratio", 0.0),
            path="execution.minimum_range_savings_ratio",
        ),
    )


def parse_max_tasks_per_child(value: Any) -> int | None:
    if value is None:
        return None
    parsed = int(value)
    if parsed < 1:
        raise ConfigError("execution.max_tasks_per_child must be null or an integer >= 1.")
    return parsed


def _sidecar_worker_recycle_mode(value: Any) -> str:
    mode = str(value or "adaptive").strip().lower()
    if mode != "adaptive":
        raise ConfigError("execution.sidecar_worker_recycle_mode must be adaptive.")
    return mode


def _positive_int(value: Any, *, path: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ConfigError(f"{path} must be an integer >= 1.")
    return parsed


def _non_negative_int(value: Any, *, path: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ConfigError(f"{path} must be an integer >= 0.")
    return parsed


def _ratio(value: Any, *, path: str) -> float:
    parsed = float(value)
    if not 0.0 <= parsed < 1.0:
        raise ConfigError(f"{path} must be >= 0 and < 1.")
    return parsed


def _positive_ratio(value: Any, *, path: str) -> float:
    parsed = float(value)
    if not 0.0 < parsed <= 1.0:
        raise ConfigError(f"{path} must be > 0 and <= 1.")
    return parsed


def _phase_memory_policies(
    memory: dict[str, Any], *, hard_limit_mb: int
) -> dict[str, PhaseMemoryPolicy]:
    phases = _mapping(memory.get("phases"), section="execution.memory.phases")
    result: dict[str, PhaseMemoryPolicy] = {}
    for phase in ("build_sidecar", "materialize", "save_dataset"):
        if phase not in phases:
            continue
        raw = _mapping(phases.get(phase), section=f"execution.memory.phases.{phase}")
        unknown = sorted(set(raw) - {"workers"})
        if unknown:
            raise ConfigError(
                f"execution.memory.phases.{phase} supports only workers; "
                f"memory is derived from execution.memory.hard_limit_mb: {unknown}"
            )
        workers = _mapping(raw.get("workers"), section=f"execution.memory.phases.{phase}.workers")
        minimum = _positive_int(
            workers.get("min", 1), path=f"execution.memory.phases.{phase}.workers.min"
        )
        maximum = _positive_int(
            workers.get("max", minimum), path=f"execution.memory.phases.{phase}.workers.max"
        )
        if minimum > maximum:
            raise ConfigError(
                f"execution.memory.phases.{phase}.workers.min must be <= workers.max."
            )
        result[phase] = PhaseMemoryPolicy(
            min_workers=minimum,
            max_workers=maximum,
        )
    return result


def _optional_positive_int(value: Any, *, path: str) -> int | None:
    if value is None:
        return None
    return _positive_int(value, path=path)


def _resolve_override_config_path(
    *, config_path: str | Path | None, project_root: Path
) -> Path | None:
    raw = config_path
    if raw:
        path = _resolve_path(raw, base_dir=project_root)
        if not path.exists():
            raise ConfigError(f"Config file does not exist: {path}")
        return path
    return None


def resolve_config_paths(
    payload: dict[str, Any],
    *,
    project_root: Path,
    asset_code: str | None,
) -> dict[str, Path]:
    raw = {
        "data_root": "DATA",
        "temp_root": ".temp",
        "metadata_root": 'f"{temp_root}/metadata"',
        "log_root": 'f"{temp_root}/logs"',
        "schedule_root": "schedules",
        **payload,
    }
    if not all(
        isinstance(key, str) and isinstance(value, (str, Path)) for key, value in raw.items()
    ):
        raise ConfigError("paths의 모든 키와 값은 문자열 경로여야 합니다.")
    scope = {"project_root": str(project_root), "asset_code": str(asset_code or "")}
    resolved: dict[str, Path] = {}
    pending = dict(raw)
    while pending:
        progressed = False
        for key, raw_value in list(pending.items()):
            value = str(raw_value)
            literal = _LITERAL_PATTERN.match(value)
            body = literal.group("body") if literal else value
            dependencies = set(_TEMPLATE_PATTERN.findall(body))
            if key in dependencies:
                raise ConfigError(f"paths.{key}가 자기 자신을 참조합니다.")
            if not dependencies.issubset(scope):
                continue
            expanded = _TEMPLATE_PATTERN.sub(lambda match: scope[match.group("name")], body)
            path = _resolve_path(expanded, base_dir=project_root)
            resolved[key] = path
            scope[key] = str(path)
            pending.pop(key)
            progressed = True
        if not progressed:
            unresolved = {
                key: sorted(set(_TEMPLATE_PATTERN.findall(str(value))) - set(scope))
                for key, value in pending.items()
            }
            raise ConfigError(f"paths template을 해석할 수 없습니다: {unresolved}")
    return resolved


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ConfigError(f"Config YAML must be a mapping: {path}")
    return payload


def _mapping(value: Any, *, section: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"Config section must be a mapping: {section}")
    return value


def _resolve_path(value: Any, *, base_dir: Path) -> Path:
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _portable_path_value(path: Path, *, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())
