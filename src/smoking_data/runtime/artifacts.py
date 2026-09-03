from __future__ import annotations

from pathlib import Path

from smoking_data.runtime.config import RuntimeConfig
from smoking_data.runtime.paths import ensure_dir
from smoking_data.runtime.yaml_loader import PresetSpec


def artifact_root_for(spec: PresetSpec, *, config: RuntimeConfig) -> Path:
    return config.temp_root / "artifacts" / spec.preset / spec.job_name


def active_snapshot_path_for(
    spec: PresetSpec, *, config: RuntimeConfig, operation_id: str | None = None
) -> Path:
    root = ensure_dir(artifact_root_for(spec, config=config) / "active_snapshot")
    if operation_id:
        root = ensure_dir(root / _operation_artifact_name(operation_id))
    return root / "active_snapshot.arrow"


def candidate_sidecar_root_for(
    spec: PresetSpec, *, config: RuntimeConfig, operation_id: str | None = None
) -> Path:
    root = ensure_dir(artifact_root_for(spec, config=config) / "candidates")
    return ensure_dir(root / _operation_artifact_name(operation_id)) if operation_id else root


def candidate_manifest_path_for(
    spec: PresetSpec, *, config: RuntimeConfig, operation_id: str | None = None
) -> Path:
    suffix = f".{_operation_artifact_name(operation_id)}" if operation_id else ""
    return artifact_root_for(spec, config=config) / f"candidates{suffix}.manifest.json"


def _operation_artifact_name(operation_id: str) -> str:
    import hashlib
    import re

    readable = re.sub(r"[^A-Za-z0-9_.-]+", "_", operation_id).strip("._") or "selector"
    digest = hashlib.sha256(operation_id.encode("utf-8")).hexdigest()[:10]
    return f"{readable[:48]}-{digest}"
