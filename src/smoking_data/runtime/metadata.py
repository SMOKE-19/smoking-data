from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from smoking_data.core.results import to_json_safe, utc_now_iso
from smoking_data.runtime.config import RuntimeConfig
from smoking_data.runtime.paths import ensure_dir, resolve_project_path
from smoking_data.runtime.transactions import refresh_dataset_manifest_provenance
from smoking_data.runtime.yaml_loader import PresetSpec

METADATA_SCHEMA_VERSION = "smoking-data.artifact-metadata.v1"
PROVENANCE_DIRECTORY_NAME = "_smoking_data"
LARGE_DETAILS_KEYS = frozenset(
    {
        "physical_plan",
        "physical_plan_actuals",
        "task_results",
        "execution_plan",
        "operation_execution_trace",
    }
)


def metadata_path_for(spec: PresetSpec, *, config: RuntimeConfig) -> Path:
    return artifact_root_for_metadata(spec, config=config) / PROVENANCE_DIRECTORY_NAME / "metadata.json"


def artifact_root_for_metadata(spec: PresetSpec, *, config: RuntimeConfig) -> Path:
    output = spec.raw.get("output") if isinstance(spec.raw, dict) else None
    if not isinstance(output, dict):
        raise ValueError("Asset output contract is required to resolve artifact metadata.")
    artifact = output.get("artifact")
    raw_root = artifact.get("root_dir") if isinstance(artifact, dict) else output.get("output_dir")
    if not raw_root:
        raise ValueError("Asset output artifact root is required to resolve metadata.")
    return resolve_project_path(str(raw_root), project_root=config.project_root)


def log_path_for(spec: PresetSpec, *, config: RuntimeConfig) -> Path:
    return config.log_root / spec.preset / f"{spec.job_name}.log"


def write_metadata(
    *,
    spec: PresetSpec,
    config: RuntimeConfig,
    result: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> Path:
    path = metadata_path_for(spec, config=config)
    ensure_dir(path.parent)
    safe_result = to_json_safe(result)
    result_payload, artifact_path = _externalize_large_details(path, safe_result)
    payload = {
        "schema_version": METADATA_SCHEMA_VERSION,
        "created_at": utc_now_iso(),
        "asset": {"code": _asset_code(spec), "job_name": spec.job_name},
        "preset": spec.preset,
        "job_name": spec.job_name,
        "yaml_path": str(spec.yaml_path),
        "yaml_hash": spec.yaml_hash,
        "result": result_payload,
        "extra": extra or {},
    }
    if artifact_path is not None:
        payload["details_artifact_path"] = str(artifact_path)
    _write_text_atomic(path, json.dumps(payload, ensure_ascii=False, indent=2))
    definition_path = path.parent / "definition.yaml"
    if spec.yaml_path.is_file():
        _write_text_atomic(definition_path, spec.yaml_path.read_text(encoding="utf-8"))
    refresh_dataset_manifest_provenance(path.parents[1])
    return path


def read_previous_yaml_hash(spec: PresetSpec, *, config: RuntimeConfig) -> str | None:
    payload = read_metadata(spec, config=config)
    if not payload:
        return None
    value = payload.get("yaml_hash")
    return str(value) if value else None


def read_metadata(spec: PresetSpec, *, config: RuntimeConfig) -> dict[str, Any] | None:
    path = metadata_path_for(spec, config=config)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _externalize_large_details(
    metadata_path: Path,
    result: dict[str, Any],
    *,
    threshold_bytes: int = 1_000_000,
) -> tuple[dict[str, Any], Path | None]:
    details = result.get("details")
    if not isinstance(details, dict):
        return result, None
    selected = {key: details[key] for key in LARGE_DETAILS_KEYS if key in details}
    encoded = json.dumps(selected, ensure_ascii=False).encode("utf-8")
    if not selected or len(encoded) < threshold_bytes:
        return result, None
    artifact_path = metadata_path.with_suffix(".details.json")
    _write_text_atomic(artifact_path, json.dumps(selected, ensure_ascii=False, indent=2))
    compact = dict(result)
    compact_details = dict(details)
    for key in selected:
        compact_details.pop(key, None)
    compact_details["details_artifact"] = {
        "path": str(artifact_path),
        "keys": sorted(selected),
        "size_bytes": len(encoded),
    }
    compact["details"] = compact_details
    return compact, artifact_path


def _write_text_atomic(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    staging = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    staging.write_text(text, encoding="utf-8")
    staging.replace(path)


def _asset_code(spec: PresetSpec) -> str:
    raw = spec.raw if isinstance(spec.raw, dict) else {}
    yaml_header = raw.get("yaml")
    if isinstance(yaml_header, dict) and yaml_header.get("asset_code"):
        return str(yaml_header["asset_code"])
    pipeline = raw.get("__pipeline")
    if isinstance(pipeline, dict):
        version = str(pipeline.get("schema_version") or "")
        if version.startswith("smoking-data.pipeline."):
            return "0301" if spec.preset == "0301" else "0201"
    return str(spec.preset)
