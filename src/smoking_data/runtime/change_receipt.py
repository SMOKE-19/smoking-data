from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from smoking_data.core.results import to_json_safe

CHANGE_RECEIPT_SCHEMA_VERSION = "smoking-data.dataset-change-receipt.v1"
CHANGE_RECEIPT_RELATIVE_PATH = Path("_smoking_data/change-receipt.json")


def build_dataset_change_receipt(
    *,
    previous_manifest: dict[str, Any] | None,
    current_parts: list[dict[str, Any]],
    manifest_context: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    previous_parts = _normalized_parts(previous_manifest)
    current = [_with_segment_id(item) for item in current_parts]
    previous_by_path = {str(item["relative_path"]): item for item in previous_parts}
    current_by_path = {str(item["relative_path"]): item for item in current}
    added: list[dict[str, Any]] = []
    replaced: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    unchanged = 0
    for path, item in current_by_path.items():
        before = previous_by_path.get(path)
        if before is None:
            added.append(item)
        elif before["segment_id"] != item["segment_id"]:
            replaced.append({"before": before, "after": item})
        else:
            unchanged += 1
    for path, item in previous_by_path.items():
        if path not in current_by_path:
            removed.append(item)
    generation_id = manifest_generation_id(current, manifest_context=manifest_context)
    parent_generation_id = (
        str(previous_manifest.get("generation_id"))
        if previous_manifest and previous_manifest.get("generation_id")
        else (
            manifest_generation_id(previous_parts, manifest_context=previous_manifest.get("context"))
            if previous_manifest
            else None
        )
    )
    receipt = {
        "schema_version": CHANGE_RECEIPT_SCHEMA_VERSION,
        "dataset_id": _dataset_id(manifest_context),
        "generation_id": generation_id,
        "parent_generation_id": parent_generation_id,
        "manifest_version": "smoking-data.dataset-manifest.v1",
        "changes": {
            "added": added,
            "replaced": replaced,
            "removed": removed,
            "unchanged_count": unchanged,
        },
    }
    return receipt, {
        "added": len(added),
        "replaced": len(replaced),
        "removed": len(removed),
        "unchanged": unchanged,
    }


def write_dataset_change_receipt(root: Path, receipt: dict[str, Any]) -> Path:
    path = root / CHANGE_RECEIPT_RELATIVE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(to_json_safe(receipt), ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    return path


def read_dataset_change_receipt(dataset_root: Path) -> dict[str, Any] | None:
    path = dataset_root / CHANGE_RECEIPT_RELATIVE_PATH
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if validate_dataset_change_receipt(value) else None


def validate_dataset_change_receipt(
    value: object, *, manifest: dict[str, Any] | None = None
) -> bool:
    if not isinstance(value, dict) or value.get("schema_version") != CHANGE_RECEIPT_SCHEMA_VERSION:
        return False
    if not all(isinstance(value.get(name), str) and value[name] for name in ("dataset_id", "generation_id")):
        return False
    if manifest is not None and value["generation_id"] != manifest.get("generation_id"):
        return False
    changes = value.get("changes")
    if not isinstance(changes, dict):
        return False
    if any(not isinstance(changes.get(name), list) for name in ("added", "replaced", "removed")):
        return False
    if not isinstance(changes.get("unchanged_count"), int) or changes["unchanged_count"] < 0:
        return False
    if any(not _valid_segment(item) for name in ("added", "removed") for item in changes[name]):
        return False
    return all(
        isinstance(item, dict)
        and _valid_segment(item.get("before"))
        and _valid_segment(item.get("after"))
        for item in changes["replaced"]
    )


def manifest_generation_id(
    parts: list[dict[str, Any]], *, manifest_context: dict[str, Any] | None
) -> str:
    document = {
        "parts": [
            {
                "relative_path": str(item.get("relative_path") or ""),
                "rows": int(item.get("rows") or 0),
                "sha256": str(item.get("sha256") or ""),
                "schema": str(item.get("schema") or ""),
            }
            for item in sorted(parts, key=lambda item: str(item.get("relative_path") or ""))
        ],
        "logical_plan_hash": str((manifest_context or {}).get("logical_plan_hash") or ""),
    }
    encoded = json.dumps(document, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return f"dataset-{hashlib.sha256(encoded.encode()).hexdigest()}"


def normalize_manifest_parts(manifest: dict[str, Any] | None) -> list[dict[str, Any]]:
    return _normalized_parts(manifest)


def _normalized_parts(manifest: dict[str, Any] | None) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for raw in (manifest or {}).get("parts") or []:
        if not isinstance(raw, dict) or not raw.get("relative_path") or not raw.get("sha256"):
            continue
        result.append(_with_segment_id(raw))
    return sorted(result, key=lambda item: str(item["relative_path"]))


def _with_segment_id(raw: dict[str, Any]) -> dict[str, Any]:
    item = {
        "relative_path": str(raw.get("relative_path") or ""),
        "rows": int(raw.get("rows") or 0),
        "size_bytes": int(raw.get("size_bytes") or 0),
        "sha256": str(raw.get("sha256") or ""),
        "schema": str(raw.get("schema") or ""),
    }
    item["schema_hash"] = hashlib.sha256(item["schema"].encode()).hexdigest()
    identity = f"{item['relative_path']}\0{item['sha256']}\0{item['rows']}"
    item["segment_id"] = str(raw.get("segment_id") or hashlib.sha256(identity.encode()).hexdigest())
    return item


def _dataset_id(context: dict[str, Any] | None) -> str:
    values = context or {}
    preset = str(values.get("preset") or "dataset")
    job_name = str(values.get("job_name") or "unknown")
    return f"{preset}:{job_name}"


def _valid_segment(value: object) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("relative_path"), str)
        and bool(value["relative_path"])
        and isinstance(value.get("segment_id"), str)
        and bool(value["segment_id"])
        and isinstance(value.get("sha256"), str)
        and len(value["sha256"]) == 64
        and isinstance(value.get("schema_hash"), str)
        and len(value["schema_hash"]) == 64
        and isinstance(value.get("rows"), int)
        and value["rows"] >= 0
    )
