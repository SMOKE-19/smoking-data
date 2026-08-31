from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from smoking_data.core.exceptions import SmokingDataError

from .backend import S3ObjectStore
from .config import PublicationSpec, load_object_store_target, validate_relative_prefix
from .publication import PublicationResult, publish_committed_dataset
from .remote_reader import open_remote_generation


def list_publication_receipts(project_root: str | Path) -> list[dict[str, Any]]:
    root = Path(project_root).expanduser().resolve()
    receipts = root / ".smoking-data" / "registry" / "publications"
    result: list[dict[str, Any]] = []
    if not receipts.is_dir():
        return result
    for path in sorted(receipts.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            result.append({**payload, "receipt_path": str(path)})
    return result


def inspect_remote_publication(
    project_root: str | Path,
    *,
    target: str,
    dataset_prefix: str,
) -> dict[str, Any]:
    handle = open_remote_generation(
        project_root,
        target_name=target,
        dataset_prefix=dataset_prefix,
    )
    return {
        "status": "committed",
        "dataset_uri": handle.dataset_uri,
        "generation_id": handle.generation_id,
        "pointer_etag": handle.pointer_etag,
        "manifest_key": handle.pointer.get("manifest_key"),
        "asset_code": handle.manifest.get("asset_code"),
        "job_name": handle.manifest.get("job_name"),
        "representations": handle.manifest.get("representations"),
        "sidecars": handle.manifest.get("sidecars"),
        "object_count": len(handle.manifest.get("objects") or []),
        "created_at": handle.manifest.get("created_at"),
    }


def retry_publication_receipt(
    receipt_path: str | Path,
    *,
    project_root: str | Path,
) -> PublicationResult:
    path = Path(receipt_path).expanduser().resolve()
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SmokingDataError(
            "Publication receipt is unreadable.",
            code="remote.receipt_invalid",
            context={"receipt_path": str(path)},
        ) from exc
    if not isinstance(receipt, dict) or not isinstance(receipt.get("publication"), dict):
        raise SmokingDataError(
            "Publication receipt has no retry contract.",
            code="remote.receipt_not_retryable",
        )
    publication = PublicationSpec.from_mapping(receipt["publication"])
    assert publication is not None
    result = publish_committed_dataset(
        str(receipt.get("local_dataset_root") or ""),
        project_root=project_root,
        publication=publication,
        asset_code=str(receipt.get("asset_code") or ""),
        job_name=str(receipt.get("job_name") or ""),
        definition_sha256=str(receipt.get("definition_sha256") or ""),
    )
    if result is None:
        raise SmokingDataError(
            "Publication retry contract is disabled.",
            code="remote.receipt_not_retryable",
        )
    return result


def plan_publication_gc(
    project_root: str | Path,
    *,
    target: str,
    dataset_prefix: str,
    retain_generations: int = 3,
) -> dict[str, Any]:
    if retain_generations < 1:
        raise SmokingDataError(
            "Publication GC must retain at least one generation.",
            code="remote.gc_retention_invalid",
        )
    resolved_target = load_object_store_target(project_root, target)
    prefix = validate_relative_prefix(dataset_prefix, path="dataset_prefix")
    store = S3ObjectStore(resolved_target)
    store.preflight()
    pointer_key = resolved_target.object_key(f"{prefix}/catalog/latest.json")
    pointer_payload, pointer_meta = store.get(pointer_key)
    try:
        pointer = json.loads(pointer_payload)
    except json.JSONDecodeError as exc:
        raise SmokingDataError(
            "Remote publication pointer is invalid JSON.",
            code="remote.invalid_pointer",
        ) from exc
    current = str(pointer.get("generation_id") or "")
    if not current:
        raise SmokingDataError(
            "Remote publication pointer has no generation identity.",
            code="remote.generation_missing",
        )
    generation_root = resolved_target.object_key(f"{prefix}/generations") + "/"
    objects = store.list_prefix(generation_root)
    grouped: dict[str, list[Any]] = {}
    for item in objects:
        relative = item.key.removeprefix(generation_root)
        generation_id, separator, _ = relative.partition("/")
        if separator and generation_id:
            grouped.setdefault(generation_id, []).append(item)
    ordered = sorted(
        grouped,
        key=lambda generation_id: max(
            (item.last_modified or "" for item in grouped[generation_id]), default=""
        ),
        reverse=True,
    )
    protected = set(ordered[:retain_generations])
    protected.add(current)
    candidates = [generation_id for generation_id in ordered if generation_id not in protected]
    candidate_objects = [
        item
        for generation_id in candidates
        for item in sorted(grouped[generation_id], key=lambda value: value.key)
    ]
    return {
        "schema_version": "smoking-data.publication-gc-plan.v1",
        "target": target,
        "dataset_prefix": prefix,
        "current_generation_id": current,
        "pointer_etag": pointer_meta.etag,
        "retain_generations": retain_generations,
        "protected_generations": sorted(protected),
        "candidate_generations": candidates,
        "candidate_object_count": len(candidate_objects),
        "candidate_bytes": sum(item.size_bytes for item in candidate_objects),
        "candidate_object_keys": [item.key for item in candidate_objects],
    }


def garbage_collect_publication(
    project_root: str | Path,
    *,
    target: str,
    dataset_prefix: str,
    retain_generations: int = 3,
    execute: bool = False,
    expected_generation_id: str | None = None,
) -> dict[str, Any]:
    plan = plan_publication_gc(
        project_root,
        target=target,
        dataset_prefix=dataset_prefix,
        retain_generations=retain_generations,
    )
    if not execute:
        return {**plan, "status": "dry_run", "deleted_object_count": 0}
    if not expected_generation_id or expected_generation_id != plan["current_generation_id"]:
        raise SmokingDataError(
            "GC execution requires the currently pinned generation identity.",
            code="remote.gc_generation_confirmation_required",
            context={"current_generation_id": plan["current_generation_id"]},
        )
    resolved_target = load_object_store_target(project_root, target)
    store = S3ObjectStore(resolved_target)
    store.preflight()
    pointer_key = resolved_target.object_key(
        f"{validate_relative_prefix(dataset_prefix, path='dataset_prefix')}/catalog/latest.json"
    )
    pointer_payload, pointer_meta = store.get(pointer_key)
    try:
        current_pointer = json.loads(pointer_payload)
    except json.JSONDecodeError as exc:
        raise SmokingDataError(
            "Remote publication pointer is invalid JSON.",
            code="remote.invalid_pointer",
        ) from exc
    if (
        str(current_pointer.get("generation_id") or "") != expected_generation_id
        or pointer_meta.etag != plan["pointer_etag"]
    ):
        raise SmokingDataError(
            "Remote publication pointer changed after the GC plan was created.",
            code="remote.gc_pointer_changed",
        )
    for key in plan["candidate_object_keys"]:
        store.delete(str(key))
    return {
        **plan,
        "status": "committed",
        "deleted_object_count": len(plan["candidate_object_keys"]),
    }
