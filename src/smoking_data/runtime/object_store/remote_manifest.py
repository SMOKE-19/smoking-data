from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

REMOTE_MANIFEST_VERSION = "smoking-data.remote-dataset-bundle.v2"
REMOTE_POINTER_VERSION = "smoking-data.remote-dataset-pointer.v1"
PUBLICATION_RECEIPT_VERSION = "smoking-data.publication-receipt.v2"


def canonical_json(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def payload_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class RemoteObject:
    role: str
    object_key: str
    size_bytes: int
    sha256: str
    etag: str | None
    version_id: str | None
    checksum_sha256: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "object_key": self.object_key,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "etag": self.etag,
            "version_id": self.version_id,
            "checksum_sha256": self.checksum_sha256,
        }


def build_remote_manifest(
    *,
    dataset_id: str,
    asset_code: str,
    job_name: str,
    generation_id: str,
    parent_generation_id: str | None,
    local_manifest_sha256: str,
    definition_sha256: str,
    schema_fingerprint: str,
    objects: Iterable[RemoteObject],
    target_identity: dict[str, Any],
    representations: dict[str, Any],
    sidecars: dict[str, Any],
    runtime_versions: dict[str, str],
    created_at: str,
) -> dict[str, Any]:
    object_rows = [item.to_dict() for item in objects]
    object_rows.sort(key=lambda item: (item["role"], item["object_key"]))
    object_set_sha256 = payload_sha256(
        [{key: item[key] for key in ("role", "object_key", "size_bytes", "sha256")} for item in object_rows]
    )
    return {
        "schema_version": REMOTE_MANIFEST_VERSION,
        "dataset_id": dataset_id,
        "asset_code": asset_code,
        "job_name": job_name,
        "generation_id": generation_id,
        "parent_generation_id": parent_generation_id,
        "local_manifest_sha256": local_manifest_sha256,
        "definition_sha256": definition_sha256,
        "schema_fingerprint": schema_fingerprint,
        "target": target_identity,
        "objects": object_rows,
        "representations": representations,
        "sidecars": sidecars,
        "object_set_sha256": object_set_sha256,
        "created_at": created_at,
        "runtime_versions": runtime_versions,
    }


def build_pointer(*, generation_id: str, manifest_key: str, manifest_sha256: str) -> dict[str, str]:
    return {
        "schema_version": REMOTE_POINTER_VERSION,
        "generation_id": generation_id,
        "manifest_key": manifest_key,
        "manifest_sha256": manifest_sha256,
        "committed_at": utc_now(),
    }
