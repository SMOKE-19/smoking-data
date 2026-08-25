from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from smoking_data.runtime.paths import file_sha256

MANIFEST_NAME = "_dataset.manifest.json"


def describe_dataset_artifacts(
    output_paths: Iterable[Path],
    *,
    metadata_path: Path | None,
    definition_sha256: str | None,
) -> list[dict[str, Any]]:
    """Describe committed dataset manifests referenced by stage output paths."""

    roots: set[Path] = set()
    for output_path in output_paths:
        candidate = output_path.resolve()
        if candidate.is_file():
            candidate = candidate.parent
        for parent in (candidate, *candidate.parents):
            if (parent / MANIFEST_NAME).is_file():
                roots.add(parent)
                break

    descriptors: list[dict[str, Any]] = []
    for root in sorted(roots):
        manifest_path = root / MANIFEST_NAME
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(manifest, dict):
            continue
        descriptors.append(
            {
                "dataset_path": str(root),
                "metadata_path": str(metadata_path) if metadata_path else None,
                "manifest_path": str(manifest_path),
                "manifest_sha256": file_sha256(manifest_path),
                "manifest_schema_version": manifest.get("version"),
                "definition_sha256": definition_sha256,
                "generation_id": manifest.get("generation_id"),
                "rows": manifest.get("rows"),
                "parts": len(manifest.get("parts") or []),
            }
        )
    return descriptors
