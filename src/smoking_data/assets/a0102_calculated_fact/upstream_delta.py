from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from smoking_data.runtime.change_receipt import (
    normalize_manifest_parts,
    read_dataset_change_receipt,
)

from .calculation_manifest import read_calculation_manifest
from .planning import CalculatedFactRunPlan


@dataclass(frozen=True, slots=True)
class UpstreamSegment:
    segment_id: str
    relative_path: str
    path: Path
    sha256: str
    rows: int


@dataclass(frozen=True, slots=True)
class UpstreamDeltaPlan:
    mode: str
    dataset_root: Path | None
    dataset_id: str | None
    generation_id: str | None
    calculation_contract_hash: str
    selected_files: tuple[Path, ...]
    current_segments: tuple[UpstreamSegment, ...]
    selected_segments: tuple[UpstreamSegment, ...]
    removed_segment_ids: tuple[str, ...]
    previous_segment_ids: tuple[str, ...]
    reason: str

    @property
    def segment_id_by_path(self) -> dict[str, str]:
        return {str(item.path): item.segment_id for item in self.current_segments}

    @property
    def selected_segment_ids(self) -> tuple[str, ...]:
        return tuple(item.segment_id for item in self.selected_segments)


def plan_upstream_delta(
    plan: CalculatedFactRunPlan, *, calculation_manifest_path: Path
) -> UpstreamDeltaPlan:
    contract_hash = calculation_contract_hash(plan)
    root = _dataset_root(plan.upstream_files)
    if root is None:
        return _full_without_receipt(plan, contract_hash, "upstream_manifest_unavailable")
    manifest = _read_json(root / "_dataset.manifest.json")
    receipt = read_dataset_change_receipt(root)
    if manifest is None or receipt is None:
        return _full_without_receipt(plan, contract_hash, "upstream_receipt_unavailable")
    generation_id = str(manifest.get("generation_id") or "")
    if not generation_id or receipt.get("generation_id") != generation_id:
        return _full_without_receipt(plan, contract_hash, "upstream_receipt_generation_mismatch")
    segments = tuple(
        UpstreamSegment(
            segment_id=str(item["segment_id"]),
            relative_path=str(item["relative_path"]),
            path=(root / str(item["relative_path"])).resolve(),
            sha256=str(item["sha256"]),
            rows=int(item["rows"]),
        )
        for item in normalize_manifest_parts(manifest)
    )
    if {item.path for item in segments} != set(plan.upstream_files):
        return _full_without_receipt(plan, contract_hash, "upstream_manifest_file_mismatch")
    dataset_id = str(receipt.get("dataset_id") or "")
    previous_manifest = read_calculation_manifest(calculation_manifest_path)
    if previous_manifest is None:
        return UpstreamDeltaPlan(
            mode="initial",
            dataset_root=root,
            dataset_id=dataset_id,
            generation_id=generation_id,
            calculation_contract_hash=contract_hash,
            selected_files=tuple(item.path for item in segments),
            current_segments=segments,
            selected_segments=segments,
            removed_segment_ids=(),
            previous_segment_ids=(),
            reason="calculation_manifest_missing",
        )
    previous = previous_manifest.segments_by_path
    previous_segment_ids = tuple(item.segment_id for item in previous_manifest.active_segments)
    if (
        previous_manifest.dataset_id != dataset_id
        or previous_manifest.calculation_contract_hash != contract_hash
    ):
        return UpstreamDeltaPlan(
            mode="full_contract_change",
            dataset_root=root,
            dataset_id=dataset_id,
            generation_id=generation_id,
            calculation_contract_hash=contract_hash,
            selected_files=tuple(item.path for item in segments),
            current_segments=segments,
            selected_segments=segments,
            removed_segment_ids=previous_segment_ids,
            previous_segment_ids=previous_segment_ids,
            reason="dataset_or_calculation_contract_changed",
        )
    selected = tuple(
        item
        for item in segments
        if item.relative_path not in previous
        or previous[item.relative_path].sha256 != item.sha256
    )
    current_paths = {item.relative_path for item in segments}
    removed = tuple(
        value.segment_id
        for path, value in previous.items()
        if path not in current_paths
    )
    return UpstreamDeltaPlan(
        mode="unchanged" if not selected and not removed else "delta",
        dataset_root=root,
        dataset_id=dataset_id,
        generation_id=generation_id,
        calculation_contract_hash=contract_hash,
        selected_files=tuple(item.path for item in selected),
        current_segments=segments,
        selected_segments=selected,
        removed_segment_ids=removed,
        previous_segment_ids=previous_segment_ids,
        reason=(
            "upstream_generation_already_consumed"
            if not selected and not removed
            else "manifest_segment_delta"
        ),
    )


def calculation_contract_hash(plan: CalculatedFactRunPlan) -> str:
    document = {
        "identity_columns": plan.spec.identity_columns,
        "binding_hash": plan.binding.binding_hash,
        "fingerprints": [
            {
                "name": item.name,
                "expression_hash": item.expression_hash,
                "binding_hash": item.binding_hash,
                "source_columns": item.source_columns,
                "constants": item.constants,
            }
            for item in plan.fingerprints
        ],
    }
    encoded = json.dumps(document, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _full_without_receipt(
    plan: CalculatedFactRunPlan, contract_hash: str, reason: str
) -> UpstreamDeltaPlan:
    return UpstreamDeltaPlan(
        mode="full_scan",
        dataset_root=None,
        dataset_id=None,
        generation_id=None,
        calculation_contract_hash=contract_hash,
        selected_files=plan.upstream_files,
        current_segments=(),
        selected_segments=(),
        removed_segment_ids=(),
        previous_segment_ids=(),
        reason=reason,
    )


def _dataset_root(files: tuple[Path, ...]) -> Path | None:
    roots: set[Path] = set()
    for path in files:
        found = next(
            (
                parent
                for parent in (path.parent, *path.parents)
                if (parent / "_dataset.manifest.json").is_file()
            ),
            None,
        )
        if found is None:
            return None
        roots.add(found.resolve())
    return next(iter(roots)) if len(roots) == 1 else None


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None
