from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from smoking_data.core.exceptions import ValidationError

OUTPUT_PHYSICAL_LAYOUT_VERSION = "smoking-data.output-physical-layout.v1"
WRITER_POLICY_VERSION = "smoking-data.arrow-parquet-writer.v1"
GENERATION_FIXED = "generation_fixed"
TASK_ADAPTIVE = "task_adaptive"


def resolve_configured_row_group_rows(
    policy: Mapping[str, Any] | None,
    *,
    fallback: int | None = None,
) -> int | None:
    """Resolve the public physical-layout override with an internal-preset fallback."""

    value = dict(policy or {}).get("row_group_rows", "auto")
    if value is None or value == "auto":
        return fallback
    return max(1, int(value))


def previous_output_physical_layout_matches(
    metadata: Mapping[str, Any] | None,
    *,
    asset_code: str,
    policy: Mapping[str, Any] | None,
    compression: str,
    configured_row_group_rows: int | None,
) -> bool:
    previous = _previous_layout(metadata)
    normalized = dict(policy or {})
    profile = str(normalized.get("profile") or _default_profile(asset_code))
    adaptation_scope = str(
        normalized.get("adaptation_scope") or _default_scope(asset_code)
    )
    if (
        previous.get("profile") != profile
        or previous.get("adaptation_scope") != adaptation_scope
        or previous.get("compression") != compression
    ):
        return False
    if configured_row_group_rows is None:
        return True
    configured = max(1, int(configured_row_group_rows))
    if adaptation_scope == GENERATION_FIXED:
        return previous.get("generation_output_row_group_rows") == configured
    task_rows = previous.get("task_output_row_group_rows")
    return isinstance(task_rows, Mapping) and bool(task_rows) and all(
        value == configured for value in task_rows.values()
    )


def resolve_output_physical_layout(
    *,
    asset_code: str,
    policy: Mapping[str, Any] | None,
    compression: str,
    configured_row_group_rows: int | None,
    task_row_group_recommendations: Mapping[str, int],
    previous_metadata: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, int]]:
    normalized = dict(policy or {})
    profile = str(normalized.get("profile") or _default_profile(asset_code))
    adaptation_scope = str(
        normalized.get("adaptation_scope") or _default_scope(asset_code)
    )
    _validate_scope(asset_code, adaptation_scope)
    recommendations = {
        str(task_id): max(1, int(rows))
        for task_id, rows in task_row_group_recommendations.items()
    }
    previous = _previous_layout(previous_metadata)

    if adaptation_scope == GENERATION_FIXED:
        selected, source = _generation_row_group_rows(
            profile=profile,
            compression=compression,
            configured=configured_row_group_rows,
            recommendations=recommendations,
            previous=previous,
        )
        task_rows = {task_id: selected for task_id in recommendations}
        generation_rows: int | None = selected
    else:
        source = "manual_override" if configured_row_group_rows is not None else "task_planner"
        task_rows = {
            task_id: (
                max(1, int(configured_row_group_rows))
                if configured_row_group_rows is not None
                else rows
            )
            for task_id, rows in recommendations.items()
        }
        generation_rows = None

    canonical = {
        "schema_version": OUTPUT_PHYSICAL_LAYOUT_VERSION,
        "asset_code": asset_code,
        "profile": profile,
        "adaptation_scope": adaptation_scope,
        "compression": compression,
        "writer_policy_version": WRITER_POLICY_VERSION,
        "generation_output_row_group_rows": generation_rows,
    }
    profile_hash = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return (
        {
            **canonical,
            "profile_hash": profile_hash,
            "selection_source": source,
            "task_output_row_group_rows": task_rows,
            "planner_recommendations": recommendations,
            "pinned_for_generation": adaptation_scope == GENERATION_FIXED,
        },
        task_rows,
    )


def _generation_row_group_rows(
    *,
    profile: str,
    compression: str,
    configured: int | None,
    recommendations: Mapping[str, int],
    previous: Mapping[str, Any],
) -> tuple[int, str]:
    if configured is not None:
        return max(1, int(configured)), "manual_override"
    previous_rows = previous.get("generation_output_row_group_rows")
    if (
        previous.get("profile") == profile
        and previous.get("adaptation_scope") == GENERATION_FIXED
        and previous.get("compression") == compression
        and isinstance(previous_rows, int)
        and not isinstance(previous_rows, bool)
        and previous_rows > 0
    ):
        return previous_rows, "previous_generation"
    if not recommendations:
        return 20_000, "planner_fallback"
    # One conservative value keeps every partition within the widest task's
    # writer-memory estimate while preserving one physical contract per generation.
    return min(recommendations.values()), "generation_planner"


def _previous_layout(metadata: Mapping[str, Any] | None) -> Mapping[str, Any]:
    result = (metadata or {}).get("result")
    if not isinstance(result, Mapping):
        return {}
    details = result.get("details")
    if not isinstance(details, Mapping):
        return {}
    layout = details.get("output_physical_layout")
    return layout if isinstance(layout, Mapping) else {}


def _validate_scope(asset_code: str, adaptation_scope: str) -> None:
    allowed = {GENERATION_FIXED} if asset_code in {"0201", "0301"} else {
        GENERATION_FIXED,
        TASK_ADAPTIVE,
    }
    if adaptation_scope not in allowed:
        raise ValidationError(
            f"Asset {asset_code} does not allow output physical layout scope "
            f"{adaptation_scope!r}.",
            code="output.invalid_physical_layout_scope",
            context={"asset_code": asset_code, "allowed": sorted(allowed)},
        )


def _default_scope(asset_code: str) -> str:
    return TASK_ADAPTIVE if asset_code == "0401" else GENERATION_FIXED


def _default_profile(asset_code: str) -> str:
    return {
        "0201": "curated_reuse_v1",
        "0301": "joined_reuse_v1",
        "0401": "analysis_snapshot_adaptive_v1",
    }.get(asset_code, "reusable_dataset_v1")
