from __future__ import annotations

from copy import deepcopy
from typing import Any

from smoking_data.core.exceptions import ValidationError

_DEFINITION_PUBLICATION_KEYS = frozenset({"target", "parquet", "sbdf"})


def publication_aware_defaults(
    defaults: dict[str, Any],
    *,
    payload: dict[str, Any],
    asset_code: str,
) -> dict[str, Any]:
    """Keep global publication policy only for an explicitly opted-in Definition."""

    prepared = deepcopy(defaults)
    publication = definition_publication(payload)
    if publication is None:
        remove_publication(prepared)
        return prepared

    unknown = sorted(set(publication) - _DEFINITION_PUBLICATION_KEYS)
    if unknown:
        raise ValidationError(
            f"Asset {asset_code} Definition publication may select target and dataset-specific representations only; shared policy belongs in .smoking-data/config.yaml.",
            code="output.publication_override_invalid",
            context={
                "path": "output.artifact.publication",
                "unsupported_keys": unknown,
            },
        )
    return prepared


def definition_publication(payload: dict[str, Any]) -> dict[str, Any] | None:
    output = payload.get("output")
    artifact = output.get("artifact") if isinstance(output, dict) else None
    publication = artifact.get("publication") if isinstance(artifact, dict) else None
    return publication if isinstance(publication, dict) else None


def remove_publication(payload: dict[str, Any]) -> None:
    output = payload.get("output")
    artifact = output.get("artifact") if isinstance(output, dict) else None
    if isinstance(artifact, dict):
        artifact.pop("publication", None)
