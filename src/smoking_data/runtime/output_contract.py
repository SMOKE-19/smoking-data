from __future__ import annotations

from typing import Any

from smoking_data.core.exceptions import ValidationError


def resolve_physical_writer_output(raw: dict[str, Any], *, asset_code: str) -> dict[str, Any]:
    """Adapt the canonical output envelope to private physical-kernel options."""

    output = raw.get("output")
    if not isinstance(output, dict):
        raise ValidationError(
            f"{asset_code} output contract is required.",
            code="asset.invalid_output_contract",
        )
    artifact = output.get("artifact")
    if not isinstance(artifact, dict):
        # Keep direct preset callers and old test fixtures working while public
        # pipeline YAMLs use the canonical envelope.
        return dict(output)
    root_dir = str(artifact.get("root_dir") or "").strip()
    if not root_dir:
        raise ValidationError(
            f"{asset_code} output.artifact.root_dir is required.",
            code="asset.invalid_output_contract",
        )
    pipeline = raw.get("__pipeline")
    partition_columns = pipeline.get("writer_partition_columns") if isinstance(pipeline, dict) else None
    if not isinstance(partition_columns, list) or len(partition_columns) != 1:
        raise ValidationError(
            f"{asset_code} writer requires exactly one partition column.",
            code="physical_kernel.partition_arity",
            context={"partition_columns": partition_columns},
        )
    artifact_format = str(artifact.get("format") or "parquet")
    return {
        "output_dir": root_dir,
        "partition_column": str(partition_columns[0]),
        # Atomic dataset publication is an engine invariant, not a YAML choice.
        "overwrite": True,
        "compression": artifact.get("compression", "zstd"),
        **(
            {"format": "sbdf", "sbdf": dict(artifact.get("sbdf") or {})}
            if artifact_format == "sbdf"
            else {}
        ),
        "physical_layout": dict(artifact.get("physical_layout") or {}),
    }
