from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from smoking_data.core.exceptions import ValidationError

CALCULATION_MANIFEST_SCHEMA_VERSION = (
    "smoking-data.calculated-fact-calculation-manifest.v1"
)


@dataclass(frozen=True, slots=True)
class CalculatedSegment:
    segment_id: str
    relative_path: str
    sha256: str
    rows: int
    output_generation_seq: int | None = None
    output_parts: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CalculationManifest:
    dataset_id: str | None
    upstream_generation_id: str | None
    calculation_contract_hash: str | None
    active_segments: tuple[CalculatedSegment, ...]

    @property
    def segments_by_path(self) -> dict[str, CalculatedSegment]:
        return {item.relative_path: item for item in self.active_segments}


def read_calculation_manifest(path: str | Path) -> CalculationManifest | None:
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        return None
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _fail(
            "incremental.invalid_calculation_manifest",
            "0102 calculation manifest is unreadable.",
            path=str(source),
            reason=str(exc),
        )
    if not isinstance(payload, dict) or payload.get("schema_version") != (
        CALCULATION_MANIFEST_SCHEMA_VERSION
    ):
        _fail(
            "incremental.invalid_calculation_manifest",
            "0102 calculation manifest schema is incompatible.",
            path=str(source),
        )
    raw_segments = payload.get("active_segments")
    if not isinstance(raw_segments, list):
        _fail(
            "incremental.invalid_calculation_manifest",
            "0102 calculation manifest active_segments must be a list.",
            path=str(source),
        )
    segments: list[CalculatedSegment] = []
    for index, item in enumerate(raw_segments):
        if not isinstance(item, Mapping):
            _fail(
                "incremental.invalid_calculation_manifest",
                "0102 calculation manifest segment must be a mapping.",
                index=index,
            )
        try:
            output_parts = tuple(str(value) for value in item.get("output_parts") or ())
            segments.append(
                CalculatedSegment(
                    segment_id=str(item["segment_id"]),
                    relative_path=str(item["relative_path"]),
                    sha256=str(item["sha256"]),
                    rows=int(item["rows"]),
                    output_generation_seq=(
                        int(item["output_generation_seq"])
                        if item.get("output_generation_seq") is not None
                        else None
                    ),
                    output_parts=output_parts,
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            _fail(
                "incremental.invalid_calculation_manifest",
                "0102 calculation manifest segment is invalid.",
                index=index,
                reason=str(exc),
            )
    if len({item.relative_path for item in segments}) != len(segments):
        _fail(
            "incremental.invalid_calculation_manifest",
            "0102 calculation manifest contains duplicate source paths.",
        )
    return CalculationManifest(
        dataset_id=_optional_string(payload.get("dataset_id")),
        upstream_generation_id=_optional_string(payload.get("upstream_generation_id")),
        calculation_contract_hash=_optional_string(
            payload.get("calculation_contract_hash")
        ),
        active_segments=tuple(segments),
    )


def write_calculation_manifest(
    path: str | Path,
    *,
    dataset_id: str | None,
    upstream_generation_id: str | None,
    calculation_contract_hash: str,
    active_segments: Sequence[CalculatedSegment],
) -> Path:
    output = Path(path).expanduser().resolve()
    payload = {
        "schema_version": CALCULATION_MANIFEST_SCHEMA_VERSION,
        "dataset_id": dataset_id,
        "upstream_generation_id": upstream_generation_id,
        "calculation_contract_hash": calculation_contract_hash,
        "active_segments": [
            {
                "segment_id": item.segment_id,
                "relative_path": item.relative_path,
                "sha256": item.sha256,
                "rows": item.rows,
                "output_generation_seq": item.output_generation_seq,
                "output_parts": list(item.output_parts),
            }
            for item in sorted(active_segments, key=lambda value: value.relative_path)
        ],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.with_suffix(output.suffix + ".tmp")
    staging.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    staging.replace(output)
    return output


def _optional_string(value: object) -> str | None:
    return str(value) if value not in (None, "") else None


def _fail(code: str, message: str, **context: object) -> None:
    raise ValidationError(message, code=code, context=context)
