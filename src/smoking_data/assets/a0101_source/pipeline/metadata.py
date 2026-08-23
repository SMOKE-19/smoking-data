"""Artifact-local provenance metadata for 0101 Source datasets."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from smoking_data.runtime.paths import file_sha256

SOURCE_ARTIFACT_METADATA_VERSION = "smoking-data.artifact-metadata.v1"


@dataclass(slots=True)
class SourceMetadataRecord:
    raw_dataset_path: str
    status: str
    event: str = "task_result"
    job_name: str | None = None
    sub_job_name: str | None = None
    task_job_name: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    sql_text: str | None = None
    sql_template: str | None = None
    sql_parameters: dict[str, str] | None = None
    sql_renderer_version: str | None = None
    sql_revision: str | None = None
    sql_revision_hash: str | None = None
    raw_dataset_fingerprint: str | None = None
    source_write_profile_path: str | None = None
    data_api_captured_fields: dict[str, Any] | None = None
    data_api_capture_status: str | None = None
    data_api_capture_match_count: int | None = None
    attempts: int = 1
    run_count: int = 1
    updated_at: str | None = None
    first_success_at: str | None = None
    last_success_at: str | None = None
    last_error_at: str | None = None
    last_error_message: str | None = None
    last_error_stage: str | None = None
    last_error_code: str | None = None
    last_error_stdout: str | None = None
    last_error_stderr: str | None = None
    test_run: dict[str, Any] | None = None


def read_source_metadata(path: Path) -> list[SourceMetadataRecord]:
    metadata_path = path / "_smoking_data" / "metadata.json" if path.is_dir() else path
    if not metadata_path.exists():
        return []
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != SOURCE_ARTIFACT_METADATA_VERSION:
        raise ValueError(f"Invalid SOURCE artifact metadata: {metadata_path}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError(f"SOURCE artifact metadata result must be an object: {metadata_path}")
    return [SourceMetadataRecord(**result)]


def write_source_artifact_provenance(
    dataset_root: Path,
    *,
    record: SourceMetadataRecord,
    definition_path: Path,
    query_sql: str,
    source_write_profile: dict[str, Any] | None = None,
) -> Path:
    if record.status != "success":
        raise ValueError("Only successful SOURCE results can be written into an artifact.")
    now = record.updated_at or _utc_now_iso()
    record.updated_at = now
    record.first_success_at = now
    record.last_success_at = now
    record.last_error_at = None
    record.last_error_message = None
    record.last_error_stage = None
    record.last_error_code = None
    record.last_error_stdout = None
    record.last_error_stderr = None
    provenance_root = dataset_root / "_smoking_data"
    provenance_root.mkdir(parents=True, exist_ok=True)
    query_path = provenance_root / "query.sql"
    query_path.write_text(query_sql.rstrip() + "\n", encoding="utf-8")
    definition_target = provenance_root / "definition.yaml"
    definition_target.write_text(definition_path.read_text(encoding="utf-8"), encoding="utf-8")
    if source_write_profile is not None:
        (provenance_root / "source-write-profile.json").write_text(
            json.dumps(source_write_profile, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    metadata_path = provenance_root / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "schema_version": SOURCE_ARTIFACT_METADATA_VERSION,
                "created_at": now,
                "asset": {"code": "0101", "job_name": record.job_name},
                "definition": {
                    "path": "_smoking_data/definition.yaml",
                    "sha256": file_sha256(definition_target),
                },
                "query": {
                    "path": "_smoking_data/query.sql",
                    "sha256": file_sha256(query_path),
                    "revision": record.sql_revision,
                    "revision_hash": record.sql_revision_hash,
                },
                "result": asdict(record),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return metadata_path


def write_source_dataset_catalog(root: str | Path) -> Path:
    dataset_root = Path(root).resolve()
    datasets: list[dict[str, Any]] = []
    for dataset in sorted(dataset_root.glob("*.dataset")):
        metadata_path = dataset / "_smoking_data" / "metadata.json"
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict) or result.get("status") != "success":
            continue
        datasets.append(
            {
                "relative_path": dataset.relative_to(dataset_root).as_posix(),
                "labels": {
                    "asset_code": "0101",
                    "sub_job": result.get("sub_job_name"),
                    "date_from": result.get("date_from"),
                    "date_to": result.get("date_to"),
                },
            }
        )
    catalog_path = dataset_root / "_smoking_data" / "dataset-catalog.json"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = catalog_path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(
            {
                "schema_version": "smoking-data.dataset-catalog.v1",
                "asset_code": "0101",
                "updated_at": _utc_now_iso(),
                "datasets": datasets,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    temporary.replace(catalog_path)
    return catalog_path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
