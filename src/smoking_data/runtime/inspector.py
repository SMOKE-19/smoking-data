from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pyarrow.parquet as pq

from smoking_data.core.exceptions import SmokingDataError
from smoking_data.core.results import to_json_safe
from smoking_data.runtime.paths import resolve_project_path

INSPECTION_SCHEMA_VERSION = "smoking-data.inspection.v1"
_MAX_JSON_FILES = 1000
_MAX_JSON_BYTES = 16 * 1024 * 1024
_MAX_PARQUET_FOOTERS = 1000
_MAX_EVIDENCE_ITEMS = 500

_FAILURE_STATES = {
    "blocked",
    "error",
    "failed",
    "failure",
    "partial",
    "timed_out",
}
_MISSING_STATES = {
    "blocked_missing_dependency",
    "skipped_missing_dependency",
}
_PROFILE_KEY_TOKENS = (
    "elapsed_sec",
    "peak_rss",
    "rss_peak",
    "memory_mb",
    "cpu_sec",
    "io_read_bytes",
    "io_write_bytes",
    "rows_per_sec",
    "bytes_per_sec",
)


def inspect_path(
    path: str | Path,
    *,
    mode: str,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    if mode not in {"dataset", "failure", "missing", "profile"}:
        raise ValueError(f"unsupported inspection mode: {mode}")
    root = Path(project_root or Path.cwd()).expanduser().resolve()
    target = resolve_project_path(path, project_root=root)
    if not target.exists():
        raise SmokingDataError(
            f"Inspection path does not exist: {target}",
            code="inspect.path_missing",
            context={"path": str(target)},
        )

    documents, skipped = _read_documents(target, mode=mode)
    failures = _collect_failures(documents)
    missing = _collect_missing_evidence(documents)
    profile_metrics = _collect_profile_metrics(documents)
    payload: dict[str, Any] = {
        "ok": True,
        "schema_version": INSPECTION_SCHEMA_VERSION,
        "inspection": {
            "mode": mode,
            "path": str(target),
            "project_root": str(root),
            "read_only": True,
        },
        "documents": [_document_summary(item) for item in documents],
        "skipped_documents": skipped,
    }
    if mode == "dataset":
        payload["dataset"] = _dataset_summary(target, documents)
        payload["failures"] = failures
        payload["missing_evidence"] = missing
        payload["profile_metrics"] = profile_metrics
    elif mode == "failure":
        payload["failures"] = failures
        payload["warnings"] = _collect_warnings(documents)
        payload["diagnosis_hints"] = _failure_hints(failures, missing)
    elif mode == "missing":
        payload["missing_evidence"] = missing
        payload["diagnosis_hints"] = _missing_hints(missing)
    else:
        payload["profile_metrics"] = profile_metrics
        payload["diagnosis_hints"] = _profile_hints(profile_metrics)
    return to_json_safe(payload)


def _read_documents(
    target: Path,
    *,
    mode: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    paths = _document_paths(target, mode=mode)
    documents: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for path in paths[:_MAX_JSON_FILES]:
        size = path.stat().st_size
        if size > _MAX_JSON_BYTES:
            skipped.append(
                {"path": str(path), "reason": "json_too_large", "size_bytes": size}
            )
            continue
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            skipped.append(
                {"path": str(path), "reason": "json_unreadable", "error": str(exc)}
            )
            continue
        documents.append({"path": path, "size_bytes": size, "value": value})
    if len(paths) > _MAX_JSON_FILES:
        skipped.append(
            {
                "path": str(target),
                "reason": "json_file_limit",
                "discovered": len(paths),
                "inspected": _MAX_JSON_FILES,
            }
        )
    return documents, skipped


def _document_paths(target: Path, *, mode: str) -> list[Path]:
    if target.is_file():
        return [target] if target.suffix.lower() == ".json" else []
    candidates: set[Path] = set()
    manifest = target / "_dataset.manifest.json"
    if manifest.is_file():
        candidates.add(manifest)
    metadata_root = target / "_smoking_data"
    if metadata_root.is_dir():
        candidates.update(path for path in metadata_root.rglob("*.json") if path.is_file())
    if mode != "dataset" or not candidates:
        candidates.update(path for path in target.rglob("*.json") if path.is_file())
    return sorted(candidates)


def _document_summary(document: dict[str, Any]) -> dict[str, Any]:
    value = document["value"]
    return {
        "path": str(document["path"]),
        "size_bytes": document["size_bytes"],
        "schema_version": value.get("schema_version") if isinstance(value, dict) else None,
        "top_level_keys": sorted(str(key) for key in value) if isinstance(value, dict) else [],
    }


def _dataset_summary(target: Path, documents: list[dict[str, Any]]) -> dict[str, Any]:
    dataset_root = target if target.is_dir() else target.parent
    parquet_paths = sorted(path for path in dataset_root.rglob("*.parquet") if path.is_file())
    inspected = parquet_paths[:_MAX_PARQUET_FOOTERS]
    rows = 0
    schema_variants: set[str] = set()
    footer_errors: list[dict[str, str]] = []
    for path in inspected:
        try:
            parquet = pq.ParquetFile(path)
            rows += int(parquet.metadata.num_rows)
            schema_variants.add(str(parquet.schema_arrow))
        except (OSError, ValueError) as exc:
            footer_errors.append({"path": str(path), "error": str(exc)})

    manifest = _first_schema_document(documents, "smoking-data.dataset-manifest.v1")
    metadata = _first_named_document(documents, "metadata.json")
    catalog = _first_schema_document(documents, "smoking-data.dataset-catalog.v1")
    source_manifest = _first_schema_document(
        documents, "smoking-data.csv-source-file-manifest.v1"
    )
    calculation_status = _first_schema_document(
        documents, "smoking-data.calculation-status.v1"
    )
    return {
        "root": str(dataset_root),
        "parquet": {
            "file_count": len(parquet_paths),
            "size_bytes": sum(path.stat().st_size for path in parquet_paths),
            "footer_files_inspected": len(inspected),
            "footer_scan_complete": len(inspected) == len(parquet_paths),
            "rows_from_inspected_footers": rows,
            "schema_variant_count": len(schema_variants),
            "footer_errors": footer_errors[:_MAX_EVIDENCE_ITEMS],
        },
        "manifest": _manifest_summary(manifest),
        "metadata": _metadata_summary(metadata),
        "dataset_catalog": _catalog_summary(catalog),
        "source_file_manifest": _source_manifest_summary(source_manifest),
        "calculation_status": _calculation_status_summary(calculation_status),
    }


def _first_schema_document(
    documents: list[dict[str, Any]], schema_version: str
) -> dict[str, Any] | None:
    for document in documents:
        value = document["value"]
        if isinstance(value, dict) and (
            value.get("schema_version") == schema_version
            or value.get("version") == schema_version
        ):
            return value
    return None


def _first_named_document(
    documents: list[dict[str, Any]], file_name: str
) -> dict[str, Any] | None:
    for document in documents:
        if document["path"].name == file_name and isinstance(document["value"], dict):
            return document["value"]
    return None


def _manifest_summary(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "version": value.get("version"),
        "generation_id": value.get("generation_id"),
        "parent_generation_id": value.get("parent_generation_id"),
        "rows": value.get("rows"),
        "part_count": len(value.get("parts") or []),
        "context": value.get("context") or {},
        "provenance_count": len(value.get("provenance") or []),
    }


def _metadata_summary(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "schema_version": value.get("schema_version"),
        "created_at": value.get("created_at"),
        "asset": value.get("asset"),
        "job_name": value.get("job_name"),
        "status": value.get("status"),
        "definition": value.get("definition"),
        "counters": value.get("counters") or {},
        "warning_count": value.get("warning_count", len(value.get("warnings") or [])),
    }


def _catalog_summary(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    datasets = [item for item in value.get("datasets") or [] if isinstance(item, dict)]
    return {
        "asset_code": value.get("asset_code"),
        "dataset_count": len(datasets),
        "labels": [item.get("labels") or {} for item in datasets[:_MAX_EVIDENCE_ITEMS]],
    }


def _source_manifest_summary(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    files = [item for item in value.get("files") or [] if isinstance(item, dict)]
    statuses: dict[str, int] = {}
    for item in files:
        status = str(item.get("status") or "unknown")
        statuses[status] = statuses.get(status, 0) + 1
    return {
        "file_count": len(files),
        "statuses": statuses,
        "relative_paths": [item.get("relative_path") for item in files[:_MAX_EVIDENCE_ITEMS]],
    }


def _calculation_status_summary(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    calculations = [
        item for item in value.get("calculations") or [] if isinstance(item, dict)
    ]
    states: dict[str, int] = {}
    for item in calculations:
        state = str(item.get("calculation_state") or "unknown")
        states[state] = states.get(state, 0) + 1
    return {"calculation_count": len(calculations), "states": states}


def _collect_failures(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for document, json_path, value in _walk_documents(documents):
        if not isinstance(value, dict):
            continue
        status = str(value.get("status") or value.get("calculation_state") or "").lower()
        has_error = bool(value.get("error_code") or value.get("error_message"))
        if not has_error and status not in _FAILURE_STATES | _MISSING_STATES:
            continue
        result.append(
            {
                "document": str(document),
                "json_path": json_path,
                "status": status or None,
                "error_code": value.get("error_code"),
                "error_message": value.get("error_message") or value.get("message"),
                "error_context": value.get("error_context") or value.get("context") or {},
            }
        )
        if len(result) >= _MAX_EVIDENCE_ITEMS:
            break
    return result


def _collect_warnings(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for document, json_path, value in _walk_documents(documents):
        if not isinstance(value, dict):
            continue
        if "warning" not in json_path.lower() and not str(value.get("code") or "").startswith(
            ("0101.", "0102.", "0103.", "0201.", "0301.", "0401.")
        ):
            continue
        if value.get("code") or value.get("message"):
            result.append(
                {
                    "document": str(document),
                    "json_path": json_path,
                    "code": value.get("code"),
                    "message": value.get("message"),
                    "context": value.get("context") or {},
                }
            )
        if len(result) >= _MAX_EVIDENCE_ITEMS:
            break
    return result


def _collect_missing_evidence(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for document, json_path, value in _walk_documents(documents):
        if not isinstance(value, dict):
            continue
        state = str(value.get("status") or value.get("calculation_state") or "").lower()
        if state in _MISSING_STATES:
            result.append(
                {
                    "document": str(document),
                    "json_path": json_path,
                    "state": state,
                    "kind": "missing_dependency",
                    "value": to_json_safe(value.get("missing_dependencies") or {}),
                }
            )
            if len(result) >= _MAX_EVIDENCE_ITEMS:
                return result
        for key, item in value.items():
            key_text = str(key).lower()
            is_missing_key = "missing" in key_text and item not in (None, "", [], {})
            is_route_signal = key_text in {"unmatched_rows", "deleted_files"} and _positive(item)
            if not is_missing_key and not is_route_signal:
                continue
            result.append(
                {
                    "document": str(document),
                    "json_path": f"{json_path}.{key}",
                    "state": state or None,
                    "kind": "route_or_deletion" if is_route_signal else "missing_dependency",
                    "value": to_json_safe(item),
                }
            )
            if len(result) >= _MAX_EVIDENCE_ITEMS:
                return result
    return result


def _collect_profile_metrics(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for document, json_path, value in _walk_documents(documents):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        key = json_path.rsplit(".", 1)[-1].lower()
        if not any(token in key for token in _PROFILE_KEY_TOKENS):
            continue
        result.append(
            {
                "document": str(document),
                "json_path": json_path,
                "metric": key,
                "value": value,
            }
        )
        if len(result) >= _MAX_EVIDENCE_ITEMS:
            break
    return result


def _walk_documents(
    documents: list[dict[str, Any]],
) -> Iterable[tuple[Path, str, Any]]:
    for document in documents:
        yield from _walk(document["path"], "$", document["value"])


def _walk(document: Path, json_path: str, value: Any) -> Iterable[tuple[Path, str, Any]]:
    yield document, json_path, value
    if isinstance(value, dict):
        for key, item in value.items():
            yield from _walk(document, f"{json_path}.{key}", item)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk(document, f"{json_path}[{index}]", item)


def _failure_hints(
    failures: list[dict[str, Any]], missing: list[dict[str, Any]]
) -> list[str]:
    hints: list[str] = []
    if not failures:
        hints.append("구조화된 error_code 또는 실패 상태를 찾지 못했습니다.")
    if missing:
        hints.append("missing_evidence가 있으므로 upstream schema·route·계산 의존성을 확인하세요.")
    if failures:
        hints.append("가장 구체적인 error_code의 error_context부터 확인하세요.")
    return hints


def _missing_hints(missing: list[dict[str, Any]]) -> list[str]:
    if not missing:
        return [
            "구조화된 missing 신호를 찾지 못했습니다. 원본 행 부재와 row selection 제외 여부를 추가 확인하세요."
        ]
    kinds = {str(item.get("kind")) for item in missing}
    hints = ["missing_evidence의 document와 json_path를 기준으로 최초 발생 단계를 찾으세요."]
    if "route_or_deletion" in kinds:
        hints.append("0103 unmatched route 또는 삭제 입력 반영 여부를 확인하세요.")
    if "missing_dependency" in kinds:
        hints.append("0102 계산 의존 칼럼과 upstream schema 변경 시점을 확인하세요.")
    return hints


def _profile_hints(metrics: list[dict[str, Any]]) -> list[str]:
    if not metrics:
        return ["프로파일 metric을 찾지 못했습니다. 실행 metadata 또는 telemetry JSON 경로를 지정하세요."]
    return [
        "definition hash와 input generation이 같은 실행끼리 비교하세요.",
        "total elapsed와 phase elapsed, peak RSS를 함께 비교하세요.",
    ]


def _positive(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0
