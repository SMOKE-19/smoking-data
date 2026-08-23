from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import yaml

from smoking_data.core.exceptions import ValidationError
from smoking_data.runtime.asset_config import deep_merge, load_effective_asset_config
from smoking_data.runtime.asset_contract import load_effective_asset_contract
from smoking_data.runtime.config import load_config
from smoking_data.runtime.operation_registry import registry_path
from smoking_data.runtime.paths import resolve_project_path
from smoking_data.runtime.template_resolution import resolve_contract_templates

RECOMMENDATION_SCHEMA_VERSION = "smoking-data.physical-layout-recommendation.v2"
SUPPORTED_EDGES = {("0101", "0201"), ("0201", "0301"), ("0301", "0401")}
TARGET_ROW_GROUP_BYTES = 8 * 1024 * 1024
MIN_ROW_GROUP_ROWS = 1_000
MAX_ROW_GROUP_ROWS = 131_072


@dataclass(frozen=True, slots=True)
class PhysicalLayoutReport:
    path: Path
    upstream_asset_code: str
    downstream_asset_code: str
    recommendation: dict[str, Any]
    evidence_count: int
    confidence: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "schema_version": RECOMMENDATION_SCHEMA_VERSION,
            "upstream_asset_code": self.upstream_asset_code,
            "downstream_asset_code": self.downstream_asset_code,
            "recommendation": self.recommendation,
            "evidence_count": self.evidence_count,
            "confidence": self.confidence,
        }


def generate_physical_layout_report(
    upstream_yaml: str | Path,
    downstream_yaml: str | Path,
    *,
    history_paths: list[str | Path] | tuple[str | Path, ...] = (),
    output_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> PhysicalLayoutReport:
    """Emit an evidence-based YAML recommendation without modifying either contract."""

    root = Path(project_root or Path.cwd()).expanduser().resolve()
    upstream_path = _resolve(upstream_yaml, root)
    downstream_path = _resolve(downstream_yaml, root)
    upstream = _read_yaml(upstream_path)
    downstream = _read_yaml(downstream_path)
    upstream_code = _asset_code(upstream, upstream_path)
    downstream_code = _asset_code(downstream, downstream_path)
    if (upstream_code, downstream_code) not in SUPPORTED_EDGES:
        raise ValidationError(
            f"Unsupported physical-layout edge: {upstream_code} -> {downstream_code}",
            code="physical_layout.unsupported_edge",
            context={"supported_edges": sorted(f"{a}->{b}" for a, b in SUPPORTED_EDGES)},
        )

    downstream_job = str((downstream.get("job") or {}).get("name") or "")
    discovered_paths = _discover_downstream_history(
        downstream, downstream_path, downstream_code, root
    )
    all_history_paths = [*discovered_paths, *history_paths]
    records = _load_history(all_history_paths, root=root)
    records.extend(_load_registry_history(root, job_name=downstream_job))
    observations = [item for raw in records if (item := _observation(raw)) is not None]
    experiment = _best_layout_experiment(records, upstream_code=upstream_code)
    current = _current_settings(upstream_code, upstream)
    recommendation, rationale = _recommend(
        upstream_code, current, observations, experiment=experiment
    )
    confidence = _confidence(observations, experiment=experiment)

    if output_path is None:
        up_job = str((upstream.get("job") or {}).get("name") or upstream_code)
        down_job = downstream_job or downstream_code
        output = root / ".smoking-data" / "reports" / "physical-layout"
        output = output / f"{up_job}__for__{down_job}.layout-recommendation.yaml"
    else:
        output = _resolve(output_path, root)
        if output.suffix.lower() not in {".yaml", ".yml"}:
            raise ValidationError(
                "Physical-layout recommendation output must be a YAML file.",
                code="physical_layout.invalid_output_path",
                context={"output_path": str(output)},
            )
    output.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    recommendation_document = _recommendation_document(
        generated_at=generated_at,
        upstream_path=upstream_path,
        downstream_path=downstream_path,
        upstream=upstream,
        downstream=downstream,
        upstream_code=upstream_code,
        downstream_code=downstream_code,
        current=current,
        recommendation=recommendation,
        rationale=rationale,
        confidence=confidence,
        evidence_count=len(observations),
        experiment=experiment,
    )
    recommendation_document["history_sources"] = [
        str(_resolve(value, root)) for value in all_history_paths
    ]
    recommendation_document["yaml_patch"] = (
        {"output": {"artifact": {"parquet_writer": recommendation}}}
        if upstream_code == "0101"
        else {
            "output": {"artifact": {"compression": recommendation.get("compression")}},
            "execution": {
                key: value for key, value in recommendation.items() if key != "compression"
            },
        }
    )
    output.write_text(
        yaml.safe_dump(recommendation_document, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return PhysicalLayoutReport(
        output,
        upstream_code,
        downstream_code,
        recommendation,
        len(observations),
        confidence,
    )


def _recommendation_document(
    *,
    generated_at: str,
    upstream_path: Path,
    downstream_path: Path,
    upstream: dict[str, Any],
    downstream: dict[str, Any],
    upstream_code: str,
    downstream_code: str,
    current: dict[str, Any],
    recommendation: dict[str, Any],
    rationale: dict[str, str],
    confidence: str,
    evidence_count: int,
    experiment: dict[str, Any] | None,
) -> dict[str, Any]:
    source = (
        "candidate_experiment"
        if experiment is not None
        else "current_value_retained"
        if recommendation == current
        else "telemetry_heuristic"
    )
    canonical = {
        "yaml": {
            "schema_version": RECOMMENDATION_SCHEMA_VERSION,
            "asset_code": upstream_code,
        },
        "generated_at": generated_at,
        "edge": {"upstream": upstream_code, "downstream": downstream_code},
        "upstream": {
            "yaml_path": str(upstream_path),
            "yaml_sha256": _sha256(upstream_path),
            "job_name": str((upstream.get("job") or {}).get("name") or ""),
        },
        "downstream": {
            "yaml_path": str(downstream_path),
            "yaml_sha256": _sha256(downstream_path),
            "job_name": str((downstream.get("job") or {}).get("name") or ""),
        },
        "current": current,
        "recommendation": recommendation,
        "rationale": rationale,
        "confidence": confidence,
        "evidence_count": evidence_count,
        "recommendation_source": source,
        "candidate_experiment": experiment,
        "fixed_constraints": {
            "file_count": "preserve",
            "relative_paths": "preserve",
            "file_row_counts": "preserve",
            "logical_schema": "preserve",
            "logical_values": "preserve",
        },
    }
    hash_basis = {key: value for key, value in canonical.items() if key != "generated_at"}
    canonical_json = json.dumps(
        hash_basis, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    canonical_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return {
        **canonical,
        "recommendation_id": f"layout-{canonical_hash[:16]}",
        "canonical_hash": canonical_hash,
    }


def _recommend(
    asset_code: str,
    current: dict[str, Any],
    observations: list[dict[str, float | int | str | None]],
    *,
    experiment: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    recommendation = dict(current)
    rationale: dict[str, str] = {}
    if not observations and experiment is None:
        return recommendation, {key: "실행 증거 부족: 현재값 유지" for key in current}

    bytes_per_row = [
        float(item["projected_bytes_per_row"])
        for item in observations
        if item.get("projected_bytes_per_row")
    ]
    if not bytes_per_row:
        bytes_per_row = [
            float(item["wide_row_bytes"])
            for item in observations
            if item.get("wide_row_bytes")
        ]
    target_rows = None
    if bytes_per_row:
        raw = TARGET_ROW_GROUP_BYTES / max(median(bytes_per_row), 1.0)
        target_rows = _power_of_two_bound(raw, MIN_ROW_GROUP_ROWS, MAX_ROW_GROUP_ROWS)

    key = "row_group_size" if asset_code == "0101" else "row_group_rows"
    if experiment is not None:
        target_rows = int(experiment["effective_row_group_rows"])
        recommendation[key] = target_rows
        rationale[key] = (
            "논리 parity가 통과한 교차 실행에서 median time 최저 후보의 실효 row group 크기"
        )
    elif target_rows is not None:
        recommendation[key] = target_rows
        rationale[key] = "실제 bytes/row 중앙값으로 약 8 MiB row group을 산정"
    else:
        rationale[key] = "행 폭 증거 부족: 현재값 유지"

    if asset_code == "0101":
        recommendation["write_page_index"] = True
        recommendation["write_statistics"] = True
        rationale["write_page_index"] = "0201 좌표 기반 범위 읽기를 지원"
        rationale["write_statistics"] = "row group pruning과 sidecar 구축을 지원"
        if target_rows is not None:
            recommendation["max_rows_per_page"] = min(target_rows, 16_384)
            rationale["max_rows_per_page"] = "선택 읽기 세분성을 유지하도록 제한"
        for name in ("compression", "data_page_size", "use_dictionary"):
            if name in current:
                rationale[name] = "비교 후보 실행이 없어 현재 계약값 유지"
    else:
        for name in ("compression", "max_source_files_per_task", "max_source_row_groups_per_task"):
            if name in current:
                rationale[name] = "현재 관측만으로 인과 분리가 불가능해 현재값 유지"
    return recommendation, rationale


def _best_layout_experiment(
    records: list[dict[str, Any]], *, upstream_code: str
) -> dict[str, Any] | None:
    if upstream_code != "0101":
        return None
    candidates = []
    experiment_controls: dict[str, Any] = {}
    for document in records:
        if document.get("schema_version") != "smoking-data.profile.du-gen-physical-layout.v1":
            continue
        parity = document.get("parity") or {}
        controls = document.get("controls") or {}
        summaries = document.get("summaries") or {}
        variants = document.get("variants") or {}
        if not parity.get("all_ok") or not isinstance(summaries, dict):
            continue
        experiment_controls = {
            "repetitions": controls.get("repetitions"),
            "run_order": controls.get("run_order"),
        }
        for name, summary in summaries.items():
            if not isinstance(summary, dict):
                continue
            variant = variants.get(name) or {}
            profile = variant.get("input_profile") or {}
            requested = int(summary.get("row_group_rows_requested") or 0)
            file_rows_max = int(profile.get("file_rows_max") or 0)
            elapsed = _number(summary.get("median_elapsed_sec"))
            if requested > 0 and file_rows_max > 0 and elapsed is not None:
                candidates.append(
                    {
                        "name": name,
                        "requested_row_group_rows": requested,
                        "effective_row_group_rows": min(requested, file_rows_max),
                        "file_rows_max": file_rows_max,
                        "median_elapsed_sec": elapsed,
                        "median_peak_tree_rss_mb": _number(
                            summary.get("median_peak_tree_rss_mb")
                        ),
                        "median_cpu_sec": _number(summary.get("median_cpu_sec")),
                        "input_row_groups": int(summary.get("input_row_groups") or 0),
                    }
                )
    if len(candidates) < 2:
        return None
    winner = min(candidates, key=lambda item: item["median_elapsed_sec"])
    return {
        "winner": winner,
        "candidates": candidates,
        "logical_parity": {"all_ok": True},
        "selection_metric": "median_elapsed_sec",
        "execution_controls": experiment_controls,
        **winner,
    }


def _observation(document: dict[str, Any]) -> dict[str, float | int | str | None] | None:
    result = document.get("result") if isinstance(document.get("result"), dict) else document
    details = result.get("details") if isinstance(result.get("details"), dict) else {}
    counters = result.get("counters") if isinstance(result.get("counters"), dict) else {}
    if not details and isinstance(document.get("metrics"), dict):
        details = document["metrics"]
    actuals = details.get("physical_plan_actuals") or {}
    tasks = actuals.get("tasks") if isinstance(actuals, dict) else []
    projected_rows = sum(float(item.get("actual_selected_rows") or 0) for item in tasks or [])
    projected_bytes = sum(
        float(item.get("actual_projected_array_bytes") or 0) for item in tasks or []
    )
    reconciliation = details.get("pivot_sizing_reconciliation") or {}
    actual = reconciliation.get("actual") if isinstance(reconciliation, dict) else {}
    telemetry = details.get("task_telemetry") or {}
    worker_profiles = telemetry.get("worker_profiles") or []
    aggregate = document.get("aggregate_process_deltas") or {}
    task_memory = details.get("task_memory") or {}
    phase = details.get("phase_profile") or {}
    output_rows = int(counters.get("output_rows") or document.get("output_rows") or 0)
    peak = (
        task_memory.get("max_peak_rss_mb")
        or telemetry.get("peak_tree_rss_mb")
        or document.get("peak_tree_rss_mb")
        or document.get("peak_rss_mb")
    )
    elapsed = (
        phase.get("total_elapsed_sec")
        or telemetry.get("elapsed_sec")
        or document.get("elapsed_sec")
    )
    # A scenario receipt containing only wall time is not a telemetry or physical-layout
    # observation. Excluding it also prevents duplicate run summaries from skewing medians.
    if not any((tasks, actual, peak, output_rows)):
        return None
    return {
        "platform": telemetry.get("platform") or document.get("platform") or "unknown",
        "tasks": int(counters.get("tasks") or len(tasks or [])),
        "output_rows": output_rows,
        "peak_rss_mb": _number(peak),
        "elapsed_sec": _number(elapsed),
        "cpu_sec": _number(
            aggregate.get("cpu_sec")
            or sum(float(item.get("cpu_sec") or 0) for item in worker_profiles)
        ),
        "read_bytes": _number(
            aggregate.get("read_bytes")
            or sum(
                float(item.get("requested_read_bytes") or item.get("read_bytes") or 0)
                for item in worker_profiles
            )
        ),
        "write_bytes": _number(
            aggregate.get("write_bytes")
            or sum(
                float(item.get("requested_write_bytes") or item.get("write_bytes") or 0)
                for item in worker_profiles
            )
        ),
        "wide_row_bytes": _number((actual or {}).get("uncompressed_wide_row_bytes")),
        "compression_ratio": _number((actual or {}).get("compression_ratio")),
        "projected_bytes_per_row": projected_bytes / projected_rows if projected_rows else None,
        "source_files": int(counters.get("coordinate_source_files") or 0),
        "source_row_groups": int(counters.get("coordinate_row_groups") or 0),
    }


def _load_history(
    paths: list[str | Path] | tuple[str | Path, ...], *, root: Path
) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for value in paths:
        path = _resolve(value, root)
        candidates = sorted(path.rglob("*.json")) if path.is_dir() else [path]
        for candidate in candidates:
            candidate = candidate.resolve()
            if candidate in seen:
                continue
            seen.add(candidate)
            try:
                payload = json.loads(candidate.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                payload = _hydrate_metadata_details(payload, candidate)
                documents.append(payload)
                for key in ("runs", "observations", "results"):
                    if isinstance(payload.get(key), list):
                        documents.extend(item for item in payload[key] if isinstance(item, dict))
    return documents


def _discover_downstream_history(
    raw: dict[str, Any], yaml_path: Path, asset_code: str, root: Path
) -> list[Path]:
    """Resolve only the downstream dataset provenance and its job-scoped logs."""

    config = load_config(project_root=root, asset_code=asset_code)
    defaults = deep_merge(
        load_effective_asset_contract(root, asset_code).definition,
        load_effective_asset_config(root, asset_code).payload,
    )
    effective = deep_merge(defaults, raw)
    job_name = str((effective.get("job") or {}).get("name") or "")
    output = effective.get("output") or {}
    resolved_output = resolve_contract_templates(
        output,
        scope={
            **config.template_scope(),
            "asset_code": asset_code,
            "job_name": job_name,
        },
        source=yaml_path,
    )
    paths: list[Path] = []
    artifact = resolved_output.get("artifact") or {}
    root_value = artifact.get("root_dir") if isinstance(artifact, dict) else None
    if root_value:
        provenance = (
            resolve_project_path(str(root_value), project_root=root) / "_smoking_data"
        )
        if provenance.is_dir():
            paths.extend(sorted(provenance.glob("*.json")))

    logging = resolved_output.get("logging") or {}
    log_value = logging.get("root_dir") if isinstance(logging, dict) else None
    if log_value and job_name:
        log_root = resolve_project_path(str(log_value), project_root=root)
        if log_root.is_dir():
            paths.extend(sorted(log_root.rglob(f"*{job_name}*.json")))
    return list(dict.fromkeys(path.resolve() for path in paths))


def _hydrate_metadata_details(payload: dict[str, Any], metadata_path: Path) -> dict[str, Any]:
    details_value = payload.get("details_artifact_path")
    if not details_value:
        return payload
    details_path = Path(str(details_value)).expanduser()
    if not details_path.is_absolute():
        details_path = metadata_path.parent / details_path
    try:
        external = json.loads(details_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return payload
    if not isinstance(external, dict):
        return payload
    result = dict(payload.get("result") or {})
    result["details"] = {**dict(result.get("details") or {}), **external}
    return {**payload, "result": result}


def _load_registry_history(root: Path, *, job_name: str) -> list[dict[str, Any]]:
    path = registry_path(root)
    if not path.exists():
        return []
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        rows = connection.execute(
            "SELECT metrics_json FROM adaptive_sizing_observations WHERE job_name = ?",
            (job_name,),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        if connection is not None:
            connection.close()
    result = []
    for (raw,) in rows:
        try:
            result.append({"metrics": json.loads(raw)})
        except (TypeError, json.JSONDecodeError):
            continue
    return result


def _current_settings(asset_code: str, raw: dict[str, Any]) -> dict[str, Any]:
    if asset_code == "0101":
        writer = (((raw.get("output") or {}).get("artifact") or {}).get("parquet_writer") or {})
        return {
            key: writer.get(key)
            for key in (
                "compression", "row_group_size", "write_page_index", "write_statistics",
                "data_page_size", "max_rows_per_page", "use_dictionary",
            )
            if key in writer
        }
    artifact = (raw.get("output") or {}).get("artifact") or {}
    physical_layout = artifact.get("physical_layout") or {}
    return {
        "compression": artifact.get("compression"),
        "row_group_rows": physical_layout.get("row_group_rows", "auto"),
        "physical_layout_profile": physical_layout.get("profile"),
        "physical_layout_adaptation_scope": physical_layout.get("adaptation_scope"),
        "max_source_files_per_task": (raw.get("execution") or {}).get(
            "max_source_files_per_task"
        ),
        "max_source_row_groups_per_task": (raw.get("execution") or {}).get(
            "max_source_row_groups_per_task"
        ),
    }


def _confidence(
    observations: list[dict[str, Any]], *, experiment: dict[str, Any] | None = None
) -> str:
    if experiment is not None:
        return "medium"
    complete = sum(
        1 for item in observations
        if item.get("peak_rss_mb") and item.get("elapsed_sec") and item.get("projected_bytes_per_row")
    )
    platforms = {item.get("platform") for item in observations if item.get("platform") != "unknown"}
    if complete >= 4 and len(platforms) >= 2:
        return "high"
    if complete >= 2:
        return "medium"
    return "low"


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ValidationError(
            f"Cannot read YAML: {path}: {exc}", code="physical_layout.invalid_yaml"
        ) from exc
    if not isinstance(raw, dict):
        raise ValidationError("YAML root must be a mapping.", code="physical_layout.invalid_yaml")
    return raw


def _asset_code(raw: dict[str, Any], path: Path) -> str:
    code = str((raw.get("yaml") or {}).get("asset_code") or "")
    if code not in {"0101", "0201", "0301", "0401"}:
        raise ValidationError(
            f"Unsupported or missing yaml.asset_code in {path}",
            code="physical_layout.invalid_asset_code",
        )
    return code


def _resolve(value: str | Path, root: Path) -> Path:
    path = Path(value).expanduser()
    return (root / path).resolve() if not path.is_absolute() else path.resolve()


def _power_of_two_bound(value: float, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, 2 ** round(math.log2(max(value, 1.0)))))


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
