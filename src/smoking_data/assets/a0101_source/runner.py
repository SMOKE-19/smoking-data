from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from smoking_data.core.pipeline import SourceSpec as PipelineSourceSpec
from smoking_data.runtime.config import load_config
from smoking_data.runtime.object_store.config import PublicationSpec
from smoking_data.runtime.object_store.publication import publish_committed_dataset
from smoking_data.runtime.parquet_probe import ensure_source_probe
from smoking_data.runtime.paths import file_sha256

from .pipeline.log import get_source_logger, log_source_event
from .pipeline.models import SourceSpec
from .pipeline.orchestrator import execute_source_raw_stage


@dataclass(frozen=True, slots=True)
class SourcePhysicalProbeReport:
    status: str
    manifest_path: str | None = None
    dataset_fingerprint: str | None = None
    reused: bool = False
    error_type: str | None = None
    error_code: str | None = None
    error_message: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "success"

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": "physical_probe",
            "status": self.status,
            "ok": self.ok,
            "manifest_path": self.manifest_path,
            "dataset_fingerprint": self.dataset_fingerprint,
            "reused": self.reused,
            "error_type": self.error_type,
            "error_code": self.error_code,
            "error_message": self.error_message,
        }


@dataclass(frozen=True, slots=True)
class SourceRunResult:
    job_name: str
    status: str
    dataset_paths: tuple[str, ...]
    metadata_paths: tuple[str, ...]
    log_path: str
    task_count: int
    success_task_count: int
    error_task_count: int
    physical_probe: SourcePhysicalProbeReport | None = None
    test_run: dict[str, object] | None = None
    remote_publications: tuple[dict[str, object], ...] = ()

    @property
    def ok(self) -> bool:
        return self.error_task_count == 0

    def to_dict(self) -> dict[str, object]:
        return {
            "job_name": self.job_name,
            "status": self.status,
            "dataset_paths": list(self.dataset_paths),
            "metadata_paths": list(self.metadata_paths),
            "log_path": self.log_path,
            "task_count": self.task_count,
            "success_task_count": self.success_task_count,
            "error_task_count": self.error_task_count,
            "physical_probe": self.physical_probe.to_dict() if self.physical_probe else None,
            "test_run": self.test_run,
            "remote_publications": list(self.remote_publications),
        }


def execute_yaml(
    yaml_path: str | Path,
    *,
    project_root: str | Path | None = None,
) -> SourceRunResult:
    result = execute_source_raw_stage(yaml_path)
    test_run = getattr(
        result.plan,
        "test_run",
        {
            "enabled": False,
            "final_task_limit": None,
            "sidecar_scope": "global",
            "global_planned_tasks": len(result.responses),
            "selected_tasks": len(result.responses),
            "selected_task_ids": [],
            "forces_execution": False,
            "output_scope": "complete_dataset",
        },
    )
    successful_paths = tuple(
        str(path_set.raw_json_path)
        for path_set, response in zip(result.path_sets, result.responses, strict=True)
        if response.status == "success"
    )
    success_task_count = sum(response.status == "success" for response in result.responses)
    error_task_count = len(result.responses) - success_task_count
    physical_probe = None
    if error_task_count == 0:
        physical_probe = _ensure_source_physical_probe(
            result.plan.spec,
            project_root=(
                Path(project_root).resolve()
                if project_root is not None
                else result.plan.spec.project.project_root
            ),
            log_path=result.log_path,
        )
    status = "success"
    if error_task_count:
        status = "partial_failure" if success_task_count else "error"
    elif physical_probe is not None and not physical_probe.ok:
        status = "success_with_probe_error"
    if test_run["enabled"] and not error_task_count:
        status = (
            "test_success_with_probe_error"
            if physical_probe is not None and not physical_probe.ok
            else "test_success"
        )
    publication = PublicationSpec.from_mapping(
        ((result.plan.spec.resolved.get("output") or {}).get("artifact") or {}).get(
            "publication"
        )
    )
    remote_publications: list[dict[str, object]] = []
    if publication is not None:
        publication_root = (
            Path(project_root).resolve()
            if project_root is not None
            else result.plan.spec.project.project_root
        )
        for dataset_path in successful_paths:
            dataset = Path(dataset_path)
            scoped = replace(
                publication,
                dataset_prefix=f"{publication.dataset_prefix}/{dataset.name}",
            )
            published = publish_committed_dataset(
                dataset,
                project_root=publication_root,
                publication=scoped,
                asset_code="0101",
                job_name=result.plan.spec.job.name,
                definition_sha256=file_sha256(result.plan.spec.path),
            )
            if published is not None:
                remote_publications.append(
                    {
                        "status": published.status,
                        "target": published.target,
                        "dataset_uri": published.dataset_uri,
                        "generation_id": published.generation_id,
                        "manifest_key": published.manifest_key,
                        "receipt_path": str(published.receipt_path),
                    }
                )
    return SourceRunResult(
        job_name=result.plan.spec.job.name,
        status=status,
        dataset_paths=successful_paths,
        metadata_paths=tuple(str(path) for path in result.metadata_paths),
        log_path=str(result.log_path),
        task_count=len(result.responses),
        success_task_count=success_task_count,
        error_task_count=error_task_count,
        physical_probe=physical_probe,
        test_run=test_run,
        remote_publications=tuple(remote_publications),
    )


def _ensure_source_physical_probe(
    spec: SourceSpec,
    *,
    project_root: Path,
    log_path: Path,
) -> SourcePhysicalProbeReport:
    logger = get_source_logger(log_path=log_path, job_name=spec.job.name)
    source = PipelineSourceSpec(
        name=spec.job.name,
        kind="parquet_dataset",
        paths=(str(Path(spec.storage.raw_dir).resolve()),),
        union_by_name=True,
        missing_columns="insert_null",
        incompatible_dtypes="error",
        asset_definition=str(spec.path),
        asset_code="0101",
    )
    try:
        handle = ensure_source_probe(
            source_name=spec.job.name,
            source=source,
            config=load_config(project_root=project_root, asset_code="0101"),
        )
        report = SourcePhysicalProbeReport(
            status="success",
            manifest_path=str(handle.manifest_path),
            dataset_fingerprint=handle.dataset_fingerprint,
            reused=handle.reused,
        )
    except Exception as exc:  # noqa: BLE001 - published source remains valid and retryable.
        report = SourcePhysicalProbeReport(
            status="failed",
            error_type=type(exc).__name__,
            error_code=(str(exc.code) if getattr(exc, "code", None) else None),
            error_message=str(exc),
        )
    log_source_event(
        logger,
        stage="physical_probe",
        status="success" if report.ok else "error",
        message=(
            "0101 physical probe completed"
            if report.ok
            else f"0101 physical probe failed: {report.error_message}"
        ),
        job_name=spec.job.name,
        error_code=report.error_code,
    )
    return report
