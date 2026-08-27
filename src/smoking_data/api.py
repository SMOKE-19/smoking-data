from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from smoking_data.core.exceptions import SmokingDataError
from smoking_data.core.pipeline import validate_pipeline_document
from smoking_data.runtime.asset_chain import load_asset_chain
from smoking_data.runtime.asset_config import asset_code_from_definition_path
from smoking_data.runtime.config import load_config
from smoking_data.runtime.object_store.config import PublicationSpec
from smoking_data.runtime.yaml_loader import load_pipeline_spec

VALIDATION_API_SCHEMA_VERSION = "smoking-data.validation-result.v1"


@dataclass(frozen=True, slots=True)
class ValidationResult:
    ok: bool
    definition_path: Path
    kind: str | None = None
    schema_version: str | None = None
    asset_code: str | None = None
    job_name: str | None = None
    yaml_sha256: str | None = None
    graph_sha256: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    error_message: str | None = None
    error_context: dict[str, Any] | None = None
    result_schema_version: str = VALIDATION_API_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "result_schema_version": self.result_schema_version,
            "definition_path": str(self.definition_path),
            "kind": self.kind,
            "schema_version": self.schema_version,
            "asset_code": self.asset_code,
            "job_name": self.job_name,
            "yaml_sha256": self.yaml_sha256,
            "graph_sha256": self.graph_sha256,
            "details": self.details,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "error_context": self.error_context,
        }


def validate_definition(
    definition_path: str | Path,
    *,
    config_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> ValidationResult:
    """Validate a Definition without running it or writing registry state."""

    path = Path(definition_path).expanduser().resolve()
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise ValueError("Definition YAML root must be an object.")
        yaml_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        header = raw.get("yaml") if isinstance(raw.get("yaml"), dict) else {}
        schema_version = str(header.get("schema_version") or "") or None
        asset_code = asset_code_from_definition_path(path)

        if schema_version == "smoking-data.publication.v1":
            publication = PublicationSpec.from_mapping(raw.get("publication"))
            if publication is None:
                raise ValueError("publication is required for publication.v1 YAML.")
            return ValidationResult(
                ok=True,
                definition_path=path,
                kind="publication",
                schema_version=schema_version,
                job_name=str((raw.get("job") or {}).get("name") or ""),
                yaml_sha256=yaml_sha256,
                details={"target": publication.target, "dataset_prefix": publication.dataset_prefix},
            )

        if schema_version == "smoking-data.asset-chain.v2" or path.name.endswith(
            (".chain.yaml", ".chain.yml")
        ):
            config = load_config(
                config_path=config_path,
                project_root=project_root,
                asset_code=asset_code,
            )
            spec = load_asset_chain(path, config=config)
            return ValidationResult(
                ok=True,
                definition_path=path,
                kind="chain",
                schema_version=spec.schema_version,
                job_name=spec.name,
                yaml_sha256=yaml_sha256,
                graph_sha256=spec.graph_hash,
                details={"topological_order": list(spec.topological_order)},
            )

        config = load_config(
            config_path=config_path,
            project_root=project_root,
            asset_code=asset_code,
        )
        if asset_code == "0102":
            from smoking_data.assets.a0102_calculated_fact.spec import load_calculated_fact_spec

            spec = load_calculated_fact_spec(path)
            return ValidationResult(
                ok=True,
                definition_path=path,
                kind="asset",
                schema_version="smoking-data.calculated-fact.v2",
                asset_code=asset_code,
                job_name=spec.job_name,
                yaml_sha256=yaml_sha256,
                details={"canonical_hash": spec.canonical_hash},
            )
        if asset_code == "0101":
            from smoking_data.assets.a0101_source.pipeline.spec import load_source_spec

            spec = load_source_spec(path)
            return ValidationResult(
                ok=True,
                definition_path=path,
                kind="asset",
                schema_version=spec.schema_version,
                asset_code=asset_code,
                job_name=spec.job.name,
                yaml_sha256=yaml_sha256,
            )
        if asset_code == "0103":
            from smoking_data.assets.a0103_csv_source import load_csv_source_spec

            spec = load_csv_source_spec(path, project_root=project_root)
            return ValidationResult(
                ok=True,
                definition_path=path,
                kind="asset",
                schema_version="smoking-data.csv-source.v1",
                asset_code=asset_code,
                job_name=spec.job_name,
                yaml_sha256=yaml_sha256,
                details={"routes": [str(item["route_name"]) for item in spec.routes]},
            )

        spec = load_pipeline_spec(path, config=config)
        validate_pipeline_document(raw)
        return ValidationResult(
            ok=True,
            definition_path=path,
            kind="asset",
            schema_version=spec.schema_version,
            asset_code=spec.asset_code,
            job_name=spec.job_name,
            yaml_sha256=spec.yaml_hash,
            graph_sha256=spec.graph_hash,
            details={"topological_order": list(spec.graph["topological_order"])},
        )
    except SmokingDataError as exc:
        return ValidationResult(
            ok=False,
            definition_path=path,
            error_code=exc.code,
            error_message=str(exc),
            error_context=exc.context,
        )
    except Exception as exc:  # noqa: BLE001 - public API returns structured failures.
        return ValidationResult(
            ok=False,
            definition_path=path,
            error_code="validation.failed",
            error_message=str(exc),
            error_context={"definition_path": str(path)},
        )
