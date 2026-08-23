from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from smoking_data.workspace_resources import workspace_resource


@dataclass(frozen=True)
class WorkspaceContribution:
    package: str
    resource_root: str
    schemas: dict[str, list[str]]
    snippet_name: str
    snippet_resource: str


ENGINE_CONTRIBUTION = WorkspaceContribution(
    package="smoking_data",
    resource_root="vscode",
    schemas={
        "asset-config-v3.schema.json": [
            "**/.smoking-data/config.yaml",
            "**/.smoking-data/assets/*/config.yaml",
        ],
        "pipeline-v6.schema.json": [
            "**/*.0301.yaml",
            "**/*.0301.yml",
            "**/*.0401.yaml",
            "**/*.0401.yml",
        ],
        "pipeline-v7.schema.json": ["**/*.0201.yaml", "**/*.0201.yml"],
        "calculated-fact-v2.schema.json": ["**/*.0102.yaml", "**/*.0102.yml"],
        "asset-chain-v2.schema.json": ["**/*.chain.yaml", "**/*.chain.yml"],
        "schedule-v1.schema.json": ["**/*.schedule.yaml", "**/*.schedule.yml"],
        "layout-migration-v1.schema.json": [
            "**/*.layout-migration.yaml",
            "**/*.layout-migration.yml",
        ],
        "physical-layout-recommendation-v2.schema.json": [
            "**/*.layout-recommendation.yaml",
            "**/*.layout-recommendation.yml",
        ],
    },
    snippet_name="smoking-data-engine.code-snippets",
    snippet_resource="engine.code-snippets",
)

STALE_MANAGED_SCHEMA_NAMES = frozenset(
    {
        "probe-v1.schema.json",
        "probe-v2.schema.json",
        "probe-v3.schema.json",
        "probe-v4.schema.json",
        "probe-v5.schema.json",
        "probe-v6.schema.json",
        "pipeline-v3.schema.json",
        "pipeline-v4.schema.json",
        "pipeline-v5.schema.json",
        "asset-chain-v1.schema.json",
        "asset-contract-v1.schema.json",
        "asset-config-v2.schema.json",
    }
)


def _source_contribution() -> WorkspaceContribution:
    from smoking_data.assets.a0101_source.workspace_contribution import workspace_contribution

    payload = workspace_contribution()
    return WorkspaceContribution(**payload)


def _csv_source_contribution() -> WorkspaceContribution:
    from smoking_data.assets.a0103_csv_source.workspace_contribution import (
        workspace_contribution,
    )

    payload = workspace_contribution()
    return WorkspaceContribution(**payload)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"VS Code JSON object expected: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _resource(contribution: WorkspaceContribution, relative: str):
    if contribution.package != "smoking_data":
        raise ValueError(f"unsupported workspace resource package: {contribution.package}")
    node = workspace_resource()
    for part in contribution.resource_root.split("/"):
        node = node.joinpath(part)
    for part in relative.split("/"):
        node = node.joinpath(part)
    return node


def _copy_resource(contribution: WorkspaceContribution, relative: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_bytes(_resource(contribution, relative).read_bytes())
    temporary.replace(target)


def _merge_extensions(vscode_dir: Path) -> None:
    path = vscode_dir / "extensions.json"
    payload = _read_json(path)
    recommendations = payload.get("recommendations", [])
    if not isinstance(recommendations, list):
        raise TypeError(f"recommendations must be an array: {path}")
    template = json.loads(
        _resource(ENGINE_CONTRIBUTION, "extensions.json").read_text(encoding="utf-8")
    )
    template_recommendations = template.get("recommendations", [])
    if not isinstance(template_recommendations, list):
        raise TypeError("workspace/vscode/extensions.json recommendations must be an array")
    payload["recommendations"] = list(
        dict.fromkeys([*recommendations, *template_recommendations])
    )
    _write_json(path, payload)


def _merge_settings(
    vscode_dir: Path,
    contributions: list[WorkspaceContribution],
) -> None:
    path = vscode_dir / "settings.json"
    payload = _read_json(path)
    template = json.loads(
        _resource(ENGINE_CONTRIBUTION, "settings.json").read_text(encoding="utf-8")
    )
    schemas = payload.setdefault("yaml.schemas", {})
    if not isinstance(schemas, dict):
        raise TypeError(f"yaml.schemas must be an object: {path}")
    managed_schema_names = {
        schema_name
        for contribution in contributions
        for schema_name in contribution.schemas
    }
    for schema_name in STALE_MANAGED_SCHEMA_NAMES | managed_schema_names:
        schemas.pop(f"./.vscode/schemas/{schema_name}", None)
    for contribution in contributions:
        for schema_name, patterns in contribution.schemas.items():
            schemas[f"./.vscode/schemas/{schema_name}"] = patterns
    for key, value in template.items():
        if key == "yaml.schemas":
            continue
        _setdefault_value(payload, key, value, source=path)
    _write_json(path, payload)


def _setdefault_value(
    payload: dict[str, Any],
    key: str,
    default: Any,
    *,
    source: Path,
) -> None:
    if key not in payload:
        payload[key] = default
        return
    current = payload[key]
    if isinstance(default, dict):
        if not isinstance(current, dict):
            raise TypeError(f"{key} must be an object: {source}")
        for nested_key, nested_default in default.items():
            _setdefault_value(
                current,
                nested_key,
                nested_default,
                source=source,
            )


def _merge_tasks(vscode_dir: Path) -> None:
    source = json.loads(_resource(ENGINE_CONTRIBUTION, "tasks.json").read_text(encoding="utf-8"))
    for task in source["tasks"]:
        task["command"] = "smoking-data"
        task["args"] = [arg for arg in task["args"] if arg not in ("-m", "smoking_data.cli")]

    path = vscode_dir / "tasks.json"
    payload = _read_json(path)
    payload.setdefault("version", "2.0.0")
    existing = payload.setdefault("tasks", [])
    if not isinstance(existing, list):
        raise TypeError(f"tasks must be an array: {path}")
    managed_labels = {task["label"] for task in source["tasks"]}
    payload["tasks"] = [
        task
        for task in existing
        if not isinstance(task, dict) or task.get("label") not in managed_labels
    ] + source["tasks"]
    _write_json(path, payload)


def initialize_workspace(target: str | Path) -> dict[str, Any]:
    workspace_root = Path(target).expanduser().resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    vscode_dir = workspace_root / ".vscode"
    vscode_dir.mkdir(parents=True, exist_ok=True)
    for schema_name in STALE_MANAGED_SCHEMA_NAMES:
        (vscode_dir / "schemas" / schema_name).unlink(missing_ok=True)

    contributions = [
        ENGINE_CONTRIBUTION,
        _source_contribution(),
        _csv_source_contribution(),
    ]
    _merge_extensions(vscode_dir)
    _merge_settings(vscode_dir, contributions)
    _merge_tasks(vscode_dir)

    written: list[str] = []
    for contribution in contributions:
        for schema_name in contribution.schemas:
            target_path = vscode_dir / "schemas" / schema_name
            _copy_resource(contribution, f"schemas/{schema_name}", target_path)
            written.append(str(target_path.relative_to(workspace_root)))
        snippet_path = vscode_dir / contribution.snippet_name
        _copy_resource(contribution, contribution.snippet_resource, snippet_path)
        written.append(str(snippet_path.relative_to(workspace_root)))

    publication_schema = vscode_dir / "schemas" / "publication-v1.schema.json"
    _copy_resource(
        ENGINE_CONTRIBUTION,
        "schemas/publication-v1.schema.json",
        publication_schema,
    )
    written.append(str(publication_schema.relative_to(workspace_root)))

    return {
        "ok": True,
        "workspace_root": str(workspace_root),
        "vscode_dir": str(vscode_dir),
        "engine": True,
        "source_0101": True,
        "source_0103": True,
        "written": written,
    }
