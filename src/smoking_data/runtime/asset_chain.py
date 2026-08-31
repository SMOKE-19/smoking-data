from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from smoking_data.core.exceptions import ValidationError
from smoking_data.core.results import to_json_safe, utc_now_iso
from smoking_data.runtime.asset_config import asset_config_fingerprint
from smoking_data.runtime.asset_contract import asset_contract_fingerprint
from smoking_data.runtime.config import RuntimeConfig, load_config
from smoking_data.runtime.events import append_stage_event
from smoking_data.runtime.paths import (
    ensure_dir,
    file_sha256,
    infer_project_root,
    resolve_project_path,
)

ASSET_CHAIN_SCHEMA_VERSION = "smoking-data.asset-chain.v2"
ASSET_CHAIN_RESULT_SCHEMA_VERSION = "smoking-data.asset-chain-result.v1"
_ASSET_CODE_PATTERN = re.compile(r"^[0-9]{4}$")
_SUPPORTED_ASSET_CODES = frozenset({"0101", "0102", "0103", "0201", "0301", "0401"})


@dataclass(frozen=True, slots=True)
class AssetNode:
    asset_id: str
    asset_code: str
    definition_path: Path
    inputs: dict[str, str]


@dataclass(frozen=True, slots=True)
class AssetChainSpec:
    name: str
    yaml_path: Path
    yaml_hash: str
    assets: tuple[AssetNode, ...]
    topological_order: tuple[str, ...]
    graph_hash: str
    failure_policy: str
    unchanged_policy: str
    max_parallel_assets: int

    @property
    def assets_by_id(self) -> dict[str, AssetNode]:
        return {asset.asset_id: asset for asset in self.assets}


@dataclass(slots=True)
class AssetExecutionResult:
    asset_id: str
    asset_code: str
    definition_path: Path
    status: str
    ok: bool
    input_fingerprint: str
    receipt_fingerprint: str | None = None
    output_paths: list[Path] = field(default_factory=list)
    metadata_path: Path | None = None
    details: dict[str, Any] = field(default_factory=dict)
    error_type: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return to_json_safe(asdict(self))


@dataclass(slots=True)
class AssetChainResult:
    ok: bool
    chain_name: str
    yaml_path: Path
    yaml_hash: str
    graph_hash: str
    topological_order: list[str]
    assets: list[AssetExecutionResult]
    metadata_path: Path | None = None
    log_path: Path | None = None
    started_at: str = field(default_factory=utc_now_iso)
    finished_at: str | None = None
    schema_version: str = ASSET_CHAIN_RESULT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return to_json_safe(asdict(self))


def load_asset_chain(
    yaml_path: str | Path,
    *,
    config: RuntimeConfig,
) -> AssetChainSpec:
    path = resolve_project_path(yaml_path, project_root=config.project_root)
    if not path.exists():
        raise ValidationError(
            f"Asset chain YAML file does not exist: {path}",
            code="asset_chain.definition_missing",
            context={"yaml_path": str(path)},
        )
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValidationError("Asset chain YAML root must be a mapping.", code="yaml.invalid_type")
    _reject_unknown(payload, {"yaml", "chain", "assets", "execution"}, path="$")
    yaml_header = _mapping(payload.get("yaml"), path="yaml")
    _reject_unknown(yaml_header, {"schema_version"}, path="yaml")
    if yaml_header.get("schema_version") != ASSET_CHAIN_SCHEMA_VERSION:
        raise ValidationError(
            f"Asset chain YAML must define yaml.schema_version: {ASSET_CHAIN_SCHEMA_VERSION}",
            code="asset_chain.invalid_schema_version",
        )

    chain = _mapping(payload.get("chain"), path="chain")
    _reject_unknown(chain, {"name"}, path="chain")
    name = _required_string(chain.get("name"), path="chain.name")
    raw_assets = payload.get("assets")
    if not isinstance(raw_assets, list) or not raw_assets:
        raise ValidationError(
            "assets must be a non-empty list.",
            code="yaml.invalid_type",
            context={"path": "assets"},
        )

    nodes: list[AssetNode] = []
    seen: set[str] = set()
    for index, raw_asset in enumerate(raw_assets):
        item_path = f"assets[{index}]"
        asset = _mapping(raw_asset, path=item_path)
        _reject_unknown(asset, {"id", "asset_code", "definition", "inputs"}, path=item_path)
        asset_id = _required_string(asset.get("id"), path=f"{item_path}.id")
        if asset_id in seen:
            raise ValidationError(
                f"Duplicate asset id: {asset_id}",
                code="asset_chain.duplicate_asset_id",
                context={"asset_id": asset_id},
            )
        seen.add(asset_id)
        asset_code = _required_string(asset.get("asset_code"), path=f"{item_path}.asset_code")
        if not _ASSET_CODE_PATTERN.fullmatch(asset_code) or asset_code not in _SUPPORTED_ASSET_CODES:
            raise ValidationError(
                f"Unsupported asset code: {asset_code}",
                code="asset_chain.unsupported_asset_code",
                context={"asset_id": asset_id, "asset_code": asset_code},
            )
        definition_value = _required_string(asset.get("definition"), path=f"{item_path}.definition")
        definition_path = Path(definition_value).expanduser()
        if not definition_path.is_absolute():
            definition_path = path.parent / definition_path
        definition_path = definition_path.resolve()
        if not definition_path.is_file():
            raise ValidationError(
                f"Asset definition does not exist: {definition_path}",
                code="asset_chain.definition_missing",
                context={"asset_id": asset_id, "definition": str(definition_path)},
            )
        inputs = _mapping(asset.get("inputs") or {}, path=f"{item_path}.inputs")
        normalized_inputs = {
            _required_string(port, path=f"{item_path}.inputs.<port>"): _required_string(
                upstream, path=f"{item_path}.inputs.{port}"
            )
            for port, upstream in inputs.items()
        }
        _validate_definition_schema(definition_path, asset_code=asset_code, asset_id=asset_id)
        nodes.append(
            AssetNode(
                asset_id=asset_id,
                asset_code=asset_code,
                definition_path=definition_path,
                inputs=normalized_inputs,
            )
        )

    by_id = {node.asset_id: node for node in nodes}
    dependencies: dict[str, set[str]] = {node.asset_id: set(node.inputs.values()) for node in nodes}
    for node in nodes:
        for port, upstream in node.inputs.items():
            if upstream == node.asset_id:
                raise ValidationError(
                    "Asset cannot depend on itself.",
                    code="asset_chain.self_reference",
                    context={"asset_id": node.asset_id, "port": port},
                )
            if upstream not in by_id:
                raise ValidationError(
                    f"Unknown upstream asset: {upstream}",
                    code="asset_chain.unknown_upstream",
                    context={"asset_id": node.asset_id, "port": port, "upstream": upstream},
                )
    order = _topological_order(nodes, dependencies)

    execution = _mapping(payload.get("execution") or {}, path="execution")
    _reject_unknown(
        execution,
        {"failure_policy", "unchanged_policy", "max_parallel_assets"},
        path="execution",
    )
    failure_policy = str(execution.get("failure_policy") or "stop_downstream")
    if failure_policy != "stop_downstream":
        raise ValidationError(
            "failure_policy currently supports only stop_downstream.",
            code="asset_chain.unsupported_failure_policy",
        )
    unchanged_policy = str(execution.get("unchanged_policy") or "skip")
    if unchanged_policy not in {"skip", "run"}:
        raise ValidationError(
            "unchanged_policy must be skip or run.",
            code="asset_chain.unsupported_unchanged_policy",
        )
    max_parallel_assets = int(execution.get("max_parallel_assets", 1) or 1)
    if max_parallel_assets != 1:
        raise ValidationError(
            "asset-chain.v2 currently supports max_parallel_assets: 1 only.",
            code="asset_chain.parallel_execution_unsupported",
        )

    graph_payload = {
        "assets": [
            {
                "id": node.asset_id,
                "asset_code": node.asset_code,
                "inputs": dict(sorted(node.inputs.items())),
            }
            for node in nodes
        ],
        "topological_order": order,
    }
    graph_hash = sha256(_canonical_json(graph_payload)).hexdigest()
    return AssetChainSpec(
        name=name,
        yaml_path=path,
        yaml_hash=file_sha256(path),
        assets=tuple(nodes),
        topological_order=tuple(order),
        graph_hash=graph_hash,
        failure_policy=failure_policy,
        unchanged_policy=unchanged_policy,
        max_parallel_assets=max_parallel_assets,
    )


def run_asset_chain(
    yaml_path: str | Path,
    *,
    config_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> AssetChainResult:
    effective_project_root = project_root or infer_project_root(yaml_path)
    config = load_config(config_path=config_path, project_root=effective_project_root)
    spec = load_asset_chain(yaml_path, config=config)
    metadata_path = _metadata_path(spec, config=config)
    log_path = _log_path(spec, config=config)
    previous = _read_previous_assets(metadata_path)
    result = AssetChainResult(
        ok=True,
        chain_name=spec.name,
        yaml_path=spec.yaml_path,
        yaml_hash=spec.yaml_hash,
        graph_hash=spec.graph_hash,
        topological_order=list(spec.topological_order),
        assets=[],
        log_path=log_path,
    )
    append_stage_event(
        log_path,
        event="asset_chain.start",
        preset=ASSET_CHAIN_SCHEMA_VERSION,
        job_name=spec.name,
        details={"yaml_hash": spec.yaml_hash, "graph_hash": spec.graph_hash},
    )

    completed: dict[str, AssetExecutionResult] = {}
    by_id = spec.assets_by_id
    for asset_id in spec.topological_order:
        node = by_id[asset_id]
        blocked_by = [
            upstream
            for upstream in node.inputs.values()
            if completed[upstream].status in {"failed", "blocked"}
        ]
        input_fingerprint = _input_fingerprint(
            node,
            completed,
            project_root=config.project_root,
        )
        if blocked_by:
            asset_result = AssetExecutionResult(
                asset_id=asset_id,
                asset_code=node.asset_code,
                definition_path=node.definition_path,
                status="blocked",
                ok=False,
                input_fingerprint=input_fingerprint,
                details={"blocked_by": blocked_by},
                error_type="UpstreamAssetFailure",
                error_message="Upstream asset failed; downstream execution was blocked.",
            )
        elif _can_reuse(
            node,
            input_fingerprint=input_fingerprint,
            previous=previous.get(asset_id),
            unchanged_policy=spec.unchanged_policy,
        ):
            prior = previous[asset_id]
            asset_result = AssetExecutionResult(
                asset_id=asset_id,
                asset_code=node.asset_code,
                definition_path=node.definition_path,
                status="reused",
                ok=True,
                input_fingerprint=input_fingerprint,
                receipt_fingerprint=str(prior["receipt_fingerprint"]),
                output_paths=[Path(value) for value in prior.get("output_paths") or []],
                metadata_path=(
                    Path(str(prior["metadata_path"])) if prior.get("metadata_path") else None
                ),
                details={"reuse_reason": "definition_and_upstream_receipts_unchanged"},
            )
        else:
            asset_result = _execute_asset(
                node,
                input_fingerprint=input_fingerprint,
                config_path=config_path,
                project_root=config.project_root,
            )
        completed[asset_id] = asset_result
        result.assets.append(asset_result)
        append_stage_event(
            log_path,
            event=f"asset_chain.asset.{asset_result.status}",
            preset=ASSET_CHAIN_SCHEMA_VERSION,
            job_name=spec.name,
            details={
                "asset_id": asset_id,
                "asset_code": node.asset_code,
                "ok": asset_result.ok,
                "error_message": asset_result.error_message,
            },
        )

    result.ok = all(item.ok for item in result.assets)
    result.finished_at = utc_now_iso()
    result.metadata_path = metadata_path
    _write_chain_metadata(result, metadata_path)
    append_stage_event(
        log_path,
        event="asset_chain.finish",
        preset=ASSET_CHAIN_SCHEMA_VERSION,
        job_name=spec.name,
        details={"ok": result.ok, "asset_count": len(result.assets)},
    )
    return result


def _execute_asset(
    node: AssetNode,
    *,
    input_fingerprint: str,
    config_path: str | Path | None,
    project_root: Path,
) -> AssetExecutionResult:
    try:
        payload = execute_asset_definition(
            node.asset_code,
            node.definition_path,
            config_path=config_path,
            project_root=project_root,
        )
        ok = bool(payload.get("ok", True))
        output_paths = [Path(value) for value in payload.get("output_paths") or []]
        metadata_value = payload.get("metadata_path")
        metadata_path = Path(str(metadata_value)) if metadata_value else None
        if not ok:
            return AssetExecutionResult(
                asset_id=node.asset_id,
                asset_code=node.asset_code,
                definition_path=node.definition_path,
                status="failed",
                ok=False,
                input_fingerprint=input_fingerprint,
                output_paths=output_paths,
                metadata_path=metadata_path,
                details=dict(payload.get("details") or {}),
                error_type=str(payload.get("error_type") or "AssetExecutionFailure"),
                error_message=str(payload.get("error_message") or "Asset execution failed."),
            )
        receipt = _receipt_fingerprint(
            node,
            input_fingerprint=input_fingerprint,
            output_paths=output_paths,
            metadata_path=metadata_path,
        )
        return AssetExecutionResult(
            asset_id=node.asset_id,
            asset_code=node.asset_code,
            definition_path=node.definition_path,
            status="executed",
            ok=True,
            input_fingerprint=input_fingerprint,
            receipt_fingerprint=receipt,
            output_paths=output_paths,
            metadata_path=metadata_path,
            details=dict(payload.get("details") or {}),
        )
    except Exception as exc:  # noqa: BLE001 - adapters become chain receipts.
        return AssetExecutionResult(
            asset_id=node.asset_id,
            asset_code=node.asset_code,
            definition_path=node.definition_path,
            status="failed",
            ok=False,
            input_fingerprint=input_fingerprint,
            error_type=type(exc).__name__,
            error_message=str(exc),
            details={
                "error_code": getattr(exc, "code", None),
                "error_context": getattr(exc, "context", None),
            },
        )


def execute_asset_definition(
    asset_code: str,
    definition_path: str | Path,
    *,
    config_path: str | Path | None,
    project_root: str | Path,
) -> dict[str, Any]:
    """Execute one definition through the same registry used by Asset Chain."""

    return _asset_executor(asset_code)(
        Path(definition_path).resolve(),
        config_path=config_path,
        project_root=Path(project_root).resolve(),
    )


def _asset_executor(asset_code: str) -> Callable[..., dict[str, Any]]:
    if asset_code == "0101":
        return _run_source_asset
    if asset_code == "0102":
        return _run_calculated_fact_asset
    if asset_code == "0103":
        return _run_csv_source_asset
    if asset_code in {"0201", "0301"}:
        return _run_pipeline_asset
    if asset_code == "0401":
        return _run_snapshot_asset
    raise ValidationError(
        f"Unsupported asset code: {asset_code}",
        code="asset_chain.unsupported_asset_code",
        context={"asset_code": asset_code},
    )


def _run_source_asset(
    definition_path: Path,
    *,
    config_path: str | Path | None,
    project_root: Path,
) -> dict[str, Any]:
    from smoking_data.assets.a0101_source import execute_yaml

    result = execute_yaml(
        definition_path,
        project_root=project_root,
    )
    payload = result.to_dict()
    return {
        "ok": result.ok,
        "output_paths": list(payload.get("dataset_paths") or []),
        "metadata_path": None,
        "details": {
            "metadata_paths": list(payload.get("metadata_paths") or []),
            "log_path": payload.get("log_path"),
            "task_count": payload.get("task_count"),
            "success_task_count": payload.get("success_task_count"),
            "error_task_count": payload.get("error_task_count"),
            "physical_probe": payload.get("physical_probe"),
        },
        "error_message": None if result.ok else f"0101 source status: {payload.get('status')}",
    }


def _run_csv_source_asset(
    definition_path: Path,
    *,
    config_path: str | Path | None,
    project_root: Path,
) -> dict[str, Any]:
    from smoking_data.assets.a0103_csv_source import run_yaml

    return run_yaml(
        definition_path,
        config_path=config_path,
        project_root=project_root,
        trigger_type="chain",
    ).to_dict()


def _run_pipeline_asset(
    definition_path: Path,
    *,
    config_path: str | Path | None,
    project_root: Path,
) -> dict[str, Any]:
    from smoking_data.runtime.runner import run_pipeline_yaml

    result = run_pipeline_yaml(
        definition_path,
        config_path=config_path,
        project_root=project_root,
        trigger_type="chain",
    )
    payload = result.to_dict()
    output_dir = (payload.get("details") or {}).get("output_dir")
    if output_dir:
        payload["output_paths"] = [str(output_dir)]
    return payload


def _run_calculated_fact_asset(
    definition_path: Path,
    *,
    config_path: str | Path | None,
    project_root: Path,
) -> dict[str, Any]:
    from smoking_data.assets.a0102_calculated_fact import run_yaml

    return run_yaml(
        definition_path,
        config_path=config_path,
        project_root=project_root,
        trigger_type="chain",
    ).to_dict()


def _run_snapshot_asset(
    definition_path: Path,
    *,
    config_path: str | Path | None,
    project_root: Path,
) -> dict[str, Any]:
    from smoking_data.assets.a0401_snapshot import run_yaml

    return run_yaml(
        definition_path,
        config_path=config_path,
        project_root=project_root,
    ).to_dict()


def _input_fingerprint(
    node: AssetNode,
    completed: dict[str, AssetExecutionResult],
    *,
    project_root: Path,
) -> str:
    payload = {
        "asset_code": node.asset_code,
        "asset_config_fingerprint": asset_config_fingerprint(
            project_root,
            node.asset_code,
        ),
        "asset_contract_fingerprint": asset_contract_fingerprint(
            project_root,
            node.asset_code,
        ),
        "definition_hash": file_sha256(node.definition_path),
        "upstreams": {
            port: completed[upstream].receipt_fingerprint
            for port, upstream in sorted(node.inputs.items())
        },
    }
    return sha256(_canonical_json(payload)).hexdigest()


def _receipt_fingerprint(
    node: AssetNode,
    *,
    input_fingerprint: str,
    output_paths: list[Path],
    metadata_path: Path | None,
) -> str:
    payload = {
        "asset_id": node.asset_id,
        "input_fingerprint": input_fingerprint,
        "outputs": [_path_receipt(path) for path in sorted(output_paths, key=str)],
        "metadata": _path_receipt(metadata_path) if metadata_path else None,
    }
    return sha256(_canonical_json(payload)).hexdigest()


def _path_receipt(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        return {"path": str(resolved), "exists": False}
    if resolved.is_file():
        stat = resolved.stat()
        return {
            "path": str(resolved),
            "exists": True,
            "kind": "file",
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    files = []
    for child in sorted(item for item in resolved.rglob("*") if item.is_file()):
        stat = child.stat()
        files.append(
            {
                "path": str(child.relative_to(resolved)),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
    return {"path": str(resolved), "exists": True, "kind": "directory", "files": files}


def _can_reuse(
    node: AssetNode,
    *,
    input_fingerprint: str,
    previous: dict[str, Any] | None,
    unchanged_policy: str,
) -> bool:
    # Root assets may observe external state, so they are always executed.
    if unchanged_policy != "skip" or not node.inputs or not previous:
        return False
    if previous.get("input_fingerprint") != input_fingerprint:
        return False
    if previous.get("status") not in {"executed", "reused"}:
        return False
    if not previous.get("receipt_fingerprint"):
        return False
    output_paths = [Path(value) for value in previous.get("output_paths") or []]
    if not output_paths or not all(path.exists() for path in output_paths):
        return False
    metadata_value = previous.get("metadata_path")
    metadata_path = Path(str(metadata_value)) if metadata_value else None
    current_receipt = _receipt_fingerprint(
        node,
        input_fingerprint=input_fingerprint,
        output_paths=output_paths,
        metadata_path=metadata_path,
    )
    return current_receipt == previous.get("receipt_fingerprint")


def _validate_definition_schema(path: Path, *, asset_code: str, asset_id: str) -> None:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValidationError(
            "Asset definition root must be a mapping.",
            code="asset_chain.invalid_definition",
            context={"asset_id": asset_id},
        )
    yaml_header = _mapping(payload.get("yaml"), path="yaml")
    _reject_unknown(yaml_header, {"schema_version", "asset_code"}, path="yaml")
    expected = {
        "0101": "smoking-data.source.v5",
        "0102": "smoking-data.calculated-fact.v2",
        "0103": "smoking-data.csv-source.v1",
        "0201": "smoking-data.pipeline.v7",
        "0301": "smoking-data.pipeline.v6",
        "0401": "smoking-data.pipeline.v8",
    }.get(asset_code)
    if expected is None:
        raise ValidationError(
            f"Unsupported asset code: {asset_code}",
            code="asset_chain.unsupported_asset_code",
            context={"asset_id": asset_id, "asset_code": asset_code},
        )
    actual_code = str(yaml_header.get("asset_code") or "")
    actual_version = yaml_header.get("schema_version")
    if actual_code != asset_code or actual_version != expected:
        raise ValidationError(
            f"{asset_code} asset must reference {expected}.",
            code="asset_chain.definition_kind_mismatch",
            context={
                "asset_id": asset_id,
                "expected_schema_version": expected,
                "actual_schema_version": actual_version,
                "actual_asset_code": actual_code or None,
            },
        )


def _topological_order(
    nodes: list[AssetNode],
    dependencies: dict[str, set[str]],
) -> list[str]:
    positions = {node.asset_id: index for index, node in enumerate(nodes)}
    remaining = {key: set(value) for key, value in dependencies.items()}
    ready = sorted((key for key, value in remaining.items() if not value), key=positions.get)
    result: list[str] = []
    while ready:
        current = ready.pop(0)
        result.append(current)
        for candidate in sorted(remaining, key=positions.get):
            if current not in remaining[candidate]:
                continue
            remaining[candidate].remove(current)
            if not remaining[candidate] and candidate not in result and candidate not in ready:
                ready.append(candidate)
        ready.sort(key=positions.get)
    if len(result) != len(nodes):
        cyclic = [node.asset_id for node in nodes if node.asset_id not in result]
        raise ValidationError(
            "Asset chain graph contains a cycle.",
            code="asset_chain.cycle",
            context={"asset_ids": cyclic},
        )
    return result


def _metadata_path(spec: AssetChainSpec, *, config: RuntimeConfig) -> Path:
    return config.metadata_root / "asset_chain" / f"{spec.name}.metadata.json"


def _log_path(spec: AssetChainSpec, *, config: RuntimeConfig) -> Path:
    return config.log_root / "asset_chain" / f"{spec.name}.jsonl"


def _read_previous_assets(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    assets = payload.get("assets") if isinstance(payload, dict) else None
    if not isinstance(assets, list):
        return {}
    return {
        str(item["asset_id"]): item
        for item in assets
        if isinstance(item, dict) and item.get("asset_id")
    }


def _write_chain_metadata(result: AssetChainResult, path: Path) -> None:
    ensure_dir(path.parent)
    staging = path.with_suffix(path.suffix + ".tmp")
    staging.write_text(
        json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    staging.replace(path)


def _mapping(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(
            f"{path} must be a mapping.",
            code="yaml.invalid_type",
            context={"path": path},
        )
    return dict(value)


def _required_string(value: Any, *, path: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValidationError(
            f"{path} must be a non-empty string.",
            code="yaml.required_key",
            context={"path": path},
        )
    return text


def _reject_unknown(value: dict[str, Any], allowed: set[str], *, path: str) -> None:
    unknown = sorted(set(value).difference(allowed))
    if unknown:
        raise ValidationError(
            f"Unsupported keys at {path}: {', '.join(unknown)}",
            code="yaml.unknown_key",
            context={"path": path, "keys": unknown},
        )


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
