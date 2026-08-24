from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from hashlib import sha256
from typing import Any

from smoking_data.core.exceptions import ValidationError

PIPELINE_SCHEMA_VERSION = "smoking-data.pipeline.v6"
CURATED_PIPELINE_SCHEMA_VERSION = "smoking-data.pipeline.v7"
CANONICAL_OPERATION_VERSION = "smoking-data.operation-canonical.v1"
PIPELINE_ASSET_CODES = frozenset({"0201", "0301", "0401"})
PIPELINE_SCHEMA_VERSION_BY_ASSET = {
    "0201": CURATED_PIPELINE_SCHEMA_VERSION,
    "0301": PIPELINE_SCHEMA_VERSION,
    "0401": PIPELINE_SCHEMA_VERSION,
}
PUBLIC_EXECUTION_KEYS = frozenset(
    {
        "workers",
        "max_tasks_per_child",
        "memory_budget_mb",
        "memory",
        "target_rows_per_part",
        "target_key_groups_per_part",
        "max_source_files_per_task",
        "max_source_row_groups_per_task",
        "reset_before_run",
        "test_run",
        "sidecar_workers",
        "sidecar_worker_recycle_mode",
        "sidecar_max_source_files",
        "sidecar_max_projected_bytes_mb",
    }
)
ROOT_KEYS = frozenset({"yaml", "job", "operations", "output", "execution"})
CURATED_ROOT_KEYS = frozenset(
    {
        "yaml",
        "job",
        "migration",
        "define_upstream",
        "combine_upstream",
        "build_sidecar",
        "materialize",
        "save_dataset",
        "output",
        "execution",
    }
)
OUTPUT_ARTIFACT_TYPES = {
    "0201": "curated_dataset",
    "0301": "joined_dataset",
    "0401": "analysis_snapshot",
}

_PORTS: dict[str, tuple[str, ...]] = {
    "define_dataset": (),
    "define_asset": (),
    "define_combined": (),
    "build_sidecar": ("source",),
    "active_row_selection": ("sidecar",),
    "materialize": ("source",),
    "join": ("left", "right"),
    "save_dataset": ("data",),
}
_NO_DATA_INPUT = frozenset({"define_dataset", "define_asset", "define_combined"})
_REMOVED_SOURCE_OPS = {
    "load_asset": "define_asset",
    "load_dataset": "define_dataset",
}
_TERMINAL = "save_dataset"


def normalize_pipeline_document(
    raw: dict[str, Any],
    *,
    asset_resolver: Callable[[str, dict[str, Any] | None], dict[str, Any]] | None = None,
    _allow_legacy_curated_internal: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate a public DAG document and lower its graph shell for mature kernels."""

    yaml_header = _mapping(raw.get("yaml"), path="yaml")
    _reject_unknown(yaml_header, {"schema_version", "asset_code"}, path="yaml")
    version = str(yaml_header.get("schema_version") or "")
    asset_code = str(yaml_header.get("asset_code") or "")
    if asset_code not in PIPELINE_ASSET_CODES:
        raise ValidationError(
            f"Unsupported yaml.asset_code: {asset_code or '<missing>'}",
            code="yaml.unsupported_asset_code",
            context={"expected": sorted(PIPELINE_ASSET_CODES), "actual": asset_code or None},
        )
    expected_version = PIPELINE_SCHEMA_VERSION_BY_ASSET.get(asset_code)
    legacy_curated_internal = (
        _allow_legacy_curated_internal
        and asset_code == "0201"
        and version == PIPELINE_SCHEMA_VERSION
        and "operations" in raw
    )
    if version != expected_version and not legacy_curated_internal:
        raise ValidationError(
            f"Unsupported schema_version: {version or '<missing>'}",
            code="yaml.unsupported_schema_version",
            context={"expected": [expected_version], "actual": version or None},
        )
    operation_phases: dict[str, str] = {}
    if asset_code == "0201" and not legacy_curated_internal:
        _reject_unknown(raw, CURATED_ROOT_KEYS, path="$")
        raw, operation_phases = _expand_curated_phase_document(raw)
    else:
        _reject_unknown(raw, ROOT_KEYS, path="$")
    job = _mapping(raw.get("job"), path="job")
    _reject_unknown(job, {"name"}, path="job")
    _required_string(job.get("name"), path="job.name")
    output = _validate_output(raw.get("output"), asset_code=asset_code)
    execution = _mapping(raw.get("execution") or {}, path="execution")
    _reject_unknown(execution, PUBLIC_EXECUTION_KEYS, path="execution")
    operations = raw.get("operations")
    if not isinstance(operations, list) or not operations:
        raise ValidationError(
            "operations must be a non-empty list.",
            code="yaml.invalid_type",
            context={"path": "operations"},
        )

    indexed: dict[str, dict[str, Any]] = {}
    positions: dict[str, int] = {}
    for index, value in enumerate(operations):
        path = f"operations[{index}]"
        operation = _mapping(value, path=path)
        operation_id = _required_string(operation.get("alias"), path=f"{path}.alias")
        operation_name = _required_string(operation.get("op"), path=f"{path}.op")
        if operation_name in _REMOVED_SOURCE_OPS:
            raise ValidationError(
                f"Operation {operation_name!r} was removed; use "
                f"{_REMOVED_SOURCE_OPS[operation_name]!r}.",
                code="operation.unsupported",
                context={
                    "path": f"{path}.op",
                    "operation": operation_name,
                    "replacement": _REMOVED_SOURCE_OPS[operation_name],
                },
            )
        if operation_id in indexed:
            raise ValidationError(
                f"Duplicate operation alias: {operation_id}",
                code="dag.duplicate_operation_alias",
                context={"path": f"{path}.alias", "operation_alias": operation_id},
            )
        indexed[operation_id] = operation
        positions[operation_id] = index

    edges: list[dict[str, str]] = []
    dependencies: dict[str, set[str]] = {operation_id: set() for operation_id in indexed}
    consumers: dict[str, set[str]] = defaultdict(set)
    for operation_id, operation in indexed.items():
        kind = str(operation["op"])
        path = f"operations[{positions[operation_id]}]"
        inputs = operation.get("inputs")
        if kind in _NO_DATA_INPUT:
            if inputs not in (None, {}):
                raise ValidationError(
                    f"{kind} does not accept inputs.",
                    code="dag.unexpected_inputs",
                    context={"operation_id": operation_id, "path": f"{path}.inputs"},
                )
            inputs = {}
        else:
            inputs = _mapping(inputs, path=f"{path}.inputs")
        required = _PORTS.get(kind, ("data",))
        allowed = set(required)
        if kind == "materialize":
            allowed.add("coordinates")
        unknown_ports = sorted(set(inputs).difference(allowed))
        if unknown_ports:
            raise ValidationError(
                "Operation contains unsupported input ports.",
                code="dag.unknown_input_port",
                context={"operation_id": operation_id, "ports": unknown_ports},
            )
        missing_ports = [port for port in required if not str(inputs.get(port) or "").strip()]
        if missing_ports:
            raise ValidationError(
                "Operation is missing required input ports.",
                code="dag.missing_input_port",
                context={"operation_id": operation_id, "ports": missing_ports},
            )
        if kind == "active_row_selection":
            _require_upstream_kind(
                indexed,
                inputs["sidecar"],
                {"build_sidecar"},
                operation_id,
                "sidecar",
            )
        if kind == "materialize" and inputs.get("coordinates"):
            _require_upstream_kind(
                indexed,
                inputs["coordinates"],
                {"active_row_selection"},
                operation_id,
                "coordinates",
            )
        if kind == "build_sidecar":
            source_id = str(inputs["source"])
            if source_id in indexed and str(indexed[source_id]["op"]) == "build_sidecar":
                raise ValidationError(
                    "build_sidecar source must be a dataset-producing operation.",
                    code="dag.input_artifact_mismatch",
                    context={"operation_id": operation_id, "input_port": "source"},
                )
        for port, upstream_value in inputs.items():
            upstream = _required_string(upstream_value, path=f"{path}.inputs.{port}")
            if upstream not in indexed:
                raise ValidationError(
                    f"Unknown upstream operation: {upstream}",
                    code="dag.unknown_input_operation",
                    context={
                        "operation_id": operation_id,
                        "input_port": port,
                        "upstream_operation_id": upstream,
                    },
                )
            if upstream == operation_id:
                raise ValidationError(
                    "Operation cannot consume itself.",
                    code="dag.self_reference",
                    context={"operation_id": operation_id, "input_port": port},
                )
            expected_artifact = _expected_input_artifact(kind, str(port))
            actual_artifact = _output_artifact(str(indexed[upstream]["op"]))
            if actual_artifact != expected_artifact:
                raise ValidationError(
                    "Input port references an incompatible artifact.",
                    code="dag.input_artifact_mismatch",
                    context={
                        "operation_id": operation_id,
                        "input_port": port,
                        "upstream_operation_id": upstream,
                        "expected_artifact": expected_artifact,
                        "actual_artifact": actual_artifact,
                    },
                )
            dependencies[operation_id].add(upstream)
            consumers[upstream].add(operation_id)
            edges.append({"from": upstream, "to": operation_id, "port": str(port)})

    ordered_ids = _stable_topological_sort(dependencies, positions)
    saves = [
        operation_id for operation_id in ordered_ids if indexed[operation_id]["op"] == _TERMINAL
    ]
    if len(saves) != 1:
        raise ValidationError(
            "Pipeline requires exactly one save_dataset operation.",
            code="dag.terminal_save_required",
            context={"operation_ids": saves},
        )
    save_id = saves[0]
    if consumers.get(save_id):
        raise ValidationError(
            "save_dataset must be terminal.",
            code="dag.save_not_terminal",
            context={"operation_id": save_id, "consumers": sorted(consumers[save_id])},
        )
    ancestors = _ancestors(save_id, dependencies)
    disconnected = sorted(set(indexed).difference(ancestors | {save_id}))
    if disconnected:
        raise ValidationError(
            "Every operation must contribute to the terminal save_dataset.",
            code="dag.disconnected_operation",
            context={"operation_ids": disconnected},
        )

    sources = _compile_sources(indexed, asset_resolver=asset_resolver)
    sinks = _compile_sinks(indexed, output=output)
    lowered_operations = _lower_operations(indexed, ordered_ids, dependencies)
    canonical_nodes, node_keys = _canonical_nodes(
        indexed,
        ordered_ids,
        sources=sources,
        operation_phases=operation_phases,
        pipeline_schema_version=version,
    )
    canonical_edges = [
        {
            "from": node_keys[edge["from"]],
            "to": node_keys[edge["to"]],
            "port": edge["port"],
            "from_alias": edge["from"],
            "to_alias": edge["to"],
        }
        for edge in edges
    ]
    graph = {
        "canonicalization_version": CANONICAL_OPERATION_VERSION,
        "nodes": canonical_nodes,
        "edges": canonical_edges,
        "alias_index": {alias: node_keys[alias] for alias in ordered_ids},
        "topological_order": [node_keys[alias] for alias in ordered_ids],
        "topological_alias_order": ordered_ids,
        "terminal_node_key": node_keys[save_id],
        "terminal_alias": save_id,
    }
    graph["graph_hash"] = sha256(
        _canonical_json(
            {
                "canonicalization_version": CANONICAL_OPERATION_VERSION,
                "nodes": [
                    {
                        "node_key": node["node_key"],
                        "spec_key": node["spec_key"],
                        "inputs": node["inputs"],
                    }
                    for node in sorted(canonical_nodes, key=lambda item: item["node_key"])
                ],
                "terminal_node_key": node_keys[save_id],
            }
        ).encode()
    ).hexdigest()
    normalized = {
        "schema_version": version,
        "job": dict(job),
        **(
            {"migration": dict(raw["migration"])}
            if isinstance(raw.get("migration"), dict)
            else {}
        ),
        "sources": sources,
        "operations": lowered_operations,
        "sinks": sinks,
        "output": output,
        "execution": dict(execution),
        "operation_phases": operation_phases,
    }
    return normalized, graph


def _expand_curated_phase_document(
    raw: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Lower the public 0201 phase contract to the internal operation DAG."""

    migration = _validate_migration_metadata(raw.get("migration"))

    upstreams = raw.get("define_upstream")
    if not isinstance(upstreams, list) or not upstreams:
        raise ValidationError(
            "define_upstream must be a non-empty list.",
            code="yaml.invalid_type",
            context={"path": "define_upstream"},
        )
    operations: list[dict[str, Any]] = []
    upstream_operations: list[dict[str, Any]] = []
    phases: dict[str, str] = {}
    for index, value in enumerate(upstreams):
        path = f"define_upstream[{index}]"
        operation = dict(_mapping(value, path=path))
        _reject_unknown(
            operation,
            {
                "op",
                "alias",
                "definition",
                "paths",
                "format",
                "union_by_name",
                "missing_columns",
                "incompatible_dtypes",
                "source_identity",
                "select",
            },
            path=path,
        )
        kind = _required_string(operation.get("op"), path=f"{path}.op")
        if kind not in {"define_asset", "define_dataset"}:
            raise ValidationError(
                "define_upstream accepts only define_asset or define_dataset.",
                code="operation.unsupported",
                context={"path": f"{path}.op", "operation": kind},
            )
        alias = _required_string(operation.get("alias"), path=f"{path}.alias")
        upstream_operations.append(operation)

    combine = raw.get("combine_upstream")
    if combine is None:
        operations.extend(upstream_operations)
        for operation in upstream_operations:
            phases[str(operation["alias"])] = "define_upstream"
    else:
        combined = _mapping(combine, path="combine_upstream")
        _reject_unknown(
            combined,
            {
                "op",
                "alias",
                "sources",
                "source_column",
                "union_by_name",
                "missing_columns",
                "incompatible_dtypes",
                "duplicate_path_policy",
            },
            path="combine_upstream",
        )
        if combined.get("op") != "union_rows":
            raise ValidationError(
                "combine_upstream.op must be union_rows.",
                code="combine_upstream.unsupported_operation",
            )
        combined_alias = _required_string(
            combined.get("alias"), path="combine_upstream.alias"
        )
        source_aliases = _string_list(
            combined.get("sources"), path="combine_upstream.sources"
        )
        upstream_by_alias = {str(item["alias"]): item for item in upstream_operations}
        if len(set(source_aliases)) != len(source_aliases) or set(source_aliases) != set(
            upstream_by_alias
        ):
            raise ValidationError(
                "combine_upstream.sources must reference every upstream exactly once.",
                code="combine_upstream.invalid_sources",
                context={"sources": source_aliases, "available": sorted(upstream_by_alias)},
            )
        source_column = _mapping(
            combined.get("source_column"), path="combine_upstream.source_column"
        )
        _reject_unknown(
            source_column,
            {"name", "dtype", "existing_column_policy"},
            path="combine_upstream.source_column",
        )
        source_column_name = _required_string(
            source_column.get("name"), path="combine_upstream.source_column.name"
        )
        if source_column.get("dtype") != "STRING" or source_column.get(
            "existing_column_policy"
        ) != "error":
            raise ValidationError(
                "source_column requires dtype=STRING and existing_column_policy=error.",
                code="combine_upstream.invalid_source_column",
            )
        for field, expected in (
            ("union_by_name", True),
            ("missing_columns", "insert_null"),
            ("incompatible_dtypes", "error"),
            ("duplicate_path_policy", "error"),
        ):
            if combined.get(field, expected) != expected:
                raise ValidationError(
                    f"combine_upstream.{field} must be {expected!r}.",
                    code="combine_upstream.invalid_policy",
                    context={"field": field},
                )
        members = [upstream_by_alias[alias] for alias in source_aliases]
        explicit_identities = []
        for member_index, member in enumerate(members):
            identity = _mapping(
                member.get("source_identity") or {},
                path=f"define_upstream[{member_index}].source_identity",
            )
            _reject_unknown(
                identity,
                {"value"},
                path=f"define_upstream[{member_index}].source_identity",
            )
            if identity.get("value"):
                explicit_identities.append(
                    _required_string(
                        identity.get("value"),
                        path=f"define_upstream[{member_index}].source_identity.value",
                    )
                )
        if len(explicit_identities) != len(set(explicit_identities)):
            raise ValidationError(
                "define_upstream source_identity values must be unique.",
                code="combine_upstream.duplicate_source_identity",
            )
        operations.append(
            {
                "op": "define_combined",
                "alias": combined_alias,
                "members": members,
                "source_column": {
                    "name": source_column_name,
                    "dtype": "STRING",
                    "existing_column_policy": "error",
                },
                "union_by_name": True,
                "missing_columns": "insert_null",
                "incompatible_dtypes": "error",
                "duplicate_path_policy": "error",
            }
        )
        phases[combined_alias] = "combine_upstream"

    sidecar = _mapping(raw.get("build_sidecar"), path="build_sidecar")
    _reject_unknown(
        sidecar,
        {"alias", "source", "columns", "operations", "execution"},
        path="build_sidecar",
    )
    sidecar_alias = _required_string(sidecar.get("alias"), path="build_sidecar.alias")
    sidecar_source = _required_string(sidecar.get("source"), path="build_sidecar.source")
    sidecar_execution = _mapping(
        sidecar.get("execution") or {}, path="build_sidecar.execution"
    )
    _reject_unknown(
        sidecar_execution,
        {"workers", "worker_recycle"},
        path="build_sidecar.execution",
    )
    sidecar_workers = sidecar_execution.get("workers", 1)
    if (
        not isinstance(sidecar_workers, int)
        or isinstance(sidecar_workers, bool)
        or sidecar_workers < 1
    ):
        raise ValidationError(
            "build_sidecar.execution.workers must be an integer >= 1.",
            code="yaml.invalid_type",
            context={"path": "build_sidecar.execution.workers", "value": sidecar_workers},
        )
    worker_recycle = _mapping(
        sidecar_execution.get("worker_recycle") or {},
        path="build_sidecar.execution.worker_recycle",
    )
    _reject_unknown(
        worker_recycle,
        {"mode", "max_source_files", "max_projected_bytes_mb"},
        path="build_sidecar.execution.worker_recycle",
    )
    recycle_mode = str(worker_recycle.get("mode") or "adaptive").strip().lower()
    if recycle_mode != "adaptive":
        raise ValidationError(
            "build_sidecar worker recycle mode must be adaptive.",
            code="build_sidecar.unsupported_worker_recycle_mode",
            context={
                "path": "build_sidecar.execution.worker_recycle.mode",
                "value": recycle_mode,
            },
        )
    for key, default in (("max_source_files", 16), ("max_projected_bytes_mb", 512)):
        value = worker_recycle.get(key, default)
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValidationError(
                f"build_sidecar.execution.worker_recycle.{key} must be an integer >= 1.",
                code="yaml.invalid_type",
                context={
                    "path": f"build_sidecar.execution.worker_recycle.{key}",
                    "value": value,
                },
            )
    sidecar_operations = sidecar.get("operations")
    if not isinstance(sidecar_operations, list) or not sidecar_operations:
        raise ValidationError(
            "build_sidecar.operations must be a non-empty list.",
            code="yaml.invalid_type",
            context={"path": "build_sidecar.operations"},
        )
    selector_items = [
        index
        for index, item in enumerate(sidecar_operations)
        if isinstance(item, dict) and item.get("op") == "active_row_selection"
    ]
    if selector_items != [len(sidecar_operations) - 1]:
        raise ValidationError(
            "build_sidecar.operations requires exactly one final active_row_selection.",
            code="build_sidecar.selector_position_invalid",
            context={"path": "build_sidecar.operations"},
        )
    current = sidecar_source
    for index, value in enumerate(sidecar_operations[:-1]):
        path = f"build_sidecar.operations[{index}]"
        operation = dict(_mapping(value, path=path))
        kind = _required_string(operation.get("op"), path=f"{path}.op")
        if kind not in {"filter", "type_cast", "add_calc"}:
            raise ValidationError(
                "build_sidecar supports filter, type_cast, and add_calc before selection.",
                code="operation.unsupported",
                context={"path": f"{path}.op", "operation": kind},
            )
        alias = str(operation.pop("alias", "") or f"{sidecar_alias}__{index + 1:02d}_{kind}")
        operation["alias"] = alias
        operation["inputs"] = {"data": current}
        operations.append(operation)
        phases[alias] = "build_sidecar"
        current = alias
    candidate_alias = f"{sidecar_alias}__candidates"
    operations.append(
        {
            "op": "build_sidecar",
            "alias": candidate_alias,
            "inputs": {"source": current},
            "columns": sidecar.get("columns", "auto"),
        }
    )
    phases[candidate_alias] = "build_sidecar"
    selector = dict(_mapping(sidecar_operations[-1], path="build_sidecar.operations[-1]"))
    selector.pop("alias", None)
    selector["alias"] = sidecar_alias
    selector["inputs"] = {"sidecar": candidate_alias}
    operations.append(selector)
    phases[sidecar_alias] = "build_sidecar"

    materialize = _mapping(raw.get("materialize"), path="materialize")
    _reject_unknown(
        materialize,
        {
            "alias",
            "source",
            "coordinates",
            "partition_by",
            "part_boundary",
            "workers",
            "max_tasks_per_child",
            "operations",
        },
        path="materialize",
    )
    materialize_alias = _required_string(materialize.get("alias"), path="materialize.alias")
    materialize_source = _required_string(materialize.get("source"), path="materialize.source")
    coordinates = _required_string(
        materialize.get("coordinates"), path="materialize.coordinates"
    )
    if coordinates != sidecar_alias:
        raise ValidationError(
            "materialize.coordinates must reference build_sidecar.alias.",
            code="materialize.invalid_coordinate_source",
            context={"coordinates": coordinates, "expected": sidecar_alias},
        )
    payload_operations = materialize.get("operations") or []
    if not isinstance(payload_operations, list):
        raise ValidationError(
            "materialize.operations must be a list.",
            code="yaml.invalid_type",
            context={"path": "materialize.operations"},
        )
    boundary_alias = materialize_alias if not payload_operations else f"{materialize_alias}__boundary"
    operations.append(
        {
            "op": "materialize",
            "alias": boundary_alias,
            "inputs": {"source": materialize_source, "coordinates": coordinates},
            "partition_by": materialize.get("partition_by"),
            "part_boundary": materialize.get("part_boundary"),
            "workers": materialize.get("workers", 1),
            "max_tasks_per_child": materialize.get("max_tasks_per_child", 1),
        }
    )
    phases[boundary_alias] = "materialize"
    current = boundary_alias
    materialize_allowed = {
        "type_cast",
        "add_calc",
        "reference_replace",
        "list_restore",
        "include_columns",
        "exclude_columns",
        "rename_columns",
        "unpivot",
        "pivot",
    }
    for index, value in enumerate(payload_operations):
        path = f"materialize.operations[{index}]"
        operation = dict(_mapping(value, path=path))
        kind = _required_string(operation.get("op"), path=f"{path}.op")
        if kind not in materialize_allowed:
            raise ValidationError(
                "Unsupported materialize payload operation.",
                code="operation.unsupported",
                context={"path": f"{path}.op", "operation": kind},
            )
        alias = str(
            operation.pop("alias", "")
            or (materialize_alias if index == len(payload_operations) - 1 else f"{materialize_alias}__{index + 1:02d}_{kind}")
        )
        operation["alias"] = alias
        operation["inputs"] = {"data": current}
        operations.append(operation)
        phases[alias] = "materialize"
        current = alias

    save = _mapping(raw.get("save_dataset"), path="save_dataset")
    _reject_unknown(save, {"alias", "input", "partition_by", "operations"}, path="save_dataset")
    save_input = _required_string(save.get("input"), path="save_dataset.input")
    if save_input != materialize_alias:
        raise ValidationError(
            "save_dataset.input must reference materialize.alias.",
            code="save_dataset.invalid_input",
            context={"input": save_input, "expected": materialize_alias},
        )
    validation_operations = save.get("operations") or []
    if not isinstance(validation_operations, list):
        raise ValidationError(
            "save_dataset.operations must be a list.",
            code="yaml.invalid_type",
            context={"path": "save_dataset.operations"},
        )
    for index, value in enumerate(validation_operations):
        path = f"save_dataset.operations[{index}]"
        operation = dict(_mapping(value, path=path))
        if operation.get("op") != "data_assertion":
            raise ValidationError(
                "save_dataset.operations accepts only data_assertion.",
                code="operation.unsupported",
                context={"path": f"{path}.op", "operation": operation.get("op")},
            )
        alias = str(operation.pop("alias", "") or f"save_dataset__{index + 1:02d}_assertion")
        operation["alias"] = alias
        operation["inputs"] = {"data": current}
        operations.append(operation)
        phases[alias] = "save_dataset"
        current = alias
    save_alias = str(save.get("alias") or "save_dataset")
    operations.append(
        {
            "op": "save_dataset",
            "alias": save_alias,
            "inputs": {"data": current},
            "partition_by": save.get("partition_by"),
        }
    )
    phases[save_alias] = "save_dataset"

    execution = dict(raw.get("execution") or {})
    execution.update(
        {
            "sidecar_workers": sidecar_workers,
            "sidecar_worker_recycle_mode": recycle_mode,
            "sidecar_max_source_files": int(worker_recycle.get("max_source_files", 16)),
            "sidecar_max_projected_bytes_mb": int(
                worker_recycle.get("max_projected_bytes_mb", 512)
            ),
        }
    )
    return (
        {
            "yaml": raw["yaml"],
            "job": raw["job"],
            **({"migration": migration} if migration is not None else {}),
            "operations": operations,
            "output": raw["output"],
            "execution": execution,
        },
        phases,
    )


def _validate_migration_metadata(value: Any) -> dict[str, Any] | None:
    """Validate the optional marker used by migration-purpose 0201 jobs."""

    if value is None:
        return None
    migration = _mapping(value, path="migration")
    _reject_unknown(
        migration,
        {
            "id",
            "mode",
            "purpose",
            "source_definition",
            "source_definition_hash",
            "source_asset",
            "source_path",
        },
        path="migration",
    )
    migration_id = _required_string(migration.get("id"), path="migration.id")
    mode = str(migration.get("mode") or "").strip().lower()
    if mode not in {"pass_through", "transform"}:
        raise ValidationError(
            "migration.mode must be pass_through or transform.",
            code="migration.invalid_mode",
            context={"path": "migration.mode", "value": mode or None},
        )
    normalized = {"id": migration_id, "mode": mode}
    for key in (
        "purpose",
        "source_definition",
        "source_definition_hash",
        "source_asset",
        "source_path",
    ):
        if migration.get(key) is not None:
            normalized[key] = _required_string(migration[key], path=f"migration.{key}")
    return normalized


def _canonical_nodes(
    indexed: dict[str, dict[str, Any]],
    ordered_aliases: list[str],
    *,
    sources: dict[str, dict[str, Any]],
    operation_phases: dict[str, str],
    pipeline_schema_version: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    nodes: list[dict[str, Any]] = []
    node_keys: dict[str, str] = {}
    for alias in ordered_aliases:
        operation = indexed[alias]
        kind = str(operation["op"])
        config = {
            key: value
            for key, value in operation.items()
            if key not in {"alias", "inputs", "op"}
        }
        if kind == "define_asset":
            source = sources[alias]
            config = {
                "asset_code": source.get("asset_code"),
                "asset_definition": source.get("asset_definition"),
                "asset_definition_hash": source.get("asset_definition_hash"),
                "paths": source.get("paths"),
            }
        elif kind == "define_combined":
            source = sources[alias]
            config = {
                "combined_members": source.get("combined_members"),
                "source_column": source.get("source_column"),
                "duplicate_path_policy": source.get("duplicate_path_policy"),
            }
        spec_payload = {
            "canonicalization_version": CANONICAL_OPERATION_VERSION,
            "pipeline_schema_version": pipeline_schema_version,
            "op": kind,
            "config": config,
        }
        if alias in operation_phases:
            spec_payload["phase"] = operation_phases[alias]
        canonical_json = _canonical_json(spec_payload)
        spec_key = f"{kind}_{sha256(canonical_json.encode()).hexdigest()}"
        inputs = {
            str(port): node_keys[str(upstream)]
            for port, upstream in sorted(dict(operation.get("inputs") or {}).items())
        }
        node_payload = {"spec_key": spec_key, "inputs": inputs}
        node_key = f"{kind}_{sha256(_canonical_json(node_payload).encode()).hexdigest()}"
        node_keys[alias] = node_key
        nodes.append(
            {
                "alias": alias,
                "op": kind,
                "spec_key": spec_key,
                "node_key": node_key,
                "canonical_json": canonical_json,
                "inputs": inputs,
            }
        )
    return nodes, node_keys


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _compile_sources(
    indexed: dict[str, dict[str, Any]],
    *,
    asset_resolver: Callable[[str, dict[str, Any] | None], dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for operation_id, operation in indexed.items():
        kind = str(operation["op"])
        if kind not in {"define_dataset", "define_asset", "define_combined"}:
            continue
        if kind == "define_combined":
            _reject_unknown(
                operation,
                {
                    "alias",
                    "op",
                    "members",
                    "source_column",
                    "union_by_name",
                    "missing_columns",
                    "incompatible_dtypes",
                    "duplicate_path_policy",
                },
                path=f"operations[{operation_id}]",
            )
            members = operation.get("members")
            if not isinstance(members, list) or len(members) < 2:
                raise ValidationError(
                    "define_combined requires at least two members.",
                    code="combine_upstream.invalid_sources",
                )
            compiled_members = [
                _compile_combined_member(
                    item,
                    asset_resolver=asset_resolver,
                    path=f"operations[{operation_id}].members[{index}]",
                )
                for index, item in enumerate(members)
            ]
            identities = [str(item["source_identity"]) for item in compiled_members]
            if len(set(identities)) != len(identities):
                raise ValidationError(
                    "Combined source identity values must be unique.",
                    code="combine_upstream.duplicate_source_identity",
                )
            result[operation_id] = {
                "kind": "parquet_dataset",
                "paths": [
                    member_path
                    for member in compiled_members
                    for member_path in member["paths"]
                ],
                "union_by_name": True,
                "missing_columns": "insert_null",
                "incompatible_dtypes": "error",
                "combined_members": compiled_members,
                "source_column": dict(operation["source_column"]),
                "duplicate_path_policy": "error",
            }
            continue
        if kind == "define_asset":
            _reject_unknown(
                operation,
                {"alias", "op", "definition", "source_identity", "select"},
                path=f"operations[{operation_id}]",
            )
            definition = _required_string(
                operation.get("definition"),
                path=f"operations[{operation_id}].definition",
            )
            resolved = (
                asset_resolver(definition, operation.get("select"))
                if asset_resolver is not None
                else {
                    "paths": [f"asset-definition:{definition}"],
                    "asset_definition": definition,
                    "asset_definition_hash": "unresolved",
                    "asset_code": "unresolved",
                }
            )
            paths = resolved.get("paths")
            if not isinstance(paths, list) or not paths or not all(
                str(item).strip() for item in paths
            ):
                raise ValidationError(
                    "define_asset resolver must return non-empty dataset paths.",
                    code="asset.invalid_output_contract",
                    context={"operation_id": operation_id, "definition": definition},
                )
            result[operation_id] = {
                "kind": "parquet_dataset",
                "paths": [str(item) for item in paths],
                "union_by_name": True,
                "missing_columns": "insert_null",
                "incompatible_dtypes": "error",
                "asset_definition": str(resolved.get("asset_definition") or definition),
                "asset_definition_hash": str(
                    resolved.get("asset_definition_hash") or ""
                ),
                "asset_code": str(resolved.get("asset_code") or ""),
            }
            continue
        _reject_unknown(
            operation,
            {
                "alias",
                "op",
                "paths",
                "format",
                "union_by_name",
                "missing_columns",
                "incompatible_dtypes",
            },
            path=f"operations[{operation_id}]",
        )
        if str(operation.get("format") or "parquet").lower() != "parquet":
            raise ValidationError(
                "define_dataset currently supports format=parquet only.",
                code="source.unsupported_kind",
                context={"operation_id": operation_id},
            )
        paths = operation.get("paths")
        if not isinstance(paths, list) or not paths or not all(str(item).strip() for item in paths):
            raise ValidationError(
                "define_dataset paths must be a non-empty string list.",
                code="yaml.invalid_type",
                context={"operation_id": operation_id, "path": "paths"},
            )
        result[operation_id] = {
            "kind": "parquet_dataset",
            "paths": [str(item) for item in paths],
            "union_by_name": operation.get("union_by_name", True),
            "missing_columns": operation.get("missing_columns") or "insert_null",
            "incompatible_dtypes": operation.get("incompatible_dtypes") or "error",
        }
    if not result:
        raise ValidationError(
            "Pipeline requires define_asset or define_dataset.",
            code="dag.source_required",
        )
    return result


def _compile_combined_member(
    raw: Any,
    *,
    asset_resolver: Callable[[str, dict[str, Any] | None], dict[str, Any]] | None,
    path: str,
) -> dict[str, Any]:
    member = _mapping(raw, path=path)
    kind = _required_string(member.get("op"), path=f"{path}.op")
    alias = _required_string(member.get("alias"), path=f"{path}.alias")
    identity = _mapping(member.get("source_identity") or {}, path=f"{path}.source_identity")
    explicit_identity = str(identity.get("value") or "").strip()
    if kind == "define_asset":
        definition = _required_string(member.get("definition"), path=f"{path}.definition")
        resolved = (
            asset_resolver(definition, member.get("select"))
            if asset_resolver is not None
            else {
                "paths": [f"asset-definition:{definition}"],
                "asset_definition": definition,
                "asset_definition_hash": "unresolved",
                "asset_code": "unresolved",
            }
        )
        paths = resolved.get("paths")
        if not isinstance(paths, list) or not paths:
            raise ValidationError(
                "Combined define_asset resolved no dataset paths.",
                code="asset.invalid_output_contract",
                context={"alias": alias},
            )
        definition_name = str(resolved.get("asset_definition") or definition)
        return {
            "alias": alias,
            "kind": "define_asset",
            "paths": [str(item) for item in paths],
            "source_identity": explicit_identity or definition_name.rsplit("/", 1)[-1],
            "asset_definition": definition_name,
            "asset_definition_hash": str(resolved.get("asset_definition_hash") or ""),
            "asset_code": str(resolved.get("asset_code") or ""),
            "selection": member.get("select") or {},
        }
    if kind == "define_dataset":
        paths = member.get("paths")
        if not isinstance(paths, list) or not paths or not all(str(item).strip() for item in paths):
            raise ValidationError(
                "Combined define_dataset paths must be non-empty.",
                code="yaml.invalid_type",
                context={"alias": alias},
            )
        return {
            "alias": alias,
            "kind": "define_dataset",
            "paths": [str(item) for item in paths],
            "source_identity": explicit_identity or alias,
            "asset_definition": None,
            "asset_definition_hash": None,
            "asset_code": None,
            "selection": member.get("select") or {},
        }
    raise ValidationError(
        "Combined member must be define_asset or define_dataset.",
        code="operation.unsupported",
        context={"alias": alias, "op": kind},
    )


def _compile_sinks(
    indexed: dict[str, dict[str, Any]],
    *,
    output: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for operation_id, operation in indexed.items():
        if operation["op"] != "save_dataset":
            continue
        _reject_unknown(
            operation,
            {
                "alias",
                "op",
                "inputs",
                "partition_by",
            },
            path=f"operations[{operation_id}]",
        )
        artifact = output["artifact"]
        write_policy = str(artifact["write_policy"])
        result[operation_id] = {
            "kind": "parquet_dataset",
            "path": str(artifact["root_dir"]),
            "compression": str(artifact["compression"]),
            "overwrite": write_policy == "atomic_replace",
        }
    return result


def _validate_output(value: Any, *, asset_code: str) -> dict[str, Any]:
    output = _mapping(value, path="output")
    _reject_unknown(output, {"artifact", "logging"}, path="output")
    artifact = _mapping(output.get("artifact"), path="output.artifact")
    _reject_unknown(
        artifact,
        {
            "type",
            "root_dir",
            "format",
            "compression",
            "write_policy",
            "physical_layout",
            "publication",
        },
        path="output.artifact",
    )
    expected_type = OUTPUT_ARTIFACT_TYPES[asset_code]
    if str(artifact.get("type") or "") != expected_type:
        raise ValidationError(
            f"output.artifact.type must be {expected_type!r} for Asset {asset_code}.",
            code="output.invalid_artifact_type",
            context={"asset_code": asset_code, "expected": expected_type},
        )
    root_dir = _required_string(artifact.get("root_dir"), path="output.artifact.root_dir")
    if str(artifact.get("format") or "") != "parquet":
        raise ValidationError(
            "output.artifact.format must be parquet.",
            code="output.unsupported_format",
        )
    compression = str(artifact.get("compression") or "").lower()
    if compression not in {"snappy", "zstd", "uncompressed"}:
        raise ValidationError(
            "Unsupported output artifact compression.",
            code="output.unsupported_compression",
            context={"value": compression},
        )
    write_policy = str(artifact.get("write_policy") or "")
    if write_policy not in {"atomic_replace", "error_if_exists"}:
        raise ValidationError(
            "Unsupported output artifact write_policy.",
            code="output.unsupported_write_policy",
            context={"value": write_policy},
        )
    default_physical_layout = {
        "profile": {
            "0201": "curated_reuse_v1",
            "0301": "joined_reuse_v1",
            "0401": "analysis_snapshot_adaptive_v1",
        }[asset_code],
        "adaptation_scope": "task_adaptive" if asset_code == "0401" else "generation_fixed",
        "row_group_rows": "auto",
    }
    physical_layout = _mapping(
        artifact.get("physical_layout") or default_physical_layout,
        path="output.artifact.physical_layout",
    )
    _reject_unknown(
        physical_layout,
        {"profile", "adaptation_scope", "row_group_rows"},
        path="output.artifact.physical_layout",
    )
    profile = _required_string(
        physical_layout.get("profile"), path="output.artifact.physical_layout.profile"
    )
    adaptation_scope = _required_string(
        physical_layout.get("adaptation_scope"),
        path="output.artifact.physical_layout.adaptation_scope",
    )
    row_group_rows = physical_layout.get("row_group_rows", "auto")
    if row_group_rows != "auto" and (
        not isinstance(row_group_rows, int)
        or isinstance(row_group_rows, bool)
        or row_group_rows < 1
    ):
        raise ValidationError(
            "output.artifact.physical_layout.row_group_rows must be auto or an integer >= 1.",
            code="output.invalid_physical_layout_row_group_rows",
            context={
                "asset_code": asset_code,
                "path": "output.artifact.physical_layout.row_group_rows",
                "value": row_group_rows,
            },
        )
    allowed_scopes = (
        {"generation_fixed"}
        if asset_code in {"0201", "0301"}
        else {"generation_fixed", "task_adaptive"}
    )
    if adaptation_scope not in allowed_scopes:
        raise ValidationError(
            f"output.artifact.physical_layout.adaptation_scope is invalid for Asset {asset_code}.",
            code="output.invalid_physical_layout_scope",
            context={"asset_code": asset_code, "allowed": sorted(allowed_scopes)},
        )
    logging = _output_auxiliary(output.get("logging"), path="output.logging")
    from smoking_data.runtime.object_store.config import PublicationSpec

    publication = PublicationSpec.from_mapping(artifact.get("publication"))
    return {
        "artifact": {
            "type": expected_type,
            "root_dir": root_dir,
            "format": "parquet",
            "compression": compression,
            "write_policy": write_policy,
            "physical_layout": {
                "profile": profile,
                "adaptation_scope": adaptation_scope,
                "row_group_rows": row_group_rows,
            },
            **(
                {"publication": dict(artifact["publication"])}
                if publication is not None
                else {}
            ),
        },
        "logging": logging,
    }


def _output_auxiliary(value: Any, *, path: str) -> dict[str, str]:
    section = _mapping(value, path=path)
    _reject_unknown(section, {"root_dir"}, path=path)
    return {"root_dir": _required_string(section.get("root_dir"), path=f"{path}.root_dir")}


def _lower_operations(
    indexed: dict[str, dict[str, Any]],
    ordered_ids: list[str],
    dependencies: dict[str, set[str]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for operation_id in ordered_ids:
        operation = dict(indexed[operation_id])
        operation.pop("alias", None)
        kind = str(operation["op"])
        inputs = dict(operation.pop("inputs", {}) or {})
        if kind in {"define_dataset", "define_asset", "define_combined"}:
            continue
        if kind == "save_dataset":
            result.append(
                {
                    "id": operation_id,
                    "op": "write_dataset",
                    "sink": operation_id,
                    "partition_by": list(operation.get("partition_by") or []),
                }
            )
            continue
        operation["id"] = operation_id
        operation["inputs"] = inputs
        if kind == "build_sidecar":
            operation["source"] = _unique_source_ancestor(
                str(inputs["source"]), indexed, dependencies
            )
        elif kind == "active_row_selection":
            operation["sidecar"] = str(inputs["sidecar"])
        elif kind == "materialize":
            operation["source"] = _unique_source_ancestor(
                str(inputs["source"]), indexed, dependencies
            )
            if inputs.get("coordinates"):
                operation["coordinates_from"] = str(inputs["coordinates"])
                operation.pop("source", None)
        elif kind == "join":
            operation["right_source"] = _unique_source_ancestor(
                str(inputs["right"]), indexed, dependencies
            )
        operation.pop("inputs", None)
        result.append(operation)
    return result


def _unique_source_ancestor(
    operation_id: str,
    indexed: dict[str, dict[str, Any]],
    dependencies: dict[str, set[str]],
) -> str:
    candidates = {
        item
        for item in (_ancestors(operation_id, dependencies) | {operation_id})
        if indexed[item]["op"] in {"define_dataset", "define_asset", "define_combined"}
    }
    if len(candidates) != 1:
        raise ValidationError(
            "Input must resolve to exactly one define_asset/define_dataset ancestor.",
            code="dag.source_ancestor_ambiguous",
            context={"operation_id": operation_id, "candidates": sorted(candidates)},
        )
    return next(iter(candidates))


def _stable_topological_sort(
    dependencies: dict[str, set[str]], positions: dict[str, int]
) -> list[str]:
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
    if len(result) != len(remaining):
        cyclic = sorted(set(remaining).difference(result), key=positions.get)
        raise ValidationError(
            "Pipeline operation graph contains a cycle.",
            code="dag.cycle_detected",
            context={"operation_ids": cyclic},
        )
    return result


def _ancestors(operation_id: str, dependencies: dict[str, set[str]]) -> set[str]:
    result: set[str] = set()
    pending = list(dependencies.get(operation_id, ()))
    while pending:
        current = pending.pop()
        if current in result:
            continue
        result.add(current)
        pending.extend(dependencies.get(current, ()))
    return result


def _require_upstream_kind(
    indexed: dict[str, dict[str, Any]],
    upstream_value: Any,
    allowed: set[str],
    operation_id: str,
    port: str,
) -> None:
    upstream = str(upstream_value or "")
    if upstream in indexed and str(indexed[upstream]["op"]) not in allowed:
        raise ValidationError(
            "Input port references an incompatible operation.",
            code="dag.input_artifact_mismatch",
            context={
                "operation_id": operation_id,
                "input_port": port,
                "upstream_operation_id": upstream,
                "allowed_operations": sorted(allowed),
            },
        )


def _expected_input_artifact(kind: str, port: str) -> str:
    if kind == "active_row_selection" and port == "sidecar":
        return "sidecar"
    if kind == "materialize" and port == "coordinates":
        return "coordinates"
    return "dataset"


def _output_artifact(kind: str) -> str:
    if kind == "build_sidecar":
        return "sidecar"
    if kind == "active_row_selection":
        return "coordinates"
    if kind == "save_dataset":
        return "terminal_receipt"
    return "dataset"


def _mapping(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(
            "Expected a mapping.", code="yaml.invalid_type", context={"path": path}
        )
    return value


def _required_string(value: Any, *, path: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValidationError(
            "Required string is missing.", code="yaml.required_key", context={"path": path}
        )
    return result


def _string_list(value: Any, *, path: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValidationError(
            "Required string list is missing.",
            code="yaml.invalid_type",
            context={"path": path},
        )
    return [_required_string(item, path=f"{path}[]") for item in value]


def _reject_unknown(
    value: dict[str, Any], allowed: set[str] | frozenset[str], *, path: str
) -> None:
    unknown = sorted(set(value).difference(allowed))
    if unknown:
        raise ValidationError(
            "Unknown YAML keys.",
            code="yaml.unknown_key",
            context={"path": path, "keys": unknown},
        )
