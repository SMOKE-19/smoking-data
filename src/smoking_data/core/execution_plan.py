from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from smoking_data.core.logical_plan import LogicalOperationPlan

EXECUTION_PLAN_VERSION = "smoking-data.operation-execution-plan.v1"


@dataclass(frozen=True, slots=True)
class ExecutableOperation:
    operation_id: str
    kind: str
    config: dict[str, Any]
    input_schema: tuple[dict[str, Any], ...]
    output_schema: tuple[dict[str, Any], ...]
    alias_lineage: dict[str, tuple[str, ...]]
    requires_complete_group: bool
    side_effect: bool
    physical_role: str
    barriers: tuple[str, ...]
    dependency_references: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OperationExecutionPlan:
    logical_plan_hash: str
    physical_plan_hash: str | None
    operations: tuple[ExecutableOperation, ...]
    expression_ir_hashes: dict[str, str]
    backend_versions: dict[str, str]
    version: str = EXECUTION_PLAN_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {**self._payload_dict(), "plan_hash": self.plan_hash}

    def _payload_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def plan_hash(self) -> str:
        payload = json.dumps(
            self._payload_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


def compile_execution_plan(
    logical_plan: LogicalOperationPlan,
    *,
    physical_plan_hash: str | None = None,
    backend_versions: dict[str, str] | None = None,
) -> OperationExecutionPlan:
    operations = tuple(
        ExecutableOperation(
            operation_id=operation.operation_id,
            kind=operation.kind.value,
            config=operation.config,
            input_schema=tuple(
                {
                    "name": column.name,
                    "dtype": column.dtype,
                    "nullable": column.nullable,
                }
                for column in operation.input_contracts
            ),
            output_schema=tuple(
                {
                    "name": column.name,
                    "dtype": column.dtype,
                    "nullable": column.nullable,
                }
                for column in operation.output_columns
            ),
            alias_lineage=dict(operation.alias_lineage),
            requires_complete_group=operation.properties.requires_complete_group,
            side_effect=operation.properties.side_effect,
            physical_role=_physical_role(operation.kind.value),
            barriers=_operation_barriers(operation),
            dependency_references=_dependency_references(operation.config),
        )
        for operation in logical_plan.operations
    )
    expression_hashes: dict[str, str] = {}
    for operation in logical_plan.operations:
        expression_ir = operation.config.get("expression_ir")
        if expression_ir is not None:
            encoded = json.dumps(
                expression_ir, sort_keys=True, separators=(",", ":"), ensure_ascii=True
            ).encode("utf-8")
            expression_hashes[operation.operation_id] = hashlib.sha256(encoded).hexdigest()
    return OperationExecutionPlan(
        logical_plan_hash=logical_plan.plan_hash,
        physical_plan_hash=physical_plan_hash,
        operations=operations,
        expression_ir_hashes=expression_hashes,
        backend_versions=dict(sorted((backend_versions or {}).items())),
    )


def _physical_role(kind: str) -> str:
    if kind == "build_sidecar":
        return "planner_sidecar_barrier"
    if kind == "active_row_selection":
        return "planner_selector_barrier"
    if kind == "materialize":
        return "payload_materialize_barrier"
    if kind == "filter":
        return "selector_prefix"
    if kind == "data_assertion":
        return "validation_barrier"
    if kind == "write_dataset":
        return "write_suffix"
    return "payload_body"


def _operation_barriers(operation: Any) -> tuple[str, ...]:
    barriers = []
    if operation.properties.requires_complete_group:
        barriers.append("complete_group")
    if operation.properties.side_effect:
        barriers.append("side_effect")
    if operation.kind.value == "data_assertion":
        barriers.extend(["global_dataset_scan", "commit_precondition"])
    if operation.kind.value in {"pivot", "join", "unpivot"}:
        barriers.append("cardinality_change")
    if operation.kind.value == "active_row_selection":
        barriers.extend(["coordinate_snapshot", "cardinality_change"])
    if operation.kind.value == "build_sidecar":
        barriers.extend(["thin_index", "source_span_boundary"])
    if operation.kind.value == "materialize":
        barriers.extend(["payload_read", "process_lifetime", "dataset_staging"])
    return tuple(barriers)


def _dependency_references(config: dict[str, Any]) -> tuple[str, ...]:
    references = []
    for key in (
        "reference_parquet",
        "lookup_path",
        "right_source",
        "sidecar",
        "coordinates_from",
        "source",
    ):
        value = config.get(key)
        if value:
            references.append(str(value))
    nested = config.get("config")
    if isinstance(nested, dict):
        for key in ("reference_parquet", "lookup_path"):
            if nested.get(key):
                references.append(str(nested[key]))
    return tuple(dict.fromkeys(references))
