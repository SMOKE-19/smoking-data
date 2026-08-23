from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, replace

from smoking_data.core.logical_plan import (
    LogicalOperationPlan,
    resolve_column_lineage,
)
from smoking_data.core.operations import OperationKind, OperationSpec

OPTIMIZER_VERSION = "smoking-data.optimizer.v1"


@dataclass(frozen=True, slots=True)
class RewriteDecision:
    rule_id: str
    applied: bool
    reason: str
    before: tuple[str, ...]
    after: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OptimizationResult:
    canonical_plan: LogicalOperationPlan
    optimized_plan: LogicalOperationPlan
    candidate_plans: tuple[LogicalOperationPlan, ...]
    trace: tuple[RewriteDecision, ...]
    explored_plans: int
    version: str = OPTIMIZER_VERSION

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "canonical_plan_hash": self.canonical_plan.plan_hash,
            "optimized_plan_hash": self.optimized_plan.plan_hash,
            "candidate_plan_hashes": [plan.plan_hash for plan in self.candidate_plans],
            "changed": self.canonical_plan.plan_hash != self.optimized_plan.plan_hash,
            "explored_plans": self.explored_plans,
            "trace": [asdict(item) for item in self.trace],
        }


Rule = Callable[[tuple[OperationSpec, ...]], tuple[tuple[OperationSpec, ...], str] | None]


@dataclass(frozen=True, slots=True)
class RewriteRuleInfo:
    rule_id: str
    tier: str
    status: str
    required_properties: tuple[str, ...]
    counterexample: str


RULE_CATALOG: tuple[RewriteRuleInfo, ...] = (
    RewriteRuleInfo(
        "tier_a.remove_identity_projection",
        "A",
        "supported",
        ("row_preserving",),
        "projection removes or reorders a column",
    ),
    RewriteRuleInfo(
        "tier_a.merge_adjacent_filters",
        "A",
        "supported",
        ("deterministic", "total", "same_schema"),
        "a predicate raises before the other filters its bad row",
    ),
    RewriteRuleInfo(
        "tier_a.merge_independent_add_calc",
        "A",
        "supported",
        ("row_local", "independent_aliases"),
        "one expression reads the other expression alias",
    ),
    RewriteRuleInfo(
        "tier_a.remove_duplicate_cast",
        "A",
        "supported",
        ("deterministic", "identical_target"),
        "the first cast changes the second cast input type",
    ),
    RewriteRuleInfo(
        "tier_b.filter_before_projection",
        "B",
        "conditional",
        ("predicate_columns_preserved",),
        "projection removes a predicate column",
    ),
    RewriteRuleInfo(
        "tier_b.filter_before_independent_add_calc",
        "B",
        "conditional",
        ("predicate_alias_independent",),
        "predicate reads the calculated alias",
    ),
    RewriteRuleInfo(
        "tier_b.filter_before_independent_cast",
        "B",
        "conditional",
        ("predicate_cast_independent", "total", "order_preserving"),
        "cast can fail or predicate reads the casted column",
    ),
    RewriteRuleInfo(
        "barrier.outer_join",
        "barrier",
        "blocked",
        ("explicit_equivalence_proof",),
        "outer join preserves unmatched rows",
    ),
    RewriteRuleInfo(
        "barrier.list_shape",
        "barrier",
        "blocked",
        ("explicit_equivalence_proof",),
        "restore/explode/nest changes element cardinality",
    ),
    RewriteRuleInfo(
        "barrier.group_order",
        "barrier",
        "blocked",
        ("complete_group", "stable_tie_break"),
        "sort-first/window/pivot depends on full ordered groups",
    ),
    RewriteRuleInfo(
        "barrier.side_effect",
        "barrier",
        "blocked",
        ("never_reorder",),
        "write or sync is externally observable",
    ),
    RewriteRuleInfo(
        "barrier.unknown",
        "barrier",
        "blocked",
        ("registered_properties",),
        "unknown null/cardinality semantics",
    ),
)


def render_rule_matrix_markdown() -> str:
    lines = [
        "| rule_id | tier | status | required properties | counterexample |",
        "| --- | --- | --- | --- | --- |",
    ]
    lines.extend(
        "| "
        + " | ".join(
            (
                item.rule_id,
                item.tier,
                item.status,
                ", ".join(item.required_properties),
                item.counterexample,
            )
        )
        + " |"
        for item in RULE_CATALOG
    )
    return "\n".join(lines) + "\n"


def optimize_logical_plan(
    plan: LogicalOperationPlan,
    *,
    enabled: bool = True,
    max_iterations: int = 8,
    max_plans: int = 32,
) -> OptimizationResult:
    if not enabled:
        return OptimizationResult(plan, plan, (plan,), (), 1)
    current = plan.operations
    seen = {_operation_signature(current)}
    candidate_operations = [current]
    trace: list[RewriteDecision] = []
    rules: tuple[tuple[str, Rule], ...] = (
        ("tier_a.remove_identity_projection", _remove_identity_projection),
        ("tier_a.merge_adjacent_filters", _merge_adjacent_filters),
        ("tier_a.merge_independent_add_calc", _merge_independent_add_calc),
        ("tier_a.remove_duplicate_cast", _remove_duplicate_cast),
        ("tier_b.filter_before_projection", _filter_before_projection),
        ("tier_b.filter_before_independent_add_calc", _filter_before_independent_add_calc),
        ("tier_b.filter_before_independent_cast", _filter_before_independent_cast),
    )
    for _ in range(max(1, max_iterations)):
        changed = False
        for rule_id, rule in rules:
            result = rule(current)
            if result is None:
                continue
            candidate, reason = result
            signature = _operation_signature(candidate)
            before_ids = tuple(item.operation_id for item in current)
            after_ids = tuple(item.operation_id for item in candidate)
            if signature in seen:
                trace.append(
                    RewriteDecision(rule_id, False, "memoized_plan", before_ids, after_ids)
                )
                continue
            if len(seen) >= max_plans:
                trace.append(
                    RewriteDecision(rule_id, False, "plan_count_limit", before_ids, before_ids)
                )
                return _result(plan, current, candidate_operations, trace, len(seen))
            _validate_rewrite(current, candidate)
            seen.add(signature)
            candidate_operations.append(candidate)
            trace.append(RewriteDecision(rule_id, True, reason, before_ids, after_ids))
            current = candidate
            changed = True
            break
        if not changed:
            break
    return _result(plan, current, candidate_operations, trace, len(seen))


def is_fixed_barrier(operation: OperationSpec) -> bool:
    properties = operation.properties
    return (
        properties.side_effect
        or properties.requires_complete_group
        or properties.order_sensitive
        or not properties.deterministic
        or operation.kind
        in {
            OperationKind.REFERENCE_REPLACE,
            OperationKind.LIST_RESTORE,
            OperationKind.SORT_FIRST,
            OperationKind.PIVOT,
            OperationKind.JOIN,
            OperationKind.PARTITION_WRITE,
        }
    )


def _result(
    plan: LogicalOperationPlan,
    operations: tuple[OperationSpec, ...],
    candidate_operations: list[tuple[OperationSpec, ...]],
    trace: list[RewriteDecision],
    explored: int,
) -> OptimizationResult:
    candidates = tuple(
        plan
        if _operation_signature(items) == _operation_signature(plan.operations)
        else _logical_plan_from_operations(plan, items)
        for items in candidate_operations
    )
    optimized = (
        plan
        if _operation_signature(operations) == _operation_signature(plan.operations)
        else _logical_plan_from_operations(plan, operations)
    )
    return OptimizationResult(plan, optimized, candidates, tuple(trace), explored)


def _logical_plan_from_operations(
    template: LogicalOperationPlan,
    operations: tuple[OperationSpec, ...],
) -> LogicalOperationPlan:
    return LogicalOperationPlan(
        preset=template.preset,
        operations=operations,
        column_lineage=resolve_column_lineage(list(operations)),
        version=template.version,
    )


def _operation_signature(operations: tuple[OperationSpec, ...]) -> tuple[str, ...]:
    return tuple(repr(operation.to_dict()) for operation in operations)


def _validate_rewrite(before: tuple[OperationSpec, ...], after: tuple[OperationSpec, ...]) -> None:
    before_writes = [item for item in before if item.properties.side_effect]
    after_writes = [item for item in after if item.properties.side_effect]
    if [item.operation_id for item in before_writes] != [
        item.operation_id for item in after_writes
    ]:
        raise ValueError("A rewrite must preserve side-effect operations and their order.")
    before_outputs = {output.name for operation in before for output in operation.output_columns}
    after_outputs = {output.name for operation in after for output in operation.output_columns}
    if before_outputs != after_outputs:
        raise ValueError("A rewrite must preserve the set of produced columns.")
    before_barriers = [item.operation_id for item in before if is_fixed_barrier(item)]
    after_barriers = [item.operation_id for item in after if is_fixed_barrier(item)]
    if before_barriers != after_barriers:
        raise ValueError("A rewrite must preserve fixed barrier order.")


def _remove_identity_projection(ops: tuple[OperationSpec, ...]):
    for index, op in enumerate(ops):
        if (
            op.kind is OperationKind.PROJECTION
            and not op.config.get("include")
            and not op.config.get("exclude")
        ):
            return ops[:index] + ops[index + 1 :], "empty projection is an identity"
    return None


def _merge_adjacent_filters(ops: tuple[OperationSpec, ...]):
    for index in range(len(ops) - 1):
        left, right = ops[index : index + 2]
        if (
            left.kind is right.kind is OperationKind.FILTER
            and left.properties.total
            and right.properties.total
        ):
            merged = replace(
                left,
                operation_id=f"{left.operation_id}__{right.operation_id}",
                config={"sql": f"({left.config['sql']}) AND ({right.config['sql']})"},
                input_columns=tuple(dict.fromkeys([*left.input_columns, *right.input_columns])),
            )
            return ops[:index] + (merged,) + ops[
                index + 2 :
            ], "deterministic total filters share a schema"
    return None


def _merge_independent_add_calc(ops: tuple[OperationSpec, ...]):
    for index in range(len(ops) - 1):
        left, right = ops[index : index + 2]
        if left.kind is right.kind is OperationKind.ADD_CALC:
            left_outputs = {item.name for item in left.output_columns}
            right_outputs = {item.name for item in right.output_columns}
            if left_outputs & set(right.input_columns) or right_outputs & set(left.input_columns):
                continue
            merged = replace(
                left,
                operation_id=f"{left.operation_id}__{right.operation_id}",
                config={
                    "expressions": [
                        *(left.config.get("expressions") or []),
                        *(right.config.get("expressions") or []),
                    ]
                },
                input_columns=tuple(dict.fromkeys([*left.input_columns, *right.input_columns])),
                output_columns=left.output_columns + right.output_columns,
                alias_lineage={**left.alias_lineage, **right.alias_lineage},
            )
            return ops[:index] + (merged,) + ops[index + 2 :], "independent row-local expressions"
    return None


def _remove_duplicate_cast(ops: tuple[OperationSpec, ...]):
    for index in range(len(ops) - 1):
        left, right = ops[index : index + 2]
        if left.kind is right.kind is OperationKind.TYPE_CAST and left.config == right.config:
            return ops[: index + 1] + ops[index + 2 :], "identical deterministic cast"
    return None


def _filter_before_projection(ops: tuple[OperationSpec, ...]):
    for index in range(len(ops) - 1):
        projection, filtering = ops[index : index + 2]
        if (
            projection.kind is not OperationKind.PROJECTION
            or filtering.kind is not OperationKind.FILTER
        ):
            continue
        include = set(projection.config.get("include") or [])
        exclude = set(projection.config.get("exclude") or [])
        required = set(filtering.input_columns)
        if (include and not required.issubset(include)) or required & exclude:
            continue
        return ops[:index] + (filtering, projection) + ops[
            index + 2 :
        ], "predicate columns survive projection"
    return None


def _filter_before_independent_add_calc(ops: tuple[OperationSpec, ...]):
    for index in range(len(ops) - 1):
        calc, filtering = ops[index : index + 2]
        if calc.kind is not OperationKind.ADD_CALC or filtering.kind is not OperationKind.FILTER:
            continue
        aliases = {item.name for item in calc.output_columns}
        if aliases & set(filtering.input_columns):
            continue
        return ops[:index] + (filtering, calc) + ops[
            index + 2 :
        ], "predicate does not depend on calculated aliases"
    return None


def _filter_before_independent_cast(ops: tuple[OperationSpec, ...]):
    for index in range(len(ops) - 1):
        casting, filtering = ops[index : index + 2]
        if (
            casting.kind is not OperationKind.TYPE_CAST
            or filtering.kind is not OperationKind.FILTER
        ):
            continue
        if not (casting.properties.total and bool(casting.config.get("order_preserving", False))):
            continue
        casted = {item.name for item in casting.output_columns}
        if casted & set(filtering.input_columns):
            continue
        return ops[:index] + (filtering, casting) + ops[
            index + 2 :
        ], "predicate does not read casted columns"
    return None
