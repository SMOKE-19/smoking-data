from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from smoking_data.core.exceptions import ValidationError

from .planner import expression_column_references
from .spec import CalculatedFactSpec

if TYPE_CHECKING:
    from .binding import BindingPlan, DependencyBinding


@dataclass(frozen=True, slots=True)
class ExpressionFingerprintSpec:
    name: str
    expression_hash: str
    binding_hash: str
    source_columns: tuple[str, ...]
    constants: tuple[str, ...] = ()


def expression_hash(expression: Mapping[str, Any]) -> str:
    return _sha256(_canonical_json(expression))


def derive_expression_fingerprint_specs(
    spec: CalculatedFactSpec,
    ir_document: Mapping[str, Any],
    binding_plan: BindingPlan,
) -> tuple[ExpressionFingerprintSpec, ...]:
    """Resolve expressions to transitive physical inputs without row-state hashes."""
    from .external_files import load_column_alias_registry

    binding_by_name = {item.logical_name: item for item in binding_plan.bindings}
    lookup_by_alias = {item.alias: item for item in spec.lookup_files}
    alias_registry = load_column_alias_registry(spec.column_alias_files)
    resolved: dict[str, tuple[set[str], set[str], list[dict[str, str | None]]]] = {}
    results: list[ExpressionFingerprintSpec] = []
    for expression in _expressions(ir_document):
        name = str(expression.get("name") or "")
        node = expression.get("expr")
        if not name or not isinstance(node, Mapping):
            _fail(
                "expression.invalid_ir",
                "Expression fingerprinting requires a named expression node.",
            )
        source_columns: set[str] = set()
        constants: set[str] = set()
        local_bindings: list[dict[str, str | None]] = []
        for dependency in expression_column_references(node):
            if dependency in resolved:
                prior_columns, prior_constants, prior_bindings = resolved[dependency]
                source_columns.update(prior_columns)
                constants.update(prior_constants)
                local_bindings.extend(prior_bindings)
                local_bindings.append(
                    {
                        "logical_name": dependency,
                        "kind": "expression",
                        "physical_name": dependency,
                        "lookup_alias": None,
                    }
                )
                continue
            binding = binding_by_name.get(dependency)
            if binding is None:
                _fail(
                    "expression.unresolved_dependency",
                    "Expression dependency is absent from the binding plan.",
                    expression=name,
                    dependency=dependency,
                )
            _add_binding_inputs(
                binding,
                lookup_by_alias=lookup_by_alias,
                alias_registry=alias_registry,
                source_columns=source_columns,
                constants=constants,
            )
            local_bindings.append(
                {
                    "logical_name": binding.logical_name,
                    "kind": binding.kind,
                    "physical_name": binding.physical_name,
                    "lookup_alias": binding.lookup_alias,
                }
            )
        unique_bindings = list(
            {_canonical_json(item): item for item in local_bindings}.values()
        )
        resolved[name] = (source_columns, constants, unique_bindings)
        results.append(
            ExpressionFingerprintSpec(
                name=name,
                expression_hash=expression_hash(expression),
                binding_hash=_sha256(
                    _canonical_json(
                        {
                            "bindings": unique_bindings,
                            "contract_version": "smoking-data.expression-binding.v1",
                        }
                    )
                ),
                source_columns=tuple(sorted(source_columns)),
                constants=tuple(sorted(constants)),
            )
        )
    return tuple(results)


def _add_binding_inputs(
    binding: DependencyBinding,
    *,
    lookup_by_alias: Mapping[str, Any],
    alias_registry: Mapping[str, str],
    source_columns: set[str],
    constants: set[str],
) -> None:
    if binding.kind in {"source", "virtual_alias"}:
        source_columns.add(binding.physical_name)
        return
    if binding.kind != "lookup" or binding.lookup_alias is None:
        _fail(
            "expression.invalid_binding",
            "Unsupported dependency binding in expression fingerprint plan.",
            dependency=binding.logical_name,
            kind=binding.kind,
        )
    lookup = lookup_by_alias[binding.lookup_alias]
    source_columns.update(alias_registry.get(key, key) for key in lookup.source_keys)
    constants.add(
        f"lookup:{lookup.alias}:{binding.physical_name}:sha256:{lookup.checksum}"
    )


def _expressions(ir_document: Mapping[str, Any]):
    for layer in ir_document.get("layers") or []:
        if isinstance(layer, Mapping):
            for expression in layer.get("expressions") or []:
                if isinstance(expression, Mapping):
                    yield expression


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _fail(code: str, message: str, **context: object) -> None:
    raise ValidationError(message, code=code, context=context)
