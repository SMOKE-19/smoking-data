from __future__ import annotations

from dataclasses import dataclass

from .normalizer import DEFAULT_WINDOW_AGGREGATES
from .rust_support import rust_function_coverage


@dataclass(frozen=True, slots=True)
class FunctionSemantics:
    canonical_name: str
    category: str
    null_policy: str
    deterministic: bool
    rust_supported: bool
    unsupported_reason: str | None = None


def canonical_function_semantics() -> dict[str, FunctionSemantics]:
    nondeterministic = {"datetimenow", "rand", "randbetween"}
    aggregate = {
        "avg",
        "average",
        "count",
        "max",
        "median",
        "min",
        "sum",
        "uniquecount",
    }
    coverage = rust_function_coverage()
    result: dict[str, FunctionSemantics] = {}
    for name in sorted(coverage.documented):
        category = (
            "window"
            if name in DEFAULT_WINDOW_AGGREGATES
            else "aggregate"
            if name in aggregate
            else "scalar"
        )
        result[name] = FunctionSemantics(
            canonical_name="avg" if name == "average" else name,
            category=category,
            null_policy="function_defined" if category != "scalar" else "propagate",
            deterministic=name not in nondeterministic,
            rust_supported=name in coverage.supported,
            unsupported_reason=coverage.unsupported.get(name),
        )
    return result
