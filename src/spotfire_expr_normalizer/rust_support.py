from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .catalog import catalog_db_path

RUST_EXPLICIT_UNSUPPORTED: dict[str, str] = {
    "autobinnumeric": "Binning requires planner-visible boundaries and cross-part state.",
    "binbyevendistance": "Binning requires planner-visible boundaries and cross-part state.",
    "binbyevendistribution": "Distribution binning requires a bounded global quantile phase.",
    "binbyevenintervals": "Binning requires planner-visible boundaries and cross-part state.",
    "binbyspecificlimits": "Specific-limit label and boundary semantics are not fixed for IR v1.",
    "chidist": "Statistical distribution functions require an audited numerical crate.",
    "chiinv": "Statistical distribution functions require an audited numerical crate.",
    "fdist": "Statistical distribution functions require an audited numerical crate.",
    "finv": "Statistical distribution functions require an audited numerical crate.",
    "normdist": "Statistical distribution functions require an audited numerical crate.",
    "norminv": "Statistical distribution functions require an audited numerical crate.",
    "tdist": "Statistical distribution functions require an audited numerical crate.",
    "tinv": "Statistical distribution functions require an audited numerical crate.",
    "greatcircledistance": "Spatial functions require an explicit coordinate-system contract.",
    "namedecode": "Spotfire name codec semantics are not documented enough for parity.",
    "nameencode": "Spotfire name codec semantics are not documented enough for parity.",
    "wkbenvelopexcenter": "WKB functions require a geometry crate and invalid-geometry policy.",
    "wkbenvelopexmax": "WKB functions require a geometry crate and invalid-geometry policy.",
    "wkbenvelopexmin": "WKB functions require a geometry crate and invalid-geometry policy.",
    "wkbenvelopeycenter": "WKB functions require a geometry crate and invalid-geometry policy.",
    "wkbenvelopeymax": "WKB functions require a geometry crate and invalid-geometry policy.",
    "wkbenvelopeymin": "WKB functions require a geometry crate and invalid-geometry policy.",
    "datetimenow": "Nondeterministic wall-clock access is forbidden without an execution timestamp.",
    "today": "Nondeterministic wall-clock access is forbidden without an execution date.",
    "parsetimespan": "Accepted Spotfire duration string formats are not fixed for IR v1.",
    "rank": "Rank tie and ordering semantics are ambiguous; use DenseRank, RankReal or RowNumber.",
}

# These documented Spotfire names compile to typed cast nodes or another canonical call.
RUST_COMPILER_ONLY_ALIASES = frozenset(
    {
        "boolean",
        "cast",
        "countbig",
        "currency",
        "date",
        "datetime",
        "integer",
        "longinteger",
        "real",
        "singlereal",
        "string",
        "time",
    }
)


@dataclass(frozen=True, slots=True)
class RustFunctionCoverage:
    documented: frozenset[str]
    supported: frozenset[str]
    unsupported: dict[str, str]


def rust_function_coverage() -> RustFunctionCoverage:
    with sqlite3.connect(catalog_db_path()) as connection:
        documented = frozenset(
            str(row[0]).lower()
            for row in connection.execute("SELECT DISTINCT normalized_name FROM spotfire_functions")
        )
    unsupported = {
        name: reason for name, reason in RUST_EXPLICIT_UNSUPPORTED.items() if name in documented
    }
    return RustFunctionCoverage(
        documented=documented,
        supported=documented.difference(unsupported),
        unsupported=unsupported,
    )


def validate_rust_ir_function_support(document: dict[str, object]) -> None:
    alternatives = {
        "rank": "Use DenseRank or RankReal with explicit partition keys.",
        "datetimenow": "Inject a deterministic execution timestamp column.",
        "today": "Inject a deterministic execution date column.",
        "parsetimespan": "Cast a typed duration or use TimeSpan(day,hour,minute,second,millisecond).",
    }

    def calls(value: object):
        if isinstance(value, dict):
            if value.get("kind") == "call":
                yield str(value.get("function") or "").lower()
            for child in value.values():
                yield from calls(child)
        elif isinstance(value, list):
            for child in value:
                yield from calls(child)

    for layer in document.get("layers") or []:
        if not isinstance(layer, dict):
            continue
        for expression in layer.get("expressions") or []:
            if not isinstance(expression, dict):
                continue
            for function in calls(expression.get("expr")):
                reason = RUST_EXPLICIT_UNSUPPORTED.get(function)
                if reason:
                    alternative = alternatives.get(
                        function,
                        "No automatic Python, DuckDB, or Polars fallback is available.",
                    )
                    raise ValueError(
                        "Rust expression function is explicitly unsupported: "
                        f"expression={expression.get('name')!r}, "
                        f"source={expression.get('source')!r}, function={function!r}, "
                        f"reason={reason} suggested_next={alternative}"
                    )
