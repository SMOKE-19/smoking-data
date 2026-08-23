from __future__ import annotations

import re
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from smoking_data.core.exceptions import ValidationError

PAYLOAD_ENGINE = "rust"
TASK_CONTRACT_VERSION = "smoking-data.payload-task.v2"
EXPRESSION_IR_VERSION = "spotfire-expression-ir.v1"
RUST_PACKAGE_DISTRIBUTION = "smoking-data"
EXPRESSION_COMPILER_DISTRIBUTION = "smoking-data"

RUST_DIRECT_CAST_TYPES = frozenset(
    {
        "TEXT",
        "STRING",
        "INT8",
        "INT16",
        "INT32",
        "INT64",
        "BIGINT",
        "FLOAT",
        "FLOAT32",
        "FLOAT64",
        "DOUBLE",
        "DATE",
        "TIME",
        "DATETIME",
        "DURATION",
        "BOOL",
        "BOOLEAN",
    }
)


def rust_package_version() -> str:
    try:
        return version(RUST_PACKAGE_DISTRIBUTION)
    except PackageNotFoundError:
        try:
            from smoking_data_engine_rs import __version__
        except (ImportError, AttributeError):
            return "unknown"
        return str(__version__)


def expression_compiler_version() -> str:
    try:
        return version(EXPRESSION_COMPILER_DISTRIBUTION)
    except PackageNotFoundError:
        try:
            from smoking_data import __version__
        except (ImportError, AttributeError):
            return "unknown"
        return str(__version__)


def validate_rust_payload_contract(
    payload: dict[str, Any],
    *,
    list_restore: dict[str, Any] | None = None,
    expression_ir: dict[str, Any] | None = None,
) -> None:
    add_calc = payload.get("add_calc") or []
    if add_calc and expression_ir is None:
        item = add_calc[0] if isinstance(add_calc[0], dict) else {}
        name = str(item.get("name") or "<unnamed>")
        expression = str(item.get("spotfire_expression") or item.get("sql") or "<missing>")
        raise ValidationError(
            "Rust payload validation failed before task creation: "
            f"expression={name!r}, source={expression!r}, "
            "ir_node='<not-compiled>', "
            "reason=typed expression IR execution is not implemented."
        )

    for item in payload.get("type_casts") or []:
        if not isinstance(item, dict):
            continue
        target_type = str(item.get("type") or "").upper()
        decimal_supported = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", target_type.replace(" ", ""))
        if target_type not in RUST_DIRECT_CAST_TYPES and decimal_supported is None:
            name = str(item.get("name") or item.get("column") or "<missing>")
            raise ValidationError(
                "Rust payload validation failed before task creation: "
                f"op='cast', column={name!r}, target_type={target_type!r}, "
                "reason=target type is not supported by the Rust payload engine."
            )


def engine_metadata(*, expression_ir_hash: str | None = None) -> dict[str, str | None]:
    return {
        "payload_engine": PAYLOAD_ENGINE,
        "payload_engine_version": rust_package_version(),
        "rust_package": RUST_PACKAGE_DISTRIBUTION,
        "task_contract_version": TASK_CONTRACT_VERSION,
        "expression_ir_version": EXPRESSION_IR_VERSION,
        "expression_compiler_version": expression_compiler_version(),
        "expression_ir_hash": expression_ir_hash,
    }
