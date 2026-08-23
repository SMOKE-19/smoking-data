from __future__ import annotations

import csv
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from smoking_data.core.exceptions import ValidationError
from spotfire_expr_normalizer import (
    build_raw_expressions,
    compile_expressions_to_ir,
    validate_rust_ir_function_support,
)

from .spec import ColumnAliasFileSpec, ExpressionFileSpec


def compile_expression_file(file: ExpressionFileSpec) -> dict[str, Any]:
    name_field = file.column_name_field
    expression_field = file.expression_field
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for row_number, row in enumerate(
        project_rows(file.path, (name_field, expression_field)), start=2
    ):
        name = str(row.get(name_field) or "").strip()
        expression = str(row.get(expression_field) or "").strip()
        if not name or not expression:
            _fail(
                "expression_file.missing_field",
                "Expression name and expression text must not be blank.",
                path=file.path,
                row_number=row_number,
            )
        if name in seen:
            _fail(
                "expression_file.duplicate_output",
                "Expression output name is duplicated.",
                path=file.path,
                row_number=row_number,
                output=name,
            )
        seen.add(name)
        pairs.append((name, expression))
    if not pairs:
        _fail("expression_file.missing_field", "Expression file contains no expressions.")
    try:
        document = compile_expressions_to_ir(build_raw_expressions(pairs)).to_dict()
        validate_rust_ir_function_support(document)
    except ValidationError:
        raise
    except (TypeError, ValueError) as exc:
        _fail(
            "expression.invalid_source",
            "Expression file could not be compiled to canonical IR.",
            path=file.path,
            reason=str(exc),
        )
    return document


def load_column_alias_registry(files: Sequence[ColumnAliasFileSpec]) -> dict[str, str]:
    """Return logical alias -> physical source while allowing source -> alias N."""

    registry: dict[str, str] = {}
    pairs: set[tuple[str, str]] = set()
    for file in files:
        source_field = file.source_column_field
        alias_field = file.alias_column_field
        for row_number, row in enumerate(
            project_rows(file.path, (source_field, alias_field)), start=2
        ):
            source = str(row.get(source_field) or "").strip()
            alias = str(row.get(alias_field) or "").strip()
            if not source or not alias:
                _fail(
                    "column_alias.invalid_binding",
                    "Column alias source and alias must not be blank.",
                    path=file.path,
                    row_number=row_number,
                )
            pair = (source, alias)
            if pair in pairs:
                _fail(
                    "column_alias.invalid_binding",
                    "Column alias pair is duplicated.",
                    path=file.path,
                    source=source,
                    alias=alias,
                )
            pairs.add(pair)
            existing = registry.get(alias)
            if existing is not None and existing != source:
                _fail(
                    "column_alias.invalid_binding",
                    "One logical alias cannot bind to multiple physical columns.",
                    alias=alias,
                    sources=sorted({existing, source}),
                )
            registry[alias] = source
    return registry


def project_rows(path: Path, fields: Sequence[str]) -> Iterator[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        yield from _csv_rows(path, fields)
        return
    if path.suffix.lower() == ".parquet":
        yield from _parquet_rows(path, fields)
        return
    _fail(
        "external_file.unsupported_extension",
        "External file must use .csv or .parquet.",
        path=path,
    )


def _csv_rows(path: Path, fields: Sequence[str]) -> Iterator[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            available = set(reader.fieldnames or [])
            _require_fields(path, fields, available)
            for row in reader:
                yield {field: row.get(field) for field in fields}
    except UnicodeDecodeError as exc:
        _fail(
            "external_file.format_mismatch",
            "CSV external file is not valid UTF-8.",
            path=path,
            reason=str(exc),
        )


def _parquet_rows(path: Path, fields: Sequence[str]) -> Iterator[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(path)
        _require_fields(path, fields, set(parquet.schema_arrow.names))
        for batch in parquet.iter_batches(batch_size=65_536, columns=list(fields)):
            for row in batch.to_pylist():
                yield row
    except ValidationError:
        raise
    except Exception as exc:
        _fail(
            "external_file.format_mismatch",
            "Parquet external file could not be read.",
            path=path,
            reason=str(exc),
        )


def _require_fields(path: Path, required: Sequence[str], available: set[str]) -> None:
    missing = sorted(set(required) - available)
    if missing:
        _fail(
            "external_file.missing_field",
            "External file is missing required fields.",
            path=path,
            fields=missing,
        )


def _fail(code: str, message: str, **context: Any) -> None:
    normalized = {key: str(value) if isinstance(value, Path) else value for key, value in context.items()}
    raise ValidationError(message, code=code, context=normalized)
