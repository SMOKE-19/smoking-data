from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from smoking_data.backends.rust_engine import CuratedTaskRequest
from smoking_data.core.exceptions import ValidationError

from .external_files import load_column_alias_registry
from .invalidation import InvalidationTaskGroup, subset_expression_ir
from .planning import CalculatedFactRunPlan


def build_long_fact_writer_config(
    plan: CalculatedFactRunPlan,
    *,
    generation_seq: int,
    source_fingerprint: str,
    calculated_at: datetime | None = None,
    expression_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    if generation_seq < 1:
        _fail("append.generation_conflict", "generation_seq must be positive.")
    all_names = [item.name for item in plan.fingerprints]
    expected_names = list(expression_names) if expression_names is not None else all_names
    if not expected_names or len(set(expected_names)) != len(expected_names):
        _fail(
            "incremental.state_mismatch",
            "Long Fact task expression names must be non-empty and unique.",
        )
    unknown = sorted(set(expected_names).difference(all_names))
    if unknown:
        _fail(
            "incremental.state_mismatch",
            "Long Fact task references expressions outside the run plan.",
            expressions=unknown,
        )
    if not source_fingerprint:
        _fail(
            "incremental.state_mismatch",
            "Long Fact task requires a source segment fingerprint.",
        )
    columns: list[dict[str, Any]] = []
    fingerprints_by_name = {item.name: item for item in plan.fingerprints}
    fact_source_names = getattr(plan, "fact_source_names", {})
    for name in expected_names:
        contract = fingerprints_by_name[name]
        columns.append(
            {
                "name": fact_source_names.get(name, name),
                "output_name": name,
                "expression_hash": contract.expression_hash,
                "binding_hash": contract.binding_hash,
                "source_fingerprint": source_fingerprint,
            }
        )
    instant = calculated_at or datetime.now(timezone.utc)
    if instant.tzinfo is None or instant.utcoffset() is None:
        _fail(
            "long_fact.invalid_timestamp",
            "calculated_at must be timezone-aware.",
        )
    calculated_at_us = int(instant.astimezone(timezone.utc).timestamp() * 1_000_000)
    return {
        "contract": "long_fact_v1",
        "identity_columns": list(plan.spec.identity_columns),
        "calculated_columns": columns,
        "generation_seq": generation_seq,
        "calculated_at_us": calculated_at_us,
    }


def build_calculated_fact_task_request(
    plan: CalculatedFactRunPlan,
    *,
    coordinate_path: Path,
    group: InvalidationTaskGroup,
    generation_seq: int,
    output_dir: Path,
    lookup_cache_dir: Path,
    task_index: int,
    batch_size: int | None,
    source_fingerprint: str,
    calculated_at: datetime | None = None,
    available_columns: Sequence[str] | None = None,
) -> CuratedTaskRequest:
    wide_output = plan.output_mode == "wide_calculated_v1"
    return CuratedTaskRequest(
        coordinate_path=coordinate_path,
        output_dir=output_dir,
        output_file_name=f"part-{task_index:06d}.parquet",
        single_partition_guaranteed=False,
        writer_input_contract=None,
        projection_columns=_projection_columns(
            plan,
            group.expression_names,
            available_columns=available_columns,
        ),
        schema={},
        expression_ir=subset_expression_ir(
            plan.expression_ir,
            tuple(
                getattr(plan, "fact_source_names", {}).get(name, name)
                for name in group.expression_names
            ),
        ),
        lookup_enrich=_lookup_enrich_configs(plan, lookup_cache_dir),
        long_fact=(
            None
            if wide_output
            else build_long_fact_writer_config(
                plan,
                generation_seq=generation_seq,
                source_fingerprint=source_fingerprint,
                calculated_at=calculated_at,
                expression_names=group.expression_names,
            )
        ),
        output_columns=[],
        output_projection_columns=(
            _wide_output_projection(
                plan,
                group.expression_names,
                available_columns=available_columns,
            )
            if wide_output
            else []
        ),
        partition_columns=list(plan.spec.partition_by),
        compression=plan.compression,
        output_row_group_rows=plan.output_row_group_rows,
        batch_size=batch_size,
    )


def _projection_columns(
    plan: CalculatedFactRunPlan,
    expression_names: Sequence[str],
    *,
    available_columns: Sequence[str] | None = None,
) -> list[dict[str, str]]:
    fingerprints = {
        item.name: item for item in plan.fingerprints if item.name in set(expression_names)
    }
    required_physical = (
        set(plan.spec.identity_columns)
        .union(plan.spec.partition_by)
        .union(getattr(plan.spec, "include_columns", ()))
    )
    for item in fingerprints.values():
        required_physical.update(item.source_columns)
    available = set(available_columns) if available_columns is not None else None
    result = [
        {"name": name, "source": name}
        for name in plan.binding.source_projection
        if name in required_physical and (available is None or name in available)
    ]
    emitted = {item["name"] for item in result}
    alias_registry = load_column_alias_registry(plan.spec.column_alias_files)
    logical_aliases = {
        item.logical_name: item.physical_name
        for item in plan.binding.bindings
        if item.kind == "virtual_alias"
    }
    for lookup in plan.spec.lookup_files:
        for source_key in lookup.source_keys:
            if source_key in alias_registry:
                logical_aliases[source_key] = alias_registry[source_key]
    for logical, physical in logical_aliases.items():
        if (
            physical in required_physical
            and logical not in emitted
            and (available is None or physical in available)
        ):
            result.append({"name": logical, "source": physical})
            emitted.add(logical)
    return result


def _wide_output_projection(
    plan: CalculatedFactRunPlan,
    expression_names: Sequence[str],
    *,
    available_columns: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    selected = set(expression_names)
    available = (
        set(available_columns)
        if available_columns is not None
        else set(plan.source_schema.names)
    )
    source_dtypes = {field.name: str(field.type) for field in plan.source_schema}
    result: list[dict[str, Any]] = []
    emitted: set[str] = set()
    for name in (
        *plan.spec.identity_columns,
        *plan.spec.partition_by,
        *plan.spec.include_columns,
    ):
        if name not in emitted:
            result.append(
                {
                    "name": name,
                    "source": name,
                    "allow_missing": name not in available,
                    "dtype": source_dtypes[name],
                }
            )
            emitted.add(name)
    dtypes = {item.name: item.output_dtype for item in plan.expressions}
    for fingerprint in plan.fingerprints:
        published = fingerprint.name
        source = plan.fact_source_names.get(published, published)
        result.append(
            {
                "name": published,
                "source": source,
                "allow_missing": published not in selected,
                "dtype": dtypes[source],
            }
        )
    return result


def wide_output_column_names(plan: CalculatedFactRunPlan) -> tuple[str, ...]:
    return tuple(
        item["name"]
        for item in _wide_output_projection(
            plan, tuple(item.name for item in plan.fingerprints)
        )
    )


def _lookup_enrich_configs(
    plan: CalculatedFactRunPlan,
    cache_dir: Path,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    bindings_by_alias: dict[str, list[dict[str, str]]] = {}
    for binding in plan.binding.bindings:
        if binding.kind == "lookup" and binding.lookup_alias is not None:
            bindings_by_alias.setdefault(binding.lookup_alias, []).append(
                {"source": binding.physical_name, "output": binding.logical_name}
            )
    for lookup in plan.spec.lookup_files:
        values = list(
            {
                (item["source"], item["output"]): item
                for item in bindings_by_alias.get(lookup.alias, [])
            }.values()
        )
        if not values:
            continue
        result.append(
            {
                "alias": lookup.alias,
                "files": [str(_normalized_lookup_path(lookup, plan, cache_dir))],
                "source_keys": list(lookup.source_keys),
                "lookup_keys": list(lookup.lookup_keys),
                "value_columns": values,
            }
        )
    return result


def _normalized_lookup_path(lookup: Any, plan: CalculatedFactRunPlan, cache_dir: Path) -> Path:
    if lookup.path.suffix.lower() == ".parquet":
        return lookup.path
    projection = list(plan.binding.lookup_projection[lookup.alias])
    output = cache_dir / f"{lookup.alias}-{lookup.checksum[:20]}.parquet"
    if output.is_file():
        return output
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        table = pcsv.read_csv(
            lookup.path,
            convert_options=pcsv.ConvertOptions(include_columns=projection),
        )
    except Exception as exc:
        _fail(
            "external_file.format_mismatch",
            "Lookup CSV could not be normalized for the Rust task.",
            lookup_alias=lookup.alias,
            reason=str(exc),
        )
    staging = output.with_suffix(output.suffix + ".tmp")
    pq.write_table(table, staging, compression="uncompressed")
    staging.replace(output)
    return output


def _fail(code: str, message: str, **context: object) -> None:
    raise ValidationError(message, code=code, context=context)
