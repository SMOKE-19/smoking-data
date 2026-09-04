from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, Iterable, Mapping, Sequence

import polars as pl

COHORT_PLAN_VERSION = "smoking-data.0201-planning-cohorts.v1"
SUBBUCKET_PLAN_VERSION = "smoking-data.0201-adaptive-subbuckets.v1"
CANDIDATE_OCCUPANCY_VERSION = "smoking-data.0201-candidate-occupancy.v3"


def canonical_scalar(value: Any, *, dtype: str | None = None) -> dict[str, Any]:
    if isinstance(value, (datetime, date, time)):
        encoded: Any = value.isoformat()
    elif isinstance(value, bytes):
        encoded = value.hex()
    elif value is None or isinstance(value, (str, int, float, bool)):
        encoded = value
    else:
        encoded = str(value)
    payload = {"type": dtype or type(value).__name__, "value": encoded}
    payload["key"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:20]
    return payload


def build_candidate_occupancy(
    frame: pl.DataFrame,
    *,
    partition_column: str,
    group_keys: Sequence[str],
    max_radix_depth: int = 3,
    max_dictionary_values: int = 256,
    source_sequence: int | None = None,
    source_fingerprint: str | None = None,
    logical_plan_hash: str | None = None,
    selector_contract_hash: str | None = None,
) -> dict[str, Any]:
    if partition_column not in frame.columns:
        raise ValueError(f"Candidate is missing partition column {partition_column!r}.")
    schema = frame.schema
    profiles: list[dict[str, Any]] = []
    for raw_partition, part in frame.partition_by(
        partition_column, as_dict=True, maintain_order=False
    ).items():
        partition_value = raw_partition[0] if isinstance(raw_partition, tuple) else raw_partition
        partition = canonical_scalar(
            partition_value,
            dtype=str(schema.get(partition_column)),
        )
        group_profiles: dict[str, dict[str, Any]] = {}
        partition_text = "" if partition_value is None else str(partition_value)
        for column in group_keys:
            if column == partition_column or column not in schema:
                continue
            series = part.get_column(column)
            dtype = series.dtype
            non_null = series.drop_nulls()
            profile: dict[str, Any] = {
                "column": column,
                "dtype": str(dtype),
                "rows": part.height,
                "nulls": series.null_count(),
                "non_null": len(non_null),
                "distinct": int(non_null.n_unique()) if len(non_null) else 0,
            }
            if dtype == pl.String:
                lengths = non_null.str.len_chars()
                prefix_matches = (
                    int(non_null.str.starts_with(partition_text).sum() or 0)
                    if partition_text
                    else 0
                )
                profile.update(
                    {
                        "kind": "string",
                        "min_length": int(lengths.min()) if len(lengths) else None,
                        "max_length": int(lengths.max()) if len(lengths) else None,
                        "partition_prefix": partition_text,
                        "prefix_matches": prefix_matches,
                        "radix": {},
                    }
                )
                if partition_text and prefix_matches:
                    valid = non_null.filter(non_null.str.starts_with(partition_text))
                    for depth in range(1, max(1, int(max_radix_depth)) + 1):
                        prefixes = valid.str.slice(len(partition_text), depth)
                        counts = (
                            pl.DataFrame({"value": prefixes})
                            .group_by("value")
                            .len(name="rows")
                            .sort(["rows", "value"], descending=[True, False])
                        )
                        profile["radix"][str(depth)] = {
                            "distinct": counts.height,
                            "max_rows": int(counts.get_column("rows").max() or 0),
                            "counts": [
                                {"value": row["value"], "rows": int(row["rows"])}
                                for row in counts.head(max_dictionary_values).iter_rows(named=True)
                            ],
                            "truncated": counts.height > max_dictionary_values,
                        }
            elif dtype.is_integer() or dtype.is_float():
                profile["kind"] = "numeric"
                if len(non_null):
                    profile["min"] = non_null.min()
                    profile["max"] = non_null.max()
                counts = (
                    part.select(pl.col(column))
                    .drop_nulls()
                    .group_by(column)
                    .len(name="rows")
                    .sort(["rows", column], descending=[True, False])
                )
                profile["values"] = [
                    {"value": row[column], "rows": int(row["rows"])}
                    for row in counts.head(max_dictionary_values).iter_rows(named=True)
                ]
                profile["values_truncated"] = counts.height > max_dictionary_values
            else:
                profile["kind"] = "other"
            group_profiles[column] = profile
        profiles.append(
            {
                "partition": partition,
                "rows": part.height,
                "estimated_bytes": max(1, int(part.estimated_size())),
                "group_keys": group_profiles,
            }
        )
    record_batches = []
    row_start = 0
    for index, batch in enumerate(frame.to_arrow().to_batches(max_chunksize=65_536)):
        batch_frame = pl.from_arrow(batch)
        partition_keys = sorted(
            {
                str(canonical_scalar(value, dtype=str(schema[partition_column]))["key"])
                for value in batch_frame.get_column(partition_column).unique().to_list()
            }
        )
        record_batches.append(
            {
                "index": index,
                "row_start": row_start,
                "rows": batch.num_rows,
                "estimated_bytes": max(1, int(batch.nbytes)),
                "partition_keys": partition_keys,
            }
        )
        row_start += batch.num_rows
    schema_hash = hashlib.sha256(str(frame.to_arrow().schema).encode("utf-8")).hexdigest()
    return {
        "schema_version": CANDIDATE_OCCUPANCY_VERSION,
        "schema_hash": schema_hash,
        "source_sequence": source_sequence,
        "source_fingerprint": source_fingerprint,
        "logical_plan_hash": logical_plan_hash,
        "selector_contract_hash": selector_contract_hash,
        "partition_column": partition_column,
        "group_keys": list(group_keys),
        "rows": frame.height,
        "estimated_bytes": max(1, int(frame.estimated_size())),
        "record_batches": record_batches,
        "partitions": sorted(profiles, key=lambda item: str(item["partition"]["key"])),
    }


def build_planning_cohorts(
    sources: Sequence[Mapping[str, Any]],
    *,
    target_rows: int,
    target_bytes: int,
    max_files: int,
    max_row_groups: int,
    previous_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    limits = {
        "rows": max(1, int(target_rows)),
        "bytes": max(1, int(target_bytes)),
        "files": max(1, int(max_files)),
        "row_groups": max(1, int(max_row_groups)),
    }
    source_by_path = {str(source["path"]): source for source in sources}
    preserved_cohorts: list[dict[str, Any]] = []
    assigned_paths: set[str] = set()
    if (
        previous_plan is not None
        and previous_plan.get("schema_version") == COHORT_PLAN_VERSION
        and previous_plan.get("limits") == limits
    ):
        for previous_cohort in previous_plan.get("cohorts") or []:
            previous_slices = list(previous_cohort.get("slices") or [])
            previous_paths = [
                str(source["path"])
                for item in previous_slices
                for source in item.get("sources") or []
            ]
            if (
                not previous_paths
                or any(path not in source_by_path or path in assigned_paths for path in previous_paths)
            ):
                continue
            reconstructed_slices = []
            for item in previous_slices:
                members = [source_by_path[str(source["path"])] for source in item["sources"]]
                reconstructed_slices.append(
                    {
                        "slice_id": str(item["slice_id"]),
                        "dataset_shard_id": str(item["dataset_shard_id"]),
                        "slice_index": int(item["slice_index"]),
                        "sources": [dict(source) for source in members],
                        "weight": _sum_source_weights(members),
                    }
                )
            preserved_cohorts.append(_cohort(reconstructed_slices))
            assigned_paths.update(previous_paths)

    by_shard: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for source in sources:
        if str(source["path"]) in assigned_paths:
            continue
        by_shard[str(source["dataset_shard_id"])].append(source)

    slices: list[dict[str, Any]] = []
    split_shards = 0
    oversized_files = 0
    for shard_id in sorted(by_shard):
        shard_sources = sorted(
            by_shard[shard_id], key=lambda item: str(item.get("relative_path") or item["path"])
        )
        shard_slices: list[list[Mapping[str, Any]]] = []
        current: list[Mapping[str, Any]] = []
        totals = _empty_weight()
        for source in shard_sources:
            weight = _source_weight(source)
            if current and _would_exceed(totals, weight, limits):
                shard_slices.append(current)
                current = []
                totals = _empty_weight()
            current.append(source)
            totals = _add_weight(totals, weight)
            if _is_oversized(weight, limits):
                oversized_files += 1
                shard_slices.append(current)
                current = []
                totals = _empty_weight()
        if current:
            shard_slices.append(current)
        if len(shard_slices) > 1:
            split_shards += 1
        for index, members in enumerate(shard_slices):
            member_paths = [str(item["path"]) for item in members]
            slice_id = _stable_id("slice", shard_id, *member_paths)
            slices.append(
                {
                    "slice_id": slice_id,
                    "dataset_shard_id": shard_id,
                    "slice_index": index,
                    "sources": [dict(item) for item in members],
                    "weight": _sum_source_weights(members),
                }
            )

    cohorts: list[dict[str, Any]] = list(preserved_cohorts)
    current_slices: list[dict[str, Any]] = []
    current_weight = _empty_weight()
    for item in sorted(slices, key=lambda value: (value["dataset_shard_id"], value["slice_index"])):
        weight = item["weight"]
        if current_slices and _would_exceed(current_weight, weight, limits):
            cohorts.append(_cohort(current_slices))
            current_slices = []
            current_weight = _empty_weight()
        current_slices.append(item)
        current_weight = _add_weight(current_weight, weight)
        if _is_oversized(weight, limits):
            cohorts.append(_cohort(current_slices))
            current_slices = []
            current_weight = _empty_weight()
    if current_slices:
        cohorts.append(_cohort(current_slices))

    all_shard_ids = {str(source["dataset_shard_id"]) for source in sources}
    all_slices = [item for cohort in cohorts for item in cohort["slices"]]
    oversized_files = sum(
        1 for source in sources if _is_oversized(_source_weight(source), limits)
    )
    split_shards = sum(
        1
        for shard_id in all_shard_ids
        if sum(1 for item in all_slices if item["dataset_shard_id"] == shard_id) > 1
    )
    merged_small_shards = sum(
        max(0, len({item["dataset_shard_id"] for item in cohort["slices"]}) - 1)
        for cohort in cohorts
    )
    return {
        "schema_version": COHORT_PLAN_VERSION,
        "limits": limits,
        "dataset_shards": len(all_shard_ids),
        "dataset_slices": len(all_slices),
        "planning_cohorts": len(cohorts),
        "split_dataset_shards": split_shards,
        "merged_small_dataset_shards": merged_small_shards,
        "oversized_source_files": oversized_files,
        "reused_cohort_memberships": len(preserved_cohorts),
        "cohorts": cohorts,
    }


def build_adaptive_subbucket_plan(
    occupancies: Iterable[Mapping[str, Any]],
    *,
    partition_column: str,
    group_keys: Sequence[str],
    target_rows: int,
    target_bytes: int,
    max_hash_buckets: int = 1024,
) -> dict[str, Any]:
    aggregated: dict[str, dict[str, Any]] = {}
    for occupancy in occupancies:
        for partition in occupancy.get("partitions") or []:
            identity = dict(partition["partition"])
            target = aggregated.setdefault(
                str(identity["key"]),
                {
                    "partition": identity,
                    "rows": 0,
                    "estimated_bytes": 0,
                    "group_keys": {},
                },
            )
            target["rows"] += int(partition.get("rows") or 0)
            target["estimated_bytes"] += int(partition.get("estimated_bytes") or 0)
            _merge_group_profiles(target["group_keys"], partition.get("group_keys") or {})

    plans: dict[str, dict[str, Any]] = {}
    for partition_key, aggregate in sorted(aggregated.items()):
        rows = int(aggregate["rows"])
        estimated_bytes = int(aggregate["estimated_bytes"])
        required_parts = max(
            1,
            math.ceil(rows / max(1, int(target_rows))),
            math.ceil(estimated_bytes / max(1, int(target_bytes))),
        )
        base = {
            "partition": aggregate["partition"],
            "rows": rows,
            "estimated_bytes": estimated_bytes,
            "target_rows": int(target_rows),
            "target_bytes": int(target_bytes),
        }
        if required_parts <= 1:
            plans[partition_key] = {**base, "method": "single", "estimated_leaves": 1}
            continue
        string_profile = next(
            (
                aggregate["group_keys"].get(column)
                for column in group_keys
                if column != partition_column
                and (aggregate["group_keys"].get(column) or {}).get("kind") == "string"
                and _is_partition_prefix_profile(aggregate["group_keys"].get(column) or {})
            ),
            None,
        )
        if string_profile is None:
            plans[partition_key] = {
                **base,
                "method": "hash",
                "hash_modulus": min(max_hash_buckets, required_parts),
                "reason": "no_safe_partition_prefix_group_key",
            }
            continue
        selected_depth = None
        selected_max_rows = rows
        radix = string_profile.get("radix") or {}
        for raw_depth in sorted(radix, key=int):
            depth_profile = radix[raw_depth]
            max_rows = int(depth_profile.get("max_rows") or rows)
            max_bytes = math.ceil(max_rows * estimated_bytes / max(1, rows))
            selected_depth = int(raw_depth)
            selected_max_rows = max_rows
            if max_rows <= target_rows and max_bytes <= target_bytes:
                break
        secondary_profile = next(
            (
                aggregate["group_keys"].get(column)
                for column in group_keys
                if column not in {partition_column, string_profile["column"]}
                and (aggregate["group_keys"].get(column) or {}).get("kind") == "numeric"
                and not (aggregate["group_keys"].get(column) or {}).get("values_truncated")
                and (aggregate["group_keys"].get(column) or {}).get("distinct", 0) > 1
            ),
            None,
        )
        secondary_column = (
            str(secondary_profile["column"])
            if selected_max_rows > target_rows and secondary_profile is not None
            else None
        )
        secondary_distinct = int(secondary_profile.get("distinct") or 1) if secondary_column else 1
        residual_rows = math.ceil(selected_max_rows / max(1, secondary_distinct))
        hash_modulus = min(
            max_hash_buckets,
            max(
                1,
                math.ceil(residual_rows / max(1, target_rows)),
                math.ceil(
                    residual_rows * estimated_bytes / max(1, rows) / max(1, target_bytes)
                ),
            ),
        )
        plans[partition_key] = {
            **base,
            "method": "radix",
            "column": string_profile["column"],
            "partition_prefix": string_profile.get("partition_prefix"),
            "suffix_start": len(str(string_profile.get("partition_prefix") or "")),
            "depth": selected_depth or 1,
            "secondary_column": secondary_column,
            "hash_modulus": hash_modulus,
            "estimated_leaves": max(
                required_parts,
                int((radix.get(str(selected_depth or 1)) or {}).get("distinct") or 1)
                * secondary_distinct
                * hash_modulus,
            ),
            "reason": "partition_prefix_adaptive_radix",
        }
    return {
        "schema_version": SUBBUCKET_PLAN_VERSION,
        "partition_column": partition_column,
        "group_keys": list(group_keys),
        "target_rows": int(target_rows),
        "target_bytes": int(target_bytes),
        "partitions": plans,
    }


def assign_subbucket(
    frame: pl.DataFrame,
    *,
    partition_plan: Mapping[str, Any],
    group_keys: Sequence[str],
) -> pl.Expr | pl.Series:
    method = str(partition_plan.get("method") or "single")
    if method == "single":
        return pl.Series("__subbucket", ["single"] * frame.height, dtype=pl.String)
    hashes = frame.select(list(group_keys)).hash_rows(seed=0)
    modulus = max(1, int(partition_plan.get("hash_modulus") or 1))
    if method == "hash":
        return (hashes % modulus).cast(pl.String).map_elements(
            lambda value: f"hash:{value}", return_dtype=pl.String
        ).alias("__subbucket")
    column = str(partition_plan["column"])
    prefix = str(partition_plan.get("partition_prefix") or "")
    start = int(partition_plan.get("suffix_start") or len(prefix))
    depth = max(1, int(partition_plan.get("depth") or 1))
    values = frame.get_column(column).cast(pl.String)
    valid = values.is_not_null() & values.str.starts_with(prefix) & (
        values.str.len_chars() >= start + depth
    )
    suffix = values.str.slice(start, depth).fill_null("__NULL__")
    labels = pl.DataFrame({"suffix": suffix}).select(
        pl.concat_str([pl.lit("radix:"), pl.col("suffix")]).alias("label")
    ).get_column("label")
    secondary = partition_plan.get("secondary_column")
    if secondary:
        secondary_values = frame.get_column(str(secondary)).cast(pl.String).fill_null("__NULL__")
        labels = pl.DataFrame({"label": labels, "secondary": secondary_values}).select(
            pl.concat_str(
                [pl.col("label"), pl.lit("|value:"), pl.col("secondary")]
            ).alias("label")
        ).get_column("label")
    if modulus > 1:
        labels = pl.DataFrame({"label": labels, "hash": (hashes % modulus).cast(pl.String)}).select(
            pl.concat_str([pl.col("label"), pl.lit("|hash:"), pl.col("hash")]).alias(
                "label"
            )
        ).get_column("label")
    fallback = (hashes % max(1, modulus)).cast(pl.String).map_elements(
        lambda value: f"fallback:{value}", return_dtype=pl.String
    )
    return pl.when(valid).then(labels).otherwise(fallback).alias("__subbucket")


def partition_plan_for_value(plan: Mapping[str, Any], value: Any, *, dtype: str) -> dict[str, Any]:
    key = canonical_scalar(value, dtype=dtype)["key"]
    resolved = (plan.get("partitions") or {}).get(key)
    if not isinstance(resolved, dict):
        return {
            "partition": canonical_scalar(value, dtype=dtype),
            "method": "hash",
            "hash_modulus": 1,
            "reason": "partition_missing_from_profile",
        }
    return resolved


def _merge_group_profiles(target: dict[str, Any], incoming: Mapping[str, Any]) -> None:
    for column, raw_profile in incoming.items():
        profile = dict(raw_profile)
        current = target.setdefault(
            column,
            {
                "column": column,
                "dtype": profile.get("dtype"),
                "kind": profile.get("kind"),
                "rows": 0,
                "nulls": 0,
                "non_null": 0,
                "distinct": 0,
                "prefix_matches": 0,
                "min_length": None,
                "max_length": None,
                "radix": {},
                "values": {},
                "values_truncated": False,
            },
        )
        for key in ("rows", "nulls", "non_null", "prefix_matches"):
            current[key] += int(profile.get(key) or 0)
        current["distinct"] = max(int(current.get("distinct") or 0), int(profile.get("distinct") or 0))
        current["partition_prefix"] = profile.get("partition_prefix")
        for key, reducer in (("min_length", min), ("max_length", max)):
            value = profile.get(key)
            if value is not None:
                current[key] = value if current[key] is None else reducer(current[key], value)
        for depth, depth_profile in (profile.get("radix") or {}).items():
            depth_target = current["radix"].setdefault(
                depth, {"counts": defaultdict(int), "truncated": False}
            )
            for item in depth_profile.get("counts") or []:
                depth_target["counts"][str(item.get("value"))] += int(item.get("rows") or 0)
            depth_target["truncated"] = bool(
                depth_target["truncated"] or depth_profile.get("truncated")
            )
        for item in profile.get("values") or []:
            current["values"][str(item.get("value"))] = (
                current["values"].get(str(item.get("value")), 0) + int(item.get("rows") or 0)
            )
        current["values_truncated"] = bool(
            current["values_truncated"] or profile.get("values_truncated")
        )
    for current in target.values():
        for depth, depth_target in current.get("radix", {}).items():
            counts = depth_target.get("counts") or {}
            depth_target["distinct"] = len(counts)
            depth_target["max_rows"] = max(counts.values(), default=0)
        if current.get("kind") == "numeric" and not current.get("values_truncated"):
            current["distinct"] = len(current.get("values") or {})


def _is_partition_prefix_profile(profile: Mapping[str, Any]) -> bool:
    non_null = int(profile.get("non_null") or 0)
    return bool(
        non_null
        and int(profile.get("prefix_matches") or 0) == non_null
        and profile.get("min_length") == profile.get("max_length")
        and profile.get("radix")
    )


def _source_weight(source: Mapping[str, Any]) -> dict[str, int]:
    return {
        "rows": int(source.get("rows") or 0),
        "bytes": int(source.get("size_bytes") or 0),
        "files": 1,
        "row_groups": max(1, int(source.get("row_groups") or 1)),
    }


def _sum_source_weights(sources: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    total = _empty_weight()
    for source in sources:
        total = _add_weight(total, _source_weight(source))
    return total


def _empty_weight() -> dict[str, int]:
    return {"rows": 0, "bytes": 0, "files": 0, "row_groups": 0}


def _add_weight(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {key: int(left.get(key, 0)) + int(right.get(key, 0)) for key in _empty_weight()}


def _would_exceed(
    current: Mapping[str, int], addition: Mapping[str, int], limits: Mapping[str, int]
) -> bool:
    combined = _add_weight(current, addition)
    return any(combined[key] > int(limits[key]) for key in limits)


def _is_oversized(weight: Mapping[str, int], limits: Mapping[str, int]) -> bool:
    return any(int(weight[key]) > int(limits[key]) for key in limits)


def _cohort(slices: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    slice_ids = [str(item["slice_id"]) for item in slices]
    sources = [dict(source) for item in slices for source in item["sources"]]
    return {
        "cohort_id": _stable_id("cohort", *slice_ids),
        "slices": [dict(item) for item in slices],
        "source_paths": [str(item["path"]) for item in sources],
        "candidate_paths": [str(item["candidate_path"]) for item in sources],
        "occupancy_paths": [str(item["occupancy_path"]) for item in sources],
        "weight": _sum_source_weights(sources),
    }


def _stable_id(prefix: str, *values: str) -> str:
    digest = hashlib.sha256("\0".join(values).encode("utf-8")).hexdigest()[:20]
    return f"{prefix}-{digest}"
