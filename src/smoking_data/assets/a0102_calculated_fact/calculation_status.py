from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from smoking_data.runtime.paths import ensure_dir

STATUS_SCHEMA_VERSION = "smoking-data.calculation-status.v1"
EVENT_SCHEMA_VERSION = "smoking-data.calculation-event.v1"
STATUS_RELATIVE_PATH = Path("_smoking_data/calculation-status.json")
EVENTS_RELATIVE_PATH = Path("_smoking_data/calculation-events.jsonl")


def update_calculation_status(
    output_root: Path,
    *,
    asset_code: str,
    job_name: str,
    upstream_asset_code: str,
    upstream_generation_id: str | None,
    upstream_schema_hash: str,
    global_skips: list[dict[str, Any]],
    segment_skips: list[dict[str, Any]],
    active_expression_names: set[str],
    observed_segment_ids: set[str],
    observed_at: datetime,
) -> dict[str, Any]:
    root = Path(output_root).expanduser().resolve()
    status_path = root / STATUS_RELATIVE_PATH
    events_path = root / EVENTS_RELATIVE_PATH
    previous = _read_json(status_path)
    previous_items = {
        _record_key(item): dict(item)
        for item in previous.get("calculations", [])
        if isinstance(item, dict) and _record_key(item)
    }
    timestamp = observed_at.isoformat()
    current_observations = _observation_records(
        global_skips=global_skips,
        segment_skips=segment_skips,
        upstream_generation_id=upstream_generation_id,
        upstream_schema_hash=upstream_schema_hash,
        observed_at=timestamp,
    )
    current_by_key = {_record_key(item): item for item in current_observations}
    events: list[dict[str, Any]] = []

    for key, current in current_by_key.items():
        old = previous_items.get(key)
        if old is None:
            current["first_blocked_at"] = timestamp
            current["last_observed_at"] = timestamp
            events.append(_event("calculation_blocked", current, timestamp))
        else:
            current["first_blocked_at"] = old.get("first_blocked_at", timestamp)
            current["last_observed_at"] = timestamp
            if _signature(old) != _signature(current):
                events.append(_event("calculation_blocked", current, timestamp))
            previous_items[key] = current

    observed_keys = {
        (str(expression), str(segment))
        for expression in active_expression_names
        for segment in observed_segment_ids
    }
    for key, old in previous_items.items():
        expression_name, segment_id = key.split("\0", 1)
        if key in current_by_key or (expression_name, segment_id) not in observed_keys:
            if key not in current_by_key:
                previous_items[key] = old
            continue
        if old.get("calculation_state") != "blocked_missing_dependency":
            continue
        recovered = {
            **old,
            "calculation_state": "recovered",
            "output_effect": "no_new_fact",
            "recovered_at": timestamp,
            "last_observed_at": timestamp,
            "current_upstream_generation_id": upstream_generation_id,
            "current_upstream_schema_hash": upstream_schema_hash,
        }
        previous_items[key] = recovered
        events.append(_event("calculation_recovered", recovered, timestamp))

    calculations = list(previous_items.values())
    calculations.extend(
        item for key, item in current_by_key.items() if key not in previous_items
    )
    calculations.sort(key=lambda item: (str(item.get("expression_name")), str(item.get("source_segment_id"))))
    summary = {
        "active": len(active_expression_names) * len(observed_segment_ids),
        "blocked_missing_dependency": sum(
            item.get("calculation_state") == "blocked_missing_dependency" for item in calculations
        ),
        "recovered": sum(item.get("calculation_state") == "recovered" for item in calculations),
        "no_new_fact": len(current_by_key),
    }
    payload = {
        "schema_version": STATUS_SCHEMA_VERSION,
        "asset_code": asset_code,
        "job_name": job_name,
        "updated_at": timestamp,
        "upstream": {
            "asset_code": upstream_asset_code,
            "generation_id": upstream_generation_id,
            "schema_hash": upstream_schema_hash,
        },
        "summary": summary,
        "calculations": calculations,
    }
    _write_json_atomic(status_path, payload)
    if events:
        ensure_dir(events_path.parent)
        with events_path.open("a", encoding="utf-8") as stream:
            for event in events:
                stream.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "status_path": str(STATUS_RELATIVE_PATH),
        "events_path": str(EVENTS_RELATIVE_PATH),
        "summary": summary,
    }


def _observation_records(
    *,
    global_skips: list[dict[str, Any]],
    segment_skips: list[dict[str, Any]],
    upstream_generation_id: str | None,
    upstream_schema_hash: str,
    observed_at: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in global_skips:
        segment_ids = item.get("source_segment_ids") or ["__dataset__"]
        for segment_id in segment_ids:
            records.append(
                _record(
                    item,
                    source_segment_id=str(segment_id),
                    upstream_generation_id=upstream_generation_id,
                    schema_hash=upstream_schema_hash,
                    observed_at=observed_at,
                )
            )
    for item in segment_skips:
        records.append(
            _record(
                item,
                source_segment_id=str(item.get("source_segment_id") or "__unknown__"),
                upstream_generation_id=upstream_generation_id,
                schema_hash=str(item.get("source_schema_hash") or upstream_schema_hash),
                observed_at=observed_at,
            )
        )
    return records


def _record(
    item: dict[str, Any],
    *,
    source_segment_id: str,
    upstream_generation_id: str | None,
    schema_hash: str,
    observed_at: str,
) -> dict[str, Any]:
    return {
        "expression_name": str(item.get("expression_name") or ""),
        "source_segment_id": source_segment_id,
        "calculation_state": "blocked_missing_dependency",
        "output_effect": "no_new_fact",
        "missing_dependencies": list(item.get("missing_dependencies") or []),
        "last_observed_at": observed_at,
        "current_upstream_generation_id": upstream_generation_id,
        "current_upstream_schema_hash": schema_hash,
        "next_action": "restore_dependency_or_update_expression",
    }


def _event(event_type: str, item: dict[str, Any], timestamp: str) -> dict[str, Any]:
    return {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event": event_type,
        "timestamp": timestamp,
        "asset_code": "0102",
        "expression_name": item.get("expression_name"),
        "source_segment_id": item.get("source_segment_id"),
        "calculation_state": item.get("calculation_state"),
        "output_effect": item.get("output_effect", "no_new_fact"),
        "missing_dependencies": item.get("missing_dependencies", []),
        "upstream_generation_id": item.get("current_upstream_generation_id"),
        "upstream_schema_hash": item.get("current_upstream_schema_hash"),
    }


def _record_key(item: dict[str, Any]) -> str:
    expression = str(item.get("expression_name") or "")
    segment = str(item.get("source_segment_id") or "")
    return f"{expression}\0{segment}" if expression and segment else ""


def _signature(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        item.get("calculation_state"),
        item.get("missing_dependencies"),
        item.get("current_upstream_schema_hash"),
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    staging = path.with_suffix(path.suffix + ".tmp")
    staging.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    staging.replace(path)
