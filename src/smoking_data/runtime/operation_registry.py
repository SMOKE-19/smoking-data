from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from smoking_data.core.pipeline import PipelineSpec
from smoking_data.runtime.paths import ensure_dir

REGISTRY_SCHEMA_VERSION = "smoking-data.operation-registry.v1"
# Increment whenever the SQLite DDL below changes so existing registries run migrations once.
REGISTRY_DB_SCHEMA_VERSION = 1
TRIGGER_TYPES = frozenset({"manual", "scheduled", "chain", "followup", "retry"})
REMOVED_OPERATION_NAMES = frozenset({"load_asset", "load_dataset"})


def new_run_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}_{uuid.uuid4().hex[:8]}"


def registry_path(project_root: Path) -> Path:
    return project_root / ".smoking-data" / "registry" / "operations.sqlite"


def completion_catalog_path(project_root: Path) -> Path:
    return project_root / ".smoking-data" / "registry" / "completion-catalog.json"


def record_definition(spec: PipelineSpec, *, project_root: Path) -> Path:
    return record_graph_definition(
        yaml_path=spec.yaml_path,
        yaml_hash=spec.yaml_hash,
        graph=spec.graph,
        project_root=project_root,
    )


def record_definition_profiled(
    spec: PipelineSpec, *, project_root: Path
) -> tuple[Path, dict[str, Any]]:
    profile: dict[str, Any] = {}
    path = record_graph_definition(
        yaml_path=spec.yaml_path,
        yaml_hash=spec.yaml_hash,
        graph=spec.graph,
        project_root=project_root,
        _profile=profile,
    )
    return path, profile


def record_graph_definition(
    *,
    yaml_path: Path,
    yaml_hash: str,
    graph: dict[str, Any],
    project_root: Path,
    _profile: dict[str, Any] | None = None,
) -> Path:
    total_started = time.perf_counter()
    path = registry_path(project_root)
    phase_started = time.perf_counter()
    connection = _connect(path)
    _set_elapsed(_profile, "connect_sec", phase_started)
    now = _utc_now()
    definition_key = sha256(str(yaml_path.resolve()).encode()).hexdigest()
    try:
        phase_started = time.perf_counter()
        if _definition_is_unchanged(
            connection,
            definition_key=definition_key,
            yaml_hash=yaml_hash,
            graph=graph,
        ):
            _set_elapsed(_profile, "unchanged_check_sec", phase_started)
            phase_started = time.perf_counter()
            with connection:
                connection.execute(
                    "UPDATE definition_operations SET last_validated_at = ? "
                    "WHERE definition_key = ?",
                    (now, definition_key),
                )
            _set_elapsed(_profile, "db_write_sec", phase_started)
            if _profile is not None:
                _profile["unchanged"] = True
                _profile["catalog_sec"] = 0.0
                _profile["node_count"] = len(graph["nodes"])
                _profile["total_sec"] = time.perf_counter() - total_started
            return path
        _set_elapsed(_profile, "unchanged_check_sec", phase_started)
        phase_started = time.perf_counter()
        with connection:
            connection.execute(
                "DELETE FROM definition_operations WHERE definition_key = ?",
                (definition_key,),
            )
            for node in graph["nodes"]:
                connection.execute(
                    """
                    INSERT INTO operation_specs (
                        spec_key, op, canonical_json, canonicalization_version,
                        first_seen_at, last_seen_at, last_alias
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spec_key) DO UPDATE SET
                        canonical_json = excluded.canonical_json,
                        last_seen_at = excluded.last_seen_at,
                        last_alias = excluded.last_alias
                    """,
                    (
                        node["spec_key"],
                        node["op"],
                        node["canonical_json"],
                        graph["canonicalization_version"],
                        now,
                        now,
                        node["alias"],
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO operation_aliases (spec_key, alias, first_seen_at, last_seen_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(spec_key, alias) DO UPDATE SET last_seen_at = excluded.last_seen_at
                    """,
                    (node["spec_key"], node["alias"], now, now),
                )
                connection.execute(
                    """
                    INSERT INTO dag_nodes (
                        node_key, spec_key, canonical_inputs_json, first_seen_at, last_seen_at,
                        last_alias
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(node_key) DO UPDATE SET
                        last_seen_at = excluded.last_seen_at,
                        last_alias = excluded.last_alias
                    """,
                    (
                        node["node_key"],
                        node["spec_key"],
                        _json(node["inputs"]),
                        now,
                        now,
                        node["alias"],
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO definition_operations (
                        definition_key, definition_path, yaml_hash, alias, spec_key, node_key,
                        last_validated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        definition_key,
                        str(yaml_path),
                        yaml_hash,
                        node["alias"],
                        node["spec_key"],
                        node["node_key"],
                        now,
                    ),
                )
                _record_event(
                    connection,
                    event_type="definition_validated",
                    occurred_at=now,
                    spec_key=node["spec_key"],
                    node_key=node["node_key"],
                    definition_key=definition_key,
                    alias=node["alias"],
                )
        _set_elapsed(_profile, "db_write_sec", phase_started)
    finally:
        phase_started = time.perf_counter()
        connection.close()
        _set_elapsed(_profile, "close_sec", phase_started)
        if _profile is not None and _profile.get("unchanged") is True:
            _profile["total_sec"] = time.perf_counter() - total_started
    phase_started = time.perf_counter()
    _write_completion_catalog(project_root, _profile=_profile)
    _set_elapsed(_profile, "catalog_sec", phase_started)
    if _profile is not None:
        _profile["unchanged"] = False
        _profile["node_count"] = len(graph["nodes"])
        _profile["total_sec"] = time.perf_counter() - total_started
    return path


def record_authoring_insert(
    *,
    project_root: Path,
    spec_key: str,
    alias: str | None = None,
) -> None:
    connection = _connect(registry_path(project_root))
    now = _utc_now()
    try:
        with connection:
            exists = connection.execute(
                "SELECT 1 FROM operation_specs WHERE spec_key = ?",
                (spec_key,),
            ).fetchone()
            if exists is None:
                raise ValueError(f"Unknown operation registry spec_key: {spec_key}")
            if alias:
                connection.execute(
                    "UPDATE operation_specs SET last_alias = ?, last_seen_at = ? WHERE spec_key = ?",
                    (alias, now, spec_key),
                )
                connection.execute(
                    """
                    INSERT INTO operation_aliases (spec_key, alias, first_seen_at, last_seen_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(spec_key, alias) DO UPDATE SET last_seen_at = excluded.last_seen_at
                    """,
                    (spec_key, alias, now, now),
                )
            _record_event(
                connection,
                event_type="authoring_inserted",
                occurred_at=now,
                spec_key=spec_key,
                alias=alias,
            )
    finally:
        connection.close()
    _write_completion_catalog(project_root)


def record_execution(
    spec: PipelineSpec,
    *,
    project_root: Path,
    trigger_type: str,
    status: str,
    started_at: str,
    finished_at: str | None,
    run_id: str | None = None,
) -> str:
    return record_graph_execution(
        graph=spec.graph,
        project_root=project_root,
        trigger_type=trigger_type,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        run_id=run_id,
    )


def record_execution_profiled(
    spec: PipelineSpec,
    *,
    project_root: Path,
    trigger_type: str,
    status: str,
    started_at: str,
    finished_at: str | None,
    run_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    profile: dict[str, Any] = {}
    execution_run_id = record_graph_execution(
        graph=spec.graph,
        project_root=project_root,
        trigger_type=trigger_type,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        run_id=run_id,
        _profile=profile,
    )
    return execution_run_id, profile


def record_graph_execution(
    *,
    graph: dict[str, Any],
    project_root: Path,
    trigger_type: str,
    status: str,
    started_at: str,
    finished_at: str | None,
    run_id: str | None = None,
    _profile: dict[str, Any] | None = None,
) -> str:
    total_started = time.perf_counter()
    if trigger_type not in TRIGGER_TYPES:
        raise ValueError(f"Unsupported operation registry trigger_type: {trigger_type}")
    run_id = run_id or new_run_id()
    phase_started = time.perf_counter()
    connection = _connect(registry_path(project_root))
    _set_elapsed(_profile, "connect_sec", phase_started)
    try:
        phase_started = time.perf_counter()
        with connection:
            for node in graph["nodes"]:
                execution_key = f"{node['node_key']}_{run_id}"
                connection.execute(
                    """
                    INSERT INTO executions (
                        execution_key, run_id, node_key, spec_key, alias_at_execution,
                        trigger_type, status, started_at, finished_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_key,
                        run_id,
                        node["node_key"],
                        node["spec_key"],
                        node["alias"],
                        trigger_type,
                        status,
                        started_at,
                        finished_at,
                    ),
                )
                connection.execute(
                    """
                    UPDATE dag_nodes
                    SET last_executed_at = ?, last_status = ?, last_alias = ?
                    WHERE node_key = ?
                    """,
                    (finished_at or started_at, status, node["alias"], node["node_key"]),
                )
                _record_event(
                    connection,
                    event_type=f"{trigger_type}_execution",
                    occurred_at=finished_at or started_at,
                    spec_key=node["spec_key"],
                    node_key=node["node_key"],
                    alias=node["alias"],
                    run_id=run_id,
                    details={"status": status},
                )
        _set_elapsed(_profile, "db_write_sec", phase_started)
    finally:
        phase_started = time.perf_counter()
        connection.close()
        _set_elapsed(_profile, "close_sec", phase_started)
    phase_started = time.perf_counter()
    _write_completion_catalog(project_root, _profile=_profile)
    _set_elapsed(_profile, "catalog_sec", phase_started)
    if _profile is not None:
        _profile["node_count"] = len(graph["nodes"])
        _profile["total_sec"] = time.perf_counter() - total_started
    return run_id


def read_completion_catalog(project_root: Path) -> dict[str, Any]:
    path = completion_catalog_path(project_root)
    if not path.is_file():
        return {"schema_version": REGISTRY_SCHEMA_VERSION, "operations": []}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {"operations": []}
    operations = payload.get("operations")
    if isinstance(operations, list):
        payload["operations"] = [
            item
            for item in operations
            if isinstance(item, dict) and item.get("op") not in REMOVED_OPERATION_NAMES
        ]
    return payload


def _connect(path: Path) -> sqlite3.Connection:
    ensure_dir(path.parent)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    # The registry and its JSON catalog are rebuildable runtime indexes. WAL NORMAL
    # avoids FULL's duplicate fsync stalls after large Parquet writes while retaining
    # transactional atomicity and database consistency after process crashes.
    connection.execute("PRAGMA synchronous = NORMAL")
    current_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if current_version >= REGISTRY_DB_SCHEMA_VERSION:
        return connection
    connection.executescript(
        """
        PRAGMA journal_mode = WAL;
        CREATE TABLE IF NOT EXISTS operation_specs (
            spec_key TEXT PRIMARY KEY,
            op TEXT NOT NULL,
            canonical_json TEXT NOT NULL,
            canonicalization_version TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_alias TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS operation_aliases (
            spec_key TEXT NOT NULL REFERENCES operation_specs(spec_key),
            alias TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (spec_key, alias)
        );
        CREATE TABLE IF NOT EXISTS dag_nodes (
            node_key TEXT PRIMARY KEY,
            spec_key TEXT NOT NULL REFERENCES operation_specs(spec_key),
            canonical_inputs_json TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_alias TEXT NOT NULL,
            last_executed_at TEXT,
            last_status TEXT
        );
        CREATE TABLE IF NOT EXISTS definition_operations (
            definition_key TEXT NOT NULL,
            definition_path TEXT NOT NULL,
            yaml_hash TEXT NOT NULL,
            alias TEXT NOT NULL,
            spec_key TEXT NOT NULL REFERENCES operation_specs(spec_key),
            node_key TEXT NOT NULL REFERENCES dag_nodes(node_key),
            last_validated_at TEXT NOT NULL,
            PRIMARY KEY (definition_key, alias)
        );
        CREATE TABLE IF NOT EXISTS executions (
            execution_key TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            node_key TEXT NOT NULL REFERENCES dag_nodes(node_key),
            spec_key TEXT NOT NULL REFERENCES operation_specs(spec_key),
            alias_at_execution TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS operation_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            spec_key TEXT,
            node_key TEXT,
            definition_key TEXT,
            alias TEXT,
            run_id TEXT,
            details_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_definition_operations_spec
            ON definition_operations(spec_key, definition_key);
        CREATE INDEX IF NOT EXISTS idx_operation_events_spec_type
            ON operation_events(spec_key, event_type);
        CREATE INDEX IF NOT EXISTS idx_executions_spec_trigger_run
            ON executions(spec_key, trigger_type, run_id);
        CREATE INDEX IF NOT EXISTS idx_executions_spec_status_run
            ON executions(spec_key, status, run_id);
        CREATE INDEX IF NOT EXISTS idx_executions_spec_finished
            ON executions(spec_key, finished_at);
        """
    )
    connection.execute(f"PRAGMA user_version = {REGISTRY_DB_SCHEMA_VERSION}")
    connection.commit()
    return connection


def _write_completion_catalog(
    project_root: Path, *, _profile: dict[str, Any] | None = None
) -> None:
    phase_started = time.perf_counter()
    connection = _connect(registry_path(project_root))
    _set_elapsed(_profile, "catalog_connect_sec", phase_started)
    try:
        phase_started = time.perf_counter()
        rows = connection.execute(
            """
            WITH definition_counts AS (
                SELECT spec_key, COUNT(DISTINCT definition_key) AS definition_count
                FROM definition_operations
                GROUP BY spec_key
            ), authoring_counts AS (
                SELECT spec_key, COUNT(*) AS authoring_insert_count
                FROM operation_events
                WHERE event_type = 'authoring_inserted'
                GROUP BY spec_key
            ), execution_counts AS (
                SELECT
                    spec_key,
                    COUNT(DISTINCT CASE WHEN trigger_type = 'manual' THEN run_id END)
                        AS manual_run_count,
                    COUNT(DISTINCT CASE WHEN trigger_type = 'scheduled' THEN run_id END)
                        AS scheduled_run_count,
                    COUNT(DISTINCT CASE WHEN trigger_type = 'chain' THEN run_id END)
                        AS chain_run_count,
                    COUNT(DISTINCT CASE WHEN trigger_type = 'retry' THEN run_id END)
                        AS retry_run_count,
                    COUNT(DISTINCT CASE WHEN status = 'success' THEN run_id END)
                        AS successful_run_count,
                    COUNT(DISTINCT CASE WHEN status = 'failed' THEN run_id END)
                        AS failed_run_count,
                    MAX(finished_at) AS last_executed_at
                FROM executions
                GROUP BY spec_key
            )
            SELECT
                specs.spec_key,
                specs.op,
                specs.canonical_json,
                specs.last_alias,
                specs.last_seen_at AS last_authored_at,
                COALESCE(defs.definition_count, 0) AS definition_count,
                COALESCE(authoring.authoring_insert_count, 0) AS authoring_insert_count,
                COALESCE(executions.manual_run_count, 0) AS manual_run_count,
                COALESCE(executions.scheduled_run_count, 0) AS scheduled_run_count,
                COALESCE(executions.chain_run_count, 0) AS chain_run_count,
                COALESCE(executions.retry_run_count, 0) AS retry_run_count,
                COALESCE(executions.successful_run_count, 0) AS successful_run_count,
                COALESCE(executions.failed_run_count, 0) AS failed_run_count,
                executions.last_executed_at AS last_executed_at
            FROM operation_specs AS specs
            LEFT JOIN definition_counts AS defs ON defs.spec_key = specs.spec_key
            LEFT JOIN authoring_counts AS authoring ON authoring.spec_key = specs.spec_key
            LEFT JOIN execution_counts AS executions ON executions.spec_key = specs.spec_key
            WHERE specs.op NOT IN ('load_asset', 'load_dataset')
            ORDER BY definition_count DESC, authoring_insert_count DESC,
                     specs.last_seen_at DESC, specs.spec_key
            """
        ).fetchall()
        _set_elapsed(_profile, "catalog_query_sec", phase_started)
    finally:
        phase_started = time.perf_counter()
        connection.close()
        _set_elapsed(_profile, "catalog_close_sec", phase_started)
    phase_started = time.perf_counter()
    operations = [
        {
            key: (int(row[key] or 0) if key.endswith("_count") else row[key])
            for key in row.keys()
        }
        for row in rows
    ]
    payload = {
        "schema_version": REGISTRY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "ranking": ["definition_count", "authoring_insert_count", "last_authored_at"],
        "operations": operations,
    }
    _set_elapsed(_profile, "catalog_encode_sec", phase_started)
    phase_started = time.perf_counter()
    path = completion_catalog_path(project_root)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)
    _set_elapsed(_profile, "catalog_write_sec", phase_started)


def _definition_is_unchanged(
    connection: sqlite3.Connection,
    *,
    definition_key: str,
    yaml_hash: str,
    graph: dict[str, Any],
) -> bool:
    rows = connection.execute(
        """
        SELECT alias, yaml_hash, spec_key, node_key
        FROM definition_operations
        WHERE definition_key = ?
        """,
        (definition_key,),
    ).fetchall()
    if len(rows) != len(graph["nodes"]):
        return False
    existing = {
        (row["alias"], row["yaml_hash"], row["spec_key"], row["node_key"]) for row in rows
    }
    expected = {
        (node["alias"], yaml_hash, node["spec_key"], node["node_key"])
        for node in graph["nodes"]
    }
    return existing == expected


def _record_event(
    connection: sqlite3.Connection,
    *,
    event_type: str,
    occurred_at: str,
    spec_key: str | None = None,
    node_key: str | None = None,
    definition_key: str | None = None,
    alias: str | None = None,
    run_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO operation_events (
            event_type, occurred_at, spec_key, node_key, definition_key, alias, run_id,
            details_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_type,
            occurred_at,
            spec_key,
            node_key,
            definition_key,
            alias,
            run_id,
            _json(details or {}),
        ),
    )


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _set_elapsed(
    profile: dict[str, Any] | None, key: str, started: float
) -> None:
    if profile is not None:
        profile[key] = time.perf_counter() - started
