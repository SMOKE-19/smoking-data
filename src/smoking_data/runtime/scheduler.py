from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

from smoking_data.core.exceptions import ValidationError
from smoking_data.core.results import to_json_safe, utc_now_iso
from smoking_data.runtime.paths import ensure_dir, resolve_project_path

SCHEDULE_SCHEMA_VERSION = "smoking-data.schedule.v1"
SCHEDULE_STATE_SCHEMA_VERSION = "smoking-data.schedule-state.v1"
_MAX_CATCH_UP_MINUTES = 7 * 24 * 60


@dataclass(frozen=True, slots=True)
class ScheduleTarget:
    alias: str
    kind: str
    definition: Path
    target_key: str

    def to_dict(self) -> dict[str, Any]:
        return to_json_safe(asdict(self))


@dataclass(frozen=True, slots=True)
class ScheduleSpec:
    alias: str
    yaml_path: Path
    project_root: Path
    definition_key: str
    revision_hash: str
    enabled: bool
    timezone: str
    cron: str
    targets: tuple[ScheduleTarget, ...]
    misfire: str
    timeout_minutes: int

    def to_dict(self) -> dict[str, Any]:
        return to_json_safe(asdict(self))


@dataclass(frozen=True, slots=True)
class ScheduleOccurrence:
    occurrence_key: str
    schedule_alias: str
    scheduled_at: str
    status: str
    targets: tuple[dict[str, Any], ...] = ()
    error_type: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return to_json_safe(asdict(self))


def load_schedule_spec(path: str | Path, *, project_root: Path) -> ScheduleSpec:
    yaml_path = resolve_project_path(path, project_root=project_root)
    if not yaml_path.is_file():
        raise ValidationError(
            f"Schedule YAML does not exist: {yaml_path}",
            code="schedule.definition_missing",
            context={"yaml_path": str(yaml_path)},
        )
    payload = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValidationError("Schedule YAML root must be a mapping.", code="yaml.invalid_type")
    _reject_unknown(payload, {"yaml", "schedule", "targets", "policy"}, path="$")
    header = _mapping(payload.get("yaml"), path="yaml")
    _reject_unknown(header, {"schema_version"}, path="yaml")
    if header.get("schema_version") != SCHEDULE_SCHEMA_VERSION:
        raise ValidationError(
            f"Schedule YAML must use {SCHEDULE_SCHEMA_VERSION}.",
            code="schedule.invalid_schema_version",
            context={"actual": header.get("schema_version")},
        )
    schedule = _mapping(payload.get("schedule"), path="schedule")
    _reject_unknown(schedule, {"alias", "enabled", "timezone", "cron"}, path="schedule")
    alias = _required_string(schedule.get("alias"), path="schedule.alias")
    enabled = schedule.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValidationError(
            "schedule.enabled must be boolean.",
            code="yaml.invalid_type",
            context={"path": "schedule.enabled"},
        )
    timezone = _required_string(schedule.get("timezone"), path="schedule.timezone")
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as error:
        raise ValidationError(
            f"Unknown schedule timezone: {timezone}",
            code="schedule.invalid_timezone",
            context={"timezone": timezone},
        ) from error
    cron = _required_string(schedule.get("cron"), path="schedule.cron")
    _parse_cron(cron)

    raw_targets = payload.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise ValidationError(
            "targets must be a non-empty list.",
            code="yaml.invalid_type",
            context={"path": "targets"},
        )
    targets: list[ScheduleTarget] = []
    target_aliases: set[str] = set()
    for index, raw_target in enumerate(raw_targets):
        target_path = f"targets[{index}]"
        target = _mapping(raw_target, path=target_path)
        _reject_unknown(target, {"alias", "kind", "definition"}, path=target_path)
        target_alias = _required_string(target.get("alias"), path=f"{target_path}.alias")
        if target_alias in target_aliases:
            raise ValidationError(
                "Schedule target aliases must be unique.",
                code="schedule.duplicate_target_alias",
                context={"alias": target_alias},
            )
        target_aliases.add(target_alias)
        target_kind = _required_string(target.get("kind"), path=f"{target_path}.kind")
        if target_kind not in {"asset", "chain"}:
            raise ValidationError(
                "target kind must be asset or chain.",
                code="schedule.unsupported_target_kind",
                context={"target_kind": target_kind, "path": target_path},
            )
        definition_value = _required_string(
            target.get("definition"), path=f"{target_path}.definition"
        )
        target_definition = Path(definition_value).expanduser()
        if not target_definition.is_absolute():
            target_definition = yaml_path.parent / target_definition
        target_definition = target_definition.resolve()
        _validate_target_definition(target_definition, target_kind=target_kind)
        target_key = sha256(
            _canonical_json(
                {"kind": target_kind, "definition": str(target_definition)}
            ).encode()
        ).hexdigest()
        targets.append(
            ScheduleTarget(
                alias=target_alias,
                kind=target_kind,
                definition=target_definition,
                target_key=target_key,
            )
        )

    policy = _mapping(payload.get("policy") or {}, path="policy")
    _reject_unknown(policy, {"misfire", "timeout_minutes"}, path="policy")
    misfire = str(policy.get("misfire") or "catch_up_once")
    if misfire not in {"skip", "catch_up_once"}:
        raise ValidationError(
            "policy.misfire must be skip or catch_up_once.",
            code="schedule.invalid_misfire_policy",
        )
    timeout_minutes = policy.get("timeout_minutes", 180)
    if (
        not isinstance(timeout_minutes, int)
        or isinstance(timeout_minutes, bool)
        or timeout_minutes < 1
    ):
        raise ValidationError(
            "policy.timeout_minutes must be a positive integer.",
            code="yaml.invalid_type",
            context={"path": "policy.timeout_minutes"},
        )
    canonical = {
        "schema_version": SCHEDULE_SCHEMA_VERSION,
        "schedule": {
            "enabled": enabled,
            "timezone": timezone,
            "cron": cron,
        },
        "targets": [
            {"kind": target.kind, "definition": str(target.definition)}
            for target in targets
        ],
        "policy": {
            "misfire": misfire,
            "timeout_minutes": timeout_minutes,
        },
    }
    definition_key = sha256(str(yaml_path).encode()).hexdigest()
    revision_hash = sha256(_canonical_json(canonical).encode()).hexdigest()
    return ScheduleSpec(
        alias=alias,
        yaml_path=yaml_path,
        project_root=project_root,
        definition_key=definition_key,
        revision_hash=revision_hash,
        enabled=enabled,
        timezone=timezone,
        cron=cron,
        targets=tuple(targets),
        misfire=misfire,
        timeout_minutes=timeout_minutes,
    )


def load_schedule_directory(path: str | Path, *, project_root: Path) -> list[ScheduleSpec]:
    directory = resolve_project_path(path, project_root=project_root)
    if not directory.is_dir():
        raise ValidationError(
            f"Schedule directory does not exist: {directory}",
            code="schedule.directory_missing",
            context={"directory": str(directory)},
        )
    paths = sorted(
        {
            *directory.glob("*.schedule.yaml"),
            *directory.glob("*.schedule.yml"),
        }
    )
    if not paths:
        raise ValidationError(
            "Schedule directory contains no *.schedule.yaml files.",
            code="schedule.definition_required",
            context={"directory": str(directory)},
        )
    specs = [load_schedule_spec(item, project_root=project_root) for item in paths]
    aliases = [spec.alias for spec in specs]
    if len(aliases) != len(set(aliases)):
        duplicates = sorted({alias for alias in aliases if aliases.count(alias) > 1})
        raise ValidationError(
            "Schedule aliases must be unique within a schedule directory.",
            code="schedule.duplicate_alias",
            context={"aliases": duplicates},
        )
    return specs


def scheduler_state_path(project_root: Path) -> Path:
    return project_root / ".smoking-data" / "scheduler" / "state.sqlite"


def scheduler_lock_path(project_root: Path) -> Path:
    return project_root / ".smoking-data" / "scheduler" / "runner.lock.sqlite"


def tick_schedules(
    path: str | Path,
    *,
    project_root: Path,
    now: datetime | None = None,
    executor: Callable[[ScheduleTarget, ScheduleSpec], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    specs = load_schedule_directory(path, project_root=project_root)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValidationError(
            "schedule tick now must include timezone information.",
            code="schedule.naive_now",
        )
    current = current.astimezone(timezone.utc).replace(second=0, microsecond=0)
    state_path = scheduler_state_path(project_root)
    occurrences: list[ScheduleOccurrence] = []
    lock_connection = _acquire_scheduler_lock(scheduler_lock_path(project_root))
    if lock_connection is None:
        return {
            "schema_version": SCHEDULE_STATE_SCHEMA_VERSION,
            "ok": True,
            "busy": True,
            "checked_at": current.isoformat(),
            "state_path": str(state_path),
            "schedule_count": len(specs),
            "occurrences": [],
        }
    connection = _connect(state_path)
    try:
        for spec in specs:
            scheduled_at = _due_occurrence(connection, spec, current)
            _upsert_schedule_state(connection, spec, checked_at=current)
            if scheduled_at is None or not spec.enabled:
                continue
            occurrence_key = sha256(
                f"{spec.definition_key}:{scheduled_at.isoformat()}".encode()
            ).hexdigest()
            claim = _claim_occurrence(
                connection,
                spec,
                occurrence_key=occurrence_key,
                scheduled_at=scheduled_at,
                now=datetime.now(timezone.utc),
            )
            if claim is not None:
                occurrences.append(claim)
                continue
            target_results = _run_targets_sequentially(
                connection,
                spec,
                occurrence_key=occurrence_key,
                executor=executor or _execute_schedule_target,
            )
            failed = next((item for item in target_results if item["status"] == "failed"), None)
            status = "failed" if failed is not None else "success"
            error_type = str(failed["error_type"]) if failed else None
            error_message = str(failed["error_message"]) if failed else None
            finished_at = utc_now_iso()
            with connection:
                connection.execute(
                    """
                    UPDATE occurrences
                    SET status = ?, finished_at = ?, error_type = ?, error_message = ?
                    WHERE occurrence_key = ?
                    """,
                    (
                        status,
                        finished_at,
                        error_type,
                        error_message,
                        occurrence_key,
                    ),
                )
            occurrences.append(
                ScheduleOccurrence(
                    occurrence_key=occurrence_key,
                    schedule_alias=spec.alias,
                    scheduled_at=scheduled_at.isoformat(),
                    status=status,
                    targets=tuple(target_results),
                    error_type=error_type,
                    error_message=error_message,
                )
            )
    finally:
        connection.close()
        _release_scheduler_lock(lock_connection)
    return {
        "schema_version": SCHEDULE_STATE_SCHEMA_VERSION,
        "ok": all(item.status not in {"failed"} for item in occurrences),
        "busy": False,
        "checked_at": current.isoformat(),
        "state_path": str(state_path),
        "schedule_count": len(specs),
        "occurrences": [item.to_dict() for item in occurrences],
    }


def _run_targets_sequentially(
    connection: sqlite3.Connection,
    spec: ScheduleSpec,
    *,
    occurrence_key: str,
    executor: Callable[[ScheduleTarget, ScheduleSpec], dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    blocked = False
    for index, target in enumerate(spec.targets):
        if blocked:
            result = {
                "index": index,
                "alias": target.alias,
                "target_key": target.target_key,
                "kind": target.kind,
                "definition": str(target.definition),
                "status": "blocked",
                "run_id": None,
                "error_type": "UpstreamScheduledTargetFailure",
                "error_message": "A previous scheduled target failed.",
            }
            _write_target_result(connection, occurrence_key, result)
            results.append(result)
            continue
        started_at = utc_now_iso()
        try:
            payload = executor(target, spec)
            ok = bool(payload.get("ok", True))
            status = "success" if ok else "failed"
            run_id = _extract_run_id(payload)
            error_type = None if ok else str(payload.get("error_type") or "ScheduledRunFailure")
            error_message = None if ok else str(
                payload.get("error_message") or "Scheduled target failed."
            )
        except BaseException as error:  # noqa: BLE001 - scheduler persists structured failure.
            status = "failed"
            run_id = None
            error_type = type(error).__name__
            error_message = str(error)
        result = {
            "index": index,
            "alias": target.alias,
            "target_key": target.target_key,
            "kind": target.kind,
            "definition": str(target.definition),
            "status": status,
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": utc_now_iso(),
            "error_type": error_type,
            "error_message": error_message,
        }
        _write_target_result(connection, occurrence_key, result)
        results.append(result)
        blocked = status == "failed"
    return results


def _execute_schedule_target(target: ScheduleTarget, spec: ScheduleSpec) -> dict[str, Any]:
    if target.kind == "chain":
        from smoking_data.runtime.asset_chain import run_asset_chain

        return run_asset_chain(target.definition, project_root=spec.project_root).to_dict()
    asset_code = _target_asset_code(target.definition)
    project_root = spec.project_root
    if asset_code == "0101":
        from smoking_data.assets.a0101_source import execute_yaml

        source_result = execute_yaml(target.definition, project_root=project_root)
        return {**source_result.to_dict(), "ok": source_result.error_task_count == 0}
    if asset_code == "0102":
        from smoking_data.assets.a0102_calculated_fact import run_yaml

        return run_yaml(
            target.definition,
            project_root=project_root,
            trigger_type="scheduled",
        ).to_dict()
    if asset_code == "0103":
        from smoking_data.assets.a0103_csv_source import run_yaml

        return run_yaml(
            target.definition,
            project_root=project_root,
            trigger_type="scheduled",
        ).to_dict()
    if asset_code in {"0201", "0301", "0401"}:
        from smoking_data.runtime.runner import run_pipeline_yaml

        return run_pipeline_yaml(
            target.definition,
            project_root=project_root,
            trigger_type="scheduled",
        ).to_dict()
    raise ValidationError(
        "Scheduled asset definition filename must end with a supported Asset code.",
        code="schedule.unsupported_asset",
        context={"definition": str(target.definition)},
    )


def _due_occurrence(
    connection: sqlite3.Connection,
    spec: ScheduleSpec,
    current_utc: datetime,
) -> datetime | None:
    row = connection.execute(
        "SELECT last_checked_at FROM schedules WHERE definition_key = ?",
        (spec.definition_key,),
    ).fetchone()
    local_now = current_utc.astimezone(ZoneInfo(spec.timezone))
    if row is None or not row["last_checked_at"]:
        return local_now if _cron_matches(spec.cron, local_now) else None
    previous = datetime.fromisoformat(str(row["last_checked_at"])).astimezone(timezone.utc)
    if spec.misfire == "skip":
        return local_now if _cron_matches(spec.cron, local_now) else None
    start = max(previous + timedelta(minutes=1), current_utc - timedelta(minutes=_MAX_CATCH_UP_MINUTES))
    candidate = start
    latest: datetime | None = None
    while candidate <= current_utc:
        local_candidate = candidate.astimezone(ZoneInfo(spec.timezone))
        if _cron_matches(spec.cron, local_candidate):
            latest = local_candidate
        candidate += timedelta(minutes=1)
    return latest


def _claim_occurrence(
    connection: sqlite3.Connection,
    spec: ScheduleSpec,
    *,
    occurrence_key: str,
    scheduled_at: datetime,
    now: datetime,
) -> ScheduleOccurrence | None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        existing = connection.execute(
            "SELECT status FROM occurrences WHERE occurrence_key = ?",
            (occurrence_key,),
        ).fetchone()
        if existing is not None:
            connection.commit()
            return ScheduleOccurrence(
                occurrence_key=occurrence_key,
                schedule_alias=spec.alias,
                scheduled_at=scheduled_at.isoformat(),
                status="duplicate",
            )
        running = connection.execute(
            """
            SELECT occurrence_key, started_at FROM occurrences
            WHERE definition_key = ? AND status = 'running'
            ORDER BY started_at DESC LIMIT 1
            """,
            (spec.definition_key,),
        ).fetchone()
        if running is not None:
            started = datetime.fromisoformat(str(running["started_at"])).astimezone(timezone.utc)
            if now - started < timedelta(minutes=spec.timeout_minutes):
                connection.commit()
                return ScheduleOccurrence(
                    occurrence_key=occurrence_key,
                    schedule_alias=spec.alias,
                    scheduled_at=scheduled_at.isoformat(),
                    status="runtime_busy",
                )
            connection.execute(
                """
                UPDATE occurrences SET status = 'timed_out', finished_at = ?
                WHERE occurrence_key = ?
                """,
                (utc_now_iso(), running["occurrence_key"]),
            )
        connection.execute(
            """
            INSERT INTO occurrences (
                occurrence_key, definition_key, revision_hash, schedule_alias,
                scheduled_at, status, started_at
            ) VALUES (?, ?, ?, ?, ?, 'running', ?)
            """,
            (
                occurrence_key,
                spec.definition_key,
                spec.revision_hash,
                spec.alias,
                scheduled_at.isoformat(),
                utc_now_iso(),
            ),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return None


def _upsert_schedule_state(
    connection: sqlite3.Connection,
    spec: ScheduleSpec,
    *,
    checked_at: datetime,
) -> None:
    with connection:
        connection.execute(
            """
            INSERT INTO schedules (
                definition_key, yaml_path, revision_hash, last_alias, last_checked_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(definition_key) DO UPDATE SET
                yaml_path = excluded.yaml_path,
                revision_hash = excluded.revision_hash,
                last_alias = excluded.last_alias,
                last_checked_at = excluded.last_checked_at
            """,
            (
                spec.definition_key,
                str(spec.yaml_path),
                spec.revision_hash,
                spec.alias,
                checked_at.isoformat(),
            ),
        )


def _connect(path: Path) -> sqlite3.Connection:
    ensure_dir(path.parent)
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        PRAGMA journal_mode = WAL;
        CREATE TABLE IF NOT EXISTS scheduler_metadata (
            schema_version TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS schedules (
            definition_key TEXT PRIMARY KEY,
            yaml_path TEXT NOT NULL,
            revision_hash TEXT NOT NULL,
            last_alias TEXT NOT NULL,
            last_checked_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS occurrences (
            occurrence_key TEXT PRIMARY KEY,
            definition_key TEXT NOT NULL,
            revision_hash TEXT NOT NULL,
            schedule_alias TEXT NOT NULL,
            scheduled_at TEXT NOT NULL,
            status TEXT NOT NULL,
            run_id TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            error_type TEXT,
            error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS occurrence_targets (
            occurrence_key TEXT NOT NULL,
            target_index INTEGER NOT NULL,
            target_key TEXT NOT NULL,
            target_alias TEXT NOT NULL,
            target_kind TEXT NOT NULL,
            definition TEXT NOT NULL,
            status TEXT NOT NULL,
            run_id TEXT,
            started_at TEXT,
            finished_at TEXT,
            error_type TEXT,
            error_message TEXT,
            PRIMARY KEY (occurrence_key, target_index)
        );
        """
    )
    connection.execute(
        "INSERT OR IGNORE INTO scheduler_metadata VALUES (?, ?)",
        (SCHEDULE_STATE_SCHEMA_VERSION, utc_now_iso()),
    )
    connection.commit()
    return connection


def _acquire_scheduler_lock(path: Path) -> sqlite3.Connection | None:
    ensure_dir(path.parent)
    connection = sqlite3.connect(path, timeout=0)
    try:
        connection.execute("CREATE TABLE IF NOT EXISTS scheduler_lock (id INTEGER PRIMARY KEY)")
        connection.commit()
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("INSERT OR IGNORE INTO scheduler_lock (id) VALUES (1)")
    except sqlite3.OperationalError as error:
        connection.close()
        if "locked" in str(error).lower():
            return None
        raise
    return connection


def _release_scheduler_lock(connection: sqlite3.Connection) -> None:
    connection.rollback()
    connection.close()


def _write_target_result(
    connection: sqlite3.Connection,
    occurrence_key: str,
    result: dict[str, Any],
) -> None:
    with connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO occurrence_targets (
                occurrence_key, target_index, target_key, target_alias, target_kind,
                definition, status, run_id, started_at, finished_at,
                error_type, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                occurrence_key,
                result["index"],
                result["target_key"],
                result["alias"],
                result["kind"],
                result["definition"],
                result["status"],
                result.get("run_id"),
                result.get("started_at"),
                result.get("finished_at"),
                result.get("error_type"),
                result.get("error_message"),
            ),
        )


def _parse_cron(expression: str) -> tuple[set[int], ...]:
    fields = expression.split()
    if len(fields) != 5:
        raise ValidationError(
            "schedule.cron must contain five fields: minute hour day month weekday.",
            code="schedule.invalid_cron",
            context={"cron": expression},
        )
    ranges = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
    try:
        parsed = tuple(
            _parse_cron_field(value, minimum=limits[0], maximum=limits[1])
            for value, limits in zip(fields, ranges, strict=True)
        )
    except ValueError as error:
        raise ValidationError(
            f"Invalid schedule.cron: {error}",
            code="schedule.invalid_cron",
            context={"cron": expression},
        ) from error
    parsed_weekdays = {0 if value == 7 else value for value in parsed[4]}
    return (*parsed[:4], parsed_weekdays)


def _parse_cron_field(value: str, *, minimum: int, maximum: int) -> set[int]:
    result: set[int] = set()
    for item in value.split(","):
        base, separator, step_value = item.partition("/")
        step = int(step_value) if separator else 1
        if step < 1:
            raise ValueError("cron step must be >= 1")
        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_value, end_value = base.split("-", 1)
            start, end = int(start_value), int(end_value)
        else:
            start = end = int(base)
        if start < minimum or end > maximum or start > end:
            raise ValueError(f"cron value {item!r} is outside {minimum}..{maximum}")
        result.update(range(start, end + 1, step))
    return result


def _cron_matches(expression: str, value: datetime) -> bool:
    minute, hour, day, month, weekday = _parse_cron(expression)
    cron_weekday = (value.weekday() + 1) % 7
    fields = expression.split()
    day_match = value.day in day
    weekday_match = cron_weekday in weekday
    day_restricted = fields[2] != "*"
    weekday_restricted = fields[4] != "*"
    calendar_match = (
        day_match or weekday_match
        if day_restricted and weekday_restricted
        else day_match and weekday_match
    )
    return (
        value.minute in minute
        and value.hour in hour
        and value.month in month
        and calendar_match
    )


def _extract_run_id(payload: dict[str, Any]) -> str | None:
    details = payload.get("details")
    registry = details.get("operation_registry") if isinstance(details, dict) else None
    run_id = registry.get("run_id") if isinstance(registry, dict) else None
    return str(run_id) if run_id else None


def _target_asset_code(path: Path) -> str:
    parts = path.name.split(".")
    return parts[-2] if len(parts) >= 3 else ""


def _validate_target_definition(path: Path, *, target_kind: str) -> None:
    if not path.is_file():
        raise ValidationError(
            f"Scheduled target definition does not exist: {path}",
            code="schedule.target_missing",
            context={"definition": str(path)},
        )
    asset_code = _target_asset_code(path)
    if target_kind == "chain" and asset_code != "chain":
        raise ValidationError(
            "Scheduled chain target filename must end with .chain.yaml.",
            code="schedule.target_kind_mismatch",
            context={"definition": str(path), "target_kind": target_kind},
        )
    if target_kind == "asset" and asset_code not in {
        "0101",
        "0102",
        "0103",
        "0201",
        "0301",
        "0401",
    }:
        raise ValidationError(
            "Scheduled asset target filename must end with a supported Asset code.",
            code="schedule.target_kind_mismatch",
            context={"definition": str(path), "target_kind": target_kind},
        )


def _mapping(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(
            f"{path} must be a mapping.",
            code="yaml.invalid_type",
            context={"path": path},
        )
    return dict(value)


def _required_string(value: Any, *, path: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValidationError(
            f"{path} is required.",
            code="yaml.required_key",
            context={"path": path},
        )
    return text


def _reject_unknown(value: dict[str, Any], allowed: set[str], *, path: str) -> None:
    unknown = sorted(set(value).difference(allowed))
    if unknown:
        raise ValidationError(
            "Unknown YAML keys.",
            code="yaml.unknown_key",
            context={"path": path, "keys": unknown},
        )


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
