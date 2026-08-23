from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote

import pyarrow.compute as pc
import pyarrow.parquet as pq

from smoking_data.core.exceptions import ValidationError

from .contract import LONG_FACT_CONTRACT_VERSION, long_fact_schema

MANIFEST_SCHEMA_VERSION = "smoking-data.calculated-fact-manifest.v1"


@dataclass(frozen=True, slots=True)
class SegmentAppendedGeneration:
    generation_seq: int
    run_key: str
    files: tuple[Path, ...]
    manifest_path: Path
    reused: bool
    output_parts_by_segment: Mapping[str, tuple[str, ...]]


class SegmentAppendTransaction:
    """Append one source-segment-derived generation without row-level state."""

    def __init__(
        self,
        dataset_root: str | Path,
        *,
        run_key: str,
        identity_columns: Sequence[str],
        partition_by: Sequence[str],
    ) -> None:
        self.dataset_root = Path(dataset_root).expanduser().resolve()
        self.run_key = str(run_key).strip()
        self.identity_columns = tuple(identity_columns)
        self.partition_by = tuple(partition_by)
        if not self.run_key:
            _fail("append.invalid_run_key", "Append run_key must be non-empty.")
        if not self.partition_by or not set(self.partition_by).issubset(self.identity_columns):
            _fail(
                "incremental.partition_mismatch",
                "Append partition_by must be a non-empty subset of identity columns.",
            )
        self.generation_seq = _reserve_generation(self.dataset_root, self.run_key)
        self._stage_root = (
            self.dataset_root
            / "_smoking_data"
            / "staging"
            / hashlib.sha256(self.run_key.encode()).hexdigest()[:20]
        )
        self._staged: list[tuple[Path, Path, int, str]] = []

    def __enter__(self) -> SegmentAppendTransaction:
        return self

    def __exit__(self, exc_type: object, *_: object) -> None:
        if exc_type is not None:
            self.rollback()

    def adopt_parquet_file(self, path: str | Path, *, source_segment_id: str) -> Path:
        source = Path(path).expanduser().resolve()
        if not source.is_file() or source.suffix.lower() != ".parquet":
            _fail("append.invalid_task_output", "Rust task output must be Parquet.", path=str(source))
        parquet = pq.ParquetFile(source)
        schema = parquet.schema_arrow
        missing = [name for name in self.identity_columns if name not in schema.names]
        if missing:
            _fail("append.invalid_task_output", "Task output misses identity columns.", columns=missing)
        expected = long_fact_schema([schema.field(name) for name in self.identity_columns])
        if schema != expected:
            _fail("long_fact.schema_mismatch", "Task output does not match long_fact_v1.")
        partition_table = parquet.read(columns=list(self.partition_by))
        values: dict[str, Any] = {}
        for name in self.partition_by:
            unique = pc.unique(partition_table.column(name).combine_chunks()).to_pylist()
            if len(unique) != 1 or unique[0] is None:
                _fail(
                    "incremental.partition_mismatch",
                    "Each output part must contain exactly one partition value.",
                    column=name,
                    values=unique[:5],
                )
            values[name] = unique[0]
        relative = Path("_generations") / f"{self.generation_seq:020d}"
        for name in self.partition_by:
            relative /= f"partition-{quote(name, safe='')}-{quote(str(values[name]), safe='')}"
        relative /= f"part-{len(self._staged):06d}.parquet"
        staged = self._stage_root / relative
        staged.parent.mkdir(parents=True, exist_ok=True)
        try:
            source.replace(staged)
        except OSError:
            shutil.copy2(source, staged)
            source.unlink()
        self._staged.append((staged, relative, parquet.metadata.num_rows, source_segment_id))
        return staged

    def commit(self, *, metadata: Mapping[str, Any]) -> SegmentAppendedGeneration:
        if not self._staged:
            _fail("append.empty_generation", "Append generation contains no FACT parts.")
        manifest_path = self.dataset_root / "_dataset.manifest.json"
        moved: list[Path] = []
        with _dataset_lock(self.dataset_root):
            manifest = _read_manifest(manifest_path)
            existing = next(
                (item for item in manifest["generations"] if item.get("run_key") == self.run_key),
                None,
            )
            if existing is not None:
                self.rollback()
                files = tuple(self.dataset_root / str(item["path"]) for item in existing["files"])
                return SegmentAppendedGeneration(
                    int(existing["generation_seq"]), self.run_key, files, manifest_path, True,
                    _parts_by_segment(existing["files"]),
                )
            try:
                file_records = []
                for staged, relative, rows, segment_id in self._staged:
                    destination = self.dataset_root / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    staged.replace(destination)
                    moved.append(destination)
                    file_records.append(
                        {
                            "path": str(relative),
                            "rows": rows,
                            "sha256": _file_sha256(destination),
                            "source_segment_id": segment_id,
                        }
                    )
                generation = {
                    "generation_seq": self.generation_seq,
                    "run_key": self.run_key,
                    "committed_at": datetime.now(timezone.utc).isoformat(),
                    "files": file_records,
                    "metadata": dict(metadata),
                }
                manifest["generations"].append(generation)
                manifest["active_generation_seq"] = self.generation_seq
                _atomic_write_json(manifest_path, manifest)
            except BaseException:
                for path in moved:
                    path.unlink(missing_ok=True)
                raise
        self.rollback()
        return SegmentAppendedGeneration(
            self.generation_seq, self.run_key, tuple(moved), manifest_path, False,
            _parts_by_segment(file_records),
        )

    def rollback(self) -> None:
        if self._stage_root.exists():
            shutil.rmtree(self._stage_root)
        self._staged.clear()


def _reserve_generation(dataset_root: Path, run_key: str) -> int:
    state_path = dataset_root / "_smoking_data" / "generation-sequence.json"
    with _dataset_lock(dataset_root):
        manifest = _read_manifest(dataset_root / "_dataset.manifest.json")
        existing = next(
            (item for item in manifest["generations"] if item.get("run_key") == run_key), None
        )
        if existing is not None:
            return int(existing["generation_seq"])
        reserved = 0
        try:
            reserved = int(json.loads(state_path.read_text(encoding="utf-8"))["last_seq"])
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            pass
        committed = max(
            (int(item.get("generation_seq") or 0) for item in manifest["generations"]), default=0
        )
        next_seq = max(reserved, committed) + 1
        _atomic_write_json(state_path, {"last_seq": next_seq})
        return next_seq


@contextmanager
def _dataset_lock(dataset_root: Path) -> Iterator[None]:
    lock_path = dataset_root / "_smoking_data" / "append.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    token = f"{os.getpid()}-{uuid.uuid4()}"
    deadline = time.monotonic() + 60.0
    while True:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump({"owner": token, "created_at": time.time()}, handle)
            break
        except FileExistsError:
            try:
                if time.time() - lock_path.stat().st_mtime > 3600.0:
                    lock_path.unlink(missing_ok=True)
                    continue
            except OSError:
                pass
            if time.monotonic() >= deadline:
                _fail("append.lock_timeout", "Timed out waiting for the dataset append lock.")
            time.sleep(0.05)
    try:
        yield
    finally:
        try:
            if json.loads(lock_path.read_text(encoding="utf-8")).get("owner") == token:
                lock_path.unlink(missing_ok=True)
        except (OSError, json.JSONDecodeError, AttributeError):
            pass


def _read_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "contract": LONG_FACT_CONTRACT_VERSION,
            "active_generation_seq": None,
            "generations": [],
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _fail("append.invalid_manifest", "Calculated FACT manifest is unreadable.", reason=str(exc))
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != MANIFEST_SCHEMA_VERSION
        or payload.get("contract") != LONG_FACT_CONTRACT_VERSION
        or not isinstance(payload.get("generations"), list)
    ):
        _fail("append.invalid_manifest", "Calculated FACT manifest is incompatible.")
    return payload


def _parts_by_segment(files: Sequence[Mapping[str, Any]]) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for item in files:
        segment_id = item.get("source_segment_id")
        if segment_id:
            grouped.setdefault(str(segment_id), []).append(str(item["path"]))
    return {key: tuple(value) for key, value in grouped.items()}


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
    staging.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    staging.replace(path)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fail(code: str, message: str, **context: object) -> None:
    raise ValidationError(message, code=code, context=context)
