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

import pyarrow as pa
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
        contract: str = LONG_FACT_CONTRACT_VERSION,
        output_columns: Sequence[str] = (),
    ) -> None:
        self.dataset_root = Path(dataset_root).expanduser().resolve()
        self.run_key = str(run_key).strip()
        self.identity_columns = tuple(identity_columns)
        self.partition_by = tuple(partition_by)
        self.contract = contract
        self.output_columns = tuple(output_columns)
        if self.contract not in {LONG_FACT_CONTRACT_VERSION, "wide_calculated_v1"}:
            _fail("append.invalid_contract", "Unsupported 0102 append contract.")
        if not self.run_key:
            _fail("append.invalid_run_key", "Append run_key must be non-empty.")
        if not self.partition_by or not set(self.partition_by).issubset(self.identity_columns):
            _fail(
                "incremental.partition_mismatch",
                "Append partition_by must be a non-empty subset of identity columns.",
            )
        manifest = _read_manifest(
            self.dataset_root / "_dataset.manifest.json", contract=self.contract
        )
        self._expected_schema = (
            _manifest_output_schema(self.dataset_root, manifest)
            if self.contract == "wide_calculated_v1"
            else None
        )
        self.generation_seq = _reserve_generation(
            self.dataset_root, self.run_key, contract=self.contract
        )
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
        if self.contract == LONG_FACT_CONTRACT_VERSION:
            expected = long_fact_schema([schema.field(name) for name in self.identity_columns])
            if schema != expected:
                _fail("long_fact.schema_mismatch", "Task output does not match long_fact_v1.")
        elif tuple(schema.names) != self.output_columns:
            _fail(
                "wide_output.schema_mismatch",
                "Task output columns do not match the planned wide output.",
                expected=list(self.output_columns),
                actual=list(schema.names),
            )
        if self.contract == "wide_calculated_v1":
            self._expected_schema = _merge_wide_schema(self._expected_schema, schema)
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
            manifest = _read_manifest(manifest_path, contract=self.contract)
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
                    if self.contract == "wide_calculated_v1":
                        assert self._expected_schema is not None
                        _normalize_wide_parquet(staged, self._expected_schema)
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
                manifest["output_schema"] = (
                    str(self._expected_schema)
                    if self._expected_schema is not None
                    else manifest.get("output_schema")
                )
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


def _reserve_generation(dataset_root: Path, run_key: str, *, contract: str) -> int:
    state_path = dataset_root / "_smoking_data" / "generation-sequence.json"
    with _dataset_lock(dataset_root):
        manifest = _read_manifest(
            dataset_root / "_dataset.manifest.json", contract=contract
        )
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


def _read_manifest(
    path: Path, *, contract: str = LONG_FACT_CONTRACT_VERSION
) -> dict[str, Any]:
    if not path.is_file():
        return {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "contract": contract,
            "output_schema": None,
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
        or payload.get("contract") != contract
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


def _manifest_output_schema(dataset_root: Path, manifest: Mapping[str, Any]) -> pa.Schema | None:
    for generation in reversed(manifest.get("generations") or []):
        for item in generation.get("files") or []:
            path = dataset_root / str(item.get("path") or "")
            if path.is_file():
                return pq.ParquetFile(path).schema_arrow
    return None


def _merge_wide_schema(expected: pa.Schema | None, actual: pa.Schema) -> pa.Schema:
    if expected is None:
        return actual
    if expected.names != actual.names:
        _fail(
            "wide_output.schema_mismatch",
            "Task output columns differ from the active dataset schema.",
            expected=expected.names,
            actual=actual.names,
        )
    fields: list[pa.Field] = []
    for expected_field, actual_field in zip(expected, actual, strict=True):
        expected_type = expected_field.type
        actual_type = actual_field.type
        if pa.types.is_null(expected_type):
            dtype = actual_type
        elif pa.types.is_null(actual_type):
            dtype = expected_type
        elif expected_type == actual_type:
            dtype = expected_type
        else:
            _fail(
                "wide_output.schema_mismatch",
                "Task output type differs from the active dataset schema.",
                column=expected_field.name,
                expected=str(expected_type),
                actual=str(actual_type),
            )
        fields.append(
            pa.field(
                expected_field.name,
                dtype,
                nullable=expected_field.nullable or actual_field.nullable,
            )
        )
    return pa.schema(fields)


def _normalize_wide_parquet(path: Path, schema: pa.Schema) -> None:
    actual = pq.ParquetFile(path).schema_arrow
    if actual == schema:
        return
    table = pq.read_table(path)
    columns = []
    for field in schema:
        column = table.column(field.name)
        if pa.types.is_null(column.type) and not pa.types.is_null(field.type):
            column = pa.chunked_array(
                [pa.nulls(len(chunk), type=field.type) for chunk in column.chunks],
                type=field.type,
            )
        elif column.type != field.type:
            _fail(
                "wide_output.schema_mismatch",
                "Task output cannot be normalized to the active dataset schema.",
                column=field.name,
                expected=str(field.type),
                actual=str(column.type),
            )
        columns.append(column)
    staging = path.with_suffix(path.suffix + ".schema.tmp")
    pq.write_table(pa.Table.from_arrays(columns, schema=schema), staging, compression="zstd")
    staging.replace(path)


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
