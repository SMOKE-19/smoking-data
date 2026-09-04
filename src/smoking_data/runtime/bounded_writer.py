from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar, cast

T = TypeVar("T")
R = TypeVar("R")
_STOP = object()


@dataclass(frozen=True, slots=True)
class _QueuedItem(Generic[T]):
    value: T
    rows: int


class WriterSession(Protocol[T, R]):
    """Format-specific state owned exclusively by the writer thread."""

    def write(self, item: T) -> None: ...

    def finish(self) -> R: ...

    def abort(self) -> None: ...


class WriterSessionFactory(Protocol[T, R]):
    def __call__(self) -> WriterSession[T, R]: ...


@dataclass(frozen=True, slots=True)
class BoundedWriterProfile:
    queue_capacity_batches: int
    batches_produced: int
    batches_written: int
    rows_produced: int
    rows_written: int
    queue_send_wait_sec: float
    writer_write_sec: float
    writer_finalize_sec: float

    def to_dict(self) -> dict[str, int | float]:
        return {
            "writer_pipeline_enabled": 1,
            "writer_thread_count": 1,
            "writer_queue_capacity_batches": self.queue_capacity_batches,
            "writer_batches_produced": self.batches_produced,
            "writer_batches_written": self.batches_written,
            "writer_rows_produced": self.rows_produced,
            "writer_rows_written": self.rows_written,
            "writer_queue_send_wait_sec": self.queue_send_wait_sec,
            "writer_write_sec": self.writer_write_sec,
            "writer_finalize_sec": self.writer_finalize_sec,
        }


@dataclass(frozen=True, slots=True)
class BoundedWriterResult(Generic[R]):
    value: R
    profile: BoundedWriterProfile


class BoundedWriterPipeline(Generic[T, R]):
    """Bounded producer→writer bridge with writer-thread-owned state."""

    def __init__(
        self,
        session_factory: WriterSessionFactory[T, R],
        *,
        capacity_batches: int = 2,
        thread_name: str = "smoking-data-writer",
    ) -> None:
        self.capacity = max(1, int(capacity_batches))
        self._queue: queue.Queue[_QueuedItem[T] | object] = queue.Queue(
            maxsize=self.capacity
        )
        self._session_factory = session_factory
        self._error: BaseException | None = None
        self._result: R | None = None
        self._result_ready = False
        self._aborted = threading.Event()
        self._finished = False
        self._batches_produced = 0
        self._batches_written = 0
        self._rows_produced = 0
        self._rows_written = 0
        self._send_wait_sec = 0.0
        self._write_sec = 0.0
        self._finalize_sec = 0.0
        self._thread = threading.Thread(target=self._run, name=thread_name, daemon=True)
        self._thread.start()

    def submit(self, item: T, *, rows: int) -> None:
        if self._finished:
            raise RuntimeError("bounded writer pipeline is already closed")
        started = time.perf_counter()
        self._put(_QueuedItem(value=item, rows=int(rows)))
        self._send_wait_sec += time.perf_counter() - started
        self._batches_produced += 1
        self._rows_produced += int(rows)

    def finish(self) -> BoundedWriterResult[R]:
        if self._finished:
            raise RuntimeError("bounded writer pipeline is already closed")
        self._finished = True
        self._put(_STOP)
        self._thread.join()
        self._raise_if_failed()
        if not self._result_ready:
            raise RuntimeError("bounded writer pipeline produced no result")
        if (
            self._batches_written != self._batches_produced
            or self._rows_written != self._rows_produced
        ):
            raise RuntimeError(
                "bounded writer count mismatch: "
                f"produced_batches={self._batches_produced} "
                f"written_batches={self._batches_written} "
                f"produced_rows={self._rows_produced} "
                f"written_rows={self._rows_written}"
            )
        return BoundedWriterResult(
            value=cast(R, self._result),
            profile=BoundedWriterProfile(
                queue_capacity_batches=self.capacity,
                batches_produced=self._batches_produced,
                batches_written=self._batches_written,
                rows_produced=self._rows_produced,
                rows_written=self._rows_written,
                queue_send_wait_sec=self._send_wait_sec,
                writer_write_sec=self._write_sec,
                writer_finalize_sec=self._finalize_sec,
            ),
        )

    def abort(self) -> None:
        if self._finished:
            return
        self._finished = True
        self._aborted.set()
        self._put(_STOP, allow_failure=True)
        self._thread.join()

    def _put(
        self, item: _QueuedItem[T] | object, *, allow_failure: bool = False
    ) -> None:
        while True:
            if not allow_failure:
                self._raise_if_failed()
            if not self._thread.is_alive():
                if allow_failure:
                    return
                self._raise_if_failed()
                raise RuntimeError("bounded writer thread stopped unexpectedly")
            try:
                self._queue.put(item, timeout=0.05)
                return
            except queue.Full:
                continue

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise RuntimeError("bounded writer pipeline failed") from self._error

    def _run(self) -> None:
        session: WriterSession[T, R] | None = None
        try:
            session = self._session_factory()
            while True:
                item = self._queue.get()
                if item is _STOP:
                    break
                queued = cast(_QueuedItem[T], item)
                started = time.perf_counter()
                session.write(queued.value)
                self._write_sec += time.perf_counter() - started
                self._batches_written += 1
                self._rows_written += queued.rows
            if self._aborted.is_set():
                session.abort()
                return
            started = time.perf_counter()
            self._result = session.finish()
            self._result_ready = True
            self._finalize_sec = time.perf_counter() - started
        except BaseException as exc:  # noqa: BLE001 - handed back to producer thread.
            self._error = exc
            if session is not None:
                try:
                    session.abort()
                except BaseException:
                    pass
