"""Bounded upload admission and a dedicated background processing pool.

Why this module exists
======================

The two API replicas share one SQLite file.  When the write lock is held,
other uploads' *registration* (the short ``INSERT`` into ``jobs``) blocks for
the configured 30 s busy timeout.  Running that wait on the event loop (or on
Starlette's shared worker-thread pool) stalls every endpoint of the replica —
including ``/health`` and read-only task / mapping queries that never need the
write lock.

Design
======

* Uploads are admitted through a **bounded** in-memory queue.  At most
  ``UPLOAD_QUEUE_LIMIT`` uploads may wait for a worker at any moment, so the
  execution resources consumed by lock-waiting uploads are always bounded;
  anything beyond that receives an immediate retryable ``503``.
* A **dedicated** pool of ``UPLOAD_WORKERS`` daemon threads performs the
  database registration and the whole pipeline.  The shared anyio / Starlette
  thread pool is never touched and therefore never enlarged.
* The HTTP coroutine waits on an ``asyncio.Future`` while registration is in
  flight: waiting itself consumes no worker thread.
* Client cancellation semantics:

  - if the request is abandoned while the upload is still queued (no database
    work has started), the item is discarded and never registered — no late
    ``processing`` row can appear;
  - once registration has begun (or finished), disconnecting the upload
    response no longer matters: an *accepted* image package keeps processing
    in the background, exactly as the specification requires;
  - a worker only releases its slot after the in-flight database operation has
    completed its commit/rollback and the connection has been closed.
* On lock timeout the unaccepted upload gets a retryable ``503``; a task that
  was already registered is marked ``failed`` and its exclusively-owned new
  mappings and staging directory are cleaned up.
"""
from __future__ import annotations

import logging
import os
import queue
import shutil
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from . import config, database, pipeline

if TYPE_CHECKING:
    import asyncio

logger = logging.getLogger(__name__)


def _registration_gate_path() -> Path | None:
    """Optional pre-registration pause used solely by the cancellation demo.

    When ``UPLOAD_REGISTRATION_GATE`` points at an existing file, every worker
    blocks *before* any database work until the file disappears.  No gate is
    configured by default, so production behaviour is unchanged.
    """
    raw = os.environ.get("UPLOAD_REGISTRATION_GATE")
    return Path(raw) if raw else None


class UploadRejected(RuntimeError):
    """Base class for retryable admission failures (mapped to HTTP 503)."""


class QueueFull(UploadRejected):
    """The bounded upload queue is saturated; the client may retry."""


class RegistrationTimeout(UploadRejected):
    """Registration did not complete within the request-side watchdog."""


@dataclass
class PendingUpload:
    """One upload moving through the bounded queue."""

    job_id: str
    job_dir: Path
    spool_path: Path
    original_name: str
    future: "asyncio.Future[bool]"
    _cancel_event: threading.Event = field(default_factory=threading.Event)

    # Registration outcome, set exactly once by the worker.
    _settled: bool = field(default=False, init=False)
    _busy: bool = field(default=False, init=False)
    _error: Exception | None = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def request_cancel(self) -> None:
        """Client disconnected while the HTTP request was still in flight."""
        self._cancel_event.set()

    def cancel_requested(self) -> bool:
        return self._cancel_event.is_set()

    # -- worker side ---------------------------------------------------------
    def settle_registered(self) -> None:
        with self._lock:
            if self._settled:
                return
            self._settled = True
        if not self.future.done():
            self.future.get_loop().call_soon_threadsafe(
                self._safe_set_result, True
            )

    def settle_busy(self) -> None:
        with self._lock:
            if self._settled:
                return
            self._settled = True
            self._busy = True
        if not self.future.done():
            self.future.get_loop().call_soon_threadsafe(
                self._safe_set_result, False
            )

    def settle_error(self, exc: Exception) -> None:
        with self._lock:
            if self._settled:
                return
            self._settled = True
            self._error = exc
        if not self.future.done():
            self.future.get_loop().call_soon_threadsafe(
                self._safe_set_exception, exc
            )

    def _safe_set_result(self, value: bool) -> None:
        if not self.future.done():
            self.future.set_result(value)

    def _safe_set_exception(self, exc: Exception) -> None:
        if not self.future.done():
            self.future.set_exception(exc)

    # -- HTTP side -----------------------------------------------------------
    def raise_for_outcome(self) -> None:
        """Translate the settled registration outcome into an API error."""
        if self._error is not None:
            raise self._error
        if self._busy:
            raise database.DatabaseBusy(
                "database write lock not acquired within the busy timeout"
            )


class UploadRunner:
    """Process-local bounded executor for upload registration and pipelines."""

    def __init__(
        self,
        workers: int | None = None,
        queue_limit: int | None = None,
    ) -> None:
        self._workers = workers or config.UPLOAD_WORKERS
        self._queue: "queue.Queue[PendingUpload | None]" = queue.Queue(
            maxsize=queue_limit or config.UPLOAD_QUEUE_LIMIT
        )
        self._threads: list[threading.Thread] = []
        self._started = False
        self._start_lock = threading.Lock()

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> None:
        with self._start_lock:
            if self._started:
                return
            for i in range(self._workers):
                thread = threading.Thread(
                    target=self._worker_loop,
                    name=f"upload-worker-{i + 1}",
                    daemon=True,
                )
                thread.start()
                self._threads.append(thread)
            self._started = True
            logger.info(
                "upload runner started: %d dedicated workers, queue limit %d",
                self._workers,
                self._queue.maxsize,
            )

    def shutdown(self) -> None:
        """Stop accepting work and let queued items fail fast.

        In-flight database operations always run to commit/rollback and close
        their connections before the worker thread exits.  Anything still
        queued is rejected here; if it had already been registered in a
        previous incarnation, the startup sweep reclaims it on the next boot.
        """
        with self._start_lock:
            if not self._started:
                return
            self._started = False
        for _ in self._threads:
            self._queue.put(None)
        for thread in self._threads:
            thread.join(timeout=config.SQLITE_BUSY_TIMEOUT_MS / 1000.0 + 10)
        # Drain anything still queued and release its waiters.
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is not None:
                item.settle_error(
                    RegistrationTimeout("service is shutting down; retry shortly")
                )
                self.discard_staging(item)
        self._threads.clear()

    # -- admission -----------------------------------------------------------
    def submit(self, item: PendingUpload) -> None:
        """Enqueue an upload or raise :class:`QueueFull` immediately."""
        try:
            self._queue.put_nowait(item)
        except queue.Full as exc:
            raise QueueFull(
                "upload queue is saturated; retry after a short backoff"
            ) from exc

    def queue_depth(self) -> int:
        return self._queue.qsize()

    # -- workers -------------------------------------------------------------
    def _worker_loop(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            try:
                self._process(item)
            finally:
                self._queue.task_done()

    def _process(self, item: PendingUpload) -> None:
        # The HTTP request may have been abandoned while this upload was still
        # queued.  Nothing has touched the database yet, so simply discard it:
        # a cancelled, queued upload must never register late.
        if item.cancel_requested() and not item._settled:
            logger.info("upload %s cancelled while queued; not registering", item.job_id)
            self.discard_staging(item)
            # The HTTP side has gone; no future resolution is observed.
            return

        # Demo-only pause point (no-op unless UPLOAD_REGISTRATION_GATE is set):
        # lets the cancellation demo hold uploads in the queue deterministically
        # while still honouring disconnects observed above on the next pass.
        gate = _registration_gate_path()
        if gate is not None:
            while gate.exists() and not item.cancel_requested():
                time.sleep(0.1)
            if item.cancel_requested() and not item._settled:
                logger.info(
                    "upload %s cancelled at the registration gate; not registering",
                    item.job_id,
                )
                self.discard_staging(item)
                return

        try:
            # Synchronous registration: the first and only write the upload
            # needs before acceptance.  On lock contention this waits for the
            # full SQLite busy timeout (default 30 s) inside this dedicated
            # worker — readers on the event loop stay unaffected.
            database.create_job(item.job_id, item.original_name)
        except database.DatabaseBusy:
            logger.warning(
                "upload %s not accepted: database busy timeout", item.job_id
            )
            item.settle_busy()
            self.discard_staging(item)
            return
        except Exception as exc:  # noqa: BLE001 - surface any other DB failure
            logger.exception("upload %s registration failed", item.job_id)
            item.settle_error(exc)
            self.discard_staging(item)
            return

        item.settle_registered()

        # From here on the package is *accepted*: processing continues even if
        # the upload response has disconnected.  The slot is held until the
        # pipeline (including its final commit/rollback and connection close)
        # finishes, so the bounded concurrency guarantee cannot be violated.
        try:
            pipeline.process_job(item.job_id, item.spool_path, item.original_name)
        except Exception:  # noqa: BLE001 - pipeline records its own failure
            logger.exception("pipeline for job %s raised unexpectedly", item.job_id)
        finally:
            item.spool_path.unlink(missing_ok=True)

    @staticmethod
    def discard_staging(item: PendingUpload) -> None:
        """Remove the exclusively-owned spool file / job staging directory."""
        item.spool_path.unlink(missing_ok=True)
        shutil.rmtree(item.job_dir, ignore_errors=True)


# Process-wide singleton used by the FastAPI application and tests.
runner = UploadRunner()
