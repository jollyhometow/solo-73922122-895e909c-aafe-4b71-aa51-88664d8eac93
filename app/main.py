"""FastAPI application: ZIP upload, job status, processed package download.

Run two replicas against the same data directory, e.g.::

    uvicorn app.main:app --host 0.0.0.0 --port 8001
    uvicorn app.main:app --host 0.0.0.0 --port 8002

The interactive OpenAPI demo is served at ``/docs``.

Concurrency notes
=================

Upload registration (and the whole processing pipeline) runs on a dedicated,
bounded worker pool (:mod:`app.runner`) — never on the event loop or on
Starlette's shared thread pool.  Consequently, even when many uploads are
waiting for the SQLite write lock, ``/health`` and the read-only job/mapping
endpoints keep serving committed data immediately (WAL readers do not block
on the writer).  Lock timeouts produce a retryable ``503``; client
disconnects before registration simply cancel the queued upload, while an
already accepted package always finishes in the background.
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import anyio
from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from . import config, database, runner as runner_mod
from .database import DatabaseBusy
from .runner import PendingUpload, QueueFull

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db()
    # Reclaim reservations/staging of jobs interrupted by a previous process
    # restart — including jobs that were registered but never executed.
    # Already committed mappings are preserved and reused.
    swept = database.sweep_stale_jobs()
    if swept["jobs"]:
        logger.info("startup sweep reclaimed %s", swept)
    runner_mod.runner.start()
    try:
        yield
    finally:
        runner_mod.runner.shutdown()


app = FastAPI(
    title="DICOM 去标识化交付 API",
    description=(
        "上传含最多 20 份未压缩 DICOM 的 ZIP，服务端替换患者标识、"
        "持久化随机 UID 映射（支持同院跨批关联与包外引用），"
        "处理完成后下载匿名化 ZIP。两个 API 进程共享同一 SQLite 映射库。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


def _retryable_503(detail: str) -> HTTPException:
    """A 503 the client may retry, with an explicit Retry-After header."""
    exc = HTTPException(503, detail)
    exc.headers = {"Retry-After": "5"}
    return exc


@app.exception_handler(DatabaseBusy)
async def _database_busy_handler(request: Request, exc: DatabaseBusy) -> JSONResponse:
    """Lock timeout while registering an unaccepted upload: retryable 503."""
    return JSONResponse(
        status_code=503,
        headers={"Retry-After": "5"},
        content={
            "detail": (
                "database write lock not acquired within the busy timeout; "
                "the upload was not accepted and may be retried"
            )
        },
    )


@app.get("/", tags=["meta"])
def root() -> dict:
    return {
        "service": "dicom-deid",
        "replica": config.REPLICA_ID,
        "docs": "/docs",
        "endpoints": [
            "POST /jobs",
            "GET  /jobs",
            "GET  /jobs/{job_id}",
            "GET  /jobs/{job_id}/download",
            "GET  /jobs/{job_id}/report",
            "GET  /mappings/stats",
            "GET  /mappings",
            "GET  /samples",
            "GET  /samples/{name}",
            "GET  /health",
        ],
    }


@app.get("/health", tags=["meta"])
def health() -> dict:
    """Replica identity + DB state — call repeatedly through nginx to observe
    round-robin between the two API processes."""
    return {"status": "ok", "replica": config.REPLICA_ID, **database.mapping_stats()}


@app.post("/jobs", status_code=202, tags=["jobs"])
async def create_upload_job(request: Request, file: UploadFile) -> dict:
    """Upload a ZIP of <= 20 uncompressed DICOM files.

    The upload is admitted through a bounded queue and registered on a
    dedicated worker pool; processing continues in the background even if the
    upload response disconnects.  Poll ``GET /jobs/{id}`` and download via
    ``GET /jobs/{id}/download`` once ``status == "done"``.

    Failures:

    * ``503`` (retryable, ``Retry-After`` set) when the bounded queue is full
      or the SQLite write lock cannot be acquired within the 30 s busy timeout;
    * disconnecting while still queued cancels the upload — no job row is ever
      registered after the fact.
    """
    if not (file.filename or "").lower().endswith(".zip"):
        raise HTTPException(415, "only .zip uploads are accepted")

    job_id = uuid.uuid4().hex
    job_dir = config.JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    spool_path = job_dir / "input.zip"

    # Stream the body to the per-job staging file.  Blocking writes are moved
    # off the event loop so /health and read endpoints stay responsive while a
    # large upload is in progress.
    size = 0
    try:
        with spool_path.open("wb") as out:
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > config.MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        413,
                        f"upload exceeds {config.MAX_UPLOAD_BYTES} bytes",
                    )
                await anyio.to_thread.run_sync(out.write, chunk)
    except HTTPException:
        spool_path.unlink(missing_ok=True)
        shutil.rmtree(job_dir, ignore_errors=True)
        raise
    except Exception:
        # Client disconnect or stream error before registration: nothing has
        # touched the database, just remove the partial staging data.
        spool_path.unlink(missing_ok=True)
        shutil.rmtree(job_dir, ignore_errors=True)
        raise

    loop = asyncio.get_running_loop()
    registration: asyncio.Future[bool] = loop.create_future()
    # Consume any exception so an "exception never retrieved" warning cannot
    # fire when the request is cancelled before the outcome is observed.
    registration.add_done_callback(lambda f: f.exception())

    item = PendingUpload(
        job_id=job_id,
        job_dir=job_dir,
        spool_path=spool_path,
        original_name=file.filename or "upload.zip",
        future=registration,
    )

    try:
        runner_mod.runner.submit(item)
    except QueueFull as exc:
        runner_mod.runner.discard_staging(item)
        raise _retryable_503(str(exc)) from exc

    def _on_disconnect() -> None:
        # Only meaningful while the upload is still queued: the worker checks
        # this flag *before* any database work, so a cancelled queued upload
        # never registers late.  Once registration started, acceptance stands.
        item.request_cancel()

    try:
        registered = await asyncio.wait_for(
            registration, timeout=config.REGISTRATION_TIMEOUT_S
        )
    except asyncio.CancelledError:
        _on_disconnect()
        raise
    except asyncio.TimeoutError as exc:
        # Defensive watchdog (the 30 s SQLite busy timeout normally fires
        # first).  If registration later succeeds anyway, the package remains
        # queryable; otherwise startup sweep reclaims it.
        item.request_cancel()
        raise _retryable_503(
            "timed out waiting for upload registration; the request may be retried"
        ) from exc

    if not registered:
        # SQLite busy timeout: the unaccepted upload is rejected with a
        # retryable 503; the worker already removed its private staging data.
        # raise_for_outcome() raises DatabaseBusy, handled by the exception
        # handler above (503 + Retry-After).
        item.raise_for_outcome()
        raise _retryable_503("database busy; retry the upload shortly")

    return {
        "job_id": job_id,
        "status": "processing",
        "status_url": f"/jobs/{job_id}",
        "download_url": f"/jobs/{job_id}/download",
        "received_by_replica": config.REPLICA_ID,
        "queue_depth": runner_mod.runner.queue_depth(),
    }


def _require_job(job_id: str) -> dict:
    job = database.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    return job


@app.get("/jobs", tags=["jobs"])
def list_jobs_endpoint() -> dict:
    return {"jobs": database.list_jobs()}


@app.get("/jobs/{job_id}", tags=["jobs"])
def job_status(job_id: str) -> dict:
    return _require_job(job_id)


@app.get("/jobs/{job_id}/report", tags=["jobs"])
def job_report(job_id: str) -> JSONResponse:
    """Return the per-file original -> anonymized mapping report."""
    job = _require_job(job_id)
    if job["status"] != "done":
        raise HTTPException(
            409, f"report unavailable while job status is {job['status']!r}"
        )
    return JSONResponse(json_safe(job["report_json"]))


@app.get("/jobs/{job_id}/download", tags=["jobs"])
def job_download(job_id: str) -> FileResponse:
    """Download the complete anonymized ZIP. Only available after success."""
    job = _require_job(job_id)
    if job["status"] != "done":
        raise HTTPException(
            409,
            f"package not downloadable while job status is {job['status']!r}; "
            "wait for 'done' (failed jobs keep no output and no new mappings)",
        )
    result_path = config.JOBS_DIR / job_id / job["result_name"]
    if not result_path.exists():
        # Package file vanished from storage; surface a consistent error
        # instead of a 500. Mapping rows stay committed (they remain valid).
        raise HTTPException(410, "result package no longer available on this replica")
    download_name = Path(job["original_name"]).stem + "_anonymized.zip"
    return FileResponse(
        result_path,
        media_type="application/zip",
        filename=download_name,
    )


@app.get("/mappings/stats", tags=["mappings"])
def mappings_stats() -> dict:
    """Committed mapping counts plus any in-flight (uncommitted) reservations."""
    return database.mapping_stats()


@app.get("/mappings", tags=["mappings"])
def committed_mappings(id_type: str | None = None) -> dict:
    """Inspect committed persistent mappings (filter by patient/study/series/
    instance).  Uncommitted in-flight reservations are never exposed."""
    if id_type is not None and id_type not in (
        "patient",
        "study",
        "series",
        "instance",
    ):
        raise HTTPException(422, "id_type must be patient|study|series|instance")
    return {"mappings": database.get_committed_mappings(id_type)}


@app.get("/samples", tags=["samples"])
def list_samples() -> dict:
    """Synthetic demo ZIPs (no real personal information) bundled in the image."""
    items = sorted(p.name for p in config.SAMPLES_DIR.glob("*.zip"))
    return {"samples": items, "download_url_template": "/samples/{name}"}


@app.get("/samples/{name}", tags=["samples"])
def sample_download(name: str) -> FileResponse:
    if "/" in name or ".." in name or not name.endswith(".zip"):
        raise HTTPException(400, "invalid sample name")
    path = config.SAMPLES_DIR / name
    if not path.exists():
        raise HTTPException(404, "sample not found")
    return FileResponse(path, media_type="application/zip", filename=name)


def json_safe(raw: str | None):
    import json

    return json.loads(raw) if raw else None
