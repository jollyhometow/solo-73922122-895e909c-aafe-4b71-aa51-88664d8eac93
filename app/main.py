"""FastAPI application: ZIP upload, job status, processed package download.

Run two replicas against the same data directory, e.g.::

    uvicorn app.main:app --host 0.0.0.0 --port 8001
    uvicorn app.main:app --host 0.0.0.0 --port 8002

The interactive OpenAPI demo is served at ``/docs``.
"""
from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from . import config, database, pipeline

app = FastAPI(
    title="DICOM 去标识化交付 API",
    description=(
        "上传含最多 20 份未压缩 DICOM 的 ZIP，服务端替换患者标识、"
        "持久化随机 UID 映射（支持同院跨批关联与包外引用），"
        "处理完成后下载匿名化 ZIP。两个 API 进程共享同一 SQLite 映射库。"
    ),
    version="1.0.0",
)


@app.on_event("startup")
def _startup() -> None:
    database.init_db()
    # Reclaim reservations of jobs interrupted by a previous process restart;
    # already committed mappings are preserved and reused.
    database.sweep_stale_jobs()


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
async def create_upload_job(
    background: BackgroundTasks, file: UploadFile
) -> dict:
    """Upload a ZIP of <= 20 uncompressed DICOM files.

    Processing runs in the background; poll ``GET /jobs/{id}`` and download
    via ``GET /jobs/{id}/download`` once ``status == "done"``.
    """
    if not (file.filename or "").lower().endswith(".zip"):
        raise HTTPException(415, "only .zip uploads are accepted")

    job_id = uuid.uuid4().hex
    job_dir = config.JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    upload_path = job_dir / "input.zip"

    # Create the job row synchronously so the very first status poll can
    # never race ahead of the background task.
    database.init_db()
    database.create_job(job_id, file.filename or "upload.zip")

    size = 0
    try:
        with upload_path.open("wb") as out:
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > config.MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        413,
                        f"upload exceeds {config.MAX_UPLOAD_BYTES} bytes",
                    )
                out.write(chunk)
    except HTTPException:
        upload_path.unlink(missing_ok=True)
        database.update_job(
            job_id,
            status="failed",
            error=f"upload exceeds {config.MAX_UPLOAD_BYTES} bytes",
        )
        raise

    def _run_pipeline() -> None:
        try:
            pipeline.process_job(job_id, upload_path, file.filename or "upload.zip")
        finally:
            upload_path.unlink(missing_ok=True)

    background.add_task(_run_pipeline)
    return {
        "job_id": job_id,
        "status": "processing",
        "status_url": f"/jobs/{job_id}",
        "download_url": f"/jobs/{job_id}/download",
        "received_by_replica": config.REPLICA_ID,
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
