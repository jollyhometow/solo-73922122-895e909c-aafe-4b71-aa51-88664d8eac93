"""Runtime configuration.

The two API processes (docker compose service api1 / api2) point at the same
shared data directory (a named volume), so they share one SQLite database and
one package store.  Every setting can be overridden with an environment
variable.
"""
from __future__ import annotations

import os
from pathlib import Path


def _data_dir() -> Path:
    env = os.environ.get("DICOM_DATA_DIR")
    if env:
        path = Path(env).resolve()
    else:
        preferred = Path("/data")
        # Inside the container /data is the writable shared volume; on a bare
        # development host fall back to a repository-local directory.
        if os.access("/", os.W_OK) and os.path.isdir(preferred):
            path = preferred
        else:
            path = (Path(__file__).resolve().parents[1] / "data").resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


DATA_DIR: Path = _data_dir()
DB_PATH: Path = DATA_DIR / "mappings.db"
JOBS_DIR: Path = DATA_DIR / "jobs"
SAMPLES_DIR: Path = DATA_DIR / "samples"
SAMPLE_OUTPUT_DIR: Path = DATA_DIR / "sample_output"

for _p in (JOBS_DIR, SAMPLES_DIR, SAMPLE_OUTPUT_DIR):
    _p.mkdir(parents=True, exist_ok=True)

# Hard limits required by the specification.
MAX_DICOMS_PER_PACKAGE = int(os.environ.get("MAX_DICOMS_PER_PACKAGE", "20"))
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(200 * 1024 * 1024)))
MAX_TOTAL_EXTRACTED_BYTES = int(
    os.environ.get("MAX_TOTAL_EXTRACTED_BYTES", str(500 * 1024 * 1024))
)

# Replica identity (api1 / api2 / ...).  Shown in /health so the round-robin
# between the two processes is observable from /docs.
REPLICA_ID = os.environ.get("REPLICA_ID", "api-local")

# DICOM UID root used to mint replacement UIDs (2.25 = UUID-based OID branch).
UID_ORG_ROOT = os.environ.get("UID_ORG_ROOT", "2.25")
PATIENT_ID_PREFIX = os.environ.get("PATIENT_ID_PREFIX", "ANON")

# SQLite busy timeout: the other replica may briefly hold the write lock
# during the short reservation/commit transactions.  Uploads keep waiting for
# exactly this long (default 30 s); on timeout the unaccepted upload gets a
# retryable 503 instead of blocking the replica indefinitely.
SQLITE_BUSY_TIMEOUT_MS = int(os.environ.get("SQLITE_BUSY_TIMEOUT_MS", "30000"))

# Bounded upload admission (per replica).  Upload registration and the whole
# processing pipeline run on a *dedicated* worker-thread pool of this size;
# the shared anyio/Starlette thread pool is never enlarged.  At most
# UPLOAD_QUEUE_LIMIT uploads may additionally wait in the in-memory queue, so
# the resources consumed by lock-waiting uploads are always bounded; anything
# beyond that receives an immediate retryable 503.
UPLOAD_WORKERS = int(os.environ.get("UPLOAD_WORKERS", "8"))
UPLOAD_QUEUE_LIMIT = int(os.environ.get("UPLOAD_QUEUE_LIMIT", "64"))

# Upper bound an HTTP request spends waiting for its job row to be registered.
# Must stay above SQLITE_BUSY_TIMEOUT_MS so the busy-timeout 503 (not this
# generic watchdog) is what clients observe under lock contention.
REGISTRATION_TIMEOUT_S = float(
    os.environ.get("REGISTRATION_TIMEOUT_S", str(SQLITE_BUSY_TIMEOUT_MS / 1000.0 + 5))
)

# How many times failure bookkeeping (rollback + job='failed') retries when
# the write lock is contested, before leaving it to the startup sweep.
FAIL_JOB_RETRIES = int(os.environ.get("FAIL_JOB_RETRIES", "3"))
