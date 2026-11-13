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
# during the short reservation/commit transactions.
SQLITE_BUSY_TIMEOUT_MS = int(os.environ.get("SQLITE_BUSY_TIMEOUT_MS", "30000"))
