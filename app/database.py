"""SQLite persistence for identifier mappings and processing jobs.

Concurrency model (two API processes share one database file)
================================================================

* The database lives in WAL mode; readers never block writers and vice versa.
* A mapping is created in two phases:

  1. ``reserve_mappings`` runs in one *short* write transaction.  Every
     identifier referenced by a package (including out-of-package references)
     is either mapped to an existing row or gets a brand-new row owned by this
     job with ``committed = 0``.  The transaction ends immediately, so the
     DICOM rewriting / ZIP packaging never holds a database write lock.
  2. When the whole package has been generated successfully, ``commit_job``
     flips the job's rows to ``committed = 1`` in another short transaction.
     Only then does the package become available for download.

* If packaging fails, ``fail_job`` deletes only rows still owned by the job
  and not re-adopted by another job.  No new mapping survives a failed
  package, while mappings committed by earlier packages are untouched.
* On startup, rows belonging to jobs that died mid-flight are reclaimed by
  ``sweep_stale_jobs`` so restarts keep reusing already stored mappings.
"""
from __future__ import annotations

import logging
import shutil
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from . import config

logger = logging.getLogger(__name__)


class DatabaseBusy(sqlite3.OperationalError):
    """Raised when the SQLite write lock cannot be acquired within the busy
    timeout.  Mapped to a retryable 503 for unaccepted uploads."""

SCHEMA = """
CREATE TABLE IF NOT EXISTS id_mappings (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    id_type          TEXT NOT NULL CHECK (id_type IN ('patient','study','series','instance')),
    original_value   TEXT NOT NULL,
    anonymized_value TEXT NOT NULL,
    owner_job        TEXT,
    committed        INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    UNIQUE (id_type, original_value)
);
CREATE INDEX IF NOT EXISTS idx_mappings_owner ON id_mappings(owner_job);
CREATE INDEX IF NOT EXISTS idx_mappings_committed ON id_mappings(committed);

CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    status        TEXT NOT NULL CHECK (status IN ('processing','done','failed')),
    original_name TEXT NOT NULL,
    result_name   TEXT,
    report_json   TEXT,
    error         TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    """Open a connection with WAL + foreign keys and safe concurrency settings.

    Note: the caller owns the connection lifetime.  Prefer the
    :func:`connection` context manager, which always closes it — a worker slot
    may only be released once the connection is actually closed.
    """
    path = db_path or config.DB_PATH
    conn = sqlite3.connect(
        str(path),
        timeout=config.SQLITE_BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,  # explicit BEGIN/COMMIT, no implicit transactions
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=%d" % config.SQLITE_BUSY_TIMEOUT_MS)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def connection(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    """Open a connection and guarantee it is closed (and its lock released) on
    every path, including exceptions and busy timeouts."""
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


_BUSY_MARKERS = ("database is locked", "database table is locked")


def is_busy_error(exc: BaseException) -> bool:
    """True when ``exc`` is a SQLite lock/busy timeout rather than a logic
    error — such failures are retryable."""
    return isinstance(exc, sqlite3.OperationalError) and any(
        marker in str(exc) for marker in _BUSY_MARKERS
    )


def raise_if_busy(exc: Exception) -> None:
    """Re-raise lock-timeout errors as :class:`DatabaseBusy`."""
    if is_busy_error(exc):
        raise DatabaseBusy(str(exc)) from exc


def init_db() -> None:
    with connection() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        # Acquire failure: no transaction is open, nothing to roll back.
        raise_if_busy(exc)
        raise
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------- jobs

def create_job(job_id: str, original_name: str) -> None:
    """Insert the processing job row synchronously.

    A lock timeout propagates as :class:`DatabaseBusy` so the upload layer
    can answer with a retryable 503 instead of swallowing it.
    """
    now = _utcnow()
    try:
        with connection() as conn:
            conn.execute(
                "INSERT INTO jobs (id, status, original_name, created_at, updated_at) "
                "VALUES (?, 'processing', ?, ?, ?)",
                (job_id, original_name, now, now),
            )
    except sqlite3.OperationalError as exc:
        raise_if_busy(exc)
        raise


def update_job(job_id: str, **fields: str) -> None:
    if not fields:
        return
    fields["updated_at"] = _utcnow()
    assignments = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [job_id]
    with connection() as conn:
        conn.execute(f"UPDATE jobs SET {assignments} WHERE id = ?", values)


def get_job(job_id: str) -> dict | None:
    with connection() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row else None


def list_jobs(limit: int = 100) -> list[dict]:
    with connection() as conn:
        rows = conn.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def sweep_stale_jobs() -> dict[str, int]:
    """Reclaim work of jobs that never finished (process restart/crash).

    Processing jobs that no longer exist as a live task are treated as dead:
    their uncommitted rows are deleted, the job is marked failed, and its
    private staging directory is removed.  Committed rows are never owned by a
    job and are therefore untouched.  This also handles jobs that were
    registered but whose worker never got to run them.
    """
    removed = 0
    with connection() as conn:
        stale = [
            r["id"]
            for r in conn.execute(
                "SELECT id FROM jobs WHERE status = 'processing'"
            ).fetchall()
        ]
        for job_id in stale:
            with transaction(conn):
                cur = conn.execute(
                    "DELETE FROM id_mappings WHERE owner_job = ? AND committed = 0",
                    (job_id,),
                )
                removed += cur.rowcount
                conn.execute(
                    "UPDATE jobs SET status = 'failed', error = ?, updated_at = ? "
                    "WHERE id = ? AND status = 'processing'",
                    ("interrupted by service restart", _utcnow(), job_id),
                )
    # Each stale job owns an exclusive staging directory under JOBS_DIR.
    cleaned_dirs = 0
    for job_id in stale:
        job_dir = config.JOBS_DIR / job_id
        if job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)
            cleaned_dirs += 1
    return {"jobs": len(stale), "mappings": removed, "staging_dirs": cleaned_dirs}


# ------------------------------------------------------------------- reservations

def reserve_mappings(
    job_id: str,
    requested: dict[str, set[str]],
    generator,
) -> dict[str, dict[str, str]]:
    """Reserve an anonymized value for every requested identifier.

    ``requested`` maps id_type -> set of original values (empty/blank values
    must be filtered out by the caller).  ``generator(id_type)`` mints a fresh
    unique anonymized value.

    Returns ``{id_type: {original: anonymized}}`` covering *all* requested
    identifiers, whether they already existed or were created by this call.
    SQLite serialises write transactions, so when two packages first meet the
    same identifier concurrently the first writer creates the row and the
    second writer reuses it: both packages end up with the same mapping.
    """
    now = _utcnow()
    result: dict[str, dict[str, str]] = {t: {} for t in requested}
    with connection() as conn:
        with transaction(conn):
            for id_type, values in requested.items():
                for original in sorted(values):
                    row = conn.execute(
                        "SELECT anonymized_value FROM id_mappings "
                        "WHERE id_type = ? AND original_value = ?",
                        (id_type, original),
                    ).fetchone()
                    if row is not None:
                        # Existing (committed, or reserved by another in-flight
                        # job) mapping: reuse it so cross-package linkage is
                        # stable.  Re-adoption means a later failure of this job
                        # must not delete the row.
                        result[id_type][original] = row["anonymized_value"]
                        continue
                    anonymized = generator(id_type)
                    while conn.execute(
                        "SELECT 1 FROM id_mappings WHERE anonymized_value = ?",
                        (anonymized,),
                    ).fetchone():
                        anonymized = generator(id_type)
                    conn.execute(
                        "INSERT INTO id_mappings (id_type, original_value, "
                        "anonymized_value, owner_job, committed, created_at) "
                        "VALUES (?, ?, ?, ?, 0, ?)",
                        (id_type, original, anonymized, job_id, now),
                    )
                    result[id_type][original] = anonymized
    return result


def commit_job(job_id: str) -> int:
    """Mark all rows owned by the job as committed and unowned."""
    with connection() as conn:
        with transaction(conn):
            cur = conn.execute(
                "UPDATE id_mappings SET committed = 1, owner_job = NULL "
                "WHERE owner_job = ?",
                (job_id,),
            )
            return cur.rowcount


def fail_job(job_id: str, error: str) -> None:
    """Roll back: delete rows still owned by this job, then mark it failed."""
    with connection() as conn:
        with transaction(conn):
            conn.execute(
                "DELETE FROM id_mappings WHERE owner_job = ? AND committed = 0",
                (job_id,),
            )
            conn.execute(
                "UPDATE jobs SET status = 'failed', error = ?, updated_at = ? "
                "WHERE id = ?",
                (error, _utcnow(), job_id),
            )


def record_failure(job_id: str, error: str) -> bool:
    """Best-effort failure bookkeeping with a bounded number of lock retries.

    Returns ``True`` when the rollback was persisted.  On permanent failure the
    job is left for :func:`sweep_stale_jobs` at the next startup, so no
    uncommitted mapping or processing row can leak forever.  The connection is
    closed (releasing any wait slot) after every attempt.
    """
    message = (error or "unknown error")[:2000]
    last_exc: Exception | None = None
    for attempt in range(max(1, config.FAIL_JOB_RETRIES)):
        try:
            fail_job(job_id, message)
            return True
        except Exception as exc:  # noqa: BLE001 - retry any transient DB error
            last_exc = exc
            logger.warning(
                "fail_job(%s) attempt %d/%d failed: %s",
                job_id,
                attempt + 1,
                config.FAIL_JOB_RETRIES,
                exc,
            )
            time.sleep(1.0)
    logger.error(
        "could not record failure for job %s after %d attempts (%s); "
        "startup sweep will reclaim it",
        job_id,
        config.FAIL_JOB_RETRIES,
        last_exc,
    )
    return False


# ---------------------------------------------------------------------- queries

def mapping_stats() -> dict[str, int]:
    with connection() as conn:
        rows = conn.execute(
            "SELECT id_type, committed, COUNT(*) AS n FROM id_mappings "
            "GROUP BY id_type, committed"
        ).fetchall()
    stats: dict[str, int] = {
        "patient_committed": 0,
        "study_committed": 0,
        "series_committed": 0,
        "instance_committed": 0,
        "uncommitted_reservations": 0,
    }
    for r in rows:
        if r["committed"]:
            stats[f"{r['id_type']}_committed"] = r["n"]
        else:
            stats["uncommitted_reservations"] += r["n"]
    return stats


def get_committed_mappings(id_type: str | None = None) -> list[dict]:
    sql = (
        "SELECT id_type, original_value, anonymized_value, created_at "
        "FROM id_mappings WHERE committed = 1"
    )
    params: tuple = ()
    if id_type:
        sql += " AND id_type = ?"
        params = (id_type,)
    sql += " ORDER BY id_type, id"
    with connection() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
