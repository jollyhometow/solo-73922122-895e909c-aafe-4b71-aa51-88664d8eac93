"""Tests for bounded upload admission, lock waits, cancellation and cleanup.

These cover the behaviours required on top of the mapping pipeline:

* upload registration waiting for the SQLite write lock never blocks
  ``/health`` or read-only job/mapping queries;
* the dedicated worker pool is bounded and a saturated queue answers with a
  retryable 503 (no shared/Starlette thread pool is involved);
* SQLite busy timeout on an *unaccepted* upload -> 503, no ``processing`` row,
  no staging directory left behind;
* busy timeout *after* registration -> the background job is marked failed,
  its exclusive new mappings and staging data are removed;
* cancellation while queued never registers a job late, while an already
  accepted package keeps processing after the upload request disconnects;
* every connection is closed (commit or rollback) before a slot is released.
"""
from __future__ import annotations

import asyncio
import sqlite3
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app import config, database, main as main_mod
from app import runner as runner_mod
from app.runner import PendingUpload, QueueFull, UploadRunner
from scripts import generate_samples


@pytest.fixture()
def temp_data(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(config, "DB_PATH", data_dir / "mappings.db")
    monkeypatch.setattr(config, "JOBS_DIR", data_dir / "jobs")
    monkeypatch.setattr(config, "SAMPLES_DIR", data_dir / "samples")
    monkeypatch.setattr(config, "SAMPLE_OUTPUT_DIR", data_dir / "sample_output")
    for p in (config.JOBS_DIR, config.SAMPLES_DIR, config.SAMPLE_OUTPUT_DIR):
        p.mkdir(parents=True, exist_ok=True)
    database.init_db()
    generate_samples.generate(config.SAMPLES_DIR)
    return data_dir


@pytest.fixture()
def sample_zip_bytes():
    path = config.SAMPLES_DIR / "sample_crossref_a.zip"
    return path.read_bytes()


@pytest.fixture()
def client(monkeypatch, temp_data):
    """A TestClient backed by a fresh, small dedicated runner per test."""
    test_runner = UploadRunner(workers=2, queue_limit=2)
    monkeypatch.setattr(runner_mod, "runner", test_runner)
    with TestClient(main_mod.app) as client:
        yield client


def _upload(client, blob, name="pkg.zip"):
    return client.post(
        "/jobs",
        files={"file": (name, blob, "application/zip")},
    )


# --------------------------------------------------------------------- happy path

def test_accepted_upload_processes_end_to_end(client, sample_zip_bytes):
    resp = _upload(client, sample_zip_bytes)
    assert resp.status_code == 202, resp.text
    job_id = resp.json()["job_id"]
    assert "queue_depth" in resp.json()

    deadline = time.time() + 30
    while time.time() < deadline:
        job = client.get(f"/jobs/{job_id}").json()
        if job["status"] in ("done", "failed"):
            break
        time.sleep(0.1)
    assert job["status"] == "done", job
    assert client.get(f"/jobs/{job_id}/download").status_code == 200
    # Done jobs keep their result directory; the spool upload is removed.
    assert not (config.JOBS_DIR / job_id / "input.zip").exists()


# --------------------------------------------------------------- read availability

def test_reads_stay_responsive_while_uploads_wait_on_write_lock(
    monkeypatch, client, sample_zip_bytes
):
    """A long-held external write lock queues uploads; reads stay < 1 s."""
    monkeypatch.setattr(config, "SQLITE_BUSY_TIMEOUT_MS", 3000)
    monkeypatch.setattr(config, "REGISTRATION_TIMEOUT_S", 15.0)

    # Hold the SQLite write lock from another connection, like a second
    # replica's in-flight write transaction.
    holder = database.connect()
    holder.execute("BEGIN IMMEDIATE")
    try:
        results: list[int] = []
        threads = [
            threading.Thread(
                target=lambda: results.append(_upload(client, sample_zip_bytes).status_code)
            )
            for _ in range(4)
        ]
        for t in threads:
            t.start()
        # Give the workers time to enter the lock wait.
        time.sleep(0.8)

        for path in ("/health", "/jobs", "/mappings", "/mappings/stats"):
            start = time.time()
            resp = client.get(path)
            elapsed = time.time() - start
            assert resp.status_code == 200, path
            assert elapsed < 1.0, f"{path} took {elapsed:.2f}s while uploads waited"

        for t in threads:
            t.join(timeout=30)
        # Every unaccepted upload hit the 3 s busy timeout -> retryable 503.
        assert results and all(code == 503 for code in results), results
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    # No processing row and no staging directory survive the rejected uploads.
    assert all(j["status"] != "processing" for j in database.list_jobs())
    assert list(config.JOBS_DIR.iterdir()) == []


def test_saturated_queue_returns_retryable_503(monkeypatch, temp_data, sample_zip_bytes):
    # One worker + one queue slot: the first upload occupies the worker, the
    # second fills the queue, the third must be rejected immediately.
    test_runner = UploadRunner(workers=1, queue_limit=1)
    monkeypatch.setattr(runner_mod, "runner", test_runner)

    block = threading.Event()
    entered = threading.Event()
    real_create = database.create_job

    def slow_create(job_id, original_name):
        entered.set()
        block.wait(timeout=10)
        return real_create(job_id, original_name)

    monkeypatch.setattr(database, "create_job", slow_create)

    results: list[int] = []
    with TestClient(main_mod.app) as client:
        first = threading.Thread(
            target=lambda: results.append(_upload(client, sample_zip_bytes).status_code)
        )
        first.start()
        assert entered.wait(timeout=5)

        queued = threading.Thread(
            target=lambda: results.append(_upload(client, sample_zip_bytes).status_code)
        )
        queued.start()
        time.sleep(0.5)

        rejected = _upload(client, sample_zip_bytes)
        assert rejected.status_code == 503
        assert rejected.headers.get("retry-after") == "5"
        assert "retry" in rejected.json()["detail"].lower()

        block.set()
        first.join(timeout=15)
        queued.join(timeout=15)

    assert sorted(results) == [202, 202]
    # The rejected upload never created a job row.
    assert len(database.list_jobs()) == 2


# ------------------------------------------------------------- busy timeout handling

def test_busy_timeout_after_registration_marks_job_failed_and_cleans(
    monkeypatch, temp_data, sample_zip_bytes
):
    """Lock timeout on the *reservation* stage: the job is already accepted,
    so it must end up failed with its new mappings and staging removed."""
    test_runner = UploadRunner(workers=1, queue_limit=4)
    monkeypatch.setattr(runner_mod, "runner", test_runner)
    monkeypatch.setattr(config, "SQLITE_BUSY_TIMEOUT_MS", 300)
    monkeypatch.setattr(config, "FAIL_JOB_RETRIES", 3)

    holder = sqlite3.connect(
        str(config.DB_PATH), timeout=5, isolation_level=None, check_same_thread=False
    )
    holder.row_factory = sqlite3.Row
    holder.execute("PRAGMA journal_mode=WAL")
    holder.execute("PRAGMA busy_timeout=5000")
    real_create = database.create_job
    real_reserve = database.reserve_mappings
    reserve_entered = threading.Event()
    may_release = threading.Event()

    def create_then_lock(job_id, original_name):
        real_create(job_id, original_name)
        # Registration committed; now hold the write lock so the pipeline's
        # reservation transaction hits the busy timeout.
        holder.execute("BEGIN IMMEDIATE")

    def locked_reserve(*args, **kwargs):
        reserve_entered.set()
        try:
            return real_reserve(*args, **kwargs)  # raises DatabaseBusy after 300ms
        finally:
            may_release.set()  # let the test thread release the lock promptly

    monkeypatch.setattr(database, "create_job", create_then_lock)
    monkeypatch.setattr(database, "reserve_mappings", locked_reserve)

    with TestClient(main_mod.app) as client:
        resp = _upload(client, sample_zip_bytes)
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]

        assert reserve_entered.wait(timeout=10)
        assert may_release.wait(timeout=10)
        holder.execute("ROLLBACK")  # rollback bookkeeping can now run
        holder.close()

        deadline = time.time() + 20
        while time.time() < deadline:
            job = database.get_job(job_id)
            if job and job["status"] != "processing":
                break
            time.sleep(0.1)

        job = database.get_job(job_id)
        assert job["status"] == "failed", job
        # Exclusive new mappings were rolled back ...
        assert database.mapping_stats()["uncommitted_reservations"] == 0
        assert database.get_committed_mappings() == []
        # ... and the whole staging directory is gone.
        assert not (config.JOBS_DIR / job_id).exists()


def test_503_response_shape_under_busy_timeout(monkeypatch, client):
    monkeypatch.setattr(config, "SQLITE_BUSY_TIMEOUT_MS", 200)

    def raise_busy(job_id, original_name):
        raise database.DatabaseBusy("database is locked")

    monkeypatch.setattr(database, "create_job", raise_busy)
    resp = _upload(client, b"PK\x03\x04-bytes")
    assert resp.status_code == 503
    assert resp.headers["retry-after"] == "5"
    assert database.list_jobs() == []


# ------------------------------------------------------------------- cancellation

def test_cancelled_queued_upload_never_registers(monkeypatch, temp_data):
    test_runner = UploadRunner(workers=1, queue_limit=2)
    test_runner.start()
    monkeypatch.setattr(runner_mod, "runner", test_runner)

    block = threading.Event()
    entered = threading.Event()
    real_create = database.create_job

    def slow_create(job_id, original_name):
        entered.set()
        block.wait(timeout=10)
        return real_create(job_id, original_name)

    monkeypatch.setattr(database, "create_job", slow_create)

    loop = asyncio.new_event_loop()

    def make_item(job_id):
        job_dir = config.JOBS_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        spool = job_dir / "input.zip"
        spool.write_bytes(b"PK")
        return PendingUpload(
            job_id=job_id,
            job_dir=job_dir,
            spool_path=spool,
            original_name="queued.zip",
            future=loop.create_future(),
        )

    occupying = make_item("occupy0000000000000000000001")
    test_runner.submit(occupying)
    assert entered.wait(timeout=5)

    cancelled = make_item("cancel00000000000000000000002")
    test_runner.submit(cancelled)
    time.sleep(0.3)  # let it settle in the queue
    cancelled.request_cancel()  # request gone before a worker touches the DB

    block.set()
    deadline = time.time() + 10
    while time.time() < deadline and test_runner.queue_depth():
        time.sleep(0.05)

    assert database.get_job(occupying.job_id) is not None  # registered + processed
    assert database.get_job(cancelled.job_id) is None      # never registered late
    assert not cancelled.job_dir.exists()                  # staging discarded
    loop.close()
    test_runner.shutdown()


def test_disconnect_after_acceptance_lets_processing_finish(
    monkeypatch, temp_data, sample_zip_bytes
):
    test_runner = UploadRunner(workers=1, queue_limit=2)
    monkeypatch.setattr(runner_mod, "runner", test_runner)

    with TestClient(main_mod.app) as client:
        accepted = threading.Event()

        real_register = PendingUpload.settle_registered

        def registered_signal(self):
            real_register(self)
            accepted.set()

        monkeypatch.setattr(PendingUpload, "settle_registered", registered_signal)

        resp_holder: dict = {}

        def do_upload():
            resp_holder["resp"] = _upload(client, sample_zip_bytes, "kept.zip")

        t = threading.Thread(target=do_upload)
        t.start()
        assert accepted.wait(timeout=10)  # registration committed, response pending
        t.join(timeout=30)

        job_id = resp_holder["resp"].json()["job_id"]
        deadline = time.time() + 30
        while time.time() < deadline:
            job = database.get_job(job_id)
            if job and job["status"] in ("done", "failed"):
                break
            time.sleep(0.1)
        # Even though the upload coroutine can be abandoned once accepted, the
        # background pipeline runs to completion.
        assert database.get_job(job_id)["status"] == "done"


def test_queue_full_does_not_consume_worker_slot(monkeypatch, temp_data):
    test_runner = UploadRunner(workers=1, queue_limit=1)
    test_runner.start()
    monkeypatch.setattr(runner_mod, "runner", test_runner)

    block = threading.Event()
    entered = threading.Event()
    real_create = database.create_job

    def slow_create(job_id, original_name):
        entered.set()
        block.wait(timeout=10)
        return real_create(job_id, original_name)

    monkeypatch.setattr(database, "create_job", slow_create)

    loop = asyncio.new_event_loop()

    def item(jid):
        job_dir = config.JOBS_DIR / jid
        job_dir.mkdir(parents=True, exist_ok=True)
        spool = job_dir / "input.zip"
        spool.write_bytes(b"PK")
        return PendingUpload(jid, job_dir, spool, "x.zip", loop.create_future())

    first = item("slot000000000000000000000000001")
    test_runner.submit(first)
    assert entered.wait(timeout=5)
    second = item("slot000000000000000000000000002")
    test_runner.submit(second)  # occupies the single queue slot

    third = item("slot000000000000000000000000003")
    with pytest.raises(QueueFull):
        test_runner.submit(third)
    test_runner.discard_staging(third)

    block.set()
    deadline = time.time() + 10
    while time.time() < deadline and test_runner.queue_depth():
        time.sleep(0.05)
    assert database.get_job(first.job_id) is not None
    assert database.get_job(second.job_id) is not None
    assert database.get_job(third.job_id) is None
    loop.close()
    test_runner.shutdown()


# ------------------------------------------------------- connections always close

def test_connections_are_closed_after_busy_wait(monkeypatch, temp_data):
    """A timed-out registration must release its connection (no descriptor /
    lock leak), so the next write succeeds immediately."""
    monkeypatch.setattr(config, "SQLITE_BUSY_TIMEOUT_MS", 200)
    holder = database.connect()
    holder.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(database.DatabaseBusy):
            database.create_job("latelock000000000000000000001", "x.zip")
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    # Lock released and failed connection closed: a fresh write works at once.
    database.create_job("afterlock0000000000000000000002", "y.zip")
    assert database.get_job("afterlock0000000000000000000002")["status"] == "processing"
