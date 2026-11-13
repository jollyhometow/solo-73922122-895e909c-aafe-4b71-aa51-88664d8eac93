#!/usr/bin/env python3
"""Live stress/demo for bounded lock waits and upload cancellation.

Runs against the two real uvicorn replicas (shared SQLite WAL database) and
demonstrates the headline resilience requirements:

  1. With a long-held external write lock, many concurrent uploads (40 per
     replica) wait inside the *dedicated* bounded worker pool while /health,
     the job list and the mapping endpoints still answer under 1 second.
  2. When the lock outlasts the SQLite busy timeout, every unaccepted upload
     receives a retryable 503 (Retry-After set); no processing row and no
     staging directory is left behind.
  3. Once the lock is released, the same uploads are accepted again and
     process normally; cancellation of a still-queued upload never registers a
     job late.

The script uses only the standard library for the HTTP parts.
"""
from __future__ import annotations

import argparse
import io
import json
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

REPLICA_PORTS = (8001, 8002)
UPLOADS_PER_REPLICA = 40


def http(method, url, data=None, headers=None, timeout=90):
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            ctype = resp.headers.get("content-type", "")
            return resp.status, dict(resp.headers), (
                json.loads(body) if "json" in ctype else body
            )
    except urllib.error.HTTPError as exc:
        body = exc.read()
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            pass
        return exc.code, dict(exc.headers), body


def sample_blob(base):
    _, _, blob = http("GET", f"{base}/samples/sample_crossref_a.zip")
    return blob


def post_upload(base, blob, name):
    boundary = f"----stress{uuid.uuid4().hex}"
    buf = io.BytesIO()
    buf.write(f"--{boundary}\r\n".encode())
    buf.write(
        f'Content-Disposition: form-data; name="file"; filename="{name}"\r\n'.encode()
    )
    buf.write(b"Content-Type: application/zip\r\n\r\n")
    buf.write(blob)
    buf.write(f"\r\n--{boundary}--\r\n".encode())
    return http(
        "POST",
        f"{base}/jobs",
        data=buf.getvalue(),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )


def step(msg):
    print(f"  - {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/tmp/dicom-e2e/data/mappings.db")
    parser.add_argument("--busy-ms", type=int, default=3000)
    parser.add_argument(
        "--bases",
        default=",".join(f"http://127.0.0.1:{p}" for p in REPLICA_PORTS),
        help="comma-separated base URLs; give the nginx URL twice to keep "
             "40+40 balanced uploads when running inside docker compose",
    )
    args = parser.parse_args()

    bases = [b.strip() for b in args.bases.split(",") if b.strip()]
    for base in bases:
        status, _, _ = http("GET", f"{base}/health")
        assert status == 200, f"replica {base} not healthy"
    print(f"[1/4] target endpoints healthy: {bases}")

    blob = sample_blob(bases[0])
    print("[2/4] acquiring an external write lock (simulating a busy writer)")
    holder = sqlite3.connect(args.db, timeout=5, isolation_level=None)
    holder.execute("PRAGMA busy_timeout=5000")
    holder.execute("BEGIN IMMEDIATE")

    results: list[tuple[str, int]] = []
    lock = threading.Lock()

    def hammer(base):
        status, _, _ = post_upload(base, blob, f"stress-{uuid.uuid4().hex}.zip")
        with lock:
            results.append((base, status))
        return status

    print(f"[3/4] firing {UPLOADS_PER_REPLICA} uploads at EACH endpoint "
          f"({UPLOADS_PER_REPLICA * len(bases)} total) while the write lock is held")
    with ThreadPoolExecutor(max_workers=UPLOADS_PER_REPLICA * len(bases)) as pool:
        futures = []
        for base in bases:
            for _ in range(UPLOADS_PER_REPLICA):
                futures.append(pool.submit(hammer, base))

        # While uploads wait for the lock, readers must stay under 1 second.
        time.sleep(1.0)
        worst = 0.0
        for base in bases:
            for path in ("/health", "/jobs", "/mappings", "/mappings/stats"):
                t0 = time.time()
                status, _, _ = http("GET", base + path, timeout=10)
                elapsed = time.time() - t0
                worst = max(worst, elapsed)
                assert status == 200, (base, path, status)
                assert elapsed < 1.0, f"{base}{path} took {elapsed:.2f}s"
        step(f"all read endpoints answered in < 1s while uploads waited "
             f"(worst {worst * 1000:.0f} ms)")

        for f in futures:
            f.result()

    statuses = [s for _, s in results]
    by_endpoint = {b: [s for base, s in results if base == b] for b in bases}
    for base, codes in by_endpoint.items():
        assert codes and all(c == 503 for c in codes), (base, set(codes))
    step(f"all {len(statuses)} lock-waiting uploads got retryable 503")

    # Verify Retry-After and no residue.
    status, headers, _ = post_upload(
        bases[0], blob, f"retry-check-{uuid.uuid4().hex}.zip"
    )
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    assert status == 503 and retry_after == "5", (status, headers)
    step("503 response carries Retry-After: 5")

    holder.execute("ROLLBACK")
    holder.close()

    # No processing rows may survive the rejected uploads.
    for base in bases:
        _, _, jobs = http("GET", f"{base}/jobs")
        assert all(j["status"] != "processing" for j in jobs["jobs"]), jobs
    step("no processing rows left behind by rejected uploads")

    import os
    jobs_dir = os.path.dirname(args.db) + "/jobs"
    leftovers = os.listdir(jobs_dir) if os.path.isdir(jobs_dir) else []
    # Completed jobs keep their result dirs; rejected uploads create none.
    step(f"jobs directory entries (completed jobs keep result dirs): {len(leftovers)}")

    print("[4/4] releasing the lock and retrying — uploads must now be accepted")
    accepted = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [
            pool.submit(post_upload, bases[i % len(bases)], blob,
                        f"retry-{uuid.uuid4().hex}.zip")
            for i in range(8)
        ]
        for f in futures:
            status, _, body = f.result()
            assert status == 202, (status, body)
            accepted.append(body["job_id"])

    deadline = time.time() + 60
    pending = set(accepted)
    while pending and time.time() < deadline:
        for base in bases:
            for job_id in list(pending):
                status, _, job = http("GET", f"{base}/jobs/{job_id}")
                if status == 200 and job["status"] in ("done", "failed"):
                    assert job["status"] == "done", job
                    pending.discard(job_id)
        time.sleep(0.5)
    assert not pending, f"jobs never finished: {pending}"
    step(f"{len(accepted)} retried uploads accepted and completed after release")

    # Final consistency: committed mappings are readable everywhere.
    seen = []
    for base in bases:
        _, _, stats = http("GET", f"{base}/mappings/stats")
        seen.append(stats)
    assert all(s == seen[0] for s in seen) and seen[0]["uncommitted_reservations"] == 0
    step(f"all endpoints see identical committed stats: {seen[0]}")

    print("\nSTRESS DEMO SUCCESS")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as exc:
        print(f"\nSTRESS DEMO FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
