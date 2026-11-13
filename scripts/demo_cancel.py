#!/usr/bin/env python3
"""Live demo of upload cancellation semantics against a running replica.

Two distinct guarantees are demonstrated over real HTTP:

  1. *Queued / pre-registration* cancellation — with the demo registration
     gate (``UPLOAD_REGISTRATION_GATE``) active, workers pause before any
     database work.  An upload whose client socket is abandoned in that window
     must never create a job row ("no late registration").
  2. *Accepted* disconnect — once a job is registered, dropping the upload
     response does not stop the package processing to ``done`` in the
     background; the result stays downloadable.

The target replica is expected to run with the gate configured and a small
bounded queue, e.g.::

    UPLOAD_WORKERS=1 UPLOAD_QUEUE_LIMIT=2 \\
    UPLOAD_REGISTRATION_GATE=/tmp/dicom-e2e/gate.flag \\
    uvicorn app.main:app --port 8003

``scripts/run_cancel_demo.sh`` starts exactly such a temporary replica.
"""
from __future__ import annotations

import argparse
import http.client
import io
import json
import os
import sys
import threading
import time
import uuid


def http_get(port, path):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    conn.request("GET", path)
    resp = conn.getresponse()
    body = resp.read()
    headers = dict(resp.getheaders())
    conn.close()
    ctype = headers.get("content-type", "")
    return resp.status, (json.loads(body) if "json" in ctype else body)


def multipart(blob: bytes, filename: str):
    boundary = f"----cancel{uuid.uuid4().hex}"
    buf = io.BytesIO()
    buf.write(f"--{boundary}\r\n".encode())
    buf.write(
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode()
    )
    buf.write(b"Content-Type: application/zip\r\n\r\n")
    buf.write(blob)
    buf.write(f"\r\n--{boundary}--\r\n".encode())
    return buf.getvalue(), f"multipart/form-data; boundary={boundary}"


def upload_and_abandon(port, blob):
    """Open the request and close the socket without reading the response."""
    body, ctype = multipart(blob, f"abandoned-{uuid.uuid4().hex}.zip")
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    conn.putrequest("POST", "/jobs")
    conn.putheader("Content-Type", ctype)
    conn.putheader("Content-Length", str(len(body)))
    conn.endheaders()
    conn.send(body)
    conn.close()  # client gone; deliberately no response read


def upload_blocking(port, blob, name):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    body, ctype = multipart(blob, name)
    conn.request("POST", "/jobs", body=body, headers={"Content-Type": ctype})
    resp = conn.getresponse()
    payload = json.loads(resp.read())
    conn.close()
    return resp.status, payload


def wait_done(port, job_id, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, job = http_get(port, f"/jobs/{job_id}")
        if status == 200 and job["status"] in ("done", "failed"):
            return job
        time.sleep(0.3)
    raise TimeoutError(job_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8003)
    parser.add_argument(
        "--gate", default="/tmp/dicom-e2e/gate.flag",
        help="path of the UPLOAD_REGISTRATION_GATE file watched by the replica",
    )
    args = parser.parse_args()
    port, gate = args.port, args.gate

    status, blob = http_get(port, "/samples/sample_crossref_a.zip")
    assert status == 200
    print(f"[setup] target replica on port {port}, sample {len(blob)} bytes")

    # ---- case 1: disconnect while registration is gated (nothing in DB) ------
    print("[1/3] registration gate active; abandoning an in-flight upload")
    open(gate, "w").write("blocked")
    before = {j["id"] for j in http_get(port, "/jobs")[1]["jobs"]}

    # Fire several uploads; the gate keeps every worker before registration.
    threads = [
        threading.Thread(
            target=lambda: upload_blocking(port, blob, f"gated-{uuid.uuid4().hex}.zip")
        )
        for _ in range(3)
    ]
    for t in threads:
        t.start()
    time.sleep(1.5)  # let requests reach the blocked workers / bounded queue

    upload_and_abandon(port, blob)
    print("      client socket closed without reading the response")
    time.sleep(1.0)
    gated = {j["id"] for j in http_get(port, "/jobs")[1]["jobs"]}
    assert gated == before, (
        "a job was registered while the gate was closed: "
        f"{gated - before}"
    )
    print("      no job row exists while registration is gated")

    os.remove(gate)  # release the workers
    for t in threads:
        t.join(timeout=30)
    time.sleep(1.0)
    # The abandoned upload must never have appeared even after release.
    after_release = {j["id"] for j in http_get(port, "/jobs")[1]["jobs"]}
    assert len(after_release - before) == 3, (
        f"expected exactly the 3 still-open uploads, got {after_release - before}"
    )
    print("      after release only the 3 connected uploads registered; "
          "the abandoned one never did")

    # ---- case 2: disconnect after acceptance does not stop processing --------
    print("[2/3] accepting an upload, then letting the client go away")
    status, payload = upload_blocking(
        port, blob, f"accepted-then-gone-{uuid.uuid4().hex}.zip"
    )
    assert status == 202, payload
    job_id = payload["job_id"]
    print(f"      job {job_id} accepted; the response is now abandoned")
    job = wait_done(port, job_id)
    assert job["status"] == "done", job
    status, _ = http_get(port, f"/jobs/{job_id}/download")
    assert status == 200
    print("      background processing finished; result remains downloadable")

    # ---- final consistency ----------------------------------------------------
    print("[3/3] final state: no processing jobs, no uncommitted reservations")
    _, stats = http_get(port, "/mappings/stats")
    assert stats["uncommitted_reservations"] == 0, stats
    _, jobs = http_get(port, "/jobs")
    assert all(j["status"] != "processing" for j in jobs["jobs"]), jobs
    print(f"      stats={stats}")

    print("\nCANCELLATION DEMO SUCCESS")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as exc:
        print(f"\nCANCELLATION DEMO FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
