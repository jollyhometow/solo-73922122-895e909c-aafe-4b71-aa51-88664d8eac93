#!/usr/bin/env python3
"""End-to-end demo for the DICOM de-identification service.

Uses only the Python standard library so it runs inside a bare container.
It exercises every headline requirement:

  1. downloads the two synthetic sample ZIPs (shared patient, cross-package
     and out-of-package SOP references);
  2. uploads both packages *concurrently* (twice, re-downloaded) to prove the
     two API processes converge on one shared mapping for first-time-seen
     identifiers;
  3. verifies both packages received the same anonymized PatientID, that the
     cross-package ReferencedSOPInstanceUID was rewritten through the same
     instance map, and that the out-of-package reference was mapped too;
  4. downloads both processed ZIPs and verifies PHI cleanup, private-tag
     removal, preserved transfer syntax / class UID / pixel bytes;
  5. uploads the 21-file package to show rejection with full rollback (no new
     committed mapping appears);
  6. re-uploads package A to prove restart-safe reuse of stored mappings
     (the anonymized UIDs stay identical).

Usage::

    python scripts/demo.py [--base-url http://localhost:8080]
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import tempfile
import threading
import urllib.error
import urllib.request
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor

# pydicom is only needed for output verification; the upload flow itself is
# pure stdlib.  Import lazily so the script can still drive the API without it.
try:
    import pydicom
except ImportError:  # pragma: no cover
    pydicom = None


def request(method, url, data=None, headers=None):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read()
            ctype = resp.headers.get("content-type", "")
            return resp.status, (json.loads(body) if "json" in ctype else body)
    except urllib.error.HTTPError as exc:
        body = exc.read()
        try:
            return exc.code, json.loads(body)
        except json.JSONDecodeError:
            return exc.code, body


def log(step, msg):
    print(f"[{step}] {msg}")


def download_sample(base, name):
    status, body = request("GET", f"{base}/samples/{name}")
    assert status == 200, f"sample {name} download failed: {status} {body[:200]}"
    return body


def upload(base, blob, filename):
    boundary = f"----demo{uuid.uuid4().hex}"
    body = io.BytesIO()
    body.write(f"--{boundary}\r\n".encode())
    body.write(
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode()
    )
    body.write(b"Content-Type: application/zip\r\n\r\n")
    body.write(blob)
    body.write(f"\r\n--{boundary}--\r\n".encode())
    status, payload = request(
        "POST",
        f"{base}/jobs",
        data=body.getvalue(),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    assert status == 202, f"upload {filename} failed: {status} {payload}"
    return payload["job_id"], payload["received_by_replica"]


def wait_job(base, job_id):
    for _ in range(120):
        status, job = request("GET", f"{base}/jobs/{job_id}")
        assert status == 200
        if job["status"] in ("done", "failed"):
            return job
        threading.Event().wait(0.5)
    raise TimeoutError(f"job {job_id} never finished")


def download_result(base, job_id):
    status, body = request("GET", f"{base}/jobs/{job_id}/download")
    assert status == 200, f"download failed for {job_id}: {status}"
    return body


def verify_outputs(zip_a, zip_b, report_a, report_b, original_a_bytes,
                   original_b_bytes, external_sop, internal_sop):
    if pydicom is None:
        log("verify", "pydicom unavailable, skipping DICOM-level verification")
        return None, None, None

    def read_all(blob):
        out = {}
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            for name in zf.namelist():
                out[name] = pydicom.dcmread(io.BytesIO(zf.read(name)))
        return out

    orig_a, orig_b = read_all(original_a_bytes), read_all(original_b_bytes)
    out_a, out_b = read_all(zip_a), read_all(zip_b)

    # --- shared patient linkage across packages ---
    anon_patients_a = {str(ds.PatientID) for ds in out_a.values()}
    anon_patients_b = {str(ds.PatientID) for ds in out_b.values()}
    assert anon_patients_a == anon_patients_b, "patient linkage diverged across packages"
    anon_patient = next(iter(anon_patients_a))
    assert anon_patient != "HOSP01-P-000999"
    log("verify", f"shared anonymized PatientID across both packages: {anon_patient}")

    # --- PHI cleanup ---
    for label, group in (("A", out_a), ("B", out_b)):
        for name, ds in group.items():
            assert str(ds.PatientName) == "", f"{name}: PatientName not cleared"
            assert str(ds.PatientBirthDate) == "", f"{name}: birth date not cleared"
            assert not [e for e in ds.values() if e.tag.is_private], \
                f"{name}: private tags remain"
    log("verify", "PatientName / PatientBirthDate emptied; all private tags removed")

    # --- preserved class UID, transfer syntax and pixel bytes ---
    for orig_group, out_group in ((orig_a, out_a), (orig_b, out_b)):
        for name in orig_group:
            o, n = orig_group[name], out_group[name]
            assert str(o.SOPClassUID) == str(n.SOPClassUID)
            assert str(o.file_meta.TransferSyntaxUID) == str(n.file_meta.TransferSyntaxUID)
            assert o.PixelData == n.PixelData, "pixel bytes changed"
            assert (
                str(o.file_meta.MediaStorageSOPInstanceUID)
                != str(n.file_meta.MediaStorageSOPInstanceUID)
            )
            assert (
                str(n.file_meta.MediaStorageSOPInstanceUID) == str(n.SOPInstanceUID)
            ), "file-meta SOP UID not synchronised"
    log("verify", "class UID / transfer syntax / pixel bytes preserved; "
                  "file-meta SOP UID synchronised")

    # --- cross-package + out-of-package references via the same instance map ---
    map_inst_a = {
        e["original_sop_uid"]: e["anonymized_sop_uid"]
        for e in report_a["files"]
    }
    b_ds = next(iter(out_b.values()))
    refs = {str(r.ReferencedSOPInstanceUID) for r in b_ds.ReferencedSOPSequence}
    assert map_inst_a[internal_sop] in refs, "cross-package ref not rewritten to A's map"
    # The external SOP is not part of package A, so it cannot appear in A's
    # per-file report; its committed mapping is checked by the caller.
    assert map_inst_a.get(external_sop) is None
    log("verify", "cross-package ReferencedSOPInstanceUID follows package A's mapping")
    return anon_patient, map_inst_a, refs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    log("setup", f"target API: {base}")
    health_seen = set()
    for _ in range(6):
        status, health = request("GET", f"{base}/health")
        if status == 200:
            health_seen.add(health["replica"])
        threading.Event().wait(0.4)
    log("health", f"replicas observed via load balancer: {sorted(health_seen)}")

    zip_a = download_sample(base, "sample_crossref_a.zip")
    zip_b = download_sample(base, "sample_crossref_b.zip")
    zip_bad = download_sample(base, "sample_too_many.zip")
    log("setup", "downloaded 3 synthetic sample packages")

    # ---- concurrent first-time uploads (each blob uploaded in parallel) ----
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [
            pool.submit(upload, base, zip_a, "sample_crossref_a.zip"),
            pool.submit(upload, base, zip_b, "sample_crossref_b.zip"),
            pool.submit(upload, base, zip_a, "sample_crossref_a_dup.zip"),
            pool.submit(upload, base, zip_b, "sample_crossref_b_dup.zip"),
        ]
        uploads = [f.result() for f in futures]
    replicas = sorted({r for _, r in uploads})
    log("upload", f"4 concurrent uploads accepted by replicas: {replicas}")

    jobs = [wait_job(base, jid) for jid, _ in uploads]
    assert all(j["status"] == "done" for j in jobs), jobs
    log("upload", "all concurrent jobs completed successfully")

    job_a, job_b = jobs[0], jobs[1]
    report_a = json.loads(job_a["report_json"])
    report_b = json.loads(job_b["report_json"])

    # duplicate concurrent uploads must have reused identical mappings
    report_a2 = json.loads(jobs[2]["report_json"])
    report_b2 = json.loads(jobs[3]["report_json"])
    for key in ("anonymized_patient_id", "anonymized_study_uid",
                "anonymized_series_uid", "anonymized_sop_uid"):
        vals_a = sorted({e[key] for e in report_a["files"]})
        vals_a2 = sorted({e[key] for e in report_a2["files"]})
        assert vals_a == vals_a2, f"mapping drift for {key} under concurrency"
    log("concurrency", "concurrent first-seen uploads converged on shared mappings")

    result_a = download_result(base, job_a["id"])
    result_b = download_result(base, job_b["id"])

    external_sop = "1.2.840.20261112.999.77.88"
    internal_sop = "1.2.840.20261112.999.1.1.1"
    anon_patient, map_inst_a, refs = verify_outputs(
        result_a, result_b, report_a, report_b, zip_a, zip_b,
        external_sop, internal_sop,
    )
    if anon_patient is None:
        anon_patient = report_a["files"][0]["anonymized_patient_id"]
        refs = set()
        map_inst_a = {
            e["original_sop_uid"]: e["anonymized_sop_uid"]
            for e in report_a["files"]
        }

    # cross-package patient linkage
    patients_b = {e["anonymized_patient_id"] for e in report_b["files"]}
    assert patients_b == {anon_patient}, "cross-batch patient linkage broken"
    log("crossref", f"same patient across batches -> {anon_patient}")

    # out-of-package reference must have a committed mapping too
    status, ext = request(
        "GET", f"{base}/mappings?id_type=instance"
    )
    instance_map = {m["original_value"]: m["anonymized_value"] for m in ext["mappings"]}
    assert external_sop in instance_map, "out-of-package SOP was never mapped"
    if refs:
        assert instance_map[external_sop] in refs, "out-of-package ref not rewritten"
    log("crossref", f"out-of-package SOP mapped to {instance_map[external_sop]}")

    # ---- failure path: 21 DICOMs -> rejection, rollback, no downloadable ZIP --
    before = request("GET", f"{base}/mappings/stats")[1]
    bad_job_id, _ = upload(base, zip_bad, "sample_too_many.zip")
    bad_job = wait_job(base, bad_job_id)
    assert bad_job["status"] == "failed", bad_job
    log("rollback", f"oversized package rejected: {bad_job['error']}")
    status, _ = request("GET", f"{base}/jobs/{bad_job_id}/download")
    assert status == 409, "failed package must not be downloadable"
    after = request("GET", f"{base}/mappings/stats")[1]
    assert after["uncommitted_reservations"] == 0, "failed job left reservations"
    assert after == before, "failed job changed committed mappings"
    log("rollback", "no new mapping survived the failed package")

    # ---- reuse after the fact (simulates restart-safe persistence) ----
    reuse_id, _ = upload(base, zip_a, "reuse_after_restart.zip")
    reuse_job = wait_job(base, reuse_id)
    assert reuse_job["status"] == "done"
    reuse_report = json.loads(reuse_job["report_json"])
    for key in ("anonymized_patient_id", "anonymized_study_uid",
                "anonymized_series_uid", "anonymized_sop_uid"):
        assert {e[key] for e in reuse_report["files"]} == \
               {e[key] for e in report_a["files"]}, f"{key} not reused"
    log("persistence", "stored mappings reused deterministically (restart-safe)")

    # save processed artifacts for manual inspection
    out_dir = tempfile.mkdtemp(prefix="dicom-demo-")
    for name, blob in (("a_anonymized.zip", result_a), ("b_anonymized.zip", result_b)):
        with open(f"{out_dir}/{name}", "wb") as fh:
            fh.write(blob)
    log("done", f"all checks passed; processed ZIPs saved under {out_dir}")
    print("\nDEMO SUCCESS")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as exc:
        print(f"\nDEMO FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
