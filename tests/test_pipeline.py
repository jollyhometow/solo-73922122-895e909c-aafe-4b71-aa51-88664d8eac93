"""Tests for the reservation/commit/rollback mapping semantics.

These tests exercise two real SQLite connections (like two API processes) and
a full pipeline run against a temporary data directory.
"""
from __future__ import annotations

import io
import json
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pydicom
import pytest

from app import config, database, identifiers, pipeline
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
    return data_dir


def _requested():
    return {
        "patient": {"P1"},
        "study": {"ST1"},
        "series": {"SE1"},
        "instance": {"I1", "I2"},
    }


def test_reserve_commit_and_persistent_reuse(temp_data):
    m1 = database.reserve_mappings("job-1", _requested(), identifiers.generate_anonymized)
    # Uncommitted reservations are visible for reuse but not "committed".
    stats = database.mapping_stats()
    assert stats["uncommitted_reservations"] == 5
    assert stats["patient_committed"] == 0

    database.commit_job("job-1")
    stats = database.mapping_stats()
    assert stats["uncommitted_reservations"] == 0
    assert stats["patient_committed"] == 1

    # A later job reuses every stored mapping instead of minting new ones.
    m2 = database.reserve_mappings("job-2", _requested(), identifiers.generate_anonymized)
    assert m1 == m2
    database.commit_job("job-2")
    assert database.mapping_stats()["patient_committed"] == 1


def test_concurrent_first_seen_identifier_converges(temp_data):
    """Two threads (two API processes) first meet the same identifiers at once."""
    requested_a = {"patient": {"P1"}, "study": set(), "series": set(),
                   "instance": {"I1"}}
    requested_b = {"patient": {"P1", "P2"}, "study": set(), "series": set(),
                   "instance": {"I1", "I2"}}
    results = {}

    def worker(name, requested):
        results[name] = database.reserve_mappings(
            name, requested, identifiers.generate_anonymized
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(worker, "job-a", requested_a)
        f2 = pool.submit(worker, "job-b", requested_b)
        f1.result(); f2.result()

    # Shared identifiers must map to exactly one anonymized value.
    assert results["job-a"]["patient"]["P1"] == results["job-b"]["patient"]["P1"]
    assert results["job-a"]["instance"]["I1"] == results["job-b"]["instance"]["I1"]
    # The distinct identifiers each got their own value.
    assert results["job-b"]["patient"]["P1"] != results["job-b"]["patient"]["P2"]

    # Committing one job must not delete rows re-adopted by the other.
    database.commit_job("job-a")
    database.fail_job("job-b", "simulated packaging failure")
    rows = {r["original_value"] for r in database.get_committed_mappings()}
    assert "P1" in rows and "I1" in rows          # committed by job-a, kept
    assert "P2" not in rows and "I2" not in rows  # owned only by failed job-b
    assert database.mapping_stats()["uncommitted_reservations"] == 0


def test_failed_package_rolls_back_everything(temp_data):
    database.create_job("job-x", "x.zip")
    database.reserve_mappings("job-x", _requested(), identifiers.generate_anonymized)
    database.fail_job("job-x", "boom")
    assert database.get_committed_mappings() == []
    assert database.mapping_stats()["uncommitted_reservations"] == 0
    job = database.get_job("job-x")
    assert job["status"] == "failed" and job["error"] == "boom"


def test_sweep_stale_jobs_after_restart(temp_data):
    database.create_job("dead-job", "dead.zip")
    database.reserve_mappings("dead-job", _requested(),
                              identifiers.generate_anonymized)
    (config.JOBS_DIR / "dead-job").mkdir(parents=True, exist_ok=True)
    result = database.sweep_stale_jobs()
    assert result == {"jobs": 1, "mappings": 5, "staging_dirs": 1}
    assert database.mapping_stats()["uncommitted_reservations"] == 0
    assert database.get_job("dead-job")["status"] == "failed"
    assert not (config.JOBS_DIR / "dead-job").exists()


def test_full_pipeline_and_anonymization(temp_data):
    generate_samples.generate(config.SAMPLES_DIR)
    zip_a = config.SAMPLES_DIR / "sample_crossref_a.zip"
    zip_b = config.SAMPLES_DIR / "sample_crossref_b.zip"

    job_a = "joba"; job_b = "jobb"
    thread_a = threading.Thread(
        target=pipeline.process_job, args=(job_a, zip_a, zip_a.name))
    thread_b = threading.Thread(
        target=pipeline.process_job, args=(job_b, zip_b, zip_b.name))
    thread_a.start(); thread_b.start()
    thread_a.join(); thread_b.join()

    assert database.get_job(job_a)["status"] == "done"
    assert database.get_job(job_b)["status"] == "done"

    # Read originals and outputs and verify every transformation rule.
    def read_zip(path):
        with zipfile.ZipFile(path) as zf:
            return {n: pydicom.dcmread(io.BytesIO(zf.read(n)))
                    for n in zf.namelist()}

    out_a = read_zip(config.JOBS_DIR / job_a / "joba_anonymized.zip")
    out_b = read_zip(config.JOBS_DIR / job_b / "jobb_anonymized.zip")
    orig_a = read_zip(zip_a)
    orig_b = read_zip(zip_b)

    anon_pid = {str(ds.PatientID) for ds in out_a.values()}
    assert anon_pid == {str(ds.PatientID) for ds in out_b.values()}
    assert next(iter(anon_pid)).startswith("ANON-")

    for orig, out in ((orig_a, out_a), (orig_b, out_b)):
        for name, ds in out.items():
            assert str(ds.PatientName) == ""
            assert str(ds.PatientBirthDate) == ""
            assert not [e for e in ds.values() if e.tag.is_private]
            assert str(ds.SOPClassUID) == str(orig[name].SOPClassUID)
            assert str(ds.file_meta.TransferSyntaxUID) == \
                   str(orig[name].file_meta.TransferSyntaxUID)
            assert ds.PixelData == orig[name].PixelData
            assert str(ds.file_meta.MediaStorageSOPInstanceUID) == \
                   str(ds.SOPInstanceUID)

    # Cross-package reference uses package A's instance mapping.
    report_a = json.loads(database.get_job(job_a)["report_json"])
    inst_map = {e["original_sop_uid"]: e["anonymized_sop_uid"]
                for e in report_a["files"]}
    b_ds = next(iter(out_b.values()))
    ref_uids = {str(r.ReferencedSOPInstanceUID)
                for r in b_ds.ReferencedSOPSequence}
    external = "1.2.840.20261112.999.77.88"
    internal = "1.2.840.20261112.999.1.1.1"
    assert inst_map[internal] in ref_uids

    # Out-of-package reference also has a committed mapping and was rewritten.
    ext_map = {m["original_value"]: m["anonymized_value"]
               for m in database.get_committed_mappings("instance")}
    assert external in ext_map and ext_map[external] in ref_uids


def test_oversized_package_fails_and_leaves_no_mapping(temp_data):
    generate_samples.generate(config.SAMPLES_DIR)
    before = database.mapping_stats()
    job_id = "toomany"
    pipeline.process_job(
        job_id, config.SAMPLES_DIR / "sample_too_many.zip", "sample_too_many.zip")
    job = database.get_job(job_id)
    assert job["status"] == "failed"
    assert "20" in job["error"]
    assert not (config.JOBS_DIR / job_id / "toomany_anonymized.zip").exists()
    assert database.mapping_stats() == before


def test_non_zip_and_path_traversal_are_rejected(temp_data):
    bad = config.JOBS_DIR / "x" / "in.zip"
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b"not a zip at all")
    with pytest.raises(pipeline.PackageError):
        pipeline.extract_zip(bad, config.JOBS_DIR / "x" / "out")

    evil = config.JOBS_DIR / "y" / "evil.zip"
    evil.parent.mkdir(parents=True)
    with zipfile.ZipFile(evil, "w") as zf:
        zf.writestr("../../etc/escape.dcm", b"x")
    with pytest.raises(pipeline.PackageError):
        pipeline.extract_zip(evil, config.JOBS_DIR / "y" / "out")


def test_generated_uid_well_formed():
    for id_type in ("study", "series", "instance"):
        uid = identifiers.generate_anonymized(id_type)
        assert uid.startswith("2.25.") and len(uid) <= 64
        pydicom.uid.UID(uid)  # raises if invalid
    pid = identifiers.generate_anonymized("patient")
    assert pid.startswith("ANON-")
