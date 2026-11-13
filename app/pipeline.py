"""End-to-end package processing pipeline.

Stages, in order:

    upload -> extract safely -> validate DICOM (count <= 20, uncompressed)
    -> collect identifiers -> short DB reservation transaction
    -> rewrite every DICOM (no DB write lock held during this stage)
    -> build result ZIP -> short DB commit transaction -> open download

Any failure before commit removes the job's reserved mappings, so a failed
package never leaves new mappings behind.
"""
from __future__ import annotations

import json
import shutil
import zipfile
from pathlib import Path

from . import config, database, identifiers


class PackageError(ValueError):
    """Any user-visible problem with an uploaded package."""


# ------------------------------------------------------------------ ZIP handling

def _safe_member_name(member: zipfile.ZipInfo) -> str:
    name = member.filename
    # Normalise and block path traversal / absolute paths / drive letters.
    if name.startswith(("/", "\\")) or ":" in name or ".." in Path(name).parts:
        raise PackageError(f"unsafe member path in ZIP: {member.filename!r}")
    return name


def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """Extract an uploaded ZIP into a fresh directory and return its files."""
    if not zipfile.is_zipfile(zip_path):
        raise PackageError("uploaded file is not a valid ZIP archive")

    total_bytes = 0
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            _safe_member_name(info)
            total_bytes += info.file_size
            if total_bytes > config.MAX_TOTAL_EXTRACTED_BYTES:
                raise PackageError(
                    "extracted package exceeds "
                    f"{config.MAX_TOTAL_EXTRACTED_BYTES} bytes"
                )
        zf.extractall(dest_dir)

    files = sorted(p for p in dest_dir.rglob("*") if p.is_file())
    if not files:
        raise PackageError("ZIP archive contains no files")
    return files


# ------------------------------------------------------------------- job pipeline

def process_job(job_id: str, zip_path: Path, original_name: str) -> None:
    """Run the full pipeline.  Called as a FastAPI background task."""
    job_dir = config.JOBS_DIR / job_id
    extract_dir = job_dir / "input"
    output_dir = job_dir / "output"
    extract_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    result_zip = job_dir / f"{job_id}_anonymized.zip"

    try:
        # The job row is created synchronously by the API before the
        # background task is scheduled; make sure it exists even when the
        # pipeline is invoked directly (tests, scripts).
        if database.get_job(job_id) is None:
            database.create_job(job_id, original_name)

        # ---- Stage 1: extraction + full validation (before any DB writes) ----
        files = extract_zip(zip_path, extract_dir)
        datasets: list[tuple[Path, object, dict]] = []
        requested: dict[str, set[str]] = {
            "patient": set(),
            "study": set(),
            "series": set(),
            "instance": set(),
        }
        for file_path in files:
            try:
                ds = identifiers.read_dicom(file_path)
            except Exception as exc:  # noqa: BLE001
                raise PackageError(
                    f"{file_path.relative_to(extract_dir)} is not a valid DICOM "
                    f"file: {exc}"
                ) from exc
            identifiers.ensure_uncompressed(ds, file_path.name)
            ids = identifiers.collect_identifiers(ds)
            for id_type, values in ids.items():
                requested[id_type].update(values)
            # Remember original values for the mapping report (the dataset
            # itself is rewritten in stage 3).
            originals = {
                "patient": str(getattr(ds, "PatientID", "") or "").strip(),
                "study": str(getattr(ds, "StudyInstanceUID", "") or "").strip(),
                "series": str(getattr(ds, "SeriesInstanceUID", "") or "").strip(),
                "instance": str(getattr(ds, "SOPInstanceUID", "") or "").strip(),
            }
            datasets.append((file_path, ds, originals))

        if len(datasets) > config.MAX_DICOMS_PER_PACKAGE:
            raise PackageError(
                f"package contains {len(datasets)} DICOM files; limit is "
                f"{config.MAX_DICOMS_PER_PACKAGE} per ZIP"
            )

        # ---- Stage 2: short reservation transaction (the only early DB write) -
        mappings = database.reserve_mappings(
            job_id, requested, identifiers.generate_anonymized
        )

        # ---- Stage 3: rewrite + repackage (no DB write lock held) ------------
        report_entries = []
        for file_path, ds, originals in datasets:
            rel = file_path.relative_to(extract_dir)
            identifiers.anonymize_dataset(ds, mappings)
            out_path = output_dir / rel
            out_path.parent.mkdir(parents=True, exist_ok=True)
            # save_as keeps file_meta (class UID + transfer syntax) and writes
            # the encoded pixel bytes back unchanged.
            ds.save_as(str(out_path), enforce_file_format=True)
            report_entries.append(
                {
                    "file": str(rel),
                    "original_patient_id": originals["patient"],
                    "anonymized_patient_id": mappings["patient"].get(
                        originals["patient"], ""
                    ),
                    "original_study_uid": originals["study"],
                    "anonymized_study_uid": str(ds.StudyInstanceUID),
                    "original_series_uid": originals["series"],
                    "anonymized_series_uid": str(ds.SeriesInstanceUID),
                    "original_sop_uid": originals["instance"],
                    "anonymized_sop_uid": str(ds.SOPInstanceUID),
                }
            )

        with zipfile.ZipFile(result_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(output_dir.rglob("*")):
                if path.is_file():
                    zf.write(path, path.relative_to(output_dir).as_posix())

        report = {
            "job_id": job_id,
            "dicom_count": len(datasets),
            "mappings_applied": {
                id_type: len(vals) for id_type, vals in mappings.items()
            },
            "files": report_entries,
        }
        report_path = job_dir / "mapping_report.json"
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))

        # ---- Stage 4: commit only after the complete package exists ----------
        database.commit_job(job_id)
        database.update_job(
            job_id,
            status="done",
            result_name=result_zip.name,
            report_json=json.dumps(report, ensure_ascii=False),
        )
    except Exception as exc:  # noqa: BLE001 - report any failure, roll back
        message = str(exc) or exc.__class__.__name__
        try:
            database.fail_job(job_id, message)
        except Exception:  # noqa: BLE001
            # Job row may be missing if failure preceded create_job; ensure it
            # exists so the client can query the failure reason.
            database.create_job(job_id, original_name)
            database.fail_job(job_id, message)
        if result_zip.exists():
            result_zip.unlink()
    finally:
        # Extracted working copy is no longer needed once processing finished.
        # The source ZIP itself is owned by the caller (the API removes its
        # uploaded temporary file after the background task completes).
        shutil.rmtree(extract_dir, ignore_errors=True)
