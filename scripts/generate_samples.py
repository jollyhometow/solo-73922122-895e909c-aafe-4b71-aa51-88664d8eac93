"""Generate fully synthetic DICOM demo packages.

No real personal information is used: every identifier, name and pixel frame
is fabricated inside this script.  Three ZIPs are produced:

* ``sample_crossref_a.zip`` - patient HOSP01-P-000999, study A, two CT slices
* ``sample_crossref_b.zip`` - same patient, study B (cross-batch linkage);
  its ReferencedSOPSequence points at a SOP inside package A *and* at a SOP
  that exists in neither package (out-of-package reference)
* ``sample_too_many.zip`` - 21 DICOM files, deliberately over the 20-file
  limit, to demonstrate failure handling with full mapping rollback

Run::

    python scripts/generate_samples.py [--out /data/samples]
"""
from __future__ import annotations

import argparse
import io
import json
import os
import zipfile
from pathlib import Path

from pydicom import Dataset, FileDataset, Sequence
from pydicom.dataset import FileMetaDataset
from pydicom.uid import (
    CTImageStorage,
    ExplicitVRLittleEndian,
    UID,
    generate_uid,
)

PATIENT_ID = "HOSP01-P-000999"
PATIENT_NAME = "PHANTOM^SYNTHETIC"
PATIENT_BIRTH_DATE = "19900101"

STUDY_A = "1.2.840.20261112.999.1"
SERIES_A = f"{STUDY_A}.1"
SOP_A1 = f"{SERIES_A}.1"
SOP_A2 = f"{SERIES_A}.2"

STUDY_B = "1.2.840.20261112.999.2"
SERIES_B = f"{STUDY_B}.1"
SOP_B1 = f"{SERIES_B}.1"

# Referenced only, never present in any package: must still get a mapping.
SOP_EXTERNAL = "1.2.840.20261112.999.77.88"

ROWS, COLS = 64, 64


def _pixel_bytes(slice_index: int) -> bytes:
    # Deterministic synthetic 16-bit frame, no numpy dependency.
    frame = bytearray()
    for r in range(ROWS):
        for c in range(COLS):
            value = (r * 4 + c * 2 + slice_index * 16) & 0xFFFF
            frame += value.to_bytes(2, "little")
    return bytes(frame)


def build_dicom(
    sop_uid: str,
    study_uid: str,
    series_uid: str,
    referenced_uids: list[str] | None = None,
    instance_number: int = 1,
) -> bytes:
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = CTImageStorage
    file_meta.MediaStorageSOPInstanceUID = UID(sop_uid)
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian

    ds = FileDataset("synthetic.dcm", {}, file_meta=file_meta, preamble=b"\0" * 128)
    # Little-endian / explicit VR are implied by the Explicit VR Little
    # Endian TransferSyntaxUID in file_meta (pydicom >= 3 derives them there).

    # Identifying information (to be removed/rewritten by the service).
    ds.PatientID = PATIENT_ID
    ds.PatientName = PATIENT_NAME
    ds.PatientBirthDate = PATIENT_BIRTH_DATE

    # UIDs (to be persistently mapped).
    ds.StudyInstanceUID = UID(study_uid)
    ds.SeriesInstanceUID = UID(series_uid)
    ds.SOPInstanceUID = UID(sop_uid)
    ds.SOPClassUID = CTImageStorage
    ds.FrameOfReferenceUID = generate_uid()

    # Descriptive synthetic attributes.
    ds.Modality = "CT"
    ds.StudyDescription = "SYNTHETIC PHANTOM STUDY"
    ds.SeriesDescription = "SYNTHETIC PHANTOM SERIES"
    ds.InstanceNumber = str(instance_number)
    ds.ContentDate = "20261112"
    ds.ContentTime = "090000"

    # Image pixel module (uncompressed, preserved byte-for-byte).
    ds.Rows = ROWS
    ds.Columns = COLS
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelData = _pixel_bytes(instance_number)

    # Private tags (to be deleted entirely by the service).
    ds.add_new(0x00090010, "LO", "SYNTHETIC SITE")          # private creator
    ds.add_new(0x00091001, "LO", "PRIVATE-SITE-IDENTIFIER")  # private element

    if referenced_uids:
        refs = []
        for ref_uid in referenced_uids:
            ref = Dataset()
            ref.ReferencedSOPClassUID = CTImageStorage
            ref.ReferencedSOPInstanceUID = UID(ref_uid)
            refs.append(ref)
        ds.ReferencedSOPSequence = Sequence(refs)

    buffer = io.BytesIO()
    ds.save_as(buffer, enforce_file_format=True)
    return buffer.getvalue()


def _write_zip(out_dir: Path, name: str, members: dict[str, bytes]) -> Path:
    path = out_dir / name
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for member, payload in sorted(members.items()):
            zf.writestr(member, payload)
    return path


def generate(out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    package_a = {
        "study_a/slice_0001.dcm": build_dicom(SOP_A1, STUDY_A, SERIES_A, instance_number=1),
        "study_a/slice_0002.dcm": build_dicom(SOP_A2, STUDY_A, SERIES_A, instance_number=2),
    }
    package_b = {
        "study_b/referencing_instance.dcm": build_dicom(
            SOP_B1,
            STUDY_B,
            SERIES_B,
            referenced_uids=[SOP_A1, SOP_EXTERNAL],
            instance_number=1,
        ),
    }
    package_too_many = {
        f"bulk/instance_{i:03d}.dcm": build_dicom(
            f"1.2.840.20261112.999.9.{i}",
            "1.2.840.20261112.999.9",
            "1.2.840.20261112.999.9.1",
            instance_number=i,
        )
        for i in range(1, 22)  # 21 files -> exceeds the 20-file limit
    }

    written = [
        str(_write_zip(out_dir, "sample_crossref_a.zip", package_a).name),
        str(_write_zip(out_dir, "sample_crossref_b.zip", package_b).name),
        str(_write_zip(out_dir, "sample_too_many.zip", package_too_many).name),
    ]

    manifest = {
        "note": "Synthetic data only; no real personal information.",
        "shared_patient_id": PATIENT_ID,
        "packages": {
            "sample_crossref_a.zip": {
                "study_uid": STUDY_A,
                "series_uid": SERIES_A,
                "sop_uids": [SOP_A1, SOP_A2],
            },
            "sample_crossref_b.zip": {
                "study_uid": STUDY_B,
                "series_uid": SERIES_B,
                "sop_uids": [SOP_B1],
                "referenced_sop_uids": {
                    SOP_A1: "inside package A (cross-package reference)",
                    SOP_EXTERNAL: "out-of-package reference (mapped anyway)",
                },
            },
            "sample_too_many.zip": {
                "dicom_count": 21,
                "expected": "rejected: >20 DICOM files; any reservation rolls back",
            },
        },
        "demo_expectations": [
            "Uploading A then B yields the SAME anonymized PatientID in both.",
            "B's referenced SOP from A is rewritten to A's anonymized SOP UID.",
            "The out-of-package SOP also receives a stable anonymized UID.",
            "Concurrent first-time uploads of the same identifier converge on "
            "one shared mapping.",
        ],
    }
    (out_dir / "sample_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False)
    )
    manifest_path = out_dir / "sample_manifest.json"
    print(f"wrote {written} and {manifest_path.name} into {out_dir}")
    return manifest


def main() -> None:
    default = Path(os.environ.get("DICOM_DATA_DIR", "/data")) / "samples"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=default)
    args = parser.parse_args()
    generate(args.out)


if __name__ == "__main__":
    main()
