"""Identifier generation, collection and DICOM anonymization helpers."""
from __future__ import annotations

import uuid

import pydicom
from pydicom import Dataset
from pydicom.uid import UID, ExplicitVRLittleEndian, ImplicitVRLittleEndian

from . import config

# UID-bearing DICOM keywords that participate in the persistent mapping.
# ReferencedSOPInstanceUID is included: it is replaced through the *same*
# instance map as SOPInstanceUID, recursively inside sequences.
_UID_KEYWORDS: dict[str, str] = {
    "StudyInstanceUID": "study",
    "SeriesInstanceUID": "series",
    "SOPInstanceUID": "instance",
    "ReferencedSOPInstanceUID": "instance",
}

# Tags that must be emptied (preserving the tag itself with a zero-length value).
_CLEAR_KEYWORDS = ("PatientName", "PatientBirthDate")

# Only uncompressed transfer syntaxes are accepted (pixel bytes are kept
# verbatim and no transcoding is performed).
_UNCOMPRESSED_TS = {
    str(ImplicitVRLittleEndian),
    str(ExplicitVRLittleEndian),
    "1.2.840.10008.1.2.2",  # Explicit VR Big Endian (retired but uncompressed)
}


# --------------------------------------------------------------------- generation

def generate_anonymized(id_type: str) -> str:
    """Mint a persistent replacement value.

    UIDs use the 2.25 (ITU-U UUID-derived) OID branch:
    ``2.25.<decimal uuid4>`` which is guaranteed <= 64 chars and globally
    unique.  Patient IDs get a short readable ``ANON-<hex>`` form.
    """
    if id_type == "patient":
        return f"{config.PATIENT_ID_PREFIX}-{uuid.uuid4().hex[:16].upper()}"
    if id_type in ("study", "series", "instance"):
        return f"{config.UID_ORG_ROOT}.{uuid.uuid4().int}"
    raise ValueError(f"unknown id_type: {id_type}")


# ------------------------------------------------------------------- collection

def collect_identifiers(ds: Dataset) -> dict[str, set[str]]:
    """Collect every non-empty mapped identifier in a dataset.

    Traverses the top level and all nested sequences recursively, so
    ReferencedSOPInstanceUID values buried in e.g. ReferencedSOPSequence are
    found.  Out-of-package references are collected here as well and allocated
    a mapping even though no matching SOP exists inside the package.
    """
    found: dict[str, set[str]] = {
        "patient": set(),
        "study": set(),
        "series": set(),
        "instance": set(),
    }

    def visit(dataset: Dataset) -> None:
        for element in dataset:
            if element.VR == "SQ":
                for item in element.value or []:
                    visit(item)
                continue
            keyword = element.keyword
            if keyword == "PatientID":
                value = str(element.value).strip()
                if value:
                    found["patient"].add(value)
            elif keyword in _UID_KEYWORDS:
                value = str(element.value).strip()
                if value:
                    found[_UID_KEYWORDS[keyword]].add(value)

    visit(ds)
    return found


# ------------------------------------------------------------------ anonymization

def _replace_uid_value(element, id_type: str, mapping: dict[str, str]) -> None:
    original = str(element.value).strip()
    if not original:
        return
    replacement = mapping[id_type][original]
    # Assigning a UID instance keeps VR UI semantics intact.
    element.value = UID(replacement)


def anonymize_dataset(
    ds: Dataset, mappings: dict[str, dict[str, str]]
) -> Dataset:
    """Apply the mappings and PHI cleanup in place; return the same dataset.

    * PatientID and the three instance-level UIDs are replaced everywhere,
      including inside sequences; ReferencedSOPInstanceUID shares the instance
      map with SOPInstanceUID.
    * The file-meta SOP Instance UID is synchronised with the new instance UID.
    * MediaStorageSOPClassUID / TransferSyntaxUID are preserved.
    * PatientName and PatientBirthDate are emptied; all private tags removed.
    * Pixel data is never touched.
    """

    def visit(dataset: Dataset) -> None:
        for element in dataset:
            if element.VR == "SQ":
                for item in element.value or []:
                    visit(item)
                continue
            keyword = element.keyword
            if keyword == "PatientID":
                original = str(element.value).strip()
                if original:
                    element.value = mappings["patient"][original]
            elif keyword in _UID_KEYWORDS:
                _replace_uid_value(element, _UID_KEYWORDS[keyword], mappings)
            elif keyword in _CLEAR_KEYWORDS:
                element.value = ""
        # Remove every private group (including private creators).
        dataset.remove_private_tags()

    visit(ds)

    # Synchronise the file meta information with the new instance UID.
    meta = getattr(ds, "file_meta", None)
    if meta is not None and hasattr(meta, "MediaStorageSOPInstanceUID"):
        original = str(meta.MediaStorageSOPInstanceUID).strip()
        if original and original in mappings["instance"]:
            meta.MediaStorageSOPInstanceUID = UID(mappings["instance"][original])
    # MediaStorageSOPClassUID and TransferSyntaxUID deliberately stay as-is.
    return ds


# --------------------------------------------------------------------- validation

def ensure_uncompressed(ds: Dataset, source: str) -> None:
    """Reject compressed/deflated transfer syntaxes (pixel bytes are preserved
    verbatim, so only uncompressed DICOM is supported)."""
    ts = str(getattr(ds.file_meta, "TransferSyntaxUID", "")) if ds.file_meta else ""
    if ts not in _UNCOMPRESSED_TS:
        raise ValueError(
            f"{source}: compressed or unsupported transfer syntax {ts!r}; "
            "only uncompressed DICOM is accepted"
        )


def read_dicom(path) -> Dataset:
    """Strict read: a real DICOM file must carry the 'DICM' preamble marker."""
    return pydicom.dcmread(str(path), force=False)
