#!/bin/sh
# Seed the shared volume with bundled synthetic samples on first start, then
# exec the given process (uvicorn).  Both API replicas run this entrypoint;
# the copy is a no-op once samples exist.
set -eu

mkdir -p "${DICOM_DATA_DIR}/samples"
if [ ! -f "${DICOM_DATA_DIR}/samples/sample_manifest.json" ]; then
    cp -an /opt/sample-seed/samples/. "${DICOM_DATA_DIR}/samples/" || true
fi

exec "$@"
