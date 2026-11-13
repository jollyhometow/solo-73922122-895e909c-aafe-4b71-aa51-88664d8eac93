#!/bin/sh
# Start a temporary API replica with a small bounded queue and the
# registration gate, run the cancellation demo against it, then stop it.
# Used by: docker compose run --rm demo-cancel
set -eu

GATE="${DICOM_DATA_DIR}/cancel_demo_gate.flag"
PORT="${PORT:-8099}"
PY="${PYTHON:-python}"
command -v "${PY}" >/dev/null 2>&1 || PY=python3

export REPLICA_ID="${REPLICA_ID:-demo-cancel}"
export UPLOAD_WORKERS="${UPLOAD_WORKERS:-1}"
export UPLOAD_QUEUE_LIMIT="${UPLOAD_QUEUE_LIMIT:-2}"
export SQLITE_BUSY_TIMEOUT_MS="${SQLITE_BUSY_TIMEOUT_MS:-3000}"
export UPLOAD_REGISTRATION_GATE="${GATE}"
rm -f "${GATE}"

echo "[run_cancel_demo] starting temporary replica on :${PORT} with gate ${GATE}"
"${PY}" -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT}" \
    > "${DICOM_DATA_DIR}/cancel_demo_uvicorn.log" 2>&1 &
UVICORN_PID=$!

FINISHED=0
cleanup() {
    rm -f "${GATE}"
    if [ "${FINISHED}" -eq 0 ]; then
        # Abnormal exit: terminate the whole process group (uvicorn + shell).
        # Do not wait() — a non-interactive sh ignores SIGTERM while blocked
        # in wait(), which would otherwise hang the script at exit.
        kill -TERM 0 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# Wait for readiness.
i=0
until "${PY}" -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:${PORT}/health', timeout=2)" 2>/dev/null; do
    i=$((i + 1))
    if [ "$i" -ge 60 ]; then
        echo "replica did not become ready; log follows" >&2
        cat "${DICOM_DATA_DIR}/cancel_demo_uvicorn.log" >&2 || true
        exit 1
    fi
    sleep 0.5
done

"${PY}" scripts/demo_cancel.py --port "${PORT}" --gate "${GATE}"

# Normal completion: stop the temporary replica and leave without firing the
# abnormal-exit branch of the EXIT trap.
FINISHED=1
kill -TERM "${UVICORN_PID}" 2>/dev/null || true
for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "${UVICORN_PID}" 2>/dev/null || break
    sleep 0.5
done
kill -KILL "${UVICORN_PID}" 2>/dev/null || true
rm -f "${GATE}"
trap - EXIT INT TERM
exit 0
