#!/usr/bin/env bash
# Start a local standalone sqld for remote provider validation (no Docker).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${SQLD_BIN:-${ROOT}/tools/bin/sqld}"
DATA_DIR="${SQLD_DB_PATH:-${ROOT}/tmp/sqld-data}"
LISTEN="${SQLD_HTTP_LISTEN_ADDR:-127.0.0.1:18080}"
PID_FILE="${ROOT}/tmp/sqld.pid"
LOG_FILE="${ROOT}/tmp/sqld.log"

if [[ ! -x "${BIN}" ]]; then
  echo "sqld not found at ${BIN}" >&2
  echo "run: ./scripts/install-sqld.sh" >&2
  exit 1
fi

if [[ -f "${PID_FILE}" ]]; then
  OLD_PID="$(cat "${PID_FILE}")"
  if kill -0 "${OLD_PID}" 2>/dev/null; then
    echo "sqld already running (pid ${OLD_PID}) on ${LISTEN}"
    echo "export LIBSQL_REMOTE_URL=http://${LISTEN}"
    exit 0
  fi
  rm -f "${PID_FILE}"
fi

mkdir -p "${DATA_DIR}" "$(dirname "${PID_FILE}")"

ARGS=(
  --db-path "${DATA_DIR}"
  --http-listen-addr "${LISTEN}"
)

# Optional trusted extensions directory (must contain trusted.lst).
if [[ -n "${SQLD_EXTENSIONS_PATH:-}" ]]; then
  ARGS+=(--extensions-path "${SQLD_EXTENSIONS_PATH}")
fi

# Optional continuous S3-compatible backup (bottomless). Requires AWS/S3 env
# vars expected by libsql-server's bottomless integration.
if [[ "${SQLD_ENABLE_BOTTOMLESS_REPLICATION:-0}" == "1" ]]; then
  ARGS+=(--enable-bottomless-replication)
  echo "bottomless replication enabled"
fi

# Prefer an alternate port by default: 8080 is often taken on developer machines.
nohup "${BIN}" "${ARGS[@]}" >"${LOG_FILE}" 2>&1 &
echo $! >"${PID_FILE}"

# Wait for HTTP health.
for _ in $(seq 1 40); do
  if curl -fsS "http://${LISTEN}/health" >/dev/null 2>&1; then
    echo "sqld ready on http://${LISTEN} (pid $(cat "${PID_FILE}"))"
    echo "export LIBSQL_REMOTE_URL=http://${LISTEN}"
    echo "log: ${LOG_FILE}"
    exit 0
  fi
  sleep 0.25
done

echo "sqld failed to become healthy; last log lines:" >&2
tail -n 40 "${LOG_FILE}" >&2 || true
exit 1
