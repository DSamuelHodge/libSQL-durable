#!/usr/bin/env bash
# Stop the sqld started by scripts/start-sqld.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${ROOT}/tmp/sqld.pid"

if [[ ! -f "${PID_FILE}" ]]; then
  echo "no pid file at ${PID_FILE}"
  exit 0
fi

PID="$(cat "${PID_FILE}")"
if kill -0 "${PID}" 2>/dev/null; then
  kill "${PID}" || true
  # Give it a moment, then force if needed.
  for _ in $(seq 1 20); do
    if ! kill -0 "${PID}" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  if kill -0 "${PID}" 2>/dev/null; then
    kill -9 "${PID}" || true
  fi
  echo "stopped sqld (pid ${PID})"
else
  echo "process ${PID} not running"
fi
rm -f "${PID_FILE}"
