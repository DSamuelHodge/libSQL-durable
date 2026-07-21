#!/usr/bin/env bash
# Light remote stress smoke against a local primary sqld (no Docker required).
#
# Defaults are conservative for older hardware / HTTP latency. Override with:
#   REMOTE_STRESS_MAX_CONCURRENT=2
#   REMOTE_STRESS_DURATION_SECS=2
#   REMOTE_STRESS_WAIT_TIMEOUT_SECS=120
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

LISTEN="${SQLD_HTTP_LISTEN_ADDR:-127.0.0.1:18080}"
export LIBSQL_REMOTE_URL="${LIBSQL_REMOTE_URL:-http://${LISTEN}}"
export LIBSQL_AUTH_TOKEN="${LIBSQL_AUTH_TOKEN:-}"

if ! curl -fsS "${LIBSQL_REMOTE_URL}/health" >/dev/null 2>&1; then
  echo "primary not up; starting local sqld..."
  ./scripts/start-sqld.sh
fi

echo "running remote stress smoke against ${LIBSQL_REMOTE_URL}"
# Serial: parallel + large-payload both use the same shared primary DB.
cargo test --no-default-features --features native-libsql \
  --test remote_libsql_provider_stress -- --nocapture --test-threads=1

echo "done."
