#!/usr/bin/env bash
# Bootstrap local sqld (if needed) and run remote-gated native validation.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

LISTEN="${SQLD_HTTP_LISTEN_ADDR:-127.0.0.1:18080}"
export LIBSQL_REMOTE_URL="${LIBSQL_REMOTE_URL:-http://${LISTEN}}"
export LIBSQL_AUTH_TOKEN="${LIBSQL_AUTH_TOKEN:-}"

if ! curl -fsS "${LIBSQL_REMOTE_URL}/health" >/dev/null 2>&1; then
  echo "remote endpoint not up; starting local sqld..."
  ./scripts/start-sqld.sh
fi

echo "running remote native validations against ${LIBSQL_REMOTE_URL}"
# Serial: shared sqld queue/history must not interleave fixed-ID provider tests.
cargo test --no-default-features --features native-libsql \
  --test remote_libsql_provider -- --nocapture --test-threads=1

echo
if [[ "${RUN_REMOTE_STRESS:-0}" == "1" ]]; then
  echo "RUN_REMOTE_STRESS=1: running remote stress smoke..."
  ./scripts/run-remote-stress.sh
else
  echo "optional: RUN_REMOTE_STRESS=1 ./scripts/run-remote-tests.sh"
  echo "       or ./scripts/run-remote-stress.sh"
fi

echo "done."
