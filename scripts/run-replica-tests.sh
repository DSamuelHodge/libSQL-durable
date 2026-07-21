#!/usr/bin/env bash
# Run embedded remote-replica durability tests against a local primary sqld.
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

echo "running replica durability tests against primary ${LIBSQL_REMOTE_URL}"
# Serial: shared primary must not interleave fixed-queue assertions across tests.
cargo test --no-default-features --features native-libsql \
  --test replica_libsql_provider -- --nocapture --test-threads=1

echo "done."
