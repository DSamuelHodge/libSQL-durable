#!/usr/bin/env bash
# Multi-node primary/replica durability + remote tuning checks.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

PRIMARY_HTTP="${SQLD_PRIMARY_HTTP:-127.0.0.1:18080}"
REPLICA_HTTP="${SQLD_REPLICA_HTTP:-127.0.0.1:18081}"

export LIBSQL_REMOTE_URL="${LIBSQL_REMOTE_URL:-http://${PRIMARY_HTTP}}"
export LIBSQL_REPLICA_HTTP_URL="${LIBSQL_REPLICA_HTTP_URL:-http://${REPLICA_HTTP}}"
export LIBSQL_AUTH_TOKEN="${LIBSQL_AUTH_TOKEN:-}"
# Remote tuning exercised by the suite (can still be overridden by caller).
export LIBSQL_BUSY_TIMEOUT_MS="${LIBSQL_BUSY_TIMEOUT_MS:-5000}"
export LIBSQL_TRANSIENT_RETRIES="${LIBSQL_TRANSIENT_RETRIES:-4}"
export LIBSQL_RETRY_BASE_DELAY_MS="${LIBSQL_RETRY_BASE_DELAY_MS:-25}"

if ! curl -fsS "${LIBSQL_REMOTE_URL}/health" >/dev/null 2>&1 \
  || ! curl -fsS "${LIBSQL_REPLICA_HTTP_URL}/health" >/dev/null 2>&1; then
  echo "cluster not up; starting primary+replica..."
  ./scripts/start-cluster.sh
fi

echo "running multi-node cluster tests"
echo "  primary=${LIBSQL_REMOTE_URL}"
echo "  replica=${LIBSQL_REPLICA_HTTP_URL}"

cargo test --no-default-features --features native-libsql \
  --test cluster_libsql_provider -- --nocapture --test-threads=1

echo "done."
