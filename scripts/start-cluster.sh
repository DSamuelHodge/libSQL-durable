#!/usr/bin/env bash
# Start a local multi-node sqld primary + replica (plaintext gRPC, no Docker).
#
# Primary HTTP :18080  gRPC :15001
# Replica HTTP :18081  (forwards writes to primary, replicates reads)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${SQLD_BIN:-${ROOT}/tools/bin/sqld}"
PRIMARY_HTTP="${SQLD_PRIMARY_HTTP:-127.0.0.1:18080}"
PRIMARY_GRPC="${SQLD_PRIMARY_GRPC:-127.0.0.1:15001}"
REPLICA_HTTP="${SQLD_REPLICA_HTTP:-127.0.0.1:18081}"
PRIMARY_DATA="${SQLD_PRIMARY_DB_PATH:-${ROOT}/tmp/cluster-primary}"
REPLICA_DATA="${SQLD_REPLICA_DB_PATH:-${ROOT}/tmp/cluster-replica}"
PRIMARY_PID_FILE="${ROOT}/tmp/cluster-primary.pid"
REPLICA_PID_FILE="${ROOT}/tmp/cluster-replica.pid"
PRIMARY_LOG="${ROOT}/tmp/cluster-primary.log"
REPLICA_LOG="${ROOT}/tmp/cluster-replica.log"

if [[ ! -x "${BIN}" ]]; then
  echo "sqld not found at ${BIN}; run ./scripts/install-sqld.sh" >&2
  exit 1
fi

free_port() {
  local port="$1"
  local pid
  pid="$(lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null | head -n 1 || true)"
  if [[ -n "${pid}" ]]; then
    kill "${pid}" 2>/dev/null || true
    sleep 0.2
  fi
}

start_if_needed() {
  local pid_file="$1"
  local listen="$2"
  local name="$3"
  if [[ -f "${pid_file}" ]]; then
    local old
    old="$(cat "${pid_file}")"
    if kill -0 "${old}" 2>/dev/null; then
      if curl -fsS "http://${listen}/health" >/dev/null 2>&1; then
        echo "${name} already running (pid ${old}) on ${listen}"
        return 0
      fi
      kill "${old}" 2>/dev/null || true
    fi
    rm -f "${pid_file}"
  fi
  return 1
}

mkdir -p "${PRIMARY_DATA}" "${REPLICA_DATA}" "${ROOT}/tmp"

PRIMARY_EXTRA=()
REPLICA_EXTRA=()
if [[ -n "${SQLD_EXTENSIONS_PATH:-}" ]]; then
  PRIMARY_EXTRA+=(--extensions-path "${SQLD_EXTENSIONS_PATH}")
  REPLICA_EXTRA+=(--extensions-path "${SQLD_EXTENSIONS_PATH}")
fi
if [[ "${SQLD_ENABLE_BOTTOMLESS_REPLICATION:-0}" == "1" ]]; then
  PRIMARY_EXTRA+=(--enable-bottomless-replication)
  echo "bottomless replication enabled on primary"
fi

if ! start_if_needed "${PRIMARY_PID_FILE}" "${PRIMARY_HTTP}" "primary"; then
  free_port "${PRIMARY_HTTP##*:}"
  free_port "${PRIMARY_GRPC##*:}"
  nohup "${BIN}" \
    --db-path "${PRIMARY_DATA}" \
    --http-listen-addr "${PRIMARY_HTTP}" \
    --grpc-listen-addr "${PRIMARY_GRPC}" \
    --no-welcome \
    "${PRIMARY_EXTRA[@]}" \
    >"${PRIMARY_LOG}" 2>&1 &
  echo $! >"${PRIMARY_PID_FILE}"
fi

for _ in $(seq 1 40); do
  if curl -fsS "http://${PRIMARY_HTTP}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
if ! curl -fsS "http://${PRIMARY_HTTP}/health" >/dev/null 2>&1; then
  echo "primary failed to become healthy" >&2
  tail -n 40 "${PRIMARY_LOG}" >&2 || true
  exit 1
fi

if ! start_if_needed "${REPLICA_PID_FILE}" "${REPLICA_HTTP}" "replica"; then
  free_port "${REPLICA_HTTP##*:}"
  nohup "${BIN}" \
    --db-path "${REPLICA_DATA}" \
    --http-listen-addr "${REPLICA_HTTP}" \
    --primary-grpc-url "http://${PRIMARY_GRPC}" \
    --no-welcome \
    "${REPLICA_EXTRA[@]}" \
    >"${REPLICA_LOG}" 2>&1 &
  echo $! >"${REPLICA_PID_FILE}"
fi

for _ in $(seq 1 40); do
  if curl -fsS "http://${REPLICA_HTTP}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
if ! curl -fsS "http://${REPLICA_HTTP}/health" >/dev/null 2>&1; then
  echo "replica failed to become healthy" >&2
  tail -n 40 "${REPLICA_LOG}" >&2 || true
  exit 1
fi

echo "cluster ready:"
echo "  primary HTTP  http://${PRIMARY_HTTP}  (pid $(cat "${PRIMARY_PID_FILE}"))"
echo "  primary gRPC  ${PRIMARY_GRPC}"
echo "  replica HTTP  http://${REPLICA_HTTP}  (pid $(cat "${REPLICA_PID_FILE}"))"
echo "export LIBSQL_REMOTE_URL=http://${PRIMARY_HTTP}"
echo "export LIBSQL_REPLICA_HTTP_URL=http://${REPLICA_HTTP}"
