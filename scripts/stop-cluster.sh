#!/usr/bin/env bash
# Stop multi-node cluster started by scripts/start-cluster.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

stop_one() {
  local pid_file="$1"
  local name="$2"
  if [[ ! -f "${pid_file}" ]]; then
    echo "no ${name} pid file"
    return 0
  fi
  local pid
  pid="$(cat "${pid_file}")"
  if kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" || true
    for _ in $(seq 1 20); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "${pid}" 2>/dev/null; then
      kill -9 "${pid}" || true
    fi
    echo "stopped ${name} (pid ${pid})"
  else
    echo "${name} process ${pid} not running"
  fi
  rm -f "${pid_file}"
}

stop_one "${ROOT}/tmp/cluster-replica.pid" "replica"
stop_one "${ROOT}/tmp/cluster-primary.pid" "primary"
