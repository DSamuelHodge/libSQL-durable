#!/usr/bin/env bash
# Install a local sqld (libsql-server) binary into tools/bin without Docker.
# Prefer this on older Macs where Docker Desktop is heavy or unavailable.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT}/tools/bin"
VERSION="${SQLD_VERSION:-0.24.32}"
TAG="libsql-server-v${VERSION}"
BASE_URL="https://github.com/tursodatabase/libsql/releases/download/${TAG}"

OS="$(uname -s)"
ARCH="$(uname -m)"

case "${OS}-${ARCH}" in
  Darwin-x86_64)  TARGET="x86_64-apple-darwin" ;;
  Darwin-arm64)   TARGET="aarch64-apple-darwin" ;;
  Linux-x86_64)   TARGET="x86_64-unknown-linux-gnu" ;;
  Linux-aarch64)  TARGET="aarch64-unknown-linux-gnu" ;;
  *)
    echo "unsupported platform: ${OS}-${ARCH}" >&2
    echo "install Docker/Colima and use the README container path, or download a release manually." >&2
    exit 1
    ;;
esac

ARCHIVE="libsql-server-${TARGET}.tar.xz"
TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "${TMP_DIR}"; }
trap cleanup EXIT

echo "downloading ${ARCHIVE} (${TAG})..."
curl -fsSL -o "${TMP_DIR}/${ARCHIVE}" "${BASE_URL}/${ARCHIVE}"
tar -xJf "${TMP_DIR}/${ARCHIVE}" -C "${TMP_DIR}"

# Archive layout: libsql-server-<target>/sqld
SQLD_SRC="$(find "${TMP_DIR}" -type f -name sqld | head -n 1)"
if [[ -z "${SQLD_SRC}" ]]; then
  echo "sqld binary not found inside archive" >&2
  exit 1
fi

mkdir -p "${BIN_DIR}"
cp "${SQLD_SRC}" "${BIN_DIR}/sqld"
chmod +x "${BIN_DIR}/sqld"

echo "installed: ${BIN_DIR}/sqld"
"${BIN_DIR}/sqld" --version 2>/dev/null || "${BIN_DIR}/sqld" --help | head -n 5
echo
echo "next:"
echo "  ./scripts/start-sqld.sh"
echo "  ./scripts/run-remote-tests.sh"
