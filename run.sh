#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

MODE="${1:-dev}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

require_cmd go
require_cmd npm
require_cmd wails

WAILS_TAG=""
if [[ "$(uname -s)" == "Linux" ]]; then
  require_cmd pkg-config
  if pkg-config --exists webkit2gtk-4.1 && pkg-config --exists libsoup-3.0; then
    WAILS_TAG="webkit2_41"
  elif pkg-config --exists webkit2gtk-4.0 && pkg-config --exists libsoup-2.4; then
    WAILS_TAG="webkit2_40"
  else
    echo "error: missing Linux desktop dependencies for Wails WebKit" >&2
    echo "install (Ubuntu 24.04+): sudo apt install -y libgtk-3-dev libwebkit2gtk-4.1-dev" >&2
    echo "install (Ubuntu 22.04): sudo apt install -y libgtk-3-dev libwebkit2gtk-4.0-dev" >&2
    exit 1
  fi
fi

if [[ ! -d frontend ]]; then
  echo "error: frontend directory not found" >&2
  exit 1
fi

run_wails() {
  (
    while IFS='=' read -r name _; do
      if [[ "$name" == SNAP_* ]]; then
        unset "$name"
      fi
    done < <(env)
    unset GTK_EXE_PREFIX GTK_PATH GTK_MODULES GTK_IM_MODULE_FILE GIO_MODULE_DIR

    "$@"
  )
}

if [[ ! -d frontend/node_modules ]]; then
  echo "Installing frontend dependencies..."
  (cd frontend && npm install)
fi

case "$MODE" in
  dev)
    echo "Starting Puppy DB Tool (dev mode)..."
    if [[ -n "$WAILS_TAG" ]]; then
      run_wails wails dev -tags "$WAILS_TAG"
    else
      run_wails wails dev
    fi
    ;;
  build)
    echo "Building frontend..."
    (cd frontend && npm run build)
    echo "Building desktop binary..."
    if [[ -n "$WAILS_TAG" ]]; then
      run_wails wails build -tags "$WAILS_TAG"
    else
      run_wails wails build
    fi
    ;;
  build-win)
    echo "Building frontend..."
    (cd frontend && npm run build)
    echo "Building Windows .exe..."
    run_wails wails build -platform windows/amd64 -nopackage -o puppy-db-tool.exe
    ;;
  *)
    echo "Usage: ./run.sh [dev|build|build-win]" >&2
    exit 1
    ;;
esac
