#!/usr/bin/env bash
set -euo pipefail

# Cloud bootstrap: create venv and install pip dependencies from requirements.txt.
PROJECT_DIR="${1:-/opt/sf}"
PY_BIN="${PY_BIN:-python3}"

cd "$PROJECT_DIR"

if ! command -v "$PY_BIN" >/dev/null 2>&1; then
  echo "Python not found: $PY_BIN"
  exit 1
fi

"$PY_BIN" -m venv .venv
./.venv/bin/python -m pip install --disable-pip-version-check --only-binary=:all: -r requirements.txt
./.venv/bin/python -m pip check

echo "Bootstrap complete."
echo "Next: create .env with TUSHARE_TOKEN, then run fetch/select commands."
