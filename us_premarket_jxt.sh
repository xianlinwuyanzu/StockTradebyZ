#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

PY_BIN="${PY_BIN:-./.venv/bin/python}"
if [[ ! -x "$PY_BIN" ]]; then
	PY_BIN="python3"
fi

echo ">>> JXT v7 premarket single refresh started"
"$PY_BIN" run_jxt_v7.py --ticker SPY --output-dir ./jxt_v7/output --cache-dir ./jxt_v7/cache

echo ">>> JXT v7 premarket run completed"
