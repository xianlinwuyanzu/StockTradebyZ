#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

PY_BIN="${PY_BIN:-./.venv/bin/python}"
if [[ ! -x "$PY_BIN" ]]; then
	PY_BIN="python3"
fi

if [[ -f ./.env ]]; then
	set -a
	source ./.env
	set +a
fi

echo ">>> 开始更新K线数据..."
"$PY_BIN" fetch_kline_yfince_us.py

echo ">>> 开始选股..."
"$PY_BIN" select_stock.py

echo ">>> 全部完成"
