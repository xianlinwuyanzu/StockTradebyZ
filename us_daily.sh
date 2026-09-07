#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

source ./deploy/load_env.sh
load_env_file ./.env

VENV_PY="./.venv/bin/python"
PY_BIN="${PY_BIN:-$VENV_PY}"
DATA_SOURCE="${DATA_SOURCE:-quantdash}"
QD_SKIP_FRESH_DAYS="${QD_SKIP_FRESH_DAYS:-0}"
YAHOO_CHART_SKIP_FRESH_DAYS="${YAHOO_CHART_SKIP_FRESH_DAYS:-0}"

ensure_python_env() {
	if [[ ! -x "$VENV_PY" ]]; then
		echo ">>> 虚拟环境不存在，请先运行 ./deploy/bootstrap_venv.sh ."
		exit 1
	fi

	if ! "$VENV_PY" -c "import pandas, tqdm, tushare, scipy, numpy, yfinance, quantdash, matplotlib, requests, lxml" >/dev/null 2>&1; then
		echo ">>> 虚拟环境依赖不完整，请先运行 ./deploy/bootstrap_venv.sh ."
		exit 1
	fi
}

ensure_python_env
PY_BIN="$VENV_PY"

echo ">>> 开始更新K线数据..."
case "$DATA_SOURCE" in
	yfinance)
		echo ">>> 数据源: yfinance"
		"$PY_BIN" fetch_kline_yfince_us.py
		;;
	quantdash)
		echo ">>> 数据源: QuantDash"
		echo ">>> QuantDash skip-fresh-days: $QD_SKIP_FRESH_DAYS"
				"$PY_BIN" fetch_kline_quantdash_us.py --days 900 --count 500 --skip-fresh-days "$QD_SKIP_FRESH_DAYS"
		;;
	yahoo_chart)
		echo ">>> 数据源: Yahoo Finance Chart API"
		echo ">>> Yahoo Chart skip-fresh-days: $YAHOO_CHART_SKIP_FRESH_DAYS"
		"$PY_BIN" fetch_kline_yahoo_chart_us.py --skip-fresh-days "$YAHOO_CHART_SKIP_FRESH_DAYS"
		;;
	*)
		echo ">>> 未知 DATA_SOURCE: $DATA_SOURCE (可选: yfinance, quantdash, yahoo_chart)"
		exit 1
		;;
esac

echo ">>> 开始选股..."
"$PY_BIN" select_stock.py

if [[ "${STOCK_TRACKING_PUSH_ENABLED:-1}" == "1" ]]; then
	echo ">>> 推送个股跟踪结果到前端..."
	if ! "$PY_BIN" publish_stock_tracking_report.py --log ./select_results.log; then
		echo ">>> 个股跟踪结果推送失败，日更任务返回失败"
		exit 1
	fi
fi

echo ">>> 全部完成"
