#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

source ./deploy/load_env.sh
load_env_file ./.env
source ./deploy/daily_runtime.sh
init_daily_runtime

VENV_PY="./.venv/bin/python"
PY_BIN="${PY_BIN:-$VENV_PY}"
DATA_SOURCE="${DATA_SOURCE:-quantdash}"
QD_SKIP_FRESH_DAYS="${QD_SKIP_FRESH_DAYS:-0}"
YAHOO_CHART_SKIP_FRESH_DAYS="${YAHOO_CHART_SKIP_FRESH_DAYS:-0}"
DATA_DIR="${DATA_DIR:-./data/us_stocks}"
CONFIG_FILE="${CONFIG_FILE:-./configs.json}"
TRADE_DATE="${TRADE_DATE:-}"
QD_DAYS="${QD_DAYS:-900}"
QD_COUNT="${QD_COUNT:-500}"

ensure_python_env() {
	if ! command -v "$PY_BIN" >/dev/null 2>&1; then
		echo ">>> Python 不可执行: $PY_BIN；请设置 PY_BIN 或运行 ./deploy/bootstrap_venv.sh ."
		exit 1
	fi

	local dependencies="pandas, tqdm, scipy, numpy, matplotlib, requests"
	case "$DATA_SOURCE" in
		yfinance) dependencies="$dependencies, yfinance, lxml" ;;
		quantdash) dependencies="$dependencies, quantdash" ;;
		yahoo_chart) ;;
		*)
			echo ">>> 未知 DATA_SOURCE: $DATA_SOURCE (可选: yfinance, quantdash, yahoo_chart)"
			exit 1
			;;
	esac
	if ! "$PY_BIN" -c "import $dependencies" >/dev/null 2>&1; then
		echo ">>> 虚拟环境依赖不完整，请先运行 ./deploy/bootstrap_venv.sh ."
		exit 1
	fi
}

if [[ ! -f "$CONFIG_FILE" ]]; then
	echo ">>> 配置文件不存在: $CONFIG_FILE"
	exit 1
fi
run_timed_stage environment ensure_python_env

echo ">>> 开始更新K线数据..."
case "$DATA_SOURCE" in
	yfinance)
		echo ">>> 数据源: yfinance"
		run_timed_stage fetch "$PY_BIN" -m data_fetch.fetch_kline_yfince_us --out "$DATA_DIR"
		;;
	quantdash)
		echo ">>> 数据源: QuantDash"
		echo ">>> QuantDash skip-fresh-days: $QD_SKIP_FRESH_DAYS"
		run_timed_stage fetch "$PY_BIN" -m data_fetch.fetch_kline_quantdash_us --out "$DATA_DIR" --days "$QD_DAYS" --count "$QD_COUNT" --skip-fresh-days "$QD_SKIP_FRESH_DAYS"
		;;
	yahoo_chart)
		echo ">>> 数据源: Yahoo Finance Chart API"
		echo ">>> Yahoo Chart skip-fresh-days: $YAHOO_CHART_SKIP_FRESH_DAYS"
		run_timed_stage fetch "$PY_BIN" -m data_fetch.fetch_kline_yahoo_chart_us --out "$DATA_DIR" --skip-fresh-days "$YAHOO_CHART_SKIP_FRESH_DAYS"
		;;
	*)
		echo ">>> 未知 DATA_SOURCE: $DATA_SOURCE (可选: yfinance, quantdash, yahoo_chart)"
		exit 1
		;;
esac

echo ">>> 开始选股..."
selection_args=(--data-dir "$DATA_DIR" --config "$CONFIG_FILE")
if [[ -n "$TRADE_DATE" ]]; then
	selection_args+=(--date "$TRADE_DATE")
fi
run_timed_stage selection "$PY_BIN" select_stock.py "${selection_args[@]}" "${SELECTION_EXTRA_ARGS[@]}"

if [[ "${STOCK_TRACKING_PUSH_ENABLED:-1}" == "1" ]]; then
	echo ">>> 推送个股跟踪结果到前端..."
	if ! run_timed_stage publish "$PY_BIN" -m reporting.publish_stock_tracking_report --log ./select_results.log; then
		echo ">>> 个股跟踪结果推送失败，日更任务返回失败"
		exit 1
	fi
fi

echo ">>> 全部完成"
