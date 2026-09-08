#!/usr/bin/env bash
set -euo pipefail

# Daily cloud workflow: fetch latest data on server and run stock selection.
PROJECT_DIR="${1:-/opt/sf}"
DATA_DIR="${DATA_DIR:-./data}"
CONFIG_FILE="${CONFIG_FILE:-./configs.json}"
TRADE_DATE="${TRADE_DATE:-}"

cd "$PROJECT_DIR"
mkdir -p ./logs

source ./deploy/load_env.sh
load_env_file ./.env
source ./deploy/daily_runtime.sh
init_daily_runtime

PY_BIN="${PY_BIN:-./.venv/bin/python}"
if ! command -v "$PY_BIN" >/dev/null 2>&1; then
  echo "Python is not executable: $PY_BIN. Set PY_BIN or run deploy/bootstrap_venv.sh first."
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config file does not exist: $CONFIG_FILE"
  exit 1
fi

if [[ -z "${TUSHARE_TOKEN:-}" ]]; then
  echo "TUSHARE_TOKEN is not set. Put it in .env or export it before running."
  exit 1
fi

run_timed_stage fetch "$PY_BIN" -m data_fetch.fetch_kline --out "$DATA_DIR"
if [[ -n "$TRADE_DATE" ]]; then
  run_timed_stage selection "$PY_BIN" select_stock.py --data-dir "$DATA_DIR" --config "$CONFIG_FILE" --date "$TRADE_DATE" "${SELECTION_EXTRA_ARGS[@]}"
else
  run_timed_stage selection "$PY_BIN" select_stock.py --data-dir "$DATA_DIR" --config "$CONFIG_FILE" "${SELECTION_EXTRA_ARGS[@]}"
fi

if [[ "${STOCK_TRACKING_PUSH_ENABLED:-1}" == "1" ]]; then
  echo "Publishing stock tracking reports..."
  if ! run_timed_stage publish "$PY_BIN" -m reporting.publish_stock_tracking_report --log ./select_results.log; then
    echo "Stock tracking report publish failed (daily selection is complete)"
    exit 1
  fi
fi

echo "Daily run completed."
