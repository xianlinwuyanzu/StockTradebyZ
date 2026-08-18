#!/usr/bin/env bash
set -euo pipefail

# Daily cloud workflow: fetch latest data on server and run stock selection.
PROJECT_DIR="${1:-/opt/sf}"
DATA_DIR="${DATA_DIR:-./data}"
CONFIG_FILE="${CONFIG_FILE:-./configs.json}"
TRADE_DATE="${TRADE_DATE:-}"

cd "$PROJECT_DIR"
mkdir -p ./logs

if [[ ! -f ./.venv/bin/python ]]; then
  echo "Missing virtual environment. Run deploy/bootstrap_venv.sh first."
  exit 1
fi

if [[ -f ./.env ]]; then
  set -a
  source ./.env
  set +a
fi

if [[ -z "${TUSHARE_TOKEN:-}" ]]; then
  echo "TUSHARE_TOKEN is not set. Put it in .env or export it before running."
  exit 1
fi

./.venv/bin/python fetch_kline.py --out "$DATA_DIR"
if [[ -n "$TRADE_DATE" ]]; then
  ./.venv/bin/python select_stock.py --data-dir "$DATA_DIR" --config "$CONFIG_FILE" --date "$TRADE_DATE"
else
  ./.venv/bin/python select_stock.py --data-dir "$DATA_DIR" --config "$CONFIG_FILE"
fi

echo "Daily run completed."
