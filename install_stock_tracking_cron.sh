#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
MARKER="# sf-stock-tracking-9am-job"
JOB_LINE="0 9 * * * cd ${ROOT_DIR} && DATA_SOURCE=quantdash STOCK_TRACKING_PUSH_ENABLED=1 bash ./us_daily.sh >> logs/us_daily_cron.log 2>&1 ${MARKER}"

CURRENT_CRON="$(mktemp)"
NEXT_CRON="$(mktemp)"
trap 'rm -f "$CURRENT_CRON" "$NEXT_CRON"' EXIT

crontab -l > "$CURRENT_CRON" 2>/dev/null || true

grep -vF "$MARKER" "$CURRENT_CRON" > "$NEXT_CRON" || true

echo "CRON_TZ=Asia/Shanghai" >> "$NEXT_CRON"
echo "$JOB_LINE" >> "$NEXT_CRON"

awk '!seen[$0]++' "$NEXT_CRON" | crontab -

echo "Installed cron job: $JOB_LINE"
