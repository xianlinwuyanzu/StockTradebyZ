#!/usr/bin/env bash
set -euo pipefail

# Package code only. No local market data, no caches, no generated reports.
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUTPUT_NAME="sf-code-only-$(date +%Y%m%d-%H%M%S).tar.gz"
OUTPUT_PATH="$ROOT_DIR/$OUTPUT_NAME"

cd "$ROOT_DIR"

tar \
  --exclude='.git' \
  --exclude='.venv' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='node_modules' \
  --exclude='data' \
  --exclude='stock_data_cache' \
  --exclude='jxt_v7/cache' \
  --exclude='jxt_v7/output' \
  --exclude='backtest_*.csv' \
  --exclude='backtest_*.png' \
  --exclude='selection_dashboard.html' \
  --exclude='selection_intersections.csv' \
  --exclude='selection_overlap.png' \
  --exclude='*.log' \
  --exclude='sf-code-only-*.tar.gz' \
  --exclude='.env' \
  -czf "$OUTPUT_PATH" .

echo "Code package created: $OUTPUT_PATH"
echo "Upload example: scp $OUTPUT_PATH user@your-server:/opt/"
