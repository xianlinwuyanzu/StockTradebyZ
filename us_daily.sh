#!/bin/bash
set -e

cd "$(dirname "$0")"

echo ">>> 开始更新K线数据..."
python fetch_kline_yfince_us.py

echo ">>> 开始选股..."
python select_stock.py

echo ">>> 全部完成"
