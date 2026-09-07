# Copilot Repository Instructions

For this repository, follow these placement rules for research work:

1. Any research or backtest script must be created under `analysis/code/`.
2. Any generated research output must be written under `analysis/runs/`.
3. Do not create `analysis_*`, `backtest_*`, `benchmark_*`, or visualization output folders in the repository root.
4. Do not move production runtime scripts unless explicitly requested.

Production/runtime examples:
- `select_stock.py`
- `us_daily.sh`
- `wave_structure.py`
- `fetch_kline_*.py`

When uncertain whether a new file is research or production, default to `analysis/code/` and mention that assumption.
