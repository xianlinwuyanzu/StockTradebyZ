# JXT v7.0 Starter (SPY, yfinance GEX)

This is a starter end-to-end implementation for one-symbol premarket analysis.

## Scope

- Symbol: SPY only by default
- Data source: yfinance
- Refresh mode: one-shot premarket refresh
- Output style: text-heavy report + machine-readable snapshots

## Outputs

Each run writes to `jxt_v7/output/YYYYMMDD/`:

- `SPY_levels.csv`: five key levels + summary score
- `SPY_report.md`: text-first narrative conclusion
- `SPY_snapshot.json`: full structured snapshot for downstream automation

## Run Once

```bash
python jxt_v7/run_jxt_v7.py --ticker SPY
```

## Premarket Script

```bash
bash us_premarket_jxt.sh
```

## Step-by-step Robustness Tests

```bash
python jxt_v7/run_jxt_v7.py --ticker SPY --run-tests --verbose
```

Test steps include:

1. Fetch daily/weekly/4H price data
2. Indicator integrity checks
3. Option chain + GEX checks (with cache fallback)
4. Five-level ordering checks
5. End-to-end output file existence checks

## Notes

- If live options fetch fails, latest cached options snapshot is used.
- This starter version is a map of key locations, not a deterministic predictor.
- Current implementation intentionally prioritizes robustness and explainability over strategy complexity.
