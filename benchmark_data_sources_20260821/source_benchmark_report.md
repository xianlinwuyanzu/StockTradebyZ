# Data source benchmark

- Symbols: AAPL, MSFT, AMZN, NVDA, HOOD, AAL, AJG, NOK
- Window: 2026-06-22 to 2026-08-21 (60 calendar days)
- yfinance uses one multi-symbol download request with threads disabled.
- yahoo_chart uses one direct query1.finance.yahoo.com Chart API request per symbol.
- quantdash first tries one batch request and optionally falls back to one request per missing symbol.
- This is a network benchmark, not a data-quality certification; repeat at different times because provider rate limits are dynamic.

## Results

| source | symbols_succeeded | symbols_failed | success_rate | rows_returned | elapsed_seconds | symbols_per_second | rows_per_second | request_count | errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| quantdash | 8 | 0 | 100.0% | 336 | 5.911 | 1.353 | 56.843 | 9 | batch PermissionError: Access mode 'batch' not available for 日/周/月K线查询. Upgrade your plan. |
| yfinance | 0 | 8 | 0.0% | 0 | 0.590 | 0.000 | 0.000 | 1 | empty or unrecognized yfinance response |
| yahoo_chart | 0 | 8 | 0.0% | 0 | 2.463 | 0.000 | 0.000 | 8 | AAPL: RuntimeError: HTTP 429: Edge: Too Many Requests | MSFT: RuntimeError: HTTP 429: Edge: Too Many Requests | AMZN: RuntimeError: HTTP 429: Edge: Too Many Requests | NVDA: RuntimeError: HTTP 429: Edge: Too Many Requests | HOOD: RuntimeError: HTTP 429: Edge: Too Many Requests | AAL: RuntimeError: HTTP 429: Edge: Too Many Requests | AJG: RuntimeError: HTTP 429: Edge: Too Many Requests | NOK: RuntimeError: HTTP 429: Edge: Too Many Requests |

## Interpretation

- Compare success rate before raw speed. A fast source returning partial data is not faster for the daily job.
- Compare elapsed seconds and request count together: batch APIs can be fast when batch access is available, but fallback requests change the result.
- Yahoo Chart API is direct and lightweight, but query1 rate limiting or IP reputation can dominate its effective throughput.
- yfinance and Yahoo Chart API share Yahoo infrastructure, so their rate-limit results are not independent.
