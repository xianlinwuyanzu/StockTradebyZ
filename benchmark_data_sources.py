from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf
from quantdash import QuantDash

from fetch_kline_quantdash_us import _normalize_market_symbol, _standardize_ohlcv, _to_dataframe_dict
from fetch_kline_yahoo_chart_us import YahooChartClient

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "AMZN", "NVDA", "HOOD", "AAL", "AJG", "NOK"]
DEFAULT_DAYS = 60
DEFAULT_OUTPUT = Path("./benchmark_data_sources_20260821")


def symbol_rows_from_yfinance(data: Any, symbols: list[str]) -> dict[str, int]:
    rows: dict[str, int] = {}
    if not isinstance(data, pd.DataFrame) or data.empty:
        return rows
    if isinstance(data.columns, pd.MultiIndex):
        level0 = {str(value).upper() for value in data.columns.get_level_values(0)}
        level1 = {str(value).upper() for value in data.columns.get_level_values(1)}
        for symbol in symbols:
            upper = symbol.upper()
            if upper in level0:
                part = data[upper]
            elif upper in level1:
                part = data.xs(upper, axis=1, level=1)
            else:
                continue
            close_column = next((column for column in part.columns if str(column).lower() == "close"), None)
            if close_column is not None:
                rows[symbol] = int(part[close_column].dropna().shape[0])
        return rows

    if len(symbols) == 1:
        close_column = next((column for column in data.columns if str(column).lower() == "close"), None)
        if close_column is not None:
            rows[symbols[0]] = int(data[close_column].dropna().shape[0])
    return rows


def result_row(
    source: str,
    symbols: list[str],
    rows: dict[str, int],
    elapsed: float,
    requests: int,
    errors: list[str],
) -> dict[str, Any]:
    success = len(rows)
    return {
        "source": source,
        "symbols_requested": len(symbols),
        "symbols_succeeded": success,
        "symbols_failed": len(symbols) - success,
        "success_rate": success / len(symbols) if symbols else 0.0,
        "rows_returned": sum(rows.values()),
        "mean_rows_per_success": sum(rows.values()) / success if success else 0.0,
        "elapsed_seconds": elapsed,
        "symbols_per_second": success / elapsed if elapsed else 0.0,
        "rows_per_second": sum(rows.values()) / elapsed if elapsed else 0.0,
        "request_count": requests,
        "errors": " | ".join(errors[:8]),
    }


def fetch_yfinance(symbols: list[str], start: datetime, end: datetime, timeout: float) -> tuple[dict[str, int], int, list[str]]:
    errors: list[str] = []
    try:
        data = yf.download(
            tickers=symbols,
            start=start,
            end=end,
            interval="1d",
            threads=False,
            progress=False,
            group_by="ticker",
            auto_adjust=False,
            actions=False,
            timeout=timeout,
        )
        rows = symbol_rows_from_yfinance(data, symbols)
        if not rows:
            errors.append("empty or unrecognized yfinance response")
        return rows, 1, errors
    except Exception as exc:
        return {}, 1, [f"{type(exc).__name__}: {exc}"]


def fetch_yahoo_chart(
    symbols: list[str],
    start: datetime,
    end: datetime,
    timeout: float,
    retries: int,
    pause: float,
) -> tuple[dict[str, int], int, list[str]]:
    client = YahooChartClient(timeout=timeout, max_retries=retries, request_pause=pause)
    rows: dict[str, int] = {}
    errors: list[str] = []
    for symbol in symbols:
        try:
            frame = client.fetch(symbol, start, end)
            if not frame.empty:
                rows[symbol] = len(frame)
            else:
                errors.append(f"{symbol}: empty")
        except Exception as exc:
            errors.append(f"{symbol}: {type(exc).__name__}: {exc}")
    return rows, client.request_count, errors


def fetch_quantdash(
    symbols: list[str],
    start: datetime,
    end: datetime,
    timeout: float,
    pause: float,
    fallback: bool,
) -> tuple[dict[str, int], int, list[str]]:
    del timeout
    api_key = os.environ.get("QUANTDASH_API_KEY", "").strip()
    if not api_key:
        return {}, 0, ["QUANTDASH_API_KEY is not configured"]

    qd = QuantDash(api_key=api_key, max_retries=0)
    qsymbols = [_normalize_market_symbol(symbol, default_market="US") for symbol in symbols]
    symbol_map = dict(zip(qsymbols, symbols))
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    rows: dict[str, int] = {}
    errors: list[str] = []
    requests = 0
    missing = list(qsymbols)

    try:
        requests += 1
        payload = qd.klines.batch(
            symbols=qsymbols,
            period="1d",
            start_time=start_ms,
            end_time=end_ms,
            to_dataframe=False,
            show_progress=False,
            max_workers=4,
            batch_size=100,
        )
        result = _to_dataframe_dict(payload, qsymbols)
        for qsymbol, frame in result.items():
            standard = _standardize_ohlcv(frame, period="1d")
            ticker = symbol_map.get(qsymbol.upper())
            if ticker and standard is not None and not standard.empty:
                rows[ticker] = len(standard)
        missing = [symbol for symbol in qsymbols if symbol not in result]
    except Exception as exc:
        errors.append(f"batch {type(exc).__name__}: {exc}")

    if fallback and missing:
        for qsymbol in missing:
            try:
                if pause > 0:
                    time.sleep(pause)
                requests += 1
                payload = qd.klines.get(
                    symbol=qsymbol,
                    period="1d",
                    start_time=start_ms,
                    end_time=end_ms,
                    to_dataframe=False,
                )
                if isinstance(payload, dict):
                    payload = pd.DataFrame(payload)
                standard = _standardize_ohlcv(payload, period="1d")
                ticker = symbol_map[qsymbol]
                if standard is not None and not standard.empty:
                    rows[ticker] = len(standard)
                else:
                    errors.append(f"{ticker}: empty fallback")
            except Exception as exc:
                errors.append(f"{symbol_map.get(qsymbol, qsymbol)} fallback {type(exc).__name__}: {exc}")

    return rows, requests, errors


def run_source(name: str, args: argparse.Namespace, symbols: list[str], start: datetime, end: datetime) -> dict[str, Any]:
    begin = time.perf_counter()
    if name == "yfinance":
        rows, requests, errors = fetch_yfinance(symbols, start, end, args.timeout)
    elif name == "yahoo_chart":
        rows, requests, errors = fetch_yahoo_chart(
            symbols, start, end, args.timeout, args.chart_retries, args.chart_pause
        )
    elif name == "quantdash":
        rows, requests, errors = fetch_quantdash(
            symbols, start, end, args.timeout, args.qd_pause, args.quantdash_fallback
        )
    else:
        raise ValueError(name)
    elapsed = time.perf_counter() - begin
    return result_row(name, symbols, rows, elapsed, requests, errors)


def write_report(output: Path, summary: pd.DataFrame, symbols: list[str], start: datetime, end: datetime, args: argparse.Namespace) -> None:
    output.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output / "source_benchmark.csv", index=False)
    ranking = summary.sort_values(["success_rate", "elapsed_seconds"], ascending=[False, True]).reset_index(drop=True)
    lines = [
        "# Data source benchmark",
        "",
        f"- Symbols: {', '.join(symbols)}",
        f"- Window: {start.date()} to {end.date()} ({args.days} calendar days)",
        "- yfinance uses one multi-symbol download request with threads disabled.",
        "- yahoo_chart uses one direct query1.finance.yahoo.com Chart API request per symbol.",
        "- quantdash first tries one batch request and optionally falls back to one request per missing symbol.",
        "- This is a network benchmark, not a data-quality certification; repeat at different times because provider rate limits are dynamic.",
        "",
        "## Results",
        "",
    ]
    display = ranking[[
        "source", "symbols_succeeded", "symbols_failed", "success_rate", "rows_returned",
        "elapsed_seconds", "symbols_per_second", "rows_per_second", "request_count", "errors",
    ]].copy()
    for column in ["success_rate"]:
        display[column] = display[column].map(lambda value: f"{value * 100:.1f}%")
    for column in ["elapsed_seconds", "symbols_per_second", "rows_per_second"]:
        display[column] = display[column].map(lambda value: f"{value:.3f}")
    lines.extend([
        "| " + " | ".join(display.columns) + " |",
        "| " + " | ".join("---" for _ in display.columns) + " |",
    ])
    for row in display.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    lines.extend([
        "",
        "## Interpretation",
        "",
        "- Compare success rate before raw speed. A fast source returning partial data is not faster for the daily job.",
        "- Compare elapsed seconds and request count together: batch APIs can be fast when batch access is available, but fallback requests change the result.",
        "- Yahoo Chart API is direct and lightweight, but query1 rate limiting or IP reputation can dominate its effective throughput.",
        "- yfinance and Yahoo Chart API share Yahoo infrastructure, so their rate-limit results are not independent.",
    ])
    (output / "source_benchmark_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare yfinance, Yahoo Chart API, and QuantDash")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--chart-retries", type=int, default=1)
    parser.add_argument("--chart-pause", type=float, default=0.35)
    parser.add_argument("--qd-pause", type=float, default=float(os.environ.get("QD_REQ_INTERVAL", "0.35")))
    parser.add_argument("--no-quantdash-fallback", dest="quantdash_fallback", action="store_false")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        raise SystemExit("no symbols")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=max(1, args.days))
    results = []
    for source in ["yfinance", "yahoo_chart", "quantdash"]:
        print(f"running {source} symbols={len(symbols)}")
        result = run_source(source, args, symbols, start, end)
        results.append(result)
        print(result)
    summary = pd.DataFrame(results)
    write_report(args.output_dir, summary, symbols, start, end, args)
    print(f"wrote {args.output_dir / 'source_benchmark_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
