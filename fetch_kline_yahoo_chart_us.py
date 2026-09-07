from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from safe_io import is_safe_ticker, ticker_csv_path, write_dataframe_csv

PROFILE_FALLBACK = "Unknown"
DEFAULT_STOCKLIST = Path("./data/tools/stocklist_sp400_20260902.csv")
DEFAULT_OUT = Path("./data/us_stocks")
DEFAULT_DAYS = 300
DEFAULT_HOST = os.environ.get("YAHOO_CHART_HOST", "query1.finance.yahoo.com")
DEFAULT_PROXY = os.environ.get("YAHOO_CHART_PROXY") or os.environ.get("YF_PROXY") or ""
DEFAULT_TIMEOUT = float(os.environ.get("YAHOO_CHART_TIMEOUT", "30"))
DEFAULT_MAX_RETRIES = int(os.environ.get("YAHOO_CHART_MAX_RETRIES", "3"))
DEFAULT_REQUEST_PAUSE = float(os.environ.get("YAHOO_CHART_REQ_INTERVAL", "0.35"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("YahooChartFetcher")


def normalize_ticker(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    symbol = str(value).strip().upper()
    if not symbol or symbol in {"UNKNOWN", "NAN", "NONE", "NULL"}:
        return ""
    if symbol.endswith(".US"):
        symbol = symbol[:-3]
    normalized = symbol.replace(".", "-")
    return normalized if is_safe_ticker(normalized) else ""


def clean_profile_value(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "n/a", "unknown"}:
        return ""
    return text


def load_stock_list(path: Path) -> tuple[list[str], dict[str, dict[str, str]]]:
    frame = pd.read_csv(path)
    symbol_column = next(
        (column for column in ["ts_code", "symbol", "ticker", "code", "Symbol"] if column in frame.columns),
        frame.columns[0],
    )
    sector_column = next(
        (column for column in ["sector", "Sector", "gics_sector", "industry_sector"] if column in frame.columns),
        None,
    )
    industry_column = next(
        (column for column in ["industry", "Industry", "gics_industry", "gics_sub_industry"] if column in frame.columns),
        None,
    )

    tickers: list[str] = []
    profiles: dict[str, dict[str, str]] = {}
    for _, row in frame.iterrows():
        ticker = normalize_ticker(row.get(symbol_column))
        if not ticker:
            continue
        tickers.append(ticker)
        sector = clean_profile_value(row.get(sector_column)) if sector_column else ""
        industry = clean_profile_value(row.get(industry_column)) if industry_column else ""
        if sector or industry:
            profiles[ticker] = {
                "sector": sector or PROFILE_FALLBACK,
                "industry": industry or PROFILE_FALLBACK,
            }
    return list(dict.fromkeys(tickers)), profiles


def _retry_after(response: requests.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _is_retryable_status(status: int) -> bool:
    return status in {403, 429, 500, 502, 503, 504}


def _chart_frame(payload: dict[str, Any]) -> pd.DataFrame:
    chart = payload.get("chart") or {}
    error = chart.get("error")
    if error:
        description = error.get("description") or error.get("code") or "Yahoo Chart API error"
        raise RuntimeError(str(description))
    results = chart.get("result") or []
    if not results:
        return pd.DataFrame()

    result = results[0]
    timestamps = result.get("timestamp") or []
    quote_items = (result.get("indicators") or {}).get("quote") or []
    if not timestamps or not quote_items:
        return pd.DataFrame()
    quote = quote_items[0]
    frame = pd.DataFrame({
        "date": pd.to_datetime(timestamps, unit="s", utc=True).tz_convert("America/New_York").tz_localize(None).normalize(),
        "open": quote.get("open", []),
        "close": quote.get("close", []),
        "high": quote.get("high", []),
        "low": quote.get("low", []),
        "volume": quote.get("volume", []),
    })
    for column in ["open", "close", "high", "low", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["volume"] = frame["volume"].fillna(0)
    return frame.dropna(subset=["date", "open", "close", "high", "low"]).drop_duplicates("date").sort_values("date").reset_index(drop=True)


class YahooChartClient:
    def __init__(
        self,
        *,
        host: str = DEFAULT_HOST,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        request_pause: float = DEFAULT_REQUEST_PAUSE,
        proxy: str = DEFAULT_PROXY,
    ) -> None:
        self.host = host.replace("https://", "").rstrip("/")
        self.timeout = timeout
        self.max_retries = max(0, max_retries)
        self.request_pause = max(0.0, request_pause)
        self.proxy = proxy.strip()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
        })
        self.last_request_at = 0.0
        self.request_count = 0

    def _pace(self) -> None:
        elapsed = time.monotonic() - self.last_request_at
        if self.last_request_at and elapsed < self.request_pause:
            time.sleep(self.request_pause - elapsed)

    def fetch(self, ticker: str, start: datetime, end: datetime, interval: str = "1d") -> pd.DataFrame:
        symbol = normalize_ticker(ticker)
        if not symbol:
            raise ValueError("empty ticker")
        url = f"https://{self.host}/v8/finance/chart/{symbol}"
        params = {
            "period1": int(start.timestamp()),
            "period2": int(end.timestamp()),
            "interval": interval,
            "events": "history",
            "includeAdjustedClose": "true",
        }
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None

        for attempt in range(self.max_retries + 1):
            self._pace()
            try:
                self.last_request_at = time.monotonic()
                self.request_count += 1
                response = self.session.get(url, params=params, timeout=self.timeout, proxies=proxies)
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise RuntimeError(f"request failed: {exc}") from exc
                time.sleep(min(30.0, 2.0 * (attempt + 1) + random.uniform(0.1, 0.5)))
                continue

            if response.status_code == 200:
                try:
                    return _chart_frame(response.json())
                except (ValueError, TypeError, RuntimeError) as exc:
                    raise RuntimeError(f"invalid chart response: {exc}") from exc

            if not _is_retryable_status(response.status_code) or attempt >= self.max_retries:
                detail = response.text[:200].replace("\n", " ")
                raise RuntimeError(f"HTTP {response.status_code}: {detail}")

            retry_after = _retry_after(response)
            wait = retry_after if retry_after is not None else 2.0 * (attempt + 1)
            time.sleep(min(60.0, wait + random.uniform(0.1, 0.8)))

        raise RuntimeError("unreachable retry state")


def save_frame(ticker: str, frame: pd.DataFrame, output_dir: Path, profile: dict[str, str]) -> bool:
    if frame.empty:
        return False
    output = frame.copy()
    output["sector"] = clean_profile_value(profile.get("sector")) or PROFILE_FALLBACK
    output["industry"] = clean_profile_value(profile.get("industry")) or PROFILE_FALLBACK
    output = output[["date", "open", "close", "high", "low", "volume", "sector", "industry"]]
    write_dataframe_csv(output, output_dir, ticker)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch US daily data from Yahoo Finance Chart API")
    parser.add_argument("--stocklist", type=Path, default=DEFAULT_STOCKLIST)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--skip-fresh-days", type=float, default=0.0)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--request-pause", type=float, default=DEFAULT_REQUEST_PAUSE)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--proxy", default=DEFAULT_PROXY)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--smoke-symbol", default="AAPL")
    args = parser.parse_args()

    client = YahooChartClient(
        host=args.host,
        timeout=args.timeout,
        max_retries=args.max_retries,
        request_pause=args.request_pause,
        proxy=args.proxy,
    )
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=max(1, args.days))
    if args.smoke:
        try:
            frame = client.fetch(args.smoke_symbol, start, end)
            print(f"smoke ok symbol={normalize_ticker(args.smoke_symbol)} rows={len(frame)} requests={client.request_count}")
            print(frame.tail(5).to_string(index=False))
            return 0
        except Exception as exc:
            logger.error("smoke failed: %s", exc)
            return 2

    tickers, profiles = load_stock_list(args.stocklist)
    args.out.mkdir(parents=True, exist_ok=True)
    if args.skip_fresh_days > 0:
        before = len(tickers)
        fresh_candidates: list[str] = []
        for ticker in tickers:
            output_path = ticker_csv_path(args.out, ticker)
            if not output_path.exists() or (time.time() - output_path.stat().st_mtime) / 86400 >= args.skip_fresh_days:
                fresh_candidates.append(ticker)
        tickers = fresh_candidates
        logger.info("skip fresh files: %d, remaining: %d", before - len(tickers), len(tickers))

    success = 0
    failed: list[str] = []
    for index, ticker in enumerate(tickers, start=1):
        try:
            frame = client.fetch(ticker, start, end)
            if save_frame(ticker, frame, args.out, profiles.get(ticker, {})):
                success += 1
            else:
                failed.append(ticker)
        except Exception as exc:
            logger.warning("%s failed: %s", ticker, exc)
            failed.append(ticker)
        if index % 25 == 0 or index == len(tickers):
            logger.info("progress: %d/%d success=%d failed=%d requests=%d", index, len(tickers), success, len(failed), client.request_count)

    logger.info("done: success=%d failed=%d total=%d requests=%d", success, len(failed), len(tickers), client.request_count)
    if failed:
        logger.info("failed symbols: %s", ",".join(failed))
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
