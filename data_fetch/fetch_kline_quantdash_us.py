from __future__ import annotations

import argparse
import logging
import os
import random
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from quantdash import QuantDash
from tqdm import tqdm
from utils.safe_io import is_safe_ticker, ticker_csv_path, write_dataframe_csv

PROFILE_FALLBACK = "Unknown"

DEFAULT_STOCKLIST = Path("./data/tools/stocklist_sp400_20260902.csv")
DEFAULT_OUT = Path("./data/us_stocks")
DEFAULT_DAYS = 900
DEFAULT_PERIOD = "1d"
DEFAULT_BATCH_SIZE = 40
DEFAULT_QD_BATCH_SIZE = 100
DEFAULT_MAX_WORKERS = 4
DEFAULT_TIMEOUT_WAIT = 30
DEFAULT_COUNT = 500
MAX_RETRIES = int(os.environ.get("QD_MAX_RETRIES", "3"))
SINGLE_REQUEST_PAUSE = float(os.environ.get("QD_REQ_INTERVAL", "0.35"))
QD_SDK_MAX_RETRIES = int(os.environ.get("QD_SDK_MAX_RETRIES", "0"))
DEFAULT_BENCHMARK_SYMBOL = os.environ.get("QD_BENCHMARK_SYMBOL", "AAPL.US")
DEFAULT_MARKET_TZ = "America/New_York"
DEFAULT_DAILY_READY_HOUR_ET = int(os.environ.get("QD_DAILY_READY_HOUR_ET", "20"))


class FatalAuthError(RuntimeError):
    pass


class BatchModeUnavailableError(RuntimeError):
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("QuantDashFetcher")


def _normalize_ticker(code: str) -> str:
    if code is None:
        return ""
    s = str(code).strip().upper()
    if not s or s in {"UNKNOWN", "NAN", "NONE", "NULL"}:
        return ""
    normalized = s.replace("_", "-")
    return normalized if is_safe_ticker(normalized) else ""


def _normalize_market_symbol(symbol: str, default_market: str = "US") -> str:
    s = _normalize_ticker(symbol)
    if not s:
        return ""

    if "." in s:
        left, right = s.rsplit(".", 1)
        right_up = right.upper()
        # Keep explicit market suffix symbols, e.g. TSLA.US / 00700.HK.
        if left and right and right_up in {"US", "HK", "SH", "SZ", "BJ"}:
            return f"{left}.{right}"

    # Keep share-class dot notation for QuantDash, e.g. BRK.B -> BRK.B.US.
    # Some symbols may support dash aliases, but dot form is required for others.

    return f"{s}.{default_market}"


def _is_fresh(path: Path, fresh_days: float) -> bool:
    if fresh_days <= 0:
        return False
    if not path.exists():
        return False
    age_days = (time.time() - path.stat().st_mtime) / 86400
    return age_days < fresh_days


def _previous_weekday(d: date) -> date:
    cur = d - timedelta(days=1)
    while cur.weekday() >= 5:
        cur -= timedelta(days=1)
    return cur


def _infer_target_trade_date_by_time(daily_ready_hour_et: int) -> date:
    """Fallback target trade date based on US/Eastern time when benchmark probing is unavailable."""
    now_et = datetime.now(ZoneInfo(DEFAULT_MARKET_TZ))
    today = now_et.date()

    if now_et.weekday() >= 5:
        return _previous_weekday(today)

    if now_et.hour < daily_ready_hour_et:
        return _previous_weekday(today)
    return today


def _try_parse_trade_date(value: object) -> Optional[date]:
    if value is None:
        return None
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()


def _get_local_history_status(csv_path: Path) -> Tuple[Optional[date], int]:
    if not csv_path.exists():
        return None, 0
    try:
        df = pd.read_csv(csv_path, usecols=["date"])
    except Exception:
        return None, 0
    if df.empty:
        return None, 0
    return _try_parse_trade_date(df["date"].iloc[-1]), len(df)


def _get_local_latest_trade_date(csv_path: Path) -> Optional[date]:
    return _get_local_history_status(csv_path)[0]


def _get_local_row_count(csv_path: Path) -> int:
    return _get_local_history_status(csv_path)[1]


def _get_benchmark_latest_trade_date(qd: QuantDash, symbol: str, period: str) -> Optional[date]:
    """Probe one benchmark symbol to detect what the provider currently considers latest daily bar date."""
    try:
        p = str(period).lower()
        if p.endswith("m"):
            data = qd.klines.intraday(symbol=symbol, count=1, period=period, to_dataframe=False)
        else:
            data = qd.klines.get(symbol=symbol, count=1, period=period, to_dataframe=False)
    except Exception as e:
        if _is_auth_error(e):
            raise FatalAuthError(str(e)) from e
        logger.warning(f"benchmark probe failed: {symbol}: {type(e).__name__}: {e}")
        return None

    if isinstance(data, dict):
        data = pd.DataFrame(data)
    if not isinstance(data, pd.DataFrame) or data.empty:
        return None

    std = _standardize_ohlcv(data, period=period)
    if std is None or std.empty:
        return None

    return _try_parse_trade_date(std["date"].iloc[-1])


def _resolve_target_trade_date(
    qd: QuantDash,
    *,
    period: str,
    benchmark_symbol: str,
    daily_ready_hour_et: int,
) -> date:
    benchmark_date = _get_benchmark_latest_trade_date(qd, symbol=benchmark_symbol, period=period)
    if benchmark_date is not None:
        logger.info(f"target trade date from benchmark {benchmark_symbol}: {benchmark_date}")
        return benchmark_date

    fallback_date = _infer_target_trade_date_by_time(daily_ready_hour_et=daily_ready_hour_et)
    logger.warning(
        f"benchmark unavailable, fallback target trade date by ET clock ({daily_ready_hour_et}:00): {fallback_date}"
    )
    return fallback_date


def _filter_up_to_date_symbols(
    symbols: List[str],
    qd_symbol_to_ticker: Dict[str, str],
    out_dir: Path,
    target_trade_date: date,
    minimum_rows: int = 0,
) -> Tuple[List[str], int]:
    remaining: List[str] = []
    skipped = 0

    for qsym in symbols:
        ticker = qd_symbol_to_ticker[qsym]
        csv_path = ticker_csv_path(out_dir, ticker)
        local_latest, local_rows = _get_local_history_status(csv_path)
        if local_latest is not None and local_latest >= target_trade_date:
            if minimum_rows > 0 and local_rows < minimum_rows:
                remaining.append(qsym)
                continue
            skipped += 1
            continue
        remaining.append(qsym)

    return remaining, skipped


def _clean_profile_value(value: object) -> str:
    if value is None:
        return ""

    if isinstance(value, str):
        text = value.strip()
    elif isinstance(value, (list, tuple, set)):
        text = "|".join(str(v).strip() for v in value if str(v).strip())
    elif isinstance(value, dict):
        text = str(value.get("name") or value.get("raw") or "").strip()
    else:
        text = str(value).strip()

    if not text:
        return ""

    if text.lower() in {"nan", "none", "null", "n/a", "unknown"}:
        return ""
    return text


def load_stock_list(stocklist_path: Path) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    try:
        df = pd.read_csv(stocklist_path)
    except Exception as e:
        logger.error(f"failed to read stock list: {e}")
        return [], {}

    symbol_col = None
    for col in ["ts_code", "symbol", "ticker", "code", "Symbol"]:
        if col in df.columns:
            symbol_col = col
            break
    if symbol_col is None:
        if len(df.columns) == 0:
            logger.error("stock list has no columns")
            return [], {}
        symbol_col = df.columns[0]

    sector_col = None
    for col in ["sector", "Sector", "gics_sector", "industry_sector"]:
        if col in df.columns:
            sector_col = col
            break

    industry_col = None
    for col in ["industry", "Industry", "gics_industry", "gics_sub_industry"]:
        if col in df.columns:
            industry_col = col
            break

    tickers: List[str] = []
    profile_seed: Dict[str, Dict[str, str]] = {}

    for _, row in df.iterrows():
        ticker = _normalize_ticker(row.get(symbol_col, ""))
        if not ticker:
            continue
        tickers.append(ticker)

        sector = _clean_profile_value(row.get(sector_col, "")) if sector_col else ""
        industry = _clean_profile_value(row.get(industry_col, "")) if industry_col else ""
        if sector or industry:
            profile_seed[ticker] = {
                "sector": sector or PROFILE_FALLBACK,
                "industry": industry or PROFILE_FALLBACK,
            }

    tickers = list(dict.fromkeys(tickers))
    logger.info(f"loaded {len(tickers)} tickers from {stocklist_path}")
    if profile_seed:
        logger.info(f"preloaded {len(profile_seed)} profile rows from stocklist")

    return tickers, profile_seed


def _to_dataframe_dict(data: object, symbols: List[str]) -> Dict[str, pd.DataFrame]:
    result: Dict[str, pd.DataFrame] = {}

    if isinstance(data, dict):
        # case 1: compact single-payload dict {timestamp/open/...}
        compact_keys = {"timestamp", "open", "high", "low", "close", "volume"}
        if compact_keys.issubset(set(data.keys())) and len(symbols) == 1:
            df = pd.DataFrame(data)
            if not df.empty:
                result[symbols[0].upper()] = df
            return result

        # case 2: batch dict {SYMBOL: DataFrame or compact-dict}
        for sym, payload in data.items():
            s = str(sym).upper()
            if isinstance(payload, pd.DataFrame) and not payload.empty:
                result[s] = payload.copy()
            elif isinstance(payload, dict):
                df = pd.DataFrame(payload)
                if not df.empty:
                    result[s] = df
        return result

    if isinstance(data, pd.DataFrame):
        if data.empty:
            return result

        cols_lower = {str(c).lower(): c for c in data.columns}
        symbol_col = None
        for k in ["symbol", "code", "ticker"]:
            if k in cols_lower:
                symbol_col = cols_lower[k]
                break

        if symbol_col is not None:
            for sym, grp in data.groupby(symbol_col):
                s = str(sym).upper()
                if s:
                    result[s] = grp.copy()
            return result

        if len(symbols) == 1:
            result[symbols[0].upper()] = data.copy()
            return result

    return result


def _parse_time_column(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    numeric_valid = numeric.notna().sum()
    if numeric_valid >= max(1, len(series) // 2):
        max_abs = numeric.abs().max()
        if pd.notna(max_abs) and max_abs > 1e11:
            dt = pd.to_datetime(numeric, unit="ms", errors="coerce", utc=True)
        else:
            dt = pd.to_datetime(numeric, unit="s", errors="coerce", utc=True)
    else:
        dt = pd.to_datetime(series, errors="coerce", utc=True)

    try:
        dt = dt.dt.tz_convert(None)
    except Exception:
        try:
            dt = dt.dt.tz_localize(None)
        except Exception:
            pass

    return dt


def _standardize_ohlcv(df: pd.DataFrame, period: str) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None

    work = df.copy()
    work.columns = [str(c) for c in work.columns]

    def pick_col(candidates: List[str]) -> Optional[str]:
        for wanted in candidates:
            for c in work.columns:
                if str(c).lower() == wanted:
                    return c
        return None

    time_col = pick_col(["trade_time", "datetime", "time", "date", "trade_date", "timestamp"])
    open_col = pick_col(["open", "o"])
    high_col = pick_col(["high", "h"])
    low_col = pick_col(["low", "l"])
    close_col = pick_col(["close", "c"])
    vol_col = pick_col(["volume", "vol", "v"])

    if not all([time_col, open_col, high_col, low_col, close_col, vol_col]):
        return None

    work = work[[time_col, open_col, high_col, low_col, close_col, vol_col]].copy()
    work.columns = ["date", "open", "high", "low", "close", "volume"]

    work["date"] = _parse_time_column(work["date"])
    work = work.dropna(subset=["date"])

    for col in ["open", "high", "low", "close", "volume"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    work = work.dropna(subset=["open", "high", "low", "close"])
    work = work.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)

    if str(period).lower().endswith("d"):
        work["date"] = work["date"].dt.strftime("%Y-%m-%d")
    else:
        work["date"] = work["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return work[["date", "open", "close", "high", "low", "volume"]]


def _is_rate_limit_error(err: Exception) -> bool:
    msg = str(err).lower()
    return (
        "rate" in msg
        or "429" in msg
        or "too many" in msg
        or "limit" in msg
        or "forbidden" in msg
        or "503" in msg
    )


def _is_auth_error(err: Exception) -> bool:
    msg = str(err).lower()
    # Some SDK 429 texts include "authentication failed" prefix.
    # Treat explicit rate-limit signatures as retriable, not fatal auth errors.
    if _is_rate_limit_error(err):
        return False
    if "invalid or expired api key" in msg:
        return True
    if "unauthorized" in msg:
        return True
    if "authentication" in msg and "failed" in msg:
        return True
    return "401" in msg


def _is_batch_mode_unavailable(err: Exception) -> bool:
    msg = str(err).lower()
    return "access mode 'batch' not available" in msg or "upgrade your plan" in msg


def _extract_retry_after_seconds(err: Exception) -> Optional[float]:
    msg = str(err).lower()
    # Example messages: "Retry after 30780ms" / "retry after 12s" / "retry after 8 seconds"
    m = re.search(r"retry\s+after\s+([0-9]+(?:\.[0-9]+)?)\s*(ms|s|sec|secs|second|seconds)?", msg)
    if not m:
        return None

    value = float(m.group(1))
    unit = (m.group(2) or "").strip()
    if unit == "ms":
        return max(0.0, value / 1000.0)
    return max(0.0, value)


def _build_kline_kwargs(period: str, start_ms: int, end_ms: int, count: int) -> Dict[str, object]:
    kwargs: Dict[str, object] = {
        "period": period,
        "start_time": start_ms,
        "end_time": end_ms,
    }
    if count > 0:
        kwargs["count"] = count
    return kwargs


def _save_one(
    ticker: str,
    df: object,
    out_dir: Path,
    profile: Optional[Dict[str, str]],
    period: str,
) -> bool:
    if isinstance(df, dict):
        df = pd.DataFrame(df)

    if not isinstance(df, pd.DataFrame):
        return False

    std = _standardize_ohlcv(df, period=period)
    if std is None or std.empty:
        return False

    profile = profile or {}
    std["sector"] = _clean_profile_value(profile.get("sector", "")) or PROFILE_FALLBACK
    std["industry"] = _clean_profile_value(profile.get("industry", "")) or PROFILE_FALLBACK

    try:
        write_dataframe_csv(std, out_dir, ticker)
    except (OSError, ValueError) as exc:
        logger.warning("failed to save %s: %s", ticker, exc)
        return False
    return True


def _fetch_batch_with_retry(
    qd: QuantDash,
    symbols: List[str],
    *,
    period: str,
    start_ms: int,
    end_ms: int,
    count: int,
    max_workers: int,
    qd_batch_size: int,
) -> Dict[str, pd.DataFrame]:
    base_kwargs = _build_kline_kwargs(period=period, start_ms=start_ms, end_ms=end_ms, count=count)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = qd.klines.batch(
                symbols=symbols,
                **base_kwargs,
                to_dataframe=False,
                show_progress=False,
                max_workers=max_workers,
                batch_size=qd_batch_size,
            )
            result = _to_dataframe_dict(data, symbols)
            if result:
                return result
            logger.warning("batch returned empty result")
            return {}
        except Exception as e:
            if _is_auth_error(e):
                raise FatalAuthError(str(e)) from e

            if _is_batch_mode_unavailable(e):
                raise BatchModeUnavailableError(str(e)) from e

            if attempt >= MAX_RETRIES:
                logger.error(f"batch failed after retries: {e}")
                return {}

            if _is_rate_limit_error(e):
                wait_s = (20 * attempt) + random.uniform(2, 6)
                retry_after_s = _extract_retry_after_seconds(e)
                if retry_after_s is not None:
                    wait_s = max(wait_s, retry_after_s + random.uniform(0.2, 0.8))
                logger.warning(f"rate limit detected, wait {wait_s:.0f}s then retry ({attempt}/{MAX_RETRIES})")
            else:
                wait_s = (10 * attempt) + random.uniform(1, 3)
                logger.warning(f"batch error, wait {wait_s:.0f}s then retry ({attempt}/{MAX_RETRIES}): {e}")
            time.sleep(wait_s)

    return {}


def _fetch_one_with_retry(
    qd: QuantDash,
    symbol: str,
    *,
    period: str,
    start_ms: int,
    end_ms: int,
    count: int,
) -> Optional[pd.DataFrame]:
    base_kwargs = _build_kline_kwargs(period=period, start_ms=start_ms, end_ms=end_ms, count=count)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = qd.klines.get(symbol=symbol, **base_kwargs, to_dataframe=False)
            if isinstance(data, dict):
                data = pd.DataFrame(data)
            if isinstance(data, pd.DataFrame) and not data.empty:
                return data
            return None
        except Exception as e:
            if _is_auth_error(e):
                raise FatalAuthError(str(e)) from e

            if attempt >= MAX_RETRIES:
                logger.warning(f"single fetch failed: {symbol}: {e}")
                return None

            if _is_rate_limit_error(e):
                wait_s = (3 * attempt) + random.uniform(0.5, 1.5)
                retry_after_s = _extract_retry_after_seconds(e)
                if retry_after_s is not None:
                    wait_s = max(wait_s, retry_after_s + random.uniform(0.2, 0.8))
                logger.warning(
                    f"single rate limit: {symbol}, wait {wait_s:.1f}s then retry ({attempt}/{MAX_RETRIES})"
                )
            else:
                wait_s = (2 * attempt) + random.uniform(0.3, 0.9)
                logger.warning(
                    f"single fetch error: {symbol}, wait {wait_s:.1f}s then retry ({attempt}/{MAX_RETRIES}): {e}"
                )
            time.sleep(wait_s)

    return None


def preflight_auth(qd: QuantDash, symbol: str, period: str) -> None:
    """Call one lightweight API to validate API key before heavy batch work."""
    try:
        p = str(period).lower()
        if p.endswith("m"):
            qd.klines.intraday(symbol=symbol, period=period, count=1, to_dataframe=False)
        else:
            qd.klines.get(symbol=symbol, period=period, count=1, to_dataframe=False)
    except Exception as e:
        if _is_auth_error(e):
            raise FatalAuthError(str(e)) from e
        raise


def run_smoke_test(qd: QuantDash, symbol: str, period: str) -> int:
    logger.info(f"smoke test symbol={symbol}, period={period}")
    try:
        p = str(period).lower()
        if p.endswith("m"):
            data = qd.klines.intraday(symbol=symbol, count=5, period=period, to_dataframe=False)
        else:
            data = qd.klines.get(symbol=symbol, count=5, period=period, to_dataframe=False)

        if isinstance(data, dict):
            data = pd.DataFrame(data)

        if isinstance(data, pd.DataFrame) and not data.empty:
            logger.info(f"smoke ok: rows={len(data)}, cols={list(data.columns)}")
            print(data.head(5).to_string(index=False))
            return 0

        logger.error("smoke failed: empty result")
        return 2
    except Exception as e:
        logger.error(f"smoke failed: {type(e).__name__}: {e}")
        return 2


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch US stock klines from QuantDash")
    parser.add_argument("--stocklist", type=Path, default=DEFAULT_STOCKLIST, help="stock list csv path")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output directory")
    parser.add_argument("--period", default=DEFAULT_PERIOD, help="kline period, e.g. 1d/60m/5m/1m")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="lookback days for start_time")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="symbols per request batch")
    parser.add_argument("--qd-batch-size", type=int, default=DEFAULT_QD_BATCH_SIZE, help="QuantDash SDK internal batch size")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="QuantDash SDK max workers")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="number of daily bars to request")
    parser.add_argument("--skip-fresh-days", type=float, default=0.0,
                        help="legacy mtime-based skip window in days; for daily mode, date-based skip is preferred")
    parser.add_argument("--skip-if-up-to-date", dest="skip_if_up_to_date", action="store_true", default=True,
                        help="for daily period, skip symbols whose latest local trade date is already up-to-date")
    parser.add_argument("--no-skip-if-up-to-date", dest="skip_if_up_to_date", action="store_false",
                        help="disable date-based skip and rely on full fetch / skip-fresh-days")
    parser.add_argument("--benchmark-symbol", default=DEFAULT_BENCHMARK_SYMBOL,
                        help="benchmark symbol used to probe provider latest trade date (default: AAPL.US)")
    parser.add_argument("--daily-ready-hour-et", type=int, default=DEFAULT_DAILY_READY_HOUR_ET,
                        help="fallback ET hour to treat daily bar as updated when benchmark probing fails")
    parser.add_argument("--smoke", action="store_true", help="run one-symbol smoke test")
    parser.add_argument("--smoke-symbol", default="AAPL.US", help="symbol used in smoke mode")
    args = parser.parse_args()

    api_key = os.environ.get("QUANTDASH_API_KEY", "").strip()
    if not api_key:
        logger.error("missing QUANTDASH_API_KEY in environment")
        return 1

    qd = QuantDash(api_key=api_key, max_retries=QD_SDK_MAX_RETRIES)

    if args.smoke:
        return run_smoke_test(qd, symbol=args.smoke_symbol, period=args.period)

    args.out.mkdir(parents=True, exist_ok=True)

    tickers, profile_seed = load_stock_list(args.stocklist)
    if not tickers:
        logger.error("no valid tickers")
        return 1

    qd_symbol_to_ticker: Dict[str, str] = {}
    qd_symbols: List[str] = []
    for ticker in tickers:
        qsym = _normalize_market_symbol(ticker, default_market="US")
        if not qsym:
            continue
        if qsym in qd_symbol_to_ticker:
            continue
        qd_symbol_to_ticker[qsym] = ticker
        qd_symbols.append(qsym)

    if not qd_symbols:
        logger.error("no valid quantdash symbols")
        return 1

    period_lower = str(args.period).lower()
    if args.skip_if_up_to_date and period_lower.endswith("d"):
        try:
            target_trade_date = _resolve_target_trade_date(
                qd,
                period=args.period,
                benchmark_symbol=args.benchmark_symbol,
                daily_ready_hour_et=args.daily_ready_hour_et,
            )
        except FatalAuthError as e:
            logger.error(f"authentication failed: {e}")
            return 1

        old_total = len(qd_symbols)
        qd_symbols, skipped = _filter_up_to_date_symbols(
            qd_symbols,
            qd_symbol_to_ticker=qd_symbol_to_ticker,
            out_dir=args.out,
            target_trade_date=target_trade_date,
            minimum_rows=max(0, args.count),
        )
        if skipped > 0:
            logger.info(
                f"skip up-to-date files: {skipped}, remaining: {len(qd_symbols)}, target_trade_date={target_trade_date}"
            )
        if old_total > 0 and len(qd_symbols) == 0:
            logger.info("all symbols are already up-to-date; nothing to fetch")
            return 0

    if args.skip_fresh_days > 0 and args.skip_if_up_to_date and period_lower.endswith("d"):
        logger.info("skip-fresh-days ignored for daily mode because date-based up-to-date detection is enabled")

    if args.skip_fresh_days > 0 and not (args.skip_if_up_to_date and period_lower.endswith("d")):
        old_total = len(qd_symbols)
        qd_symbols = [
            s for s in qd_symbols
            if not _is_fresh(args.out / f"{qd_symbol_to_ticker[s]}.csv", args.skip_fresh_days)
        ]
        skipped = old_total - len(qd_symbols)
        if skipped > 0:
            logger.info(f"skip fresh files: {skipped}, remaining: {len(qd_symbols)}")

    if not qd_symbols:
        logger.info("all symbols are fresh; nothing to fetch")
        return 0

    try:
        preflight_auth(qd, symbol=qd_symbols[0], period=args.period)
    except FatalAuthError as e:
        logger.error(f"authentication failed: {e}")
        return 1
    except Exception as e:
        logger.warning(f"preflight warning (will continue): {type(e).__name__}: {e}")

    logger.info(f"start fetching {len(qd_symbols)} symbols, period={args.period}")

    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=max(args.days, 1))
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    total = len(qd_symbols)
    success = 0
    failed: List[str] = []
    batch_enabled = True

    with tqdm(
        total=total,
        desc="QuantDash 日线",
        unit="stk",
        dynamic_ncols=True,
        mininterval=0.5,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}",
    ) as progress:
        for i in range(0, total, max(1, args.batch_size)):
            batch = qd_symbols[i : i + max(1, args.batch_size)]
            batch_idx = i // max(1, args.batch_size) + 1
            total_batches = (total - 1) // max(1, args.batch_size) + 1

            logger.info(f"batch {batch_idx}/{total_batches}: {len(batch)} symbols")
            batch_result: Dict[str, pd.DataFrame] = {}
            if batch_enabled:
                try:
                    batch_result = _fetch_batch_with_retry(
                        qd,
                        batch,
                        period=args.period,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        count=args.count,
                        max_workers=args.max_workers,
                        qd_batch_size=args.qd_batch_size,
                    )
                except FatalAuthError as e:
                    logger.error(f"authentication failed: {e}")
                    return 1
                except BatchModeUnavailableError as e:
                    batch_enabled = False
                    logger.warning(f"batch mode unavailable, fallback to single-symbol mode: {e}")

            for qsym in batch:
                ticker = qd_symbol_to_ticker[qsym]
                df = batch_result.get(qsym)
                used_single_fallback = False
                if df is None:
                    used_single_fallback = True
                    try:
                        df = _fetch_one_with_retry(
                            qd,
                            qsym,
                            period=args.period,
                            start_ms=start_ms,
                            end_ms=end_ms,
                            count=args.count,
                        )
                    except Exception as e:
                        if _is_auth_error(e):
                            logger.error(f"authentication failed: {e}")
                            return 1
                        df = None

                if df is not None:
                    profile = profile_seed.get(ticker, {"sector": PROFILE_FALLBACK, "industry": PROFILE_FALLBACK})
                    if _save_one(ticker, df, args.out, profile, period=args.period):
                        success += 1
                    else:
                        failed.append(ticker)
                else:
                    failed.append(ticker)

                progress.update(1)
                progress.set_postfix(ok=success, fail=len(failed), refresh=False)

                # Pacing after single fallback calls to reduce 429 bursts.
                if used_single_fallback and SINGLE_REQUEST_PAUSE > 0:
                    time.sleep(SINGLE_REQUEST_PAUSE + random.uniform(0, 0.15))

            # gentle pause between batches
            if i + args.batch_size < total:
                time.sleep(random.uniform(1.5, 4.0))

    logger.info("==================================================")
    logger.info(f"done: success={success}, failed={len(failed)}, total={total}")
    logger.info(f"output: {args.out.resolve()}")

    if failed:
        uniq_failed = list(dict.fromkeys(failed))
        print("\n==================================================")
        print(f"failed symbols ({len(uniq_failed)}):")
        print(", ".join(uniq_failed))
        print("==================================================")

    return 0 if success > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
