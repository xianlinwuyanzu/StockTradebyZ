from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf


logger = logging.getLogger("jxt_v7")


FIB_PRICE_RATIOS = [0.382, 0.5, 0.618, 1.272, 1.618]
FIB_TIME_STEPS = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233]
TICKER_ALIAS = {
    "SPX": "SPY",
    "^GSPC": "SPY",
}


@dataclass
class PipelineResult:
    levels: Dict[str, float]
    summary_text: str
    snapshot: Dict[str, Any]
    output_paths: Dict[str, str]


def _norm_pdf(x: np.ndarray | float) -> np.ndarray | float:
    return np.exp(-0.5 * np.square(x)) / math.sqrt(2.0 * math.pi)


def _norm_cdf(x: np.ndarray | float) -> np.ndarray | float:
    return 0.5 * (1.0 + np.vectorize(math.erf)(np.array(x) / math.sqrt(2.0)))


def _safe_float(v: Any, default: float = float("nan")) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [c[0] if isinstance(c, tuple) else c for c in out.columns]

    rename_map = {}
    for col in out.columns:
        lc = str(col).lower()
        if lc == "datetime" or lc == "date":
            rename_map[col] = "date"
        elif lc == "open":
            rename_map[col] = "open"
        elif lc == "high":
            rename_map[col] = "high"
        elif lc == "low":
            rename_map[col] = "low"
        elif lc == "close":
            rename_map[col] = "close"
        elif lc == "volume":
            rename_map[col] = "volume"

    out = out.rename(columns=rename_map)
    if "date" not in out.columns:
        out = out.reset_index().rename(columns={"index": "date"})
    else:
        out = out.reset_index(drop=True)

    if "date" not in out.columns:
        out = out.reset_index().rename(columns={out.columns[0]: "date"})

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in out.columns:
            out[col] = np.nan

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["date", "open", "high", "low", "close"])\
             .sort_values("date")\
             .drop_duplicates(subset=["date"])\
             .reset_index(drop=True)

    return out[["date", "open", "high", "low", "close", "volume"]]


class JXTV7Pipeline:
    def __init__(
        self,
        ticker: str,
        cache_dir: Path,
        output_dir: Path,
        risk_free_rate: float = 0.04,
    ) -> None:
        input_ticker = ticker.upper().strip()
        self.ticker = TICKER_ALIAS.get(input_ticker, input_ticker)
        if self.ticker != input_ticker:
            logger.warning("ticker %s is mapped to %s for this strategy", input_ticker, self.ticker)
        self.cache_dir = cache_dir
        self.output_dir = output_dir
        self.risk_free_rate = risk_free_rate
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _synthetic_daily(self, bars: int = 760, base_price: float = 500.0) -> pd.DataFrame:
        rng = np.random.default_rng(42)
        end_dt = pd.Timestamp(datetime.now().date())
        dates = pd.bdate_range(end=end_dt, periods=bars)

        ret = rng.normal(0.0003, 0.0105, size=bars)
        close = base_price * np.cumprod(1.0 + ret)
        open_ = np.r_[close[0], close[:-1]] * (1.0 + rng.normal(0.0, 0.002, size=bars))
        high = np.maximum(open_, close) * (1.0 + np.abs(rng.normal(0.0025, 0.0015, size=bars)))
        low = np.minimum(open_, close) * (1.0 - np.abs(rng.normal(0.0025, 0.0015, size=bars)))
        volume = rng.integers(20_000_000, 130_000_000, size=bars)

        df = pd.DataFrame(
            {
                "date": dates,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
        return _normalize_ohlcv(df)

    def _hourly_from_daily(self, daily: pd.DataFrame) -> pd.DataFrame:
        rng = np.random.default_rng(7)
        rows: List[Dict[str, Any]] = []

        for _, row in daily.iterrows():
            d = pd.Timestamp(row["date"]).normalize()
            o = _safe_float(row["open"])
            h = _safe_float(row["high"])
            l = _safe_float(row["low"])
            c = _safe_float(row["close"])
            v = max(_safe_float(row["volume"], 0.0), 0.0)

            if not np.isfinite(o + h + l + c):
                continue

            path = np.linspace(o, c, 6)
            jitter = rng.normal(0.0, max(abs(c - o), c * 0.002) / 4.0, size=6)
            path = path + jitter
            path[0] = o
            path[-1] = c

            for i in range(6):
                ts = d + pd.Timedelta(hours=10 + i)
                seg_open = path[i - 1] if i > 0 else o
                seg_close = path[i]
                seg_high = min(max(seg_open, seg_close) * (1.0 + 0.002), h)
                seg_low = max(min(seg_open, seg_close) * (1.0 - 0.002), l)
                rows.append(
                    {
                        "date": ts,
                        "open": seg_open,
                        "high": seg_high,
                        "low": seg_low,
                        "close": seg_close,
                        "volume": v / 6.0,
                    }
                )

        return _normalize_ohlcv(pd.DataFrame(rows))

    def _synthetic_options(self, spot: float, as_of: str) -> pd.DataFrame:
        as_of_ts = pd.Timestamp(as_of)
        expiries = [as_of_ts + pd.Timedelta(days=d) for d in [7, 14, 28, 42]]
        strikes = np.arange(round(spot * 0.8), round(spot * 1.21), 2.0)

        rows: List[Dict[str, Any]] = []
        for exp in expiries:
            for k in strikes:
                dist = abs(k - spot) / max(spot, 1.0)
                base_oi = max(200, int(8000 * math.exp(-5.5 * dist)))
                iv = max(0.12, min(0.45, 0.18 + 0.25 * dist))

                call_price = max(spot - k, 0.0) + spot * iv * 0.02
                put_price = max(k - spot, 0.0) + spot * iv * 0.02

                rows.append(
                    {
                        "strike": float(k),
                        "openInterest": float(base_oi),
                        "impliedVolatility": float(iv),
                        "lastPrice": float(call_price),
                        "option_type": "call",
                        "expiry": exp,
                    }
                )
                rows.append(
                    {
                        "strike": float(k),
                        "openInterest": float(base_oi * 0.95),
                        "impliedVolatility": float(iv),
                        "lastPrice": float(put_price),
                        "option_type": "put",
                        "expiry": exp,
                    }
                )

        return pd.DataFrame(rows)

    def _retry(self, fn, retries: int = 3, base_wait: float = 1.5):
        last_err = None
        for i in range(1, retries + 1):
            try:
                return fn()
            except Exception as e:
                last_err = e
                logger.warning("attempt %d/%d failed: %s", i, retries, e)
                if i < retries:
                    wait = base_wait * i
                    logger.info("retrying after %.1fs", wait)
                    import time
                    time.sleep(wait)
        raise RuntimeError(f"all retries failed: {last_err}")

    def fetch_price_data(self) -> Dict[str, Any]:
        logger.info("fetching price data for %s", self.ticker)

        def _fetch_daily() -> pd.DataFrame:
            df = yf.download(
                tickers=self.ticker,
                period="3y",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            return _normalize_ohlcv(df)

        def _fetch_hourly() -> pd.DataFrame:
            df = yf.download(
                tickers=self.ticker,
                period="730d",
                interval="1h",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            return _normalize_ohlcv(df)

        daily = pd.DataFrame()
        hourly = pd.DataFrame()
        daily_source = "live"
        hourly_source = "live"

        try:
            daily = self._retry(_fetch_daily)
        except Exception as e:
            logger.warning("daily live fetch failed: %s", e)

        if daily.empty:
            daily_cache = self.cache_dir / f"{self.ticker}_daily.csv"
            if daily_cache.exists():
                logger.warning("using cached daily data: %s", daily_cache)
                daily = _normalize_ohlcv(pd.read_csv(daily_cache, parse_dates=["date"]))
                daily_source = "cache"

        if daily.empty:
            logger.warning("using synthetic daily fallback for robustness")
            daily = self._synthetic_daily()
            daily_source = "synthetic"

        try:
            hourly = self._retry(_fetch_hourly)
        except Exception as e:
            logger.warning("hourly live fetch failed: %s", e)

        if hourly.empty:
            hourly_cache = self.cache_dir / f"{self.ticker}_1h.csv"
            if hourly_cache.exists():
                logger.warning("using cached hourly data: %s", hourly_cache)
                hourly = _normalize_ohlcv(pd.read_csv(hourly_cache, parse_dates=["date"]))
                hourly_source = "cache"

        if hourly.empty:
            logger.warning("using derived hourly-from-daily fallback for robustness")
            hourly = self._hourly_from_daily(daily)
            hourly_source = "derived"

        daily_cache = self.cache_dir / f"{self.ticker}_daily.csv"
        hourly_cache = self.cache_dir / f"{self.ticker}_1h.csv"
        daily.to_csv(daily_cache, index=False)
        hourly.to_csv(hourly_cache, index=False)

        logger.info("price data source: daily=%s hourly=%s", daily_source, hourly_source)

        weekly = (
            daily.set_index("date")
            .resample("W-FRI")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna(subset=["open", "high", "low", "close"])
            .reset_index()
        )

        h4 = (
            hourly.set_index("date")
            .resample("4h")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna(subset=["open", "high", "low", "close"])
            .reset_index()
        )

        if len(h4) < 120:
            logger.warning("4H bars too short, deriving from daily fallback")
            h4 = (
                daily.set_index("date")
                .resample("4h")
                .agg(
                    {
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "volume": "sum",
                    }
                )
                .dropna(subset=["open", "high", "low", "close"])
                .reset_index()
            )

        return {
            "daily": daily,
            "weekly": weekly,
            "h4": h4,
            "source": {
                "daily": daily_source,
                "hourly": hourly_source,
            },
        }

    def fetch_options_chain(self, spot: Optional[float] = None) -> Tuple[pd.DataFrame, str, str]:
        logger.info("fetching options chain for %s", self.ticker)
        as_of = datetime.now().strftime("%Y-%m-%d")

        def _fetch() -> pd.DataFrame:
            t = yf.Ticker(self.ticker)
            expiries = list(t.options or [])
            if not expiries:
                raise RuntimeError("no option expiries returned")

            all_rows: List[pd.DataFrame] = []
            for exp in expiries[:8]:
                chain = t.option_chain(exp)
                calls = chain.calls.copy()
                puts = chain.puts.copy()

                for frame, side in [(calls, "call"), (puts, "put")]:
                    keep = [c for c in ["strike", "openInterest", "impliedVolatility", "lastPrice"] if c in frame.columns]
                    if not keep:
                        continue
                    frame = frame[keep].copy()
                    frame["option_type"] = side
                    frame["expiry"] = pd.to_datetime(exp)
                    all_rows.append(frame)

            if not all_rows:
                raise RuntimeError("option chain parsed empty")

            out = pd.concat(all_rows, ignore_index=True)
            for col in ["strike", "openInterest", "impliedVolatility", "lastPrice"]:
                if col not in out.columns:
                    out[col] = np.nan
            out["strike"] = pd.to_numeric(out["strike"], errors="coerce")
            out["openInterest"] = pd.to_numeric(out["openInterest"], errors="coerce").fillna(0.0)
            out["impliedVolatility"] = pd.to_numeric(out["impliedVolatility"], errors="coerce")
            out["lastPrice"] = pd.to_numeric(out["lastPrice"], errors="coerce")
            out = out.dropna(subset=["strike", "expiry"]).reset_index(drop=True)
            return out

        source = "live"
        try:
            options_df = self._retry(_fetch, retries=2, base_wait=2.0)
            cache_file = self.cache_dir / f"{self.ticker}_options_{as_of}.json"
            options_df.to_json(cache_file, orient="records", date_format="iso")
        except Exception as e:
            logger.warning("live options fetch failed, trying cache fallback: %s", e)
            cand = sorted(self.cache_dir.glob(f"{self.ticker}_options_*.json"))
            if cand:
                cache_file = cand[-1]
                options_df = pd.read_json(cache_file)
                if "expiry" in options_df.columns:
                    options_df["expiry"] = pd.to_datetime(options_df["expiry"], errors="coerce")
                source = "cache"
                as_of = cache_file.stem.split("_")[-1]
            else:
                if spot is None or not np.isfinite(spot):
                    spot = 500.0
                logger.warning("using synthetic options fallback for robustness")
                options_df = self._synthetic_options(float(spot), as_of)
                source = "synthetic"

        if options_df.empty:
            raise RuntimeError("options data empty after fallback")

        return options_df, as_of, source

    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out = out.sort_values("date").reset_index(drop=True)

        for span in [12, 47, 200]:
            out[f"ema{span}"] = out["close"].ewm(span=span, adjust=False).mean()

        prev_close = out["close"].shift(1)
        tr = pd.concat(
            [
                (out["high"] - out["low"]).abs(),
                (out["high"] - prev_close).abs(),
                (out["low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        out["atr14"] = tr.rolling(14).mean()

        mid = out["close"].rolling(20).mean()
        std = out["close"].rolling(20).std(ddof=0)
        upper = mid + 2.0 * std
        lower = mid - 2.0 * std
        out["bbw"] = (upper - lower) / mid.replace(0, np.nan)

        ema12 = out["close"].ewm(span=12, adjust=False).mean()
        ema26 = out["close"].ewm(span=26, adjust=False).mean()
        out["macd"] = ema12 - ema26
        out["macd_signal"] = out["macd"].ewm(span=9, adjust=False).mean()

        ll = out["low"].rolling(9).min()
        hh = out["high"].rolling(9).max()
        rsv = (out["close"] - ll) / (hh - ll).replace(0, np.nan) * 100.0
        out["kdj_k"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
        out["kdj_d"] = out["kdj_k"].ewm(alpha=1 / 3, adjust=False).mean()
        out["kdj_j"] = 3.0 * out["kdj_k"] - 2.0 * out["kdj_d"]

        out["vol_ma20"] = out["volume"].rolling(20).mean()
        out["vol_ratio"] = out["volume"] / out["vol_ma20"].replace(0, np.nan)
        return out

    def analyze_trend(self, tf_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        details: Dict[str, Any] = {}
        total_score = 0.0

        for tf in ["weekly", "daily", "h4"]:
            df = self.add_indicators(tf_data[tf])
            last = df.iloc[-1]
            e12, e47, e200 = [_safe_float(last[c]) for c in ["ema12", "ema47", "ema200"]]
            slope47 = _safe_float(df["ema47"].iloc[-1] - df["ema47"].iloc[-6]) if len(df) >= 6 else 0.0
            close = _safe_float(last["close"])

            score = 0.0
            if e12 > e47 > e200:
                score += 1.0
                align = "bull"
            elif e12 < e47 < e200:
                score -= 1.0
                align = "bear"
            else:
                align = "mixed"

            score += 0.5 if slope47 > 0 else -0.5
            score += 0.5 if close > e47 else -0.5
            total_score += score

            details[tf] = {
                "close": close,
                "ema12": e12,
                "ema47": e47,
                "ema200": e200,
                "ema47_slope": slope47,
                "alignment": align,
                "score": score,
            }

        if total_score >= 2.0:
            conclusion = "multi-timeframe bullish resonance"
        elif total_score <= -2.0:
            conclusion = "multi-timeframe bearish resonance"
        else:
            conclusion = "multi-timeframe mixed resonance"

        return {
            "score": total_score,
            "details": details,
            "conclusion": conclusion,
        }

    @staticmethod
    def analyze_structure(daily_df: pd.DataFrame) -> Dict[str, Any]:
        df = daily_df.copy().sort_values("date").reset_index(drop=True)
        if len(df) < 30:
            return {"score": 0.0, "signals": ["insufficient bars for structure analysis"]}

        last = df.iloc[-1]
        prev = df.iloc[-2]
        prior20 = df.iloc[-21:-1]

        signals: List[str] = []
        score = 0.0

        prior_high = prior20["high"].max()
        prior_low = prior20["low"].min()
        close = _safe_float(last["close"])
        high = _safe_float(last["high"])
        low = _safe_float(last["low"])
        open_ = _safe_float(last["open"])

        if close > prior_high:
            signals.append("true breakout above 20-bar high")
            score += 1.0
        elif high > prior_high and close < prior_high:
            signals.append("failed breakout above 20-bar high")
            score -= 0.6

        if close < prior_low:
            signals.append("true breakdown below 20-bar low")
            score -= 1.0
        elif low < prior_low and close > prior_low:
            signals.append("failed breakdown below 20-bar low")
            score += 0.6

        body = max(abs(close - open_), 1e-8)
        upper_wick = high - max(open_, close)
        lower_wick = min(open_, close) - low

        if upper_wick / body >= 1.6:
            signals.append("long upper wick")
            score -= 0.4
        if lower_wick / body >= 1.6:
            signals.append("long lower wick")
            score += 0.4

        if high < _safe_float(prev["high"]) and low > _safe_float(prev["low"]):
            signals.append("inside bar")
        if high > _safe_float(prev["high"]) and low < _safe_float(prev["low"]):
            signals.append("outside bar")

        prev_open = _safe_float(prev["open"])
        prev_close = _safe_float(prev["close"])
        if close > open_ and prev_close < prev_open and close >= prev_open and open_ <= prev_close:
            signals.append("bullish engulfing")
            score += 0.6
        if close < open_ and prev_close > prev_open and close <= prev_open and open_ >= prev_close:
            signals.append("bearish engulfing")
            score -= 0.6

        return {"score": score, "signals": signals}

    @staticmethod
    def _fib_levels_from_swing(df: pd.DataFrame, lookback: int) -> List[Tuple[str, float]]:
        if len(df) < lookback:
            return []

        seg = df.iloc[-lookback:].copy()
        hi_idx = seg["high"].idxmax()
        lo_idx = seg["low"].idxmin()
        hi = _safe_float(seg.loc[hi_idx, "high"])
        lo = _safe_float(seg.loc[lo_idx, "low"])
        rng = abs(hi - lo)
        if rng <= 1e-8:
            return []

        levels: List[Tuple[str, float]] = []
        up_swing = lo_idx < hi_idx

        if up_swing:
            for r in [0.382, 0.5, 0.618]:
                levels.append((f"retr_{lookback}_{r}", hi - r * rng))
            for e in [1.272, 1.618]:
                levels.append((f"ext_{lookback}_{e}", hi + (e - 1.0) * rng))
        else:
            for r in [0.382, 0.5, 0.618]:
                levels.append((f"retr_{lookback}_{r}", lo + r * rng))
            for e in [1.272, 1.618]:
                levels.append((f"ext_{lookback}_{e}", lo - (e - 1.0) * rng))

        return levels

    def analyze_fib_cluster(self, daily: pd.DataFrame, h4: pd.DataFrame, spot: float) -> Dict[str, Any]:
        levels: List[Tuple[str, float]] = []
        for lb in [233, 89]:
            levels.extend(self._fib_levels_from_swing(daily, lb))
        for lb in [144, 34]:
            levels.extend(self._fib_levels_from_swing(h4, lb))

        clean_levels = [(name, p) for name, p in levels if np.isfinite(p) and p > 0]
        if not clean_levels:
            return {
                "score": 0.0,
                "levels": [],
                "clusters": [],
                "supports": [],
                "resistances": [],
            }

        tol = max(spot * 0.004, 0.5)
        buckets: Dict[int, List[Tuple[str, float]]] = {}
        for name, p in clean_levels:
            key = int(round(p / tol))
            buckets.setdefault(key, []).append((name, p))

        clusters: List[Dict[str, Any]] = []
        for vals in buckets.values():
            prices = [x[1] for x in vals]
            center = float(np.mean(prices))
            clusters.append(
                {
                    "center": center,
                    "count": len(vals),
                    "members": [x[0] for x in vals],
                    "distance_pct": abs(center - spot) / spot * 100.0,
                }
            )

        clusters = sorted(clusters, key=lambda x: (-x["count"], x["distance_pct"]))

        supports = sorted([c["center"] for c in clusters if c["center"] < spot])
        resistances = sorted([c["center"] for c in clusters if c["center"] > spot])

        score = 0.0
        if clusters:
            if clusters[0]["count"] >= 3:
                score += 1.0
            if clusters[0]["distance_pct"] <= 1.5:
                score += 0.5

        key_levels = [round(c["center"], 2) for c in clusters[:6]]

        return {
            "score": score,
            "levels": key_levels,
            "clusters": clusters,
            "supports": supports,
            "resistances": resistances,
        }

    def analyze_options_gex(self, options_df: pd.DataFrame, spot: float, as_of: str) -> Dict[str, Any]:
        df = options_df.copy()
        if df.empty:
            return {
                "score": 0.0,
                "total_gex": 0.0,
                "total_dex": 0.0,
                "call_wall": float("nan"),
                "put_wall": float("nan"),
                "zero_gamma": float("nan"),
                "gamma_flip": float("nan"),
                "expected_move": float("nan"),
                "expected_move_pct": float("nan"),
                "by_strike": [],
                "as_of": as_of,
            }

        today = pd.Timestamp(as_of)
        df["T"] = (pd.to_datetime(df["expiry"]) - today).dt.days / 365.0
        df["T"] = df["T"].clip(lower=1.0 / 365.0)

        sigma = df["impliedVolatility"].replace([np.inf, -np.inf], np.nan).fillna(0.20)
        sigma = sigma.clip(lower=0.05, upper=3.0)

        K = df["strike"].astype(float).clip(lower=1e-8)
        S = float(spot)
        T = df["T"].astype(float)
        r = self.risk_free_rate

        sqrtT = np.sqrt(T)
        d1 = (np.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
        gamma = _norm_pdf(d1) / (S * sigma * sqrtT)

        call_delta = _norm_cdf(d1)
        put_delta = call_delta - 1.0

        is_call = (df["option_type"].astype(str).str.lower() == "call").to_numpy()
        oi = df["openInterest"].fillna(0.0).to_numpy(dtype=float)
        gamma_val = np.array(gamma, dtype=float)

        side = np.where(is_call, 1.0, -1.0)
        gex = gamma_val * oi * 100.0 * (S * S) * 0.01 * side

        delta = np.where(is_call, np.array(call_delta, dtype=float), np.array(put_delta, dtype=float))
        dex = delta * oi * 100.0 * S

        df["gex"] = gex
        df["dex"] = dex

        by_strike = (
            df.groupby("strike", as_index=False)[["gex", "dex", "openInterest"]]
            .sum()
            .sort_values("strike")
            .reset_index(drop=True)
        )

        call_rows = df[df["option_type"] == "call"]
        put_rows = df[df["option_type"] == "put"]

        call_wall = float("nan")
        put_wall = float("nan")
        if not call_rows.empty:
            call_wall = _safe_float(call_rows.loc[call_rows["openInterest"].idxmax(), "strike"])
        if not put_rows.empty:
            put_wall = _safe_float(put_rows.loc[put_rows["openInterest"].idxmax(), "strike"])

        gamma_flip = float("nan")
        strikes = by_strike["strike"].to_numpy(dtype=float)
        net_g = by_strike["gex"].to_numpy(dtype=float)
        for i in range(1, len(strikes)):
            g0, g1 = net_g[i - 1], net_g[i]
            if g0 == 0:
                gamma_flip = strikes[i - 1]
                break
            if g0 * g1 < 0:
                w = abs(g0) / (abs(g0) + abs(g1))
                gamma_flip = strikes[i - 1] + (strikes[i] - strikes[i - 1]) * w
                break
        if not np.isfinite(gamma_flip) and len(strikes) > 0:
            gamma_flip = float(strikes[np.argmin(np.abs(net_g))])

        expected_move = float("nan")
        expected_move_pct = float("nan")
        if not df.empty:
            near_exp = pd.to_datetime(df["expiry"]).min()
            near_df = df[pd.to_datetime(df["expiry"]) == near_exp]
            if not near_df.empty:
                idx_atm = (near_df["strike"] - S).abs().idxmin()
                atm_strike = _safe_float(near_df.loc[idx_atm, "strike"])
                call_atm = near_df[(near_df["option_type"] == "call") & (near_df["strike"] == atm_strike)]
                put_atm = near_df[(near_df["option_type"] == "put") & (near_df["strike"] == atm_strike)]
                if not call_atm.empty and not put_atm.empty:
                    c = _safe_float(call_atm["lastPrice"].iloc[0], 0.0)
                    p = _safe_float(put_atm["lastPrice"].iloc[0], 0.0)
                    expected_move = c + p
                    if S > 0:
                        expected_move_pct = expected_move / S * 100.0

        total_gex = float(np.nansum(gex))
        total_dex = float(np.nansum(dex))

        score = 0.0
        if np.isfinite(total_gex):
            if total_gex > 0:
                score += 0.6
            else:
                score -= 0.6
        if np.isfinite(gamma_flip):
            if abs(gamma_flip - S) / S <= 0.01:
                score += 0.4

        by_strike_records = by_strike.round(4).to_dict(orient="records")

        return {
            "score": score,
            "total_gex": total_gex,
            "total_dex": total_dex,
            "call_wall": call_wall,
            "put_wall": put_wall,
            "zero_gamma": gamma_flip,
            "gamma_flip": gamma_flip,
            "expected_move": expected_move,
            "expected_move_pct": expected_move_pct,
            "by_strike": by_strike_records,
            "as_of": as_of,
        }

    @staticmethod
    def analyze_volume_profile(daily_df: pd.DataFrame) -> Dict[str, Any]:
        df = daily_df.copy().sort_values("date").tail(90).reset_index(drop=True)
        if len(df) < 20:
            return {
                "poc": float("nan"),
                "hvn": [],
                "lvn": [],
                "profile": [],
            }

        lo = _safe_float(df["low"].min())
        hi = _safe_float(df["high"].max())
        if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
            return {
                "poc": float("nan"),
                "hvn": [],
                "lvn": [],
                "profile": [],
            }

        bins = np.linspace(lo, hi, 41)
        centers = (bins[:-1] + bins[1:]) / 2.0
        vols = np.zeros(len(centers), dtype=float)

        typical = (df["high"] + df["low"] + df["close"]) / 3.0
        idx = np.digitize(typical, bins) - 1
        idx = np.clip(idx, 0, len(centers) - 1)

        for i, v in zip(idx, df["volume"].fillna(0.0).to_numpy(dtype=float)):
            vols[int(i)] += v

        poc_idx = int(np.argmax(vols))
        poc = float(centers[poc_idx])

        hvn_idx = np.argsort(vols)[-3:][::-1]
        hvn = sorted([float(centers[i]) for i in hvn_idx])

        nz = np.where(vols > 0)[0]
        lvn = []
        if len(nz) > 0:
            low3 = nz[np.argsort(vols[nz])[:3]]
            lvn = sorted([float(centers[i]) for i in low3])

        profile = [{"price": float(p), "volume": float(v)} for p, v in zip(centers, vols)]
        return {"poc": poc, "hvn": hvn, "lvn": lvn, "profile": profile}

    @staticmethod
    def analyze_volatility(ind_daily: pd.DataFrame) -> Dict[str, Any]:
        last = ind_daily.iloc[-1]
        atr = _safe_float(last.get("atr14"))
        bbw = _safe_float(last.get("bbw"))
        vol_ratio = _safe_float(last.get("vol_ratio"))
        macd = _safe_float(last.get("macd"))
        macd_signal = _safe_float(last.get("macd_signal"))
        kdj_j = _safe_float(last.get("kdj_j"))

        score = 0.0
        tags: List[str] = []

        if np.isfinite(bbw):
            if bbw < 0.06:
                tags.append("volatility compression")
            elif bbw > 0.12:
                tags.append("volatility expansion")
                score += 0.3

        if np.isfinite(vol_ratio):
            if vol_ratio >= 1.4:
                tags.append("volume expansion")
                score += 0.3
            elif vol_ratio <= 0.8:
                tags.append("volume contraction")

        if np.isfinite(macd) and np.isfinite(macd_signal):
            if macd > macd_signal:
                tags.append("MACD bullish")
                score += 0.2
            else:
                tags.append("MACD bearish")
                score -= 0.2

        if np.isfinite(kdj_j):
            if kdj_j > 90:
                tags.append("KDJ overbought")
                score -= 0.2
            elif kdj_j < 10:
                tags.append("KDJ oversold")
                score += 0.2

        return {
            "score": score,
            "atr": atr,
            "bbw": bbw,
            "vol_ratio": vol_ratio,
            "macd": macd,
            "macd_signal": macd_signal,
            "kdj_j": kdj_j,
            "tags": tags,
        }

    @staticmethod
    def analyze_time_cluster(tf_data: Dict[str, pd.DataFrame], today: date) -> Dict[str, Any]:
        candidates: Dict[date, List[str]] = {}

        def _add(tf_name: str, df: pd.DataFrame, lookback: int) -> None:
            if len(df) < max(lookback, 40):
                return

            seg = df.iloc[-lookback:].copy().reset_index(drop=True)
            hi_pos = int(seg["high"].idxmax())
            lo_pos = int(seg["low"].idxmin())
            anchors = [("high", hi_pos), ("low", lo_pos)]

            for kind, p in anchors:
                for n in FIB_TIME_STEPS:
                    t = p + n
                    if t < len(seg):
                        d = pd.Timestamp(seg.loc[t, "date"]).date()
                        if today <= d <= today + timedelta(days=14):
                            candidates.setdefault(d, []).append(f"{tf_name}_{kind}_fib{n}")

        _add("weekly", tf_data["weekly"], 120)
        _add("daily", tf_data["daily"], 180)
        _add("h4", tf_data["h4"], 220)

        ranked = sorted(candidates.items(), key=lambda x: (-len(x[1]), x[0]))
        windows = [{"date": str(d), "overlap": len(tags), "tags": tags} for d, tags in ranked[:5]]

        score = 0.0
        if windows:
            if windows[0]["overlap"] >= 3:
                score += 0.8
            elif windows[0]["overlap"] == 2:
                score += 0.4

        return {
            "score": score,
            "windows": windows,
        }

    @staticmethod
    def _pick_nearest(values: List[float], x: float) -> Optional[float]:
        clean = [v for v in values if np.isfinite(v)]
        if not clean:
            return None
        return min(clean, key=lambda v: abs(v - x))

    def synthesize_levels(
        self,
        spot: float,
        atr: float,
        ind_daily: pd.DataFrame,
        fib: Dict[str, Any],
        gex: Dict[str, Any],
        vp: Dict[str, Any],
    ) -> Dict[str, float]:
        last = ind_daily.iloc[-1]
        ema47 = _safe_float(last.get("ema47"))
        ema200 = _safe_float(last.get("ema200"))

        mid_candidates: List[float] = []
        for v in [ema47, vp.get("poc"), gex.get("zero_gamma")]:
            if np.isfinite(_safe_float(v)):
                mid_candidates.append(float(v))

        near_fib = self._pick_nearest([float(x) for x in fib.get("levels", []) if np.isfinite(x)], spot)
        if near_fib is not None:
            mid_candidates.append(near_fib)

        if not mid_candidates:
            axis = spot
        else:
            axis = float(np.median(mid_candidates))

        supports: List[float] = []
        resistances: List[float] = []

        for v in fib.get("supports", []):
            if np.isfinite(v):
                supports.append(float(v))
        for v in fib.get("resistances", []):
            if np.isfinite(v):
                resistances.append(float(v))

        put_wall = _safe_float(gex.get("put_wall"))
        call_wall = _safe_float(gex.get("call_wall"))
        if np.isfinite(put_wall):
            supports.append(put_wall)
        if np.isfinite(call_wall):
            resistances.append(call_wall)

        for v in vp.get("hvn", []):
            v = _safe_float(v)
            if np.isfinite(v):
                if v <= axis:
                    supports.append(v)
                else:
                    resistances.append(v)

        if np.isfinite(ema200):
            if ema200 <= axis:
                supports.append(ema200)
            else:
                resistances.append(ema200)

        if not np.isfinite(atr) or atr <= 0:
            atr = max(spot * 0.01, 1.0)

        supports = sorted(set(round(x, 2) for x in supports if x < axis))
        resistances = sorted(set(round(x, 2) for x in resistances if x > axis))

        if len(supports) < 2:
            supports.extend([round(axis - 0.8 * atr, 2), round(axis - 1.6 * atr, 2)])
            supports = sorted(set(supports))

        if len(resistances) < 2:
            resistances.extend([round(axis + 0.8 * atr, 2), round(axis + 1.6 * atr, 2)])
            resistances = sorted(set(resistances))

        secondary_support = max([x for x in supports if x < axis], default=round(axis - 0.8 * atr, 2))
        extreme_support = min(supports) if supports else round(axis - 1.6 * atr, 2)

        secondary_resistance = min([x for x in resistances if x > axis], default=round(axis + 0.8 * atr, 2))
        extreme_resistance = max(resistances) if resistances else round(axis + 1.6 * atr, 2)

        return {
            "extreme_resistance": round(extreme_resistance, 2),
            "secondary_resistance": round(secondary_resistance, 2),
            "axis": round(axis, 2),
            "secondary_support": round(secondary_support, 2),
            "extreme_support": round(extreme_support, 2),
        }

    @staticmethod
    def _resonance_label(score: float) -> str:
        if score >= 2.0:
            return "strong resonance"
        if score >= 0.8:
            return "moderate resonance"
        if score <= -1.5:
            return "risk-off resonance"
        return "mixed resonance"

    def build_summary_text(
        self,
        spot: float,
        levels: Dict[str, float],
        trend: Dict[str, Any],
        structure: Dict[str, Any],
        fib: Dict[str, Any],
        gex: Dict[str, Any],
        vp: Dict[str, Any],
        vol: Dict[str, Any],
        tcluster: Dict[str, Any],
        total_score: float,
        price_source: Dict[str, str],
        options_source: str,
    ) -> str:
        def _fmt(v: Any, nd: int = 2) -> str:
            x = _safe_float(v)
            if np.isfinite(x):
                return f"{x:.{nd}f}"
            return "n/a"

        resonance = self._resonance_label(total_score)
        resonance_cn = {
            "strong resonance": "强共振",
            "moderate resonance": "中等共振",
            "risk-off resonance": "风险偏好下降",
            "mixed resonance": "混合共振",
        }.get(resonance, resonance)

        trend_raw = str(trend.get("conclusion", ""))
        trend_cn = {
            "multi-timeframe bullish resonance": "多周期偏多共振",
            "multi-timeframe bearish resonance": "多周期偏空共振",
            "multi-timeframe mixed resonance": "多周期方向分歧",
        }.get(trend_raw, trend_raw)

        signal_text = ", ".join(structure.get("signals", [])[:6]) if structure.get("signals") else ""
        signal_text = (
            signal_text
            .replace("true breakout above 20-bar high", "20日新高有效突破")
            .replace("failed breakout above 20-bar high", "20日新高假突破")
            .replace("true breakdown below 20-bar low", "20日新低有效跌破")
            .replace("failed breakdown below 20-bar low", "20日新低假跌破")
            .replace("long upper wick", "长上影")
            .replace("long lower wick", "长下影")
            .replace("inside bar", "Inside Bar")
            .replace("outside bar", "Outside Bar")
            .replace("bullish engulfing", "看涨吞没")
            .replace("bearish engulfing", "看跌吞没")
        )

        windows = tcluster.get("windows", [])
        if windows:
            time_text = ", ".join([f"{w['date']} (重叠 {w['overlap']})" for w in windows[:3]])
        else:
            time_text = "未来两周暂无高置信时间共振窗口"

        fib_text = ", ".join([str(x) for x in fib.get("levels", [])[:6]]) if fib.get("levels") else "n/a"

        gex_text = (
            f"Call墙 {_fmt(gex.get('call_wall'))}, Put墙 {_fmt(gex.get('put_wall'))}, "
            f"零伽马位 {_fmt(gex.get('zero_gamma'))}, 总GEX {_safe_float(gex.get('total_gex')):.2e}"
            if np.isfinite(_safe_float(gex.get("call_wall"))) and np.isfinite(_safe_float(gex.get("put_wall")))
            else "GEX暂不可用"
        )

        expected_move_text = "n/a"
        em = _safe_float(gex.get("expected_move"))
        em_pct = _safe_float(gex.get("expected_move_pct"))
        if np.isfinite(em) and np.isfinite(em_pct):
            expected_move_text = f"约 ±{em:.2f} ({em_pct:.2f}%)"

        vol_tags = ", ".join(vol.get("tags", [])) if vol.get("tags") else ""
        vol_tags = (
            vol_tags
            .replace("volatility compression", "波动率收缩")
            .replace("volatility expansion", "波动率扩张")
            .replace("volume expansion", "成交量放大")
            .replace("volume contraction", "成交量收缩")
            .replace("MACD bullish", "MACD偏多")
            .replace("MACD bearish", "MACD偏空")
            .replace("KDJ overbought", "KDJ超买")
            .replace("KDJ oversold", "KDJ超卖")
        )

        source_cn = {
            "live": "实时行情",
            "cache": "缓存回退",
            "synthetic": "合成回退",
            "derived": "日线派生",
        }.get(options_source, options_source)

        price_daily_raw = str(price_source.get("daily", "unknown"))
        price_hourly_raw = str(price_source.get("hourly", "unknown"))
        price_daily_cn = {
            "live": "实时行情",
            "cache": "缓存回退",
            "synthetic": "合成回退",
        }.get(price_daily_raw, price_daily_raw)
        price_hourly_cn = {
            "live": "实时行情",
            "cache": "缓存回退",
            "derived": "日线派生",
        }.get(price_hourly_raw, price_hourly_raw)

        weight = {
            "live": 1.0,
            "cache": 0.72,
            "derived": 0.65,
            "synthetic": 0.45,
        }
        reliability = (
            0.45 * weight.get(price_daily_raw, 0.5)
            + 0.25 * weight.get(price_hourly_raw, 0.5)
            + 0.30 * weight.get(options_source, 0.5)
        )
        if reliability >= 0.85:
            reliability_text = "高"
        elif reliability >= 0.68:
            reliability_text = "中"
        else:
            reliability_text = "低"

        institution_zone = []
        if np.isfinite(_safe_float(gex.get("put_wall"))) and np.isfinite(_safe_float(gex.get("call_wall"))):
            lo = min(_safe_float(gex["put_wall"]), _safe_float(gex["call_wall"]))
            hi = max(_safe_float(gex["put_wall"]), _safe_float(gex["call_wall"]))
            institution_zone.append(f"{lo:.2f} - {hi:.2f}")
        if np.isfinite(_safe_float(vp.get("poc"))):
            institution_zone.append(f"POC {vp['poc']:.2f}")
        inst_text = ", ".join(institution_zone) if institution_zone else "n/a"

        hvn_fmt = [f"{_safe_float(x):.2f}" for x in vp.get("hvn", []) if np.isfinite(_safe_float(x))]
        lvn_fmt = [f"{_safe_float(x):.2f}" for x in vp.get("lvn", []) if np.isfinite(_safe_float(x))]
        hvn_text = ", ".join(hvn_fmt) if hvn_fmt else "n/a"
        lvn_text = ", ".join(lvn_fmt) if lvn_fmt else "n/a"
        poc_text = _fmt(vp.get("poc"))

        break_up = max(_safe_float(levels["secondary_resistance"]), _safe_float(levels["axis"]))
        break_dn = min(_safe_float(levels["secondary_support"]), _safe_float(levels["axis"]))
        fail_long = _safe_float(levels["axis"])
        fail_short = _safe_float(levels["axis"])

        lines = [
            f"# JXT v7.0 盘前报告 - {self.ticker}",
            "",
            f"- 现价参考: {spot:.2f}",
            f"- 多因子共振等级: {resonance_cn} (综合分 {total_score:.2f})",
            f"- 多周期结论: {trend_cn}",
            f"- 数据可靠性: {reliability_text} (日线/小时线/期权: {price_daily_cn}/{price_hourly_cn}/{source_cn})",
            "",
            "## 五个关键点位",
            f"- 极致压力位: {levels['extreme_resistance']}",
            f"- 次级压力位: {levels['secondary_resistance']}",
            f"- 多空中轴: {levels['axis']}",
            f"- 次级支撑位: {levels['secondary_support']}",
            f"- 极致支撑位: {levels['extreme_support']}",
            "",
            "## 文字结论",
            f"1. 趋势层: 周线/日线/4小时合并结果为 {trend_cn}。若价格持续站在中轴与次级支撑之上，优先按顺势处理；若跌破中轴且回收失败，优先切换为防守节奏。",
            f"2. 结构层: {signal_text if signal_text else '当前未出现明显结构切换信号'}。结构信号用于确认突破真假，不单独作为方向依据。",
            f"3. 价格层(Fib): 关键Fib位 {fib_text}。多个Fib重叠区域优先视为价格共振区，盘中先看是否出现放量确认，再决定追随或反向。",
            f"4. 机构仓位层(GEX): {gex_text}；近月隐含预期波动 {expected_move_text}。当价格接近零伽马位/伽马翻转位，市场波动机制更容易切换，仓位应主动收敛，避免过度押注。",
            f"5. 成交密集区层: POC {poc_text} | HVN {hvn_text} | LVN {lvn_text}。POC与中轴重叠时代表平衡区，若带量脱离，通常意味着日内方向性增强。",
            f"6. 波动率层: {vol_tags if vol_tags else '暂无显著标签'}。若处于收缩后放量扩张阶段，优先关注关键位触发后的单边延续。",
            f"7. 时间层(本周窗口): {time_text}。时间共振日不等于必然反转日，但通常是波动放大的高优先观察窗口。",
            "",
            "## 盘中执行框架",
            f"- 顺势做多触发: 价格有效上破 {break_up:.2f} 且5分钟收盘站稳，优先看向 {levels['extreme_resistance']:.2f}。",
            f"- 顺势做空触发: 价格有效跌破 {break_dn:.2f} 且5分钟收盘站稳下方，优先看向 {levels['extreme_support']:.2f}。",
            f"- 多单失效线: 跌回并持续位于 {fail_long:.2f} 下方，减少追多仓位。",
            f"- 空单失效线: 站回并持续位于 {fail_short:.2f} 上方，减少追空仓位。",
            "- 仓位建议: 若数据可靠性为“低”，单次仓位不超过常规仓位的50%。",
            "",
            "## 机构重点关注区域",
            f"- {inst_text}",
            "",
            "## 执行备注",
            "- 本版本采用盘前单次刷新，不做盘中自动再平衡。",
            "- 当期权接口不可用时，自动回退到缓存或合成数据，保证流程不中断。",
            f"- 本次价格数据来源: 日线 {price_daily_cn} / 小时线 {price_hourly_cn}。",
            f"- 本次期权数据来源: {source_cn}。",
            "- 模型核心是定位关键区域，不是给出确定性预测。",
        ]

        return "\n".join(lines)

    def run(self) -> PipelineResult:
        tf_data = self.fetch_price_data()
        daily = tf_data["daily"]
        ind_daily = self.add_indicators(daily)

        spot = _safe_float(ind_daily.iloc[-1]["close"])
        if not np.isfinite(spot) or spot <= 0:
            raise RuntimeError("invalid latest close for spot")

        trend = self.analyze_trend(tf_data)
        structure = self.analyze_structure(daily)
        fib = self.analyze_fib_cluster(tf_data["daily"], tf_data["h4"], spot)

        options_df, opt_as_of, opt_source = self.fetch_options_chain(spot=spot)
        gex = self.analyze_options_gex(options_df, spot, opt_as_of)

        vp = self.analyze_volume_profile(daily)
        vol = self.analyze_volatility(ind_daily)
        tcluster = self.analyze_time_cluster(tf_data, today=date.today())

        total_score = (
            trend.get("score", 0.0)
            + structure.get("score", 0.0)
            + fib.get("score", 0.0)
            + gex.get("score", 0.0)
            + vol.get("score", 0.0)
            + tcluster.get("score", 0.0)
        )

        levels = self.synthesize_levels(
            spot=spot,
            atr=_safe_float(vol.get("atr"), spot * 0.01),
            ind_daily=ind_daily,
            fib=fib,
            gex=gex,
            vp=vp,
        )

        summary_text = self.build_summary_text(
            spot=spot,
            levels=levels,
            trend=trend,
            structure=structure,
            fib=fib,
            gex=gex,
            vp=vp,
            vol=vol,
            tcluster=tcluster,
            total_score=total_score,
            price_source=tf_data.get("source", {}),
            options_source=opt_source,
        )

        source_weight = {
            "live": 1.0,
            "cache": 0.72,
            "derived": 0.65,
            "synthetic": 0.45,
        }
        price_src = tf_data.get("source", {})
        reliability_score = (
            0.45 * source_weight.get(str(price_src.get("daily", "unknown")), 0.5)
            + 0.25 * source_weight.get(str(price_src.get("hourly", "unknown")), 0.5)
            + 0.30 * source_weight.get(str(opt_source), 0.5)
        )

        run_date = datetime.now().strftime("%Y%m%d")
        out_dir = self.output_dir / run_date
        out_dir.mkdir(parents=True, exist_ok=True)

        levels_path = out_dir / f"{self.ticker}_levels.csv"
        report_path = out_dir / f"{self.ticker}_report.md"
        snapshot_path = out_dir / f"{self.ticker}_snapshot.json"

        levels_row = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "ticker": self.ticker,
            "spot": round(spot, 4),
            **levels,
            "resonance_score": round(float(total_score), 4),
            "trend_conclusion": trend.get("conclusion"),
            "time_cluster_top": tcluster.get("windows", [{}])[0].get("date") if tcluster.get("windows") else None,
        }
        pd.DataFrame([levels_row]).to_csv(levels_path, index=False)

        snapshot = {
            "ticker": self.ticker,
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "spot": spot,
            "levels": levels,
            "resonance_score": total_score,
            "trend": trend,
            "structure": structure,
            "fib": {
                "score": fib.get("score"),
                "levels": fib.get("levels"),
                "clusters": fib.get("clusters", [])[:10],
            },
            "gex": {
                "score": gex.get("score"),
                "total_gex": gex.get("total_gex"),
                "total_dex": gex.get("total_dex"),
                "call_wall": gex.get("call_wall"),
                "put_wall": gex.get("put_wall"),
                "zero_gamma": gex.get("zero_gamma"),
                "expected_move": gex.get("expected_move"),
                "expected_move_pct": gex.get("expected_move_pct"),
                "as_of": gex.get("as_of"),
                "source": opt_source,
            },
            "data_source": {
                "price_daily": price_src.get("daily"),
                "price_hourly": price_src.get("hourly"),
                "options": opt_source,
                "reliability_score": round(float(reliability_score), 4),
            },
            "volume_profile": {
                "poc": vp.get("poc"),
                "hvn": vp.get("hvn"),
                "lvn": vp.get("lvn"),
            },
            "volatility": vol,
            "time_cluster": tcluster,
        }

        with snapshot_path.open("w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)

        report_path.write_text(summary_text, encoding="utf-8")

        return PipelineResult(
            levels=levels,
            summary_text=summary_text,
            snapshot=snapshot,
            output_paths={
                "levels_csv": str(levels_path),
                "report_md": str(report_path),
                "snapshot_json": str(snapshot_path),
            },
        )

    def run_step_tests(self) -> List[Tuple[str, bool, str]]:
        results: List[Tuple[str, bool, str]] = []

        try:
            tf_data = self.fetch_price_data()
            ok = (
                len(tf_data["daily"]) >= 250
                and len(tf_data["weekly"]) >= 80
                and len(tf_data["h4"]) >= 120
            )
            msg = (
                f"daily={len(tf_data['daily'])}, weekly={len(tf_data['weekly'])}, h4={len(tf_data['h4'])}"
            )
            results.append(("step1_fetch_prices", ok, msg))
        except Exception as e:
            results.append(("step1_fetch_prices", False, str(e)))
            return results

        try:
            ind_daily = self.add_indicators(tf_data["daily"])
            required = ["ema12", "ema47", "ema200", "atr14", "bbw", "macd", "kdj_j"]
            ok = all(col in ind_daily.columns for col in required)
            ok = ok and np.isfinite(_safe_float(ind_daily.iloc[-1]["ema47"]))
            results.append(("step2_indicators", ok, "indicator columns and last ema47 validated"))
        except Exception as e:
            results.append(("step2_indicators", False, str(e)))

        try:
            options_df, as_of, source = self.fetch_options_chain(spot=_safe_float(ind_daily.iloc[-1]["close"]))
            gex = self.analyze_options_gex(options_df, _safe_float(ind_daily.iloc[-1]["close"]), as_of)
            ok = len(options_df) > 0 and "total_gex" in gex
            msg = f"options_rows={len(options_df)}, source={source}"
            results.append(("step3_options_gex", ok, msg))
        except Exception as e:
            results.append(("step3_options_gex", False, str(e)))

        try:
            fib = self.analyze_fib_cluster(tf_data["daily"], tf_data["h4"], _safe_float(ind_daily.iloc[-1]["close"]))
            vp = self.analyze_volume_profile(tf_data["daily"])
            levels = self.synthesize_levels(
                spot=_safe_float(ind_daily.iloc[-1]["close"]),
                atr=_safe_float(ind_daily.iloc[-1].get("atr14"), 1.0),
                ind_daily=ind_daily,
                fib=fib,
                gex=gex if "gex" in locals() else {
                    "put_wall": float("nan"),
                    "call_wall": float("nan"),
                    "zero_gamma": float("nan"),
                },
                vp=vp,
            )
            ok = (
                levels["extreme_resistance"] >= levels["secondary_resistance"] >= levels["axis"]
                and levels["axis"] >= levels["secondary_support"] >= levels["extreme_support"]
            )
            results.append(("step4_level_order", ok, str(levels)))
        except Exception as e:
            results.append(("step4_level_order", False, str(e)))

        try:
            out = self.run()
            ok = all(Path(p).exists() for p in out.output_paths.values())
            results.append(("step5_outputs", ok, json.dumps(out.output_paths)))
        except Exception as e:
            results.append(("step5_outputs", False, str(e)))

        return results
