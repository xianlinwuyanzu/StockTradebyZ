from __future__ import annotations

import argparse
import io
import logging
import sys
from pathlib import Path

import pandas as pd
import requests

DEFAULT_SERIES_ID = "SP500"
DEFAULT_OUTPUT = Path("./data/indices/SPX_FRED.csv")
DEFAULT_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
DEFAULT_TIMEOUT = 20.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("FREDIndexFetcher")


def _fetch_fred_series(series_id: str, timeout: float) -> pd.DataFrame:
    response = requests.get(DEFAULT_URL, params={"id": series_id}, timeout=timeout)
    response.raise_for_status()

    text = response.text or ""
    if not text.strip():
        raise RuntimeError("empty response from FRED")

    frame = pd.read_csv(io.StringIO(text))
    value_col = series_id
    if "observation_date" not in frame.columns or value_col not in frame.columns:
        header = text.splitlines()[0] if text.splitlines() else ""
        raise RuntimeError(f"unexpected CSV header from FRED: {header}")

    out = frame[["observation_date", value_col]].copy()
    out = out.rename(columns={"observation_date": "date", value_col: "close"})
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)

    # Keep an OHLCV-like shape so downstream readers can reuse this file directly if needed.
    out["open"] = out["close"]
    out["high"] = out["close"]
    out["low"] = out["close"]
    out["volume"] = 0
    out = out[["date", "open", "close", "high", "low", "volume"]]
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch SPX-like index series from FRED")
    parser.add_argument("--series-id", default=DEFAULT_SERIES_ID, help="FRED series id, e.g. SP500")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="output CSV path")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds")
    parser.add_argument("--start-date", default="", help="optional start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="", help="optional end date (YYYY-MM-DD)")
    args = parser.parse_args()

    series_id = str(args.series_id).strip().upper()
    if not series_id:
        logger.error("empty --series-id")
        return 2

    try:
        frame = _fetch_fred_series(series_id=series_id, timeout=max(1.0, args.timeout))
    except Exception as exc:
        logger.error("fetch failed series=%s err=%s", series_id, exc)
        return 2

    if args.start_date:
        frame = frame[frame["date"] >= args.start_date]
    if args.end_date:
        frame = frame[frame["date"] <= args.end_date]
    frame = frame.reset_index(drop=True)

    if frame.empty:
        logger.error("no rows after date filtering, series=%s", series_id)
        return 2

    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.output, index=False)

    logger.info("saved %s rows=%d series=%s", args.output, len(frame), series_id)
    logger.info("date range: %s -> %s", frame["date"].iloc[0], frame["date"].iloc[-1])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
