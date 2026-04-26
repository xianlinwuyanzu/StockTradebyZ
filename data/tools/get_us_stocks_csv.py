"""
获取标普500 + 纳斯达克100 成分股代码
数据源（不依赖维基百科）：
  - 标普500  ：GitHub datasets/s-and-p-500-companies（含名称、行业）
  - 纳斯达克100：NASDAQ 官方 API api.nasdaq.com
输出格式：ts_code,symbol,name,area,industry
运行方式：python data/tools/get_us_core_stocks_csv.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from yfinance.screener import screen

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("USCoreStocks")

OUT_DIR = Path(__file__).resolve().parent
OUT_FILE = OUT_DIR / "stocklist_us.csv"

# 浏览器 UA，避免被拒
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# 数据源
_SP500_CSV_URL = (
    "https://raw.githubusercontent.com/datasets/"
    "s-and-p-500-companies/main/data/constituents.csv"
)


def get_sp500() -> pd.DataFrame:
    """
    从 GitHub（datasets/s-and-p-500-companies）获取标普500成分股。
    返回列：Symbol, Name, Sector —— 直接映射到输出格式，无需额外查询。
    """
    logger.info("获取标普500成分股（GitHub CSV）...")
    df = pd.read_csv(_SP500_CSV_URL)          # 列：Symbol, Name, Sector
    result = pd.DataFrame({
        "ts_code":  df["Symbol"].str.strip(),
        "symbol":   df["Symbol"].str.strip(),
        "name":     df["Security"].fillna("Unknown"),
        "area":     "US",
        "industry": df["GICS Sector"].fillna("Unknown"),
    })
    logger.info("标普500: %d 只", len(result))
    return result


def get_nasdaq100() -> pd.DataFrame:
    """
    获取纳斯达克100成分股。
    数据源：维基百科（需浏览器 UA，否则返回 403）
    说明：NASDAQ 官方没有免费公开的成分股 API，维基百科是最稳定的替代方案。
    """
    from io import StringIO
    logger.info("获取纳斯达克100成分股（Wikipedia）...")
    resp = requests.get(
        "https://en.wikipedia.org/wiki/Nasdaq-100",
        headers=_HEADERS, timeout=30,
    )
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text), flavor="lxml")
    for tbl in tables:
        matched = [c for c in tbl.columns if str(c).lower() in ("ticker", "symbol")]
        if matched:
            tickers = tbl[matched[0]].astype(str).str.strip().tolist()
            tickers = [t for t in tickers if t and t.lower() not in ("nan", "ticker", "symbol")]
            if len(tickers) > 50:
                result = pd.DataFrame({
                    "ts_code":  tickers,
                    "symbol":   tickers,
                    "name":     "Unknown",
                    "area":     "US",
                    "industry": "Unknown",
                })
                logger.info("纳斯达克100: %d 只", len(result))
                return result
    raise ValueError("未在维基百科页面找到纳斯达克100成分股表格")


def get_most_actives(count: int = 100) -> pd.DataFrame:
    """
    通过 yfinance screener 获取当日成交量最活跃的前 N 只股票。
    接口：yfinance.screener.screen('most_actives')
    返回字段：symbol, longName, region
    """
    logger.info("获取最活跃股票 Top%d（yfinance screener）...", count)
    result = screen("most_actives", count=count)
    quotes = result.get("quotes", [])
    if not quotes:
        logger.warning("most_actives 返回空列表")
        return pd.DataFrame(columns=["ts_code", "symbol", "name", "area", "industry"])

    rows = pd.DataFrame({
        "ts_code":  [q["symbol"] for q in quotes],
        "symbol":   [q["symbol"] for q in quotes],
        "name":     [q.get("longName") or q.get("shortName") or q["symbol"] for q in quotes],
        "area":     [q.get("region", "US") for q in quotes],
        "industry": "Unknown",   # screener 不返回行业字段
    })
    logger.info("最活跃股票: %d 只", len(rows))
    return rows


def main() -> None:
    sp500_df      = get_sp500()
    nasdaq_df     = get_nasdaq100()
    actives_df    = get_most_actives(count=100)

    merged = (
        pd.concat([sp500_df, nasdaq_df, actives_df], ignore_index=True)
        .drop_duplicates(subset=["ts_code"], keep="first")
        .reset_index(drop=True)
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_FILE, index=False)
    logger.info(
        "完成：标普500=%d，纳斯达克100=%d，最活跃=%d，合并去重=%d → %s",
        len(sp500_df), len(nasdaq_df), len(actives_df), len(merged), OUT_FILE,
    )


if __name__ == "__main__":
    main()
