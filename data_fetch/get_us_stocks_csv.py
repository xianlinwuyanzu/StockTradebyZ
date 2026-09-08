"""Build a liquid US stock universe with coverage across all major sectors.

Sources:
    - S&P 500 constituents (GICS sector and sub-industry)
    - S&P MidCap 400 constituents (official MDY holdings)
    - Nasdaq-100 constituents
    - Yahoo most-active stocks
    - Yahoo equity screener, limited to the largest stocks in each major sector

The legacy columns (``ts_code,symbol,name,area,industry``) stay intact.  The
additional metadata columns make sector coverage and source membership
auditable without putting ETFs into the stock candidate universe.
"""

from __future__ import annotations

import logging
import argparse
import re
import sys
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
from xml.etree import ElementTree as ET

import pandas as pd
import requests
import yfinance as yf
from yfinance.screener import EquityQuery, screen

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("USCoreStocks")

OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "tools"
OUT_FILE = OUT_DIR / "stocklist_us.csv"

GICS_SECTORS = (
    "Information Technology",
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Materials",
    "Real Estate",
    "Utilities",
)

# Yahoo Finance uses slightly different names from GICS for several sectors.
YAHOO_SECTOR_TO_GICS = {
    "Technology": "Information Technology",
    "Communication Services": "Communication Services",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Energy": "Energy",
    "Financial Services": "Financials",
    "Healthcare": "Health Care",
    "Industrials": "Industrials",
    "Basic Materials": "Materials",
    "Real Estate": "Real Estate",
    "Utilities": "Utilities",
}

STOCK_COLUMNS = [
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "sector",
    "industry_detail",
    "asset_type",
    "source",
]

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
_SP400_HOLDINGS_URL = (
    "https://www.ssga.com/library-content/products/fund-data/etfs/us/"
    "holdings-daily-us-en-mdy.xlsx"
)


def _clean_text(value: object, fallback: str = "Unknown") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "unknown"}:
        return fallback
    return text


def _normalize_symbol(value: object) -> str:
    symbol = _clean_text(value, fallback="")
    if not symbol:
        return ""
    return symbol.upper().replace("_", "-")


def _empty_stock_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=STOCK_COLUMNS)


def _gics_sector(value: object) -> str:
    text = _clean_text(value, fallback="")
    if text in GICS_SECTORS:
        return text
    return YAHOO_SECTOR_TO_GICS.get(text, "")


def get_sp500() -> pd.DataFrame:
    """
    从 GitHub（datasets/s-and-p-500-companies）获取标普500成分股。
    返回列：Symbol, Name, Sector —— 直接映射到输出格式，无需额外查询。
    """
    logger.info("获取标普500成分股（GitHub CSV）...")
    df = pd.read_csv(_SP500_CSV_URL)          # 列：Symbol, Name, Sector
    symbols = df["Symbol"].map(_normalize_symbol)
    sectors = df["GICS Sector"].map(lambda value: _clean_text(value))
    result = pd.DataFrame({
        "ts_code": symbols,
        "symbol": symbols,
        "name": df["Security"].map(_clean_text),
        "area": "US",
        "industry": sectors,
        "sector": sectors,
        "industry_detail": df.get("GICS Sub-Industry", pd.Series("Unknown", index=df.index)).map(_clean_text),
        "asset_type": "stock",
        "source": "sp500",
    })
    result = result[result["symbol"] != ""].reset_index(drop=True)
    logger.info("标普500: %d 只", len(result))
    return result


def _xlsx_column_index(reference: str) -> int:
    letters = re.sub(r"[0-9]+$", "", reference)
    index = 0
    for letter in letters:
        index = index * 26 + ord(letter) - ord("A") + 1
    return index - 1


def _read_xlsx_rows(payload: bytes) -> list[list[str]]:
    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    namespaced = {"main": namespace}
    with ZipFile(BytesIO(payload)) as workbook:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            shared_root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
            shared_strings = [
                "".join(text.text or "" for text in item.iter(f"{{{namespace}}}t"))
                for item in shared_root.findall("main:si", namespaced)
            ]

        sheet_root = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
        rows: list[list[str]] = []
        for row_element in sheet_root.findall(".//main:row", namespaced):
            values: dict[int, str] = {}
            for cell in row_element.findall("main:c", namespaced):
                cell_value = cell.find("main:v", namespaced)
                text = "" if cell_value is None else cell_value.text or ""
                if cell.attrib.get("t") == "s" and text:
                    text = shared_strings[int(text)]
                elif cell.attrib.get("t") == "inlineStr":
                    text = "".join(text_node.text or "" for text_node in cell.iter(f"{{{namespace}}}t"))
                values[_xlsx_column_index(cell.attrib["r"])] = text
            if values:
                rows.append([values.get(index, "") for index in range(max(values) + 1)])
        return rows


def get_sp400() -> pd.DataFrame:
    """从 State Street 的 MDY 官方持仓文件获取标普中盘 400 成分股。"""
    logger.info("获取标普中盘400成分股（MDY 官方持仓）...")
    response = requests.get(_SP400_HOLDINGS_URL, headers=_HEADERS, timeout=30)
    response.raise_for_status()
    rows = _read_xlsx_rows(response.content)
    header_index = next(
        (index for index, row in enumerate(rows) if len(row) >= 2 and row[0] == "Name" and row[1] == "Ticker"),
        None,
    )
    if header_index is None:
        raise ValueError("未在 MDY 持仓文件找到 Name/Ticker 表头")

    header = rows[header_index]
    column_index = {name: index for index, name in enumerate(header)}
    required_columns = {"Name", "Ticker", "Sector"}
    if not required_columns.issubset(column_index):
        raise ValueError(f"MDY 持仓文件缺少字段: {sorted(required_columns - set(column_index))}")

    records: list[dict[str, str]] = []
    for row in rows[header_index + 1:]:
        ticker = row[column_index["Ticker"]].strip() if len(row) > column_index["Ticker"] else ""
        if not ticker:
            continue
        sector = row[column_index["Sector"]].strip() if len(row) > column_index["Sector"] else ""
        name = row[column_index["Name"]].strip() if len(row) > column_index["Name"] else ""
        if ticker.upper().startswith("CASH_") or sector == "Unassigned":
            continue
        symbol = _normalize_symbol(ticker)
        if not symbol:
            continue
        records.append({
            "ts_code": symbol,
            "symbol": symbol,
            "name": _clean_text(name, fallback=symbol),
            "area": "US",
            "industry": _clean_text(sector),
            "sector": _clean_text(sector),
            "industry_detail": "Unknown",
            "asset_type": "stock",
            "source": "sp400",
        })

    result = pd.DataFrame(records, columns=STOCK_COLUMNS)
    result = result[result["symbol"] != ""].drop_duplicates("symbol").reset_index(drop=True)
    if len(result) != 400 or result["symbol"].nunique() != 400:
        raise ValueError(f"S&P 400 成分股数量异常: rows={len(result)}, unique={result['symbol'].nunique()}")
    logger.info("标普中盘400: %d 只", len(result))
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
                symbols = [_normalize_symbol(ticker) for ticker in tickers]
                result = pd.DataFrame({
                    "ts_code": symbols,
                    "symbol": symbols,
                    "name": "Unknown",
                    "area": "US",
                    "industry": "Unknown",
                    "sector": "Unknown",
                    "industry_detail": "Unknown",
                    "asset_type": "stock",
                    "source": "nasdaq100",
                })
                result = result[result["symbol"] != ""].reset_index(drop=True)
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
        return _empty_stock_frame()

    symbols = [_normalize_symbol(q.get("symbol")) for q in quotes]
    sectors = [_gics_sector(q.get("sector")) or "Unknown" for q in quotes]
    rows = pd.DataFrame({
        "ts_code": symbols,
        "symbol": symbols,
        "name": [
            _clean_text(q.get("longName") or q.get("shortName") or q.get("symbol"))
            for q in quotes
        ],
        "area":     [q.get("region", "US") for q in quotes],
        "industry": sectors,
        "sector": sectors,
        "industry_detail": [_clean_text(q.get("industry")) for q in quotes],
        "asset_type": "stock",
        "source": "most_active",
    })
    rows = rows[rows["symbol"] != ""].reset_index(drop=True)
    logger.info("最活跃股票: %d 只", len(rows))
    return rows


def get_major_sector_stocks(count: int = 100) -> pd.DataFrame:
    """Fetch the largest equity quotes returned for each Yahoo sector."""
    if count <= 0:
        return _empty_stock_frame()

    rows: list[dict[str, str]] = []
    for gics_sector in GICS_SECTORS:
        yahoo_sector = next(
            yahoo_name
            for yahoo_name, mapped_sector in YAHOO_SECTOR_TO_GICS.items()
            if mapped_sector == gics_sector
        )
        query = EquityQuery("eq", ["sector", yahoo_sector])
        try:
            response = screen(
                query,
                count=count,
                sortField="intradaymarketcap",
                sortAsc=False,
            )
        except Exception as exc:
            logger.warning("行业筛选失败 %s: %s", gics_sector, exc)
            continue

        quotes = response.get("quotes", [])
        sector_rows = 0
        for quote in quotes:
            symbol = _normalize_symbol(quote.get("symbol"))
            if not symbol:
                continue
            quote_type = _clean_text(quote.get("quoteType"), fallback="EQUITY").upper()
            if quote_type not in {"EQUITY", "STOCK"}:
                continue
            rows.append({
                "ts_code": symbol,
                "symbol": symbol,
                "name": _clean_text(
                    quote.get("longName") or quote.get("shortName") or symbol,
                    fallback=symbol,
                ),
                "area": "US",
                "industry": gics_sector,
                "sector": gics_sector,
                "industry_detail": _clean_text(quote.get("industry")),
                "asset_type": "stock",
                "source": f"sector:{gics_sector}",
            })
            sector_rows += 1
        logger.info("主要行业 %s: %d 只", gics_sector, sector_rows)

    return pd.DataFrame(rows, columns=STOCK_COLUMNS) if rows else _empty_stock_frame()


def _known_value(value: object) -> str:
    text = _clean_text(value, fallback="")
    return "" if text.lower() == "unknown" else text


def merge_stock_sources(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Deduplicate symbols while preserving all source memberships."""
    available = [frame for frame in frames if frame is not None and not frame.empty]
    if not available:
        return _empty_stock_frame()

    combined = pd.concat(available, ignore_index=True)
    combined["symbol"] = combined["symbol"].map(_normalize_symbol)
    combined["ts_code"] = combined["symbol"]
    combined = combined[combined["symbol"] != ""].copy()

    merged_rows: list[dict[str, str]] = []
    for symbol, group in combined.groupby("symbol", sort=False):
        first = group.iloc[0].to_dict()
        for field in ["name", "area", "industry", "sector", "industry_detail"]:
            value = next((_known_value(item) for item in group[field] if _known_value(item)), "")
            if value:
                first[field] = value
        first["ts_code"] = symbol
        first["symbol"] = symbol
        first["asset_type"] = "stock"
        first["source"] = "|".join(dict.fromkeys(
            _clean_text(item, fallback="")
            for item in group["source"]
            if _clean_text(item, fallback="")
        ))
        merged_rows.append({column: first.get(column, "Unknown") for column in STOCK_COLUMNS})

    merged = pd.DataFrame(merged_rows, columns=STOCK_COLUMNS)
    covered = set(merged.loc[merged["sector"].isin(GICS_SECTORS), "sector"])
    missing = [sector for sector in GICS_SECTORS if sector not in covered]
    if missing:
        raise RuntimeError(f"主要行业覆盖不完整，缺少: {', '.join(missing)}")
    return merged.reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="生成覆盖美股主要行业的股票池")
    parser.add_argument("--output", type=Path, default=OUT_FILE, help="输出股票名单 CSV")
    parser.add_argument("--sector-count", type=int, default=100, help="每个一级行业最多抓取的股票数")
    parser.add_argument("--active-count", type=int, default=100, help="最活跃股票数量")
    parser.add_argument("--skip-sector-screener", action="store_true", help="跳过行业筛选，仅使用基础指数和活跃股")
    args = parser.parse_args()

    sp500_df = get_sp500()
    sp400_df = get_sp400()
    nasdaq_df = get_nasdaq100()
    actives_df = get_most_actives(count=max(0, args.active_count))
    sector_df = (
        _empty_stock_frame()
        if args.skip_sector_screener
        else get_major_sector_stocks(count=max(0, args.sector_count))
    )

    merged = merge_stock_sources([sp500_df, sp400_df, nasdaq_df, actives_df, sector_df])

    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output, index=False)
    counts = merged.groupby("sector").size().reindex(GICS_SECTORS, fill_value=0)
    logger.info(
        "完成：标普500=%d，标普中盘400=%d，纳斯达克100=%d，最活跃=%d，行业筛选=%d，合并去重=%d → %s",
        len(sp500_df), len(sp400_df), len(nasdaq_df), len(actives_df), len(sector_df), len(merged), args.output,
    )
    logger.info("一级行业覆盖：%s", "; ".join(f"{sector}={count}" for sector, count in counts.items()))


if __name__ == "__main__":
    main()
