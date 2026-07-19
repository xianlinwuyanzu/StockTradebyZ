from __future__ import annotations

import argparse
import concurrent.futures
import importlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import matplotlib.pyplot as plt
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("backtest")


# Use ASCII pinyin labels to avoid CJK font issues while staying recognizable.
PLOT_STRATEGY_NAME_MAP: Dict[str, str] = {
    "少妇战法": "Shaofu (BBI-KDJ)",
    "补票战法": "Bupiao (BBI-ShortLong)",
    "填坑战法": "Tiankeng (Peak-KDJ)",
    "上穿60放量战法": "Shangchuan60 (MA60-Cross-Volume)",
    "暴力K战法": "BaoliK (BigBullishVolume)",
    "主任BBD战法": "ZhurenBBD (BBD-Momentum)",
    "主任BBD战法-抄底": "ZhurenBBD-Chaodi (BBD-Momentum-Bottom)",
    "起爆启爆战法": "QibaoQibao (Burst-Signal)",
    "SuperB1战法": "SuperB1",
}


def plot_name(name: str) -> str:
    return PLOT_STRATEGY_NAME_MAP.get(name, name)


def load_data(data_dir: Path, codes: Iterable[str]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for code in codes:
        fp = data_dir / f"{code}.csv"
        if not fp.exists():
            logger.warning("%s 不存在，跳过", fp.name)
            continue

        df = pd.read_csv(fp, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
        required_cols = {"date", "open", "close", "high", "low", "volume"}
        if not required_cols.issubset(df.columns):
            logger.warning("%s 缺少必要列，跳过", fp.name)
            continue

        # 仅保留回测需要字段，避免后续处理歧义。
        keep_cols = [c for c in ["date", "open", "close", "high", "low", "volume"] if c in df.columns]
        df = df[keep_cols].copy()

        for c in ["open", "close", "high", "low", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["date", "open", "close", "high", "low"]).reset_index(drop=True)

        if df.empty:
            continue
        frames[code] = df

    return frames


def load_config(cfg_path: Path) -> List[Dict[str, Any]]:
    if not cfg_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {cfg_path}")

    with cfg_path.open(encoding="utf-8") as f:
        cfg_raw = json.load(f)

    if isinstance(cfg_raw, list):
        cfgs = cfg_raw
    elif isinstance(cfg_raw, dict) and "selectors" in cfg_raw:
        cfgs = cfg_raw["selectors"]
    else:
        cfgs = [cfg_raw]

    return [cfg for cfg in cfgs if cfg.get("activate", True) is not False]


def instantiate_selector(cfg: Dict[str, Any]):
    cls_name = cfg.get("class")
    if not cls_name:
        raise ValueError("缺少 class 字段")

    module = importlib.import_module("Selector")
    cls = getattr(module, cls_name)
    params = cfg.get("params", {})
    alias = cfg.get("alias", cls_name)
    return alias, cls(**params)


def next_day_buy_and_tplus3_sell(
    hist: pd.DataFrame,
    signal_date: pd.Timestamp,
) -> Optional[Dict[str, Any]]:
    dates = hist["date"]
    pos_arr = dates.searchsorted(signal_date, side="right")
    buy_idx = int(pos_arr)

    # 次日开盘买入（第一个严格大于 signal_date 的交易日）
    if buy_idx >= len(hist):
        return None

    # 默认卖出点：买入日后的第 4 个交易日收盘价。
    sell_idx = buy_idx + 4
    if sell_idx >= len(hist):
        return None

    buy_open = float(hist.iloc[buy_idx]["open"])
    if buy_open <= 0:
        return None

    # 持仓期间若出现 -3% 回撤则止损（按止损价成交）。
    stop_loss_price = buy_open * 0.97
    actual_sell_idx = sell_idx
    actual_sell_price = float(hist.iloc[sell_idx]["close"])

    for i in range(buy_idx, sell_idx):
        day_low = float(hist.iloc[i]["low"])
        if day_low <= stop_loss_price:
            actual_sell_idx = i
            actual_sell_price = stop_loss_price
            break

    ret = actual_sell_price / buy_open - 1.0

    return {
        "buy_date": pd.Timestamp(hist.iloc[buy_idx]["date"]),
        "sell_date": pd.Timestamp(hist.iloc[actual_sell_idx]["date"]),
        "buy_open": buy_open,
        "sell_close": actual_sell_price,
        "return": ret,
    }


def run_backtest(
    data: Dict[str, pd.DataFrame],
    selectors: List[Dict[str, Any]],
    start_date: Optional[pd.Timestamp],
    end_date: Optional[pd.Timestamp],
    workers: int = 1,
) -> pd.DataFrame:
    all_dates = pd.Index(sorted({d for df in data.values() for d in df["date"].tolist()}))
    if start_date is not None:
        all_dates = all_dates[all_dates >= start_date]
    if end_date is not None:
        all_dates = all_dates[all_dates <= end_date]

    if len(all_dates) == 0:
        return pd.DataFrame()

    records: List[Dict[str, Any]] = []

    if workers <= 1:
        for cfg in selectors:
            strategy_records = _run_single_strategy(cfg, data, all_dates)
            records.extend(strategy_records)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(_run_single_strategy, cfg, data, all_dates)
                for cfg in selectors
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    strategy_records = future.result()
                except Exception as e:
                    logger.error("并行策略回测失败：%s", e)
                    continue
                records.extend(strategy_records)

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def _run_single_strategy(
    cfg: Dict[str, Any],
    data: Dict[str, pd.DataFrame],
    all_dates: pd.Index,
) -> List[Dict[str, Any]]:
    try:
        alias, selector = instantiate_selector(cfg)
    except Exception as e:
        logger.error("策略 %s 实例化失败：%s", cfg.get("class", "UNKNOWN"), e)
        return []

    logger.info("开始回测策略: %s", alias)
    strategy_records: List[Dict[str, Any]] = []

    for trade_date in all_dates:
        picks = selector.select(pd.Timestamp(trade_date), data)
        if not picks:
            continue

        for code in picks:
            hist = data.get(code)
            if hist is None or hist.empty:
                continue

            trade_info = next_day_buy_and_tplus3_sell(hist, pd.Timestamp(trade_date))
            if trade_info is None:
                continue

            strategy_records.append(
                {
                    "strategy": alias,
                    "signal_date": pd.Timestamp(trade_date).date().isoformat(),
                    "code": code,
                    "buy_date": trade_info["buy_date"].date().isoformat(),
                    "sell_date": trade_info["sell_date"].date().isoformat(),
                    "buy_open": trade_info["buy_open"],
                    "sell_close": trade_info["sell_close"],
                    "return": trade_info["return"],
                }
            )

    logger.info("策略完成: %s, 交易笔数: %d", alias, len(strategy_records))
    return strategy_records


def build_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()

    g = trades.groupby("strategy")
    summary = g["return"].agg(["count", "mean", "median", "std"]).reset_index()
    summary = summary.rename(
        columns={
            "count": "trade_count",
            "mean": "avg_return",
            "median": "median_return",
            "std": "return_std",
        }
    )

    win_rate = (trades.assign(is_win=trades["return"] > 0)
                .groupby("strategy", as_index=False)["is_win"]
                .mean()
                .rename(columns={"is_win": "win_rate"}))
    summary = summary.merge(win_rate, on="strategy", how="left")

    summary = summary.sort_values("avg_return", ascending=False).reset_index(drop=True)
    return summary


def build_daily_return_series(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()

    base = trades.assign(signal_date=pd.to_datetime(trades["signal_date"]))
    grouped = (
        base.groupby(["signal_date", "strategy"], as_index=False)["return"]
        .mean()
    )

    all_dates = pd.date_range(base["signal_date"].min(), base["signal_date"].max(), freq="D")
    all_strategies = sorted(base["strategy"].dropna().unique().tolist())
    full_index = pd.MultiIndex.from_product(
        [all_dates, all_strategies], names=["signal_date", "strategy"]
    ).to_frame(index=False)

    daily = (
        full_index
        .merge(grouped, on=["signal_date", "strategy"], how="left")
        .fillna({"return": 0.0})
        .sort_values(["signal_date", "strategy"])
        .reset_index(drop=True)
    )
    return daily


def plot_daily_trade_return(daily: pd.DataFrame, output_path: Path) -> None:
    if daily.empty:
        return

    plt.figure(figsize=(14, 7))
    for strategy, g in daily.groupby("strategy"):
        plt.plot(g["signal_date"], g["return"], label=plot_name(strategy), linewidth=1.3)

    plt.axhline(0.0, color="gray", linewidth=1.0, linestyle="--")
    plt.title("Strategy Daily Single-Trade Return")
    plt.xlabel("Date")
    plt.ylabel("Single-Trade Return")
    plt.legend(loc="best", fontsize=9)
    plt.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def plot_cumulative_return(daily: pd.DataFrame, output_path: Path) -> None:
    if daily.empty:
        return

    plt.figure(figsize=(14, 7))
    for strategy, g in daily.groupby("strategy"):
        g = g.sort_values("signal_date").copy()
        g["cum_return"] = (1.0 + g["return"]).cumprod() - 1.0
        plt.plot(g["signal_date"], g["cum_return"], label=plot_name(strategy), linewidth=1.5)

    plt.axhline(0.0, color="gray", linewidth=1.0, linestyle="--")
    plt.title("Strategy Cumulative Return")
    plt.xlabel("Date")
    plt.ylabel("Cumulative Return")
    plt.legend(loc="best", fontsize=9)
    plt.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="按次日开盘买入/第4日收盘卖出（-3%止损）规则回测各策略")
    parser.add_argument("--data-dir", default="./data/us_stocks", help="美股CSV目录")
    parser.add_argument("--config", default="./configs.json", help="策略配置文件")
    parser.add_argument("--tickers", default="all", help="all 或逗号分隔代码")
    parser.add_argument("--start-date", default=None, help="回测起始日 YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="回测结束日 YYYY-MM-DD")
    parser.add_argument("--out-trades", default="./backtest_trades.csv", help="逐笔交易输出")
    parser.add_argument("--out-summary", default="./backtest_summary.csv", help="策略汇总输出")
    parser.add_argument("--out-plot-daily", default="./backtest_daily_trade_return.png", help="单次交易收益曲线图")
    parser.add_argument("--out-plot-cum", default="./backtest_cumulative_return.png", help="累计收益曲线图")
    parser.add_argument("--workers", type=int, default=1, help="并行进程数（按策略并行）")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        raise FileNotFoundError(f"数据目录不存在: {data_dir}")

    codes = (
        [f.stem for f in data_dir.glob("*.csv")]
        if args.tickers.lower() == "all"
        else [c.strip() for c in args.tickers.split(",") if c.strip()]
    )
    if not codes:
        raise ValueError("股票池为空")

    data = load_data(data_dir, codes)
    if not data:
        raise ValueError("未加载到任何有效行情数据")

    selectors = load_config(Path(args.config))
    start_date = pd.to_datetime(args.start_date) if args.start_date else None
    end_date = pd.to_datetime(args.end_date) if args.end_date else None

    trades = run_backtest(data, selectors, start_date, end_date, workers=max(1, args.workers))
    if trades.empty:
        logger.warning("无可用回测交易记录")
        return

    summary = build_summary(trades)

    out_trades = Path(args.out_trades)
    out_summary = Path(args.out_summary)
    out_plot_daily = Path(args.out_plot_daily)
    out_plot_cum = Path(args.out_plot_cum)
    trades.to_csv(out_trades, index=False, encoding="utf-8")
    summary.to_csv(out_summary, index=False, encoding="utf-8")

    daily = build_daily_return_series(trades)
    plot_daily_trade_return(daily, out_plot_daily)
    plot_cumulative_return(daily, out_plot_cum)

    logger.info("回测完成：交易明细 %s", out_trades)
    logger.info("回测完成：策略汇总 %s", out_summary)
    logger.info("回测完成：单次交易收益图 %s", out_plot_daily)
    logger.info("回测完成：累计收益图 %s", out_plot_cum)
    logger.info("\n%s", summary.to_string(index=False, float_format=lambda x: f"{x:.4%}" if abs(x) < 2 else f"{x:.4f}"))


if __name__ == "__main__":
    main()
