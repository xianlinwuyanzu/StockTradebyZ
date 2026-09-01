from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from backtest_two_wave_j3 import (
    DEFAULTS,
    _compute_j,
    find_setups,
    is_medium_bullish,
    is_small_j,
    load_frames,
    next_open_fill,
)


@dataclass(frozen=True)
class StrategySpec:
    name: str
    entry_mode: str
    allocation_mode: str
    exit_mode: str
    hold_days: int
    stop_pct: float | None = None


@dataclass(frozen=True)
class SimTrade:
    strategy: str
    code: str
    signal_idx: int
    buy_count: int
    portfolio_return: float
    deployed_return: float
    exit_reason: str
    censored: bool
    entry_date: str
    exit_date: str
    breakout: bool | None


DEFAULT_BUILD_WINDOW = 10
BUY3_TOLERANCE = 0.02
J_WINDOW = 120
J_THRESHOLD = 15.0
J_QUANTILE = 0.10


def candidate_valid_j(frame: pd.DataFrame, j_values: pd.Series, setup: Any, index: int, require_upturn: bool) -> bool:
    if not is_small_j(j_values, index):
        return False
    period = index - setup.start2
    close = float(frame["close"].iloc[index])
    top_close = float(frame["close"].iloc[setup.top2])
    start_close = float(frame["close"].iloc[setup.start2])
    pullback = (top_close - close) / top_close
    j_pullback_drop = float(j_values.iloc[setup.top2] - j_values.iloc[index])
    changes = frame["close"].iloc[setup.start2 : index + 1].diff().dropna()
    efficiency = abs(close - start_close) / float(changes.abs().sum()) if len(changes) else 0.0
    if not DEFAULTS["min_complete_wave_period"] <= period <= DEFAULTS["max_complete_wave_period"]:
        return False
    if close < start_close * (1.0 + DEFAULTS["min_step"]):
        return False
    price_pullback_valid = DEFAULTS["min_pullback"] <= pullback <= DEFAULTS["max_pullback"]
    j_pullback_valid = j_pullback_drop >= DEFAULTS["min_j_pullback_drop"]
    if pullback > DEFAULTS["max_pullback"] or not (price_pullback_valid or j_pullback_valid):
        return False
    if efficiency < DEFAULTS["min_complete_wave_path_efficiency"]:
        return False
    if require_upturn and (index == 0 or float(j_values.iloc[index]) <= float(j_values.iloc[index - 1])):
        return False
    return True


def find_entry(frame: pd.DataFrame, setup: Any, mode: str) -> int | None:
    if mode == "j_first":
        return setup.j_entry
    j_values = _compute_j(frame)
    if mode in {"j_continue", "j_upturn"}:
        require_upturn = mode == "j_upturn"
        end = min(len(frame) - 1, setup.top2 + 1 + int(DEFAULTS["j_entry_max_period"]))
        for index in range(setup.top2 + 1, end + 1):
            if candidate_valid_j(frame, j_values, setup, index, require_upturn):
                return index
        return None
    if mode == "breakout":
        level = float(frame["close"].iloc[setup.top2]) * (1.0 + float(DEFAULTS["third_wave_breakout_pct"]))
        for index in range(setup.top2 + 1, len(frame)):
            if float(frame["close"].iloc[index]) >= level:
                return index
        return None
    raise ValueError(f"unknown entry mode: {mode}")


def build_fills(frame: pd.DataFrame, signal_idx: int, allocation_mode: str) -> list[tuple[int, float]] | None:
    first = next_open_fill(frame, signal_idx, "buy1", 1.0 if allocation_mode == "all_in" else 1.0 / 3.0)
    if first is None:
        return None
    fills = [(first.exec_idx, first.price, first.allocation)]
    if allocation_mode == "all_in":
        return fills

    buy2 = None
    end = min(len(frame) - 1, first.exec_idx + DEFAULT_BUILD_WINDOW)
    for check in range(first.exec_idx + 2, end + 1, 2):
        if float(frame["close"].iloc[check]) < float(frame["close"].iloc[check - 1]) and float(frame["close"].iloc[check]) < first.price:
            buy2 = next_open_fill(frame, check, "buy2", 1.0 / 3.0)
            break
    if buy2 is None:
        return fills
    fills.append((buy2.exec_idx, buy2.price, buy2.allocation))

    for check in range(buy2.exec_idx + 1, end + 1):
        close = float(frame["close"].iloc[check])
        if close > buy2.price and abs(close / first.price - 1.0) <= BUY3_TOLERANCE:
            buy3 = next_open_fill(frame, check, "buy3", 1.0 / 3.0)
            if buy3 is not None:
                fills.append((buy3.exec_idx, buy3.price, buy3.allocation))
            break
    return fills


def portfolio_value(fills: list[tuple[int, float, float]], exit_price: float, partial: bool = False, partial_price: float | None = None) -> tuple[float, float, float]:
    invested = sum(item[2] for item in fills)
    shares = sum(item[2] / item[1] for item in fills)
    cash = 1.0 - invested
    if partial and partial_price is not None:
        sold = shares / 2.0
        cash += sold * partial_price
        shares -= sold
    cash += shares * exit_price
    return cash - 1.0, (cash - 1.0) / invested if invested else 0.0, invested


def exit_trade(frame: pd.DataFrame, fills: list[tuple[int, float, float]], setup: Any, spec: StrategySpec) -> tuple[int | None, float | None, str, bool]:
    last_buy = max(item[0] for item in fills)
    end = min(len(frame) - 1, last_buy + spec.hold_days - 1)
    if end <= last_buy or end >= len(frame):
        return None, None, "censored", True

    avg_entry = sum(item[1] * item[2] for item in fills) / sum(item[2] for item in fills)
    if spec.stop_pct is not None:
        stop = avg_entry * (1.0 - spec.stop_pct)
        for index in range(last_buy, end + 1):
            if float(frame["low"].iloc[index]) <= stop:
                return index, stop, "structure_or_pct_stop", False

    if spec.exit_mode == "fixed":
        return end, float(frame["close"].iloc[end]), f"fixed_{spec.hold_days}d", False

    if spec.exit_mode == "target_top":
        target = float(frame["close"].iloc[setup.top2])
        for index in range(last_buy, end + 1):
            if float(frame["close"].iloc[index]) >= target:
                return index, float(frame["close"].iloc[index]), "target_second_top", False
        return end, float(frame["close"].iloc[end]), f"target_or_{spec.hold_days}d", False

    if spec.exit_mode == "target_breakout":
        target = float(frame["close"].iloc[setup.top2]) * (1.0 + float(DEFAULTS["third_wave_breakout_pct"]))
        for index in range(last_buy, end + 1):
            if float(frame["close"].iloc[index]) >= target:
                return index, float(frame["close"].iloc[index]), "target_breakout", False
        return end, float(frame["close"].iloc[end]), f"breakout_or_{spec.hold_days}d", False

    if spec.exit_mode == "ma5_trail":
        closes = frame["close"]
        for index in range(last_buy + 2, end + 1):
            ma5 = float(closes.iloc[max(0, index - 4) : index + 1].mean())
            if float(closes.iloc[index]) < ma5:
                return index, float(closes.iloc[index]), "close_below_ma5", False
        return end, float(frame["close"].iloc[end]), f"ma5_or_{spec.hold_days}d", False

    if spec.exit_mode == "bullish_partial":
        medium = 0
        partial_idx = None
        partial_price = None
        bullish_after = 0
        for index in range(last_buy + 1, end + 1):
            row = frame.iloc[index]
            if is_medium_bullish(row, {"medium_bull_pct": 0.03, "medium_bull_body_ratio": 0.50}):
                medium += 1
            if partial_idx is None and medium >= 3:
                partial_idx = index
                partial_price = float(row["close"])
                continue
            if partial_idx is not None and index > partial_idx and float(row["close"]) > float(row["open"]):
                bullish_after += 1
                if bullish_after >= 2:
                    return index, float(row["close"]), "3_medium_half_2_bull_full", False
        if partial_idx is not None and partial_price is not None:
            return end, float(frame["close"].iloc[end]), f"partial_or_{spec.hold_days}d", False
        return end, float(frame["close"].iloc[end]), f"bullish_or_{spec.hold_days}d", False

    raise ValueError(f"unknown exit mode: {spec.exit_mode}")


def simulate(frame: pd.DataFrame, setup: Any, spec: StrategySpec) -> SimTrade | None:
    signal_idx = find_entry(frame, setup, spec.entry_mode)
    if signal_idx is None:
        return None
    fills = build_fills(frame, signal_idx, spec.allocation_mode)
    if fills is None:
        return None
    exit_idx, exit_price, reason, censored = exit_trade(frame, fills, setup, spec)
    if censored or exit_idx is None or exit_price is None:
        return SimTrade(spec.name, setup.code, signal_idx, len(fills), np.nan, np.nan, reason, True, str(frame.date.iloc[signal_idx].date()), "", setup.third_wave_breakout)
    portfolio_ret, deployed_ret, _ = portfolio_value(fills, exit_price)
    return SimTrade(spec.name, setup.code, signal_idx, len(fills), portfolio_ret, deployed_ret, reason, False, str(frame.date.iloc[signal_idx].date()), str(frame.date.iloc[exit_idx].date()), setup.third_wave_breakout)


def build_specs() -> list[StrategySpec]:
    specs: list[StrategySpec] = []
    for entry in ["j_first", "j_continue", "j_upturn"]:
        for allocation in ["scale3", "all_in"]:
            for hold in [5, 8, 10, 15, 20]:
                specs.append(StrategySpec(f"{entry}_{allocation}_fixed{hold}", entry, allocation, "fixed", hold))
            specs.append(StrategySpec(f"{entry}_{allocation}_target_top20", entry, allocation, "target_top", 20))
            specs.append(StrategySpec(f"{entry}_{allocation}_target_breakout25", entry, allocation, "target_breakout", 25))
            specs.append(StrategySpec(f"{entry}_{allocation}_ma5trail20", entry, allocation, "ma5_trail", 20))
            specs.append(StrategySpec(f"{entry}_{allocation}_bullish_partial15", entry, allocation, "bullish_partial", 15))
    for allocation in ["scale3", "all_in"]:
        for hold in [5, 8, 10, 15, 20]:
            specs.append(StrategySpec(f"breakout_{allocation}_fixed{hold}", "breakout", allocation, "fixed", hold))
        specs.append(StrategySpec(f"breakout_{allocation}_stop10_hold20", "breakout", allocation, "fixed", 20, 0.10))
    return specs


def run() -> None:
    parser = argparse.ArgumentParser(description="Compare two-wave trading strategies")
    parser.add_argument("--data-dir", type=Path, default=Path("./data/us_stocks"))
    parser.add_argument("--output-dir", type=Path, default=Path("./backtest_two_wave_compare_20260820"))
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    frames = load_frames(args.data_dir)
    latest = max(frame.date.max() for frame in frames)
    nominal_start = latest - pd.Timedelta(days=365)
    frame_map: dict[str, pd.DataFrame] = {}
    setups: list[Any] = []
    for frame in frames:
        window = frame[(frame.date >= nominal_start) & (frame.date <= latest)].reset_index(drop=True)
        if len(window) < 30:
            continue
        frame_map[str(window.code.iloc[0])] = window
        setups.extend(find_setups(window, DEFAULTS))

    specs = build_specs()
    trades: list[SimTrade] = []
    for spec in specs:
        for setup in setups:
            trade = simulate(frame_map[setup.code], setup, spec)
            if trade is not None:
                trades.append(trade)
    trade_df = pd.DataFrame([trade.__dict__ for trade in trades])
    trade_df.to_csv(args.output_dir / "strategy_trade_details.csv", index=False)

    summary_rows: list[dict[str, Any]] = []
    complete_entry_dates = pd.to_datetime(trade_df.loc[~trade_df.censored, "entry_date"])
    split_date = complete_entry_dates.median() if not complete_entry_dates.empty else pd.Timestamp("1970-01-01")
    for name, group in trade_df.groupby("strategy"):
        complete = group[~group.censored]
        values = complete.portfolio_return.dropna()
        if values.empty:
            continue
        early = complete[pd.to_datetime(complete.entry_date) < split_date].portfolio_return.dropna()
        late = complete[pd.to_datetime(complete.entry_date) >= split_date].portfolio_return.dropna()
        summary_rows.append({
            "strategy": name,
            "entry_mode": name.split("_")[0],
            "trade_count": len(group),
            "complete_count": len(complete),
            "censored_count": int(group.censored.sum()),
            "mean_portfolio_return": values.mean(),
            "median_portfolio_return": values.median(),
            "win_rate": (values > 0).mean(),
            "p25_portfolio_return": values.quantile(0.25),
            "p75_portfolio_return": values.quantile(0.75),
            "max_return": values.max(),
            "min_return": values.min(),
            "mean_deployed_return": complete.deployed_return.mean(),
            "median_deployed_return": complete.deployed_return.median(),
            "split_date": split_date.date().isoformat(),
            "early_count": len(early),
            "late_count": len(late),
            "early_median_return": early.median() if not early.empty else np.nan,
            "late_median_return": late.median() if not late.empty else np.nan,
            "min_half_median_return": min(early.median(), late.median()) if not early.empty and not late.empty else np.nan,
            "half_median_gap": abs(early.median() - late.median()) if not early.empty and not late.empty else np.nan,
        })
    summary = pd.DataFrame(summary_rows).sort_values(["median_portfolio_return", "mean_portfolio_return"], ascending=False).reset_index(drop=True)
    summary.insert(0, "rank_by_median", range(1, len(summary) + 1))
    summary["rank_by_stable_half_median"] = summary["min_half_median_return"].rank(method="min", ascending=False).astype("Int64")
    summary.to_csv(args.output_dir / "strategy_comparison.csv", index=False)

    top = summary.head(12).copy()
    fig, ax = plt.subplots(figsize=(15, 7))
    x = np.arange(len(top))
    width = 0.38
    ax.bar(x - width / 2, top.median_portfolio_return * 100, width, label="median portfolio return")
    ax.bar(x + width / 2, top.mean_portfolio_return * 100, width, label="mean portfolio return")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(top.strategy, rotation=70, ha="right", fontsize=7)
    ax.set_ylabel("Return (%)")
    ax.set_title("Two-wave strategy comparison: top 12 by median return")
    ax.legend()
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(args.output_dir / "strategy_comparison.png", dpi=160)
    plt.close(fig)

    report = [
        "# 二波交易策略比较实验",
        "",
        f"- 二波事件：{len(setups)} 个；策略版本：{len(specs)} 个。",
        f"- 名义窗口：{nominal_start.date()} 至 {latest.date()}；结果是同一事件集上的样本内比较。",
        f"- 二波回调条件：价格回调达到 {DEFAULTS['min_pullback'] * 100:.0f}% 或 J 下降达到 {DEFAULTS['min_j_pullback_drop']:.0f}，且价格回调不超过 {DEFAULTS['max_pullback'] * 100:.0f}%。",
        "- 该实验用于寻找当前样本中的候选高收益规则，不代表样本外最优；策略参数排名存在过拟合风险。",
        "- `portfolio_return` 按总资金计算；分批策略未使用的资金保留现金。",
        "- 未计手续费、滑点、税费；重叠事件未模拟账户持仓冲突。",
        "",
        "## 推荐排名",
        "",
        "排名按完整交易的组合收益率中位数，其次按平均值；同时查看胜率和最小单笔收益，避免只看均值。",
        "",
    ]
    cols = ["rank_by_median", "strategy", "complete_count", "median_portfolio_return", "mean_portfolio_return", "win_rate", "min_return", "median_deployed_return"]
    top_display = top[cols].copy()
    for col in ["median_portfolio_return", "mean_portfolio_return", "win_rate", "min_return", "median_deployed_return"]:
        top_display[col] = top_display[col].map(lambda value: f"{value * 100:.2f}%")
    report.append(dataframe_markdown(top_display))
    stable = summary.sort_values(["min_half_median_return", "half_median_gap"], ascending=[False, True]).head(10)
    stable_display = stable[["rank_by_stable_half_median", "strategy", "early_count", "late_count", "early_median_return", "late_median_return", "min_half_median_return", "half_median_gap"]].copy()
    for col in ["early_median_return", "late_median_return", "min_half_median_return", "half_median_gap"]:
        stable_display[col] = stable_display[col].map(lambda value: "" if pd.isna(value) else f"{value * 100:.2f}%")
    report.extend([
        "",
        "## 前后半段稳健性排名",
        "",
        "按前后两个时间段中较低的收益中位数排序，作为比样本内最高值更保守的参考：",
        "",
    ])
    report.append(dataframe_markdown(stable_display))
    report.extend([
        "",
        "## 文件",
        "",
        "- 全部策略排名：`strategy_comparison.csv`",
        "- 全部逐笔交易：`strategy_trade_details.csv`",
        "- 前 12 策略均值/中位数图：`strategy_comparison.png`",
        "- 稳健性字段：`early_median_return`、`late_median_return`、`min_half_median_return`；前后半段分界日见 `split_date`。",
        "",
        "## 解释",
        "",
        "`j_first` 是当前实现的首个有效低 J；`j_continue` 会跳过第一个不满足周期/结构条件的低 J，继续找后续有效 J；`j_upturn` 额外要求 J 当日高于前一日。`breakout` 不使用 J，等第二顶部上方 2% 的收盘突破后次日开盘进入。",
        "",
        "固定持有策略用于回答“持有多久更合适”；`target_top`/`target_breakout` 用价格目标退出；`ma5_trail` 用收盘跌破 5 日均线退出；`bullish_partial` 使用中阳线分批止盈。",
        "",
        "最高收益策略只代表当前事件样本内的排名。正式采用前，应使用时间切分样本外验证，并优先关注中位数、最小收益和最大亏损，而不是只选平均收益最高的一行。",
    ])
    (args.output_dir / "strategy_comparison_report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    print(summary.head(10).to_string(index=False))
    print(f"wrote {args.output_dir}: specs={len(specs)} trades={len(trade_df)}")


def dataframe_markdown(frame: pd.DataFrame) -> str:
    columns = list(frame.columns)
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in frame.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


if __name__ == "__main__":
    run()
