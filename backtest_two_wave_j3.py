from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from scan_three_wave import (
    NARROW_DEFAULTS,
    _compute_j,
    _merge_platforms,
    _raw_pivots,
    _refine_low_starts,
)


J_THRESHOLD = 15.0
J_QUANTILE = 0.10
J_WINDOW = 120


@dataclass(frozen=True)
class TwoWaveSetup:
    code: str
    start1: int
    top1: int
    start2: int
    top2: int
    j_entry: int | None
    j_value: float | None
    first_wave_period: int
    second_wave_period: int | None
    first_wave_return: float
    second_wave_return: float
    first_pullback: float
    first_j_pullback_drop: float
    second_pullback: float | None
    second_j_pullback_drop_to_j: float | None
    first_wave_efficiency: float
    second_wave_efficiency: float | None
    third_wave_reached_top: bool | None
    third_wave_breakout: bool | None
    third_wave_observed_bars: int
    third_wave_censored: bool


@dataclass(frozen=True)
class Fill:
    name: str
    signal_idx: int
    exec_idx: int
    price: float
    allocation: float


@dataclass(frozen=True)
class TradeResult:
    setup: TwoWaveSetup
    buy1: Fill
    buy2: Fill | None
    buy3: Fill | None
    partial_exit_idx: int | None
    partial_exit_price: float | None
    final_exit_idx: int
    final_exit_price: float
    exit_reason: str
    portfolio_return: float
    deployed_return: float
    deployed_fraction: float
    average_entry_price: float
    medium_bull_count: int
    bullish_after_partial: int
    trade_censored: bool


DEFAULTS = {
    "min_wave_bars": 2,
    "max_wave_bars": 20,
    "min_complete_wave_period": 7,
    "max_complete_wave_period": 25,
    "min_up_return": 0.06,
    "min_pullback": 0.04,
    "min_j_pullback_drop": 30.0,
    "max_pullback": 0.28,
    "min_step": 0.02,
    "min_path_efficiency": 0.58,
    "min_complete_wave_path_efficiency": 0.10,
    "max_up_adverse": 0.12,
    "max_down_adverse": 0.12,
    "j_entry_max_period": 25,
    "third_wave_horizon": 25,
    "third_wave_breakout_pct": 0.02,
    "build_window": 10,
    "buy3_tolerance": 0.02,
    "sideways_days": 10,
    "sideways_range_pct": 0.08,
    "medium_bull_pct": 0.03,
    "medium_bull_body_ratio": 0.50,
    "fallback_days": 5,
}


def load_frames(data_dir: Path) -> list[pd.DataFrame]:
    required = {"date", "open", "close", "high", "low", "volume"}
    frames: list[pd.DataFrame] = []
    for path in sorted(data_dir.glob("*.csv")):
        try:
            frame = pd.read_csv(path, parse_dates=["date"])
        except Exception:
            continue
        if not required.issubset(frame.columns):
            continue
        frame = (
            frame.dropna(subset=list(required))
            .sort_values("date")
            .drop_duplicates("date")
            .reset_index(drop=True)
        )
        if frame.empty:
            continue
        for column in ["open", "close", "high", "low", "volume"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna(subset=["open", "close", "high", "low"]).reset_index(drop=True)
        frame["code"] = path.stem
        frames.append(frame)
    return frames


def is_small_j(j_values: pd.Series, index: int) -> bool:
    current = float(j_values.iloc[index])
    start = max(0, index - J_WINDOW + 1)
    quantile = float(j_values.iloc[start : index + 1].quantile(J_QUANTILE))
    return current < J_THRESHOLD or current <= quantile


def path_efficiency(frame: pd.DataFrame, start: int, end: int) -> float:
    segment = frame.iloc[start : end + 1]
    changes = segment["close"].diff().dropna()
    total_path = float(changes.abs().sum())
    if total_path == 0:
        return 0.0
    return abs(float(segment["close"].iloc[-1] - segment["close"].iloc[0])) / total_path


def adverse_excursion(frame: pd.DataFrame, start: int, end: int, direction: str) -> float:
    segment = frame.iloc[start : end + 1]
    if direction == "up":
        return float(((segment["close"].cummax() - segment["close"]) / segment["close"].cummax()).max())
    return float(((segment["close"] - segment["close"].cummin()) / segment["close"].cummin()).max())


def validate_two_wave_structure(
    frame: pd.DataFrame,
    start1: int,
    top1: int,
    start2: int,
    top2: int,
    cfg: dict[str, float | int],
) -> bool:
    widths = [top1 - start1, start2 - top1, top2 - start2]
    if any(width < int(cfg["min_wave_bars"]) or width > int(cfg["max_wave_bars"]) for width in widths):
        return False

    wave1_period = start2 - start1
    if not int(cfg["min_complete_wave_period"]) <= wave1_period <= int(cfg["max_complete_wave_period"]):
        return False

    close = frame["close"]
    start1_price = float(close.iloc[start1])
    top1_price = float(close.iloc[top1])
    start2_price = float(close.iloc[start2])
    top2_price = float(close.iloc[top2])
    j_values = _compute_j(frame)
    j_pullback_drop = float(j_values.iloc[top1] - j_values.iloc[start2])
    wave1_return = top1_price / start1_price - 1.0
    wave2_return = top2_price / start2_price - 1.0
    pullback1 = (top1_price - start2_price) / top1_price
    high_step = top2_price / top1_price - 1.0
    low_step = start2_price / start1_price - 1.0

    if wave1_return < float(cfg["min_up_return"]) or wave2_return < float(cfg["min_up_return"]):
        return False
    price_pullback_valid = float(cfg["min_pullback"]) <= pullback1 <= float(cfg["max_pullback"])
    j_pullback_valid = j_pullback_drop >= float(cfg["min_j_pullback_drop"])
    if pullback1 > float(cfg["max_pullback"]) or not (price_pullback_valid or j_pullback_valid):
        return False
    if high_step < float(cfg["min_step"]) or low_step < float(cfg["min_step"]):
        return False

    if path_efficiency(frame, start1, start2) < float(cfg["min_complete_wave_path_efficiency"]):
        return False
    if path_efficiency(frame, start1, top1) < float(cfg["min_path_efficiency"]):
        return False
    if path_efficiency(frame, start2, top2) < float(cfg["min_path_efficiency"]):
        return False
    if adverse_excursion(frame, start1, top1, "up") > float(cfg["max_up_adverse"]):
        return False
    if adverse_excursion(frame, start2, top2, "up") > float(cfg["max_up_adverse"]):
        return False
    if adverse_excursion(frame, top1, start2, "down") > float(cfg["max_down_adverse"]):
        return False
    return True


def find_first_j_entry(
    frame: pd.DataFrame,
    j_values: pd.Series,
    start2: int,
    top2: int,
    cfg: dict[str, float | int],
) -> tuple[int | None, float | None, int | None, float | None]:
    first_signal: tuple[int, float] | None = None
    search_end = min(len(frame) - 1, start2 + int(cfg["j_entry_max_period"]))
    for index in range(top2 + 1, search_end + 1):
        if not is_small_j(j_values, index):
            continue
        if first_signal is None:
            first_signal = (index, float(j_values.iloc[index]))
        period = index - start2
        close = float(frame["close"].iloc[index])
        top2_close = float(frame["close"].iloc[top2])
        start2_close = float(frame["close"].iloc[start2])
        pullback2 = (top2_close - close) / top2_close
        valid_period = int(cfg["min_complete_wave_period"]) <= period <= int(cfg["max_complete_wave_period"])
        valid_trend = close >= start2_close * (1.0 + float(cfg["min_step"]))
        j_pullback_drop = float(j_values.iloc[top2] - j_values.iloc[index])
        price_pullback_valid = float(cfg["min_pullback"]) <= pullback2 <= float(cfg["max_pullback"])
        j_pullback_valid = j_pullback_drop >= float(cfg["min_j_pullback_drop"])
        valid_pullback = pullback2 <= float(cfg["max_pullback"]) and (price_pullback_valid or j_pullback_valid)
        valid_efficiency = path_efficiency(frame, start2, index) >= float(cfg["min_complete_wave_path_efficiency"])
        if valid_period and valid_trend and valid_pullback and valid_efficiency:
            return index, float(j_values.iloc[index]), first_signal[0], first_signal[1]
    return None, None, None if first_signal is None else first_signal[0], None if first_signal is None else first_signal[1]


def evaluate_third_wave(
    frame: pd.DataFrame,
    top2: int,
    entry_idx: int | None,
    cfg: dict[str, float | int],
) -> tuple[bool | None, bool | None, int, bool]:
    start = (entry_idx + 1) if entry_idx is not None else (top2 + 1)
    end = min(len(frame) - 1, start + int(cfg["third_wave_horizon"]))
    observed = frame.iloc[start : end + 1]
    if observed.empty:
        return None, None, 0, True
    top_price = float(frame["close"].iloc[top2])
    max_high = float(observed["high"].max())
    reached = max_high >= top_price
    breakout = max_high >= top_price * (1.0 + float(cfg["third_wave_breakout_pct"]))
    censored = end - start + 1 < int(cfg["third_wave_horizon"])
    return reached, breakout, len(observed), censored


def next_open_fill(frame: pd.DataFrame, signal_idx: int, name: str, allocation: float) -> Fill | None:
    execution_idx = signal_idx + 1
    if execution_idx >= len(frame):
        return None
    price = float(frame["open"].iloc[execution_idx])
    if price <= 0:
        return None
    return Fill(name, signal_idx, execution_idx, price, allocation)


def is_medium_bullish(row: pd.Series, cfg: dict[str, float | int]) -> bool:
    if float(row["close"]) <= float(row["open"]):
        return False
    body = float(row["close"] - row["open"])
    candle_range = float(row["high"] - row["low"])
    if float(row["close"] / row["open"] - 1.0) < float(cfg["medium_bull_pct"]):
        return False
    return candle_range > 0 and body / candle_range >= float(cfg["medium_bull_body_ratio"])


def is_bullish(row: pd.Series) -> bool:
    return float(row["close"]) > float(row["open"])


def is_sideways(frame: pd.DataFrame, end_idx: int, days: int, max_range: float) -> bool:
    start_idx = end_idx - days + 1
    if start_idx < 0:
        return False
    closes = frame["close"].iloc[start_idx : end_idx + 1]
    if len(closes) < days or float(closes.min()) <= 0:
        return False
    return float(closes.max() / closes.min() - 1.0) <= max_range


def simulate_trade(
    frame: pd.DataFrame,
    setup: TwoWaveSetup,
    cfg: dict[str, float | int],
) -> TradeResult | None:
    if setup.j_entry is None:
        return None

    buy1 = next_open_fill(frame, setup.j_entry, "buy1", 1.0 / 3.0)
    if buy1 is None:
        return None

    buy2: Fill | None = None
    buy3: Fill | None = None
    first_check = buy1.exec_idx + 2
    build_end = min(len(frame) - 1, buy1.exec_idx + int(cfg["build_window"]))
    if first_check <= build_end and first_check + 1 < len(frame):
        previous_close = float(frame["close"].iloc[first_check - 1])
        check_close = float(frame["close"].iloc[first_check])
        if check_close < previous_close and check_close < buy1.price:
            buy2 = next_open_fill(frame, first_check, "buy2", 1.0 / 3.0)

    if buy2 is not None:
        rebound_start = buy2.exec_idx + 1
        rebound_end = min(len(frame) - 1, buy1.exec_idx + int(cfg["build_window"]))
        for index in range(rebound_start, rebound_end + 1):
            close = float(frame["close"].iloc[index])
            near_buy1 = abs(close / buy1.price - 1.0) <= float(cfg["buy3_tolerance"])
            rising_from_buy2 = close > buy2.price
            if near_buy1 and rising_from_buy2:
                buy3 = next_open_fill(frame, index, "buy3", 1.0 / 3.0)
                if buy3 is not None:
                    break

    fills = [buy1] + ([buy2] if buy2 is not None else []) + ([buy3] if buy3 is not None else [])
    last_buy = max(fills, key=lambda fill: fill.exec_idx)
    partial_exit_idx: int | None = None
    partial_exit_price: float | None = None
    final_exit_idx: int | None = None
    final_exit_price: float | None = None
    exit_reason = ""
    medium_count = 0
    bullish_after_partial = 0
    partial_signal_idx: int | None = None
    last_buy_idx = last_buy.exec_idx
    horizon_end = min(len(frame) - 1, last_buy_idx + int(cfg["sideways_days"]))

    for index in range(last_buy_idx + 1, horizon_end + 1):
        row = frame.iloc[index]
        if not partial_exit_idx and is_medium_bullish(row, cfg):
            medium_count += 1
        if partial_exit_idx is None and medium_count >= 3:
            partial_signal_idx = index
            partial_fill = next_open_fill(frame, index, "take_half", 0.0)
            if partial_fill is not None:
                partial_exit_idx = partial_fill.exec_idx
                partial_exit_price = partial_fill.price
                continue

        if partial_exit_idx is not None and index > partial_signal_idx and is_bullish(row):
            bullish_after_partial += 1
            if bullish_after_partial >= 2:
                final_fill = next_open_fill(frame, index, "take_rest", 0.0)
                if final_fill is not None:
                    final_exit_idx = final_fill.exec_idx
                    final_exit_price = final_fill.price
                    exit_reason = "3_medium_bull_half_then_2_bull_full"
                    break

        if partial_exit_idx is not None and index - last_buy_idx >= int(cfg["sideways_days"]):
            if is_sideways(frame, index, int(cfg["sideways_days"]), float(cfg["sideways_range_pct"])):
                final_fill = next_open_fill(frame, index, "sideways_exit", 0.0)
                if final_fill is not None:
                    final_exit_idx = final_fill.exec_idx
                    final_exit_price = final_fill.price
                    exit_reason = "sideways_10d"
                    break

        if partial_exit_idx is None and index - last_buy_idx >= int(cfg["fallback_days"]):
            final_fill = next_open_fill(frame, index, "fallback_exit", 0.0)
            if final_fill is not None:
                final_exit_idx = final_fill.exec_idx
                final_exit_price = final_fill.price
                exit_reason = "fallback_5d"
                break

    trade_censored = False
    if final_exit_idx is None:
        if partial_exit_idx is not None:
            final_signal = min(len(frame) - 1, last_buy_idx + int(cfg["sideways_days"]))
            final_fill = next_open_fill(frame, final_signal, "post_partial_timeout", 0.0)
            exit_reason = "post_partial_10d_timeout"
        else:
            final_signal = min(len(frame) - 1, last_buy_idx + int(cfg["fallback_days"]))
            final_fill = next_open_fill(frame, final_signal, "data_end_or_fallback", 0.0)
            exit_reason = "data_end_or_fallback"
        if final_fill is None:
            final_exit_idx = len(frame) - 1
            final_exit_price = float(frame["close"].iloc[-1])
            exit_reason = "data_end_close"
            trade_censored = True
        else:
            final_exit_idx = final_fill.exec_idx
            final_exit_price = final_fill.price

    shares = 0.0
    invested = 0.0
    for fill in fills:
        shares += fill.allocation / fill.price
        invested += fill.allocation
    cash = 1.0 - invested
    if partial_exit_idx is not None and partial_exit_price is not None:
        sold_shares = shares / 2.0
        cash += sold_shares * partial_exit_price
        shares -= sold_shares
    cash += shares * float(final_exit_price)
    portfolio_return = cash - 1.0
    deployed_return = portfolio_return / invested if invested > 0 else 0.0
    average_entry = invested / sum(fill.allocation / fill.price for fill in fills)

    return TradeResult(
        setup=setup,
        buy1=buy1,
        buy2=buy2,
        buy3=buy3,
        partial_exit_idx=partial_exit_idx,
        partial_exit_price=partial_exit_price,
        final_exit_idx=int(final_exit_idx),
        final_exit_price=float(final_exit_price),
        exit_reason=exit_reason,
        portfolio_return=portfolio_return,
        deployed_return=deployed_return,
        deployed_fraction=invested,
        average_entry_price=average_entry,
        medium_bull_count=medium_count,
        bullish_after_partial=bullish_after_partial,
        trade_censored=trade_censored,
    )


def find_setups(frame: pd.DataFrame, cfg: dict[str, float | int]) -> list[TwoWaveSetup]:
    pivots = _merge_platforms(frame, _raw_pivots(frame, NARROW_DEFAULTS), NARROW_DEFAULTS)
    pivots = _refine_low_starts(frame, pivots, NARROW_DEFAULTS)
    j_values = _compute_j(frame)
    setups: list[TwoWaveSetup] = []
    seen: set[tuple[int, int, int, int]] = set()

    for index in range(len(pivots) - 3):
        sequence = pivots[index : index + 4]
        if [pivot.kind for pivot in sequence] != ["L", "H", "L", "H"]:
            continue
        start1, top1, start2, top2 = sequence
        key = (start1.start, top1.start, start2.start, top2.start)
        if key in seen:
            continue
        seen.add(key)
        if not validate_two_wave_structure(frame, start1.start, top1.start, start2.start, top2.start, cfg):
            continue

        entry_idx, entry_j, first_signal_idx, first_signal_j = find_first_j_entry(
            frame, j_values, start2.start, top2.end, cfg
        )
        third_reached, third_breakout, observed, censored = evaluate_third_wave(frame, top2.start, entry_idx, cfg)
        second_period = entry_idx - start2.start if entry_idx is not None else None
        second_return = None if entry_idx is None else float(frame["close"].iloc[top2.start] / frame["close"].iloc[start2.start] - 1.0)
        second_pullback = None if entry_idx is None else float((frame["close"].iloc[top2.start] - frame["close"].iloc[entry_idx]) / frame["close"].iloc[top2.start])
        first_j_pullback_drop = float(j_values.iloc[top1.start] - j_values.iloc[start2.start])
        second_j_pullback_drop = None if entry_idx is None else float(j_values.iloc[top2.start] - j_values.iloc[entry_idx])
        setups.append(
            TwoWaveSetup(
                code=str(frame["code"].iloc[0]),
                start1=start1.start,
                top1=top1.start,
                start2=start2.start,
                top2=top2.start,
                j_entry=entry_idx,
                j_value=entry_j if entry_idx is not None else first_signal_j,
                first_wave_period=start2.start - start1.start,
                second_wave_period=second_period,
                first_wave_return=float(frame["close"].iloc[top1.start] / frame["close"].iloc[start1.start] - 1.0),
                second_wave_return=second_return,
                first_pullback=float((frame["close"].iloc[top1.start] - frame["close"].iloc[start2.start]) / frame["close"].iloc[top1.start]),
                first_j_pullback_drop=first_j_pullback_drop,
                second_pullback=second_pullback,
                second_j_pullback_drop_to_j=second_j_pullback_drop,
                first_wave_efficiency=path_efficiency(frame, start1.start, start2.start),
                second_wave_efficiency=None if entry_idx is None else path_efficiency(frame, start2.start, entry_idx),
                third_wave_reached_top=third_reached,
                third_wave_breakout=third_breakout,
                third_wave_observed_bars=observed,
                third_wave_censored=censored,
            )
        )
    return setups


def date_text(frame: pd.DataFrame, index: int | None) -> str:
    if index is None:
        return ""
    return pd.Timestamp(frame["date"].iloc[index]).date().isoformat()


def setup_row(frame: pd.DataFrame, setup: TwoWaveSetup) -> dict[str, Any]:
    return {
        "code": setup.code,
        "start1_date": date_text(frame, setup.start1),
        "top1_date": date_text(frame, setup.top1),
        "start2_date": date_text(frame, setup.start2),
        "top2_date": date_text(frame, setup.top2),
        "j_entry_date": date_text(frame, setup.j_entry),
        "j_value": setup.j_value,
        "first_wave_period": setup.first_wave_period,
        "second_wave_period_to_j": setup.second_wave_period,
        "first_wave_return": setup.first_wave_return,
        "second_wave_return": setup.second_wave_return,
        "first_pullback": setup.first_pullback,
        "first_j_pullback_drop": setup.first_j_pullback_drop,
        "second_pullback_to_j": setup.second_pullback,
        "second_j_pullback_drop_to_j": setup.second_j_pullback_drop_to_j,
        "first_wave_efficiency": setup.first_wave_efficiency,
        "second_wave_efficiency_to_j": setup.second_wave_efficiency,
        "third_wave_reached_top": setup.third_wave_reached_top,
        "third_wave_breakout": setup.third_wave_breakout,
        "third_wave_observed_bars": setup.third_wave_observed_bars,
        "third_wave_censored": setup.third_wave_censored,
        "has_j_entry": setup.j_entry is not None,
    }


def trade_row(trade: TradeResult, frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "code": trade.setup.code,
        "j_signal_date": date_text(frame, trade.setup.j_entry),
        "buy1_date": date_text(frame, trade.buy1.exec_idx),
        "buy1_price": trade.buy1.price,
        "buy2_date": date_text(frame, trade.buy2.exec_idx) if trade.buy2 else "",
        "buy2_price": trade.buy2.price if trade.buy2 else np.nan,
        "buy3_date": date_text(frame, trade.buy3.exec_idx) if trade.buy3 else "",
        "buy3_price": trade.buy3.price if trade.buy3 else np.nan,
        "buy_count": len([fill for fill in [trade.buy1, trade.buy2, trade.buy3] if fill is not None]),
        "average_entry_price": trade.average_entry_price,
        "partial_exit_date": date_text(frame, trade.partial_exit_idx),
        "partial_exit_price": trade.partial_exit_price,
        "final_exit_date": date_text(frame, trade.final_exit_idx),
        "final_exit_price": trade.final_exit_price,
        "exit_reason": trade.exit_reason,
        "portfolio_return": trade.portfolio_return,
        "deployed_return": trade.deployed_return,
        "deployed_fraction": trade.deployed_fraction,
        "medium_bull_count": trade.medium_bull_count,
        "bullish_after_partial": trade.bullish_after_partial,
        "trade_censored": trade.trade_censored,
        "wave1_start": date_text(frame, trade.setup.start1),
        "wave2_start": date_text(frame, trade.setup.start2),
        "wave2_top": date_text(frame, trade.setup.top2),
        "wave3_predicted_start": date_text(frame, trade.setup.j_entry),
    }


def dataframe_to_markdown(frame: pd.DataFrame, columns: list[str]) -> str:
    selected = frame[columns].copy()

    def format_value(value: object) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, float):
            return f"{value:.4f}"
        return str(value)

    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in selected.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(format_value(value) for value in row) + " |")
    return "\n".join(lines)


def summarize(setups: list[TwoWaveSetup], trades: list[TradeResult]) -> pd.DataFrame:
    total = len(setups)
    entry_setups = [setup for setup in setups if setup.j_entry is not None]
    observed = [setup for setup in setups if not setup.third_wave_censored]
    observed_entry = [setup for setup in entry_setups if not setup.third_wave_censored]
    reach_h2_count = sum(bool(setup.third_wave_reached_top) for setup in observed)
    breakout_count = sum(bool(setup.third_wave_breakout) for setup in observed)
    breakout_entry_count = sum(bool(setup.third_wave_breakout) for setup in observed_entry)
    complete_trades = [trade for trade in trades if not trade.trade_censored]
    trade_returns = [trade.portfolio_return for trade in complete_trades]
    rows = [
        ("two_wave_patterns", total),
        ("two_wave_code_count", len({setup.code for setup in setups})),
        ("min_pullback_parameter", float(DEFAULTS["min_pullback"])),
        ("min_j_pullback_drop_parameter", float(DEFAULTS["min_j_pullback_drop"])),
        ("patterns_with_j_entry", len(entry_setups)),
        ("j_entry_rate_all_patterns", len(entry_setups) / total if total else np.nan),
        ("third_wave_reach_h2_rate_observed", np.mean([bool(s.third_wave_reached_top) for s in observed]) if observed else np.nan),
        ("third_wave_breakout_rate_observed", np.mean([bool(s.third_wave_breakout) for s in observed]) if observed else np.nan),
        ("third_wave_breakout_rate_with_j_entry", np.mean([bool(s.third_wave_breakout) for s in observed_entry]) if observed_entry else np.nan),
        ("observed_pattern_count", len(observed)),
        ("third_wave_reach_h2_count_observed", reach_h2_count),
        ("third_wave_breakout_count_observed", breakout_count),
        ("observed_j_entry_pattern_count", len(observed_entry)),
        ("third_wave_breakout_count_with_j_entry", breakout_entry_count),
        ("censored_pattern_count", total - len(observed)),
        ("trade_count", len(trades)),
        ("complete_trade_count", len(complete_trades)),
        ("censored_trade_count", len(trades) - len(complete_trades)),
        ("trade_win_rate", np.mean([value > 0 for value in trade_returns]) if trade_returns else np.nan),
        ("trade_mean_portfolio_return", np.mean(trade_returns) if trade_returns else np.nan),
        ("trade_median_portfolio_return", np.median(trade_returns) if trade_returns else np.nan),
        ("trade_mean_deployed_return", np.mean([trade.deployed_return for trade in complete_trades]) if complete_trades else np.nan),
        ("trade_median_deployed_return", np.median([trade.deployed_return for trade in complete_trades]) if complete_trades else np.nan),
    ]
    return pd.DataFrame(rows, columns=["metric", "value"])


def plot_returns(trades: list[TradeResult], frame_map: dict[str, pd.DataFrame], output: Path) -> None:
    values = [trade.portfolio_return * 100.0 for trade in trades if not trade.trade_censored]
    trades = [trade for trade in trades if not trade.trade_censored]
    labels = [f"{trade.setup.code}\n{date_text(frame_map[trade.setup.code], trade.setup.j_entry)}" for trade in trades]
    fig, ax = plt.subplots(figsize=(14, 6))
    if values:
        ax.plot(range(1, len(values) + 1), values, marker="o", linewidth=1.4)
        ax.axhline(0.0, color="black", linewidth=0.8)
        ax.set_xticks(range(1, len(values) + 1))
        ax.set_xticklabels(labels, rotation=75, ha="right", fontsize=7)
    ax.set_title("Two-wave J-entry backtest: return per trade")
    ax.set_xlabel("Trade sequence")
    ax.set_ylabel("Portfolio return (%)")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(output, dpi=160)
    plt.close(fig)


def write_report(
    output: Path,
    setups: list[TwoWaveSetup],
    trades: list[TradeResult],
    summary: pd.DataFrame,
    frame_map: dict[str, pd.DataFrame],
    nominal_start: pd.Timestamp,
    latest: pd.Timestamp,
    cfg: dict[str, float | int],
) -> None:
    actual_start = min(frame["date"].min() for frame in frame_map.values())
    actual_end = max(frame["date"].max() for frame in frame_map.values())
    lines = [
        "# 二波 + J 低位三起回测",
        "",
        "## 回测口径",
        "",
        "- 数据源：当前 `data/us_stocks` 本地数据库；每只股票使用其全部可用日线。",
        f"- 名义回测窗口：`{nominal_start.date()}` 至 `{latest.date()}`；当前文件实际可用日期范围：`{actual_start.date()}` 至 `{actual_end.date()}`。",
        "- 结构识别：用完整历史窗口回看确认局部拐点和二波结构；买入与卖出从 J 信号之后按次日开盘执行。因此这是结构事件研究，不是完全无未来信息的实时扫描回测。",
        "- 事件样本：相邻二波事件可能重叠；本报告统计逐笔事件，不模拟同一账户的持仓冲突、资金占用和组合净值。",
        "- 成本口径：暂未计入手续费、滑点、税费和涨跌停/流动性影响。",
        "- 二波结构：`1起 -> 第一顶部 -> 2起 -> 第二顶部`。",
        f"- 当前二波回调条件：价格回调达到 `{cfg['min_pullback'] * 100:.0f}%`，或从顶部到起点的 J 下降达到 `{cfg['min_j_pullback_drop']:.0f}`；价格回调仍不得超过 `{cfg['max_pullback'] * 100:.0f}%`。",
        "- `3起`：第二顶部之后第一个满足少妇战法 J 小值条件、且使 `2起 -> 3起` 周期正常的交易日；第二波回调同样允许价格回调或 J 显著下降满足其一。",
        "- 少妇战法 J 条件：`J < 15` 或 `J <= 最近 120 根 J 的 10% 分位数`。",
        "- 买入执行：J 信号日收盘确认，下一交易日开盘买入；每个买入计划投入总资金的 1/3。",
        "- 买2：买1执行日后的第 2、4、6... 个交易日检查；首次收盘继续下跌才补买，首次不跌则不再补买。",
        "- 买3：买2后价格上涨，并回到买1价格 ±2% 内，下一交易日开盘补买。",
        "- 中阳线：阳线实体涨幅 >= 3%，且实体占当日振幅 >= 50%。",
        "- 退出：10 个交易日内第 3 根中阳线卖出一半，之后再出现 2 根阳线卖出剩余；未触发则第 5 个交易日兜底退出；若已部分止盈，10 日时横盘或超时退出剩余。",
        "- 横盘：最近 10 个交易日收盘价最高/最低之比不超过 8%。",
        "- 第三浪确认：后续 25 个可观察交易日内最高价达到第二顶部为回收，超过第二顶部 2% 为突破。数据不足的事件单独标记为 censored，不计入概率分母。",
        "",
        "## 汇总",
        "",
    ]
    for row in summary.itertuples(index=False):
        value = row.value
        if isinstance(value, float) and not np.isnan(value):
            if "rate" in row.metric or "return" in row.metric:
                value = f"{value * 100:.2f}%"
            else:
                value = f"{value:.4f}"
        lines.append(f"- `{row.metric}`: {value}")
    lines.extend(
        [
            "",
            "## 交易明细",
            "",
            "交易明细写入 `two_wave_trades.csv`；二波事件全集写入 `two_wave_events.csv`。收益折线图写入 `two_wave_trade_returns.png`。",
            "",
        ]
    )
    if trades:
        trade_df = pd.DataFrame([trade_row(trade, frame_map[trade.setup.code]) for trade in trades])
        display_columns = ["code", "j_signal_date", "buy1_date", "buy2_date", "buy3_date", "buy_count", "average_entry_price", "final_exit_date", "exit_reason", "portfolio_return", "deployed_return"]
        lines.append(dataframe_to_markdown(trade_df, display_columns))
    else:
        lines.append("没有满足 J 买点并完成可执行交易的事件。")
    lines.extend(
        [
            "",
            "## 解释",
            "",
            "`portfolio_return` 按计划总资金计算，未执行的分批资金留在现金中；`deployed_return` 只按已经实际买入的仓位计算。默认报告和收益折线图使用完整退出交易的 `portfolio_return`。靠近数据尾部、没有完成退出周期的交易保留在明细中，但不计入收益统计。",
            "由于本回测按第 5 个交易日兜底退出，未在前 5 日触发三中阳的交易不会继续等待到第 10 日，因此 10 日横盘条件只可能出现在已经触发半仓止盈、仍持有剩余仓位的交易中。这个优先级是对原始规则矛盾处的明确解释，后续可以另做延迟兜底的敏感性回测。",
            "",
            "第三浪概率同时给出两种口径：所有已完成观察的二波事件中的突破率，以及有 J 买点事件中的突破率。靠近数据尾部、未来观察不足 25 个交易日的事件标记为 censored，避免把尚未走完的第三浪当成失败。",
        ]
    )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest two-wave J-low third-wave entries")
    parser.add_argument("--data-dir", type=Path, default=Path("./data/us_stocks"))
    parser.add_argument("--output-dir", type=Path, default=Path("./backtest_two_wave_j3_20260820"))
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    frames = load_frames(args.data_dir)
    if not frames:
        raise SystemExit("no usable OHLCV files")
    latest = max(frame["date"].max() for frame in frames)
    start = latest - pd.Timedelta(days=365)
    setups: list[TwoWaveSetup] = []
    frame_map: dict[str, pd.DataFrame] = {}
    for frame in frames:
        window = frame[(frame["date"] >= start) & (frame["date"] <= latest)].reset_index(drop=True)
        if len(window) < 30:
            continue
        frame_map[str(window["code"].iloc[0])] = window
        setups.extend(find_setups(window, DEFAULTS))

    trades = [trade for setup in setups if setup.j_entry is not None for trade in [simulate_trade(frame_map[setup.code], setup, DEFAULTS)] if trade is not None]
    event_df = pd.DataFrame([setup_row(frame_map[setup.code], setup) for setup in setups])
    trade_df = pd.DataFrame([trade_row(trade, frame_map[trade.setup.code]) for trade in trades])
    summary = summarize(setups, trades)
    event_df.to_csv(args.output_dir / "two_wave_events.csv", index=False)
    trade_df.to_csv(args.output_dir / "two_wave_trades.csv", index=False)
    summary.to_csv(args.output_dir / "two_wave_summary.csv", index=False)
    code_summary = (
        event_df.groupby("code", as_index=False)
        .agg(two_wave_count=("code", "size"))
        .sort_values(["two_wave_count", "code"], ascending=[False, True])
    )
    code_summary.to_csv(args.output_dir / "two_wave_code_summary.csv", index=False)
    plot_returns(trades, frame_map, args.output_dir / "two_wave_trade_returns.png")
    write_report(
        args.output_dir / "two_wave_backtest_report.md",
        setups,
        trades,
        summary,
        frame_map,
        start,
        latest,
        DEFAULTS,
    )
    print(json.dumps({"latest": str(latest.date()), "patterns": len(setups), "trades": len(trades), "output_dir": str(args.output_dir)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
