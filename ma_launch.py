from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


MA_LAUNCH_DEFAULTS: dict[str, float | int] = {
    "anchor_before_bars": 3,
    "anchor_after_bars": 5,
    "confirmation_window": 12,
    "compression_atr_max": 0.35,
    "compression_percent_max": 0.015,
    "compression_days": 2,
    "compression_lookback": 3,
    "ma60_flat_atr_min": -0.30,
    "expansion_atr_min": 0.80,
    "expansion_ratio_min": 2.50,
    "bullish_days": 2,
    "bullish_lookback": 3,
    "ma60_rising_days": 3,
    "ma60_rising_lookback": 5,
    "above_ma60_hold_days": 2,
    "breakout_lookback": 20,
    "minimum_advance": 0.06,
    "minimum_history_rows": 80,
}


def _empty_result(status: str, reason: str) -> dict[str, Any]:
    return {
        "ma_launch_passed": False,
        "ma_launch_status": status,
        "ma_launch_reasons": [reason],
    }


def evaluate_ma_launch(
    frame: pd.DataFrame,
    start_index: int,
    cfg: dict[str, float | int] | None = None,
) -> dict[str, Any]:
    config = dict(MA_LAUNCH_DEFAULTS)
    if cfg:
        config.update(cfg)

    minimum_rows = int(config["minimum_history_rows"])
    if frame.empty or start_index < 0 or start_index >= len(frame):
        return _empty_result("invalid_start", "波浪首起点无效")

    work = frame.sort_values("date").drop_duplicates("date").reset_index(drop=True).copy()
    required = {"date", "close", "high", "low"}
    if not required.issubset(work.columns):
        return _empty_result("missing_columns", "K线字段不足")
    if len(work) < minimum_rows or start_index < minimum_rows - 1:
        return _empty_result("insufficient_history", f"首起点前需要至少{minimum_rows}根K线")

    close = pd.to_numeric(work["close"], errors="coerce")
    high = pd.to_numeric(work["high"], errors="coerce")
    low = pd.to_numeric(work["low"], errors="coerce")
    if close.isna().any() or high.isna().any() or low.isna().any():
        return _empty_result("invalid_prices", "K线价格包含无效值")

    short_periods = (5, 8, 13, 20)
    for period in (*short_periods, 60):
        work[f"ma{period}"] = close.rolling(period, min_periods=period).mean()

    previous_close = close.shift(1)
    true_range = pd.concat(
        [high - low, (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    work["atr20"] = true_range.rolling(20, min_periods=20).mean()

    short_columns = [f"ma{period}" for period in short_periods]
    short_max = work[short_columns].max(axis=1)
    short_min = work[short_columns].min(axis=1)
    short_mean = work[short_columns].mean(axis=1)
    work["band_atr"] = (short_max - short_min) / work["atr20"].replace(0.0, np.nan)
    work["band_percent"] = (short_max - short_min) / short_mean.replace(0.0, np.nan)
    work["ma60_slope_atr"] = (
        (work["ma60"] - work["ma60"].shift(5)) / work["atr20"].replace(0.0, np.nan)
    )
    work["ma60_slope_percent"] = work["ma60"] / work["ma60"].shift(5) - 1.0

    compression = (
        (work["band_atr"] <= float(config["compression_atr_max"]))
        & (work["band_percent"] <= float(config["compression_percent_max"]))
        & (work["ma60_slope_atr"] >= float(config["ma60_flat_atr_min"]))
    )
    compression_confirmed = (
        compression.rolling(int(config["compression_lookback"]), min_periods=1).sum()
        >= int(config["compression_days"])
    )

    anchor_start = max(minimum_rows - 1, start_index - int(config["anchor_before_bars"]))
    anchor_end = min(len(work) - 1, start_index + int(config["anchor_after_bars"]))
    confirmed_indexes = [
        index for index in range(anchor_start, anchor_end + 1)
        if bool(compression_confirmed.iloc[index])
    ]
    if not confirmed_indexes:
        return _empty_result("no_compression", "首起点附近未出现短均线粘合")

    compression_candidates: set[int] = set()
    compression_lookback = int(config["compression_lookback"])
    for index in confirmed_indexes:
        for candidate_index in range(max(0, index - compression_lookback + 1), index + 1):
            if bool(compression.iloc[candidate_index]):
                compression_candidates.add(candidate_index)
    if not compression_candidates:
        return _empty_result("no_compression", "首起点附近未出现短均线粘合")
    compression_index = min(compression_candidates, key=lambda index: float(work["band_atr"].iloc[index]))

    bullish_order = (
        (work["ma5"] > work["ma8"])
        & (work["ma8"] > work["ma13"])
        & (work["ma13"] > work["ma20"])
    )
    bullish_confirmed = (
        bullish_order.rolling(int(config["bullish_lookback"]), min_periods=1).sum()
        >= int(config["bullish_days"])
    )
    expansion = (
        bullish_confirmed
        & (work["band_atr"] >= float(config["expansion_atr_min"]))
        & (
            work["band_atr"]
            >= float(config["expansion_ratio_min"]) * float(work["band_atr"].iloc[compression_index])
        )
    )
    ma60_rising = (
        (work["ma60"].diff() > 0.0)
        .rolling(int(config["ma60_rising_lookback"]), min_periods=1)
        .sum()
        >= int(config["ma60_rising_days"])
    ) & (work["ma60_slope_percent"] > 0.0)
    all_short_above_ma60 = short_min > work["ma60"]
    above_ma60_confirmed = (
        all_short_above_ma60.rolling(int(config["above_ma60_hold_days"]), min_periods=1).sum()
        >= int(config["above_ma60_hold_days"])
    )
    prior_high = close.shift(1).rolling(
        int(config["breakout_lookback"]),
        min_periods=int(config["breakout_lookback"]),
    ).max()
    breakout = close > prior_high

    search_start = compression_index + 1
    search_end = min(len(work) - 1, compression_index + int(config["confirmation_window"]))

    def first_index(mask: pd.Series) -> int | None:
        return next((index for index in range(search_start, search_end + 1) if bool(mask.iloc[index])), None)

    breakout_index = first_index(breakout)
    expansion_index = first_index(expansion)
    ma60_turn_index = first_index(ma60_rising)
    above_ma60_index = first_index(above_ma60_confirmed)
    stages = (breakout_index, expansion_index, ma60_turn_index, above_ma60_index)
    if any(index is None for index in stages):
        missing_labels = [
            label
            for label, index in zip(
                ("价格突破", "短均线发散", "MA60转升", "短均线站上MA60"),
                stages,
            )
            if index is None
        ]
        result = _empty_result("incomplete_confirmation", "、".join(missing_labels) + "未在确认窗口内完成")
        result["ma_launch_compression_date"] = pd.Timestamp(work["date"].iloc[compression_index]).date().isoformat()
        return result

    activation_index = max(index for index in stages if index is not None)
    advance = float(close.iloc[activation_index] / close.iloc[compression_index] - 1.0)
    if advance < float(config["minimum_advance"]):
        result = _empty_result("insufficient_advance", "确认时涨幅不足")
        result["ma_launch_compression_date"] = pd.Timestamp(work["date"].iloc[compression_index]).date().isoformat()
        return result

    def date_at(index: int) -> str:
        return pd.Timestamp(work["date"].iloc[index]).date().isoformat()

    return {
        "ma_launch_passed": True,
        "ma_launch_status": "passed",
        "ma_launch_compression_date": date_at(compression_index),
        "ma_launch_breakout_date": date_at(breakout_index),
        "ma_launch_expansion_date": date_at(expansion_index),
        "ma_launch_ma60_turn_date": date_at(ma60_turn_index),
        "ma_launch_above_ma60_date": date_at(above_ma60_index),
        "ma_launch_activation_date": date_at(activation_index),
        "ma_launch_band_atr_compression": round(float(work["band_atr"].iloc[compression_index]), 4),
        "ma_launch_band_atr_activation": round(float(work["band_atr"].iloc[activation_index]), 4),
        "ma_launch_ma60_slope_percent": round(float(work["ma60_slope_percent"].iloc[activation_index] * 100.0), 4),
        "ma_launch_advance_percent": round(advance * 100.0, 4),
        "ma_launch_reasons": ["短均线粘合后多头发散", "价格与短均线站上MA60", "MA60由平转升"],
    }