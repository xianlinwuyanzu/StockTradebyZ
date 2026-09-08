from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from features.bullish_gap import measure_bullish_gaps


IMPULSE_DEFAULTS = {
    "impulse_pullback_enabled": True,
    "impulse_quality_weight": 1.0,
    "impulse_min_bars": 3,
    "impulse_max_bars": 6,
    "impulse_min_return": 0.15,
    "impulse_min_efficiency": 0.90,
    "impulse_min_gap_count": 2,
    "impulse_pullback_min_bars": 2,
    "impulse_pullback_max_bars": 5,
    "impulse_pullback_min_pct": 0.05,
    "impulse_pullback_max_pct": 0.15,
    "impulse_max_retracement": 0.50,
    "impulse_reference_j_limit": 70.0,
    "impulse_min_j_drop": 30.0,
    "impulse_reference_price_tolerance": 0.02,
}


@dataclass(frozen=True)
class ImpulsePullback:
    impulse_start: int
    impulse_end: int
    impulse_return: float
    impulse_efficiency: float
    quality: float
    first_reference_index: int
    reference_index: int
    pullback: float
    retracement: float
    j_drop: float


def find_impulse_pullback(
    frame: pd.DataFrame,
    j_values: pd.Series,
    start_index: int,
    top_start: int,
    top_end: int,
    top_price: float,
    cfg: dict[str, float | int],
) -> ImpulsePullback | None:
    if not cfg["impulse_pullback_enabled"]:
        return None
    impulses = []
    for bars in range(int(cfg["impulse_min_bars"]), int(cfg["impulse_max_bars"]) + 1):
        impulse_start = top_start - bars
        if impulse_start < start_index:
            continue
        segment = frame.iloc[impulse_start : top_start + 1]
        if not (segment["close"] > segment["open"]).all():
            continue
        net = float(segment["close"].iloc[-1] - segment["close"].iloc[0])
        distance = float(segment["close"].diff().abs().sum())
        gain = net / float(segment["close"].iloc[0])
        efficiency = net / distance if distance > 0 else 0.0
        if gain < float(cfg["impulse_min_return"]) or efficiency < float(cfg["impulse_min_efficiency"]):
            continue
        gaps = measure_bullish_gaps(frame, [(impulse_start, top_start)])
        if gaps.body_count < int(cfg["impulse_min_gap_count"]) or gaps.consecutive_count < 1:
            continue
        speed = gain / bars
        quality = min(1.0, 0.4 * min(1.0, speed / 0.05) + 0.4 * efficiency + 0.2)
        impulses.append((quality, gain, impulse_start, efficiency))
    if not impulses:
        return None
    quality, gain, impulse_start, efficiency = max(impulses)
    start_price = float(frame["close"].iloc[start_index])
    impulse_price = float(frame["close"].iloc[impulse_start])
    wave_range = top_price - impulse_price
    if wave_range <= 0:
        return None
    search_start = max(
        top_end + int(cfg["impulse_pullback_min_bars"]),
        top_end + int(cfg["min_top_to_reference_bars"]),
        start_index + int(cfg["min_reference_period"]),
    )
    search_end = min(
        len(frame) - 1,
        top_end + int(cfg["impulse_pullback_max_bars"]),
        start_index + int(cfg["max_reference_period"]),
    )
    first_reference = None
    selected = None
    top_j = float(j_values.iloc[top_start : top_end + 1].max())
    for index in range(top_end + 1, search_end + 1):
        close = float(frame["close"].iloc[index])
        pullback = 1.0 - close / top_price
        retracement = (top_price - close) / wave_range
        if (
            close < start_price
            or close > top_price
            or pullback > min(float(cfg["max_pullback"]), float(cfg["impulse_pullback_max_pct"]))
            or retracement > float(cfg["impulse_max_retracement"])
        ):
            break
        if index < search_start:
            continue
        current_j = float(j_values.iloc[index])
        j_drop = top_j - current_j
        if selected is not None:
            lowest_close = float(frame["close"].iloc[top_end + 1 : index + 1].min())
            previous_lowest = float(frame["close"].iloc[top_end + 1 : index].min())
            if close > lowest_close * (1.0 + float(cfg["impulse_reference_price_tolerance"])):
                break
            if current_j >= float(j_values.iloc[selected.reference_index]):
                break
            if close < previous_lowest:
                continue
        if (
            pullback < float(cfg["impulse_pullback_min_pct"])
            or current_j >= float(cfg["impulse_reference_j_limit"])
            or j_drop < float(cfg["impulse_min_j_drop"])
        ):
            continue
        if first_reference is None:
            first_reference = index
        selected = ImpulsePullback(
            impulse_start, top_start, gain, efficiency, quality,
            first_reference, index, pullback, retracement, j_drop,
        )
    return selected