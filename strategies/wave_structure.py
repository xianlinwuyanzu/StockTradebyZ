from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from features.ma_launch import evaluate_ma_launch
from features.bullish_gap import BullishGap, GAP_DEFAULTS, gap_reference_limit, measure_bullish_gaps
from strategies.scan_three_wave import (
    NARROW_DEFAULTS,
    PivotZone,
    _compute_j,
    _merge_platforms,
    _path_metrics,
    _raw_pivots,
    _refine_low_starts,
)


@dataclass(frozen=True)
class ActiveWave:
    code: str
    starts: tuple[PivotZone, ...]
    tops: tuple[PivotZone, ...]
    current_start: PivotZone
    completed_wave_count: int
    current_wave_number: int
    state: str
    current_period: int
    current_return: float
    current_peak_date: pd.Timestamp
    current_peak_return: float
    current_drawdown: float
    current_path_efficiency: float
    current_up_path_efficiency: float
    current_up_adverse: float
    lower_low_quality: float
    lower_low_break_pct: float
    structure_position_priority: float
    structure_position_phase: str
    price_position_priority: float
    current_wave_progress: float
    current_j: float
    current_j_change: float
    prior_wave_j_min: float | None
    current_j_vs_prior_min: float | None
    j_low_proximity_priority: float
    j_decline_priority: float
    structure_position_reasons: tuple[str, ...]
    path_efficiency_quality: float
    up_path_efficiency_quality: float
    pullback_path_efficiency_quality: float
    wave_smoothness: float
    wave_directional_consistency: float
    wave_step_regularity: float
    start_price: float
    start_price_position: float | None
    start_price_low_score: float
    start_price_prior_high: float | None
    start_price_drawdown_from_prior_high: float | None
    weekly_j_at_start: float | None
    weekly_j_peak_before_start: float | None
    weekly_j_peak_date: pd.Timestamp | None
    weekly_j_drop_before_start: float | None
    weekly_j_reset: bool
    weekly_j_reset_score: float
    complete_wave_periods: tuple[int, ...]
    up_returns: tuple[float, ...]
    pullbacks: tuple[float, ...]
    higher_highs: tuple[float, ...]
    higher_lows: tuple[float, ...]
    start_j_values: tuple[float, ...]
    score: float
    score_raw: float
    structure_match_percent: float
    structure_match_raw_max: float
    score_anchor_raw: float
    score_scale: float
    score_components_raw: tuple[tuple[str, float], ...]
    score_components: tuple[tuple[str, float], ...]
    score_reasons: tuple[str, ...]
    timing_score: float
    timing_reference_index: int | None
    timing_reference_value: float | None
    timing_j_peak: float | None
    timing_j_lowest: float | None
    timing_j_rebound: float
    timing_doji: bool
    timing_doji_bonus: float
    timing_score_components: tuple[tuple[str, float], ...]
    timing_score_reasons: tuple[str, ...]
    bullish_gap: BullishGap = BullishGap()
    reference_j_limit: float = 5.0


def _cfg_with_defaults(cfg: dict[str, float | int] | None) -> dict[str, float | int]:
    merged = dict(NARROW_DEFAULTS)
    merged.update(GAP_DEFAULTS)
    if cfg:
        merged.update(cfg)
    return merged


def _alternating_sequence(
    pivots: list[PivotZone],
    start_index: int,
) -> tuple[tuple[PivotZone, ...], tuple[PivotZone, ...], int, PivotZone | None]:
    starts: list[PivotZone] = []
    tops: list[PivotZone] = []
    cursor = start_index
    while cursor + 1 < len(pivots):
        if pivots[cursor].kind != "L" or pivots[cursor + 1].kind != "H":
            break
        starts.append(pivots[cursor])
        tops.append(pivots[cursor + 1])
        cursor += 2
        if cursor >= len(pivots) or pivots[cursor].kind != "L":
            break
    trailing_low = pivots[cursor] if cursor < len(pivots) and pivots[cursor].kind == "L" else None
    return tuple(starts), tuple(tops), cursor, trailing_low

def _validate_completed_prefix(
    frame: pd.DataFrame,
    starts: tuple[PivotZone, ...],
    tops: tuple[PivotZone, ...],
    cfg: dict[str, float | int],
    allow_open_last: bool = False,
    j_values: pd.Series | None = None,
) -> dict[str, Any] | None:
    """Validate paired low/high pivots, leaving the last up-leg open."""
    if len(starts) < 2 or len(starts) != len(tops):
        return None

    widths: list[int] = []
    for index, top in enumerate(tops):
        widths.append(top.start - starts[index].end)
        if index + 1 < len(starts):
            widths.append(starts[index + 1].start - top.end)
    if any(
        width < int(cfg["min_wave_bars"]) or width > int(cfg["max_wave_bars"])
        for width in widths
    ):
        return None

    complete_periods = tuple(
        starts[index + 1].start - starts[index].start
        for index in range(len(starts) - 1)
    )
    if any(
        period < int(cfg["min_complete_wave_period"])
        or period > int(cfg["max_complete_wave_period"])
        for period in complete_periods
    ):
        return None

    open_period = tops[-1].end - starts[-1].start
    if open_period > int(cfg["max_open_wave_period"]):
        return None

    up_returns = tuple(
        tops[index].value / starts[index].value - 1.0
        for index in range(len(starts))
    )
    pullbacks = tuple(
        (tops[index].value - starts[index + 1].value) / tops[index].value
        for index in range(len(starts) - 1)
    )
    higher_highs = tuple(
        tops[index + 1].value / tops[index].value - 1.0
        for index in range(len(tops) - 1)
    )
    higher_lows = tuple(
        starts[index + 1].value / starts[index].value - 1.0
        for index in range(len(starts) - 1)
    )

    completed_up_returns = up_returns[:-1] if allow_open_last else up_returns
    if completed_up_returns and min(completed_up_returns) < float(cfg["min_up_return"]):
        return None
    if pullbacks:
        if max(pullbacks) > float(cfg["max_pullback"]):
            return None
        if j_values is None:
            j_values = _compute_j(frame)
        j_pullback_drops = tuple(
            float(j_values.iloc[tops[index].start])
            - float(j_values.iloc[starts[index + 1].start])
            for index in range(len(starts) - 1)
        )
        if any(
            pullback < float(cfg["min_pullback"])
            and j_drop < float(cfg["min_j_pullback_drop"])
            for pullback, j_drop in zip(pullbacks, j_pullback_drops)
        ):
            return None
    if any(higher_low < -float(cfg["lower_low_tolerance"]) for higher_low in higher_lows):
        return None
    for index, higher_high in enumerate(higher_highs):
        next_wave_number = index + 2
        if next_wave_number < int(cfg["top_plateau_after_wave"]):
            if higher_high < float(cfg["min_step"]):
                return None
        elif higher_high < -float(cfg["top_plateau_tolerance"]):
            return None

    up_metrics = [
        _path_metrics(frame, starts[index].end, tops[index].start, "up")
        for index in range(len(starts))
    ]
    down_metrics = [
        _path_metrics(frame, tops[index].end, starts[index + 1].start, "down")
        for index in range(len(starts) - 1)
    ]
    complete_efficiencies = tuple(
        _path_metrics(frame, starts[index].end, starts[index + 1].start, "full")[0]
        for index in range(len(starts) - 1)
    )
    if complete_efficiencies and min(complete_efficiencies) < float(cfg["min_complete_wave_path_efficiency"]):
        return None
    completed_up_metrics = up_metrics[:-1] if allow_open_last else up_metrics
    if completed_up_metrics and min(metric[0] for metric in completed_up_metrics) < float(cfg["min_path_efficiency"]):
        return None
    if max(metric[1] for metric in up_metrics) > float(cfg["max_up_adverse"]):
        return None
    if down_metrics and max(metric[1] for metric in down_metrics) > float(cfg["max_down_adverse"]):
        return None

    completed_up_smoothness = tuple(
        _segment_smoothness(
            frame,
            starts[index].end,
            tops[index].start,
            "up",
        )
        for index in range(len(starts) - 1 if allow_open_last else len(starts))
    )
    completed_down_smoothness = tuple(
        _segment_smoothness(
            frame,
            tops[index].end,
            starts[index + 1].start,
            "down",
        )
        for index in range(len(starts) - 1)
    )
    completed_smoothness = completed_up_smoothness + completed_down_smoothness

    return {
        "complete_periods": complete_periods,
        "up_returns": up_returns,
        "pullbacks": pullbacks,
        "higher_highs": higher_highs,
        "higher_lows": higher_lows,
        "complete_efficiencies": complete_efficiencies,
        "completed_up_efficiencies": tuple(
            metric[0] for metric in completed_up_metrics
        ),
        "completed_up_smoothness": completed_up_smoothness,
        "completed_down_efficiencies": tuple(metric[0] for metric in down_metrics),
        "completed_smoothness": completed_smoothness,
    }


def _validate_trailing_low(
    frame: pd.DataFrame,
    starts: tuple[PivotZone, ...],
    tops: tuple[PivotZone, ...],
    trailing_low: PivotZone,
    cfg: dict[str, float | int],
    j_values: pd.Series | None = None,
) -> bool:
    if not starts or not tops:
        return False
    top = tops[-1]
    previous_start = starts[-1]
    down_width = trailing_low.start - top.end
    complete_period = trailing_low.start - previous_start.start
    pullback = (top.value - trailing_low.value) / top.value
    higher_low = trailing_low.value / previous_start.value - 1.0
    if down_width < int(cfg["min_wave_bars"]) or down_width > int(cfg["max_wave_bars"]):
        return False
    if complete_period < int(cfg["min_complete_wave_period"]) or complete_period > int(cfg["max_complete_wave_period"]):
        return False
    if pullback > float(cfg["max_pullback"]):
        return False
    if pullback < float(cfg["min_pullback"]):
        if j_values is None:
            j_values = _compute_j(frame)
        j_pullback_drop = float(j_values.iloc[top.start]) - float(j_values.iloc[trailing_low.start])
        if j_pullback_drop < float(cfg["min_j_pullback_drop"]):
            return False
    if higher_low < -float(cfg["lower_low_tolerance"]):
        return False
    _, adverse = _path_metrics(frame, top.end, trailing_low.start, "down")
    return adverse <= float(cfg["max_down_adverse"])


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _range_score(value: float, low: float, high: float) -> float:
    if high <= low:
        return 1.0 if value >= high else 0.0
    return _clip01((float(value) - low) / (high - low))


def _mean_or_default(values: tuple[float, ...], default: float = 0.0) -> float:
    return float(np.mean(values)) if values else default


def _lower_low_quality(value: float, cfg: dict[str, float | int]) -> float:
    tolerance = float(cfg["lower_low_tolerance"])
    ideal = float(cfg["min_step"])
    return _clip01((float(value) + tolerance) / max(ideal + tolerance, 1e-9))


def _active_low_quality(break_pct: float, cfg: dict[str, float | int]) -> float:
    tolerance = float(cfg["active_low_break_tolerance"])
    return 1.0 - _clip01(float(break_pct) / max(tolerance, 1e-9))


def _structure_position_context(
    frame: pd.DataFrame,
    starts: tuple[PivotZone, ...],
    current_start: PivotZone,
    latest_index: int,
    latest_close: float,
    peak_index: int,
    peak_price: float,
    current_drawdown: float,
    j_values: pd.Series,
    cfg: dict[str, float | int],
) -> dict[str, Any]:
    current_start_price = float(current_start.value)
    wave_range = max(peak_price - current_start_price, 1e-9)
    wave_progress = _clip01((latest_close - current_start_price) / wave_range)
    drawdown_priority = _clip01(current_drawdown / max(float(cfg["max_pullback"]), 1e-9))
    price_position_priority = 0.60 * (1.0 - wave_progress) + 0.40 * drawdown_priority

    if starts and starts[-1].start == current_start.start:
        prior_starts = starts[:-1]
    else:
        prior_starts = starts
    prior_wave_boundaries = (*prior_starts, current_start)
    prior_j_values: list[float] = []
    for index, start in enumerate(prior_starts):
        next_start = prior_wave_boundaries[index + 1]
        segment = j_values.iloc[start.start : next_start.start + 1]
        if not segment.empty:
            prior_j_values.append(float(segment.min()))
    prior_wave_j_min = min(prior_j_values) if prior_j_values else None

    current_j = float(j_values.iloc[latest_index])
    recent_days = max(1, int(cfg["position_recent_j_days"]))
    recent_start = max(current_start.start, latest_index - recent_days)
    current_j_change = current_j - float(j_values.iloc[recent_start])
    current_j_segment = j_values.iloc[current_start.start : latest_index + 1]
    current_j_peak = float(current_j_segment.max()) if not current_j_segment.empty else current_j
    j_decline_from_peak = max(0.0, current_j_peak - current_j)
    j_decline_priority = _clip01(
        j_decline_from_peak / max(float(cfg["position_j_decline_scale"]), 1e-9)
    )
    if prior_wave_j_min is None:
        current_j_vs_prior_min = None
        j_low_proximity_priority = 0.0
    else:
        current_j_vs_prior_min = current_j - prior_wave_j_min
        j_low_proximity_priority = 1.0 - _clip01(
            current_j_vs_prior_min / max(float(cfg["position_j_low_tolerance"]), 1e-9)
        )
    j_falling_priority = _clip01(
        -current_j_change / max(float(cfg["position_j_decline_scale"]), 1e-9)
    )
    j_position_priority = (
        0.55 * j_low_proximity_priority
        + 0.30 * j_decline_priority
        + 0.15 * j_falling_priority
    )
    structure_position_priority = round(
        100.0 * (0.55 * price_position_priority + 0.45 * j_position_priority),
        4,
    )

    if current_drawdown > float(cfg["min_pullback"]):
        phase = "高位回调中"
    elif peak_index < latest_index:
        phase = "顶部整理中"
    else:
        phase = "上升趋势中"
    reasons = (
        f"价格位置优先级 {price_position_priority * 100:.1f}（当前浪已走 {wave_progress * 100:.1f}%，峰值回撤 {current_drawdown * 100:.2f}%）",
        f"J 低位接近度 {j_low_proximity_priority * 100:.1f}（当前 J {current_j:.2f}，前几波区间最低 J {prior_wave_j_min:.2f}）"
        if prior_wave_j_min is not None
        else "J 低位接近度不可计算（没有更早起点）",
        f"J 下行优先级 {j_decline_priority * 100:.1f}（当前浪 J 从峰值回落 {j_decline_from_peak:.2f}）",
        f"结构位置阶段：{phase}；优先级越高表示越接近低位机会区",
    )
    return {
        "priority": structure_position_priority,
        "phase": phase,
        "price_priority": price_position_priority,
        "wave_progress": wave_progress,
        "current_j": current_j,
        "current_j_change": current_j_change,
        "prior_wave_j_min": prior_wave_j_min,
        "current_j_vs_prior_min": current_j_vs_prior_min,
        "j_low_proximity_priority": j_low_proximity_priority,
        "j_decline_priority": j_decline_priority,
        "reasons": reasons,
    }


def _segment_smoothness(
    frame: pd.DataFrame,
    start: int,
    end: int,
    direction: str,
) -> tuple[float, float, float]:
    segment = frame.iloc[start : end + 1]["close"].astype(float)
    changes = segment.diff().dropna().to_numpy()
    if len(changes) == 0:
        return 0.0, 0.0, 0.0

    signed_changes = changes if direction == "up" else -changes
    directional_consistency = float(np.mean(signed_changes >= 0.0))
    absolute_changes = np.abs(changes)
    mean_change = float(np.mean(absolute_changes))
    if mean_change == 0.0:
        step_regularity = 1.0
    else:
        coefficient_of_variation = float(np.std(absolute_changes) / mean_change)
        step_regularity = 1.0 / (1.0 + coefficient_of_variation)
    smoothness = 0.7 * directional_consistency + 0.3 * step_regularity
    return smoothness, directional_consistency, step_regularity


def _start_price_context(
    frame: pd.DataFrame,
    start_index: int,
    cfg: dict[str, float | int],
) -> dict[str, Any]:
    start_price = float(frame["close"].iloc[start_index])
    before = frame.iloc[:start_index].tail(max(1, int(cfg["start_price_lookback"])))
    if before.empty:
        return {
            "start_price": start_price,
            "position": None,
            "low_score": 0.0,
            "prior_high": None,
            "drawdown": None,
            "quality": 0.0,
        }

    prior_high = float(before["close"].max())
    prior_low = float(before["close"].min())
    price_range = prior_high - prior_low
    position = (start_price - prior_low) / price_range if price_range > 0 else 0.5
    position = _clip01(position)
    drawdown = max(0.0, 1.0 - start_price / prior_high) if prior_high > 0 else 0.0
    quality = _clip01(drawdown / max(float(cfg["start_price_high_drawdown_scale"]), 1e-9))
    return {
        "start_price": start_price,
        "position": position,
        "low_score": 1.0 - position,
        "prior_high": prior_high,
        "drawdown": drawdown,
        "quality": quality,
    }


def _weekly_j_context(
    frame: pd.DataFrame,
    start_index: int,
    cfg: dict[str, float | int],
) -> dict[str, Any]:
    start_date = pd.Timestamp(frame["date"].iloc[start_index])
    daily = frame[["date", "open", "high", "low", "close", "volume"]].copy()
    daily["date"] = pd.to_datetime(daily["date"])
    weekly = (
        daily.set_index("date")
        .resample("W-FRI")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    completed = weekly[weekly["date"] < start_date].reset_index(drop=True)
    if completed.empty:
        return {
            "j_at_start": None,
            "peak": None,
            "peak_date": None,
            "drop": None,
            "reset": False,
            "score": 0.0,
        }

    weekly_j = _compute_j(completed)
    completed = completed.assign(J=weekly_j.to_numpy())
    lookback = completed.tail(max(1, int(cfg["weekly_j_lookback_weeks"])))
    latest = completed.iloc[-1]
    peak_index = lookback["J"].idxmax()
    peak = lookback.loc[peak_index]
    current_j = float(latest["J"])
    peak_j = float(peak["J"])
    high_threshold = float(cfg["weekly_j_high_threshold"])
    low_threshold = float(cfg["weekly_j_low_threshold"])
    low_score = 1.0 - _clip01((current_j - low_threshold) / max(high_threshold - low_threshold, 1e-9))
    peak_score = _clip01((peak_j - high_threshold) / max(100.0 - high_threshold, 1e-9))
    drop = peak_j - current_j
    drop_score = _clip01(drop / max(peak_j - low_threshold, 1e-9)) if peak_j > low_threshold else 0.0
    reset = peak_j >= high_threshold and current_j <= low_threshold
    return {
        "j_at_start": current_j,
        "peak": peak_j,
        "peak_date": pd.Timestamp(peak["date"]),
        "drop": drop,
        "reset": reset,
        "score": 0.45 * low_score + 0.30 * peak_score + 0.25 * drop_score,
    }


def _find_timing_reference(
    frame: pd.DataFrame,
    j_values: pd.Series,
    start: PivotZone,
    top: PivotZone,
    cfg: dict[str, float | int],
) -> int | None:
    search_start = max(
        top.end + int(cfg["timing_min_top_to_reference_bars"]),
        start.start + int(cfg["timing_min_reference_period"]),
    )
    search_end = min(
        len(frame) - 1,
        start.start + int(cfg["timing_max_reference_period"]),
    )
    if search_start > search_end:
        return None

    start_price = float(frame["close"].iloc[start.start])
    top_price = float(top.value)
    reference_j_limit = gap_reference_limit(
        measure_bullish_gaps(frame, [(start.start, top.start)]),
        float(cfg["timing_reference_j_limit"]),
        cfg,
    )
    support_tolerance = float(cfg["timing_reference_support_tolerance"])
    for index in range(search_start, search_end + 1):
        if float(j_values.iloc[index]) >= reference_j_limit:
            continue
        close = float(frame["close"].iloc[index])
        pullback = (top_price - close) / top_price if top_price > 0 else 1.0
        if close > top_price or pullback > float(cfg["max_pullback"]):
            return None
        if close < start_price * (1.0 - support_tolerance):
            continue
        return index
    return None


def _is_timing_doji(
    frame: pd.DataFrame,
    index: int,
    j_value: float,
    cfg: dict[str, float | int],
) -> bool:
    if j_value > float(cfg["timing_low_j_threshold"]):
        return False
    open_price = float(frame["open"].iloc[index])
    close = float(frame["close"].iloc[index])
    high = float(frame["high"].iloc[index])
    low = float(frame["low"].iloc[index])
    candle_range = max(0.0, high - low)
    body = abs(close - open_price)
    if candle_range <= 1e-9:
        return body <= max(abs(close) * 0.001, 1e-9)
    return body / candle_range <= float(cfg["timing_doji_body_ratio"])


def _timing_score_context(
    frame: pd.DataFrame,
    j_values: pd.Series,
    start: PivotZone,
    top: PivotZone,
    latest_index: int,
    cfg: dict[str, float | int],
) -> dict[str, Any]:
    empty: dict[str, Any] = {
        "score": 0.0,
        "reference_index": None,
        "reference_value": None,
        "j_peak": None,
        "j_lowest": None,
        "j_rebound": 0.0,
        "doji": False,
        "doji_bonus": 0.0,
        "components": (),
        "reasons": ("当前未进入 2顶 后回落参考区间",),
    }
    if latest_index <= top.end:
        return empty

    top_price = float(top.value)
    latest_close = float(frame["close"].iloc[latest_index])
    if top_price <= 0.0 or latest_close >= top_price:
        return empty

    current_j = float(j_values.iloc[latest_index])
    doji = _is_timing_doji(frame, latest_index, current_j, cfg)
    doji_bonus = float(cfg["timing_doji_bonus"]) if doji else 0.0
    reference_index = _find_timing_reference(frame, j_values, start, top, cfg)
    j_segment = j_values.iloc[top.end : latest_index + 1].astype(float)
    j_peak = float(j_segment.max()) if not j_segment.empty else current_j
    target_j = gap_reference_limit(
        measure_bullish_gaps(frame, [(start.start, top.start)]),
        float(cfg["timing_reference_j_limit"]),
        cfg,
    )
    base_score = 0.0
    j_lowest = None
    j_rebound = 0.0
    components: list[tuple[str, float]] = []
    reasons: list[str] = []

    if reference_index is not None:
        post_reference = j_values.iloc[reference_index : latest_index + 1].astype(float)
        lowest_offset = int(post_reference.to_numpy().argmin())
        lowest_index = reference_index + lowest_offset
        j_lowest = float(j_values.iloc[lowest_index])
        j_rebound = max(0.0, current_j - j_lowest)
        rebound_score = 1.0 - _clip01(
            j_rebound / max(float(cfg["timing_j_rebound_scale"]), 1e-9)
        )
        base_score = 10.0 * rebound_score
        components.append(("timing_j_rebound", base_score))
        reasons.append(
            f"已找到 2起参考点（{pd.Timestamp(frame['date'].iloc[reference_index]).date().isoformat()}，J {float(j_values.iloc[reference_index]):.2f}）；"
            f"当前 J {current_j:.2f}，较参考区间低点回升 {j_rebound:.2f}，回升越多择时越低"
        )
    else:
        elapsed = latest_index - start.start
        max_period = int(cfg["timing_max_reference_period"])
        if elapsed < int(cfg["timing_min_reference_period"]) or elapsed > max_period:
            reasons.append(f"尚未在参考窗口内形成 J<{target_j:g} 的回落参考点")
        else:
            j_span = max(j_peak - target_j, 1.0)
            drop_progress = _clip01((j_peak - current_j) / j_span)
            target_proximity = 1.0 - _clip01(abs(current_j - target_j) / j_span)
            base_score = 10.0 * (0.7 * drop_progress + 0.3 * target_proximity)
            components.append(("timing_j_pullback", base_score))
            reasons.append(
                f"2顶后回落中，J 从高点 {j_peak:.2f} 回落至当前 {current_j:.2f}；"
                f"越接近 J<{target_j:g} 参考区间择时越高"
            )

    if doji:
        components.append(("timing_doji_bonus", doji_bonus))
        reasons.append(
            f"当前日 J {current_j:.2f} 较低且出现日线十字星，择时加分 +{doji_bonus:.2f}"
        )
    elif current_j <= float(cfg["timing_low_j_threshold"]):
        reasons.append(f"当前日 J {current_j:.2f} 较低，但未形成日线十字星")

    return {
        "score": round(base_score + doji_bonus, 4),
        "reference_index": reference_index,
        "reference_value": None if reference_index is None else float(j_values.iloc[reference_index]),
        "j_peak": j_peak,
        "j_lowest": j_lowest,
        "j_rebound": j_rebound,
        "doji": doji,
        "doji_bonus": doji_bonus,
        "components": tuple((name, round(value, 4)) for name, value in components),
        "reasons": tuple(reasons) if reasons else empty["reasons"],
    }


def _structure_raw_max(cfg: dict[str, float | int]) -> float:
    return sum(
        (
            1.0,
            1.0,
            float(cfg["up_path_efficiency_weight"]),
            float(cfg["up_direction_consistency_weight"]),
            float(cfg["pullback_path_efficiency_weight"]),
            float(cfg.get("bullish_gap_weight", 1.0)),
            1.0,
            1.0,
            1.0,
        )
    )


def _score_active_wave(
    *,
    lower_low_quality: float,
    up_path_efficiency_quality: float,
    up_direction_consistency: float,
    pullback_path_efficiency_quality: float,
    start_price_low_score: float,
    start_price_quality: float,
    weekly_j_reset_score: float,
    cfg: dict[str, float | int],
    bullish_gap: BullishGap = BullishGap(),
) -> tuple[float, tuple[tuple[str, float], ...], tuple[str, ...]]:
    raw_components = (
        ("structure_base", 1.0),
        ("low_structure_quality", lower_low_quality),
        (
            "up_path_efficiency",
            float(cfg["up_path_efficiency_weight"]) * up_path_efficiency_quality,
        ),
        (
            "up_direction_consistency",
            float(cfg["up_direction_consistency_weight"]) * up_direction_consistency,
        ),
        (
            "pullback_path_efficiency",
            float(cfg["pullback_path_efficiency_weight"]) * pullback_path_efficiency_quality,
        ),
        ("low_position", start_price_low_score),
        ("start_price_quality", start_price_quality),
        ("weekly_j_reset", weekly_j_reset_score),
        ("bullish_gap", float(cfg.get("bullish_gap_weight", 1.0)) * bullish_gap.quality),
    )
    anchor_raw = float(cfg.get("score_anchor_raw", 5.114))
    reference_points = float(cfg.get("score_reference_points", 10.0))
    if anchor_raw <= 0.0 or reference_points <= 0.0:
        raise ValueError("score calibration values must be positive")
    score_scale = reference_points / anchor_raw
    components = tuple(
        (name, round(value * score_scale, 4))
        for name, value in raw_components
    )
    score = round(sum(value for _, value in components), 4)
    reasons = (
        f"结构基准 +{components[0][1]:.3f}分（原始 1.000；硬条件通过，不按当前第几波加分）",
        f"低点模型匹配度 +{components[1][1]:.3f}分（原始 {lower_low_quality:.3f}；允许小幅破低但破低越深分越低）",
        f"上涨路径效率 +{components[2][1]:.3f}分（原始 {up_path_efficiency_quality:.3f}；上涨腿权重更高，包含完整波效率参考）",
        f"上涨方向一致性 +{components[3][1]:.3f}分（原始 {up_direction_consistency:.3f}；只看上涨日方向，不要求每日步长均匀）",
        f"回撤路径效率 +{components[4][1]:.3f}分（原始 {pullback_path_efficiency_quality:.3f}；回撤路径作为较低权重辅助项）",
        f"1起低位程度 +{components[5][1]:.3f}分（原始 {start_price_low_score:.3f}；1起价格在起点前区间的位置）",
        f"1起前高回落 +{components[6][1]:.3f}分（原始 {start_price_quality:.3f}；从起点前高位回落的幅度）",
        f"周线 J 重置 +{components[7][1]:.3f}分（原始 {weekly_j_reset_score:.3f}；优先奖励高位 J 回落至 {float(cfg['weekly_j_low_threshold']):.0f} 以下）",
        f"阳线缺口 +{components[8][1]:.3f}分（实体 {bullish_gap.body_count} 组，其中完整 {bullish_gap.full_count} 组，连续衔接 {bullish_gap.consecutive_count} 次）",
    )
    return score, components, reasons


def _build_active_candidate(
    frame: pd.DataFrame,
    starts: tuple[PivotZone, ...],
    tops: tuple[PivotZone, ...],
    current_start: PivotZone,
    completed_metrics: dict[str, Any],
    completed_wave_count: int,
    current_wave_number: int,
    cfg: dict[str, float | int],
) -> ActiveWave | None:
    latest_index = len(frame) - 1
    current_period = latest_index - current_start.start
    if current_period < int(cfg["min_wave_bars"]) or current_period > int(cfg["max_open_wave_period"]):
        return None

    current_segment = frame.iloc[current_start.start : latest_index + 1]
    close = current_segment["close"].astype(float)
    latest_close = float(close.iloc[-1])
    current_start_price = float(current_start.value)
    active_low_break_pct = max(0.0, 1.0 - float(close.min()) / current_start_price)
    if latest_close <= current_start_price or active_low_break_pct > float(cfg["active_low_break_tolerance"]):
        return None

    peak_offset = int(close.to_numpy().argmax())
    peak_index = current_start.start + peak_offset
    peak_price = float(close.iloc[peak_offset])
    current_return = latest_close / current_start_price - 1.0
    peak_return = peak_price / current_start_price - 1.0
    current_drawdown = (peak_price - latest_close) / peak_price if peak_price > 0 else 1.0
    current_efficiency, _ = _path_metrics(frame, current_start.start, latest_index, "up")
    current_up_efficiency, current_up_adverse = _path_metrics(frame, current_start.start, peak_index, "up")

    if peak_return < float(cfg["min_step"]):
        return None
    if current_up_efficiency < float(cfg["min_complete_wave_path_efficiency"]):
        return None
    if current_up_adverse > float(cfg["max_up_adverse"]):
        return None
    if current_drawdown > float(cfg["max_pullback"]):
        return None

    complete_efficiencies = tuple(completed_metrics.get("complete_efficiencies", ()))
    completed_up_efficiencies = tuple(completed_metrics.get("completed_up_efficiencies", ()))
    average_complete_efficiency = _mean_or_default(
        complete_efficiencies,
        float(cfg["min_complete_wave_path_efficiency"]),
    )
    average_up_efficiency = _mean_or_default(
        completed_up_efficiencies + (current_up_efficiency,),
        float(cfg["min_path_efficiency"]),
    )
    complete_path_efficiency_quality = _range_score(
            average_complete_efficiency,
            float(cfg["min_complete_wave_path_efficiency"]),
            0.60,
        )
    average_up_path_efficiency_quality = _range_score(
            average_up_efficiency,
            float(cfg["min_path_efficiency"]),
            1.0,
        )
    up_path_efficiency_quality = (
        0.25 * complete_path_efficiency_quality
        + 0.75 * average_up_path_efficiency_quality
    )

    current_smoothness = _segment_smoothness(
        frame,
        current_start.start,
        peak_index,
        "up",
    )
    up_smoothness_metrics = tuple(completed_metrics.get("completed_up_smoothness", ())) + (current_smoothness,)
    up_direction_consistency = _mean_or_default(
        tuple(metric[1] for metric in up_smoothness_metrics),
    )
    wave_step_regularity = _mean_or_default(
        tuple(metric[2] for metric in up_smoothness_metrics),
    )
    wave_smoothness = up_direction_consistency
    wave_directional_consistency = up_direction_consistency

    completed_down_efficiencies = tuple(completed_metrics.get("completed_down_efficiencies", ()))
    average_pullback_efficiency = _mean_or_default(
        completed_down_efficiencies,
        float(cfg["min_complete_wave_path_efficiency"]),
    )
    pullback_path_efficiency_quality = _range_score(
        average_pullback_efficiency,
        float(cfg["min_complete_wave_path_efficiency"]),
        1.0,
    )

    start_context = _start_price_context(frame, starts[0].start, cfg)
    weekly_context = _weekly_j_context(frame, starts[0].start, cfg)
    completed_lower_low_qualities = tuple(
        _lower_low_quality(value, cfg)
        for value in tuple(completed_metrics.get("higher_lows", ()))
    )
    lower_low_quality_values = completed_lower_low_qualities + (
        _active_low_quality(active_low_break_pct, cfg),
    )
    lower_low_quality = min(lower_low_quality_values, default=1.0)
    completed_lower_low_break = max(
        (max(0.0, -value) for value in tuple(completed_metrics.get("higher_lows", ()))),
        default=0.0,
    )
    lower_low_break_pct = max(completed_lower_low_break, active_low_break_pct)

    j_values = _compute_j(frame)
    timing_context = _timing_score_context(
        frame,
        j_values,
        starts[-1],
        tops[-1],
        latest_index,
        cfg,
    )
    position_context = _structure_position_context(
        frame,
        starts,
        current_start,
        latest_index,
        latest_close,
        peak_index,
        peak_price,
        current_drawdown,
        j_values,
        cfg,
    )
    gap_segments = [
        (start.start, top.start)
        for start, top in zip(starts, tops)
        if start.start != current_start.start
    ]
    gap_segments.append((current_start.start, peak_index))
    bullish_gap = measure_bullish_gaps(frame, gap_segments)
    reference_j_limit = gap_reference_limit(
        measure_bullish_gaps(frame, [(starts[-1].start, tops[-1].start)]),
        float(cfg["timing_reference_j_limit"]),
        cfg,
    )
    score, score_components, reasons = _score_active_wave(
        lower_low_quality=lower_low_quality,
        up_path_efficiency_quality=up_path_efficiency_quality,
        up_direction_consistency=up_direction_consistency,
        pullback_path_efficiency_quality=pullback_path_efficiency_quality,
        start_price_low_score=float(start_context["low_score"]),
        start_price_quality=float(start_context["quality"]),
        weekly_j_reset_score=float(weekly_context["score"]),
        bullish_gap=bullish_gap,
        cfg=cfg,
    )
    score_anchor_raw = float(cfg.get("score_anchor_raw", 5.114))
    score_reference_points = float(cfg.get("score_reference_points", 10.0))
    score_scale = score_reference_points / score_anchor_raw
    score_components_raw = tuple(
        (name, round(value / score_scale, 4))
        for name, value in score_components
    )
    score_raw = round(sum(value for _, value in score_components_raw), 4)
    structure_match_raw_max = _structure_raw_max(cfg)
    structure_match_percent = round(
        100.0 * _clip01(score_raw / max(structure_match_raw_max, 1e-9)),
        2,
    )

    if current_drawdown <= float(cfg["min_pullback"]):
        state = "上升浪进行中" if peak_index == latest_index else "顶部整理中"
    else:
        state = "回调未确认"

    start_j_values = tuple(float(j_values.iloc[start.end]) for start in starts)
    all_starts = starts if current_start.start == starts[-1].start else (*starts, current_start)
    return ActiveWave(
        code=str(frame["code"].iloc[0]),
        starts=all_starts,
        tops=tops,
        current_start=current_start,
        completed_wave_count=completed_wave_count,
        current_wave_number=current_wave_number,
        state=state,
        current_period=current_period,
        current_return=current_return,
        current_peak_date=pd.Timestamp(frame["date"].iloc[peak_index]),
        current_peak_return=peak_return,
        current_drawdown=current_drawdown,
        current_path_efficiency=current_efficiency,
        current_up_path_efficiency=current_up_efficiency,
        current_up_adverse=current_up_adverse,
        lower_low_quality=lower_low_quality,
        lower_low_break_pct=lower_low_break_pct,
        structure_position_priority=float(position_context["priority"]),
        structure_position_phase=str(position_context["phase"]),
        price_position_priority=float(position_context["price_priority"]),
        current_wave_progress=float(position_context["wave_progress"]),
        current_j=float(position_context["current_j"]),
        current_j_change=float(position_context["current_j_change"]),
        prior_wave_j_min=position_context["prior_wave_j_min"],
        current_j_vs_prior_min=position_context["current_j_vs_prior_min"],
        j_low_proximity_priority=float(position_context["j_low_proximity_priority"]),
        j_decline_priority=float(position_context["j_decline_priority"]),
        structure_position_reasons=tuple(position_context["reasons"]),
        path_efficiency_quality=up_path_efficiency_quality,
        up_path_efficiency_quality=up_path_efficiency_quality,
        pullback_path_efficiency_quality=pullback_path_efficiency_quality,
        wave_smoothness=wave_smoothness,
        wave_directional_consistency=wave_directional_consistency,
        wave_step_regularity=wave_step_regularity,
        start_price=float(start_context["start_price"]),
        start_price_position=start_context["position"],
        start_price_low_score=float(start_context["low_score"]),
        start_price_prior_high=start_context["prior_high"],
        start_price_drawdown_from_prior_high=start_context["drawdown"],
        weekly_j_at_start=weekly_context["j_at_start"],
        weekly_j_peak_before_start=weekly_context["peak"],
        weekly_j_peak_date=weekly_context["peak_date"],
        weekly_j_drop_before_start=weekly_context["drop"],
        weekly_j_reset=bool(weekly_context["reset"]),
        weekly_j_reset_score=float(weekly_context["score"]),
        complete_wave_periods=tuple(completed_metrics["complete_periods"]),
        up_returns=tuple(completed_metrics["up_returns"]),
        pullbacks=tuple(completed_metrics["pullbacks"]),
        higher_highs=tuple(completed_metrics["higher_highs"]),
        higher_lows=tuple(completed_metrics["higher_lows"]),
        start_j_values=start_j_values,
        score=score,
        score_raw=score_raw,
        structure_match_percent=structure_match_percent,
        structure_match_raw_max=structure_match_raw_max,
        score_anchor_raw=score_anchor_raw,
        score_scale=score_scale,
        score_components_raw=score_components_raw,
        score_components=score_components,
        score_reasons=reasons,
        timing_score=float(timing_context["score"]),
        timing_reference_index=timing_context["reference_index"],
        timing_reference_value=timing_context["reference_value"],
        bullish_gap=bullish_gap,
        reference_j_limit=reference_j_limit,
        timing_j_peak=timing_context["j_peak"],
        timing_j_lowest=timing_context["j_lowest"],
        timing_j_rebound=float(timing_context["j_rebound"]),
        timing_doji=bool(timing_context["doji"]),
        timing_doji_bonus=float(timing_context["doji_bonus"]),
        timing_score_components=timing_context["components"],
        timing_score_reasons=timing_context["reasons"],
    )


def find_active_wave(
    frame: pd.DataFrame,
    cfg: dict[str, float | int] | None = None,
    min_completed_waves: int = 1,
) -> ActiveWave | None:
    """Return the latest valid wave group that is still structurally active."""
    if frame.empty or len(frame) < 25:
        return None
    cfg = _cfg_with_defaults(cfg)
    work = frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    if not {"open", "close", "high", "low", "volume"}.issubset(work.columns):
        return None

    pivots = _merge_platforms(work, _raw_pivots(work, cfg), cfg)
    pivots = _refine_low_starts(work, pivots, cfg)
    j_values = _compute_j(work)
    possible: list[ActiveWave] = []

    for start_index, pivot in enumerate(pivots):
        if pivot.kind != "L":
            continue
        paired_starts, paired_tops, _, trailing_low = _alternating_sequence(pivots, start_index)
        if len(paired_starts) < 2:
            continue

        for prefix_length in range(len(paired_starts), 1, -1):
            starts = paired_starts[:prefix_length]
            tops = paired_tops[:prefix_length]
            has_trailing_low = (
                trailing_low is not None
                and trailing_low.start > tops[-1].end
                and prefix_length == len(paired_starts)
            )
            metrics = _validate_completed_prefix(
                work,
                starts,
                tops,
                cfg,
                allow_open_last=not has_trailing_low,
                j_values=j_values,
            )
            if metrics is None:
                continue

            if has_trailing_low:
                if _validate_trailing_low(work, starts, tops, trailing_low, cfg, j_values=j_values):
                    candidate = _build_active_candidate(
                        work,
                        starts,
                        tops,
                        trailing_low,
                        metrics,
                        completed_wave_count=prefix_length,
                        current_wave_number=prefix_length + 1,
                        cfg=cfg,
                    )
                    if candidate is not None:
                        possible.append(candidate)

            candidate = _build_active_candidate(
                work,
                starts,
                tops,
                starts[-1],
                metrics,
                completed_wave_count=prefix_length - 1,
                current_wave_number=prefix_length,
                cfg=cfg,
            )
            if candidate is not None:
                possible.append(candidate)
            break

    eligible = [candidate for candidate in possible if candidate.completed_wave_count >= min_completed_waves]
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda candidate: (
            candidate.current_start.start,
            candidate.completed_wave_count,
            candidate.score,
        ),
    )


def active_wave_to_dict(candidate: ActiveWave, frame: pd.DataFrame, score_threshold: float) -> dict[str, Any]:
    def date_at(index: int) -> str:
        return pd.Timestamp(frame["date"].iloc[index]).date().isoformat()

    result = {
        "code": candidate.code,
        "as_of_date": date_at(len(frame) - 1),
        "cycle_start_date": date_at(candidate.starts[0].start),
        "current_wave_number": candidate.current_wave_number,
        "completed_wave_count": candidate.completed_wave_count,
        "wave_count_in_sequence": len(candidate.starts),
        "state": candidate.state,
        "structure_quality": candidate.score,
        "score": candidate.score,
        "structure_match_percent": candidate.structure_match_percent,
        "structure_match_raw_max": candidate.structure_match_raw_max,
        "score_components": {
            name: round(value, 4)
            for name, value in candidate.score_components
        },
        "score_components_raw": {
            name: round(value, 4)
            for name, value in candidate.score_components_raw
        },
        "score_raw": candidate.score_raw,
        "score_reference": "AJG_manual_sample",
        "score_reference_raw": candidate.score_anchor_raw,
        "score_reference_points": 10.0,
        "score_scale": candidate.score_scale,
        "score_version": "wave-v7-structure-quality-timing",
        "score_threshold": score_threshold,
        "score_reasons": list(candidate.score_reasons),
        "timing_score": candidate.timing_score,
        "timing_score_components": {
            name: round(value, 4)
            for name, value in candidate.timing_score_components
        },
        "timing_score_reasons": list(candidate.timing_score_reasons),
        "timing_reference_date": (
            date_at(candidate.timing_reference_index)
            if candidate.timing_reference_index is not None
            else None
        ),
        "timing_reference_value": candidate.timing_reference_value,
        "timing_j_peak": candidate.timing_j_peak,
        "timing_j_lowest": candidate.timing_j_lowest,
        "timing_j_rebound": candidate.timing_j_rebound,
        "timing_doji": candidate.timing_doji,
        "timing_doji_bonus": candidate.timing_doji_bonus,
        "start_sequence": " -> ".join(
            f"{index + 1}起:{date_at(start.start)}"
            for index, start in enumerate(candidate.starts)
        ),
        "start_dates": [date_at(start.start) for start in candidate.starts],
        "top_dates": [date_at(top.start) for top in candidate.tops],
        "current_start_date": date_at(candidate.current_start.start),
        "current_peak_date": candidate.current_peak_date.date().isoformat(),
        "current_period": candidate.current_period,
        "current_return": candidate.current_return,
        "current_peak_return": candidate.current_peak_return,
        "current_drawdown": candidate.current_drawdown,
        "current_path_efficiency": candidate.current_path_efficiency,
        "current_up_path_efficiency": candidate.current_up_path_efficiency,
        "bullish_gap": candidate.bullish_gap.to_dict(),
        "reference_j_limit": candidate.reference_j_limit,
        "current_up_adverse": candidate.current_up_adverse,
        "lower_low_quality": candidate.lower_low_quality,
        "lower_low_break_pct": candidate.lower_low_break_pct,
        "structure_position_priority": candidate.structure_position_priority,
        "structure_position_phase": candidate.structure_position_phase,
        "price_position_priority": candidate.price_position_priority,
        "current_wave_progress": candidate.current_wave_progress,
        "current_j": candidate.current_j,
        "current_j_change": candidate.current_j_change,
        "prior_wave_j_min": candidate.prior_wave_j_min,
        "current_j_vs_prior_min": candidate.current_j_vs_prior_min,
        "j_low_proximity_priority": candidate.j_low_proximity_priority,
        "j_decline_priority": candidate.j_decline_priority,
        "structure_position_reasons": list(candidate.structure_position_reasons),
        "path_efficiency_quality": candidate.path_efficiency_quality,
        "up_path_efficiency_quality": candidate.up_path_efficiency_quality,
        "pullback_path_efficiency_quality": candidate.pullback_path_efficiency_quality,
        "wave_smoothness": candidate.wave_smoothness,
        "wave_directional_consistency": candidate.wave_directional_consistency,
        "wave_step_regularity": candidate.wave_step_regularity,
        "start_price": candidate.start_price,
        "start_price_position": candidate.start_price_position,
        "start_price_low_score": candidate.start_price_low_score,
        "start_price_prior_high": candidate.start_price_prior_high,
        "start_price_drawdown_from_prior_high": candidate.start_price_drawdown_from_prior_high,
        "weekly_j_at_start": candidate.weekly_j_at_start,
        "weekly_j_peak_before_start": candidate.weekly_j_peak_before_start,
        "weekly_j_peak_date": (
            candidate.weekly_j_peak_date.date().isoformat()
            if candidate.weekly_j_peak_date is not None
            else None
        ),
        "weekly_j_drop_before_start": candidate.weekly_j_drop_before_start,
        "weekly_j_reset": candidate.weekly_j_reset,
        "weekly_j_reset_score": candidate.weekly_j_reset_score,
        "complete_wave_periods": list(candidate.complete_wave_periods),
        "up_returns": list(candidate.up_returns),
        "pullbacks": list(candidate.pullbacks),
        "higher_highs": list(candidate.higher_highs),
        "higher_lows": list(candidate.higher_lows),
        "start_j_values": list(candidate.start_j_values),
        "latest_close": float(frame["close"].iloc[-1]),
    }
    result.update(evaluate_ma_launch(frame, candidate.starts[0].start))
    return result
