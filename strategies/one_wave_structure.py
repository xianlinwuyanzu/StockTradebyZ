from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from features.ma_launch import evaluate_ma_launch
from features.impulse_pullback import IMPULSE_DEFAULTS, ImpulsePullback, find_impulse_pullback
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


ONE_WAVE_DEFAULTS = {
    **GAP_DEFAULTS,
    **IMPULSE_DEFAULTS,
    "selection_branch": "low_j_pullback",
    "min_wave_bars": 2,
    "max_wave_bars": 20,
    "min_up_return": 0.06,
    "min_path_efficiency": 0.58,
    "path_efficiency_weight": 2.0,
    "max_up_adverse": 0.12,
    "max_one_wave_period": 30,
    "start_price_lookback": 120,
    "start_price_high_drawdown_scale": 0.40,
    "start_price_drawdown_weight": 1.5,
    "min_reference_period": 7,
    "max_reference_period": 25,
    "min_top_to_reference_bars": 2,
    "max_reference_j": 5.0,
    "max_reference_rebound": 0.15,
    "j_rebound_exit_threshold": 80.0,
    "timing_j_rebound_scale": 30.0,
    "timing_low_j_threshold": 10.0,
    "timing_doji_body_ratio": 0.20,
    "timing_doji_bonus": 3.0,
    "pullback_path_efficiency_low": 0.35,
    "pullback_path_efficiency_high": 0.85,
    "pullback_path_efficiency_penalty": 5.0,
    "max_pullback": 0.28,
    "max_observation_after_reference": 25,
    "support_close_tolerance": 0.0,
    "two_wave_min_step": 0.02,
    "two_wave_min_return": 0.06,
    "two_wave_min_period": 7,
    "two_wave_max_period": 25,
    "two_wave_max_bars": 20,
    "two_wave_min_path_efficiency": 0.58,
    "two_wave_max_up_adverse": 0.12,
}


@dataclass(frozen=True)
class OneWaveCandidate:
    code: str
    start1: PivotZone
    top1: PivotZone
    j2_reference_index: int
    j2_reference_value: float
    j2_lowest_index: int
    j2_lowest_value: float
    start_price_prior_high: float | None
    start_price_drawdown_from_prior_high: float
    current_period: int
    wave1_period: int
    top1_to_reference_period: int
    wave1_return: float
    current_return: float
    pullback_from_top1: float
    wave1_path_efficiency: float
    wave1_up_adverse: float
    pullback_path_efficiency: float
    current_peak_index: int
    current_peak_return: float
    current_drawdown: float
    lowest_close_after_top1: float
    support_gap_from_start1: float
    current_j: float
    current_j_change: float
    state: str
    score: float
    score_components: tuple[tuple[str, float], ...]
    score_reasons: tuple[str, ...]
    timing_score: float
    timing_reference_index: int
    timing_reference_value: float
    timing_j_lowest: float
    timing_j_rebound: float
    timing_doji: bool
    timing_doji_bonus: float
    timing_score_components: tuple[tuple[str, float], ...]
    timing_score_reasons: tuple[str, ...]
    bullish_gap: BullishGap = BullishGap()
    reference_j_limit: float = 5.0
    selection_branch: str = "low_j_pullback"
    impulse_pullback: ImpulsePullback | None = None


def _cfg_with_defaults(cfg: dict[str, float | int] | None) -> dict[str, float | int]:
    merged = dict(ONE_WAVE_DEFAULTS)
    if cfg:
        merged.update(cfg)
    return merged


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _range_score(value: float, low: float, high: float) -> float:
    if high <= low:
        return 1.0 if value >= high else 0.0
    return _clip01((float(value) - low) / (high - low))


def _start_price_drawdown_context(
    frame: pd.DataFrame,
    start_index: int,
    cfg: dict[str, float | int],
) -> tuple[float | None, float]:
    start_price = float(frame["close"].iloc[start_index])
    before = frame.iloc[:start_index].tail(max(1, int(cfg["start_price_lookback"])))
    if before.empty:
        return None, 0.0

    prior_high = float(before["close"].max())
    drawdown = max(0.0, 1.0 - start_price / prior_high) if prior_high > 0 else 0.0
    return prior_high, drawdown


def _segment_smoothness(frame: pd.DataFrame, start: int, end: int) -> float:
    changes = frame.iloc[start : end + 1]["close"].astype(float).diff().dropna().to_numpy()
    if len(changes) == 0:
        return 0.0
    direction = float(np.mean(changes >= 0.0))
    mean_change = float(np.mean(np.abs(changes)))
    if mean_change <= 0.0:
        regularity = 1.0
    else:
        regularity = 1.0 / (1.0 + float(np.std(np.abs(changes)) / mean_change))
    return 0.7 * direction + 0.3 * regularity


def _find_j2_reference(
    frame: pd.DataFrame,
    j_values: pd.Series,
    start1: PivotZone,
    top1: PivotZone,
    cfg: dict[str, float | int],
) -> int | None:
    search_start = top1.end + int(cfg["min_top_to_reference_bars"])
    search_end = min(len(frame) - 1, start1.start + int(cfg["max_reference_period"]))
    if search_start > search_end:
        return None

    start_price = float(frame["close"].iloc[start1.start])
    top_price = float(top1.value)
    reference_j_limit = gap_reference_limit(
        measure_bullish_gaps(frame, [(start1.start, top1.start)]),
        float(cfg["max_reference_j"]),
        cfg,
    )
    for index in range(search_start, search_end + 1):
        if float(j_values.iloc[index]) >= reference_j_limit:
            continue
        period = index - start1.start
        if period < int(cfg["min_reference_period"]):
            continue
        close = float(frame["close"].iloc[index])
        pullback = (top_price - close) / top_price if top_price > 0 else 1.0
        if close > top_price or pullback > float(cfg["max_pullback"]):
            return None
        if close < start_price * (1.0 - float(cfg["support_close_tolerance"])):
            return None
        return index
    return None


def _has_valid_second_wave(
    frame: pd.DataFrame,
    pivots: list[PivotZone],
    start1: PivotZone,
    top1: PivotZone,
    j2_reference_index: int,
    cfg: dict[str, float | int],
) -> bool:
    start1_price = float(frame["close"].iloc[start1.start])
    reference_price = float(frame["close"].iloc[j2_reference_index])
    top1_price = float(top1.value)
    for pivot in pivots:
        if pivot.kind != "H" or pivot.start <= j2_reference_index:
            continue
        if pivot.start - j2_reference_index < int(cfg["min_wave_bars"]):
            continue
        if pivot.start - j2_reference_index > int(cfg["two_wave_max_bars"]):
            continue
        period = pivot.start - j2_reference_index
        if not int(cfg["two_wave_min_period"]) <= period <= int(cfg["two_wave_max_period"]):
            continue
        second_return = pivot.value / reference_price - 1.0 if reference_price > 0 else -1.0
        higher_high = pivot.value / top1_price - 1.0 if top1_price > 0 else -1.0
        higher_low = reference_price / start1_price - 1.0 if start1_price > 0 else -1.0
        if second_return < float(cfg["two_wave_min_return"]):
            continue
        if higher_high < float(cfg["two_wave_min_step"]):
            continue
        if higher_low < float(cfg["two_wave_min_step"]):
            continue
        efficiency, adverse = _path_metrics(frame, j2_reference_index, pivot.start, "up")
        if efficiency < float(cfg["two_wave_min_path_efficiency"]):
            continue
        if adverse > float(cfg["two_wave_max_up_adverse"]):
            continue
        return True
    return False


def _score_candidate(
    *,
    wave1_return: float,
    wave1_path_efficiency: float,
    wave1_smoothness: float,
    support_gap: float,
    j2_value: float,
    j2_reference_limit: float,
    rebound_return: float,
    freshness: float,
    start_price_drawdown: float,
    start_price_high_drawdown_scale: float,
    start_price_drawdown_weight: float,
    path_efficiency_weight: float,
    pullback_path_efficiency: float,
    pullback_path_efficiency_low: float,
    pullback_path_efficiency_high: float,
    pullback_path_efficiency_penalty: float,
    bullish_gap: BullishGap = BullishGap(),
    bullish_gap_weight: float = 1.0,
) -> tuple[float, tuple[tuple[str, float], ...], tuple[str, ...]]:
    pullback_efficiency_penalty = -float(pullback_path_efficiency_penalty) * (
        1.0 - _range_score(
            pullback_path_efficiency,
            pullback_path_efficiency_low,
            pullback_path_efficiency_high,
        )
    )
    components = (
        ("structure_base", 2.0),
        ("wave1_return", 1.5 * _range_score(wave1_return, 0.06, 0.40)),
        ("wave1_path_efficiency", path_efficiency_weight * _range_score(wave1_path_efficiency, 0.58, 1.0)),
        ("wave1_smoothness", 1.0 * _range_score(wave1_smoothness, 0.60, 1.0)),
        ("start1_support", 1.5 * (1.0 - _clip01(max(0.0, support_gap) / 0.25))),
        ("j2_oversold", 1.5 * _clip01(max(0.0, -j2_value) / 30.0)),
        ("rebound", 1.0 * _clip01(max(0.0, rebound_return) / 0.12)),
        ("signal_freshness", 1.0 * _clip01(freshness)),
        ("pullback_path_efficiency", pullback_efficiency_penalty),
        ("start_price_drawdown", start_price_drawdown_weight * _clip01(
            max(0.0, start_price_drawdown) / max(start_price_high_drawdown_scale, 1e-9)
        )),
        ("bullish_gap", bullish_gap_weight * bullish_gap.quality),
    )
    score = round(sum(value for _, value in components), 4)
    reference_label = f"J<{j2_reference_limit:g}"
    reasons = (
        f"结构基准 +{components[0][1]:.2f}分（1起、1顶和 {reference_label} 参考点均已确认）",
        f"第一波涨幅 +{components[1][1]:.2f}分（第一波涨幅 {wave1_return * 100:.2f}%）",
        f"第一波路径效率 +{components[2][1]:.2f}分（效率 {wave1_path_efficiency:.3f}）",
        f"第一波平滑度 +{components[3][1]:.2f}分（平滑度 {wave1_smoothness:.3f}）",
        f"1起支撑 +{components[4][1]:.2f}分（回落期间最低收盘距 1起 {support_gap * 100:.2f}%）",
        f"J 超卖加分 +{components[5][1]:.2f}分（参考 J {j2_value:.2f}，负 J 越低加分越高）",
        f"回调后回升 +{components[6][1]:.2f}分（参考点后涨幅 {rebound_return * 100:.2f}%）",
        f"信号新鲜度 +{components[7][1]:.2f}分（观察期剩余比例 {freshness * 100:.1f}%）",
        f"下降路径效率 {components[8][1]:+.2f}分（效率 {pullback_path_efficiency:.3f}，仅评分不淘汰）",
        f"1起前高回落 +{components[9][1]:.2f}分（相对起点前最高收盘回落 {start_price_drawdown * 100:.2f}%）",
        f"阳线缺口 +{components[10][1]:.2f}分（实体 {bullish_gap.body_count} 组，其中完整 {bullish_gap.full_count} 组，连续衔接 {bullish_gap.consecutive_count} 次）",
    )
    return score, components, reasons


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


def _timing_score_after_reference(
    frame: pd.DataFrame,
    j_values: pd.Series,
    reference_index: int,
    cfg: dict[str, float | int],
) -> dict[str, Any]:
    current_j = float(j_values.iloc[-1])
    post_reference = j_values.iloc[reference_index:].astype(float)
    lowest_offset = int(post_reference.to_numpy().argmin())
    lowest_index = reference_index + lowest_offset
    j_lowest = float(j_values.iloc[lowest_index])
    j_rebound = max(0.0, current_j - j_lowest)
    base_score = 10.0 * (1.0 - _clip01(
        j_rebound / max(float(cfg["timing_j_rebound_scale"]), 1e-9)
    ))
    timing_doji = _is_timing_doji(frame, len(frame) - 1, current_j, cfg)
    timing_doji_bonus = float(cfg["timing_doji_bonus"]) if timing_doji else 0.0
    components: list[tuple[str, float]] = [("timing_j_rebound", base_score)]
    reasons = [
        f"已找到 2起参考点（{pd.Timestamp(frame['date'].iloc[reference_index]).date().isoformat()}，J {float(j_values.iloc[reference_index]):.2f}）；"
        f"当前 J {current_j:.2f}，较参考区间低点回升 {j_rebound:.2f}，J 越低且回升越少择时越高"
    ]
    if timing_doji:
        components.append(("timing_doji_bonus", timing_doji_bonus))
        reasons.append(
            f"当前日 J {current_j:.2f} 较低且出现日线十字星，择时加分 +{timing_doji_bonus:.2f}"
        )
    elif current_j <= float(cfg["timing_low_j_threshold"]):
        reasons.append(f"当前日 J {current_j:.2f} 较低，但未形成日线十字星")

    return {
        "score": round(base_score + timing_doji_bonus, 4),
        "reference_index": reference_index,
        "reference_value": float(j_values.iloc[reference_index]),
        "j_lowest": j_lowest,
        "j_rebound": j_rebound,
        "doji": timing_doji,
        "doji_bonus": timing_doji_bonus,
        "components": tuple((name, round(value, 4)) for name, value in components),
        "reasons": tuple(reasons),
    }


def _build_candidate(
    frame: pd.DataFrame,
    pivots: list[PivotZone],
    start1: PivotZone,
    top1: PivotZone,
    j_values: pd.Series,
    j2_reference_index: int,
    cfg: dict[str, float | int],
    enforce_exit_rules: bool = True,
    impulse_pullback: ImpulsePullback | None = None,
) -> OneWaveCandidate | None:
    latest_index = len(frame) - 1
    if latest_index < j2_reference_index or (latest_index == j2_reference_index and impulse_pullback is None):
        return None
    current_period = latest_index - start1.start
    if enforce_exit_rules:
        if current_period > int(cfg["max_one_wave_period"]):
            return None
        if latest_index - j2_reference_index > int(cfg["max_observation_after_reference"]):
            return None

    start_price = float(frame["close"].iloc[start1.start])
    start_price_prior_high, start_price_drawdown = _start_price_drawdown_context(
        frame,
        start1.start,
        cfg,
    )
    top_price = float(top1.value)
    latest_close = float(frame["close"].iloc[latest_index])
    close_after_top = frame["close"].iloc[top1.end : latest_index + 1].astype(float)
    lowest_close = float(close_after_top.min())
    if enforce_exit_rules and impulse_pullback is not None:
        impulse_price = float(frame["close"].iloc[impulse_pullback.impulse_start])
        if (
            1.0 - lowest_close / top_price > float(cfg["impulse_pullback_max_pct"])
            or (top_price - lowest_close) / (top_price - impulse_price) > float(cfg["impulse_max_retracement"])
        ):
            return None
    support_gap = lowest_close / start_price - 1.0 if start_price > 0 else -1.0
    if enforce_exit_rules and support_gap < -float(cfg["support_close_tolerance"]):
        return None
    if enforce_exit_rules and latest_close < start_price:
        return None

    wave1_period = top1.start - start1.start
    if not int(cfg["min_wave_bars"]) <= wave1_period <= int(cfg["max_wave_bars"]):
        return None
    wave1_return = top_price / start_price - 1.0 if start_price > 0 else -1.0
    if wave1_return < float(cfg["min_up_return"]):
        return None

    wave1_efficiency, wave1_adverse = _path_metrics(frame, start1.start, top1.start, "up")
    if wave1_efficiency < float(cfg["min_path_efficiency"]):
        return None
    if wave1_adverse > float(cfg["max_up_adverse"]):
        return None

    j2_close = float(frame["close"].iloc[j2_reference_index])
    pullback = (top_price - j2_close) / top_price if top_price > 0 else 1.0
    if pullback < 0.0 or pullback > float(cfg["max_pullback"]):
        return None
    pullback_path_efficiency, _ = _path_metrics(
        frame, top1.end, j2_reference_index, "down"
    )
    if enforce_exit_rules and _has_valid_second_wave(
        frame, pivots, start1, top1, j2_reference_index, cfg
    ):
        return None

    current_segment = frame.iloc[start1.start : latest_index + 1]
    peak_offset = int(current_segment["close"].astype(float).to_numpy().argmax())
    peak_index = start1.start + peak_offset
    peak_price = float(frame["close"].iloc[peak_index])
    current_drawdown = (peak_price - latest_close) / peak_price if peak_price > 0 else 1.0
    if enforce_exit_rules and current_drawdown > float(cfg["max_pullback"]):
        return None

    j_segment = j_values.iloc[top1.end : latest_index + 1]
    lowest_j_offset = int(j_segment.astype(float).to_numpy().argmin())
    lowest_j_index = top1.end + lowest_j_offset
    current_j = float(j_values.iloc[latest_index])
    j_after_reference = j_values.iloc[j2_reference_index : latest_index + 1].astype(float)
    lowest_j_after_reference = float(j_after_reference.min())
    max_j_after_reference = float(j_after_reference.max())
    close_after_reference = frame["close"].iloc[j2_reference_index : latest_index + 1].astype(float)
    max_rebound_return = (
        float(close_after_reference.max()) / j2_close - 1.0
        if j2_close > 0
        else 0.0
    )
    recent_start = max(j2_reference_index, latest_index - 3)
    current_j_change = current_j - float(j_values.iloc[recent_start])
    current_return = latest_close / start_price - 1.0
    rebound_return = latest_close / j2_close - 1.0 if j2_close > 0 else 0.0
    if enforce_exit_rules:
        if max_rebound_return >= float(cfg["max_reference_rebound"]):
            return None
        if (
            max_j_after_reference >= float(cfg["j_rebound_exit_threshold"])
            and max_j_after_reference > lowest_j_after_reference
        ):
            return None
    freshness = 1.0 - (latest_index - j2_reference_index) / max(
        int(cfg["max_observation_after_reference"]), 1
    )
    smoothness = _segment_smoothness(frame, start1.start, top1.start)
    bullish_gap = measure_bullish_gaps(frame, [(start1.start, top1.start)])
    reference_j_limit = gap_reference_limit(bullish_gap, float(cfg["max_reference_j"]), cfg)
    if impulse_pullback is not None:
        reference_j_limit = float(cfg["impulse_reference_j_limit"])
    score, components, reasons = _score_candidate(
        wave1_return=wave1_return,
        wave1_path_efficiency=wave1_efficiency,
        wave1_smoothness=smoothness,
        support_gap=support_gap,
        j2_value=float(j_values.iloc[j2_reference_index]),
        j2_reference_limit=reference_j_limit,
        rebound_return=rebound_return,
        freshness=freshness,
        start_price_drawdown=start_price_drawdown,
        start_price_high_drawdown_scale=float(cfg["start_price_high_drawdown_scale"]),
        start_price_drawdown_weight=float(cfg["start_price_drawdown_weight"]),
        path_efficiency_weight=float(cfg["path_efficiency_weight"]),
        pullback_path_efficiency=pullback_path_efficiency,
        pullback_path_efficiency_low=float(cfg["pullback_path_efficiency_low"]),
        pullback_path_efficiency_high=float(cfg["pullback_path_efficiency_high"]),
        pullback_path_efficiency_penalty=float(cfg["pullback_path_efficiency_penalty"]),
        bullish_gap=bullish_gap,
        bullish_gap_weight=float(cfg["bullish_gap_weight"]),
    )
    if impulse_pullback is not None:
        impulse_bonus = max(0.0, float(cfg["impulse_quality_weight"])) * impulse_pullback.quality
        components += (("impulse_quality", impulse_bonus),)
        reasons += (
            f"强势推进 +{impulse_bonus:.2f}分（涨幅 {impulse_pullback.impulse_return * 100:.2f}%，效率 {impulse_pullback.impulse_efficiency:.3f}，全阳线推进）",
            f"强势快回调分支：J<{reference_j_limit:g}，顶部 J 回落 {impulse_pullback.j_drop:.2f}，回吐推进幅度 {impulse_pullback.retracement * 100:.2f}%",
        )
        score = round(sum(value for _, value in components), 4)
    timing_context = _timing_score_after_reference(
        frame,
        j_values,
        j2_reference_index,
        cfg,
    )
    state = "一波回调后回升" if rebound_return > 0.0 else "一波有效（2起参考）"
    if impulse_pullback is not None:
        state = "一波强势快回调（2起候选）"
    return OneWaveCandidate(
        code=str(frame["code"].iloc[0]),
        start1=start1,
        top1=top1,
        j2_reference_index=j2_reference_index,
        j2_reference_value=float(j_values.iloc[j2_reference_index]),
        j2_lowest_index=lowest_j_index,
        j2_lowest_value=float(j_values.iloc[lowest_j_index]),
        start_price_prior_high=start_price_prior_high,
        start_price_drawdown_from_prior_high=start_price_drawdown,
        current_period=current_period,
        wave1_period=wave1_period,
        top1_to_reference_period=j2_reference_index - top1.end,
        wave1_return=wave1_return,
        current_return=current_return,
        pullback_from_top1=pullback,
        wave1_path_efficiency=wave1_efficiency,
        wave1_up_adverse=wave1_adverse,
        pullback_path_efficiency=pullback_path_efficiency,
        current_peak_index=peak_index,
        current_peak_return=peak_price / start_price - 1.0,
        current_drawdown=current_drawdown,
        lowest_close_after_top1=lowest_close,
        support_gap_from_start1=support_gap,
        current_j=current_j,
        current_j_change=current_j_change,
        state=state,
        score=score,
        score_components=components,
        score_reasons=reasons,
        timing_score=float(timing_context["score"]),
        timing_reference_index=int(timing_context["reference_index"]),
        timing_reference_value=float(timing_context["reference_value"]),
        timing_j_lowest=float(timing_context["j_lowest"]),
        timing_j_rebound=float(timing_context["j_rebound"]),
        timing_doji=bool(timing_context["doji"]),
        timing_doji_bonus=float(timing_context["doji_bonus"]),
        timing_score_components=timing_context["components"],
        timing_score_reasons=timing_context["reasons"],
        bullish_gap=bullish_gap,
        reference_j_limit=reference_j_limit,
        selection_branch="impulse_pullback" if impulse_pullback is not None else "low_j_pullback",
        impulse_pullback=impulse_pullback,
    )


def find_active_one_wave(
    frame: pd.DataFrame,
    cfg: dict[str, float | int] | None = None,
) -> OneWaveCandidate | None:
    candidates = _find_one_wave_candidates(frame, cfg, enforce_exit_rules=True)
    return _select_best_one_wave_candidate(candidates)


def find_one_wave_preselection(
    frame: pd.DataFrame,
    cfg: dict[str, float | int] | None = None,
) -> OneWaveCandidate | None:
    """Return the best structurally valid 1起 candidate before lifecycle exits."""
    candidates = find_one_wave_preselections(frame, cfg)
    return _select_best_one_wave_candidate(candidates)


def find_one_wave_preselections(
    frame: pd.DataFrame,
    cfg: dict[str, float | int] | None = None,
) -> list[OneWaveCandidate]:
    """Return all structurally valid 1起 candidates before lifecycle exits."""
    return _find_one_wave_candidates(frame, cfg, enforce_exit_rules=False)


def _find_one_wave_candidates(
    frame: pd.DataFrame,
    cfg: dict[str, float | int] | None,
    enforce_exit_rules: bool,
) -> list[OneWaveCandidate]:
    if frame.empty or len(frame) < 25:
        return []
    config = _cfg_with_defaults(cfg)
    work = frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    required = {"open", "close", "high", "low", "volume"}
    if not required.issubset(work.columns):
        return []

    pivots = _merge_platforms(work, _raw_pivots(work, NARROW_DEFAULTS), NARROW_DEFAULTS)
    pivots = _refine_low_starts(work, pivots, NARROW_DEFAULTS)
    j_values = _compute_j(work)
    candidates: list[OneWaveCandidate] = []
    for index in range(len(pivots) - 1):
        start1 = pivots[index]
        top1 = pivots[index + 1]
        if start1.kind != "L" or top1.kind != "H":
            continue
        branch = config["selection_branch"]
        reference_index = (
            _find_j2_reference(work, j_values, start1, top1, config)
            if branch == "low_j_pullback" else None
        )
        impulse = (
            find_impulse_pullback(
                work, j_values, start1.start, top1.start, top1.end, top1.value, config,
            )
            if branch == "impulse_pullback" else None
        )
        references = []
        if reference_index is not None:
            references.append((reference_index, None))
        if impulse is not None:
            references.append((impulse.reference_index, impulse))
        for selected_index, branch_context in references:
            candidate = _build_candidate(
                work, pivots, start1, top1, j_values, selected_index, config,
                enforce_exit_rules=enforce_exit_rules,
                impulse_pullback=branch_context,
            )
            if candidate is not None:
                candidates.append(candidate)

    return candidates


def _select_best_one_wave_candidate(
    candidates: list[OneWaveCandidate],
) -> OneWaveCandidate | None:
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda candidate: (
            candidate.j2_reference_index,
            candidate.score,
            candidate.start1.start,
        ),
    )


def one_wave_to_dict(
    candidate: OneWaveCandidate,
    frame: pd.DataFrame,
    score_threshold: float,
) -> dict[str, Any]:
    def date_at(index: int) -> str:
        return pd.Timestamp(frame["date"].iloc[index]).date().isoformat()

    result = {
        "code": candidate.code,
        "as_of_date": date_at(len(frame) - 1),
        "cycle_start_date": date_at(candidate.start1.start),
        "current_wave_number": 1,
        "completed_wave_count": 0,
        "wave_count_in_sequence": 1,
        "state": candidate.state,
        "selection_branch": candidate.selection_branch,
        "score": candidate.score,
        "score_threshold": score_threshold,
        "score_components": {name: round(value, 4) for name, value in candidate.score_components},
        "score_reasons": list(candidate.score_reasons),
        "timing_score": candidate.timing_score,
        "timing_score_components": {
            name: round(value, 4)
            for name, value in candidate.timing_score_components
        },
        "timing_score_reasons": list(candidate.timing_score_reasons),
        "timing_reference_date": date_at(candidate.timing_reference_index),
        "timing_reference_value": candidate.timing_reference_value,
        "timing_j_lowest": candidate.timing_j_lowest,
        "timing_j_rebound": candidate.timing_j_rebound,
        "timing_doji": candidate.timing_doji,
        "timing_doji_bonus": candidate.timing_doji_bonus,
        "start_sequence": f"1起:{date_at(candidate.start1.start)}",
        "start_dates": [date_at(candidate.start1.start)],
        "top_dates": [date_at(candidate.top1.start)],
        "start_price": float(frame["close"].iloc[candidate.start1.start]),
        "start_price_prior_high": candidate.start_price_prior_high,
        "start_price_drawdown_from_prior_high": candidate.start_price_drawdown_from_prior_high,
        "current_start_date": date_at(candidate.start1.start),
        "current_peak_date": date_at(candidate.current_peak_index),
        "current_period": candidate.current_period,
        "current_return": candidate.current_return,
        "current_peak_return": candidate.current_peak_return,
        "current_drawdown": candidate.current_drawdown,
        "current_j": candidate.current_j,
        "current_j_change": candidate.current_j_change,
        "start1_price": float(frame["close"].iloc[candidate.start1.start]),
        "top1_price": float(candidate.top1.value),
        "wave1_period": candidate.wave1_period,
        "wave1_return": candidate.wave1_return,
        "wave1_path_efficiency": candidate.wave1_path_efficiency,
        "wave1_up_adverse": candidate.wave1_up_adverse,
        "pullback_path_efficiency": candidate.pullback_path_efficiency,
        "top1_to_j2_period": candidate.top1_to_reference_period,
        "pullback_from_top1": candidate.pullback_from_top1,
        "j2_reference_date": date_at(candidate.j2_reference_index),
        "j2_reference_value": candidate.j2_reference_value,
        "reference_j_limit": candidate.reference_j_limit,
        "bullish_gap": candidate.bullish_gap.to_dict(),
        "j2_lowest_date": date_at(candidate.j2_lowest_index),
        "j2_lowest_value": candidate.j2_lowest_value,
        "lowest_close_after_top1": candidate.lowest_close_after_top1,
        "support_gap_from_start1": candidate.support_gap_from_start1,
        "max_observation_after_reference": candidate.current_period,
        "latest_close": float(frame["close"].iloc[-1]),
    }
    impulse = candidate.impulse_pullback
    if impulse is not None:
        result["impulse_pullback"] = {
            "start_date": date_at(impulse.impulse_start),
            "top_date": date_at(impulse.impulse_end),
            "return": impulse.impulse_return,
            "efficiency": impulse.impulse_efficiency,
            "quality": impulse.quality,
            "first_candidate_date": date_at(impulse.first_reference_index),
            "reference_date": date_at(impulse.reference_index),
            "reference_status": "candidate",
            "pullback": impulse.pullback,
            "retracement": impulse.retracement,
            "j_drop": impulse.j_drop,
        }
    result.update(evaluate_ma_launch(frame, candidate.start1.start))
    return result