from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from bullish_gap import measure_bullish_gaps
from one_wave_structure import _timing_score_after_reference
from scan_three_wave import PivotZone, _compute_j, _merge_platforms, _path_metrics, _raw_pivots, _refine_low_starts
from wave_structure import _build_active_candidate, _cfg_with_defaults, _validate_completed_prefix, active_wave_to_dict


MOZHUA_DEFAULTS = {
    "mozhua_body_j_limit": 40.0,
    "mozhua_full_j_limit": 50.0,
    "mozhua_strong_j_limit": 70.0,
    "mozhua_min_j_drop": 30.0,
    "mozhua_min_pullback": 0.05,
    "mozhua_max_pullback": 0.15,
    "mozhua_max_retracement": 0.50,
    "mozhua_reference_max_bars": 10,
    "mozhua_reference_price_tolerance": 0.02,
    "mozhua_max_rebound": 0.15,
    "mozhua_observation_bars": 10,
    "impulse_quality_weight": 1.0,
    "timing_reference_j_limit": 5.0,
}


@dataclass(frozen=True)
class GapReference:
    index: int
    first_index: int
    j_limit: float
    j_drop: float
    pullback: float
    retracement: float
    gap_context: dict[str, Any]


def preceding_leg_context(frame: pd.DataFrame, start: int, top: int, cfg: dict[str, Any]) -> dict[str, Any]:
    gaps = measure_bullish_gaps(frame, [(start, top)])
    segment = frame.iloc[start : top + 1]
    previous = segment.shift(1)
    body_gap = (segment.close > segment.open) & (segment.open > previous[["open", "close"]].max(axis=1))
    full_gap = body_gap & (segment.low > previous.high)
    directional_body_count = int(body_gap.sum())
    directional_full_count = int(full_gap.sum())
    limit = float(cfg["timing_reference_j_limit"])
    if directional_body_count:
        limit = max(limit, float(cfg["mozhua_body_j_limit"]))
    if directional_full_count:
        limit = max(limit, float(cfg["mozhua_full_j_limit"]))
    impulse_quality = 0.0
    impulse_start = None
    for bars in range(3, 7):
        begin = top - bars
        if begin < start:
            continue
        segment = frame.iloc[begin : top + 1]
        gain = float(segment.close.iloc[-1] / segment.close.iloc[0] - 1.0)
        efficiency, _ = _path_metrics(frame, begin, top, "up")
        impulse_gaps = measure_bullish_gaps(frame, [(begin, top)])
        if (
            (segment.close > segment.open).all() and gain >= 0.15 and efficiency >= 0.90
            and impulse_gaps.body_count >= 2 and impulse_gaps.consecutive_count >= 1
        ):
            quality = min(1.0, 0.4 * min(1.0, gain / bars / 0.05) + 0.4 * efficiency + 0.2)
            if quality > impulse_quality:
                impulse_quality, impulse_start = quality, begin
    if impulse_start is not None:
        limit = max(limit, float(cfg["mozhua_strong_j_limit"]))
    return {
        **gaps.to_dict(),
        "directional_body_count": directional_body_count,
        "directional_full_count": directional_full_count,
        "j_limit": limit,
        "impulse_quality": impulse_quality,
        "impulse_start_index": impulse_start,
    }


def find_gap_reference(
    frame: pd.DataFrame, j_values: pd.Series, start: PivotZone, top: PivotZone,
    end: int, cfg: dict[str, Any],
) -> GapReference | None:
    context = preceding_leg_context(frame, start.start, top.start, cfg)
    start_price = float(frame.close.iloc[start.start])
    wave_range = top.value - start_price
    if wave_range <= 0:
        return None
    earliest = max(top.end + int(cfg["timing_min_top_to_reference_bars"]), start.start + int(cfg["timing_min_reference_period"]))
    latest = min(end, top.end + int(cfg["mozhua_reference_max_bars"]), start.start + int(cfg["timing_max_reference_period"]))
    peak_j = float(j_values.iloc[top.start : top.end + 1].max())
    selected = None
    first_index = None
    for index in range(top.end + 1, latest + 1):
        close = float(frame.close.iloc[index])
        pullback = 1.0 - close / top.value
        retracement = (top.value - close) / wave_range
        if close > top.value or close < start_price or pullback > float(cfg["mozhua_max_pullback"]) or retracement > float(cfg["mozhua_max_retracement"]):
            break
        if index < earliest:
            continue
        current_j = float(j_values.iloc[index])
        j_drop = peak_j - current_j
        if selected is not None:
            lowest = float(frame.close.iloc[top.end + 1 : index + 1].min())
            if close > lowest * (1.0 + float(cfg["mozhua_reference_price_tolerance"])) or current_j >= float(j_values.iloc[selected.index]):
                break
        if current_j >= context["j_limit"] or j_drop < float(cfg["mozhua_min_j_drop"]) or pullback < float(cfg["mozhua_min_pullback"]):
            continue
        if first_index is None:
            first_index = index
        selected = GapReference(index, first_index, context["j_limit"], j_drop, pullback, retracement, context)
    return selected


def find_mozhua(frame: pd.DataFrame, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    cfg = {**_cfg_with_defaults(None), **MOZHUA_DEFAULTS, **(params or {})}
    if len(frame) < 25 or not {"date", "open", "high", "low", "close", "volume"}.issubset(frame.columns):
        return None
    work = frame.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    if "code" not in work:
        work["code"] = ""
    pivots = _refine_low_starts(work, _merge_platforms(work, _raw_pivots(work, cfg), cfg), cfg)
    j_values = _compute_j(work)
    candidates = []
    for position in range(len(pivots) - 3):
        first, top1, low2, top2 = pivots[position : position + 4]
        if (first.kind, top1.kind, low2.kind, top2.kind) != ("L", "H", "L", "H"):
            continue
        second_reference = find_gap_reference(work, j_values, first, top1, top2.start - 1, cfg)
        if second_reference is None:
            continue
        second_price = float(work.close.iloc[second_reference.index])
        second = PivotZone("L", second_reference.index, second_reference.index, second_price, second_price, second_price)
        third_reference = find_gap_reference(work, j_values, second, top2, len(work) - 1, cfg)
        if third_reference is None or third_reference.index != len(work) - 1:
            continue
        if not (second_reference.gap_context["directional_body_count"] or third_reference.gap_context["directional_body_count"]):
            continue
        starts, tops = (first, second), (top1, top2)
        if _validate_completed_prefix(work, starts, tops, cfg, j_values=j_values) is None:
            continue
        reference_index = third_reference.index
        reference_price = float(work.close.iloc[reference_index])
        history = work.close.iloc[top2.end + 1 :].astype(float)
        post_reference = work.close.iloc[reference_index:].astype(float)
        lowest = float(history.min())
        if (
            len(work) - 1 - reference_index > int(cfg["mozhua_observation_bars"])
            or lowest < second_price
            or 1.0 - lowest / top2.value > float(cfg["mozhua_max_pullback"])
            or (top2.value - lowest) / (top2.value - second_price) > float(cfg["mozhua_max_retracement"])
            or float(post_reference.max()) >= top2.value
            or float(post_reference.max()) / reference_price - 1.0 >= float(cfg["mozhua_max_rebound"])
        ):
            continue
        metrics = _validate_completed_prefix(work, starts, tops, cfg, allow_open_last=True, j_values=j_values)
        candidate = _build_active_candidate(work, starts, tops, second, metrics, 1, 2, cfg)
        if candidate is None:
            continue
        detail = active_wave_to_dict(candidate, work, float(cfg.get("score_threshold", 1.0)))
        timing = _timing_score_after_reference(work, j_values, reference_index, cfg)
        detail.update({
            "strategy_name": "魔抓策略", "selection_branch": "mozhua_two_wave",
            "state": "魔抓：二顶后回调（3起参考）", "reference_stage": 3,
            "reference_j_limit": third_reference.j_limit,
            "j2_reference_date": str(work.date.iloc[second_reference.index].date()),
            "j3_reference_date": str(work.date.iloc[reference_index].date()),
            "timing_reference_date": str(work.date.iloc[reference_index].date()),
            "timing_reference_value": float(j_values.iloc[reference_index]),
            "timing_score": timing["score"], "timing_score_components": dict(timing["components"]),
            "timing_score_reasons": [reason.replace("2起", "3起") for reason in timing["reasons"]],
            "timing_j_lowest": timing["j_lowest"], "timing_j_rebound": timing["j_rebound"],
            "timing_doji": timing["doji"], "timing_doji_bonus": timing["doji_bonus"],
            "reference_status": "candidate", "gap_references": {},
        })
        for label, reference in (("second", second_reference), ("third", third_reference)):
            context = dict(reference.gap_context)
            impulse_start = context.pop("impulse_start_index")
            context["impulse_start_date"] = None if impulse_start is None else str(work.date.iloc[impulse_start].date())
            detail["gap_references"][label] = {
                "date": str(work.date.iloc[reference.index].date()),
                "first_candidate_date": str(work.date.iloc[reference.first_index].date()),
                "j": float(j_values.iloc[reference.index]), "j_limit": reference.j_limit,
                "j_drop": reference.j_drop, "pullback": reference.pullback,
                "retracement": reference.retracement, "preceding_leg": context,
            }
        quality = (second_reference.gap_context["impulse_quality"] + third_reference.gap_context["impulse_quality"]) / 2.0
        weight = max(0.0, float(cfg["impulse_quality_weight"]))
        bonus = round(weight * quality, 4)
        detail["score_components_raw"]["impulse_quality"] = bonus
        detail["score_components"]["impulse_quality"] = round(bonus * candidate.score_scale, 4)
        detail["score_raw"] = round(sum(detail["score_components_raw"].values()), 4)
        detail["score"] = round(sum(detail["score_components"].values()), 4)
        detail["structure_quality"] = detail["score"]
        detail["structure_match_raw_max"] += weight
        detail["structure_match_percent"] = round(100 * detail["score_raw"] / detail["structure_match_raw_max"], 2)
        detail["score_reasons"].append(f"强势推进 +{detail['score_components']['impulse_quality']:.3f}分（两段推进质量平均 {quality:.3f}）")
        if detail["score"] >= float(cfg.get("score_threshold", 1.0)):
            candidates.append(detail)
    return max(candidates, key=lambda item: (item["j3_reference_date"], item["timing_score"], item["score"])) if candidates else None
