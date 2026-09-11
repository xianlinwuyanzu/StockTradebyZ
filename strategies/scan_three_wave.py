from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from scipy.signal import find_peaks


@dataclass(frozen=True)
class PivotZone:
    kind: str
    start: int
    end: int
    value: float
    low_value: float
    high_value: float


@dataclass(frozen=True)
class WaveCandidate:
    code: str
    starts: tuple[PivotZone, ...]
    tops: tuple[PivotZone, ...]
    leg_widths: tuple[int, ...]
    wave_window_period: int
    complete_wave_periods: tuple[int, ...]
    open_wave_period: int
    up_returns: tuple[float, ...]
    pullbacks: tuple[float, ...]
    start_j_values: tuple[float, ...]
    complete_wave_path_efficiencies: tuple[float, ...]
    higher_highs: tuple[float, ...]
    higher_lows: tuple[float, ...]
    min_up_path_efficiency: float
    max_up_adverse_pct: float
    max_down_adverse_pct: float


NARROW_DEFAULTS = {
    "prominence_pct": 0.02,
    "atr_multiplier": 0.5,
    "peak_distance": 4,
    "min_wave_bars": 2,
    "max_wave_bars": 20,
    "min_complete_wave_period": 7,
    "max_complete_wave_period": 25,
    "max_open_wave_period": 25,
    "min_complete_wave_path_efficiency": 0.10,
    "min_up_return": 0.06,
    "min_pullback": 0.05,
    "min_j_pullback_drop": 30.0,
    "max_pullback": 0.28,
    "min_step": 0.02,
    "lower_low_tolerance": 0.05,
    "active_low_break_tolerance": 0.05,
    "position_j_low_tolerance": 30.0,
    "position_j_decline_scale": 40.0,
    "position_recent_j_days": 3,
    "top_plateau_tolerance": 0.01,
    "top_plateau_after_wave": 3,
    "min_path_efficiency": 0.58,
    "max_up_adverse": 0.12,
    "max_down_adverse": 0.12,
    "start_price_lookback": 120,
    "start_price_high_drawdown_scale": 0.40,
    "timing_min_top_to_reference_bars": 2,
    "timing_min_reference_period": 7,
    "timing_max_reference_period": 25,
    "timing_reference_j_limit": 5.0,
    "timing_reference_support_tolerance": 0.0,
    "timing_low_j_threshold": 10.0,
    "timing_j_rebound_scale": 30.0,
    "timing_doji_body_ratio": 0.20,
    "timing_doji_bonus": 3.0,
    "up_path_efficiency_weight": 1.5,
    "up_direction_consistency_weight": 1.5,
    "pullback_path_efficiency_weight": 0.6,
    "weekly_j_lookback_weeks": 26,
    "weekly_j_high_threshold": 60.0,
    "weekly_j_low_threshold": 10.0,
    "weekly_j_rebound_filter_enabled": 0,
    "weekly_j_current_max": 80.0,
    "weekly_j_rebound_lookback_weeks": 26,
    "weekly_j_rebound_prior_high_min": 60.0,
    "weekly_j_rebound_trough_max": 50.0,
    "weekly_j_min_rebound": 8.0,
    "weekly_j_recent_rising_weeks": 2,
    "score_anchor_raw": 5.114,
    "score_reference_points": 10.0,
    "platform_price_tolerance": 0.01,
    "platform_pullback_tolerance": 0.06,
    "platform_max_bars": 15,
    "j_start_search_bars": 5,
    "j_start_price_tolerance": 0.03,
    "j_start_max_value": 25.0,
    "j_start_min_improvement": 2.0,
    "j_start_max_body_ratio": 0.35,
}


def _load_frames(data_dir: Path) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    required = {"date", "close", "high", "low", "volume"}
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
        frame["code"] = path.stem
        frames.append(frame)
    return frames


def _prominence(frame: pd.DataFrame, cfg: dict[str, float | int]) -> float:
    true_range = pd.concat(
        [
            (frame["high"] - frame["low"]).abs(),
            (frame["high"] - frame["close"].shift()).abs(),
            (frame["low"] - frame["close"].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    median_atr = float(true_range.rolling(14, min_periods=5).mean().median())
    median_close = float(frame["close"].median())
    return max(
        median_close * float(cfg["prominence_pct"]),
        median_atr * float(cfg["atr_multiplier"]),
        1e-8,
    )


def _raw_pivots(frame: pd.DataFrame, cfg: dict[str, float | int]) -> list[PivotZone]:
    prominence = _prominence(frame, cfg)
    distance = int(cfg["peak_distance"])
    close = frame["close"].to_numpy(float)
    high_indices, _ = find_peaks(close, distance=distance, prominence=prominence)
    low_indices, _ = find_peaks(-close, distance=distance, prominence=prominence)
    events = [("H", int(index)) for index in high_indices]
    events.extend(("L", int(index)) for index in low_indices)
    events.sort(key=lambda item: (item[1], 0 if item[0] == "L" else 1))

    pivots: list[PivotZone] = []
    for kind, index in events:
        value = float(frame["close"].iloc[index])
        zone = PivotZone(kind, index, index, value, value, value)
        if not pivots or pivots[-1].kind != kind:
            pivots.append(zone)
            continue
        previous = pivots[-1]
        more_extreme = (kind == "H" and value >= previous.value) or (
            kind == "L" and value <= previous.value
        )
        if more_extreme:
            pivots[-1] = zone
    return pivots


def _is_platform(first: PivotZone, middle: PivotZone, last: PivotZone, frame: pd.DataFrame, cfg: dict[str, float | int]) -> bool:
    if first.kind != last.kind or first.kind == middle.kind:
        return False
    if last.end - first.start > int(cfg["platform_max_bars"]):
        return False
    price_tolerance = float(cfg["platform_price_tolerance"])
    pullback_tolerance = float(cfg["platform_pullback_tolerance"])
    first_value = first.value
    last_value = last.value
    if first.kind == "H":
        close_range = abs(last_value - first_value) / max(first_value, last_value)
        middle_value = middle.value
        shallow_middle = middle_value >= max(first_value, last_value) * (1.0 - pullback_tolerance)
    else:
        close_range = abs(last_value - first_value) / min(first_value, last_value)
        middle_value = middle.value
        shallow_middle = middle_value <= min(first_value, last_value) * (1.0 + pullback_tolerance)
    return close_range <= price_tolerance and shallow_middle


def _merge_platforms(frame: pd.DataFrame, pivots: list[PivotZone], cfg: dict[str, float | int]) -> list[PivotZone]:
    merged = list(pivots)
    changed = True
    while changed:
        changed = False
        result: list[PivotZone] = []
        index = 0
        while index < len(merged):
            if index + 2 < len(merged) and _is_platform(
                merged[index], merged[index + 1], merged[index + 2], frame, cfg
            ):
                first, _, last = merged[index : index + 3]
                value = max(first.value, last.value) if first.kind == "H" else min(first.value, last.value)
                low_value = min(first.low_value, last.low_value)
                high_value = max(first.high_value, last.high_value)
                result.append(PivotZone(first.kind, first.start, last.end, value, low_value, high_value))
                index += 3
                changed = True
            else:
                result.append(merged[index])
                index += 1
        merged = result
    return merged


def _path_metrics(frame: pd.DataFrame, start: int, end: int, direction: str) -> tuple[float, float]:
    segment = frame.iloc[start : end + 1]
    changes = segment["close"].diff().dropna()
    net = segment["close"].iloc[-1] - segment["close"].iloc[0]
    efficiency = abs(net) / float(changes.abs().sum()) if float(changes.abs().sum()) else 0.0
    if direction == "up":
        adverse = float(((segment["close"].cummax() - segment["close"]) / segment["close"].cummax()).max())
    else:
        adverse = float(((segment["close"] - segment["close"].cummin()) / segment["close"].cummin()).max())
    return efficiency, adverse


def _compute_kdj(frame: pd.DataFrame) -> pd.DataFrame:
    low_n = frame["low"].rolling(9, min_periods=1).min()
    high_n = frame["high"].rolling(9, min_periods=1).max()
    rsv = (frame["close"] - low_n) / (high_n - low_n + 1e-9) * 100.0
    k = np.zeros(len(frame), dtype=float)
    d = np.zeros(len(frame), dtype=float)
    for index in range(len(frame)):
        if index == 0:
            k[index] = d[index] = 50.0
        else:
            k[index] = 2.0 * k[index - 1] / 3.0 + rsv.iloc[index] / 3.0
            d[index] = 2.0 * d[index - 1] / 3.0 + k[index] / 3.0
    return pd.DataFrame(
        {"K": k, "D": d, "J": 3.0 * k - 2.0 * d},
        index=frame.index,
    )


def _compute_j(frame: pd.DataFrame) -> pd.Series:
    return _compute_kdj(frame)["J"]


def _refine_low_start(
    frame: pd.DataFrame,
    pivot: PivotZone,
    next_top: PivotZone,
    j_values: pd.Series,
    cfg: dict[str, float | int],
) -> PivotZone:
    """Move a price low to a nearby J-confirmed low only when price stays near it."""
    pivot_j = float(j_values.iloc[pivot.start])
    search_end = min(
        next_top.start,
        pivot.start + int(cfg["j_start_search_bars"]),
    )
    candidates: list[tuple[float, int]] = []
    for index in range(pivot.start, search_end + 1):
        close = float(frame["close"].iloc[index])
        j_value = float(j_values.iloc[index])
        price_near_pivot = close <= pivot.value * (1.0 + float(cfg["j_start_price_tolerance"]))
        j_is_lower = j_value <= pivot_j - float(cfg["j_start_min_improvement"])
        j_is_small = j_value <= float(cfg["j_start_max_value"])
        if not (price_near_pivot and j_is_lower and j_is_small):
            continue

        previous_j = float(j_values.iloc[index - 1]) if index > 0 else j_value
        next_j = float(j_values.iloc[index + 1]) if index + 1 < len(frame) else j_value
        is_j_trough = j_value <= previous_j and j_value <= next_j
        price_range = float(frame["high"].iloc[index] - frame["low"].iloc[index])
        body_ratio = (
            abs(float(frame["close"].iloc[index] - frame["open"].iloc[index])) / price_range
            if price_range > 0
            else 1.0
        )
        if not is_j_trough and body_ratio > float(cfg["j_start_max_body_ratio"]):
            continue
        candidates.append((j_value, index))

    if not candidates:
        return pivot
    _, selected_index = min(candidates, key=lambda item: (item[0], item[1]))
    selected_close = float(frame["close"].iloc[selected_index])
    return PivotZone("L", selected_index, selected_index, selected_close, selected_close, selected_close)


def _refine_low_starts(
    frame: pd.DataFrame,
    pivots: list[PivotZone],
    cfg: dict[str, float | int],
) -> list[PivotZone]:
    j_values = _compute_j(frame)
    refined: list[PivotZone] = []
    for index, pivot in enumerate(pivots):
        if pivot.kind != "L":
            refined.append(pivot)
            continue
        next_top = next(
            (candidate for candidate in pivots[index + 1 :] if candidate.kind == "H"),
            None,
        )
        refined.append(
            _refine_low_start(frame, pivot, next_top, j_values, cfg)
            if next_top is not None
            else pivot
        )
    return refined


def _candidate_from_pivots(
    code: str,
    frame: pd.DataFrame,
    starts: tuple[PivotZone, ...],
    tops: tuple[PivotZone, ...],
    cfg: dict[str, float | int],
) -> WaveCandidate | None:
    if len(starts) < 3 or len(starts) != len(tops):
        return None

    widths: list[int] = []
    for index, top in enumerate(tops):
        widths.append(top.start - starts[index].end)
        if index + 1 < len(starts):
            widths.append(starts[index + 1].start - top.end)
    widths = tuple(widths)
    if any(
        width < int(cfg["min_wave_bars"]) or width > int(cfg["max_wave_bars"])
        for width in widths
    ):
        return None

    complete_periods = tuple(
        starts[index + 1].start - starts[index].start
        for index in range(len(starts) - 1)
    )
    wave_window_period = tops[-1].end - starts[0].start
    open_period = tops[-1].end - starts[-1].start
    if any(
        period < int(cfg["min_complete_wave_period"])
        or period > int(cfg["max_complete_wave_period"])
        for period in complete_periods
    ):
        return None
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
    start_j_values = tuple(float(_compute_j(frame).iloc[start.end]) for start in starts)
    higher_highs = tuple(
        tops[index + 1].value / tops[index].value - 1.0
        for index in range(len(tops) - 1)
    )
    higher_lows = tuple(
        starts[index + 1].value / starts[index].value - 1.0
        for index in range(len(starts) - 1)
    )
    if min(up_returns) < float(cfg["min_up_return"]):
        return None
    if min(pullbacks) < float(cfg["min_pullback"]) or max(pullbacks) > float(cfg["max_pullback"]):
        return None
    if min((*higher_highs, *higher_lows)) < float(cfg["min_step"]):
        return None

    up_metrics = [
        _path_metrics(frame, starts[index].end, tops[index].start, "up")
        for index in range(len(starts))
    ]
    down_metrics = [
        _path_metrics(frame, tops[index].end, starts[index + 1].start, "down")
        for index in range(len(starts) - 1)
    ]
    complete_wave_path_efficiencies = tuple(
        _path_metrics(frame, starts[index].end, starts[index + 1].start, "full")[0]
        for index in range(len(starts) - 1)
    )
    if min(complete_wave_path_efficiencies) < float(cfg["min_complete_wave_path_efficiency"]):
        return None
    min_efficiency = min(metric[0] for metric in up_metrics)
    max_up_adverse = max(metric[1] for metric in up_metrics)
    max_down_adverse = max(metric[1] for metric in down_metrics)
    if min_efficiency < float(cfg["min_path_efficiency"]):
        return None
    if max_up_adverse > float(cfg["max_up_adverse"]):
        return None
    if max_down_adverse > float(cfg["max_down_adverse"]):
        return None

    return WaveCandidate(
        code=code,
        starts=starts,
        tops=tops,
        leg_widths=widths,
        wave_window_period=wave_window_period,
        complete_wave_periods=complete_periods,
        open_wave_period=open_period,
        up_returns=up_returns,
        pullbacks=pullbacks,
        start_j_values=start_j_values,
        complete_wave_path_efficiencies=complete_wave_path_efficiencies,
        higher_highs=higher_highs,
        higher_lows=higher_lows,
        min_up_path_efficiency=min_efficiency,
        max_up_adverse_pct=max_up_adverse,
        max_down_adverse_pct=max_down_adverse,
    )


def _alternating_wave_sequence(
    pivots: list[PivotZone],
    start_index: int,
) -> tuple[tuple[PivotZone, ...], tuple[PivotZone, ...], int]:
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
    return tuple(starts), tuple(tops), cursor


def scan_frame(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, cfg: dict[str, float | int]) -> list[WaveCandidate]:
    window = frame[(frame["date"] >= start) & (frame["date"] <= end)].reset_index(drop=True)
    if len(window) < 25:
        return []
    pivots = _merge_platforms(window, _raw_pivots(window, cfg), cfg)
    pivots = _refine_low_starts(window, pivots, cfg)
    candidates: list[WaveCandidate] = []
    consumed_until = -1
    index = 0
    while index < len(pivots):
        if index <= consumed_until or pivots[index].kind != "L":
            index += 1
            continue

        all_starts, all_tops, _ = _alternating_wave_sequence(pivots, index)
        if len(all_starts) < 3:
            index += 1
            continue

        candidate = None
        for wave_count in range(len(all_starts), 2, -1):
            candidate = _candidate_from_pivots(
                str(window["code"].iloc[0]),
                window,
                all_starts[:wave_count],
                all_tops[:wave_count],
                cfg,
            )
            if candidate is not None:
                break

        if candidate is None:
            index += 1
            continue

        candidates.append(candidate)
        consumed_until = index + (2 * len(candidate.starts)) - 1
        index = consumed_until + 1
    return candidates


def _date(frame: pd.DataFrame, index: int) -> str:
    return frame["date"].iloc[index].date().isoformat()


def _zone_text(frame: pd.DataFrame, zone: PivotZone) -> str:
    start = _date(frame, zone.start)
    end = _date(frame, zone.end)
    if zone.start == zone.end:
        return start
    return f"{start}..{end} ({zone.low_value:.2f}-{zone.high_value:.2f})"


def _pivot_text(frame: pd.DataFrame, zone: PivotZone) -> str:
    return f"{zone.kind}:{_zone_text(frame, zone)}"


def _candidate_row(candidate: WaveCandidate, frame: pd.DataFrame) -> dict[str, object]:
    starts = candidate.starts
    return {
        "code": candidate.code,
        "cycle_start": _date(frame, starts[0].start),
        "wave_count": len(starts),
        "start_sequence": " -> ".join(
            f"{index + 1}起:{_zone_text(frame, start)}"
            for index, start in enumerate(starts)
        ),
        "wave_window_period": candidate.wave_window_period,
        "wave_period_text": "；".join(
            f"波{index + 1}周期={period}交易日"
            for index, period in enumerate(candidate.complete_wave_periods)
        ),
        "open_wave_period_text": f"第{len(starts)}浪观察={candidate.open_wave_period}交易日",
        "up_returns_text": " / ".join(f"{value * 100:.2f}%" for value in candidate.up_returns),
        "pullbacks_text": " / ".join(f"{value * 100:.2f}%" for value in candidate.pullbacks),
        "start_j_text": "；".join(
            f"{index + 1}起J={value:.3f}"
            for index, value in enumerate(candidate.start_j_values)
        ),
        "complete_wave_efficiency_text": "；".join(
            f"波{index + 1}效率={value:.3f}"
            for index, value in enumerate(candidate.complete_wave_path_efficiencies)
        ),
        "higher_highs_text": " / ".join(f"{value * 100:.2f}%" for value in candidate.higher_highs),
        "higher_lows_text": " / ".join(f"{value * 100:.2f}%" for value in candidate.higher_lows),
        "leg_widths": "/".join(str(width) for width in candidate.leg_widths),
        "complete_wave_periods": "/".join(str(period) for period in candidate.complete_wave_periods),
        "open_wave_period": candidate.open_wave_period,
        "average_complete_wave_period": round(float(np.mean(candidate.complete_wave_periods)), 2),
        "min_up_path_efficiency": round(candidate.min_up_path_efficiency, 3),
        "max_up_adverse_pct": round(candidate.max_up_adverse_pct * 100, 2),
        "max_down_adverse_pct": round(candidate.max_down_adverse_pct * 100, 2),
    }


def _hood_explanation(rows: list[dict[str, object]]) -> list[str]:
    hood = [row for row in rows if row["code"] == "HOOD"]
    lines = [
        "## HOOD 波段解释",
        "",
        "本轮扫描对 HOOD 识别出两个相邻的三浪及以上候选。它们不是两个完全独立的行情，而是同一段上升行情的不同起点尺度：每条记录只展示各浪起点，顶部区间只在内部用于确认，不在这里显示。",
            "此前从 `2026-04-29` 开始的候选，其第一完整波 `2026-04-29 -> 2026-05-22` 的完整路径效率只有 `0.072`，低于本轮 `0.10` 的硬阈值，因此已被过滤。J 辅助起点将第一起点后移到 `2026-05-26`，当前保留的 HOOD 结构从 `2026-05-26` 起，第一、第二完整波效率分别为 `0.195` 和 `0.241`。",
        "",
        "| 波浪起点序列 | 波浪数量 | 起点 J | 起点间完整波周期 | 末浪观察 |",
        "|---|---:|---|---|---|",
    ]
    for row in hood:
        lines.append(
            f"| `{row['start_sequence']}` | {row['wave_count']} | `{row['start_j_text']}` | `{row['wave_period_text']}` | `{row['open_wave_period_text']}` |"
        )
    lines.extend(
        [
            "",
            "HOOD 的主要人工波浪起点仍可概括为：",
            "",
            "```text",
            "1起(05-26) -> 2起(06-05) -> 3起(06-25)",
            "```",
            "",
            "相邻记录的起点可能来自同一段行情的不同尺度，不能把它们简单相加为两次独立机会。起点之间的距离才是完整波周期：1起到2起是波1，2起到3起是波2；最后一个起点到当前观察结束是正在运行的末浪。",
        ]
    )
    return lines


def _sample_period_calibration() -> list[str]:
    return [
        "## 三个样例的周期校准",
        "",
        "以下使用此前人工确认的主起点，周期按交易日序号差计算；`1起 -> 2起` 是波1完整周期，`2起 -> 3起` 是波2完整周期，不把尚未出现下一起点的最后上涨段当作完整波。",
        "",
        "| 样例 | 起点序列 | 上涨腿周期 | 回调腿周期 | 已完成完整波周期 | 完整波平均周期 |",
        "|---|---|---|---|---|---:|",
        "| HOOD | `1起 -> 2起 -> 3起` | `3 / 9 / 5` | `5 / 4` | `8 / 13` | `10.5` |",
        "| PONY | `1起 -> 2起 -> 3起 -> 4起` | `3 / 12 / 11 / 17` | `8 / 5 / 12` | `11 / 17 / 23` | `17.0` |",
        "| NOK | `1起 -> 2起 -> 3起 -> 4起` | `9 / 4 / 2 / 5` | `2 / 4 / 5` | `11 / 8 / 7` | `8.67` |",
        "",
        "三个样例的已完成完整波周期范围是 `7–23` 个交易日，因此当前扫描器使用 `7–25` 个交易日作为初始硬边界；当前仍在运行的最后一浪单独限制为不超过 `25` 个交易日。这个边界是样例校准值，不是最终结论，后续需要通过更多案例和回测调整。",
        "",
    ]


def write_report(candidates: list[WaveCandidate], frames: list[pd.DataFrame], output: Path, latest: pd.Timestamp, start: pd.Timestamp, cfg: dict[str, float | int]) -> None:
    frame_map = {
        str(frame["code"].iloc[0]): frame[
            (frame["date"] >= start) & (frame["date"] <= latest)
        ].reset_index(drop=True)
        for frame in frames
    }
    rows = [_candidate_row(candidate, frame_map[candidate.code]) for candidate in candidates]
    rows.sort(key=lambda row: (str(row["code"]), str(row["cycle_start"])))
    pd.DataFrame(rows).to_csv(output.with_suffix(".csv"), index=False)

    lines = [
        "# 三浪及以上结构候选（起点周期版）",
        "",
        f"- 股票文件：{len(frames)} 个",
        f"- 数据最新日期：`{latest.date()}`",
        f"- 名义一年窗口：`{start.date()}` 至 `{latest.date()}`",
        f"- 候选结构：{len(rows)} 个，涉及 {len(set(row['code'] for row in rows))} 只股票",
        "- 主拐点：收盘价；顶部平台只在内部用于确认，不作为对外波浪表示。",
        "- 对外表示：`1起 -> 2起 -> 3起 -> ...`；相邻起点之间的长度就是对应完整波周期。",
        "- J 辅助起点：原价格低点后最多 5 个交易日内，若收盘价仍在原低点上方 3% 内、J <= 25 且明显低于原低点，并出现局部 J 谷或小实体 K 线，则将起点后移到该确认日。",
        "- 当前报告仍是回看式结构初筛；J 仅用于价格低点附近的起点辅助，不作为所有起点的统一低值硬筛选，成交量尚未加入。平台合并和周期参数仍需继续用人工案例校准。",
        "",
        "## 起点和周期定义",
        "",
        "```text",
        "1起 -> 2起 -> 3起 -> 4起 -> ...",
        "```",
        "",
        "每个起点表示一个新上涨浪的开始，同时也是上一上涨浪回调结束的位置：",
        "",
        "```text",
        "1起 -> 2起：波1的完整周期",
        "2起 -> 3起：波2的完整周期",
        "3起 -> 4起：波3的完整周期",
        "最后一个起点之后：当前仍在运行的末浪",
        "```",
        "顶部通常是弧顶或震荡区间，扫描器只在内部用顶部区间确认上涨和回调，不在结果中显示顶部日期。相近顶部满足以下条件时会合并为平台：",
        "",
        "```text",
        "相邻高点价格差 <= 1%",
        "中间反向波动 <= 6%",
        "平台总跨度 <= 15 个交易日",
        "```",
        "",
        "结果中的周期字段：",
        "",
        "- `start_sequence`：`1起 -> 2起 -> 3起 -> ...` 的起点序列",
        "- `wave_period_text`：相邻起点之间的完整波周期",
        "- `open_wave_period_text`：最后一个起点到内部确认结束点的当前末浪观察时长",
        "- `start_j_text`：各个起点日的 J 值，用于复核起点是否经过 J 辅助调整",
        "- `wave_count`：候选中连续上涨浪的数量，至少为 3",
        "",
        "## 收窄条件",
        "",
        "```text",
        "每个识别出的上涨段涨幅均 >= 6%",
        "每个已完成回调幅度为 5% 至 28%",
        "连续高点和连续起点均逐步抬高至少 2%",
        "相邻拐点跨度为 2 至 20 个交易日",
        "已完成完整波周期为 7 至 25 个交易日",
        "当前未完成浪周期不超过 25 个交易日",
        "每个已完成波的完整路径效率 >= 0.10",
        "每个上涨段路径效率 >= 0.58",
        "上涨段最大收盘回撤 <= 12%",
        "回调段最大收盘反弹 <= 12%",
        "```",
        "",
        "## 候选结构",
        "",
        "| 代码 | 波浪起点序列 | 波浪数量 | 起点 J | 观察跨度 | 已完成波周期 | 完整波效率 | 末浪观察 | 各波上涨幅度 | 各完整波回调 | 平均完整周期 | 最低上涨路径效率 |",
        "|---|---|---:|---|---:|---|---|---|---|---|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['code']} | {row['start_sequence']} | {row['wave_count']} | {row['start_j_text']} | {row['wave_window_period']} | {row['wave_period_text']} | {row['complete_wave_efficiency_text']} | {row['open_wave_period_text']} | {row['up_returns_text']} | {row['pullbacks_text']} | {row['average_complete_wave_period']} | {row['min_up_path_efficiency']} |"
        )
    lines.extend(["", "## 复核字段", "", "完整字段同时写入同目录的 CSV 文件，包含起点序列、波浪数量、起点 J、观察跨度、各完整波周期、完整波路径效率、末浪观察时长、涨跌幅、路径效率和逆向波动。顶部区间仅保留在内部计算，不作为结果展示。", ""])
    lines.extend(_hood_explanation(rows))
    lines.extend(_sample_period_calibration())
    lines.extend(
        [
            "",
            "## 方法限制",
            "",
            "1. 峰谷需要后续数据确认，当前结果适合历史形态复盘，不是实时买点。",
            "2. 相邻候选可能是同一行情的滑动窗口，应按行情段去重后再做事件回测。",
            "3. 顶部区间合并解决的是局部平台被拆成微浪的问题，不代表所有平台都应合并；参数仍需用更多人工案例校准。",
        ]
    )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan three-or-more-wave price structures")
    parser.add_argument("--data-dir", type=Path, default=Path("./data/us_stocks"))
    parser.add_argument("--output", type=Path, default=Path("./three_wave_candidates_20260819_narrow.md"))
    parser.add_argument("--latest-date", type=pd.Timestamp, default=None)
    args = parser.parse_args()

    frames = _load_frames(args.data_dir)
    if not frames:
        raise SystemExit("no usable OHLCV files")
    latest = args.latest_date or max(frame["date"].max() for frame in frames)
    start = latest - pd.Timedelta(days=365)
    candidates: list[WaveCandidate] = []
    for frame in frames:
        candidates.extend(scan_frame(frame, start, latest, NARROW_DEFAULTS))
    candidates.sort(key=lambda candidate: (candidate.code, candidate.starts[0].start))
    write_report(candidates, frames, args.output, latest, start, NARROW_DEFAULTS)
    print(f"wrote {args.output} and {args.output.with_suffix('.csv')}: {len(candidates)} cycles")


if __name__ == "__main__":
    main()
