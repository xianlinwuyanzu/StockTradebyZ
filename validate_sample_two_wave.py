from __future__ import annotations

from pathlib import Path

import pandas as pd

from backtest_two_wave_j3 import DEFAULTS, find_first_j_entry, find_setups, load_frames, validate_two_wave_structure, _compute_j


SAMPLES = {
    "AJG": {
        "reference_start": "2026-05-13",
        "reference_end": "2026-06-11",
        "manual_points": ["2026-05-13", "2026-05-21", "2026-05-29", "2026-06-11"],
        "description": "AJG 样例：05-13 -> 05-21 -> 05-29 -> 06-11；价格回调不足 4%，但 J 显著下降。",
    },
    "HOOD": {
        "reference_start": "2026-05-26",
        "reference_end": "2026-07-08",
        "manual_points": ["2026-05-26", "2026-05-29", "2026-06-05", "2026-06-18"],
        "description": "HOOD 主样例的前两波：05-26 -> 05-29 -> 06-05 -> 06-18。",
    },
    "PONY": {
        "reference_start": "2025-06-23",
        "reference_end": "2025-09-29",
        "manual_points": ["2025-06-23", "2025-06-26", "2025-07-09", "2025-07-25"],
        "description": "PONY 样例的前两波：06-23 -> 06-26 -> 07-09 -> 07-25。",
    },
    "NOK": {
        "reference_start": "2026-01-28",
        "reference_end": "2026-06-03",
        "manual_points": ["2026-01-29", "2026-02-11", "2026-02-13", "2026-02-20"],
        "description": "NOK 样例的前两波：01-29 -> 02-11 -> 02-13 -> 02-20。",
    },
}


def metrics(frame: pd.DataFrame, points: list[str]) -> dict[str, object]:
    indices = [int(frame.index[frame.date == pd.Timestamp(date)][0]) for date in points]
    start1, top1, start2, top2 = indices
    close = frame["close"]
    values = [float(close.iloc[index]) for index in indices]
    widths = [top1 - start1, start2 - top1, top2 - start2]
    first_return = values[1] / values[0] - 1.0
    second_return = values[3] / values[2] - 1.0
    pullback = (values[1] - values[2]) / values[1]
    higher_high = values[3] / values[1] - 1.0
    higher_low = values[2] / values[0] - 1.0
    full_efficiency = abs(values[2] - values[0]) / float(close.iloc[start1:start2 + 1].diff().abs().sum())
    first_up_efficiency = abs(values[1] - values[0]) / float(close.iloc[start1:top1 + 1].diff().abs().sum())
    second_up_efficiency = abs(values[3] - values[2]) / float(close.iloc[start2:top2 + 1].diff().abs().sum())
    j_values = _compute_j(frame)
    point_j_values = [float(j_values.iloc[index]) for index in indices]
    j_pullback_drop = point_j_values[1] - point_j_values[2]
    price_pullback_valid = DEFAULTS["min_pullback"] <= pullback <= DEFAULTS["max_pullback"]
    j_pullback_valid = j_pullback_drop >= DEFAULTS["min_j_pullback_drop"]
    return {
        "indices": indices,
        "values": values,
        "widths": widths,
        "first_return": first_return,
        "second_return": second_return,
        "pullback": pullback,
        "higher_high": higher_high,
        "higher_low": higher_low,
        "full_efficiency": full_efficiency,
        "first_up_efficiency": first_up_efficiency,
        "second_up_efficiency": second_up_efficiency,
        "j_values": point_j_values,
        "j_pullback_drop": float(j_pullback_drop),
        "pullback_condition_passes": bool(
            pullback <= DEFAULTS["max_pullback"] and (price_pullback_valid or j_pullback_valid)
        ),
        "passes_current": validate_two_wave_structure(frame, start1, top1, start2, top2, DEFAULTS),
        "j_search": find_first_j_entry(frame, j_values, start2, top2, DEFAULTS),
    }


def reason(m: dict[str, object]) -> list[str]:
    failures: list[str] = []
    widths = m["widths"]
    if any(width < DEFAULTS["min_wave_bars"] or width > DEFAULTS["max_wave_bars"] for width in widths):
        failures.append("单段周期不在 2 至 20 个交易日")
    if not DEFAULTS["min_complete_wave_period"] <= widths[0] + widths[1] <= DEFAULTS["max_complete_wave_period"]:
        failures.append("第一完整波周期不在 7 至 25 个交易日")
    if m["first_return"] < DEFAULTS["min_up_return"] or m["second_return"] < DEFAULTS["min_up_return"]:
        failures.append("上涨段涨幅低于 6%")
    price_pullback_valid = DEFAULTS["min_pullback"] <= m["pullback"] <= DEFAULTS["max_pullback"]
    j_pullback_valid = m["j_pullback_drop"] >= DEFAULTS["min_j_pullback_drop"]
    if m["pullback"] > DEFAULTS["max_pullback"] or not (price_pullback_valid or j_pullback_valid):
        failures.append(
            f"第一回调幅度 {m['pullback'] * 100:.2f}%，J 下降 {m['j_pullback_drop']:.2f}，均未达到价格 4% 或 J 下降 {DEFAULTS['min_j_pullback_drop']:.0f}"
        )
    if m["higher_high"] < DEFAULTS["min_step"]:
        failures.append("第二顶部没有至少抬高 2%")
    if m["higher_low"] < DEFAULTS["min_step"]:
        failures.append("第二起点没有至少抬高 2%")
    if m["full_efficiency"] < DEFAULTS["min_complete_wave_path_efficiency"]:
        failures.append(f"第一完整波路径效率 {m['full_efficiency']:.3f} 低于 0.10")
    if m["first_up_efficiency"] < DEFAULTS["min_path_efficiency"]:
        failures.append(f"第一上涨腿路径效率 {m['first_up_efficiency']:.3f} 低于 0.58")
    if m["second_up_efficiency"] < DEFAULTS["min_path_efficiency"]:
        failures.append(f"第二上涨腿路径效率 {m['second_up_efficiency']:.3f} 低于 0.58")
    return failures


def main() -> None:
    output_dir = Path("./sample_two_wave_validation_20260820")
    output_dir.mkdir(parents=True, exist_ok=True)
    frames = {frame.code.iloc[0]: frame for frame in load_frames(Path("./data/us_stocks"))}
    rows: list[dict[str, object]] = []
    lines = [
        "# 三个样例的二波识别验证",
        "",
        "本报告使用已补入 `data/us_stocks` 的本地数据和当前 `find_setups` 二波硬条件，按三个样例的人工拐点逐项验证；同时记录算法在对应参考区间内是否找到自动候选。",
        "",
        "## 当前数据",
        "",
    ]
    for code, sample in SAMPLES.items():
        frame = frames.get(code)
        if frame is None:
            rows.append({
                "code": code,
                "data_start": "",
                "data_end": "",
                "reference_start": sample["reference_start"],
                "reference_end": sample["reference_end"],
                "manual_structure_passes": False,
                "manual_failure_reasons": "本地没有该股票数据",
                "automatic_setup_found": False,
            })
            lines.extend([f"### {code}", "", "- 本地没有该股票数据，跳过人工点位验证。", ""])
            continue
        end = pd.Timestamp(sample["reference_end"])
        usable = frame[frame.date <= end].reset_index(drop=True)
        missing_points = [date for date in sample["manual_points"] if not (usable.date == pd.Timestamp(date)).any()]
        if missing_points:
            rows.append({
                "code": code,
                "data_start": frame.date.min().date().isoformat(),
                "data_end": frame.date.max().date().isoformat(),
                "reference_start": sample["reference_start"],
                "reference_end": sample["reference_end"],
                "manual_structure_passes": False,
                "manual_failure_reasons": f"本地数据缺少人工点位：{', '.join(missing_points)}",
                "automatic_setup_found": False,
            })
            lines.extend([
                f"### {code}",
                "",
                f"- 数据覆盖：`{frame.date.min().date()}` 至 `{frame.date.max().date()}`",
                f"- 人工点位无法验证，本地数据缺少：`{', '.join(missing_points)}`",
                "- 该样例不计入当前规则通过率。",
                "",
            ])
            continue
        manual = metrics(usable, sample["manual_points"])
        failures = reason(manual)
        automatic = find_first_matching_setup(usable, sample["reference_start"], sample["reference_end"])
        rows.append({
            "code": code,
            "data_start": frame.date.min().date().isoformat(),
            "data_end": frame.date.max().date().isoformat(),
            "reference_start": sample["reference_start"],
            "reference_end": sample["reference_end"],
            "manual_structure_passes": manual["passes_current"],
            "manual_failure_reasons": "; ".join(failures),
            "automatic_setup_found": automatic is not None,
            "manual_full_wave_efficiency": manual["full_efficiency"],
            "manual_first_up_efficiency": manual["first_up_efficiency"],
            "manual_second_up_efficiency": manual["second_up_efficiency"],
            "manual_pullback": manual["pullback"],
            "manual_j_pullback_drop": manual["j_pullback_drop"],
            "manual_pullback_condition_passes": manual["pullback_condition_passes"],
            "manual_j_values": "/".join(f"{value:.3f}" for value in manual["j_values"]),
        })
        lines.extend([
            f"### {code}",
            "",
            f"- 数据覆盖：`{frame.date.min().date()}` 至 `{frame.date.max().date()}`",
            f"- 参考区间：`{sample['reference_start']}` 至 `{sample['reference_end']}`",
            f"- 人工拐点：`{' -> '.join(sample['manual_points'])}`",
            f"- 人工结构是否通过当前硬条件：`{'通过' if not failures else '不通过'}`",
            f"- 当前算法是否自动找到候选：`{'是' if automatic is not None else '否'}`",
            f"- J 值（四个参考点）：`{' / '.join(f'{value:.3f}' for value in manual['j_values'])}`",
            f"- 完整波路径效率：`{manual['full_efficiency']:.3f}`",
            f"- 第一上涨腿路径效率：`{manual['first_up_efficiency']:.3f}`",
            f"- 第二上涨腿路径效率：`{manual['second_up_efficiency']:.3f}`",
            f"- 第一回调幅度：`{manual['pullback'] * 100:.2f}%`",
            f"- 第一回调 J 下降：`{manual['j_pullback_drop']:.2f}`",
            f"- 第一回调 OR 条件是否通过：`{'是' if manual['pullback_condition_passes'] else '否'}`",
        ])
        if failures:
            lines.append(f"- 未通过原因：`{'；'.join(failures)}`")
        if automatic is not None:
            lines.append(f"- 自动候选起点：`{' -> '.join(automatic)}`")
        lines.extend(["", sample["description"], ""])

    pd.DataFrame(rows).to_csv(output_dir / "sample_two_wave_validation.csv", index=False)
    lines.extend([
        "## 综合结论",
        "",
        "- 回调门槛采用 OR：价格回调达到 4%，或第一顶部到 2起的 J 下降达到显著变化阈值。",
        "- 数据覆盖不完整的样例只报告缺失日期，不把数据缺口误判为规则失败。",
    ])
    (output_dir / "sample_two_wave_validation.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {output_dir / 'sample_two_wave_validation.md'} and csv")


def find_first_matching_setup(frame: pd.DataFrame, reference_start: str, reference_end: str) -> list[str] | None:
    for setup in find_setups(frame, DEFAULTS):
        start = frame.date.iloc[setup.start1]
        end = frame.date.iloc[setup.top2]
        if start >= pd.Timestamp(reference_start) - pd.Timedelta(days=10) and end <= pd.Timestamp(reference_end):
            return [
                frame.date.iloc[setup.start1].date().isoformat(),
                frame.date.iloc[setup.start2].date().isoformat(),
                frame.date.iloc[setup.top2].date().isoformat(),
            ]
    return None


if __name__ == "__main__":
    main()
