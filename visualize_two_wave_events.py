from __future__ import annotations

import argparse
import html
from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd


FONT_CANDIDATES = [
    "Noto Sans CJK SC",
    "Noto Sans CJK JP",
    "WenQuanYi Micro Hei",
    "Source Han Sans SC",
    "Microsoft YaHei",
    "SimHei",
    "DejaVu Sans",
]


COLORS = {
    "line": "#174A68",
    "start1": "#0B7A75",
    "start2": "#D97706",
    "j_entry": "#C2417A",
    "top": "#64748B",
    "pre": "#EEF2F7",
    "wave": "#DCEEFF",
    "post": "#E6F4EA",
    "grid": "#CBD5E1",
}


def configure_font() -> str:
    installed = {font.name for font in font_manager.fontManager.ttflist}
    selected = next((name for name in FONT_CANDIDATES if name in installed), "DejaVu Sans")
    plt.rcParams["font.family"] = [selected, "DejaVu Sans", "sans-serif"]
    plt.rcParams["axes.unicode_minus"] = False
    return selected


def load_events(path: Path) -> pd.DataFrame:
    date_columns = [
        "start1_date",
        "top1_date",
        "start2_date",
        "top2_date",
        "j_entry_date",
    ]
    events = pd.read_csv(path, parse_dates=date_columns)
    events.insert(0, "event_id", np.arange(1, len(events) + 1))
    return events


def load_frame(data_dir: Path, code: str) -> pd.DataFrame | None:
    path = data_dir / f"{code}.csv"
    if not path.exists():
        return None
    frame = pd.read_csv(path, parse_dates=["date"])
    required = {"date", "open", "close", "high", "low", "volume"}
    if not required.issubset(frame.columns):
        return None
    frame = frame.dropna(subset=["date", "open", "close", "high", "low"]).copy()
    for column in ["open", "close", "high", "low", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["open", "close", "high", "low"]).sort_values("date").drop_duplicates("date").reset_index(drop=True)


def locate(frame: pd.DataFrame, value: Any) -> int | None:
    if pd.isna(value):
        return None
    matches = frame.index[frame["date"] == pd.Timestamp(value)]
    return int(matches[0]) if len(matches) else None


def bool_text(value: Any) -> str:
    if pd.isna(value):
        return "未判定"
    return "是" if bool(value) else "否"


def pct_text(value: Any) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value) * 100:.2f}%"


def plot_event(
    event: pd.Series,
    frame: pd.DataFrame,
    output_path: Path,
    pre_days: int,
    post_days: int,
) -> dict[str, Any] | None:
    start1 = locate(frame, event["start1_date"])
    top1 = locate(frame, event["top1_date"])
    start2 = locate(frame, event["start2_date"])
    top2 = locate(frame, event["top2_date"])
    j_entry = locate(frame, event["j_entry_date"])
    if None in (start1, top1, start2, top2):
        return None
    assert start1 is not None and top1 is not None and start2 is not None and top2 is not None

    segment_start = max(0, start1 - pre_days)
    segment_end = min(len(frame) - 1, top2 + post_days)
    segment = frame.iloc[segment_start : segment_end + 1].copy().reset_index(drop=True)
    x = mdates.date2num(segment["date"].to_numpy(dtype="datetime64[ms]"))

    relative = {
        "start1": start1 - segment_start,
        "top1": top1 - segment_start,
        "start2": start2 - segment_start,
        "top2": top2 - segment_start,
    }
    if j_entry is not None and segment_start <= j_entry <= segment_end:
        relative["j_entry"] = j_entry - segment_start

    pre_end = relative["start1"]
    wave_end = relative["top2"]
    post_end = len(segment) - 1

    fig, ax = plt.subplots(figsize=(13.5, 6.8))
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#FFFFFF")
    ax.axvspan(x[0], x[pre_end], color=COLORS["pre"], alpha=0.95, zorder=0)
    ax.axvspan(x[pre_end], x[wave_end], color=COLORS["wave"], alpha=0.75, zorder=0)
    if post_end > wave_end:
        ax.axvspan(x[wave_end], x[post_end], color=COLORS["post"], alpha=0.8, zorder=0)

    ax.plot(x, segment["close"], color=COLORS["line"], linewidth=2.0, marker="o", markersize=3.2, zorder=3)

    ax.scatter(x[relative["start1"]], segment["close"].iloc[relative["start1"]], s=90, color=COLORS["start1"], zorder=5, edgecolor="white", linewidth=1.2)
    ax.scatter(x[relative["start2"]], segment["close"].iloc[relative["start2"]], s=90, color=COLORS["start2"], zorder=5, edgecolor="white", linewidth=1.2)
    if "j_entry" in relative:
        ax.scatter(x[relative["j_entry"]], segment["close"].iloc[relative["j_entry"]], s=150, color=COLORS["j_entry"], marker="*", zorder=6, edgecolor="white", linewidth=1.0)

    for key, label in [("top1", "内部顶部1"), ("top2", "内部顶部2")]:
        point_index = relative[key]
        xpos = x[point_index]
        ax.scatter(xpos, segment["close"].iloc[point_index], s=65, color=COLORS["top"], marker="^", zorder=5, edgecolor="white", linewidth=0.8)
        ax.axvline(xpos, color=COLORS["top"], linewidth=0.8, linestyle="--", alpha=0.65, zorder=1)
        ax.annotate(label, (xpos, segment["close"].iloc[point_index]), xytext=(0, 12), textcoords="offset points", ha="center", fontsize=8, color=COLORS["top"])

    for key, label, color in [("start1", "1起", COLORS["start1"]), ("start2", "2起", COLORS["start2"]), ("j_entry", "3起/J买点", COLORS["j_entry"])]:
        if key not in relative:
            continue
        point_index = relative[key]
        xpos = x[point_index]
        ypos = float(segment["close"].iloc[point_index])
        ax.annotate(label, (xpos, ypos), xytext=(0, -19), textcoords="offset points", ha="center", fontsize=9, fontweight="bold", color=color)
        ax.axvline(xpos, color=color, linewidth=1.0, alpha=0.55, zorder=1)

    ax.text((x[0] + x[pre_end]) / 2, 0.96, f"前{pre_days}交易日", transform=ax.get_xaxis_transform(), ha="center", va="top", fontsize=9, color="#475569")
    ax.text((x[pre_end] + x[wave_end]) / 2, 0.96, f"二波周期：{wave_end - pre_end}交易日", transform=ax.get_xaxis_transform(), ha="center", va="top", fontsize=9, color="#1D4ED8", fontweight="bold")
    if post_end > wave_end:
        actual_post = post_end - wave_end
        ax.text((x[wave_end] + x[post_end]) / 2, 0.96, f"后续{actual_post}交易日", transform=ax.get_xaxis_transform(), ha="center", va="top", fontsize=9, color="#166534")

    ax.set_xlim(x[0], x[-1])
    ax.xaxis_date()
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=12))
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
    ax.grid(axis="y", color=COLORS["grid"], linewidth=0.7, alpha=0.65)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color("#94A3B8")
    ax.set_ylabel("收盘价")

    code = str(event["code"])
    event_id = int(event["event_id"])
    j_value = event.get("j_value")
    info = (
        f"二波事件 #{event_id} | {code}\n"
        f"1起 {event['start1_date'].date()} -> 2起 {event['start2_date'].date()} -> 第二顶部 {event['top2_date'].date()}\n"
        f"J买点 {event['j_entry_date'].date() if pd.notna(event['j_entry_date']) else '-'} | J={float(j_value):.2f} | "
        f"第三浪突破：{bool_text(event['third_wave_breakout'])}"
    )
    ax.set_title(info, loc="left", fontsize=12, pad=18, color="#0F172A", fontweight="bold")
    legend_items = [
        Line2D([0], [0], color=COLORS["line"], marker="o", linewidth=2, markersize=4, label="每日收盘价"),
        Line2D([0], [0], marker="o", color=COLORS["start1"], linestyle="None", markersize=8, label="1起"),
        Line2D([0], [0], marker="o", color=COLORS["start2"], linestyle="None", markersize=8, label="2起"),
        Line2D([0], [0], marker="*", color=COLORS["j_entry"], linestyle="None", markersize=11, label="3起/J买点"),
        Line2D([0], [0], marker="^", color=COLORS["top"], linestyle="None", markersize=7, label="内部顶部"),
    ]
    ax.legend(handles=legend_items, loc="upper left", bbox_to_anchor=(0, -0.14), ncol=5, frameon=False, fontsize=8)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    return {
        "event_id": event_id,
        "code": code,
        "start1_date": event["start1_date"].date().isoformat(),
        "top1_date": event["top1_date"].date().isoformat(),
        "start2_date": event["start2_date"].date().isoformat(),
        "top2_date": event["top2_date"].date().isoformat(),
        "j_entry_date": "" if pd.isna(event["j_entry_date"]) else event["j_entry_date"].date().isoformat(),
        "j_value": event.get("j_value"),
        "pre_days_available": relative["start1"],
        "wave_period_days": wave_end - pre_end,
        "post_days_available": post_end - wave_end,
        "third_wave_breakout": event.get("third_wave_breakout"),
        "plot": output_path.name,
    }


def write_gallery(output_dir: Path, rows: list[dict[str, Any]], font_name: str, pre_days: int, post_days: int) -> None:
    cards = []
    for row in rows:
        code = html.escape(str(row["code"]))
        event_id = int(row["event_id"])
        j_date = row["j_entry_date"] or "-"
        breakout = "是" if bool(row["third_wave_breakout"]) else "否"
        cards.append(
            f"""<article class="card" data-code="{code}">
<a href="plots/{html.escape(row['plot'])}"><img loading="lazy" src="plots/{html.escape(row['plot'])}" alt="{code} 二波事件 {event_id}"></a>
<div class="meta"><strong>#{event_id} {code}</strong><br>
1起 {row['start1_date']} | 2起 {row['start2_date']} | 顶部 {row['top2_date']}<br>
J买点 {j_date} | 波段 {row['wave_period_days']} 日 | 突破 {breakout}</div></article>"""
        )
    content = "\n".join(cards)
    page = f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
<meta charset=\"utf-8\">
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>二波收盘价波形画廊</title>
<style>
:root {{ color-scheme: light; font-family: {html.escape(font_name)}, sans-serif; }}
body {{ margin: 0; color: #0f172a; background: #f8fafc; }}
header {{ position: sticky; top: 0; z-index: 2; padding: 18px 24px; background: rgba(248,250,252,.96); border-bottom: 1px solid #cbd5e1; backdrop-filter: blur(8px); }}
h1 {{ margin: 0 0 8px; font-size: 21px; }}
p {{ margin: 4px 0; color: #475569; font-size: 13px; }}
.controls {{ display: flex; gap: 10px; align-items: center; margin-top: 12px; flex-wrap: wrap; }}
input {{ width: min(320px, 80vw); padding: 9px 11px; border: 1px solid #94a3b8; border-radius: 6px; font: inherit; }}
main {{ padding: 22px; }}
.gallery {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(360px, 1fr)); gap: 18px; }}
.card {{ background: white; border: 1px solid #dbe3ec; border-radius: 7px; overflow: hidden; box-shadow: 0 2px 8px rgba(15,23,42,.06); }}
.card img {{ display: block; width: 100%; height: auto; border-bottom: 1px solid #e2e8f0; }}
.meta {{ padding: 10px 12px 12px; line-height: 1.65; font-size: 12px; color: #475569; }}
.meta strong {{ color: #0f172a; font-size: 14px; }}
.hidden {{ display: none; }}
</style>
</head>
<body>
<header>
<h1>二波收盘价波形画廊</h1>
<p>共 {len(rows)} 条事件。每张图：前 {pre_days} 个交易日 + 二波周期 + 第二顶部后最多 {post_days} 个交易日。</p>
<p>图中标记：1起、2起、J买点/预判3起；顶部仅作内部参考标记。</p>
<div class=\"controls\"><input id=\"filter\" placeholder=\"输入股票代码筛选，例如 ORCL 或 HOOD\" oninput=\"filterCards()\"><span id=\"count\">显示 {len(rows)} 条</span></div>
</header>
<main><section class=\"gallery\">{content}</section></main>
<script>
function filterCards() {{
  const query = document.getElementById('filter').value.trim().toUpperCase();
  let visible = 0;
  document.querySelectorAll('.card').forEach(card => {{
    const show = !query || card.dataset.code.toUpperCase().includes(query);
    card.classList.toggle('hidden', !show);
    if (show) visible++;
  }});
  document.getElementById('count').textContent = `显示 ${{visible}} 条`;
}}
</script>
</body>
</html>
"""
    (output_dir / "index.html").write_text(page, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Visualize two-wave close-price events")
    parser.add_argument("--events", type=Path, default=Path("./backtest_two_wave_j3_20260820/two_wave_events.csv"))
    parser.add_argument("--data-dir", type=Path, default=Path("./data/us_stocks"))
    parser.add_argument("--output-dir", type=Path, default=Path("./two_wave_visualizations_20260821"))
    parser.add_argument("--pre-days", type=int, default=10)
    parser.add_argument("--post-days", type=int, default=40)
    args = parser.parse_args()

    font_name = configure_font()
    events = load_events(args.events)
    plots_dir = args.output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    frame_cache: dict[str, pd.DataFrame | None] = {}
    rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for _, event in events.iterrows():
        code = str(event["code"])
        if code not in frame_cache:
            frame_cache[code] = load_frame(args.data_dir, code)
        frame = frame_cache[code]
        if frame is None:
            skipped.append({"event_id": int(event["event_id"]), "code": code, "reason": "missing_data_file"})
            continue
        filename = f"{int(event['event_id']):04d}_{code}_{event['start1_date']:%Y%m%d}_{event['top2_date']:%Y%m%d}.png"
        row = plot_event(event, frame, plots_dir / filename, args.pre_days, args.post_days)
        if row is None:
            skipped.append({"event_id": int(event["event_id"]), "code": code, "reason": "missing_pivot_date"})
            continue
        rows.append(row)

    summary = pd.DataFrame(rows)
    summary.to_csv(args.output_dir / "visualization_index.csv", index=False)
    pd.DataFrame(skipped).to_csv(args.output_dir / "skipped_events.csv", index=False)
    write_gallery(args.output_dir, rows, font_name, args.pre_days, args.post_days)
    (args.output_dir / "README.md").write_text(
        "\n".join(
            [
                "# 二波可视化输出",
                "",
                f"- 事件图：{len(rows)} 张",
                f"- 跳过事件：{len(skipped)} 条",
                f"- 每张图包含：前 {args.pre_days} 个交易日、完整二波周期、第二顶部后最多 {args.post_days} 个交易日",
                "- 入口：`index.html`",
                "- 图像目录：`plots/`",
                "- 索引：`visualization_index.csv`",
                "- 跳过清单：`skipped_events.csv`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"generated={len(rows)} skipped={len(skipped)} output={args.output_dir} font={font_name}")


if __name__ == "__main__":
    main()
