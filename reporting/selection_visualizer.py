from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Circle, Patch
import numpy as np
import pandas as pd


PALETTE = (
    "#176B87",
    "#D96C06",
    "#2E8B57",
    "#A23B72",
    "#5C6BC0",
    "#B56576",
    "#00838F",
    "#7A6F3A",
)
CHINESE_FONT_CANDIDATES = [
    "Noto Sans CJK SC",
    "Noto Sans CJK JP",
    "WenQuanYi Micro Hei",
    "WenQuanYi Micro Hei Mono",
    "Noto Sans SC",
    "Source Han Sans SC",
    "Microsoft YaHei",
    "SimHei",
    "PingFang SC",
    "Hiragino Sans GB",
    "Arial Unicode MS",
]


def _pick_available_font(candidates: List[str]) -> str:
    # Rebuild font list instead of using stale cache so newly installed fonts are visible.
    fm = font_manager._load_fontmanager(try_read_cache=False)
    installed = {f.name for f in fm.ttflist}
    for name in candidates:
        if name in installed:
            return name
    return "DejaVu Sans"


def _configure_fonts() -> None:
    """为报告图自动选择系统可用中文字体，避免 findfont 警告。"""
    chosen = _pick_available_font(CHINESE_FONT_CANDIDATES)
    plt.rcParams["font.family"] = [chosen, "DejaVu Sans", "sans-serif"]
    plt.rcParams["font.sans-serif"] = CHINESE_FONT_CANDIDATES + ["DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False


def _initial_circle_positions(count: int) -> np.ndarray:
    if count == 1:
        return np.array([[0.0, 0.0]])
    if count == 2:
        return np.array([[-0.85, 0.0], [0.85, 0.0]])

    angles = np.linspace(np.pi / 2, np.pi / 2 + 2 * np.pi, count, endpoint=False)
    return np.column_stack((1.6 * np.cos(angles), 1.6 * np.sin(angles)))


def _circle_layout(selection_sets: Dict[str, Set[str]], radii: np.ndarray) -> np.ndarray:
    """根据策略 Jaccard 相似度布置圆心，交集越高的策略越靠近。"""
    count = len(selection_sets)
    positions = _initial_circle_positions(count)
    if count < 2:
        return positions

    sets = list(selection_sets.values())
    for _ in range(500):
        forces = np.zeros_like(positions)
        for left in range(count):
            for right in range(left + 1, count):
                delta = positions[right] - positions[left]
                distance = float(np.linalg.norm(delta))
                if distance < 1e-6:
                    delta = np.array([0.01 * (right + 1), 0.01 * (left + 1)])
                    distance = float(np.linalg.norm(delta))

                union_size = len(sets[left] | sets[right])
                similarity = len(sets[left] & sets[right]) / union_size if union_size else 0.0
                # 完全不相交的圆保留轻微间隔，高度相交的圆允许明显重叠。
                target_distance = (radii[left] + radii[right]) * (1.12 - 0.62 * similarity)
                adjustment = 0.025 * (distance - target_distance) * delta / distance
                forces[left] += adjustment
                forces[right] -= adjustment

        # 轻微向中心收拢，避免相互独立的策略被推离画布。
        forces -= positions * 0.003
        positions += forces

    positions -= positions.mean(axis=0)
    return positions


def build_intersection_rows(selection_sets: Dict[str, Set[str]]) -> pd.DataFrame:
    """按每只股票命中的完整策略组合生成交集明细。"""
    memberships: Dict[Tuple[str, ...], List[str]] = {}
    all_codes = sorted(set().union(*selection_sets.values())) if selection_sets else []
    for code in all_codes:
        strategy_group = tuple(name for name, picks in selection_sets.items() if code in picks)
        memberships.setdefault(strategy_group, []).append(code)

    rows = [
        {
            "strategy_count": len(group),
            "strategies": " + ".join(group),
            "stock_count": len(codes),
            "stocks": ", ".join(sorted(codes)),
        }
        for group, codes in memberships.items()
    ]
    return pd.DataFrame(rows, columns=["strategy_count", "strategies", "stock_count", "stocks"]).sort_values(
        ["strategy_count", "stock_count", "strategies"], ascending=[False, False, True]
    ) if rows else pd.DataFrame(columns=["strategy_count", "strategies", "stock_count", "stocks"])


def render_selection_dashboard(
        selection_sets: Dict[str, Iterable[str]],
        output_path: Path,
        trade_date: pd.Timestamp,
) -> None:
        """输出自包含 HTML 仪表盘，仅展示策略、数量和股票代码。"""
        normalized = {name: set(picks) for name, picks in selection_sets.items()}
        names = list(normalized)
        largest_selection = max((len(picks) for picks in normalized.values()), default=1)
        radii = np.array([
                0.72 + 0.53 * np.sqrt(len(picks) / largest_selection)
                for picks in normalized.values()
        ])
        positions = _circle_layout(normalized, radii)
        extent = max(2.9, float(np.abs(positions).max(initial=0.0) + radii.max(initial=0.0) + 0.65))

        circles = [
                {
                        "name": name,
                        "codes": sorted(picks),
                        "color": PALETTE[index % len(PALETTE)],
                        "x": round(float((position[0] + extent) / (2 * extent) * 760 + 20), 2),
                        "y": round(float((extent - position[1]) / (2 * extent) * 540 + 40), 2),
                        "radius": round(float(radius / (2 * extent) * 540), 2),
                }
                for index, ((name, picks), position, radius) in enumerate(zip(normalized.items(), positions, radii))
        ]
        intersections = [
                {
                        "strategies": group.split(" + "),
                        "codes": stocks.split(", ") if stocks else [],
                }
                for group, stocks in zip(
                        build_intersection_rows(normalized)["strategies"],
                        build_intersection_rows(normalized)["stocks"],
                )
        ]
        total_codes = sorted(set().union(*normalized.values())) if normalized else []
        payload = {
                "date": trade_date.strftime("%Y-%m-%d"),
                "circles": circles,
                "intersections": intersections,
                "strategyCount": len(names),
                "totalCodes": len(total_codes),
        }
        payload_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
        title = html.escape(f"策略选股仪表盘 | {trade_date:%Y-%m-%d}")

        document = f"""<!doctype html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title}</title>
    <style>
        :root {{ --ink:#172033; --muted:#6b7280; --line:#e6e9ef; --panel:#ffffff; --canvas:#f5f7fa; }}
        * {{ box-sizing:border-box; }}
        body {{ margin:0; background:var(--canvas); color:var(--ink); font:14px "Noto Sans CJK SC","Noto Sans SC","WenQuanYi Micro Hei","Source Han Sans SC","Microsoft YaHei","PingFang SC","Hiragino Sans GB","Arial Unicode MS",sans-serif; }}
        main {{ max-width:1540px; margin:0 auto; padding:28px; }}
        header {{ display:flex; justify-content:space-between; align-items:end; margin-bottom:20px; gap:16px; }}
        h1 {{ margin:0; font-size:24px; font-weight:700; letter-spacing:0; }}
        .summary {{ color:var(--muted); line-height:1.7; text-align:right; }}
        .layout {{ display:grid; grid-template-columns:minmax(580px,1.35fr) minmax(300px,.65fr); gap:18px; }}
        .panel {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; box-shadow:0 8px 24px rgba(23,32,51,.05); }}
        .map {{ min-height:650px; padding:18px; overflow:auto; }}
        .panel-title {{ margin:0 0 4px; font-size:16px; }}
        .hint {{ margin:0 0 14px; color:var(--muted); font-size:12px; }}
        svg {{ display:block; width:100%; min-width:760px; height:auto; background:#fbfcfe; border:1px solid #edf0f4; border-radius:6px; }}
        .strategy-circle {{ cursor:pointer; transition:opacity .15s; }}
        .strategy-circle:hover, .strategy-circle.active {{ opacity:.78; }}
        .strategy-label {{ font-size:12px; font-weight:700; fill:#172033; pointer-events:none; }}
        .strategy-count {{ font-size:12px; fill:#344054; pointer-events:none; }}
        .strategy-codes {{ font-size:11px; fill:#344054; pointer-events:none; }}
        .side {{ display:grid; gap:18px; align-content:start; }}
        .detail, .intersections {{ padding:18px; }}
        .detail h2, .intersections h2 {{ margin:0 0 6px; font-size:16px; }}
        .code-list {{ display:flex; flex-wrap:wrap; gap:7px; margin-top:16px; max-height:310px; overflow:auto; padding-right:3px; }}
        .code {{ padding:5px 8px; border-radius:4px; background:#f1f4f8; color:#27364b; font:600 12px ui-monospace,SFMono-Regular,Menlo,monospace; }}
        .intersection-row {{ width:100%; border:0; border-top:1px solid var(--line); padding:11px 0; text-align:left; background:transparent; color:var(--ink); cursor:pointer; }}
        .intersection-row:hover {{ color:#176b87; }}
        .intersection-row strong {{ display:block; font-size:13px; }}
        .intersection-row span {{ display:block; margin-top:4px; color:var(--muted); font-size:12px; }}
        @media (max-width:960px) {{ main {{ padding:16px; }} header {{ align-items:start; flex-direction:column; }} .summary {{ text-align:left; }} .layout {{ grid-template-columns:1fr; }} }}
    </style>
</head>
<body>
    <main>
        <header>
            <div><h1>策略选股仪表盘</h1><div class="hint">交易日 <span id="date"></span></div></div>
            <div class="summary"><span id="strategyCount"></span> 个策略<br><span id="totalCodes"></span> 只唯一股票</div>
        </header>
        <section class="layout">
            <section class="panel map"><h2 class="panel-title">策略范围</h2><p class="hint">圆面积表示入选数量，圆心显示股票代码摘要。点击圆形查看完整代码。</p><svg id="strategyMap" viewBox="0 0 800 620" role="img" aria-label="策略选股范围图"></svg></section>
            <aside class="side">
                <section class="panel detail"><h2 id="detailTitle">选择一个策略或交集</h2><p class="hint" id="detailHint">点击左侧圆形或下方交集条目查看完整股票代码。</p><div id="codeList" class="code-list"></div></section>
                <section class="panel intersections"><h2>策略交集</h2><p class="hint">点击查看该组合的完整股票代码。</p><div id="intersectionList"></div></section>
            </aside>
        </section>
    </main>
    <script>
        const dashboard = {payload_json};
        const ns = 'http://www.w3.org/2000/svg';
        const svg = document.getElementById('strategyMap');
        const codeList = document.getElementById('codeList');
        const detailTitle = document.getElementById('detailTitle');
        const detailHint = document.getElementById('detailHint');
        document.getElementById('date').textContent = dashboard.date;
        document.getElementById('strategyCount').textContent = dashboard.strategyCount;
        document.getElementById('totalCodes').textContent = dashboard.totalCodes;
        const addText = (parent, text, x, y, className) => {{
            const node = document.createElementNS(ns, 'text'); node.setAttribute('x', x); node.setAttribute('y', y); node.setAttribute('text-anchor', 'middle'); node.setAttribute('class', className); node.textContent = text; parent.appendChild(node);
        }};
        const showCodes = (title, codes, hint) => {{
            detailTitle.textContent = title; detailHint.textContent = hint; codeList.replaceChildren();
            codes.forEach(code => {{ const item = document.createElement('span'); item.className = 'code'; item.textContent = code; codeList.appendChild(item); }});
        }};
        dashboard.circles.forEach(circle => {{
            const group = document.createElementNS(ns, 'g'); group.setAttribute('class', 'strategy-circle');
            const shape = document.createElementNS(ns, 'circle'); shape.setAttribute('cx', circle.x); shape.setAttribute('cy', circle.y); shape.setAttribute('r', circle.radius); shape.setAttribute('fill', circle.color); shape.setAttribute('fill-opacity', '.22'); shape.setAttribute('stroke', circle.color); shape.setAttribute('stroke-width', '2.2'); group.appendChild(shape);
            addText(group, circle.name, circle.x, circle.y - 17, 'strategy-label');
            addText(group, `${{circle.codes.length}} 只`, circle.x, circle.y + 1, 'strategy-count');
            const preview = circle.codes.slice(0, 5); preview.forEach((code, index) => addText(group, code, circle.x, circle.y + 20 + index * 14, 'strategy-codes'));
            if (circle.codes.length > preview.length) addText(group, `+${{circle.codes.length - preview.length}}`, circle.x, circle.y + 20 + preview.length * 14, 'strategy-codes');
            group.addEventListener('click', () => {{ document.querySelectorAll('.strategy-circle').forEach(node => node.classList.remove('active')); group.classList.add('active'); showCodes(circle.name, circle.codes, `共 ${{circle.codes.length}} 只股票代码`); }});
            svg.appendChild(group);
        }});
        const intersectionList = document.getElementById('intersectionList');
        dashboard.intersections.forEach(item => {{
            const row = document.createElement('button'); row.className = 'intersection-row';
            const title = document.createElement('strong'); title.textContent = item.strategies.join(' + ');
            const meta = document.createElement('span'); meta.textContent = `${{item.codes.length}} 只 · ${{item.codes.slice(0, 5).join(' · ')}}${{item.codes.length > 5 ? ' ...' : ''}}`;
            row.append(title, meta); row.addEventListener('click', () => showCodes(item.strategies.join(' + '), item.codes, `该策略组合共 ${{item.codes.length}} 只股票代码`)); intersectionList.appendChild(row);
        }});
    </script>
</body>
</html>"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(document, encoding="utf-8")


def render_selection_overlap(
    selection_sets: Dict[str, Iterable[str]],
    output_path: Path,
    detail_path: Path,
    trade_date: pd.Timestamp,
) -> pd.DataFrame:
    """输出策略圆形交集图和完整交集明细 CSV，并返回明细。"""
    _configure_fonts()
    normalized = {name: set(picks) for name, picks in selection_sets.items()}
    intersections = build_intersection_rows(normalized)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    intersections.to_csv(detail_path, index=False, encoding="utf-8-sig")

    names = list(normalized)
    colors = [PALETTE[index % len(PALETTE)] for index in range(len(names))]
    largest_selection = max((len(picks) for picks in normalized.values()), default=1)
    radii = np.array([
        0.72 + 0.53 * np.sqrt(len(picks) / largest_selection)
        for picks in normalized.values()
    ])
    positions = _circle_layout(normalized, radii)

    figure = plt.figure(figsize=(17, 10), facecolor="#F7F8FA")
    grid = figure.add_gridspec(1, 2, width_ratios=[1.25, 1.0], wspace=0.08)
    ax_chart = figure.add_subplot(grid[0, 0])
    ax_detail = figure.add_subplot(grid[0, 1])
    ax_chart.set_facecolor("#FFFFFF")
    ax_detail.set_facecolor("#FFFFFF")

    for (name, picks), color, (x_pos, y_pos), circle_radius in zip(normalized.items(), colors, positions, radii):
        circle = Circle(
            (x_pos, y_pos),
            circle_radius,
            facecolor=color,
            edgecolor=color,
            alpha=0.20,
            linewidth=2.2,
        )
        ax_chart.add_patch(circle)
        ax_chart.text(
            x_pos,
            y_pos + circle_radius + 0.12,
            f"{name}\n{len(picks)} 只",
            color=color,
            fontsize=10,
            fontweight="bold",
            ha="center",
            va="bottom",
        )

    total_codes = set().union(*normalized.values()) if normalized else set()
    ax_chart.set_title(
        f"策略选股交集 | {trade_date:%Y-%m-%d}",
        loc="left",
        fontsize=18,
        fontweight="bold",
        color="#1E293B",
        pad=18,
    )
    ax_chart.text(
        0.0,
        1.01,
        f"共 {len(names)} 个策略，覆盖 {len(total_codes)} 只唯一股票。圆面积对应入选数量，圆心距离按交集相似度排列。",
        transform=ax_chart.transAxes,
        fontsize=10,
        color="#64748B",
    )
    extent = max(2.9, float(np.abs(positions).max(initial=0.0) + radii.max(initial=0.0) + 0.65))
    ax_chart.set_xlim(-extent, extent)
    ax_chart.set_ylim(-extent, extent)
    ax_chart.set_aspect("equal")
    ax_chart.axis("off")

    ax_detail.set_title("交集明细", loc="left", fontsize=16, fontweight="bold", color="#1E293B", pad=18)
    ax_detail.text(
        0.0,
        1.01,
        f"精确交集与完整股票列表已导出至 {detail_path.name}",
        transform=ax_detail.transAxes,
        fontsize=9.5,
        color="#64748B",
    )
    ax_detail.axis("off")

    if intersections.empty:
        ax_detail.text(0.5, 0.5, "当日没有策略命中股票", ha="center", va="center", fontsize=13, color="#64748B")
    else:
        displayed = intersections.head(12).copy()
        displayed["strategies"] = displayed["strategies"].str.replace(" + ", "\n", regex=False)
        displayed["stocks"] = displayed["stocks"].map(lambda value: value if len(value) <= 48 else f"{value[:45]}...")
        table = ax_detail.table(
            cellText=displayed[["strategies", "stock_count", "stocks"]].values,
            colLabels=["命中策略组合", "数量", "股票"],
            colWidths=[0.37, 0.10, 0.53],
            cellLoc="left",
            loc="upper left",
            bbox=[0.0, 0.06, 1.0, 0.88],
        )
        table.auto_set_font_size(False)
        table.set_fontsize(8.5)
        for (row, column), cell in table.get_celld().items():
            cell.set_edgecolor("#E2E8F0")
            if row == 0:
                cell.set_facecolor("#E8EEF3")
                cell.set_text_props(color="#334155", fontweight="bold")
            else:
                cell.set_facecolor("#FFFFFF" if row % 2 else "#F8FAFC")
                cell.set_text_props(color="#334155")

    figure.savefig(output_path, dpi=180, bbox_inches="tight", facecolor=figure.get_facecolor())
    plt.close(figure)
    return intersections