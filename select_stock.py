from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from selection_visualizer import render_selection_dashboard, render_selection_overlap

# ---------- 日志 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # 将日志写入文件
        logging.FileHandler("select_results.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("select")


# ---------- 工具 ----------

def load_data(data_dir: Path, codes: Iterable[str]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for code in codes:
        fp = data_dir / f"{code}.csv"
        if not fp.exists():
            logger.warning("%s 不存在，跳过", fp.name)
            continue
        df = pd.read_csv(fp, parse_dates=["date"]).sort_values("date")
        frames[code] = df
    return frames


def load_config(cfg_path: Path) -> List[Dict[str, Any]]:
    if not cfg_path.exists():
        logger.error("配置文件 %s 不存在", cfg_path)
        sys.exit(1)
    with cfg_path.open(encoding="utf-8") as f:
        cfg_raw = json.load(f)

    # 兼容三种结构：单对象、对象数组、或带 selectors 键
    if isinstance(cfg_raw, list):
        cfgs = cfg_raw
    elif isinstance(cfg_raw, dict) and "selectors" in cfg_raw:
        cfgs = cfg_raw["selectors"]
    else:
        cfgs = [cfg_raw]

    if not cfgs:
        logger.error("configs.json 未定义任何 Selector")
        sys.exit(1)

    return cfgs


def instantiate_selector(cfg: Dict[str, Any]):
    """动态加载 Selector 类并实例化"""
    cls_name: str = cfg.get("class")
    if not cls_name:
        raise ValueError("缺少 class 字段")

    try:
        module = importlib.import_module("Selector")
        cls = getattr(module, cls_name)
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f"无法加载 Selector.{cls_name}: {e}") from e

    params = cfg.get("params", {})
    return cfg.get("alias", cls_name), cls(**params)


# ---------- 主函数 ----------

def main():
    p = argparse.ArgumentParser(description="Run selectors defined in configs.json")
    p.add_argument("--data-dir", default="./data/us_stocks", help="CSV 行情目录")
    p.add_argument("--config", default="./configs.json", help="Selector 配置文件")
    p.add_argument("--date", help="交易日 YYYY-MM-DD；缺省=数据最新日期")
    p.add_argument("--tickers", default="all", help="'all' 或逗号分隔股票代码列表")
    p.add_argument("--visualization-output", default="./selection_overlap.png", help="策略交集圆形图输出路径")
    p.add_argument("--intersection-output", default="./selection_intersections.csv", help="策略交集明细 CSV 输出路径")
    p.add_argument("--dashboard-output", default="./selection_dashboard.html", help="交互式选股仪表盘输出路径")
    p.add_argument("--no-visualization", action="store_true", help="不生成策略交集可视化")
    p.add_argument("--no-dashboard", action="store_true", help="不生成交互式选股仪表盘")
    args = p.parse_args()

    # --- 加载行情 ---
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error("数据目录 %s 不存在", data_dir)
        sys.exit(1)

    codes = (
        [f.stem for f in data_dir.glob("*.csv")]
        if args.tickers.lower() == "all"
        else [c.strip() for c in args.tickers.split(",") if c.strip()]
    )
    if not codes:
        logger.error("股票池为空！")
        sys.exit(1)

    data = load_data(data_dir, codes)
    if not data:
        logger.error("未能加载任何行情数据")
        sys.exit(1)

    trade_date = (
        pd.to_datetime(args.date)
        if args.date
        else max(df["date"].max() for df in data.values())
    )
    if not args.date:
        logger.info("未指定 --date，使用最近日期 %s", trade_date.date())

    # --- 加载 Selector 配置 ---
    selector_cfgs = load_config(Path(args.config))

    # --- 逐个 Selector 运行 ---
    selection_sets: Dict[str, List[str]] = {}
    for cfg in selector_cfgs:
        if cfg.get("activate", True) is False:
            continue
        try:
            alias, selector = instantiate_selector(cfg)
        except Exception as e:
            logger.error("跳过配置 %s：%s", cfg, e)
            continue

        picks = selector.select(trade_date, data)
        selection_sets[alias] = picks

        # 将结果写入日志，同时输出到控制台
        logger.info("")
        logger.info("============== 选股结果 [%s] ==============", alias)
        logger.info("交易日: %s", trade_date.date())
        logger.info("符合条件股票数: %d", len(picks))
        logger.info("%s", ", ".join(picks) if picks else "无符合条件股票")

    if not args.no_visualization:
        chart_path = Path(args.visualization_output)
        detail_path = Path(args.intersection_output)
        intersections = render_selection_overlap(selection_sets, chart_path, detail_path, trade_date)
        logger.info("策略交集圆形图: %s", chart_path)
        logger.info("策略交集明细: %s", detail_path)
        logger.info("非空策略组合数: %d", len(intersections))

    if not args.no_dashboard:
        dashboard_path = Path(args.dashboard_output)
        render_selection_dashboard(selection_sets, dashboard_path, trade_date)
        logger.info("交互式选股仪表盘: %s", dashboard_path)


if __name__ == "__main__":
    main()
