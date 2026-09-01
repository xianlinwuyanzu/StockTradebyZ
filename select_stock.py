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
from scan_three_wave import _compute_kdj
from bbd_signals import SIGNAL_COLUMNS, compute_bbd_signals

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


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if hasattr(value, "item"):
        value = value.item()
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _selector_passes_prefix(selector: Any, history: pd.DataFrame) -> bool:
    if selector is None or history.empty:
        return False
    passes_filters = getattr(selector, "_passes_filters", None)
    if not callable(passes_filters):
        return False
    max_window = int(getattr(selector, "max_window", len(history)))
    return bool(passes_filters(history.tail(max_window)))


def _compute_rocket_signals(
    full_history: pd.DataFrame,
    selectors: Dict[str, Any],
) -> pd.Series:
    """按现有三个买点 Selector 的判定，生成逐日火箭买点事件。"""
    signal_rows: list[list[str]] = [[] for _ in range(len(full_history))]
    burst_selector = selectors.get("BurstSignalSelector")
    golden_pit_selector = selectors.get("MultiCycleRSVSelector")
    market_selector = selectors.get("MarketStructureBuySelector")
    previous_golden_pit_active = False
    golden_pit_entry_only = bool(getattr(golden_pit_selector, "golden_pit_entry_only", False))

    for index in range(len(full_history)):
        prefix = full_history.iloc[: index + 1]
        if _selector_passes_prefix(burst_selector, prefix):
            signal_rows[index].append("起爆")

        golden_pit_active = _selector_passes_prefix(golden_pit_selector, prefix)
        if golden_pit_active and (golden_pit_entry_only or not previous_golden_pit_active):
            signal_rows[index].append("黄金坑")
        previous_golden_pit_active = golden_pit_active

        if _selector_passes_prefix(market_selector, prefix):
            signal_rows[index].append("峰谷结构超低")

    return pd.Series(signal_rows, index=full_history.index, dtype=object)


def write_wave_selection_outputs(
    details: Dict[str, Dict[str, Any]],
    data: Dict[str, pd.DataFrame],
    trade_date: pd.Timestamp,
    output_dir: Path,
    data_days: int,
    score_threshold: float,
    strategy_name: str,
    signal_selectors: Dict[str, Any] | None = None,
    preselection_details: Dict[str, List[Dict[str, Any]]] | None = None,
) -> None:
    """写出结构化选股评分索引和前端可读取的最近日线数据。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_dir = output_dir / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    for stale_file in daily_dir.glob("*.csv"):
        stale_file.unlink()
    result_items: List[Dict[str, Any]] = []
    preselection_items: List[Dict[str, Any]] = []

    daily_items: Dict[str, Dict[str, Any] | None] = {}

    def build_daily_item(code: str) -> Dict[str, Any] | None:
        if code in daily_items:
            return daily_items[code]
        hist = data.get(code)
        if hist is None or hist.empty:
            daily_items[code] = None
            return None
        history = (
            hist[hist["date"] <= trade_date]
            .sort_values("date")
            .drop_duplicates("date")
            .tail(max(1, data_days))
            .reset_index(drop=True)
        )
        if history.empty:
            daily_items[code] = None
            return None

        # Compute J on the full available history before taking the display window,
        # so the 50-day output keeps the same indicator warm-up as wave detection.
        history = history.copy()
        full_history = hist[hist["date"] <= trade_date].sort_values("date").drop_duplicates("date").reset_index(drop=True)
        kdj_values = _compute_kdj(full_history)
        for column in ("K", "D", "J"):
            history[column] = kdj_values[column].iloc[-len(history):].to_numpy()
        bbd_values = compute_bbd_signals(full_history)
        for column in SIGNAL_COLUMNS:
            history[column] = bbd_values[column].iloc[-len(history):].to_numpy()
        rocket_values = _compute_rocket_signals(full_history, signal_selectors or {})
        history["rocket_signals"] = rocket_values.iloc[-len(history):].to_numpy()
        recent = history

        daily_path = daily_dir / f"{code}.csv"
        recent.to_csv(daily_path, index=False, encoding="utf-8-sig")
        daily_item = {
            "daily_data_file": f"daily/{code}.csv",
            "daily_data_rows": len(recent),
            "daily_data_start": pd.Timestamp(recent["date"].iloc[0]).date().isoformat(),
            "daily_data_end": pd.Timestamp(recent["date"].iloc[-1]).date().isoformat(),
            "recent_daily_data": [
            {key: _json_safe_value(value) for key, value in row.items()}
            for row in recent.to_dict(orient="records")
            ],
        }
        daily_items[code] = daily_item
        return daily_item

    def build_result_item(code: str, detail: Dict[str, Any]) -> Dict[str, Any] | None:
        daily_item = build_daily_item(code)
        if daily_item is None:
            return None
        item = dict(detail)
        item.update(daily_item)
        return item

    def numeric_detail_value(detail: Dict[str, Any], key: str, fallback: float = -1.0) -> float:
        try:
            return float(detail.get(key, fallback))
        except (TypeError, ValueError):
            return fallback

    if any("timing_score" in detail for detail in details.values()):
        detail_items = sorted(
            details.items(),
            key=lambda entry: (
                -numeric_detail_value(entry[1], "timing_score"),
                -numeric_detail_value(entry[1], "structure_quality", numeric_detail_value(entry[1], "score")),
                entry[0],
            ),
        )
    else:
        detail_items = sorted(details.items())

    for code, detail in detail_items:
        item = build_result_item(code, detail)
        if item is not None:
            result_items.append(item)

    for code, candidates in sorted((preselection_details or {}).items()):
        for detail in candidates:
            item = build_result_item(code, detail)
            if item is not None:
                preselection_items.append(item)

    payload = {
        "strategy": strategy_name,
        "trade_date": trade_date.date().isoformat(),
        "score_threshold": score_threshold,
        "data_days": data_days,
        "result_count": len(result_items),
        "results": result_items,
        "preselection_count": len(preselection_items),
        "preselection_results": preselection_items,
    }
    if strategy_name == "二波选股策略":
        first_detail = next(iter(details.values()), {})
        raw_max = numeric_detail_value(first_detail, "structure_match_raw_max", 8.6)
        score_scale = numeric_detail_value(first_detail, "score_scale", 10.0 / 5.114)
        if raw_max > 0.0 and score_scale > 0.0:
            payload["structure_match_threshold_percent"] = round(
                max(0.0, min(100.0, score_threshold / score_scale / raw_max * 100.0)),
                2,
            )
    (output_dir / "wave_selection_results.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary_rows = []
    for item in result_items:
        summary = {
            key: value
            for key, value in item.items()
            if key != "recent_daily_data"
        }
        for key, value in list(summary.items()):
            if isinstance(value, (list, tuple, dict)):
                summary[key] = json.dumps(value, ensure_ascii=False)
        summary_rows.append(summary)
    pd.DataFrame(summary_rows).to_csv(
        output_dir / "wave_selection_results.csv",
        index=False,
        encoding="utf-8-sig",
    )


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
    logger.info("选股运行开始")

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
    signal_selectors: Dict[str, Any] = {}
    wave_output_specs: List[
        tuple[
            str,
            Dict[str, Dict[str, Any]],
            Dict[str, List[Dict[str, Any]]],
            Path,
            int,
            float,
            Dict[str, Any],
        ]
    ] = []
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
        signal_selectors[selector.__class__.__name__] = selector

        result_details = getattr(selector, "result_details", None)
        if isinstance(result_details, dict):
            preselection_details = getattr(selector, "preselection_details", {})
            if not isinstance(preselection_details, dict):
                preselection_details = {}
            output_dir = Path(getattr(selector, "output_dir", "./wave_selection_data"))
            data_days = int(getattr(selector, "data_days", 70))
            score_threshold = float(getattr(selector, "score_threshold", 1.0))
            wave_output_specs.append(
                (
                    alias,
                    result_details,
                    preselection_details,
                    output_dir,
                    data_days,
                    score_threshold,
                    signal_selectors,
                )
            )

        # 将结果写入日志，同时输出到控制台
        logger.info("")
        logger.info("============== 选股结果 [%s] ==============", alias)
        logger.info("交易日: %s", trade_date.date())
        logger.info("符合条件股票数: %d", len(picks))
        logger.info("%s", ", ".join(picks) if picks else "无符合条件股票")

    for (
        alias,
        details,
        preselection_details,
        output_dir,
        data_days,
        score_threshold,
        output_signal_selectors,
    ) in wave_output_specs:
        write_wave_selection_outputs(
            details=details,
            data=data,
            trade_date=trade_date,
            output_dir=output_dir,
            data_days=data_days,
            score_threshold=score_threshold,
            strategy_name=alias,
            signal_selectors=output_signal_selectors,
            preselection_details=preselection_details,
        )
        logger.info(
            "结构化选股结果: %s（%s，%d 只，最近 %d 根日线）",
            output_dir,
            alias,
            len(details),
            data_days,
        )

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
