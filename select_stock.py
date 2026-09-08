from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from time import perf_counter
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from reporting.selection_visualizer import render_selection_dashboard, render_selection_overlap
from strategies.scan_three_wave import _compute_kdj
from features.bbd_signals import SIGNAL_COLUMNS, compute_bbd_signals

MOZHUA_STRATEGY_NAME = "魔抓策略"
MOZHUA_HISTORY_FILE_NAME = "mozhua_selection_history.json"
MOZHUA_HISTORY_WINDOW_DAYS = 10
_DAILY_OUTPUT_FIELDS = {
    "daily_data_file",
    "daily_data_rows",
    "daily_data_start",
    "daily_data_end",
    "recent_daily_data",
}

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
        module = importlib.import_module("strategies.Selector")
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
    start_index: int = 0,
) -> pd.Series:
    """按现有三个买点 Selector 的判定，生成逐日火箭买点事件。"""
    signal_rows: list[list[str]] = [[] for _ in range(len(full_history))]
    burst_selector = selectors.get("BurstSignalSelector")
    golden_pit_selector = selectors.get("MultiCycleRSVSelector")
    market_selector = selectors.get("MarketStructureBuySelector")
    previous_golden_pit_active = False
    golden_pit_entry_only = bool(getattr(golden_pit_selector, "golden_pit_entry_only", False))

    if not any((burst_selector, golden_pit_selector, market_selector)):
        return pd.Series(signal_rows, index=full_history.index, dtype=object)

    start_index = max(0, min(start_index, len(full_history)))
    if start_index > 0 and not golden_pit_entry_only:
        previous_golden_pit_active = _selector_passes_prefix(golden_pit_selector, full_history.iloc[:start_index])

    for index in range(start_index, len(full_history)):
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


def _available_trade_dates(
    data: Dict[str, pd.DataFrame], through_date: pd.Timestamp
) -> List[pd.Timestamp]:
    dates: set[pd.Timestamp] = set()
    for frame in data.values():
        if frame is None or frame.empty or "date" not in frame:
            continue
        parsed_dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
        dates.update(pd.Timestamp(value).normalize() for value in parsed_dates if value <= through_date)
    return sorted(dates)


def _history_record(
    code: str, detail: Dict[str, Any], selected_trade_date: pd.Timestamp
) -> Dict[str, Any]:
    record = {
        key: value for key, value in detail.items() if key not in _DAILY_OUTPUT_FIELDS
    }
    record["code"] = code
    record["selected_trade_date"] = selected_trade_date.date().isoformat()
    return _json_safe_value(record)


def _history_record_key(record: Dict[str, Any]) -> tuple[str, str] | None:
    code = str(record.get("code") or "").strip().upper()
    selected_date = str(
        record.get("selected_trade_date")
        or record.get("selection_date")
        or record.get("as_of_date")
        or ""
    ).strip()
    if not code or not selected_date:
        return None
    try:
        selected_date = pd.Timestamp(selected_date).date().isoformat()
    except (TypeError, ValueError):
        return None
    record["code"] = code
    record["selected_trade_date"] = selected_date
    return code, selected_date


def _load_selection_history(history_path: Path) -> List[Dict[str, Any]]:
    if not history_path.exists():
        return []
    try:
        raw = json.loads(history_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    raw_records = raw.get("records", []) if isinstance(raw, dict) else raw
    if not isinstance(raw_records, list):
        return []
    records: List[Dict[str, Any]] = []
    for raw_record in raw_records:
        if not isinstance(raw_record, dict):
            continue
        record = {
            key: value
            for key, value in raw_record.items()
            if key not in _DAILY_OUTPUT_FIELDS
        }
        if _history_record_key(record) is not None:
            records.append(record)
    return records


def _collect_selector_history(
    selector: Any,
    data: Dict[str, pd.DataFrame],
    trade_date: pd.Timestamp,
    history_window_days: int,
) -> List[Dict[str, Any]]:
    available_dates = _available_trade_dates(data, trade_date)
    if not available_dates:
        return []
    scan_dates = available_dates[-(max(1, history_window_days) + 1):]
    records: List[Dict[str, Any]] = []
    for scan_date in scan_dates:
        selector.select(scan_date, data)
        result_details = getattr(selector, "result_details", {})
        if not isinstance(result_details, dict):
            continue
        for code, detail in result_details.items():
            if isinstance(detail, dict):
                records.append(_history_record(code, detail, scan_date))
    return records


def _merge_selection_history(
    existing_records: List[Dict[str, Any]],
    seeded_records: List[Dict[str, Any]],
    details: Dict[str, Dict[str, Any]],
    current_trade_date: pd.Timestamp | None,
    available_dates: List[pd.Timestamp],
    history_window_days: int,
) -> tuple[List[Dict[str, Any]], set[str]]:
    records_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
    for record in [*existing_records, *seeded_records]:
        key = _history_record_key(record)
        if key is not None:
            records_by_key[key] = record
    if current_trade_date is not None:
        for code, detail in details.items():
            record = _history_record(code, detail, current_trade_date)
            key = _history_record_key(record)
            if key is not None:
                records_by_key[key] = record

    retained_dates = available_dates[-(max(1, history_window_days) + 1):]
    retained_date_strings = {item.date().isoformat() for item in retained_dates}
    retained_records = [
        record
        for record in records_by_key.values()
        if str(record.get("selected_trade_date")) in retained_date_strings
    ]
    retained_records.sort(
        key=lambda record: (
            str(record.get("selected_trade_date") or ""),
            str(record.get("code") or ""),
        ),
        reverse=True,
    )
    historical_dates = {
        str(record["selected_trade_date"])
        for record in retained_records
        if current_trade_date is None
        or str(record.get("selected_trade_date")) != current_trade_date.date().isoformat()
    }
    return retained_records, historical_dates


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
    daily_history_cache: Dict[str, pd.DataFrame] | None = None,
    signal_history_days: int | None = None,
    history_window_days: int | None = None,
    history_selector: Any | None = None,
) -> None:
    """写出评分及日线；缓存仅在相同运行内共享，信号窗口需覆盖所有输出天数。"""
    if signal_history_days is not None and signal_history_days < max(1, data_days):
        raise ValueError("signal_history_days must cover data_days")
    if daily_history_cache is None:
        daily_history_cache = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_dir = output_dir / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    for stale_file in daily_dir.glob("*.csv"):
        stale_file.unlink()
    result_items: List[Dict[str, Any]] = []
    preselection_items: List[Dict[str, Any]] = []
    historical_items: List[Dict[str, Any]] = []

    daily_items: Dict[str, Dict[str, Any] | None] = {}

    def build_daily_item(code: str) -> Dict[str, Any] | None:
        if code in daily_items:
            return daily_items[code]
        hist = data.get(code)
        if hist is None or hist.empty:
            daily_items[code] = None
            return None
        if code not in daily_history_cache:
            full_history = hist[hist["date"] <= trade_date].sort_values("date").drop_duplicates("date").reset_index(drop=True)
            if full_history.empty:
                daily_items[code] = None
                return None
            kdj_values = _compute_kdj(full_history)
            bbd_values = compute_bbd_signals(full_history)
            signal_start = 0 if signal_history_days is None else max(0, len(full_history) - signal_history_days)
            rocket_values = _compute_rocket_signals(full_history, signal_selectors or {}, start_index=signal_start)
            for column in ("K", "D", "J"):
                full_history[column] = kdj_values[column].to_numpy()
            for column in SIGNAL_COLUMNS:
                full_history[column] = bbd_values[column].to_numpy()
            full_history["rocket_signals"] = rocket_values.to_numpy()
            daily_history_cache[code] = full_history
        recent = daily_history_cache[code].tail(max(1, data_days)).reset_index(drop=True).copy()

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

    history_dates: set[str] = set()
    if strategy_name == MOZHUA_STRATEGY_NAME and history_window_days is not None:
        history_path = output_dir / MOZHUA_HISTORY_FILE_NAME
        available_dates = _available_trade_dates(data, trade_date)
        current_trade_date = available_dates[-1] if available_dates else None
        existing_history = _load_selection_history(history_path)
        seeded_history: List[Dict[str, Any]] = []
        if not history_path.exists() and history_selector is not None:
            seeded_history = _collect_selector_history(
                history_selector,
                data,
                trade_date,
                history_window_days,
            )
        retained_history, history_dates = _merge_selection_history(
            existing_history,
            seeded_history,
            details,
            current_trade_date,
            available_dates,
            history_window_days,
        )
        for record in retained_history:
            if current_trade_date is not None and (
                str(record.get("selected_trade_date")) == current_trade_date.date().isoformat()
            ):
                continue
            code = str(record.get("code") or "").strip().upper()
            if not code:
                continue
            item = build_result_item(code, record)
            if item is not None:
                item["history_status"] = "historical_selection"
                historical_items.append(item)
        historical_items.sort(
            key=lambda item: (
                str(item.get("selected_trade_date") or ""),
                numeric_detail_value(item, "timing_score"),
                str(item.get("code") or ""),
            ),
            reverse=True,
        )
        history_payload = {
            "strategy": strategy_name,
            "history_window_days": history_window_days,
            "updated_trade_date": (
                current_trade_date.date().isoformat() if current_trade_date is not None else None
            ),
            "records": retained_history,
        }
        history_path.write_text(
            json.dumps(_json_safe_value(history_payload), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

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
    if strategy_name == MOZHUA_STRATEGY_NAME and history_window_days is not None:
        payload.update({
            "history_window_days": history_window_days,
            "historical_result_count": len(historical_items),
            "historical_results": historical_items,
            "historical_trade_dates": sorted(history_dates, reverse=True),
        })
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
    if strategy_name == MOZHUA_STRATEGY_NAME and history_window_days is not None:
        historical_summary_rows = []
        for item in historical_items:
            summary = {
                key: value
                for key, value in item.items()
                if key != "recent_daily_data"
            }
            for key, value in list(summary.items()):
                if isinstance(value, (list, tuple, dict)):
                    summary[key] = json.dumps(value, ensure_ascii=False)
            historical_summary_rows.append(summary)
        pd.DataFrame(historical_summary_rows).to_csv(
            output_dir / "wave_selection_history.csv",
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
    p.add_argument("--timing-output", help="可选的分阶段耗时 JSON 输出路径")
    args = p.parse_args()
    run_started = perf_counter()
    timings: List[Dict[str, Any]] = []

    def record_timing(stage: str, started: float, **context: Any) -> None:
        elapsed = perf_counter() - started
        timings.append({"stage": stage, "seconds": round(elapsed, 6), **context})
        logger.info("耗时 [%s]: %.3fs %s", stage, elapsed, context or "")

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

    stage_started = perf_counter()
    data = load_data(data_dir, codes)
    record_timing("load_data", stage_started, symbols=len(data))
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
            int | None,
            Any | None,
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

        stage_started = perf_counter()
        picks = selector.select(trade_date, data)
        record_timing("selector", stage_started, strategy=alias, selected=len(picks))
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
                    MOZHUA_HISTORY_WINDOW_DAYS
                    if selector.__class__.__name__ == "MoZhuaSelector"
                    else None,
                    selector if selector.__class__.__name__ == "MoZhuaSelector" else None,
                )
            )

        # 将结果写入日志，同时输出到控制台
        logger.info("")
        logger.info("============== 选股结果 [%s] ==============", alias)
        logger.info("交易日: %s", trade_date.date())
        logger.info("符合条件股票数: %d", len(picks))
        logger.info("%s", ", ".join(picks) if picks else "无符合条件股票")

    daily_history_cache: Dict[str, pd.DataFrame] = {}
    signal_history_days = max((spec[4] for spec in wave_output_specs), default=1)
    for (
        alias,
        details,
        preselection_details,
        output_dir,
        data_days,
        score_threshold,
        output_signal_selectors,
        history_window_days,
        history_selector,
    ) in wave_output_specs:
        stage_started = perf_counter()
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
            daily_history_cache=daily_history_cache,
            signal_history_days=signal_history_days,
            history_window_days=history_window_days,
            history_selector=history_selector,
        )
        record_timing("wave_output", stage_started, strategy=alias, cached_symbols=len(daily_history_cache))
        logger.info(
            "结构化选股结果: %s（%s，%d 只，最近 %d 根日线）",
            output_dir,
            alias,
            len(details),
            data_days,
        )

    if not args.no_visualization:
        stage_started = perf_counter()
        chart_path = Path(args.visualization_output)
        detail_path = Path(args.intersection_output)
        intersections = render_selection_overlap(selection_sets, chart_path, detail_path, trade_date)
        record_timing("visualization", stage_started)
        logger.info("策略交集圆形图: %s", chart_path)
        logger.info("策略交集明细: %s", detail_path)
        logger.info("非空策略组合数: %d", len(intersections))

    if not args.no_dashboard:
        stage_started = perf_counter()
        dashboard_path = Path(args.dashboard_output)
        render_selection_dashboard(selection_sets, dashboard_path, trade_date)
        record_timing("dashboard", stage_started)
        logger.info("交互式选股仪表盘: %s", dashboard_path)

    record_timing("total", run_started)
    if args.timing_output:
        timing_path = Path(args.timing_output)
        timing_path.parent.mkdir(parents=True, exist_ok=True)
        timing_path.write_text(json.dumps({
            "trade_date": trade_date.date().isoformat(),
            "symbols": len(data), "stages": timings,
        }, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
