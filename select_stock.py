from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List
import os
import tushare as ts
import pandas as pd

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

def zpool(codes):
    # 从当前目录下的 config.json 读取配置
    config_path = os.path.join(os.path.dirname(__file__), 'zpool.json')
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        # 从JSON获取目标板块定义
        target_industries = config.get('target_industries', [])  # 行业板块
        target_concepts = config.get('target_concepts', [])      # 概念板块
        
    except FileNotFoundError:
        print(f"警告：配置文件 {config_path} 不存在，使用默认配置")
        target_industries = []
        target_concepts = ['人工智能', '风电']

    # 初始化Tushare Pro接口
    ts.set_token('745e967a0e097ad5f40c3f665fd81133bb4fcfd0fcdcb1908cc8fd06')  # 替换为你的Token
    pro = ts.pro_api()

    # 1. 概念板块池子过滤
    concepts_results = {}
    codes_res = []
    for code in codes:
        # 格式转换：纯数字 -> Tushare标准格式
        ts_code = f"{code}.SH" if str(code).startswith(('6', '9')) else f"{code}.SZ"
        
        try:
            # 获取该股票的所有概念标签
            df = pro.concept_detail(ts_code=ts_code)
            matched_concepts = [c for c in df['concept_name'] if c in target_concepts]
            concepts_results[code] = matched_concepts
        except Exception as e:
            print(f"查询失败 {code}: {e}")
            concepts_results[code] = []
        if concepts_results[code]:
            codes_res.append(code)
        else:
            print("code not in zpool concepts:", code)

    # 2. 行业板块池子过滤
    results = []
    if not target_industries:
        print("no target_industries")
        return codes_res
    # 获取股票基础信息（含行业分类和上市交易所）
    stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry,exchange')
    # 可选，可以保存所有行业板类目到文件
    # stock_basic["industry"].drop_duplicates().to_csv('所有行业板类目.txt', sep='\t', header=False, index=False)
    for code in codes_res:
         # 1. 从基础信息获取行业和交易所
        stock_info = stock_basic[stock_basic['ts_code'].str.startswith(code)].iloc[0]
        industry = stock_info['industry']
        # 2. 行业板块匹配
        if target_industries and industry in target_industries:
            results.append(code)
        else: 
            print("######code not in zpool industries:", code)
    return results

# ---------- 主函数 ----------

def main():
    p = argparse.ArgumentParser(description="Run selectors defined in configs.json")
    p.add_argument("--data-dir", default="./data", help="CSV 行情目录")
    p.add_argument("--config", default="./configs.json", help="Selector 配置文件")
    p.add_argument("--date", help="交易日 YYYY-MM-DD；缺省=数据最新日期")
    p.add_argument("--tickers", default="all", help="'all' 或逗号分隔股票代码列表")
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
    for cfg in selector_cfgs:
        if cfg.get("activate", True) is False:
            continue
        try:
            alias, selector = instantiate_selector(cfg)
        except Exception as e:
            logger.error("跳过配置 %s：%s", cfg, e)
            continue

        picks = selector.select(trade_date, data)
        res = zpool(picks)

        # 将结果写入日志，同时输出到控制台
        logger.info("")
        logger.info("============== 选股结果 [%s] ==============", alias)
        logger.info("交易日: %s", trade_date.date())
        logger.info("符合图形股票: %s", ", ".join(picks) if picks else "无符合图形股票")
        logger.info("最终结果: %s", ", ".join(res) if res else "无符合条件股票")


if __name__ == "__main__":
    main()
