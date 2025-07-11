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
import pywencai as wc
import akshare as ak
import time
import requests

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
    
def get_pool_ak(target_concept: list) -> list:
    """通过AKShare获取概念标签（兼容东方财富/同花顺双数据源）"""
    # code_short = stock_code[:6]  # 去除市场后缀
    # concepts = set()
    file_path = Path('./datas/pool_ak.json')
    if file_path.exists():
        print("使用本地数据pool_ak")
        with open('pool_th.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data

    codes = []
    stock_board_concept_name_em_df = ak.stock_board_concept_name_em()
    concepts_all = stock_board_concept_name_em_df['板块名称'].tolist()
    # with open('ak_concepts_all.json', "w", encoding="utf-8") as f:
    #     json.dump(concepts_all, f, ensure_ascii=False, indent=4)
    for ban in target_concept:
        if not ban in concepts_all:
            continue
        stock_board_concept_cons_em_df = ak.stock_board_concept_cons_em(symbol=ban)
        codes.append(stock_board_concept_cons_em_df['代码'].tolist())
    file_path = Path('./datas/pool_ak.json')
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(codes, f, ensure_ascii=False, indent=4)
    return codes


def get_stocks_by_concept(code, name):
    """获取指定概念指数下的成分股数据"""
    url = f"https://d.10jqka.com.cn/v2/blockrank/{code}/199112/d1000.js"
    headers = {
        'Referer': 'http://q.10jqka.com.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    }
    
    try:
        # print(f"正在获取 {name}({code}) 的成分股...")
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            json_str = response.text.split('(', 1)[1].rsplit(')', 1)[0]
            data = json.loads(json_str)
            
            stock_list = data.get('items', [])
            if stock_list:
                # 只提取需要的字段，不再处理价格和涨跌幅
                stocks_df = pd.DataFrame(
                    [(s.get('5', '').zfill(6),
                      s.get('55', ''),
                      name)  # 直接使用概念名称
                     for s in stock_list],
                    columns=['股票代码', '股票名称', '所属概念']
                )
                return stocks_df
            print(f"警告: {name}({code}) 未找到成分股数据")
            return pd.DataFrame(columns=['所属概念','股票代码', '股票名称' ])
        print(f"错误: 获取 {name}({code}) 数据失败，状态码: {response.status_code}")
        return None
    except Exception as e:
        print(f"错误: 获取 {name}({code}) 数据时发生异常: {str(e)}")
        return None
def get_pool_th(target_concepts):
    file_path = Path('./datas/pool_th.json')
    if file_path.exists():
        print("使用本地数据pool_th")
        with open('pool_th.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    indexs = wc.get(query="同花顺概念指数", query_type="zhishu", sort_order='desc', loop=True)
    # 获取所有概念指数
    # 用于存储所有成分股的列表
    th_pool = []
    # 遍历每个概念指数获取成分股
    for idx, row in indexs.iterrows():
        code = row['code']
        name = row['指数简称']
        if not name in target_concepts:
            continue
        # 获取成分股
        stocks_df = get_stocks_by_concept(code, name)
        if stocks_df is not None and not stocks_df.empty:
            th_pool.append([stocks_df['股票代码'].tolist()])
            res = [item for sublist in th_pool for item in sublist]
            print("get concept: ", name)
    file_path = Path('./datas/pool_th.json')
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(res[0], f, ensure_ascii=False, indent=4)
    return res[0]

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
    th_pool = get_pool_th(target_concepts)
    ak_pool = get_pool_ak(target_concepts)
    # 初始化Tushare Pro接口
    ts.set_token('745e967a0e097ad5f40c3f665fd81133bb4fcfd0fcdcb1908cc8fd06')  # 替换为你的Token
    pro = ts.pro_api()

    # 1. 概念板块池子过滤
    concepts_results = {}
    codes_res = []
    print(th_pool,ak_pool)
    for code in codes:
        #1. 使用akshare接口 和 同花顺接口 判断是否符合池子
        if code in ak_pool or code in th_pool:
            codes_res.append(code)
            continue
        #2. 使用tushare接口判断是否符合池子
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
