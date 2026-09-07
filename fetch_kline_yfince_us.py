from __future__ import annotations

import argparse
import collections
import logging
import random
import sys
import time
import warnings
from pathlib import Path
from typing import List, Dict, Optional
import os

import pandas as pd
import yfinance as yf
from tqdm import tqdm
from datetime import datetime, timedelta
from safe_io import is_safe_ticker, write_dataframe_csv

# --------------------------- 配置 --------------------------- #
warnings.filterwarnings("ignore")

# 可选代理：未配置时使用系统默认网络，不强制连接本机代理端口。
proxy = os.environ.get('YF_PROXY', '').strip()
if proxy:
    os.environ['HTTP_PROXY'] = proxy
    os.environ['HTTPS_PROXY'] = proxy

# 修复1: 使用合理的日期范围（避免未来日期）
END_DATE = datetime.today().strftime("%Y-%m-%d")
# 改为获取过去300天数据（确保是历史日期）
START_DATE = (datetime.today() - timedelta(days=300)).strftime("%Y-%m-%d")

# 限流参数（核心配置）
BATCH_SIZE = 20                    # 每批下载的股票数量（降低并发压力）
DELAY_BETWEEN_BATCHES = 45         # 批次间隔基准（秒），实际会加随机抖动
MAX_RETRIES = 3                    # 失败重试次数
TIMEOUT = 30                       # 单次请求超时（秒）

# 增量更新：跳过已有且足够新的文件
SKIP_IF_FRESH_DAYS = 0.5             # CSV 文件在 N 天内更新过则跳过（0=禁用）

# 全局滑动窗口限速：每分钟最多发出的批次请求数
RATE_LIMIT_CALLS = 8               # 每窗口允许的最大批次数
RATE_LIMIT_WINDOW = 60             # 窗口大小（秒）

# 滑动窗口限速器（模块级单例）
_call_timestamps: collections.deque = collections.deque()
PROFILE_FALLBACK = "Unknown"

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("USStockFetcher")

# --------------------------- 限速工具 --------------------------- #
def _rate_limit_wait() -> None:
    """
    全局滑动窗口限速：确保过去 RATE_LIMIT_WINDOW 秒内的批次数不超过 RATE_LIMIT_CALLS。
    比固定等待更高效：空闲时几乎不等待，繁忙时自动踩刹车。
    """
    now = time.monotonic()
    # 清理窗口外的旧时间戳
    while _call_timestamps and now - _call_timestamps[0] > RATE_LIMIT_WINDOW:
        _call_timestamps.popleft()

    if len(_call_timestamps) >= RATE_LIMIT_CALLS:
        # 窗口已满，等到最旧那次请求滑出窗口
        oldest = _call_timestamps[0]
        wait = RATE_LIMIT_WINDOW - (now - oldest) + random.uniform(1, 3)
        if wait > 0:
            logger.info(f"⏳ 限速等待 {wait:.0f}s（滑动窗口已达 {RATE_LIMIT_CALLS} 批/分钟上限）")
            time.sleep(wait)

    _call_timestamps.append(time.monotonic())


def _is_fresh(path: Path) -> bool:
    """判断 CSV 文件是否足够新，足够新则跳过下载。"""
    if SKIP_IF_FRESH_DAYS <= 0:
        return False
    if not path.exists():
        return False
    age_days = (time.time() - path.stat().st_mtime) / 86400
    return age_days < SKIP_IF_FRESH_DAYS


def _normalize_ticker(code: str) -> str:
    """统一股票代码格式（用于下载与元数据映射）。"""
    if code is None:
        return ""
    s = str(code).strip().upper()
    if not s or s in {"UNKNOWN", "NAN", "NONE", "NULL"}:
        return ""
    normalized = s.replace(".", "-")
    return normalized if is_safe_ticker(normalized) else ""


def _clean_profile_value(value: object) -> str:
    """清洗行业字段，统一为可落盘字符串。"""
    if value is None:
        return ""

    if isinstance(value, str):
        text = value.strip()
    elif isinstance(value, (list, tuple, set)):
        text = "|".join(str(v).strip() for v in value if str(v).strip())
    elif isinstance(value, dict):
        # 部分来源可能是对象结构，优先抽取 name。
        text = str(value.get("name") or value.get("raw") or "").strip()
    else:
        text = str(value).strip()

    if not text:
        return ""

    if text.lower() in {"nan", "none", "null", "n/a", "unknown"}:
        return ""
    return text


def load_profile_seed(stocklist_path: Path) -> Dict[str, Dict[str, str]]:
    """
    尝试从股票列表文件中加载 sector/industry，减少额外网络请求。
    返回：{TICKER: {"sector": ..., "industry": ...}}
    """
    try:
        df = pd.read_csv(stocklist_path)
    except Exception as e:
        logger.warning(f"读取股票列表元数据失败（将回退到 yfinance）：{e}")
        return {}

    symbol_col = None
    for col in ["ts_code", "symbol", "ticker", "code", "Symbol"]:
        if col in df.columns:
            symbol_col = col
            break
    if symbol_col is None:
        if len(df.columns) == 0:
            return {}
        symbol_col = df.columns[0]

    sector_col = None
    for col in ["sector", "Sector", "gics_sector", "industry_sector"]:
        if col in df.columns:
            sector_col = col
            break

    industry_col = None
    for col in ["industry", "Industry", "gics_industry", "gics_sub_industry"]:
        if col in df.columns:
            industry_col = col
            break

    if sector_col is None and industry_col is None:
        return {}

    seed: Dict[str, Dict[str, str]] = {}
    for _, row in df.iterrows():
        ticker = _normalize_ticker(row.get(symbol_col, ""))
        if not ticker:
            continue

        sector = _clean_profile_value(row.get(sector_col, "")) if sector_col else ""
        industry = _clean_profile_value(row.get(industry_col, "")) if industry_col else ""
        if not sector and not industry:
            continue

        seed[ticker] = {
            "sector": sector or PROFILE_FALLBACK,
            "industry": industry or PROFILE_FALLBACK,
        }

    if seed:
        logger.info(f"从 stocklist 预加载到 {len(seed)} 条行业映射")
    return seed


def fetch_ticker_profile(
    ticker: str,
    seed_profile: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """
    获取单只股票行业信息，优先用 seed，不足时回退到 yfinance。
    """
    profile = {
        "sector": PROFILE_FALLBACK,
        "industry": PROFILE_FALLBACK,
    }

    if seed_profile:
        sector = _clean_profile_value(seed_profile.get("sector", ""))
        industry = _clean_profile_value(seed_profile.get("industry", ""))
        if sector:
            profile["sector"] = sector
        if industry:
            profile["industry"] = industry

    need_remote = (
        profile["sector"] == PROFILE_FALLBACK
        or profile["industry"] == PROFILE_FALLBACK
    )
    if not need_remote:
        return profile

    try:
        t = yf.Ticker(ticker)
        try:
            info = t.get_info() or {}
        except Exception:
            # 兼容部分版本/环境下 get_info 不稳定的情况
            info = t.info or {}

        if isinstance(info, dict):
            if profile["sector"] == PROFILE_FALLBACK:
                s = _clean_profile_value(
                    info.get("sector")
                    or info.get("sectorDisp")
                    or info.get("category")
                )
                if s:
                    profile["sector"] = s

            if profile["industry"] == PROFILE_FALLBACK:
                i = _clean_profile_value(
                    info.get("industry")
                    or info.get("industryDisp")
                )
                if i:
                    profile["industry"] = i
    except Exception as e:
        logger.debug(f"{ticker}: 拉取行业信息失败，使用默认值。原因: {e}")

    return profile


# --------------------------- 核心函数 --------------------------- #
def download_batch(tickers: List[str]) -> Dict[str, pd.DataFrame]:
    """
    批量下载一批股票数据
    """
    try:
        # 使用 yfinance 内置多线程批量下载
        data = yf.download(
            tickers=tickers,
            start=START_DATE,
            end=END_DATE,
            interval="1d",
            threads=False,          # 关闭并发，改为串行，避免瞬间并发触发 429
            progress=False,
            timeout=TIMEOUT,
            group_by="ticker"       # 按股票代码分组
        )
        
        # 空数据检查
        if data.empty:
            logger.warning(f"批次 {tickers[0]}... 返回空数据")
            return {}
        
        # 将 MultiIndex 数据转为字典
        # 兼容单 ticker（普通列）和多 ticker（MultiIndex 列）两种情况
        result = {}
        if isinstance(data.columns, pd.MultiIndex):
            # 多 ticker：columns 是 (price_field, ticker) 或 (ticker, price_field)
            level0 = data.columns.get_level_values(0)
            for ticker in tickers:
                ticker_upper = ticker.upper()
                if ticker_upper in level0:
                    result[ticker] = data[ticker_upper].copy()
        else:
            # 单 ticker：直接返回整个 DataFrame
            if len(tickers) == 1:
                result[tickers[0]] = data.copy()
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg or "too many requests" in error_msg.lower():
            logger.error(f"⛔️ 触发 429 限流！当前批次: {tickers[:3]}...")
            raise RuntimeError(f"Yahoo 429 限流: {error_msg}")
        elif "403" in error_msg or "forbidden" in error_msg.lower():
            logger.error(f"🚫 触发 403 封禁！")
            raise RuntimeError(f"Yahoo 403 封禁: {error_msg}")
        else:
            logger.warning(f"批次 {tickers[0]}... 下载失败: {error_msg}")
            return {}

def process_and_save_data(
    ticker: str,
    df: pd.DataFrame,
    output_dir: Path,
    profile: Optional[Dict[str, str]] = None,
) -> bool:
    """
    处理和保存单只股票数据
    """
    try:
        if df is None or df.empty:
            logger.debug(f"{ticker}: 空数据，跳过")
            return False
        
        # 重置索引
        df = df.reset_index()
        
        # 标准化列名
        rename_map = {}
        if 'Date' in df.columns:
            rename_map['Date'] = 'date'
        if 'Open' in df.columns:
            rename_map['Open'] = 'open'
        if 'Close' in df.columns:
            rename_map['Close'] = 'close'
        if 'High' in df.columns:
            rename_map['High'] = 'high'
        if 'Low' in df.columns:
            rename_map['Low'] = 'low'
        if 'Volume' in df.columns:
            rename_map['Volume'] = 'volume'
        
        df = df.rename(columns=rename_map)
        
        # 确保有 date 列
        if 'date' not in df.columns:
            logger.warning(f"{ticker}: 缺少date列")
            return False
        
        # 转换为datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # 去重和排序
        df = df.drop_duplicates(subset='date').sort_values('date').reset_index(drop=True)

        # 统一追加行业字段（每个交易日重复同一标签，便于按票回读时直接可用）
        profile = profile or {}
        df['sector'] = _clean_profile_value(profile.get('sector', '')) or PROFILE_FALLBACK
        df['industry'] = _clean_profile_value(profile.get('industry', '')) or PROFILE_FALLBACK
        
        # 选择需要的列
        required_columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'sector', 'industry']
        for col in required_columns[1:]:  # date 已存在
            if col not in df.columns:
                df[col] = None
        
        df = df[required_columns]
        
        # 保存为CSV
        write_dataframe_csv(df, output_dir, ticker)
        
        logger.debug(f"{ticker}: 保存成功，{len(df)} 行数据")
        return True
        
    except Exception as e:
        logger.error(f"{ticker}: 处理失败 - {e}")
        return False

def load_stock_list(stocklist_path: Path) -> List[str]:
    """
    加载股票列表
    # 修复2: 明确使用 ts_code 列（这是你的输入文件中的股票代码列）
    """
    try:
        df = pd.read_csv(stocklist_path)
        
        # 修复: 明确使用 ts_code 列
        if 'ts_code' in df.columns:
            codes = df['ts_code'].astype(str).str.strip().tolist()
            logger.info(f"使用 'ts_code' 列加载股票代码")
        else:
            # 如果 ts_code 列不存在，回退到原始逻辑
            logger.warning(f"'ts_code' 列不存在，尝试其他列名")
            for col_name in ['ts_code', 'symbol', 'ticker', 'code']:
                if col_name in df.columns:
                    codes = df[col_name].astype(str).str.strip().tolist()
                    logger.info(f"使用 '{col_name}' 列加载股票代码")
                    break
            else:
                # 使用第一列
                codes = df.iloc[:, 0].astype(str).str.strip().tolist()
                logger.info(f"使用第一列加载股票代码")
        
        # 过滤空值和无效值，并统一代码格式（BRK.B -> BRK-B）
        codes = [_normalize_ticker(c) for c in codes]
        codes = [c for c in codes if c]
        
        # 去重
        codes = list(dict.fromkeys(codes))
        
        logger.info(f"从 {stocklist_path} 加载了 {len(codes)} 只股票")
        if codes:
            logger.info(f"示例: {codes[:5]}")
        
        return codes
        
    except Exception as e:
        logger.error(f"加载股票列表失败: {e}")
        return []

# --------------------------- 主程序 --------------------------- #
def main():
    parser = argparse.ArgumentParser(description="批量下载美股日线数据")
    parser.add_argument("--stocklist", type=Path, default="./data/tools/stocklist_sp400_20260902.csv",
                       help="股票列表CSV文件路径")
    parser.add_argument("--out", type=Path, default="./data/us_stocks", 
                       help="输出目录")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                       help="每批下载的股票数量")
    parser.add_argument("--delay", type=int, default=DELAY_BETWEEN_BATCHES,
                       help="批次间隔时间（秒）")
    args = parser.parse_args()
    
    # 创建输出目录
    args.out.mkdir(parents=True, exist_ok=True)
    
    # 1. 加载股票列表
    all_tickers = load_stock_list(args.stocklist)
    if not all_tickers:
        logger.error("没有可下载的股票代码")
        return

    # 尝试从 stocklist 预加载行业映射，减少额外网络请求
    profile_seed = load_profile_seed(args.stocklist)
    profile_cache: Dict[str, Dict[str, str]] = {}

    # 调试: 显示前几个股票代码
    logger.info(f"前10个股票代码: {all_tickers[:10]}")

    # 增量优化：过滤掉已有新鲜 CSV 的股票
    if SKIP_IF_FRESH_DAYS > 0:
        fresh = [t for t in all_tickers if _is_fresh(args.out / f"{t}.csv")]
        skip_count = len(fresh)
        all_tickers = [t for t in all_tickers if not _is_fresh(args.out / f"{t}.csv")]
        if skip_count:
            logger.info(f"⚡ 增量跳过 {skip_count} 只（CSV 在 {SKIP_IF_FRESH_DAYS} 天内已更新），剩余 {len(all_tickers)} 只")
    
    # 2. 分批下载
    total_batches = (len(all_tickers) - 1) // args.batch_size + 1
    success_count = 0
    fail_count = 0
    failed_tickers: List[str] = []
    
    for batch_idx in range(0, len(all_tickers), args.batch_size):
        batch_num = batch_idx // args.batch_size + 1
        batch_tickers = all_tickers[batch_idx:batch_idx + args.batch_size]
        
        logger.info(f"\n{'='*50}")
        logger.info(f"批次 {batch_num}/{total_batches}: 下载 {len(batch_tickers)} 只股票")
        logger.info(f"示例: {batch_tickers[:3]}...")
        
        # 3. 批量下载（带限速 + 重试）
        batch_data = {}
        for retry in range(MAX_RETRIES):
            try:
                _rate_limit_wait()          # 滑动窗口限速，替代固定等待
                batch_data = download_batch(batch_tickers)
                break
            except RuntimeError as e:
                if "429" in str(e) or "403" in str(e):
                    # 限流/封禁，延长等待时间
                    wait_time = 60 * (retry + 1)  # 60s, 120s, 180s
                    logger.warning(f"⚠️ 限流检测，等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ 下载失败: {e}")
                    break
            except Exception as e:
                logger.error(f"❌ 未知错误: {e}")
                if retry < MAX_RETRIES - 1:
                    time.sleep(10)
        
        # 4. 处理并保存数据
        if batch_data:
            for ticker in batch_tickers:
                if ticker in batch_data:
                    ticker_key = _normalize_ticker(ticker)
                    if ticker_key in profile_cache:
                        profile = profile_cache[ticker_key]
                    else:
                        profile = fetch_ticker_profile(
                            ticker=ticker_key,
                            seed_profile=profile_seed.get(ticker_key),
                        )
                        profile_cache[ticker_key] = profile

                    if process_and_save_data(ticker, batch_data[ticker], args.out, profile=profile):
                        success_count += 1
                    else:
                        fail_count += 1
                        failed_tickers.append(ticker)
                else:
                    logger.warning(f"{ticker}: 不在返回数据中")
                    fail_count += 1
                    failed_tickers.append(ticker)
        else:
            logger.warning(f"批次 {batch_num} 无数据，全部标记为失败")
            fail_count += len(batch_tickers)
            failed_tickers.extend(batch_tickers)
        
        # 5. 批次间隔（滑动窗口限速已内置节流，此处仅保留小抖动防指纹）
        if batch_num < total_batches:
            jitter = random.uniform(1, 5)
            time.sleep(jitter)
    
    # 6. 最终统计
    logger.info(f"\n{'='*50}")
    logger.info(f"📊 下载完成统计")
    logger.info(f"成功: {success_count} 只")
    logger.info(f"失败: {fail_count} 只")
    logger.info(f"总计: {len(all_tickers)} 只")
    logger.info(f"数据保存至: {args.out.resolve()}")

    if failed_tickers:
        print(f"\n{'='*50}")
        print(f"❌ 下载失败的股票（共 {len(failed_tickers)} 只）：")
        print(', '.join(failed_tickers))
        print('='*50)

if __name__ == "__main__":
    main()