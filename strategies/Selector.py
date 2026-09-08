from typing import Dict, List, Optional, Any

from scipy.signal import find_peaks
import numpy as np
import pandas as pd

from features.bbd_signals import compute_bbd_signals
from strategies.one_wave_structure import (
    find_active_one_wave,
    find_one_wave_preselections,
    one_wave_to_dict,
)
from strategies.wave_structure import active_wave_to_dict, find_active_wave

# --------------------------- 通用指标 --------------------------- #

def compute_kdj(df: pd.DataFrame, n: int = 9) -> pd.DataFrame:
    if df.empty:
        return df.assign(K=np.nan, D=np.nan, J=np.nan)

    low_n = df["low"].rolling(window=n, min_periods=1).min()
    high_n = df["high"].rolling(window=n, min_periods=1).max()
    rsv = (df["close"] - low_n) / (high_n - low_n + 1e-9) * 100

    K = np.zeros_like(rsv, dtype=float)
    D = np.zeros_like(rsv, dtype=float)
    for i in range(len(df)):
        if i == 0:
            K[i] = D[i] = 50.0
        else:
            K[i] = 2 / 3 * K[i - 1] + 1 / 3 * rsv.iloc[i]
            D[i] = 2 / 3 * D[i - 1] + 1 / 3 * K[i]
    J = 3 * K - 2 * D
    return df.assign(K=K, D=D, J=J)


def compute_bbi(df: pd.DataFrame) -> pd.Series:
    ma3 = df["close"].rolling(3).mean()
    ma6 = df["close"].rolling(6).mean()
    ma12 = df["close"].rolling(12).mean()
    ma24 = df["close"].rolling(24).mean()
    return (ma3 + ma6 + ma12 + ma24) / 4


def compute_rsv(
    df: pd.DataFrame,
    n: int,
) -> pd.Series:
    """
    按公式：RSV(N) = 100 × (C - LLV(L,N)) ÷ (HHV(C,N) - LLV(L,N))
    - C 用收盘价最高值 (HHV of close)
    - L 用最低价最低值 (LLV of low)
    """
    low_n = df["low"].rolling(window=n, min_periods=1).min()
    high_close_n = df["close"].rolling(window=n, min_periods=1).max()
    rsv = (df["close"] - low_n) / (high_close_n - low_n + 1e-9) * 100.0
    return rsv


def compute_dif(df: pd.DataFrame, fast: int = 12, slow: int = 26) -> pd.Series:
    """计算 MACD 指标中的 DIF (EMA fast - EMA slow)。"""
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    return ema_fast - ema_slow


def compute_tdx_sma(series: pd.Series, n: int, m: int) -> pd.Series:
    """
    计算通达信 SMA(X, N, M)（递推平滑，不是简单均线）。
    等价写法：EMA(alpha=M/N, adjust=False)。
    """
    if n <= 0:
        raise ValueError("n 必须 > 0")
    if m <= 0 or m > n:
        raise ValueError("m 必须满足 0 < m <= n")
    return series.astype(float).ewm(alpha=m / n, adjust=False).mean()


def cross_up(a: pd.Series, b: pd.Series) -> pd.Series:
    """CROSS(a,b): 当日上穿。"""
    return ((a > b) & (a.shift(1) <= b.shift(1))).fillna(False)


def ref_prev_cross_value(series: pd.Series, cross_sig: pd.Series) -> pd.Series:
    """
    对每个时点，返回“上一笔同类 cross 发生时”的 series 值。
    若不存在上一笔 cross，则返回 NaN。
    """
    n = len(series)
    if n == 0:
        return pd.Series(dtype=float)

    pos = np.arange(n, dtype=float)
    cross_pos = pd.Series(np.where(cross_sig.to_numpy(), pos, np.nan), index=series.index)
    prev_pos = cross_pos.shift(1).ffill()

    out = pd.Series(np.nan, index=series.index, dtype=float)
    valid = prev_pos.notna()
    if valid.any():
        idx = prev_pos[valid].astype(int).to_numpy()
        out.loc[valid] = series.astype(float).to_numpy()[idx]
    return out


def bbi_deriv_uptrend(
    bbi: pd.Series,
    *,
    min_window: int,
    max_window: int | None = None,
    q_threshold: float = 0.0,
) -> bool:
    """
    判断 BBI 是否“整体上升”。

    令最新交易日为 T，在区间 [T-w+1, T]（w 自适应，w ≥ min_window 且 ≤ max_window）
    内，先将 BBI 归一化：BBI_norm(t) = BBI(t) / BBI(T-w+1)。

    再计算一阶差分 Δ(t) = BBI_norm(t) - BBI_norm(t-1)。  
    若 Δ(t) 的前 q_threshold 分位数 ≥ 0，则认为该窗口通过；只要存在
    **最长** 满足条件的窗口即可返回 True。q_threshold=0 时退化为
    “全程单调不降”（旧版行为）。

    Parameters
    ----------
    bbi : pd.Series
        BBI 序列（最新值在最后一位）。
    min_window : int
        检测窗口的最小长度。
    max_window : int | None
        检测窗口的最大长度；None 表示不设上限。
    q_threshold : float, default 0.0
        允许一阶差分为负的比例（0 ≤ q_threshold ≤ 1）。
    """
    if not 0.0 <= q_threshold <= 1.0:
        raise ValueError("q_threshold 必须位于 [0, 1] 区间内")

    bbi = bbi.dropna()
    if len(bbi) < min_window:
        return False

    longest = min(len(bbi), max_window or len(bbi))

    # 自最长窗口向下搜索，找到任一满足条件的区间即通过
    for w in range(longest, min_window - 1, -1):
        seg = bbi.iloc[-w:]                # 区间 [T-w+1, T]
        norm = seg / seg.iloc[0]           # 归一化
        diffs = np.diff(norm.values)       # 一阶差分
        if np.quantile(diffs, q_threshold) >= 0:
            return True
    return False


def _find_peaks(
    df: pd.DataFrame,
    *,
    column: str = "high",
    distance: Optional[int] = None,
    prominence: Optional[float] = None,
    height: Optional[float] = None,
    width: Optional[float] = None,
    rel_height: float = 0.5,
    **kwargs: Any,
) -> pd.DataFrame:
    
    if column not in df.columns:
        raise KeyError(f"'{column}' not found in DataFrame columns: {list(df.columns)}")

    y = df[column].to_numpy()

    indices, props = find_peaks(
        y,
        distance=distance,
        prominence=prominence,
        height=height,
        width=width,
        rel_height=rel_height,
        **kwargs,
    )

    peaks_df = df.iloc[indices].copy()
    peaks_df["is_peak"] = True

    # Flatten SciPy arrays into columns (only those with same length as indices)
    for key, arr in props.items():
        if isinstance(arr, (list, np.ndarray)) and len(arr) == len(indices):
            peaks_df[f"peak_{key}"] = arr

    return peaks_df

def last_valid_ma_cross_up(
    close: pd.Series,
    ma: pd.Series,
    lookback_n: int | None = None,
) -> Optional[int]:
    """
    查找“有效上穿 MA”的最后一个交易日 T（close[T-1] < ma[T-1] 且 close[T] ≥ ma[T]）。
    - 返回的是 **整数位置**（iloc 用）。
    - lookback_n: 仅在最近 N 根内查找；None 则全历史。
    """
    n = len(close)
    start = 1  # 至少要从 1 起，因为要看 T-1
    if lookback_n is not None:
        start = max(start, n - lookback_n)

    # 自后向前找最后一次有效上穿
    for i in range(n - 1, start - 1, -1):
        if i - 1 < 0:
            continue
        c_prev, c_now = close.iloc[i - 1], close.iloc[i]
        m_prev, m_now = ma.iloc[i - 1], ma.iloc[i]
        if pd.notna(c_prev) and pd.notna(c_now) and pd.notna(m_prev) and pd.notna(m_now):
            if c_prev < m_prev and c_now >= m_now:
                return i
    return None


def compute_zx_lines(
    df: pd.DataFrame,
    m1: int = 14, m2: int = 28, m3: int = 57, m4: int = 114
) -> tuple[pd.Series, pd.Series]:
    """返回 (ZXDQ, ZXDKX)
    ZXDQ = EMA(EMA(C,10),10)
    ZXDKX = (MA(C,14)+MA(C,28)+MA(C,57)+MA(C,114))/4
    """
    close = df["close"].astype(float)
    zxdq = close.ewm(span=10, adjust=False).mean().ewm(span=10, adjust=False).mean()

    ma1 = close.rolling(window=m1, min_periods=m1).mean()
    ma2 = close.rolling(window=m2, min_periods=m2).mean()
    ma3 = close.rolling(window=m3, min_periods=m3).mean()
    ma4 = close.rolling(window=m4, min_periods=m4).mean()
    zxdkx = (ma1 + ma2 + ma3 + ma4) / 4.0
    return zxdq, zxdkx


def passes_day_constraints_today(df: pd.DataFrame, pct_limit: float = 0.02, amp_limit: float = 0.07) -> bool:
    """
    所有战法的统一当日过滤：
    1) 当前交易日相较于前一日涨跌幅 < pct_limit（绝对值）
    2) 当日振幅（High-Low 相对 Low） < amp_limit
    """
    if len(df) < 2:
        return False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    close_today = float(last["close"])
    close_yest = float(prev["close"])
    high_today = float(last["high"])
    low_today  = float(last["low"])
    if close_yest <= 0 or low_today <= 0:
        return False
    pct_chg = abs(close_today / close_yest - 1.0)
    amplitude = (high_today - low_today) / low_today
    return (pct_chg < pct_limit) and (amplitude < amp_limit)


def zx_condition_at_positions(
    df: pd.DataFrame,
    *,
    require_close_gt_long: bool = True,
    require_short_gt_long: bool = True,
    pos: int | None = None,
) -> bool:
    """
    在指定位置 pos（iloc 位置；None 表示当日）检查知行条件：
      - 收盘 > 长期线（可选）
      - 短期线 > 长期线（可选）
    注：长期线需满样本；若为 NaN 直接返回 False。
    """
    if df.empty:
        return False
    zxdq, zxdkx = compute_zx_lines(df)
    if pos is None:
        pos = len(df) - 1

    if pos < 0 or pos >= len(df):
        return False

    s = float(zxdq.iloc[pos])
    l = float(zxdkx.iloc[pos]) if pd.notna(zxdkx.iloc[pos]) else float("nan")
    c = float(df["close"].iloc[pos])

    if not np.isfinite(l) or not np.isfinite(s):
        return False

    if require_close_gt_long and not (c > l):
        return False
    if require_short_gt_long and not (s > l):
        return False
    return True

# --------------------------- Selector 类 --------------------------- #
class BBIKDJSelector:
    """
    自适应 *BBI(导数)* + *KDJ* 选股器
        • BBI: 允许 bbi_q_threshold 比例的回撤
        • KDJ: J < threshold ；或位于历史 J 的 j_q_threshold 分位及以下
        • MACD: DIF > 0
        • 收盘价波动幅度 ≤ price_range_pct
        • 当日约束: 绝对涨跌幅 < day_pct_limit、振幅 < day_amp_limit
        • 知行线: 收盘 > 长期线，且可配置是否要求 短期线 > 长期线
    """

    def __init__(
        self,
        j_threshold: float = -5,
        bbi_min_window: int = 90,
        max_window: int = 90,
        price_range_pct: float = 100.0,
        bbi_q_threshold: float = 0.05,
        j_q_threshold: float = 0.10,
        day_pct_limit: float = 0.02,
        day_amp_limit: float = 0.07,
        require_short_gt_long: bool = True,
    ) -> None:
        self.j_threshold = j_threshold
        self.bbi_min_window = bbi_min_window
        self.max_window = max_window
        self.price_range_pct = price_range_pct
        self.bbi_q_threshold = bbi_q_threshold  # ← 原 q_threshold
        self.j_q_threshold = j_q_threshold      # ← 新增
        self.day_pct_limit = day_pct_limit
        self.day_amp_limit = day_amp_limit
        self.require_short_gt_long = require_short_gt_long

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        hist = hist.copy()
        hist["BBI"] = compute_bbi(hist)
        
        if not passes_day_constraints_today(
            hist,
            pct_limit=self.day_pct_limit,
            amp_limit=self.day_amp_limit,
        ):
            return False

        # 0. 收盘价波动幅度约束（最近 max_window 根 K 线）
        win = hist.tail(self.max_window)
        high, low = win["close"].max(), win["close"].min()
        if low <= 0 or (high / low - 1) > self.price_range_pct:           
            return False

        # 1. BBI 上升（允许部分回撤）
        if not bbi_deriv_uptrend(
            hist["BBI"],
            min_window=self.bbi_min_window,
            max_window=self.max_window,
            q_threshold=self.bbi_q_threshold,
        ):            
            return False

        # 2. KDJ 过滤 —— 双重条件
        kdj = compute_kdj(hist)
        j_today = float(kdj.iloc[-1]["J"])

        # 最近 max_window 根 K 线的 J 分位
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_quantile = float(j_window.quantile(self.j_q_threshold))

        if not (j_today < self.j_threshold or j_today <= j_quantile):
            
            return False
        
        # —— 2.5 60日均线条件（使用通用函数）
        hist["MA60"] = hist["close"].rolling(window=60, min_periods=1).mean()

        # 当前必须在 MA60 上方（保持原条件）
        if hist["close"].iloc[-1] < hist["MA60"].iloc[-1]:
            return False

        # 寻找最近一次“有效上穿 MA60”的 T（使用 max_window 作为回看长度，避免过旧）
        t_pos = last_valid_ma_cross_up(hist["close"], hist["MA60"], lookback_n=self.max_window)
        if t_pos is None:
            return False        

        # 3. MACD：DIF > 0
        hist["DIF"] = compute_dif(hist)
        if hist["DIF"].iloc[-1] <= 0:
            return False
       
        # 4. 当日：收盘>长期线；是否要求短期线>长期线由参数控制
        if not zx_condition_at_positions(
            hist,
            require_close_gt_long=True,
            require_short_gt_long=self.require_short_gt_long,
            pos=None,
        ):
            return False

        return True

    # ---------- 多股票批量 ---------- #
    def select(
        self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            # 额外预留 20 根 K 线缓冲
            hist = hist.tail(self.max_window + 20)
            if self._passes_filters(hist):
                picks.append(code)
        return picks


class SuperB1Selector:
    """SuperB1 选股器

    过滤逻辑概览
    ----------------
    1. **历史匹配 (t_m)** — 在 *lookback_n* 个交易日窗口内，至少存在一日
       满足 :class:`BBIKDJSelector`。

    2. **盘整区间** — 区间 ``[t_m, date-1]`` 收盘价波动率不超过 ``close_vol_pct``。

    3. **当日下跌** — ``(close_{date-1} - close_date) / close_{date-1}``
       ≥ ``price_drop_pct``。

    4. **J 值极低** — ``J < j_threshold`` *或* 位于历史 ``j_q_threshold`` 分位。
    """

    # ---------------------------------------------------------------------
    # 构造函数
    # ---------------------------------------------------------------------
    def __init__(
        self,
        *,
        lookback_n: int = 60,
        close_vol_pct: float = 0.05,
        price_drop_pct: float = 0.03,
        j_threshold: float = -5,
        j_q_threshold: float = 0.10,
        # ↓↓↓ 新增：嵌套 BBIKDJSelector 配置
        B1_params: Optional[Dict[str, Any]] = None        
    ) -> None:        
        # ---------- 参数合法性检查 ----------
        if lookback_n < 2:
            raise ValueError("lookback_n 应 ≥ 2")
        if not (0 < close_vol_pct < 1):
            raise ValueError("close_vol_pct 应位于 (0, 1) 区间")
        if not (0 < price_drop_pct < 1):
            raise ValueError("price_drop_pct 应位于 (0, 1) 区间")
        if not (0 <= j_q_threshold <= 1):
            raise ValueError("j_q_threshold 应位于 [0, 1] 区间")
        if B1_params is None:
            raise ValueError("bbi_params没有给出")

        # ---------- 基本参数 ----------
        self.lookback_n = lookback_n
        self.close_vol_pct = close_vol_pct
        self.price_drop_pct = price_drop_pct
        self.j_threshold = j_threshold
        self.j_q_threshold = j_q_threshold

        # ---------- 内部 BBIKDJSelector ----------
        self.bbi_selector = BBIKDJSelector(**(B1_params or {}))

        # 为保证给 BBIKDJSelector 提供足够历史，预留额外缓冲
        self._extra_for_bbi = self.bbi_selector.max_window + 20

    # 单支股票过滤核心
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        if len(hist) < 2:
            return False

        # —— 新增：所有战法统一当日过滤
        if not passes_day_constraints_today(hist):
            return False

        # ---------- Step-0: 数据量判断 ----------
        if len(hist) < self.lookback_n + self._extra_for_bbi:
            return False

        # ---------- Step-1: 搜索满足 BBIKDJ 的 t_m ----------
        lb_hist = hist.tail(self.lookback_n + 1)  # +1 以排除自身
        tm_idx: int | None = None
        for idx in lb_hist.index[:-1]:
            if self.bbi_selector._passes_filters(hist.loc[:idx]):
                tm_idx = idx
                stable_seg = hist.loc[tm_idx : hist.index[-2], "close"]
                if len(stable_seg) < 3:
                    tm_idx = None
                    break
                high, low = stable_seg.max(), stable_seg.min()
                if low <= 0 or (high / low - 1) > self.close_vol_pct:
                    tm_idx = None
                    continue
                else:
                    break
        if tm_idx is None:
            return False

        # —— 新增：在 t_m 当日检查【收盘>长期线 且 短期线>长期线】
        tm_pos = hist.index.get_loc(tm_idx)
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=tm_pos):
            return False

        # ---------- Step-3: 当日相对前一日跌幅 ----------
        close_today, close_prev = hist["close"].iloc[-1], hist["close"].iloc[-2]
        if close_prev <= 0 or (close_prev - close_today) / close_prev < self.price_drop_pct:
            return False

        # ---------- Step-4: J 值极低 ----------
        kdj = compute_kdj(hist)
        j_today = float(kdj["J"].iloc[-1])
        j_window = kdj["J"].iloc[-self.lookback_n:].dropna()
        j_q_val = float(j_window.quantile(self.j_q_threshold)) if not j_window.empty else np.nan
        if not (j_today < self.j_threshold or j_today <= j_q_val):
            return False

        # —— 当日仅要求【短期线>长期线】
        if not zx_condition_at_positions(hist, require_close_gt_long=False, require_short_gt_long=True, pos=None):
            return False

        return True

    # 批量选股接口
    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:        
        picks: List[str] = []
        min_len = self.lookback_n + self._extra_for_bbi

        for code, df in data.items():
            hist = df[df["date"] <= date].tail(min_len)
            if len(hist) < min_len:
                continue
            if self._passes_filters(hist):
                picks.append(code)

        return picks


class PeakKDJSelector:
    """
    Peaks + KDJ 选股器    
    """

    def __init__(
        self,
        j_threshold: float = -5,
        max_window: int = 90,
        fluc_threshold: float = 0.03,
        gap_threshold: float = 0.02,
        j_q_threshold: float = 0.10,
    ) -> None:
        self.j_threshold = j_threshold
        self.max_window = max_window
        self.fluc_threshold = fluc_threshold  # 当日↔peak_(t-n) 波动率上限
        self.gap_threshold = gap_threshold    # oc_prev 必须高于区间最低收盘价的比例
        self.j_q_threshold = j_q_threshold

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        if hist.empty:
            return False
        
        if not passes_day_constraints_today(hist):
            return False

        hist = hist.copy().sort_values("date")
        hist["oc_max"] = hist[["open", "close"]].max(axis=1)

        # 1. 提取 peaks
        peaks_df = _find_peaks(
            hist,
            column="oc_max",
            distance=6,
            prominence=0.5,
        )
        
        # 至少两个峰      
        date_today = hist.iloc[-1]["date"]
        peaks_df = peaks_df[peaks_df["date"] < date_today]
        if len(peaks_df) < 2:               
            return False

        peak_t = peaks_df.iloc[-1]          # 最新一个峰
        peaks_list = peaks_df.reset_index(drop=True)
        oc_t = peak_t.oc_max
        total_peaks = len(peaks_list)

        # 2. 回溯寻找 peak_(t-n)
        target_peak = None        
        for idx in range(total_peaks - 2, -1, -1):
            peak_prev = peaks_list.loc[idx]
            oc_prev = peak_prev.oc_max
            if oc_t <= oc_prev:             # 要求 peak_t > peak_(t-n)
                continue

            # 只有当“总峰数 ≥ 3”时才检查区间内其他峰 oc_max
            if total_peaks >= 3 and idx < total_peaks - 2:
                inter_oc = peaks_list.loc[idx + 1 : total_peaks - 2, "oc_max"]
                if not (inter_oc < oc_prev).all():
                    continue

            # 新增： oc_prev 高于区间最低收盘价 gap_threshold
            date_prev = peak_prev.date
            mask = (hist["date"] > date_prev) & (hist["date"] < peak_t.date)
            min_close = hist.loc[mask, "close"].min()
            if pd.isna(min_close):
                continue                    # 区间无数据
            if oc_prev <= min_close * (1 + self.gap_threshold):
                continue

            target_peak = peak_prev
            
            break

        if target_peak is None:
            return False

        # 3. 当日收盘价波动率
        close_today = hist.iloc[-1]["close"]
        fluc_pct = abs(close_today - target_peak.close) / target_peak.close
        if fluc_pct > self.fluc_threshold:
            return False

        # 4. KDJ 过滤
        kdj = compute_kdj(hist)
        j_today = float(kdj.iloc[-1]["J"])
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_quantile = float(j_window.quantile(self.j_q_threshold))
        if not (j_today < self.j_threshold or j_today <= j_quantile):
            return False

        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True

    # ---------- 多股票批量 ---------- #
    def select(
        self,
        date: pd.Timestamp,
        data: Dict[str, pd.DataFrame],
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            hist = hist.tail(self.max_window + 20)  # 额外缓冲
            if self._passes_filters(hist):
                picks.append(code)
        return picks
    

class BBIShortLongSelector:
    """
    BBI 上升 + 短/长期 RSV 条件 + DIF > 0 选股器
    """
    def __init__(
        self,
        n_short: int = 3,
        n_long: int = 21,
        m: int = 3,
        bbi_min_window: int = 90,
        max_window: int = 150,
        bbi_q_threshold: float = 0.05,
        upper_rsv_threshold: float = 75,
        lower_rsv_threshold: float = 25
    ) -> None:
        if m < 2:
            raise ValueError("m 必须 ≥ 2")
        self.n_short = n_short
        self.n_long = n_long
        self.m = m
        self.bbi_min_window = bbi_min_window
        self.max_window = max_window
        self.bbi_q_threshold = bbi_q_threshold
        self.upper_rsv_threshold = upper_rsv_threshold
        self.lower_rsv_threshold = lower_rsv_threshold

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        hist = hist.copy()
        hist["BBI"] = compute_bbi(hist)
        
        if not passes_day_constraints_today(hist):
            return False      

        # 1. BBI 上升（允许部分回撤）
        if not bbi_deriv_uptrend(
            hist["BBI"],
            min_window=self.bbi_min_window,
            max_window=self.max_window,
            q_threshold=self.bbi_q_threshold,
        ):
            return False

        # 2. 计算短/长期 RSV -----------------
        hist["RSV_short"] = compute_rsv(hist, self.n_short)
        hist["RSV_long"] = compute_rsv(hist, self.n_long)

        if len(hist) < self.m:
            return False                        # 数据不足

        win = hist.iloc[-self.m :]              # 最近 m 天
        long_ok = (win["RSV_long"] >= self.upper_rsv_threshold).all() # 长期 RSV 全 ≥ upper_rsv_threshold

        short_series = win["RSV_short"]

        # 条件：从最近 m 天的第一天起，存在某天 i 满足 RSV_short[i] >= upper，
        # 且在该天之后（j > i）存在某天 j 满足 RSV_short[j] < lower
        mask_upper = short_series >= self.upper_rsv_threshold
        mask_lower = short_series < self.lower_rsv_threshold

        has_upper_then_lower = False
        if mask_upper.any():
            upper_indices = np.where(mask_upper.to_numpy())[0]
            for i in upper_indices:
                # 只检查 i 之后的日子
                if i + 1 < len(short_series) and mask_lower.iloc[i + 1 :].any():
                    has_upper_then_lower = True
                    break
        
        end_ok = short_series.iloc[-1] >= self.upper_rsv_threshold

        if not (long_ok and has_upper_then_lower and end_ok):
            return False

        # 3. MACD：DIF > 0 -------------------
        hist["DIF"] = compute_dif(hist)
        if hist["DIF"].iloc[-1] <= 0:
            return False

        # 4. 新增：知行情形
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True


    # ---------- 多股票批量 ---------- #
    def select(
        self,
        date: pd.Timestamp,
        data: Dict[str, pd.DataFrame],
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            # 预留足够长度：RSV 计算窗口 + BBI 检测窗口 + m
            need_len = (
                max(self.n_short, self.n_long)
                + self.bbi_min_window
                + self.m
            )
            hist = hist.tail(max(need_len, self.max_window))
            if self._passes_filters(hist):
                picks.append(code)
        return picks
    
    
class MA60CrossVolumeWaveSelector:
    """
    条件：
    1) 当日 J 绝对低或相对低（J < j_threshold 或 J ≤ 近 max_window 根 J 的 j_q_threshold 分位）
    2) 最近 lookback_n 内，存在一次“有效上穿 MA60”（t-1 收盘 < MA60, t 收盘 ≥ MA60）；
       且从该上穿日 T 到今天的“上涨波段”日均成交量 ≥ 上穿前等长窗口的日均成交量 * vol_multiple
       —— 上涨波段定义为 [T, today] 间的所有交易日（不做趋势单调性强约束，稳健且可复现）
    3) 近 ma60_slope_days（默认 5）个交易日的 MA60 回归斜率 > 0
    """
    def __init__(
        self,
        *,
        lookback_n: int = 60,
        vol_multiple: float = 1.5,
        j_threshold: float = -5.0,
        j_q_threshold: float = 0.10,
        ma60_slope_days: int = 5,
        max_window: int = 120,   # 用于计算 J 分位        
    ) -> None:
        if lookback_n < 2:
            raise ValueError("lookback_n 应 ≥ 2")
        if not (0.0 <= j_q_threshold <= 1.0):
            raise ValueError("j_q_threshold 应位于 [0,1]")
        if ma60_slope_days < 2:
            raise ValueError("ma60_slope_days 应 ≥ 2")
        self.lookback_n = lookback_n
        self.vol_multiple = vol_multiple
        self.j_threshold = j_threshold
        self.j_q_threshold = j_q_threshold
        self.ma60_slope_days = ma60_slope_days
        self.max_window = max_window        

    @staticmethod
    def _ma_slope_positive(series: pd.Series, days: int) -> bool:
        """对最近 days 个点做一阶线性回归，斜率 > 0 判为正"""
        seg = series.dropna().tail(days)
        if len(seg) < days:
            return False
        x = np.arange(len(seg), dtype=float)
        # 线性回归（最小二乘）：斜率 k
        k, _ = np.polyfit(x, seg.values.astype(float), 1)
        return bool(k > 0)

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        """
        hist：按日期升序，最后一行是目标交易日
        需包含列：date, open, high, low, close, volume
        """
        if hist.empty:
            return False

        hist = hist.copy().sort_values("date")
        # 至少要有 60 日用于 MA60，再加 lookback/slope 的缓冲
        min_len = max(60 + self.lookback_n + self.ma60_slope_days, self.max_window + 5)
        if len(hist) < min_len:
            return False
        
        if not passes_day_constraints_today(hist):
            return False

        # --- 计算指标 ---
        kdj = compute_kdj(hist)
        j_today = float(kdj["J"].iloc[-1])
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_q_val = float(j_window.quantile(self.j_q_threshold))

        # 1) 当日 J 绝对低或相对低
        if not (j_today < self.j_threshold or j_today <= j_q_val):
            return False

        # 2) MA60 及有效上穿（使用通用函数）
        hist["MA60"] = hist["close"].rolling(window=60, min_periods=1).mean()
        if hist["close"].iloc[-1] < hist["MA60"].iloc[-1]:
            return False

        t_pos = last_valid_ma_cross_up(hist["close"], hist["MA60"], lookback_n=self.lookback_n)
        if t_pos is None:
            return False

        # === [T, today] 内以 High 最大值的交易日为 Tmax ===
        seg_T_to_today = hist.iloc[t_pos:]
        if seg_T_to_today.empty:
            return False

        # 若并列最高，默认取“第一次”出现的那天；要“最后一次”可改见注释
        tmax_label = seg_T_to_today["high"].idxmax()
        int_pos_T   = t_pos
        int_pos_Tmax = hist.index.get_loc(tmax_label)

        if int_pos_Tmax < int_pos_T:
            return False

        # 上涨波段 [T, Tmax]（含端点）
        wave = hist.iloc[int_pos_T : int_pos_Tmax + 1]
        wave_len = len(wave)
        if wave_len < 3:
            return False

        # 等长前置窗口 [T - wave_len, T-1]
        pre_start_pos = max(0, int_pos_T - min(wave_len, 10))
        pre = hist.iloc[pre_start_pos:int_pos_T]
        if len(pre) < max(5, min(10, wave_len)):
            return False

        # 成交量均值对比
        wave_avg_vol = float(wave["volume"].replace(0, np.nan).dropna().mean())
        pre_avg_vol  = float(pre["volume"].replace(0, np.nan).dropna().mean())
        if not (np.isfinite(wave_avg_vol) and np.isfinite(pre_avg_vol) and pre_avg_vol > 0):
            return False

        if wave_avg_vol < self.vol_multiple * pre_avg_vol:
            return False

        # 3) MA60 斜率 > 0（保留原实现）
        if not self._ma_slope_positive(hist["MA60"], self.ma60_slope_days):
            return False
        
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        # 给足 60 日均线与量能比较的历史长度
        need_len = max(60 + self.lookback_n + self.ma60_slope_days, self.max_window + 20)
        for code, df in data.items():
            hist = df[df["date"] <= date].tail(need_len)
            if len(hist) < need_len:
                continue
            if self._passes_filters(hist):
                picks.append(code)
        return picks

class BigBullishVolumeSelector:    

    def __init__(
        self,
        *,
        up_pct_threshold: float = 0.04,       # 长阳阈值：例如 0.04 表示涨幅>4%
        upper_wick_pct_max: float = 0.5,      # 上影线比例上限（口径由 wick_mode 决定）
        vol_lookback_n: int = 20,             # 放量比较的历史天数 n
        vol_multiple: float = 1.5,            # 放量倍数阈值
        min_history: int | None = None,       # 最少历史长度（默认自动 = vol_lookback_n + 2）
        require_bullish_close: bool = True,   # 可选：要求当日收阳（close >= open）
        ignore_zero_volume: bool = True,      # 计算均量时是否忽略 volume=0
        close_lt_zxdq_mult: float = 1.0       # 例如 1.0 表示 close < zxdq；1.02 表示 close < 1.02*zxdq        
    ) -> None:
        if up_pct_threshold <= 0:
            raise ValueError("up_pct_threshold 应 > 0")
        if upper_wick_pct_max < 0:
            raise ValueError("upper_wick_pct_max 应 >= 0")
        if vol_lookback_n < 1:
            raise ValueError("vol_lookback_n 应 >= 1")
        if vol_multiple <= 0:
            raise ValueError("vol_multiple 应 > 0")
        if close_lt_zxdq_mult <= 0:
            raise ValueError("close_lt_zxdq_mult 应 > 0")    

        self.up_pct_threshold = float(up_pct_threshold)
        self.upper_wick_pct_max = float(upper_wick_pct_max)
        self.vol_lookback_n = int(vol_lookback_n)
        self.vol_multiple = float(vol_multiple)
        self.require_bullish_close = bool(require_bullish_close)
        self.ignore_zero_volume = bool(ignore_zero_volume)
        self.close_lt_zxdq_mult = float(close_lt_zxdq_mult)
        self.eps = float(1e-12)        
        self.min_history = int(min_history) if min_history is not None else (self.vol_lookback_n + 2)
        

    @staticmethod
    def _to_float(x) -> float:
        try:
            return float(x)
        except Exception:
            return float("nan")

    def _upper_wick_pct(self, o: float, h: float, c: float) -> float:
        return (h - max(o, c)) / max(o, c)

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        if hist is None or hist.empty:
            return False

        hist = hist.sort_values("date").copy()

        if len(hist) < self.min_history:
            return False
        if len(hist) < (self.vol_lookback_n + 2):
            return False  # 至少需要：T、T-1、以及 T-1 往前 n 天

        today = hist.iloc[-1]
        prev  = hist.iloc[-2]

        oT = self._to_float(today.get("open"))
        hT = self._to_float(today.get("high"))
        lT = self._to_float(today.get("low"))
        cT = self._to_float(today.get("close"))
        vT = self._to_float(today.get("volume"))

        cP = self._to_float(prev.get("close"))

        # 基础合法性
        if not (np.isfinite(oT) and np.isfinite(hT) and np.isfinite(lT) and np.isfinite(cT) and np.isfinite(vT) and np.isfinite(cP)):
            return False
        if cP <= 0 or cT <= 0:
            return False
        if hT < max(oT, cT) or lT > min(oT, cT):
            # K线数据异常（不一定必需，但建议保持严谨）
            return False

        # (可选) 要求当日收阳
        if self.require_bullish_close and not (cT >= oT):
            return False

        # 1) 长阳：涨幅 > 阈值
        pct_chg = cT / cP - 1.0
        if pct_chg <= self.up_pct_threshold:
            return False

        # 2) 上影线百分比 < 阈值
        wick_pct = self._upper_wick_pct(oT, hT, cT)
        if not np.isfinite(wick_pct):
            return False
        if wick_pct >= self.upper_wick_pct_max:
            return False

        # 3) 放量：当日成交量 > 前 n 日均量 * 倍数
        vol_hist = hist["volume"].iloc[-(self.vol_lookback_n + 1):-1].astype(float)  # T-n ... T-1
        if self.ignore_zero_volume:
            vol_hist = vol_hist.replace(0, np.nan).dropna()

        if len(vol_hist) < max(3, int(self.vol_lookback_n * 0.6)):
            # 有效样本过少就不做判断（你也可以改成直接 False 或严格要求=vol_lookback_n）
            return False

        avg_vol = float(vol_hist.mean())
        if not (np.isfinite(avg_vol) and avg_vol > 0):
            return False

        if vT < self.vol_multiple * avg_vol:
            return False
        
        # 4) 偏离短线小于阈值
        try:
            zxdq, _ = compute_zx_lines(hist)
            zxdq_T = float(zxdq.iloc[-1])
        except Exception:
            zxdq_T = float("nan")

        if not np.isfinite(zxdq_T):
            return False
        else:
            if not (cT < zxdq_T * self.close_lt_zxdq_mult):
                return False

        return True

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        need_len = max(self.min_history, self.vol_lookback_n + 2)

        for code, df in data.items():
            if df is None or df.empty:
                continue
            hist = df[df["date"] <= date].tail(need_len)
            if len(hist) < need_len:
                continue
            if self._passes_filters(hist):
                picks.append(code)

        return picks


class BBDMomentumSignalSelector:
    """
    基于通达信指标的买点选股器。

    当日出现以下任一启用信号即入选：
    1) BBD 金叉
    2) 动能金叉
    3) B 底背（BBD 底背离）
    4) 动底背（动能线底背离）
    """

    def __init__(
        self,
        *,
        max_window: int = 240,
        use_bbd_golden_cross: bool = True,
        use_momentum_golden_cross: bool = True,
        use_bbd_bottom_divergence: bool = True,
        use_momentum_bottom_divergence: bool = True,
    ) -> None:
        if max_window < 30:
            raise ValueError("max_window 应 >= 30")

        self.max_window = max_window
        self.use_bbd_golden_cross = bool(use_bbd_golden_cross)
        self.use_momentum_golden_cross = bool(use_momentum_golden_cross)
        self.use_bbd_bottom_divergence = bool(use_bbd_bottom_divergence)
        self.use_momentum_bottom_divergence = bool(use_momentum_bottom_divergence)

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        required_cols = {"open", "high", "low", "close"}
        if hist.empty or not required_cols.issubset(hist.columns):
            return False

        hist = hist.sort_values("date").copy()
        if len(hist) < 30:
            return False

        signal_values = compute_bbd_signals(hist)

        buy_sig = pd.Series(False, index=hist.index)
        if self.use_bbd_golden_cross:
            buy_sig = buy_sig | signal_values["bbd_buy"]
        if self.use_momentum_golden_cross:
            buy_sig = buy_sig | signal_values["momentum_buy"]
        if self.use_bbd_bottom_divergence:
            buy_sig = buy_sig | signal_values["bbd_bottom_divergence"]
        if self.use_momentum_bottom_divergence:
            buy_sig = buy_sig | signal_values["momentum_bottom_divergence"]

        return bool(buy_sig.iloc[-1])

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        need_len = max(30, self.max_window)

        for code, df in data.items():
            if df is None or df.empty:
                continue
            hist = df[df["date"] <= date].tail(need_len)
            if len(hist) < 30:
                continue
            if self._passes_filters(hist):
                picks.append(code)

        return picks


class BBDMomentumKDJSelector:
    """
    BBD 动能信号 + 当日 KDJ J 值过滤 选股器。

    逻辑：在 BBDMomentumSignalSelector 的所有买点条件基础上，
    再要求当日 KDJ 的 J 值必须小于配置阈值。
    """

    def __init__(
        self,
        *,
        max_window: int = 240,
        kdj_j_threshold: float = 20.0,
        use_bbd_golden_cross: bool = True,
        use_momentum_golden_cross: bool = True,
        use_bbd_bottom_divergence: bool = True,
        use_momentum_bottom_divergence: bool = True,
    ) -> None:
        if max_window < 30:
            raise ValueError("max_window 应 >= 30")
        if kdj_j_threshold is None:
            raise ValueError("kdj_j_threshold 不能为空")

        self.max_window = max_window
        self.kdj_j_threshold = float(kdj_j_threshold)
        self.use_bbd_golden_cross = bool(use_bbd_golden_cross)
        self.use_momentum_golden_cross = bool(use_momentum_golden_cross)
        self.use_bbd_bottom_divergence = bool(use_bbd_bottom_divergence)
        self.use_momentum_bottom_divergence = bool(use_momentum_bottom_divergence)

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        required_cols = {"open", "high", "low", "close"}
        if hist.empty or not required_cols.issubset(hist.columns):
            return False

        hist = hist.sort_values("date").copy()
        if len(hist) < 30:
            return False

        signal_values = compute_bbd_signals(hist)

        buy_sig = pd.Series(False, index=hist.index)
        if self.use_bbd_golden_cross:
            buy_sig = buy_sig | signal_values["bbd_buy"]
        if self.use_momentum_golden_cross:
            buy_sig = buy_sig | signal_values["momentum_buy"]
        if self.use_bbd_bottom_divergence:
            buy_sig = buy_sig | signal_values["bbd_bottom_divergence"]
        if self.use_momentum_bottom_divergence:
            buy_sig = buy_sig | signal_values["momentum_bottom_divergence"]

        if not bool(buy_sig.iloc[-1]):
            return False

        kdj = compute_kdj(hist)
        j_today = float(kdj["J"].iloc[-1])
        if not np.isfinite(j_today):
            return False
        if j_today >= self.kdj_j_threshold:
            return False
        return True

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        need_len = max(30, self.max_window)

        for code, df in data.items():
            if df is None or df.empty:
                continue
            hist = df[df["date"] <= date].tail(need_len)
            if len(hist) < 30:
                continue
            if self._passes_filters(hist):
                j_today = float(compute_kdj(hist)["J"].iloc[-1])
                picks.append(code)

        return picks


class MultiCycleRSVSelector:
    """
    指标 2 的多周期 RSV 超卖选股器。

    默认在“黄金坑”区间内持续入选：短、中、长三条平滑 RSV
    同时小于 oversold_threshold。可选仅在首次进入黄金坑时入选。
    """

    def __init__(
        self,
        *,
        max_window: int = 120,
        short_window: int = 8,
        middle_window: int = 21,
        long_window: int = 55,
        oversold_threshold: float = 15.0,
        bottom_threshold: float = 20.0,
        use_golden_pit: bool = True,
        use_bottom_entry: bool = False,
        golden_pit_entry_only: bool = False,
    ) -> None:
        if max_window < max(short_window, middle_window, long_window):
            raise ValueError("max_window 必须不小于最长 RSV 窗口")
        if min(short_window, middle_window, long_window) < 1:
            raise ValueError("RSV 窗口必须 >= 1")
        if not use_golden_pit and not use_bottom_entry:
            raise ValueError("至少启用一种 RSV 买点信号")

        self.max_window = int(max_window)
        self.short_window = int(short_window)
        self.middle_window = int(middle_window)
        self.long_window = int(long_window)
        self.oversold_threshold = float(oversold_threshold)
        self.bottom_threshold = float(bottom_threshold)
        self.use_golden_pit = bool(use_golden_pit)
        self.use_bottom_entry = bool(use_bottom_entry)
        self.golden_pit_entry_only = bool(golden_pit_entry_only)

    @staticmethod
    def _rsv(close: pd.Series, high: pd.Series, low: pd.Series, window: int) -> pd.Series:
        low_n = low.rolling(window=window, min_periods=window).min()
        high_n = high.rolling(window=window, min_periods=window).max()
        return (close - low_n) / (high_n - low_n + 1e-9) * 100.0

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        required_cols = {"high", "low", "close"}
        min_history = max(self.short_window, self.middle_window, self.long_window)
        if hist.empty or not required_cols.issubset(hist.columns) or len(hist) < min_history:
            return False

        hist = hist.sort_values("date").copy()
        close = hist["close"].astype(float)
        high = hist["high"].astype(float)
        low = hist["low"].astype(float)

        short_line = compute_tdx_sma(compute_tdx_sma(self._rsv(close, high, low, self.short_window), 3, 1), 3, 1)
        middle_line = compute_tdx_sma(self._rsv(close, high, low, self.middle_window), 5, 1)
        long_line = compute_tdx_sma(self._rsv(close, high, low, self.long_window), 5, 1)

        golden_pit = (
            (short_line < self.oversold_threshold)
            & (middle_line < self.oversold_threshold)
            & (long_line < self.oversold_threshold)
        )
        bottom_zone = middle_line < self.bottom_threshold

        golden_pit_entry = golden_pit & ~golden_pit.shift(1, fill_value=False)
        bottom_entry = bottom_zone & ~bottom_zone.shift(1, fill_value=False)

        buy_sig = pd.Series(False, index=hist.index)
        if self.use_golden_pit:
            buy_sig = buy_sig | (golden_pit_entry if self.golden_pit_entry_only else golden_pit)
        if self.use_bottom_entry:
            buy_sig = buy_sig | bottom_entry
        return bool(buy_sig.iloc[-1])

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        need_len = max(self.max_window, self.long_window)
        for code, df in data.items():
            if df is None or df.empty:
                continue
            hist = df[df["date"] <= date].tail(need_len)
            if self._passes_filters(hist):
                picks.append(code)
        return picks


class MarketStructureBuySelector:
    """
    指标 1 的实时峰谷结构买入选股器。

    峰谷在其后 pivot_confirm_bars 根 K 线走完后才确认，因此不使用
    BACKSET 回填历史，信号日期可直接用于当日收盘筛选与次日交易。
    """

    def __init__(
        self,
        *,
        max_window: int = 240,
        pivot_window: int = 24,
        pivot_confirm_bars: int = 5,
        retest_tolerance: float = 0.0,
        use_probe_signal: bool = True,
        use_strong_bottom_signal: bool = True,
    ) -> None:
        if pivot_window <= pivot_confirm_bars:
            raise ValueError("pivot_window 必须大于 pivot_confirm_bars")
        if max_window < pivot_window:
            raise ValueError("max_window 必须不小于 pivot_window")
        if retest_tolerance < 0:
            raise ValueError("retest_tolerance 必须 >= 0")
        if not use_probe_signal and not use_strong_bottom_signal:
            raise ValueError("至少启用一种结构买点信号")

        self.max_window = int(max_window)
        self.pivot_window = int(pivot_window)
        self.pivot_confirm_bars = int(pivot_confirm_bars)
        self.retest_tolerance = float(retest_tolerance)
        self.use_probe_signal = bool(use_probe_signal)
        self.use_strong_bottom_signal = bool(use_strong_bottom_signal)

    def _structure_count(self, hist: pd.DataFrame) -> int:
        """返回原公式四项买入条件中当前已满足的数量。"""
        if len(hist) < self.pivot_window:
            return 0

        high = hist["high"].astype(float).to_numpy()
        low = hist["low"].astype(float).to_numpy()
        pivots: List[tuple[str, int, float]] = []

        # 第 i 根 K 线仅确认 i-confirm 根的峰谷；窗口与原 A1/A2 一致。
        for confirmed_at in range(self.pivot_window - 1, len(hist)):
            pivot_at = confirmed_at - self.pivot_confirm_bars
            start = confirmed_at - self.pivot_window + 1
            candidate_high = high[pivot_at] >= np.max(high[start : confirmed_at + 1])
            candidate_low = low[pivot_at] <= np.min(low[start : confirmed_at + 1])

            candidates: List[tuple[str, float]] = []
            if candidate_high:
                candidates.append(("high", float(high[pivot_at])))
            if candidate_low:
                candidates.append(("low", float(low[pivot_at])))
            for kind, price in candidates:
                if not pivots:
                    pivots.append((kind, pivot_at, price))
                elif pivots[-1][0] == kind:
                    previous_price = pivots[-1][2]
                    is_more_extreme = (kind == "high" and price >= previous_price) or (
                        kind == "low" and price <= previous_price
                    )
                    if is_more_extreme:
                        pivots[-1] = (kind, pivot_at, price)
                else:
                    pivots.append((kind, pivot_at, price))

        if len(pivots) < 3:
            return 0

        previous_high, swing_low, secondary_high = pivots[-3:]
        if (previous_high[0], swing_low[0], secondary_high[0]) != ("high", "low", "high"):
            return 0

        # 前高 -> 低点 -> 次高的时间顺序是原 VVM11/VVM21 的两项基础条件。
        count = 2
        if previous_high[2] > secondary_high[2]:
            count += 1
        if low[-1] <= swing_low[2] * (1.0 + self.retest_tolerance):
            count += 1
        return count

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        required_cols = {"high", "low"}
        if hist.empty or not required_cols.issubset(hist.columns) or len(hist) < self.pivot_window:
            return False

        hist = hist.sort_values("date").copy()
        current_count = self._structure_count(hist)
        previous_count = self._structure_count(hist.iloc[:-1])

        probe_signal = current_count == 2 and previous_count != 2
        strong_bottom_signal = current_count >= 3 and previous_count < 3
        return bool(
            (self.use_probe_signal and probe_signal)
            or (self.use_strong_bottom_signal and strong_bottom_signal)
        )

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            if df is None or df.empty:
                continue
            hist = df[df["date"] <= date].tail(self.max_window)
            if self._passes_filters(hist):
                picks.append(code)
        return picks


class MACDDivergenceBottomSelector:
    """
    指标 4 的 MACD 底背离确认选股器。

    在 MACD 负轴的相邻或隔一段空头周期中，价格创新低而 DIF 未创新低时，
    先标记底背离；次日 DIF 的负值绝对值至少收缩 min_diff_contraction
    才发出一次“抄底”信号，对齐原公式的 DXDX。
    """

    def __init__(
        self,
        *,
        max_window: int = 240,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
        min_diff_contraction: float = 0.01,
        use_regular_divergence: bool = True,
        use_three_segment_divergence: bool = True,
    ) -> None:
        if max_window < slow + signal:
            raise ValueError("max_window 必须足以覆盖 MACD 预热期")
        if fast < 1 or slow <= fast or signal < 1:
            raise ValueError("MACD 参数必须满足 1 <= fast < slow，signal >= 1")
        if min_diff_contraction < 0:
            raise ValueError("min_diff_contraction 必须 >= 0")
        if not use_regular_divergence and not use_three_segment_divergence:
            raise ValueError("至少启用一种底背离形态")

        self.max_window = int(max_window)
        self.fast = int(fast)
        self.slow = int(slow)
        self.signal = int(signal)
        self.min_diff_contraction = float(min_diff_contraction)
        self.use_regular_divergence = bool(use_regular_divergence)
        self.use_three_segment_divergence = bool(use_three_segment_divergence)

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        if hist.empty or "close" not in hist.columns or len(hist) < self.slow + self.signal:
            return False

        close = hist.sort_values("date")["close"].astype(float).to_numpy()
        diffs = pd.Series(close).ewm(span=self.fast, adjust=False).mean() - pd.Series(close).ewm(span=self.slow, adjust=False).mean()
        dea = diffs.ewm(span=self.signal, adjust=False).mean()
        macd = (diffs - dea) * 2.0

        negative_segments: List[tuple[float, float]] = []
        bottom_divergence = np.zeros(len(close), dtype=bool)
        start: Optional[int] = None
        for pos, is_negative in enumerate((macd < 0.0).to_numpy()):
            if is_negative and start is None:
                start = pos
            if start is None or (is_negative and pos != len(close) - 1):
                continue

            end = pos if is_negative else pos - 1
            segment_close_low = float(np.min(close[start : end + 1]))
            segment_diff_low = float(diffs.iloc[start : end + 1].min())

            regular_divergence = (
                len(negative_segments) >= 1
                and segment_close_low < negative_segments[-1][0]
                and segment_diff_low > negative_segments[-1][1]
            )
            three_segment_divergence = (
                len(negative_segments) >= 2
                and segment_close_low < negative_segments[-2][0]
                and segment_diff_low < negative_segments[-1][1]
                and segment_diff_low > negative_segments[-2][1]
            )
            if (self.use_regular_divergence and regular_divergence) or (
                self.use_three_segment_divergence and three_segment_divergence
            ):
                bottom_divergence[end] = bool(diffs.iloc[end] < 0.0)

            negative_segments.append((segment_close_low, segment_diff_low))
            start = None

        if len(close) < 2:
            return False
        previous_divergence = bottom_divergence[:-1]
        diff_contraction = abs(float(diffs.iloc[-2])) >= abs(float(diffs.iloc[-1])) * (1.0 + self.min_diff_contraction)
        buy_sig = bool(previous_divergence[-1] and diff_contraction)

        # DXDX 仅在 JJJ 从未成立变为成立的首日显示。
        if not buy_sig:
            return False
        if len(close) < 3 or not bottom_divergence[-3]:
            return True
        prior_contraction = abs(float(diffs.iloc[-3])) >= abs(float(diffs.iloc[-2])) * (1.0 + self.min_diff_contraction)
        return not prior_contraction

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            if df is None or df.empty:
                continue
            hist = df[df["date"] <= date].tail(self.max_window)
            if self._passes_filters(hist):
                picks.append(code)
        return picks


class BurstSignalSelector:
    """
    起爆/启爆信号选股器。

    起爆（CDSY）条件：
    - (C-MA(C,34))/MA(C,34)*100 < -14
    - (H-L)/L > 0.07 且 (C-O)/O > 0.07 且 C/REF(C,1) > 1.05

    启爆条件：满足以下任一
    - XL2: LLV(L,3)=LLV(L,60) 且 C/REF(C,1) >= 1.04
    - XL4: CROSS((C-EMA(C,21))/EMA(C,21)*100, -20)
    - XL5: LLV(L,3)=LLV(L,120) 且 C/REF(C,1) >= 1.06
    - XL7: CROSS((C-MA(C,24))/MA(C,24)*100, -20)

    默认当日“起爆或启爆”任一成立即入选；可通过 require_both_signals
    配置为“起爆且启爆”同时成立。
    """

    def __init__(
        self,
        *,
        max_window: int = 240,
        require_both_signals: bool = False,
        apply_xl2_filter: bool = True,
        xl2_filter_n: int = 5,
        llv_equal_tolerance: float = 1e-9,
    ) -> None:
        if max_window < 121:
            raise ValueError("max_window 应 >= 121")
        if xl2_filter_n < 1:
            raise ValueError("xl2_filter_n 应 >= 1")
        if llv_equal_tolerance < 0:
            raise ValueError("llv_equal_tolerance 应 >= 0")

        self.max_window = max_window
        self.require_both_signals = bool(require_both_signals)
        self.apply_xl2_filter = bool(apply_xl2_filter)
        self.xl2_filter_n = int(xl2_filter_n)
        self.llv_equal_tolerance = float(llv_equal_tolerance)

    @staticmethod
    def _tdx_filter(sig: pd.Series, n: int) -> pd.Series:
        """近似通达信 FILTER(X,N)：触发后 N 根内抑制重复信号。"""
        arr = sig.fillna(False).astype(bool).to_numpy()
        out = np.zeros(len(arr), dtype=bool)
        cooldown = 0
        for i, flag in enumerate(arr):
            if cooldown > 0:
                cooldown -= 1
                continue
            if flag:
                out[i] = True
                cooldown = n
        return pd.Series(out, index=sig.index)

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        required_cols = {"open", "high", "low", "close"}
        if hist.empty or not required_cols.issubset(hist.columns):
            return False

        hist = hist.sort_values("date").copy()
        if len(hist) < 121:
            return False

        open_ = hist["open"].astype(float)
        high = hist["high"].astype(float)
        low = hist["low"].astype(float)
        close = hist["close"].astype(float)
        prev_close = close.shift(1)
        eps = 1e-9

        ma34 = close.rolling(window=34, min_periods=34).mean()
        ttt1 = (close - ma34) / (ma34 + eps) * 100.0 < -14.0
        ttt2 = (
            ((high - low) / (low + eps) > 0.07)
            & ((close - open_) / (open_ + eps) > 0.07)
            & (close / (prev_close + eps) > 1.05)
        )
        qibao_sig = ttt1 & ttt2

        llv3 = low.rolling(window=3, min_periods=3).min()
        llv60 = low.rolling(window=60, min_periods=60).min()
        llv120 = low.rolling(window=120, min_periods=120).min()

        xl2_raw = (
            np.isclose(llv3.to_numpy(), llv60.to_numpy(), rtol=0.0, atol=self.llv_equal_tolerance)
            & (close / (prev_close + eps) >= 1.04).to_numpy()
        )
        xl2_raw = pd.Series(xl2_raw, index=hist.index)

        ema21 = close.ewm(span=21, adjust=False).mean()
        xl3 = (close - ema21) / (ema21 + eps) * 100.0
        xl4 = cross_up(xl3, pd.Series(-20.0, index=hist.index))

        xl5 = (
            np.isclose(llv3.to_numpy(), llv120.to_numpy(), rtol=0.0, atol=self.llv_equal_tolerance)
            & (close / (prev_close + eps) >= 1.06).to_numpy()
        )
        xl5 = pd.Series(xl5, index=hist.index)

        ma24 = close.rolling(window=24, min_periods=24).mean()
        xl6 = (close - ma24) / (ma24 + eps) * 100.0
        xl7 = cross_up(xl6, pd.Series(-20.0, index=hist.index))

        xl2_sig = self._tdx_filter(xl2_raw, self.xl2_filter_n) if self.apply_xl2_filter else xl2_raw
        qibao_enable_sig = xl2_sig | xl4 | xl5 | xl7

        if self.require_both_signals:
            return bool(qibao_sig.iloc[-1] and qibao_enable_sig.iloc[-1])
        return bool(qibao_sig.iloc[-1] or qibao_enable_sig.iloc[-1])

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        need_len = max(121, self.max_window)

        for code, df in data.items():
            if df is None or df.empty:
                continue
            hist = df[df["date"] <= date].tail(need_len)
            if len(hist) < 121:
                continue
            if self._passes_filters(hist):
                picks.append(code)

        return picks


class WaveStructureSelector:
    """选择截至最新日线仍未破坏的连续上升波浪结构。

    已完成的上涨-回调波必须满足完整结构条件；最后一波可以尚未结束，
    只要最新价格没有跌破当前起点、回撤上限或当前浪的时间边界，就保留为第 N 浪。
    ``score_threshold`` 是模型匹配度评分线，不代表收益概率。
    """

    def __init__(
        self,
        score_threshold: float = 1.0,
        min_completed_waves: int = 1,
        data_days: int = 70,
        output_dir: str = "./wave_selection_data",
        min_pullback: float = 0.04,
        min_j_pullback_drop: float = 30.0,
        max_pullback: float = 0.28,
        min_up_return: float = 0.06,
        min_step: float = 0.02,
        lower_low_tolerance: float = 0.05,
        active_low_break_tolerance: float = 0.05,
        position_j_low_tolerance: float = 30.0,
        position_j_decline_scale: float = 40.0,
        position_recent_j_days: int = 3,
        top_plateau_tolerance: float = 0.01,
        top_plateau_after_wave: int = 3,
        min_path_efficiency: float = 0.58,
        up_path_efficiency_weight: float = 1.5,
        up_direction_consistency_weight: float = 1.5,
        pullback_path_efficiency_weight: float = 0.6,
        timing_min_top_to_reference_bars: int = 2,
        timing_min_reference_period: int = 7,
        timing_max_reference_period: int = 25,
        timing_reference_j_limit: float = 5.0,
        timing_reference_support_tolerance: float = 0.0,
        timing_low_j_threshold: float = 10.0,
        timing_j_rebound_scale: float = 30.0,
        timing_doji_body_ratio: float = 0.20,
        timing_doji_bonus: float = 3.0,
        max_open_wave_period: int = 25,
        start_price_lookback: int = 120,
        start_price_high_drawdown_scale: float = 0.40,
        weekly_j_lookback_weeks: int = 26,
        weekly_j_high_threshold: float = 60.0,
        weekly_j_low_threshold: float = 10.0,
        score_anchor_raw: float = 5.114,
        score_reference_points: float = 10.0,
        bullish_gap_weight: float = 1.0,
        body_gap_reference_j_limit: float = 10.0,
        full_gap_reference_j_limit: float = 20.0,
    ) -> None:
        self.score_threshold = float(score_threshold)
        self.min_completed_waves = max(1, int(min_completed_waves))
        self.data_days = max(1, int(data_days))
        self.output_dir = output_dir
        self.result_details: Dict[str, Dict[str, Any]] = {}
        self.wave_config = {
            "min_pullback": float(min_pullback),
            "min_j_pullback_drop": float(min_j_pullback_drop),
            "max_pullback": float(max_pullback),
            "min_up_return": float(min_up_return),
            "min_step": float(min_step),
            "lower_low_tolerance": float(lower_low_tolerance),
            "active_low_break_tolerance": float(active_low_break_tolerance),
            "position_j_low_tolerance": float(position_j_low_tolerance),
            "position_j_decline_scale": float(position_j_decline_scale),
            "position_recent_j_days": max(1, int(position_recent_j_days)),
            "top_plateau_tolerance": float(top_plateau_tolerance),
            "top_plateau_after_wave": max(2, int(top_plateau_after_wave)),
            "min_path_efficiency": float(min_path_efficiency),
            "up_path_efficiency_weight": float(up_path_efficiency_weight),
            "up_direction_consistency_weight": float(up_direction_consistency_weight),
            "pullback_path_efficiency_weight": float(pullback_path_efficiency_weight),
            "timing_min_top_to_reference_bars": max(0, int(timing_min_top_to_reference_bars)),
            "timing_min_reference_period": max(1, int(timing_min_reference_period)),
            "timing_max_reference_period": max(1, int(timing_max_reference_period)),
            "timing_reference_j_limit": float(timing_reference_j_limit),
            "timing_reference_support_tolerance": float(timing_reference_support_tolerance),
            "timing_low_j_threshold": float(timing_low_j_threshold),
            "timing_j_rebound_scale": float(timing_j_rebound_scale),
            "timing_doji_body_ratio": float(timing_doji_body_ratio),
            "timing_doji_bonus": float(timing_doji_bonus),
            "max_open_wave_period": int(max_open_wave_period),
            "start_price_lookback": max(1, int(start_price_lookback)),
            "start_price_high_drawdown_scale": float(start_price_high_drawdown_scale),
            "weekly_j_lookback_weeks": max(1, int(weekly_j_lookback_weeks)),
            "weekly_j_high_threshold": float(weekly_j_high_threshold),
            "weekly_j_low_threshold": float(weekly_j_low_threshold),
            "score_anchor_raw": float(score_anchor_raw),
            "score_reference_points": float(score_reference_points),
            "bullish_gap_weight": max(0.0, float(bullish_gap_weight)),
            "body_gap_reference_j_limit": float(body_gap_reference_j_limit),
            "full_gap_reference_j_limit": float(full_gap_reference_j_limit),
        }

    def select(
        self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]
    ) -> List[str]:
        picks: List[str] = []
        self.result_details = {}
        for code, df in data.items():
            hist = (
                df[df["date"] <= date]
                .sort_values("date")
                .drop_duplicates("date")
                .reset_index(drop=True)
            )
            hist["code"] = code
            candidate = find_active_wave(
                hist,
                cfg=self.wave_config,
                min_completed_waves=self.min_completed_waves,
            )
            if candidate is None or candidate.score < self.score_threshold:
                continue
            detail = active_wave_to_dict(candidate, hist, self.score_threshold)
            detail["data_days"] = self.data_days
            detail["output_dir"] = self.output_dir
            self.result_details[code] = detail
            picks.append(code)

        return sorted(
            picks,
            key=lambda code: (
                -self.result_details[code]["timing_score"],
                -self.result_details[code]["structure_quality"],
                code,
            ),
        )


class OneWaveEntrySelector:
    """选择仍处于第一上涨波回调阶段、且尚未进入二波结果的股票。"""

    def __init__(
        self,
        score_threshold: float = 1.0,
        data_days: int = 70,
        output_dir: str = "./one_wave_entry_data",
        min_wave_bars: int = 2,
        max_wave_bars: int = 20,
        min_up_return: float = 0.06,
        min_path_efficiency: float = 0.58,
        path_efficiency_weight: float = 2.0,
        max_up_adverse: float = 0.12,
        max_one_wave_period: int = 30,
        start_price_lookback: int = 120,
        start_price_high_drawdown_scale: float = 0.40,
        start_price_drawdown_weight: float = 1.5,
        min_reference_period: int = 7,
        max_reference_period: int = 25,
        min_top_to_reference_bars: int = 2,
        max_reference_j: float = 5.0,
        max_reference_rebound: float = 0.15,
        j_rebound_exit_threshold: float = 80.0,
        timing_j_rebound_scale: float = 30.0,
        timing_low_j_threshold: float = 10.0,
        timing_doji_body_ratio: float = 0.20,
        timing_doji_bonus: float = 3.0,
        pullback_path_efficiency_low: float = 0.35,
        pullback_path_efficiency_high: float = 0.85,
        pullback_path_efficiency_penalty: float = 5.0,
        max_pullback: float = 0.28,
        max_observation_after_reference: int = 25,
        support_close_tolerance: float = 0.0,
        exclude_existing_two_wave: bool = True,
        existing_two_wave_score_threshold: float = 1.0,
        bullish_gap_weight: float = 1.0,
        body_gap_reference_j_limit: float = 10.0,
        full_gap_reference_j_limit: float = 20.0,
        impulse_pullback_enabled: bool = False,
        impulse_quality_weight: float = 1.0,
        impulse_reference_j_limit: float = 70.0,
    ) -> None:
        self.score_threshold = float(score_threshold)
        self.data_days = max(1, int(data_days))
        self.output_dir = output_dir
        self.wave_config = {
            "min_wave_bars": max(1, int(min_wave_bars)),
            "selection_branch": "low_j_pullback",
            "max_wave_bars": max(1, int(max_wave_bars)),
            "min_up_return": float(min_up_return),
            "min_path_efficiency": float(min_path_efficiency),
            "path_efficiency_weight": float(path_efficiency_weight),
            "max_up_adverse": float(max_up_adverse),
            "max_one_wave_period": max(1, int(max_one_wave_period)),
            "start_price_lookback": max(1, int(start_price_lookback)),
            "start_price_high_drawdown_scale": float(start_price_high_drawdown_scale),
            "start_price_drawdown_weight": float(start_price_drawdown_weight),
            "min_reference_period": max(1, int(min_reference_period)),
            "max_reference_period": max(1, int(max_reference_period)),
            "min_top_to_reference_bars": max(1, int(min_top_to_reference_bars)),
            "max_reference_j": float(max_reference_j),
            "max_reference_rebound": float(max_reference_rebound),
            "j_rebound_exit_threshold": float(j_rebound_exit_threshold),
            "timing_j_rebound_scale": float(timing_j_rebound_scale),
            "timing_low_j_threshold": float(timing_low_j_threshold),
            "timing_doji_body_ratio": float(timing_doji_body_ratio),
            "timing_doji_bonus": float(timing_doji_bonus),
            "pullback_path_efficiency_low": float(pullback_path_efficiency_low),
            "pullback_path_efficiency_high": float(pullback_path_efficiency_high),
            "pullback_path_efficiency_penalty": float(pullback_path_efficiency_penalty),
            "max_pullback": float(max_pullback),
            "max_observation_after_reference": max(1, int(max_observation_after_reference)),
            "support_close_tolerance": max(0.0, float(support_close_tolerance)),
            "impulse_pullback_enabled": bool(impulse_pullback_enabled),
            "impulse_quality_weight": max(0.0, float(impulse_quality_weight)),
            "impulse_reference_j_limit": float(impulse_reference_j_limit),
            "bullish_gap_weight": max(0.0, float(bullish_gap_weight)),
            "body_gap_reference_j_limit": float(body_gap_reference_j_limit),
            "full_gap_reference_j_limit": float(full_gap_reference_j_limit),
        }
        self.exclude_existing_two_wave = bool(exclude_existing_two_wave)
        self.existing_two_wave_score_threshold = float(existing_two_wave_score_threshold)
        self.result_details: Dict[str, Dict[str, Any]] = {}
        self.preselection_details: Dict[str, List[Dict[str, Any]]] = {}

    @staticmethod
    def _is_existing_two_wave_result(hist: pd.DataFrame, score_threshold: float) -> bool:
        candidate = find_active_wave(hist, cfg=WaveStructureSelector().wave_config, min_completed_waves=1)
        return candidate is not None and candidate.score >= score_threshold

    def select(
        self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]
    ) -> List[str]:
        picks: List[str] = []
        self.result_details = {}
        self.preselection_details = {}
        for code, df in data.items():
            if df is None or df.empty:
                continue
            hist = (
                df[df["date"] <= date]
                .sort_values("date")
                .drop_duplicates("date")
                .reset_index(drop=True)
            )
            if len(hist) < 25:
                continue
            hist["code"] = code
            preselection_candidates = find_one_wave_preselections(
                hist, cfg=self.wave_config
            )
            candidate = find_active_one_wave(hist, cfg=self.wave_config)
            active_key = (
                candidate.start1.start,
                candidate.top1.start,
                candidate.j2_reference_index,
                candidate.selection_branch,
            ) if candidate is not None else None
            preselection_rows: List[Dict[str, Any]] = []
            for preselection in preselection_candidates:
                preselection_key = (
                    preselection.start1.start,
                    preselection.top1.start,
                    preselection.j2_reference_index,
                    preselection.selection_branch,
                )
                detail = one_wave_to_dict(
                    preselection, hist, self.score_threshold
                )
                detail["preselection_stage"] = "1起预选"
                if active_key == preselection_key and (
                    candidate is not None and candidate.score >= self.score_threshold
                ):
                    detail["preselection_status"] = "当前一波结果"
                elif candidate is not None and candidate.score < self.score_threshold:
                    detail["preselection_status"] = "评分未达到当前阈值"
                else:
                    detail["preselection_status"] = "已退出当前一波"
                preselection_rows.append(detail)
            if preselection_rows:
                self.preselection_details[code] = preselection_rows
            if candidate is None or candidate.score < self.score_threshold:
                continue
            if self.exclude_existing_two_wave and self._is_existing_two_wave_result(
                hist, self.existing_two_wave_score_threshold
            ):
                continue
            detail = one_wave_to_dict(candidate, hist, self.score_threshold)
            detail["data_days"] = self.data_days
            detail["output_dir"] = self.output_dir
            self.result_details[code] = detail
            picks.append(code)

        return sorted(
            picks,
            key=lambda code: (
                -self.result_details[code]["timing_score"],
                -self.result_details[code]["score"],
                code,
            ),
        )


class MoZhuaSelector(WaveStructureSelector):
    """魔抓策略：前段缺口放宽下一起点，观察二顶后的三起参考。"""

    def __init__(
        self,
        output_dir: str = "./mozhua_selection_data",
        impulse_quality_weight: float = 1.0,
        mozhua_body_j_limit: float = 40.0,
        mozhua_full_j_limit: float = 50.0,
        mozhua_strong_j_limit: float = 70.0,
        mozhua_min_j_drop: float = 30.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(output_dir=output_dir, **kwargs)
        self.wave_config.update({
            "impulse_quality_weight": max(0.0, float(impulse_quality_weight)),
            "mozhua_body_j_limit": float(mozhua_body_j_limit),
            "mozhua_full_j_limit": float(mozhua_full_j_limit),
            "mozhua_strong_j_limit": float(mozhua_strong_j_limit),
            "mozhua_min_j_drop": float(mozhua_min_j_drop),
            "score_threshold": self.score_threshold,
        })

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        from strategies.mozhua_structure import find_mozhua

        self.result_details = {}
        for code, frame in data.items():
            if frame is None or frame.empty:
                continue
            history = frame.loc[frame["date"] <= date].copy()
            history["code"] = code
            detail = find_mozhua(history, self.wave_config)
            if detail is None:
                continue
            detail["data_days"] = self.data_days
            detail["output_dir"] = self.output_dir
            self.result_details[code] = detail
        return sorted(self.result_details, key=lambda code: (
            -self.result_details[code]["timing_score"],
            -self.result_details[code]["score"], code,
        ))

