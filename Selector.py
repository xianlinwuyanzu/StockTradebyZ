from typing import Dict, List

import numpy as np
import pandas as pd


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


def bbi_deriv_uptrend(
    bbi: pd.Series,
    offset_n: int,
    min_window: int,
    max_window: int | None = None,
) -> bool:
    """
    以最新日 T，向前 offset_n 得到锚点 **T-n**；
    在 [T-n-w+1, T-n] 区间上（w 自适应，w ≥ min_window 且 ≤ max_window）：
        归一化 BBI(t) = BBI(t) / BBI(T-n-w+1)
    要求 *每一日* 的一阶差分 ≥ 0（单调不降）。
    选最长满足条件的窗口，存在即通过。
    """
    bbi = bbi.dropna()
    if len(bbi) <= offset_n + min_window:
        return False

    anchor_idx = len(bbi) - offset_n - 1                 # T-n 的位置
    longest = min(anchor_idx + 1, max_window or (anchor_idx + 1))

    for w in range(longest, min_window - 1, -1):
        seg = bbi.iloc[anchor_idx - w + 1 : anchor_idx + 1]
        norm = seg / seg.iloc[0]                         # 归一化
        if np.diff(norm.values).min() >= 0:              # 每日导数(差分)皆非负
            return True
    return False


# --------------------------- Selector 类 --------------------------- #
class BBIKDJSelector:
    """
    自适应 *BBI(导数)* + *KDJ* 选股器，加入 DIF>0 约束

    Parameters
    ----------
    threshold         : 日线 J 值上限  
    bbi_min_window    : BBI 最短窗口  
    bbi_offset_n      : 锚点距 T 的偏移 n  
    max_window        : 参与指标计算的最大 K 线数（防止全历史过慢）
    """

    def __init__(
        self,
        threshold: float = -5,
        bbi_min_window: int = 90,
        bbi_offset_n: int = 0,
        max_window: int = 90,
    ) -> None:
        self.threshold = threshold
        self.bbi_min_window = bbi_min_window
        self.bbi_offset_n = bbi_offset_n
        self.max_window = max_window

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        hist = hist.copy()
        hist["BBI"] = compute_bbi(hist)

        # 1. BBI 必须整体上升
        if not bbi_deriv_uptrend(
            hist["BBI"],
            offset_n=self.bbi_offset_n,
            min_window=self.bbi_min_window,
            max_window=self.max_window,
        ):
            
            return False

        # 2. 日线 J 值
        j_today = float(compute_kdj(hist).iloc[-1]["J"])
        if j_today >= self.threshold:
            
            return False

        # 3. MACD 中 DIF>0
        hist["DIF"] = compute_dif(hist)
        if hist["DIF"].iloc[-1] <= 0:
            
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
            hist = hist.tail(self.max_window + self.bbi_offset_n + 5)  # 再多保留一点缓冲            
            if self._passes_filters(hist):
                picks.append(code)
        return picks



class BBIShortLongSelector:
    """
    BBI 上升 + 短/长期 RSV 条件 + DIF>0 选股器

    Parameters
    ----------
    n_short        : 短期 RSV (公式中的 N1)
    n_long         : 长期 RSV (公式中的 N2)
    m              : 判断区间长度 (最近 m 个交易日，含 T)
    bbi_min_window : BBI 上升检测的最短窗口
    bbi_offset_n   : BBI 锚点相对 T 的偏移
    max_window     : 读取历史的最大 K 线数
    """

    def __init__(
        self,
        n_short: int = 3,
        n_long: int = 21,
        m: int = 3,
        bbi_min_window: int = 90,
        bbi_offset_n: int = 0,
        max_window: int = 150,
    ) -> None:
        if m < 2:
            raise ValueError("m 必须 ≥ 2")
        self.n_short = n_short
        self.n_long = n_long
        self.m = m
        self.bbi_min_window = bbi_min_window
        self.bbi_offset_n = bbi_offset_n
        self.max_window = max_window

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        # 1. BBI 上升 ------------------
        hist = hist.copy()
        hist["BBI"] = compute_bbi(hist)

        if not bbi_deriv_uptrend(
            hist["BBI"],
            offset_n=self.bbi_offset_n,
            min_window=self.bbi_min_window,
            max_window=self.max_window,
        ):
            return False

        # 2. 计算短期 / 长期 RSV ----------
        hist["RSV_short"] = compute_rsv(hist, self.n_short)
        hist["RSV_long"] = compute_rsv(hist, self.n_long)

        if len(hist) < self.m:
            return False                                          # 数据不足

        win = hist.iloc[-self.m :]                                # 最近 m 天
        long_ok = (win["RSV_long"] >= 80).all()                   # 长期 ≥ 80

        short_series = win["RSV_short"]
        short_start_end_ok = short_series.iloc[0] >= 80 and short_series.iloc[-1] >= 80
        short_has_below_20 = (short_series < 20).any()

        if not (long_ok and short_start_end_ok and short_has_below_20):
            return False

        # 3. MACD 中 DIF>0 --------------
        hist["DIF"] = compute_dif(hist)
        return hist["DIF"].iloc[-1] > 0

    # ---------- 多股票批量 ---------- #
    def select(
        self,
        date: pd.Timestamp,
        data: Dict[str, pd.DataFrame],
    ) -> List[str]:
        """在给定交易日 `date` 对股票池 `data` 做筛选"""
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            # 预留足够长度：RSV 计算窗口 + BBI 检测窗口 + m
            need_len = (
                max(self.n_short, self.n_long)
                + self.bbi_min_window
                + self.bbi_offset_n
                + self.m
            )
            hist = hist.tail(max(need_len, self.max_window))
            if self._passes_filters(hist):
                picks.append(code)
        return picks


class BreakoutVolumeKDJSelector:
    """
    放量突破 + KDJ 选股器（更新版）

    规则
    ----
    1. 指定日 T0：J < j_threshold
    2. 在向前 offset 个交易日窗口内，存在某日 T 满足：
       2.1  (close_T / close_{T-1} - 1) × 100 ≥ up_threshold          # 已是“较前一日涨幅”
       2.2  数据区间内除 T 以外所有交易日的 volume ≤ volume_threshold × volume_T
       2.3  J(T … T0-1) 全部 > j_threshold
       2.4  close_T > 区间内 T 之前所有收盘价
    """

    def __init__(
        self,
        j_threshold: float = 0.0,
        up_threshold: float = 3.0,
        volume_threshold: float = 2.0 / 3,
        offset: int = 15,
        max_window: int = 120,
    ) -> None:
        self.j_threshold = j_threshold
        self.up_threshold = up_threshold
        self.volume_threshold = volume_threshold
        self.offset = offset
        self.max_window = max_window

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:

        if len(hist) < self.offset + 2:           # 至少 offset+1 根K线及一根前序K线
            return False

        # 控制窗口长度
        hist = hist.tail(self.max_window).copy()

        # ---- 计算指标 ----
        hist = compute_kdj(hist)
        hist["pct_chg"] = hist["close"].pct_change() * 100   # 已按“较前一日”计算

        # 1) 指定日 J
        if hist["J"].iloc[-1] >= self.j_threshold:
            return False

        n = len(hist)
        wnd_start = max(0, n - self.offset - 1)
        last_idx = n - 1                                     # T0 索引

        for t_idx in range(wnd_start, last_idx):             # 不含最后一天
            
            row = hist.iloc[t_idx]            

            # 2.1 较前一日涨幅
            if row["pct_chg"] < self.up_threshold:                
                continue

            # 2.2 区间内全局缩量：除 T 外所有 volume 均 ≤ vol_T × volume_threshold
            vol_T = row["volume"]
            if vol_T <= 0:                
                continue
            vols_except_T = hist["volume"].drop(index=hist.index[t_idx])
            if not (vols_except_T <= self.volume_threshold * vol_T).all():                
                continue

            # 2.4 价格创新高
            if not (row["close"] > hist["close"].iloc[:t_idx].max()):                
                continue

            # 2.3 J 高位持续
            if not (hist["J"].iloc[t_idx:last_idx] > hist["J"].iloc[-1]-10).all():                
                continue

            # 成功找到符合条件的 T
            return True

        return False
        
    # ---------- 多股票批量 ---------- #
    def select(
        self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]
    ) -> List[str]:

        picks: List[str] = []
        for code, df in data.items():
            
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            if self._passes_filters(hist):
                picks.append(code)
        return picks
