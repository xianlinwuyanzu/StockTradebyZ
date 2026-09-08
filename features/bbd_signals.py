from __future__ import annotations

import numpy as np
import pandas as pd


SIGNAL_COLUMNS = [
    "bbd",
    "bbd_support",
    "momentum_line",
    "momentum_aux",
    "bbd_buy",
    "bbd_sell",
    "bbd_bottom_divergence",
    "bbd_top_divergence",
    "momentum_buy",
    "momentum_sell",
    "momentum_bottom_divergence",
    "momentum_top_divergence",
    "buy_signal",
    "sell_signal",
    "buy_signals",
    "sell_signals",
    "buy_signal_type",
    "sell_signal_type",
]


def _tdx_sma(series: pd.Series, n: int, m: int) -> pd.Series:
    return series.astype(float).ewm(alpha=m / n, adjust=False).mean()


def _cross_up(left: pd.Series, right: pd.Series) -> pd.Series:
    return ((left > right) & (left.shift(1) <= right.shift(1))).fillna(False)


def _previous_cross_value(series: pd.Series, cross_signal: pd.Series) -> pd.Series:
    positions = np.arange(len(series), dtype=float)
    cross_positions = pd.Series(
        np.where(cross_signal.to_numpy(dtype=bool), positions, np.nan),
        index=series.index,
    )
    previous_positions = cross_positions.shift(1).ffill()
    result = pd.Series(np.nan, index=series.index, dtype=float)
    valid = previous_positions.notna()
    if valid.any():
        indices = previous_positions.loc[valid].astype(int).to_numpy()
        result.loc[valid] = series.astype(float).to_numpy()[indices]
    return result


def _label_series(masks: dict[str, pd.Series]) -> pd.Series:
    mask_frame = pd.DataFrame(masks)
    return mask_frame.apply(
        lambda row: "|".join(row.index[row.to_numpy(dtype=bool)]),
        axis=1,
    )


def _signal_lists(masks: dict[str, pd.Series]) -> list[list[str]]:
    mask_frame = pd.DataFrame(masks)
    return [list(mask_frame.columns[row.to_numpy(dtype=bool)]) for _, row in mask_frame.iterrows()]


def compute_bbd_signals(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"high", "low", "close"}
    if not required.issubset(frame.columns):
        raise ValueError(f"missing BBD columns: {sorted(required - set(frame.columns))}")

    close = frame["close"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    al = (close + low + high) / 3.0
    ao = _tdx_sma(al, 5, 1) - _tdx_sma(al, 13, 1)
    bbd = (ao - _tdx_sma(ao, 3, 1)) * 100.0
    bbd_support = _tdx_sma(bbd, 5, 2)
    momentum_line = ao * 10.0
    momentum_aux = ao.ewm(span=5, adjust=False).mean() * 10.0

    bbd_buy = _cross_up(bbd, bbd_support)
    bbd_sell = _cross_up(bbd_support, bbd)
    momentum_buy = _cross_up(momentum_line, momentum_aux)
    momentum_sell = _cross_up(momentum_aux, momentum_line)

    previous_bbd_buy_close = _previous_cross_value(close, bbd_buy)
    previous_bbd_buy_value = _previous_cross_value(bbd, bbd_buy)
    bbd_bottom_divergence = (
        bbd_buy
        & previous_bbd_buy_close.notna()
        & previous_bbd_buy_value.notna()
        & (previous_bbd_buy_close > close)
        & (bbd > previous_bbd_buy_value)
    )

    previous_bbd_sell_close = _previous_cross_value(close, bbd_sell)
    previous_bbd_sell_value = _previous_cross_value(bbd, bbd_sell)
    bbd_top_divergence = (
        bbd_sell
        & previous_bbd_sell_close.notna()
        & previous_bbd_sell_value.notna()
        & (previous_bbd_sell_close < close)
        & (previous_bbd_sell_value > bbd)
    )

    previous_momentum_buy_close = _previous_cross_value(close, momentum_buy)
    previous_momentum_buy_value = _previous_cross_value(momentum_line, momentum_buy)
    momentum_bottom_divergence = (
        momentum_buy
        & previous_momentum_buy_close.notna()
        & previous_momentum_buy_value.notna()
        & (previous_momentum_buy_close > close)
        & (momentum_line > previous_momentum_buy_value)
    )

    previous_momentum_sell_close = _previous_cross_value(close, momentum_sell)
    previous_momentum_sell_value = _previous_cross_value(momentum_line, momentum_sell)
    momentum_top_divergence = (
        momentum_sell
        & previous_momentum_sell_close.notna()
        & previous_momentum_sell_value.notna()
        & (previous_momentum_sell_close < close)
        & (previous_momentum_sell_value > momentum_line)
    )

    buy_masks = {
        "BBD金叉": bbd_buy,
        "B底背": bbd_bottom_divergence,
        "动能金叉": momentum_buy,
        "动底背": momentum_bottom_divergence,
    }
    sell_masks = {
        "BBD死叉": bbd_sell,
        "B顶背": bbd_top_divergence,
        "动能死叉": momentum_sell,
        "动顶背": momentum_top_divergence,
    }

    result = pd.DataFrame(
        {
            "bbd": bbd,
            "bbd_support": bbd_support,
            "momentum_line": momentum_line,
            "momentum_aux": momentum_aux,
            "bbd_buy": bbd_buy,
            "bbd_sell": bbd_sell,
            "bbd_bottom_divergence": bbd_bottom_divergence,
            "bbd_top_divergence": bbd_top_divergence,
            "momentum_buy": momentum_buy,
            "momentum_sell": momentum_sell,
            "momentum_bottom_divergence": momentum_bottom_divergence,
            "momentum_top_divergence": momentum_top_divergence,
        },
        index=frame.index,
    )
    result["buy_signal"] = result[["bbd_buy", "bbd_bottom_divergence", "momentum_buy", "momentum_bottom_divergence"]].any(axis=1)
    result["sell_signal"] = result[["bbd_sell", "bbd_top_divergence", "momentum_sell", "momentum_top_divergence"]].any(axis=1)
    result["buy_signals"] = _signal_lists(buy_masks)
    result["sell_signals"] = _signal_lists(sell_masks)
    result["buy_signal_type"] = _label_series(buy_masks)
    result["sell_signal_type"] = _label_series(sell_masks)
    return result[SIGNAL_COLUMNS]
