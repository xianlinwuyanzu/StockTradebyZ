from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

import pandas as pd


GAP_DEFAULTS = {
    "bullish_gap_weight": 1.0,
    "body_gap_reference_j_limit": 10.0,
    "full_gap_reference_j_limit": 20.0,
}


@dataclass(frozen=True)
class BullishGap:
    body_count: int = 0
    full_count: int = 0
    consecutive_count: int = 0
    quality: float = 0.0

    def to_dict(self) -> dict[str, int | float]:
        return asdict(self)


def measure_bullish_gaps(
    frame: pd.DataFrame, segments: Iterable[tuple[int, int]]
) -> BullishGap:
    body_positions: set[int] = set()
    full_positions: set[int] = set()
    consecutive_positions: set[int] = set()
    for start, end in segments:
        segment = frame.iloc[start : end + 1][["open", "high", "low", "close"]].astype(float)
        previous = segment.shift(1)
        bullish = (segment["close"] > segment["open"]) & (previous["close"] > previous["open"])
        body = bullish & (segment["open"] > previous["close"])
        full = body & (segment["low"] > previous["high"])
        consecutive = body & body.shift(1, fill_value=False)
        body_positions.update(start + offset for offset, hit in enumerate(body) if hit)
        full_positions.update(start + offset for offset, hit in enumerate(full) if hit)
        consecutive_positions.update(start + offset for offset, hit in enumerate(consecutive) if hit)
    quality = min(
        1.0,
        0.25 * len(body_positions)
        + 0.10 * len(full_positions)
        + 0.15 * len(consecutive_positions),
    )
    return BullishGap(len(body_positions), len(full_positions), len(consecutive_positions), quality)


def gap_reference_limit(
    gaps: BullishGap, base_limit: float, cfg: dict[str, float | int]
) -> float:
    if gaps.full_count:
        return max(base_limit, float(cfg.get("full_gap_reference_j_limit", 20.0)))
    if gaps.body_count:
        return max(base_limit, float(cfg.get("body_gap_reference_j_limit", 10.0)))
    return base_limit