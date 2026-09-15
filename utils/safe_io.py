from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd

_SAFE_TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,11}$")


def is_safe_ticker(value: object) -> bool:
    return isinstance(value, str) and bool(_SAFE_TICKER_PATTERN.fullmatch(value))


def ticker_csv_path(output_dir: Path, ticker: str) -> Path:
    if not is_safe_ticker(ticker):
        raise ValueError(f"unsafe ticker for output filename: {ticker!r}")

    output_root = output_dir.resolve()
    output_path = (output_root / f"{ticker}.csv").resolve(strict=False)
    try:
        output_path.relative_to(output_root)
    except ValueError as exc:
        raise ValueError(f"ticker output escaped output directory: {ticker!r}") from exc
    return output_path


def write_dataframe_csv(frame: object, output_dir: Path, ticker: str) -> Path:
    output_path = ticker_csv_path(output_dir, ticker)
    file_descriptor = os.open(
        output_path,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        os.fchmod(file_descriptor, 0o600)
        with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="") as output_file:
            frame.to_csv(output_file, index=False)
        file_descriptor = -1
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)
    return output_path


def merge_dataframe_with_csv(frame: pd.DataFrame, output_dir: Path, ticker: str) -> pd.DataFrame:
    output_path = ticker_csv_path(output_dir, ticker)
    if not output_path.exists():
        return frame

    try:
        existing = pd.read_csv(output_path)
    except Exception:
        return frame

    if existing.empty or "date" not in existing.columns:
        return frame

    combined = pd.concat([existing, frame], ignore_index=True, sort=False)
    if "date" not in combined.columns:
        return frame

    combined["_sort_date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.dropna(subset=["_sort_date"])
    if combined.empty:
        return frame

    ordered_columns = [column for column in frame.columns if column in combined.columns]
    extra_columns = [column for column in combined.columns if column not in ordered_columns and column != "_sort_date"]
    merged = (
        combined.sort_values("_sort_date")
        .drop_duplicates(subset=["_sort_date"], keep="last")
        .sort_values("_sort_date")
        .reset_index(drop=True)[ordered_columns + extra_columns]
    )
    parsed_dates = pd.to_datetime(merged["date"], errors="coerce")
    if parsed_dates.notna().all():
        has_intraday_time = (parsed_dates.dt.time != pd.Timestamp("00:00:00").time()).any()
        date_format = "%Y-%m-%d %H:%M:%S" if has_intraday_time else "%Y-%m-%d"
        merged["date"] = parsed_dates.dt.strftime(date_format)
    return merged
