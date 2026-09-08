from __future__ import annotations

import os
import re
from pathlib import Path

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