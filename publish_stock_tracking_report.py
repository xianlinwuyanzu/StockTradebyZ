#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from zoneinfo import ZoneInfo

_HEADER_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ \[INFO\] =+ 选股结果 \[(?P<name>.+?)\] =+")
_RUN_START_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ \[INFO\] (?:选股运行开始|未指定 --date)")
_TRADE_DAY_RE = re.compile(r"^交易日:\s*(\d{4}-\d{2}-\d{2})$")
_COUNT_RE = re.compile(r"^符合条件股票数:\s*(\d+)$")
_INFO_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ \[INFO\] (.*)$")


def _resolve_log_timezone() -> timezone | ZoneInfo:
    timezone_name = os.getenv("STOCK_TRACKING_LOG_TZ", "Asia/Shanghai").strip() or "Asia/Shanghai"
    try:
        return ZoneInfo(timezone_name)
    except Exception:
        return timezone.utc


_LOG_TIMEZONE = _resolve_log_timezone()


@dataclass
class StrategyBlock:
    name: str
    trade_date: date | None
    pick_count: int
    picks: list[str]
    updated_at: datetime
    run_id: int


def _extract_info_text(line: str) -> str | None:
    match = _INFO_RE.match(line.strip())
    if not match:
        return None
    return match.group(1).strip()


def _parse_picks(text: str) -> list[str]:
    if not text or text == "无符合条件股票":
        return []
    return [item.strip().upper() for item in text.split(",") if item.strip()]


def parse_strategy_blocks(log_path: Path) -> list[StrategyBlock]:
    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    blocks: list[StrategyBlock] = []

    i = 0
    run_id = 0
    while i < len(lines):
        line = lines[i].strip()
        if _RUN_START_RE.match(line):
            run_id += 1
            i += 1
            continue
        header = _HEADER_RE.match(line)
        if not header:
            i += 1
            continue

        updated_at = (
            datetime.strptime(header.group("ts"), "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=_LOG_TIMEZONE)
            .astimezone(timezone.utc)
        )
        name = header.group("name").strip()

        trade_day: date | None = None
        pick_count = 0
        picks: list[str] = []

        j = i + 1
        while j < len(lines):
            next_line = lines[j].strip()
            if _HEADER_RE.match(next_line) or _RUN_START_RE.match(next_line):
                break

            info_text = _extract_info_text(next_line)
            if info_text is None:
                j += 1
                continue

            trade_day_match = _TRADE_DAY_RE.match(info_text)
            if trade_day_match:
                trade_day = datetime.strptime(trade_day_match.group(1), "%Y-%m-%d").date()
                j += 1
                continue

            count_match = _COUNT_RE.match(info_text)
            if count_match:
                pick_count = int(count_match.group(1))
                j += 1
                continue

            if info_text == "无符合条件股票":
                picks = []
                j += 1
                continue

            if "," in info_text or re.fullmatch(r"[A-Z0-9.\-]+", info_text):
                parsed = _parse_picks(info_text)
                if parsed:
                    picks = parsed

            j += 1

        if pick_count != len(picks) and pick_count == 0 and picks:
            pick_count = len(picks)

        blocks.append(StrategyBlock(
            name=name,
            trade_date=trade_day,
            pick_count=pick_count,
            picks=picks,
            updated_at=updated_at,
            run_id=run_id,
        ))

        i = j

    return blocks


def latest_snapshot(blocks: list[StrategyBlock]) -> tuple[date | None, list[StrategyBlock], datetime]:
    latest_run_id = max(item.run_id for item in blocks)
    if latest_run_id > 0:
        recent = [item for item in blocks if item.run_id == latest_run_id]
    else:
        latest_ts = max(item.updated_at for item in blocks)
        cutoff = latest_ts - timedelta(minutes=40)
        recent = [item for item in blocks if item.updated_at >= cutoff]
        if not recent:
            recent = blocks

    dedup: dict[str, StrategyBlock] = {}
    for item in recent:
        prev = dedup.get(item.name)
        if prev is None or item.updated_at > prev.updated_at:
            dedup[item.name] = item

    selected = sorted(dedup.values(), key=lambda item: (-item.pick_count, item.name))

    trade_days = [item.trade_date for item in selected if item.trade_date is not None]
    if trade_days:
        counter = Counter(trade_days)
        trade_day = counter.most_common(1)[0][0]
    else:
        trade_day = None

    generated_at = datetime.now(timezone.utc)
    return trade_day, selected, generated_at


def build_payload(source_log: Path, blocks: list[StrategyBlock]) -> dict[str, Any]:
    trade_day, latest, generated_at = latest_snapshot(blocks)
    return {
        "generated_at": generated_at.isoformat(),
        "trade_date": trade_day.isoformat() if trade_day else None,
        "source_log": str(source_log),
        "strategies": [
            {
                "name": item.name,
                "pick_count": item.pick_count,
                "picks": item.picks,
                "updated_at": item.updated_at.isoformat(),
            }
            for item in latest
        ],
    }


def _derive_wave_api_url(stock_api_url: str, explicit_wave_api_url: str) -> str:
    if explicit_wave_api_url.strip():
        return explicit_wave_api_url.strip()
    marker = "/stock-tracking/latest"
    if marker in stock_api_url:
        return stock_api_url.replace(marker, "/stock-tracking/wave-selection/latest")
    return stock_api_url.rstrip("/") + "/wave-selection/latest"


def _derive_one_wave_api_url(stock_api_url: str, explicit_one_wave_api_url: str) -> str:
    if explicit_one_wave_api_url.strip():
        return explicit_one_wave_api_url.strip()
    marker = "/stock-tracking/latest"
    if marker in stock_api_url:
        return stock_api_url.replace(marker, "/stock-tracking/one-wave-entry/latest")
    return stock_api_url.rstrip("/") + "/one-wave-entry/latest"


def build_wave_payload(wave_json_path: Path, generated_at: datetime, fallback_trade_day: date | None) -> dict[str, Any] | None:
    if not wave_json_path.exists():
        return None
    try:
        raw = json.loads(wave_json_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if not isinstance(raw, dict):
        return None
    results = raw.get("results")
    if not isinstance(results, list):
        return None

    raw_trade_day = raw.get("trade_date")
    if isinstance(raw_trade_day, str) and raw_trade_day.strip():
        resolved_trade_day = raw_trade_day.strip()
    elif fallback_trade_day is not None:
        resolved_trade_day = fallback_trade_day.isoformat()
    else:
        resolved_trade_day = None

    strategy_name = str(raw.get("strategy") or "二波选股策略")
    score_threshold = float(raw.get("score_threshold") or 0.0)
    data_days = int(raw.get("data_days") or 0)
    result_count = int(raw.get("result_count") or len(results))
    preselection_results = raw.get("preselection_results")
    if not isinstance(preselection_results, list):
        preselection_results = []
    preselection_count = int(raw.get("preselection_count") or len(preselection_results))

    return {
        "generated_at": generated_at.isoformat(),
        "trade_date": resolved_trade_day,
        "strategy": strategy_name,
        "score_threshold": score_threshold,
        "data_days": data_days,
        "result_count": result_count,
        "results": results,
        "preselection_count": preselection_count,
        "preselection_results": preselection_results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish latest stock strategy picks to backend API")
    parser.add_argument("--log", default="./select_results.log", help="Path to selector log file")
    parser.add_argument("--api-url", default=os.getenv("STOCK_TRACKING_API_URL", "https://mingzhishan.cloud/api/v1/stock-tracking/latest"))
    parser.add_argument("--wave-api-url", default=os.getenv("WAVE_SELECTION_API_URL", ""))
    parser.add_argument("--wave-json", default="./wave_selection_data/wave_selection_results.json", help="Path to wave selection structured json")
    parser.add_argument("--one-wave-api-url", default=os.getenv("ONE_WAVE_ENTRY_API_URL", ""))
    parser.add_argument("--one-wave-json", default="./one_wave_entry_data/wave_selection_results.json", help="Path to one-wave entry structured json")
    parser.add_argument("--token", default=os.getenv("STOCK_TRACKING_PUSH_TOKEN", "change-me-stock-tracking-token"))
    parser.add_argument("--timeout", type=int, default=20)
    args = parser.parse_args()

    log_path = Path(args.log)
    if not log_path.exists():
        print(f"[stock-tracking] log not found: {log_path}")
        return 1

    if not args.token.strip():
        print("[stock-tracking] missing STOCK_TRACKING_PUSH_TOKEN, skip publish")
        return 2

    blocks = parse_strategy_blocks(log_path)
    if not blocks:
        print("[stock-tracking] no strategy blocks found, skip publish")
        return 3

    payload = build_payload(log_path, blocks)
    generated_at = datetime.fromisoformat(str(payload.get("generated_at")))
    raw_trade_day = payload.get("trade_date")
    fallback_trade_day = date.fromisoformat(raw_trade_day) if isinstance(raw_trade_day, str) and raw_trade_day else None
    response = requests.post(
        args.api_url,
        json=payload,
        headers={"X-Stock-Tracking-Token": args.token.strip()},
        timeout=args.timeout,
    )
    response.raise_for_status()

    strategy_count = len(payload.get("strategies", []))
    print(f"[stock-tracking] published {strategy_count} strategies to frontend")

    wave_payload = build_wave_payload(Path(args.wave_json), generated_at, fallback_trade_day)
    if wave_payload is None:
        print(f"[stock-tracking] wave payload not found or invalid: {args.wave_json}")
    else:
        wave_api_url = _derive_wave_api_url(args.api_url, args.wave_api_url)
        try:
            wave_response = requests.post(
                wave_api_url,
                json=wave_payload,
                headers={"X-Stock-Tracking-Token": args.token.strip()},
                timeout=args.timeout,
            )
            wave_response.raise_for_status()
            print(f"[stock-tracking] published wave-selection {wave_payload.get('result_count', 0)} rows")
        except Exception as exc:
            print(f"[stock-tracking] wave-selection publish failed: {exc}")

    one_wave_payload = build_wave_payload(Path(args.one_wave_json), generated_at, fallback_trade_day)
    if one_wave_payload is None:
        print(f"[stock-tracking] one-wave payload not found or invalid: {args.one_wave_json}")
    else:
        one_wave_api_url = _derive_one_wave_api_url(args.api_url, args.one_wave_api_url)
        try:
            one_wave_response = requests.post(
                one_wave_api_url,
                json=one_wave_payload,
                headers={"X-Stock-Tracking-Token": args.token.strip()},
                timeout=args.timeout,
            )
            one_wave_response.raise_for_status()
            print(f"[stock-tracking] published one-wave-entry {one_wave_payload.get('result_count', 0)} rows")
        except Exception as exc:
            print(f"[stock-tracking] one-wave-entry publish failed: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
