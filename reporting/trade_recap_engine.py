from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from reporting.trade_matching import match_trades


@dataclass(slots=True)
class RoundTrip:
    symbol: str
    entry_date: date
    exit_date: date
    quantity: float
    entry_price: float
    exit_price: float
    fees: float
    net_pnl: float

    @property
    def side(self) -> str:
        return "long" if self.quantity >= 0 else "short"


def _to_date(value: Any, field_name: str) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"missing {field_name}")
    return date.fromisoformat(text)


def _to_float(value: Any, field_name: str) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip().replace(",", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"invalid number for {field_name}: {value}") from exc


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        raise ValueError("missing symbol")
    return text


def _load_round_trips(payload: dict[str, Any]) -> list[RoundTrip]:
    raw_rounds = payload.get("rounds")
    if not isinstance(raw_rounds, list):
        raise ValueError("payload.rounds must be a list")

    rounds: list[RoundTrip] = []
    for index, item in enumerate(raw_rounds):
        if not isinstance(item, dict):
            raise ValueError(f"rounds[{index}] must be an object")

        symbol = _normalize_symbol(item.get("symbol"))
        entry_date = _to_date(item.get("entry_date"), "entry_date")
        exit_date = _to_date(item.get("exit_date"), "exit_date")
        quantity = _to_float(item.get("quantity", 0), "quantity")
        entry_price = _to_float(item.get("entry_price", 0), "entry_price")
        exit_price = _to_float(item.get("exit_price", 0), "exit_price")
        fees = _to_float(item.get("fees", 0), "fees")
        net_pnl = _to_float(item.get("net_pnl", 0), "net_pnl")

        if exit_date < entry_date:
            raise ValueError(f"rounds[{index}] exit_date before entry_date")

        rounds.append(
            RoundTrip(
                symbol=symbol,
                entry_date=entry_date,
                exit_date=exit_date,
                quantity=quantity,
                entry_price=entry_price,
                exit_price=exit_price,
                fees=fees,
                net_pnl=net_pnl,
            )
        )

    return rounds


def _read_daily_bars(data_dir: Path, symbol: str) -> list[dict[str, Any]]:
    csv_path = data_dir / f"{symbol}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"daily csv not found: {csv_path}")

    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            day_text = str(raw.get("date") or "").strip()
            if not day_text:
                continue
            try:
                day_value = date.fromisoformat(day_text)
            except ValueError:
                continue

            open_value = _to_float(raw.get("open", 0), "open")
            high_value = _to_float(raw.get("high", 0), "high")
            low_value = _to_float(raw.get("low", 0), "low")
            close_value = _to_float(raw.get("close", 0), "close")
            volume_value = _to_float(raw.get("volume", 0), "volume")

            rows.append(
                {
                    "date": day_value,
                    "open": open_value,
                    "high": high_value,
                    "low": low_value,
                    "close": close_value,
                    "volume": volume_value,
                }
            )

    rows.sort(key=lambda item: item["date"])
    return rows


def _first_ge_index(days: list[date], target: date) -> int:
    for index, day_value in enumerate(days):
        if day_value >= target:
            return index
    return len(days) - 1


def _last_le_index(days: list[date], target: date) -> int:
    for index in range(len(days) - 1, -1, -1):
        if days[index] <= target:
            return index
    return 0


def _build_round_chart(
    trade_round: RoundTrip,
    data_dir: Path,
    window_before: int,
    window_after: int,
) -> dict[str, Any]:
    bars = _read_daily_bars(data_dir, trade_round.symbol)
    if not bars:
        raise ValueError(f"no daily bars for {trade_round.symbol}")

    days = [row["date"] for row in bars]
    if trade_round.entry_date < days[0] or trade_round.exit_date > days[-1]:
        raise ValueError("trade dates outside available daily history")
    entry_index = _first_ge_index(days, trade_round.entry_date)
    exit_index = _last_le_index(days, trade_round.exit_date)
    if exit_index < entry_index:
        exit_index = entry_index

    start_index = max(0, entry_index - max(0, window_before))
    end_index = min(len(bars) - 1, exit_index + max(0, window_after))

    window_rows = bars[start_index : end_index + 1]
    serialized_bars = [
        {
            "date": row["date"].isoformat(),
            "open": round(row["open"], 4),
            "high": round(row["high"], 4),
            "low": round(row["low"], 4),
            "close": round(row["close"], 4),
            "volume": row["volume"],
        }
        for row in window_rows
    ]

    entry_row = bars[entry_index]
    exit_row = bars[exit_index]

    markers = [
        {
            "type": "buy" if trade_round.side == "long" else "sell",
            "symbol": trade_round.symbol,
            "trade_date": trade_round.entry_date.isoformat(),
            "price": round(trade_round.entry_price, 4),
            "mapped_date": entry_row["date"].isoformat(),
            "quantity": trade_round.quantity,
        },
        {
            "type": "sell" if trade_round.side == "long" else "buy",
            "symbol": trade_round.symbol,
            "trade_date": trade_round.exit_date.isoformat(),
            "price": round(trade_round.exit_price, 4),
            "mapped_date": exit_row["date"].isoformat(),
            "quantity": trade_round.quantity,
        },
    ]

    return {
        "symbol": trade_round.symbol,
        "side": trade_round.side,
        "entry_date": trade_round.entry_date.isoformat(),
        "exit_date": trade_round.exit_date.isoformat(),
        "entry_price": round(trade_round.entry_price, 4),
        "exit_price": round(trade_round.exit_price, 4),
        "quantity": trade_round.quantity,
        "fees": round(trade_round.fees, 4),
        "net_pnl": round(trade_round.net_pnl, 4),
        "window_before_bars": window_before,
        "window_after_bars": window_after,
        "bars": serialized_bars,
        "markers": markers,
        "source": "sf:data/us_stocks",
    }


def _build_symbol_charts(
    rounds: list[RoundTrip], charts: list[dict[str, Any]], data_dir: Path,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for chart in charts:
        grouped.setdefault(chart["symbol"], []).append(chart)

    symbol_charts = []
    for symbol, fragments in grouped.items():
        symbol_rounds = [item for item in rounds if item.symbol == symbol]
        start_date = min(chart["bars"][0]["date"] for chart in fragments)
        end_date = max(chart["bars"][-1]["date"] for chart in fragments)
        bars = [
            {**row, "date": row["date"].isoformat()}
            for row in _read_daily_bars(data_dir, symbol)
            if start_date <= row["date"].isoformat() <= end_date
        ]
        merged_markers: dict[tuple, dict[str, Any]] = {}
        for fragment in fragments:
            for marker in fragment["markers"]:
                key = (marker["trade_date"], marker["type"], marker["price"], marker["mapped_date"])
                if key not in merged_markers:
                    merged_markers[key] = {**marker, "quantity": 0.0}
                merged_markers[key]["quantity"] += abs(marker["quantity"])
        sides = {item.side for item in symbol_rounds}
        symbol_charts.append({
            "symbol": symbol,
            "side": next(iter(sides)) if len(sides) == 1 else "mixed",
            "entry_date": min(item.entry_date for item in symbol_rounds).isoformat(),
            "exit_date": max(item.exit_date for item in symbol_rounds).isoformat(),
            "round_count": len(symbol_rounds),
            "charted_round_count": len(fragments),
            "fees": round(sum(item.fees for item in symbol_rounds), 4),
            "net_pnl": round(sum(item.net_pnl for item in symbol_rounds), 4),
            "bars": bars,
            "markers": [merged_markers[key] for key in sorted(merged_markers)],
        })
    return symbol_charts


def _compute_summary(rounds: list[RoundTrip]) -> dict[str, Any]:
    count = len(rounds)
    if count == 0:
        return {
            "count": 0,
            "net_pnl": 0.0,
            "total_fees": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_pnl": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "expectancy": 0.0,
            "top10_abs_concentration": 0.0,
            "best_symbol": None,
            "worst_symbol": None,
        }

    pnls = [round_item.net_pnl for round_item in rounds]
    net_pnl = sum(pnls)
    total_fees = sum(round_item.fees for round_item in rounds)

    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]

    gross_profit = sum(wins)
    gross_loss = -sum(losses)
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

    symbol_agg: dict[str, float] = {}
    for round_item in rounds:
        symbol_agg[round_item.symbol] = symbol_agg.get(round_item.symbol, 0.0) + round_item.net_pnl

    ranked_symbols = sorted(symbol_agg.items(), key=lambda item: item[1], reverse=True)

    abs_total = sum(abs(value) for value in pnls)
    top10_abs = sum(sorted((abs(value) for value in pnls), reverse=True)[:10])

    return {
        "count": count,
        "net_pnl": round(net_pnl, 4),
        "total_fees": round(total_fees, 4),
        "win_rate": round(len(wins) / count, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
        "avg_pnl": round(net_pnl / count, 4),
        "avg_win": round(sum(wins) / len(wins), 4) if wins else 0.0,
        "avg_loss": round(sum(losses) / len(losses), 4) if losses else 0.0,
        "expectancy": round(net_pnl / count, 4),
        "top10_abs_concentration": round((top10_abs / abs_total), 4) if abs_total > 0 else 0.0,
        "best_symbol": ranked_symbols[0][0] if ranked_symbols else None,
        "worst_symbol": ranked_symbols[-1][0] if ranked_symbols else None,
    }


def build_trade_recap(
    payload: dict[str, Any],
    data_dir: Path,
    window_before: int = 20,
    window_after: int = 20,
) -> dict[str, Any]:
    warnings = list(payload.get("warnings", []))
    if "trades" in payload:
        matched_rounds, matching_warnings = match_trades(payload["trades"])
        payload = {"rounds": matched_rounds}
        warnings.extend(matching_warnings)
    rounds = _load_round_trips(payload)

    chart_rows: list[dict[str, Any]] = []

    for round_item in rounds:
        try:
            chart = _build_round_chart(
                    trade_round=round_item,
                    data_dir=data_dir,
                    window_before=window_before,
                    window_after=window_after,
                )
            chart_rows.append(chart)
            before_count = sum(row["date"] < chart["markers"][0]["mapped_date"] for row in chart["bars"])
            after_count = sum(row["date"] > chart["markers"][1]["mapped_date"] for row in chart["bars"])
            if before_count < window_before or after_count < window_after:
                warnings.append(f"{round_item.symbol}: 部分图表不足所需前后交易日窗口，仅展示已有行情。")
        except FileNotFoundError:
            warnings.append(f"missing symbol daily data: {round_item.symbol}")
        except Exception as exc:
            warnings.append(f"failed to build chart for {round_item.symbol}: {exc}")

    response = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": _compute_summary(rounds),
        "round_count": len(rounds),
        "chart_count": len(chart_rows),
        "charts": chart_rows,
        "symbol_charts": _build_symbol_charts(rounds, chart_rows, data_dir),
        "warnings": list(dict.fromkeys(warnings)),
        "source": {
            "strategy_repo": "sf",
            "market_data_dir": str(data_dir),
            "window_before": window_before,
            "window_after": window_after,
        },
    }
    return response


def _read_payload(path_value: str) -> dict[str, Any]:
    if path_value == "-":
        raw_text = sys.stdin.read()
        if not raw_text.strip():
            raise ValueError("stdin payload is empty")
        return json.loads(raw_text)
    path = Path(path_value)
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build trade recap report from normalized rounds and sf daily bars")
    parser.add_argument("--input", required=True, help="Path to normalized JSON payload or '-' for stdin")
    parser.add_argument("--output", default="", help="Output file path (defaults to stdout)")
    parser.add_argument("--data-dir", default="./data/us_stocks", help="Daily bars directory")
    parser.add_argument("--window-before", type=int, default=20)
    parser.add_argument("--window-after", type=int, default=20)
    args = parser.parse_args()

    payload = _read_payload(args.input)
    report = build_trade_recap(
        payload=payload,
        data_dir=Path(args.data_dir),
        window_before=max(0, args.window_before),
        window_after=max(0, args.window_after),
    )

    text = json.dumps(report, ensure_ascii=False)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
