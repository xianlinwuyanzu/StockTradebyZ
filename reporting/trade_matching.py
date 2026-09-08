from collections import defaultdict, deque
from datetime import datetime
from decimal import Decimal
import re


def match_trades(trades: list[dict]) -> tuple[list[dict], list[str]]:
    inventory = defaultdict(deque)
    unmatched = defaultdict(lambda: Decimal(0))
    rounds = []
    for trade in sorted(trades, key=lambda item: item["timestamp"]):
        symbol = trade["symbol"]
        if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,19}", symbol):
            raise ValueError("Invalid trade symbol")
        timestamp = datetime.fromisoformat(trade["timestamp"])
        quantity, price, fees = (Decimal(str(trade[field])) for field in ("quantity", "price", "fees"))
        if not all(value.is_finite() for value in (quantity, price, fees)) or quantity == 0 or price <= 0:
            raise ValueError("Invalid trade values")
        direction = Decimal(1) if quantity > 0 else Decimal(-1)
        remaining = abs(quantity)
        fee_per_share = fees / remaining
        lots = inventory[symbol]
        while remaining and lots and lots[0]["direction"] != direction:
            lot = lots[0]
            matched = min(remaining, lot["remaining"])
            allocated_fees = matched * (lot["fee_per_share"] + fee_per_share)
            pnl = matched * lot["direction"] * (price - lot["price"]) - allocated_fees
            rounds.append({
                "symbol": symbol, "entry_date": lot["timestamp"].date().isoformat(),
                "exit_date": timestamp.date().isoformat(), "quantity": float(matched * lot["direction"]),
                "entry_price": float(lot["price"]), "exit_price": float(price),
                "fees": float(allocated_fees), "net_pnl": float(pnl),
            })
            remaining -= matched
            lot["remaining"] -= matched
            if not lot["remaining"]:
                lots.popleft()
        if remaining:
            if "O" not in trade["codes"]:
                unmatched[symbol] += remaining
            else:
                lots.append({"remaining": remaining, "direction": direction, "price": price,
                             "fee_per_share": fee_per_share, "timestamp": timestamp})
    warnings = ["FIFO 配对片段口径：仅计算有完整开平仓成本的已实现盈亏；胜率不等于逐股胜率，费用仅含已匹配部分。"]
    for symbol, quantity in unmatched.items():
        warnings.append(f"{symbol}：{quantity} 股平仓缺少期初成本，已排除。")
    for symbol, lots in inventory.items():
        quantity = sum(lot["remaining"] * lot["direction"] for lot in lots)
        if quantity:
            warnings.append(f"{symbol}：期末剩余 {quantity} 股，未实现盈亏未计入。")
    return rounds, warnings