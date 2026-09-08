import unittest
from reporting.trade_matching import match_trades


def trade(day, quantity, price, fees, codes):
    return {"symbol": "TEST", "timestamp": f"2026-08-{day:02}T10:00:00", "quantity": quantity,
            "price": price, "fees": fees, "codes": codes}


class MatchingTests(unittest.TestCase):
    def test_partial_close_and_fees(self):
        rounds, warnings = match_trades([trade(1, 100, 10, 2, ["O"]), trade(2, -40, 12, 1, ["C"])])
        self.assertAlmostEqual(rounds[0]["net_pnl"], 78.2)
        self.assertAlmostEqual(rounds[0]["fees"], 1.8)
        self.assertTrue(any("60" in warning for warning in warnings))

    def test_short(self):
        rounds, _ = match_trades([trade(1, -10, 20, 1, ["O"]), trade(2, 10, 15, 1, ["C"])])
        self.assertEqual(rounds[0]["net_pnl"], 48)
        self.assertEqual(rounds[0]["quantity"], -10)

    def test_missing_initial_cost(self):
        rounds, warnings = match_trades([trade(1, -10, 20, 1, ["C"])])
        self.assertEqual(rounds, [])
        self.assertTrue(any("期初成本" in warning for warning in warnings))

    def test_fifo_and_sort(self):
        rounds, _ = match_trades([trade(3, -15, 30, 0, ["C"]), trade(1, 10, 10, 0, ["O"]), trade(2, 10, 20, 0, ["O"])])
        self.assertEqual([item["net_pnl"] for item in rounds], [200, 50])

    def test_symbol_charts_merge_fragments_and_keep_continuous_history(self):
        from datetime import date
        from pathlib import Path
        from unittest.mock import patch
        from reporting.trade_recap_engine import build_trade_recap

        bars = [{"date": date(2026, 8, day), "open": 10, "high": 30,
                 "low": 10, "close": 20, "volume": 100} for day in range(1, 31)]
        rounds = [
            {"symbol": symbol, "entry_date": f"2026-08-{entry:02}",
             "exit_date": f"2026-08-{exit_day:02}", "quantity": quantity,
             "entry_price": 10, "exit_price": 20, "fees": 1, "net_pnl": pnl}
            for symbol, entry, exit_day, quantity, pnl in [
                ("TEST", 3, 5, 4, 39), ("TEST", 3, 6, 6, 59),
                ("OTHER", 3, 5, 1, 9), ("TEST", 20, 23, -2, -21),
            ]
        ]
        with patch("reporting.trade_recap_engine._read_daily_bars", return_value=bars):
            report = build_trade_recap({"rounds": rounds}, Path("/unused"), 1, 2)
        self.assertEqual(len(report["symbol_charts"]), 2)
        chart = report["symbol_charts"][0]
        self.assertEqual(chart["symbol"], "TEST")
        self.assertEqual(chart["round_count"], 3)
        self.assertEqual(chart["net_pnl"], 77)
        self.assertEqual(chart["fees"], 3)
        self.assertEqual(chart["side"], "mixed")
        self.assertEqual([bar["date"] for bar in chart["bars"]],
                         [f"2026-08-{day:02}" for day in range(2, 26)])
        self.assertEqual(len(chart["markers"]), 5)
        self.assertEqual(chart["markers"][0]["quantity"], 10)
        self.assertEqual(chart["markers"][-2]["type"], "sell")
        self.assertEqual(chart["markers"][-1]["type"], "buy")
        self.assertEqual(report["summary"]["net_pnl"], 86)

    def test_chart_rejects_out_of_range(self):
        from datetime import date
        from pathlib import Path
        from unittest.mock import patch
        from reporting.trade_recap_engine import RoundTrip, _build_round_chart

        round_trip = RoundTrip("TEST", date(2026, 8, 1), date(2026, 8, 20), 10, 10, 20, 1, 99)
        with patch("reporting.trade_recap_engine._read_daily_bars", return_value=[{"date": date(2026, 8, 10)}]):
            with self.assertRaisesRegex(ValueError, "outside"):
                _build_round_chart(round_trip, Path("/unused"), 20, 20)


if __name__ == "__main__":
    unittest.main()