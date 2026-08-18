from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jxt_v7.pipeline import JXTV7Pipeline


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="JXT v7.0 starter pipeline (premarket single refresh)")
    parser.add_argument("--ticker", default="SPY", help="symbol to analyze, default SPY (SPX will be mapped to SPY)")
    parser.add_argument("--cache-dir", default="./jxt_v7/cache", help="cache directory")
    parser.add_argument("--output-dir", default="./jxt_v7/output", help="output directory")
    parser.add_argument("--risk-free-rate", type=float, default=0.04, help="annual risk-free rate")
    parser.add_argument("--run-tests", action="store_true", help="run step-by-step robustness checks")
    parser.add_argument("--verbose", action="store_true", help="verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)

    pipeline = JXTV7Pipeline(
        ticker=args.ticker,
        cache_dir=Path(args.cache_dir),
        output_dir=Path(args.output_dir),
        risk_free_rate=args.risk_free_rate,
    )

    if args.run_tests:
        results = pipeline.run_step_tests()
        print("\n=== Step-by-step test results ===")
        for name, ok, msg in results:
            status = "PASS" if ok else "FAIL"
            print(f"[{status}] {name}: {msg}")

        if any(not ok for _, ok, _ in results):
            raise SystemExit(2)
        return

    result = pipeline.run()

    print("\n=== JXT v7.0 run completed ===")
    print(f"Ticker: {pipeline.ticker}")
    print("Outputs:")
    for k, p in result.output_paths.items():
        print(f"- {k}: {p}")


if __name__ == "__main__":
    main()
