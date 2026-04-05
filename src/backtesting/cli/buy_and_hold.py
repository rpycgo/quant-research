"""
backtesting.cli.buy_and_hold
=============================
CLI entry point for the passive buy-and-hold benchmark.

Registered as ``qr-buy-and-hold`` in ``[project.scripts]``.

Usage
-----
::

    qr-buy-and-hold --symbol BTCUSDT
    qr-buy-and-hold --symbol BTCUSDT --start 2020-04-01 --end 2026-01-31
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys

from backtesting.assets.crypto import CryptoLoader
from backtesting.benchmarks.buy_and_hold import BuyAndHoldBenchmark
from backtesting.core.config_loader import BacktestConfigLoader

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qr-buy-and-hold",
        description="Passive buy-and-hold benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbol", type=str, required=True,
        help="Trading pair symbol, e.g. BTCUSDT.",
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="Start date (ISO-8601). Overrides config.",
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="End date (ISO-8601). Overrides config.",
    )
    parser.add_argument(
        "--config-dir", type=str,
        default=str(_REPO_ROOT / "src" / "configs"),
        help="Path to configs/ directory.",
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    return parser


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, level),
    )


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    _setup_logging(args.log_level)
    log = logging.getLogger(__name__)

    loader = BacktestConfigLoader(config_dir=args.config_dir)
    bt_cfg = loader.get_backtest_settings()
    ds_cfg = loader.get_data_settings()

    wfa_config = bt_cfg["walk_forward_settings"]
    start = args.start or wfa_config["start_date"]
    end   = args.end   or wfa_config["end_date"]

    collection_cfg = ds_cfg["binance_collection"]
    data_loader = CryptoLoader(config=collection_cfg, project_root=_REPO_ROOT)

    log.info("Loading data for %s ...", args.symbol)
    df = data_loader.load(symbol=args.symbol, start=start, end=end)

    bnh = BuyAndHoldBenchmark(start=start, end=end)
    metrics = bnh.run(df)
    bnh.print_report(metrics, symbol=args.symbol)

    return 0


if __name__ == "__main__":
    sys.exit(main())
