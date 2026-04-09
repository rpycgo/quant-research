"""
data.cli.funding_rate_collect
==============================
CLI entry point for historical funding rate data collection from Binance.

Registered as ``qr-collect-funding`` in ``[project.scripts]``.

Reads supported_symbols and date range from data_settings.toml by default.
Downloads monthly ZIP archives via FundingRateLoader, merges into a single
CSV per symbol, and removes all intermediate files.

Output: ``data/crypto/binance/funding_rate/<symbol_lower>.csv``

Usage
-----
::

    qr-collect-funding
    qr-collect-funding --symbol BTCUSDT
    qr-collect-funding --symbol ETHUSDT --start 2021-01-01 --end 2025-12-31
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys
from datetime import datetime

from backtesting.core.config_loader import BacktestConfigLoader
from data.funding_rate.loader import FundingRateLoader

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qr-collect-funding",
        description="Binance Futures funding rate historical data collector",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbol", type=str, default=None,
        help="Trading pair symbol, e.g. BTCUSDT. "
             "Defaults to all supported_symbols in data_settings.toml.",
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="Collection start date (ISO-8601). "
             "Defaults to analysis_start_date in config.",
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="Collection end date (ISO-8601). "
             "Defaults to analysis_end_date in config.",
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
    args   = parser.parse_args()
    _setup_logging(args.log_level)
    log = logging.getLogger(__name__)

    config_loader  = BacktestConfigLoader(config_dir=args.config_dir)
    ds_cfg         = config_loader.get_data_settings()
    event_cfg      = ds_cfg["event_detection"]
    collection_cfg = ds_cfg["binance_collection"]

    symbols = (
        [args.symbol.upper()]
        if args.symbol
        else collection_cfg["supported_symbols"]
    )

    start_str = args.start or event_cfg["analysis_start_date"]
    end_str   = args.end   or event_cfg["analysis_end_date"]
    start     = datetime.strptime(start_str, "%Y-%m-%d")
    end       = datetime.strptime(end_str,   "%Y-%m-%d")

    loader  = FundingRateLoader(config=collection_cfg, project_root=_REPO_ROOT)
    results: dict[str, bool] = {}

    for symbol in symbols:
        log.info("--- %s ---", symbol)
        path = loader.collect(symbol, start=start, end=end)
        results[symbol] = path is not None

    succeeded = [s for s, ok in results.items() if ok]
    failed    = [s for s, ok in results.items() if not ok]

    log.info("Collection complete — succeeded: %s", succeeded or "none")
    if failed:
        log.warning("Failed: %s", failed)

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
