"""
data.cli.collect
=================
CLI entry point for historical OHLCV data collection from Binance Futures.

Registered as ``qr-collect`` in ``[project.scripts]``.

Saves to data/crypto/binance/futures/<symbol_lower>_<interval>.csv.

Usage
-----
::

    qr-collect --symbol BTCUSDT --start 2024-01-01 --end 2026-01-31
    qr-collect --symbol ETHUSDT --interval 1m --start 2024-01-01
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys

from backtesting.core.config_loader import BacktestConfigLoader
from backtesting.assets.crypto import CryptoLoader

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qr-collect",
        description="Binance Futures OHLCV historical data collector",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbol", type=str, required=True,
                        help="Trading pair symbol, e.g. BTCUSDT.")
    parser.add_argument("--start", type=str, required=True,
                        help="Collection start date (ISO-8601).")
    parser.add_argument("--end", type=str, default=None,
                        help="Collection end date (ISO-8601). Defaults to today.")
    parser.add_argument("--interval", type=str, default=None,
                        help="Candlestick interval, e.g. 5m, 1h. Overrides config.")
    parser.add_argument("--config-dir", type=str,
                        default=str(_REPO_ROOT / "src" / "configs"),
                        help="Path to configs/ directory.")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

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

    config_loader = BacktestConfigLoader(config_dir=args.config_dir)
    ds_cfg = config_loader.get_data_settings()
    collection_cfg = ds_cfg["binance_collection"]

    if args.interval:
        collection_cfg["interval"] = args.interval

    loader = CryptoLoader(config=collection_cfg, project_root=_REPO_ROOT)

    log.info(
        "Collecting %s %s from %s to %s ...",
        args.symbol,
        collection_cfg["interval"],
        args.start,
        args.end or "today",
    )

    df = loader.fetch(symbol=args.symbol, start=args.start, end=args.end)

    if df.empty:
        log.error("No data returned for %s.", args.symbol)
        return 1

    save_path = loader.save(df, symbol=args.symbol)
    log.info("Done — %d rows saved -> %s", len(df), save_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
