"""
backtesting.cli.validate
=========================
CLI entry point for statistical validation of backtest results.

Registered as ``qr-validate`` in ``[project.scripts]``.

Reads a trades CSV from ``results/`` and runs:
- Bootstrap CI (Sharpe, Total Return, MDD)
- Permutation test (p-value against null Sharpe)
- Subperiod analysis (Bull + COVID Recovery / Crypto Winter / Bull Run)

Usage
-----
::

    qr-validate --result results/mdrs_sde_btc_btcusdt_20260101_trades.csv
    qr-validate --result results/mdrs_sde_btc_btcusdt_20260101_trades.csv --no-subperiods
    qr-validate --result results/mdrs_sde_btc_btcusdt_20260101_trades.csv \\
        --subperiod "Bull 2020,2020-04-01,2021-12-31" \\
        --subperiod "Bear 2022,2022-01-01,2023-12-31"
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys

import pandas as pd

from backtesting.benchmarks.statistical_validator import StatisticalValidator


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qr-validate",
        description="Statistical validation of backtest results",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--result", type=str, required=True,
        help="Path to trades CSV file from results/.",
    )
    parser.add_argument(
        "--n-bootstrap", type=int, default=10_000,
        help="Number of bootstrap resamples.",
    )
    parser.add_argument(
        "--n-permutations", type=int, default=10_000,
        help="Number of permutation test resamples.",
    )
    parser.add_argument(
        "--confidence", type=float, default=0.95,
        help="Bootstrap confidence level (default 0.95 = 95%% CI).",
    )
    parser.add_argument(
        "--no-subperiods", action="store_true",
        help="Skip subperiod analysis.",
    )
    parser.add_argument(
        "--subperiod", type=str, action="append", dest="subperiods",
        metavar="LABEL,START,END",
        help=(
            "Custom subperiod in format 'label,start,end' (ISO-8601 dates). "
            "Can be specified multiple times. Overrides defaults."
        ),
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


def _parse_subperiods(
    raw: list[str] | None,
    ) -> list[tuple[str, str, str]] | None:
    """Parse --subperiod CLI args into list of (label, start, end) tuples."""
    if not raw:
        return None
    result = []
    for item in raw:
        parts = item.split(",")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid --subperiod format: '{item}'. "
                f"Expected 'label,start,end'."
            )
        result.append((parts[0].strip(), parts[1].strip(), parts[2].strip()))

    return result


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    _setup_logging(args.log_level)
    log = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Load trades
    # ------------------------------------------------------------------
    result_path = pathlib.Path(args.result)
    if not result_path.exists():
        log.error("File not found: %s", result_path)
        return 1

    log.info("Loading trades from %s ...", result_path)
    trades = pd.read_csv(result_path)
    trades["exit_time"] = pd.to_datetime(trades["exit_time"])

    if trades.empty or "PnL" not in trades.columns:
        log.error("Trades file is empty or missing 'PnL' column.")
        return 1

    log.info("Loaded %d trades.", len(trades))

    # ------------------------------------------------------------------
    # Bootstrap CI
    # ------------------------------------------------------------------
    log.info("Running Bootstrap CI (n=%d) ...", args.n_bootstrap)
    bootstrap = StatisticalValidator.bootstrap_ci(
        trades,
        n_bootstrap=args.n_bootstrap,
        confidence=args.confidence,
    )

    # ------------------------------------------------------------------
    # Permutation test
    # ------------------------------------------------------------------
    log.info("Running Permutation test (n=%d) ...", args.n_permutations)
    permutation = StatisticalValidator.permutation_test(
        trades,
        n_permutations=args.n_permutations,
    )

    # ------------------------------------------------------------------
    # Subperiod analysis
    # ------------------------------------------------------------------
    if args.no_subperiods:
        subperiods = pd.DataFrame()
    else:
        custom = _parse_subperiods(args.subperiods)
        log.info("Running Subperiod analysis ...")
        subperiods = StatisticalValidator.subperiod_analysis(
            trades,
            subperiods=custom,
        )

    # ------------------------------------------------------------------
    # Print report
    # ------------------------------------------------------------------
    StatisticalValidator.print_report(bootstrap, permutation, subperiods)

    return 0


if __name__ == "__main__":
    sys.exit(main())
