"""
backtesting.cli.backtest
========================
Installable CLI entry point for the quant-research backtesting module.

Registered as ``qr-backtest`` in ``[project.scripts]``.

Orchestrates the full pipeline:

1. Load and preprocess OHLCV data via CryptoLoader + CryptoPreprocessor.
2. Construct the model adapter via ModelRegistry.
3. Run walk-forward analysis via WalkForwardRunner.
4. Compute and print performance metrics via PerformanceAnalyzer.
5. Persist trade results and parameter summaries to ``results/``.

Event tagging is only applied when the model requires it (controlled by
``ModelRegistry.requires_event_tagging()``). Rule-based and statistical
benchmark models skip this step and use full_data as train_data directly.

Usage
-----
::

    qr-backtest --model mdrs_sde_btc --symbol BTCUSDT

    qr-backtest --model garch_btc --symbol BTCUSDT \\
        --start 2024-01-01 --end 2026-01-31

    qr-backtest --model simple_breakout_btc --symbol BTCUSDT
    qr-backtest --model ma_crossover_btc --symbol BTCUSDT
    qr-backtest --model rsi_btc --symbol BTCUSDT
    qr-backtest --model hmm_btc --symbol BTCUSDT

    # Ablation flags
    qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-ema-sigma
    qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-sticky
    qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-adx
    qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-ema-sigma --no-sticky --no-adx

    # List all registered models
    qr-backtest --list-models
"""
from __future__ import annotations

import argparse
import pickle
import logging
import pathlib
import sys
from datetime import datetime
from typing import Any

from pathlib import Path
import pandas as pd

from backtesting.assets.crypto import CryptoLoader, CryptoPreprocessor, FundingRateManager
from backtesting.core.config_loader import BacktestConfigLoader
from backtesting.engines import (
    GenericBacktestEngine,
    PerformanceAnalyzer,
    WalkForwardRunner,
)
from backtesting.models.registry import ModelRegistry
from mdrs_sde.data.dataset_builder import DatasetBuilder

# Repository root: src/backtesting/cli/backtest.py → 4 levels up
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qr-backtest",
        description="quant-research walk-forward backtesting runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Model key, e.g. mdrs_sde_btc or garch_btc.",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="BTCUSDT",
        help="Trading pair symbol.",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Walk-forward start date (ISO-8601). Overrides config.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="Walk-forward end date (ISO-8601). Overrides config.",
    )
    parser.add_argument(
        "--config-dir",
        type=str,
        default=str(_REPO_ROOT / "src" / "configs"),
        help="Path to configs/ directory.",
    )
    parser.add_argument(
        "--list-models",
        action="store_true",
        help="Print all registered model keys and exit.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Python logging level.",
    )
    # ------------------------------------------------------------------
    # Ablation flags
    # ------------------------------------------------------------------
    parser.add_argument(
        "--no-sticky",
        action="store_true",
        help="Disable sticky filter.",
    )
    parser.add_argument(
        "--no-adx",
        action="store_true",
        help="Disable ADX gate.",
    )
    parser.add_argument(
        "--no-funding",
        action="store_true",
        help="Disable funding rate deduction from PnL.",
    )
    parser.add_argument(
        "--fit-only",
        action="store_true",
        help=(
            "Run fit() + predict() for all windows and save signals to "
            "results/<stem>_signals.pkl. Skip backtest execution. "
            "Use --from-signals to run backtest later."
        ),
    )
    parser.add_argument(
        "--from-signals",
        metavar="SIGNALS_PKL",
        default=None,
        help=(
            "Path to a signals .pkl file produced by --fit-only. "
            "Skips MCMC and runs backtest only using persisted signals."
        ),
    )

    return parser


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, level),
    )


def _override_wfa_dates(
    wfa_config: dict[str, Any],
    start: str | None,
    end: str | None,
    ) -> dict[str, Any]:
    cfg = wfa_config.copy()
    if start:
        cfg["start_date"] = start
    if end:
        cfg["end_date"] = end
    return cfg


def _save_results(
    args: argparse.Namespace,
    trades: pd.DataFrame,
    param_summary: dict[str, pd.DataFrame],
    model_key: str,
    symbol: str,
    ) -> None:
    results_dir = _REPO_ROOT / "results"
    results_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = f"{model_key}_{symbol.lower()}_{timestamp}"

    trades_path = results_dir / f"{stem}_trades.csv"
    params_path = results_dir / f"{stem}_params.pkl"

    if not trades.empty:
        trades.to_csv(trades_path, index=False)
        logging.getLogger(__name__).info("Trades saved → %s", trades_path)

    if not args.from_signals:
        with open(params_path, "wb") as fh:
            pickle.dump(param_summary, fh)
        logging.getLogger(__name__).info("Params saved → %s", params_path)


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    _setup_logging(args.log_level)
    log = logging.getLogger(__name__)

    if args.list_models:
        print("Registered models:")
        for key in ModelRegistry.available():
            print(f"  {key}")
        return 0

    if args.model is None:
        parser.error("--model is required (or use --list-models to see options).")

    # ------------------------------------------------------------------
    # 1. Configuration
    # ------------------------------------------------------------------
    loader = BacktestConfigLoader(config_dir=args.config_dir)
    bt_cfg = loader.get_backtest_settings()
    ds_cfg = loader.get_data_settings()

    wfa_config = _override_wfa_dates(
        bt_cfg["walk_forward_settings"],
        args.start,
        args.end,
    )

    # Ablation: override filter_config from CLI flags
    filter_cfg = bt_cfg.get("filters", {}).copy()
    if args.no_sticky:
        filter_cfg["use_sticky"] = False
    if args.no_adx:
        filter_cfg["use_adx"] = False

    log.info(
        "Run: model=%s | symbol=%s | %s → %s | filters=%s",
        args.model,
        args.symbol,
        wfa_config["start_date"],
        wfa_config["end_date"],
        filter_cfg,
    )

    # ------------------------------------------------------------------
    # 2. Data loading and preprocessing
    # ------------------------------------------------------------------
    collection_cfg   = ds_cfg["binance_collection"]
    preprocessor_cfg = ds_cfg["event_detection"]

    data_loader = CryptoLoader(
        config=collection_cfg,
        project_root=_REPO_ROOT,
    )
    preprocessor = CryptoPreprocessor(settings=preprocessor_cfg)

    log.info("Loading data for %s …", args.symbol)
    raw_data = data_loader.load(
        symbol=args.symbol,
        start=preprocessor_cfg["analysis_start_date"],
        end=wfa_config["end_date"],
    )

    log.info("Preprocessing …")
    full_data = preprocessor.run_full_pipeline(raw_data)

    # ------------------------------------------------------------------
    # 3. Event tagging (MDRS-SDE only)
    # ------------------------------------------------------------------
    # Load events for all models — used by WalkForwardRunner for event sigma
    events_dir  = _REPO_ROOT / preprocessor_cfg.get("events_directory", "data/events")
    events_path = events_dir / f"{args.symbol.lower()}_{collection_cfg['interval']}.toml"
    builder     = DatasetBuilder(project_root=_REPO_ROOT)
    events      = builder.load_events_from_path(events_path)

    if ModelRegistry.requires_event_tagging(args.model):
        log.info("Applying event tagging for %s ...", args.model)
        full_data   = builder.apply_event_tagging(full_data, events)
        full_data   = preprocessor.calculate_directional_indicator(full_data)
        train_data  = builder.slice_training_data(full_data)
        log.info("Event tagging complete — %d training rows.", len(train_data))
    else:
        log.info("Skipping event tagging for %s.", args.model)
        full_data  = preprocessor.calculate_directional_indicator(full_data)
        train_data = full_data

    # ------------------------------------------------------------------
    # 4. Model and engine construction
    # ------------------------------------------------------------------
    model  = ModelRegistry.get(args.model, loader)
    # Apply CLI filter overrides directly to model so predict() honours
    # --no-sticky / --no-adx in both fit_all and --from-signals modes
    if hasattr(model, "_filters"):
        model._filters = filter_cfg.copy()

    funding_mgr = (
        None
        if args.no_funding
        else FundingRateManager(project_root=_REPO_ROOT)
    )
    if args.no_funding:
        log.info("Funding rate deduction disabled (--no-funding).")
    else:
        log.info("Funding rate deduction enabled.")

    engine = GenericBacktestEngine(
        config=bt_cfg,
        funding_rate_manager=funding_mgr,
        symbol=None if args.no_funding else args.symbol,
    )
    runner = WalkForwardRunner(
        model=model,
        engine=engine,
        wfa_config=wfa_config,
        filter_config=filter_cfg,
    )

    # ------------------------------------------------------------------
    # 5. Walk-forward analysis
    # ------------------------------------------------------------------
    log.info("Starting walk-forward analysis ...")
    stem = f"{args.model}_{args.symbol.lower()}_{pd.Timestamp.now():%Y%m%d_%H%M%S}"
    signals_path = _REPO_ROOT / "results" / f"{stem}_signals.pkl"

    if args.from_signals:
        # ── backtest-only mode ──────────────────────────────────────
        signals_path = Path(args.from_signals)
        if not signals_path.exists():
            log.error("Signals file not found: %s", signals_path)
            return 1

        log.info("Loading signals from %s ...", signals_path)
        all_trades, param_summary = runner.backtest_from_signals(signals_path, bt_cfg)

    elif args.fit_only:
        # ── fit-only mode (no backtest) ─────────────────────────────
        runner.fit_all(full_data, train_data, signals_path)
        log.info("Signals saved → %s", signals_path)
        log.info("Run backtest with:")
        log.info("  qr-backtest --model %s --symbol %s --from-signals %s",
                 args.model, args.symbol, signals_path)

        return 0

    else:
        # ── standard mode: fit + signals saved + backtest ───────────
        runner.fit_all(full_data, train_data, signals_path)
        log.info("Signals saved → %s", signals_path)
        all_trades, param_summary = runner.backtest_from_signals(signals_path, bt_cfg)

    if all_trades.empty:
        log.warning("No trades were generated. Check signals and data coverage.")
        return 1

    # ------------------------------------------------------------------
    # 6. Performance report
    # ------------------------------------------------------------------
    metrics, _equity, _drawdown = PerformanceAnalyzer.calculate_metrics(all_trades)
    PerformanceAnalyzer.print_report(metrics)

    signal_col = "confidence" if "confidence" in all_trades.columns else "signal"
    if signal_col in full_data.columns:
        ic_df = PerformanceAnalyzer.calculate_ic(full_data, signal_col)
        print("\nIC Analysis:")
        print(ic_df.to_string())

    # ------------------------------------------------------------------
    # 7. Persist results
    # ------------------------------------------------------------------
    _save_results(args, all_trades, param_summary, args.model, args.symbol)

    log.info("Done.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
