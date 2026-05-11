"""
backtesting.cli.frozen_eval
============================
Registered as ``qr-frozen-eval`` in ``[project.scripts]``.

Frozen-parameter robustness evaluation for PAITS.

Trains the MCMC regime detector on a single fixed training block
(e.g. 2020-01-01 ~ 2020-12-31), freezes all estimated parameters,
then evaluates the system on the subsequent test period
(e.g. 2021-01-01 ~ 2025-12-31) without any re-estimation.

This contrasts with the rolling walk-forward protocol (which
re-estimates params every month) and directly answers:
    "How robust is the knowledge-based detector when market
     conditions shift beyond its estimation horizon?"

Performance degradation relative to rolling WFA provides empirical
justification that periodic Bayesian recalibration is a structural
necessity rather than an optional design choice.

Evaluation pipeline
-------------------
1. Load and preprocess full OHLCV data
2. Slice training block: [train_start, train_end]
   - Apply event tagging (MDRS-SDE)
   - Fit MCMC → single frozen params dict
3. Slice test block: [test_start, test_end]
   - Roll month by month
   - For each month: predict(frozen_params) → run_backtest()
4. Compare against rolling WFA baseline

Usage
-----
::
    qr-frozen-eval \\
        --model       mdrs_sde_btc \\
        --symbol      BTCUSDT \\
        --train-start 2020-01-01 \\
        --train-end   2020-12-31 \\
        --test-start  2021-01-01 \\
        --test-end    2025-12-31 \\
        --config-dir  src/configs \\
        --out         results/frozen_eval.csv

    # Compare multiple freeze points
    qr-frozen-eval \\
        --model       mdrs_sde_btc \\
        --symbol      BTCUSDT \\
        --train-start 2020-01-01 \\
        --train-end   2021-12-31 \\
        --test-start  2022-01-01 \\
        --test-end    2025-12-31 \\
        --config-dir  src/configs \\
        --out         results/frozen_eval_2yr.csv
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


def _compute_metrics(trades: pd.DataFrame) -> dict[str, float]:
    """Compute key performance metrics from completed trades."""
    if trades.empty:
        return {"trades": 0, "ret": 0.0, "sharpe": 0.0,
                "mdd": 0.0, "wr": 0.0}
    equity = (1 + trades["PnL"]).cumprod()
    mdd    = ((equity / equity.cummax()) - 1).min() * 100
    ret    = (equity.iloc[-1] - 1) * 100
    n      = len(trades)
    dur    = (pd.to_datetime(trades["entry_time"]).max() -
              pd.to_datetime(trades["entry_time"]).min()).days
    ann    = np.sqrt(n / max(dur / 365, 1e-9))
    sharpe = (trades["PnL"].mean() / trades["PnL"].std() * ann
              if trades["PnL"].std() > 0 else 0.0)
    wr     = (trades["PnL"] > 0).mean() * 100
    return {"trades": n, "ret": round(ret, 2),
            "sharpe": round(sharpe, 3),
            "mdd": round(mdd, 2), "wr": round(wr, 2)}


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="qr-frozen-eval",
        description=(
            "Frozen-parameter robustness evaluation. "
            "Fits MCMC on a single training block and evaluates on "
            "a subsequent test period without re-estimation."
        ),
    )
    parser.add_argument("--model",       default="mdrs_sde_btc",
                        help="Model key (default: mdrs_sde_btc)")
    parser.add_argument("--symbol",      default="BTCUSDT")
    parser.add_argument("--train-start", required=True,
                        help="Training block start (ISO-8601, e.g. 2020-01-01)")
    parser.add_argument("--train-end",   required=True,
                        help="Training block end   (ISO-8601, e.g. 2020-12-31)")
    parser.add_argument("--test-start",  required=True,
                        help="Test block start (ISO-8601, e.g. 2021-01-01)")
    parser.add_argument("--test-end",    required=True,
                        help="Test block end   (ISO-8601, e.g. 2025-12-31)")
    parser.add_argument("--config-dir",
                        default=str(_REPO_ROOT / "src" / "configs"),
                        help="Path to configs/ directory")
    parser.add_argument("--no-funding",  action="store_true",
                        help="Disable funding rate deduction")
    parser.add_argument("--out",
                        default="results/frozen_eval.csv",
                        help="Output CSV path")
    parser.add_argument("--log-level",   default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # ── imports (require backtesting package) ─────────────────────────
    sys.path.insert(0, str(_REPO_ROOT / "src"))
    try:
        from backtesting.assets.crypto import (
            CryptoLoader, CryptoPreprocessor, FundingRateManager,
        )
        from backtesting.core.config_loader import BacktestConfigLoader
        from backtesting.engines.engine import GenericBacktestEngine
        from backtesting.models.registry import ModelRegistry
        from mdrs_sde.data.dataset_builder import DatasetBuilder
    except ImportError as exc:
        log.error("Cannot import backtesting package: %s", exc)
        log.error("Run from the quant-research repo root with uv run.")
        return 1

    # ── 1. Configuration ──────────────────────────────────────────────
    loader  = BacktestConfigLoader(config_dir=args.config_dir)
    bt_cfg  = loader.get_backtest_settings()
    ds_cfg  = loader.get_data_settings()
    filter_cfg = bt_cfg.get("filters", {}).copy()

    collection_cfg   = ds_cfg["binance_collection"]
    preprocessor_cfg = ds_cfg["event_detection"]

    log.info(
        "Frozen eval: model=%s | symbol=%s | "
        "train=[%s ~ %s] | test=[%s ~ %s]",
        args.model, args.symbol,
        args.train_start, args.train_end,
        args.test_start,  args.test_end,
    )

    # ── 2. Data loading ───────────────────────────────────────────────
    data_loader  = CryptoLoader(config=collection_cfg,
                                project_root=_REPO_ROOT)
    preprocessor = CryptoPreprocessor(settings=preprocessor_cfg)

    log.info("Loading data for %s …", args.symbol)
    raw_data  = data_loader.load(
        symbol=args.symbol,
        start=args.train_start,
        end=args.test_end,
    )
    full_data = preprocessor.run_full_pipeline(raw_data)

    # ── 3. Event tagging ──────────────────────────────────────────────
    events_dir  = _REPO_ROOT / preprocessor_cfg.get(
        "events_directory", "data/events")
    events_path = (events_dir /
                   f"{args.symbol.lower()}_{collection_cfg['interval']}.toml")
    builder = DatasetBuilder(project_root=_REPO_ROOT)
    events  = builder.load_events_from_path(events_path)

    if ModelRegistry.requires_event_tagging(args.model):
        log.info("Applying event tagging …")
        full_data  = builder.apply_event_tagging(full_data, events)
        full_data  = preprocessor.calculate_directional_indicator(full_data)
        train_data = builder.slice_training_data(full_data)
    else:
        full_data  = preprocessor.calculate_directional_indicator(full_data)
        train_data = full_data

    # ── 4. Slice training block ───────────────────────────────────────
    train_slice = train_data.loc[args.train_start:args.train_end].copy()
    log.info("Training block: %d rows (%s ~ %s)",
             len(train_slice),
             train_slice.index.min(), train_slice.index.max())

    if train_slice.empty:
        log.error("Training slice is empty. Check --train-start/--train-end.")
        return 1

    # ── 5. Fit MCMC on training block (single fit) ────────────────────
    model = ModelRegistry.get(args.model, loader)
    if hasattr(model, "_filters"):
        model._filters = filter_cfg.copy()

    log.info("Fitting MCMC on training block [%s ~ %s] …",
             args.train_start, args.train_end)
    fit_result = model.fit(train_slice)

    if not fit_result:
        log.error("MCMC fit returned empty result. Cannot proceed.")
        return 1

    # Extract frozen params (posterior mean estimates)
    frozen_params = fit_result.get("estimates", fit_result)
    log.info("Frozen params: %s",
             {k: f"{v:.4f}" for k, v in frozen_params.items()
              if isinstance(v, (int, float))})

    # ── 6. Construct engine ───────────────────────────────────────────
    funding_mgr = (
        None if args.no_funding
        else FundingRateManager(project_root=_REPO_ROOT)
    )
    engine      = GenericBacktestEngine(
        config=bt_cfg,
        funding_rate_manager=funding_mgr,
        symbol=None if args.no_funding else args.symbol,
    )
    exec_params = engine.get_fixed_params()

    # ── 7. Roll through test months with frozen params ────────────────
    test_start = pd.Timestamp(args.test_start)
    test_end   = pd.Timestamp(args.test_end)

    month_starts = pd.date_range(start=test_start, end=test_end, freq="MS")
    all_trades   = []

    log.info("Evaluating %d test months with frozen params …",
             len(month_starts))

    for ms in month_starts:
        me = min(ms + relativedelta(months=1) - pd.Timedelta(seconds=1),
                 test_end)
        test_slice = full_data.loc[ms:me].copy()

        if test_slice.empty:
            continue

        try:
            signal_df = model.predict(test_slice, frozen_params)
        except Exception as exc:
            log.error("predict() failed for %s: %s",
                      ms.strftime("%Y-%m-%d"), exc)
            continue

        try:
            trades = engine.run_backtest(signal_df, exec_params)
            if not trades.empty:
                trades["month"] = ms.strftime("%Y-%m")
                all_trades.append(trades)
        except Exception as exc:
            log.error("run_backtest() failed for %s: %s",
                      ms.strftime("%Y-%m-%d"), exc)

    # ── 8. Aggregate and report ───────────────────────────────────────
    if all_trades:
        combined = (pd.concat(all_trades)
                    .sort_values("entry_time")
                    .reset_index(drop=True))
    else:
        combined = pd.DataFrame()

    m = _compute_metrics(combined)

    print("\n" + "=" * 65)
    print("FROZEN PARAMETER ROBUSTNESS EVALUATION")
    print("=" * 65)
    print(f"  Training block : {args.train_start} ~ {args.train_end}")
    print(f"  Test block     : {args.test_start} ~ {args.test_end}")
    print(f"  Model          : {args.model}")
    print("-" * 65)
    print(f"  Trades         : {m['trades']}")
    print(f"  Total Return   : {m['ret']:.2f}%")
    print(f"  Sharpe Ratio   : {m['sharpe']:.3f}")
    print(f"  Max Drawdown   : {m['mdd']:.2f}%")
    print(f"  Win Rate       : {m['wr']:.2f}%")
    print("=" * 65)
    print()
    print("Compare against rolling WFA result from qr-backtest")
    print("to quantify the recalibration benefit.")

    # ── 9. Save ───────────────────────────────────────────────────────
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    result_row = {
        "train_start": args.train_start,
        "train_end":   args.train_end,
        "test_start":  args.test_start,
        "test_end":    args.test_end,
        "model":       args.model,
        **{f"frozen_{k}": v for k, v in m.items()},
    }

    if not combined.empty:
        combined.to_csv(args.out.replace(".csv", "_trades.csv"), index=False)
        log.info("Trades saved → %s",
                 args.out.replace(".csv", "_trades.csv"))

    pd.DataFrame([result_row]).to_csv(args.out, index=False)
    log.info("Summary saved → %s", args.out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
