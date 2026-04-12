"""
backtesting.engines.walk_forward
=================================
Walk-forward analysis (WFA) orchestrator.

Slices the full dataset into rolling train / test windows, calls
``model.fit`` -> ``model.predict`` -> ``engine.run_backtest`` for each
window (optionally in parallel via ``joblib``), and aggregates the
per-window results into a single trades ``DataFrame`` and a summary of
model parameters.

Walk-forward schedule
---------------------
Given a ``start_date``, an ``end_date``, ``training_months``, and
``testing_months`` (all from ``configs/backtest_settings.toml``):

* Training window : ``[test_start - training_months, test_start)``
* Testing window  : ``[test_start, test_start + testing_months)``
* Windows advance by one calendar month (``freq="MS"``).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from dateutil.relativedelta import relativedelta
from joblib import Parallel, delayed

from backtesting.core.base_model import BaseModel
from backtesting.engines.engine import GenericBacktestEngine

logger = logging.getLogger(__name__)


@dataclass
class WindowResult:
    """Container for a single walk-forward window's output.

    Attributes:
        window_label: ISO date string identifying the test-window start.
        trades:       Completed trades ``DataFrame`` (may be empty).
        params:       Estimated model parameters for this window.
        signal_df:    Subset of test data with ``signal`` and ``confidence``
                      columns; used for IC analysis.
    """
    window_label: str
    trades: pd.DataFrame
    params: dict[str, Any]
    signal_df: pd.DataFrame = field(default_factory=pd.DataFrame)


class WalkForwardRunner:
    """Orchestrates a walk-forward backtest for a given model and symbol.

    Args:
        model:         Concrete :class:`~backtesting.core.base_model.BaseModel`
                       instance — already constructed with its config.
        engine:        :class:`~backtesting.engines.engine.GenericBacktestEngine`
                       instance — already constructed with backtest config.
        wfa_config:    Parsed ``[walk_forward_settings]`` section from
                       ``backtest_settings.toml``. Required keys:
                       ``start_date``, ``end_date``, ``training_months``,
                       ``testing_months``, ``parallel_jobs``.
        filter_config: Parsed ``[filters]`` section from
                       ``backtest_settings.toml``. Controls filter toggles.
                       Defaults to empty dict.

    Example::

        loader  = BacktestConfigLoader()
        bt_cfg  = loader.get_backtest_settings()
        model   = ModelRegistry.get("mdrs_sde_btc", loader)
        engine  = GenericBacktestEngine(bt_cfg)
        runner  = WalkForwardRunner(
            model, engine,
            wfa_config=bt_cfg["walk_forward_settings"],
            filter_config=bt_cfg["filters"],
        )
        result  = runner.run(preprocessed_data, train_data)
    """
    def __init__(
        self,
        model: BaseModel,
        engine: GenericBacktestEngine,
        wfa_config: dict[str, Any],
        filter_config: dict[str, Any] | None = None,
        ) -> None:
        self._model                   = model
        self._engine                  = engine
        self._start                   = pd.Timestamp(wfa_config["start_date"])
        self._end                     = pd.Timestamp(wfa_config["end_date"])
        self._train_months            = wfa_config.get("training_months", 3)
        self._test_months             = wfa_config.get("testing_months", 1)
        self._n_jobs                  = wfa_config.get("parallel_jobs", 1)

        filters = filter_config or {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fit_all(
        self,
        full_data: pd.DataFrame,
        train_data: pd.DataFrame,
        signals_path: str | Path,
        ) -> Path:
        """Run fit() + predict() for all windows and persist WindowResults.

        Executes the expensive MCMC fit and signal generation for every
        walk-forward window, then saves the resulting :class:`WindowResult`
        list to *signals_path* as a pickle file.

        This is the first step of the standard two-phase workflow:

        1. ``fit_all()``             — fit + predict, persist signals
        2. ``backtest_from_signals()`` — backtest only, repeat freely

        The persisted ``.pkl`` file contains the full signal DataFrame
        per window (OHLCV + signal + confidence + all feature columns),
        enabling backtest replay without re-running MCMC.

        Args:
            full_data:    Full OHLCV + feature DataFrame (used for
                          out-of-sample slicing).
            train_data:   Training-eligible rows (pre-filtered by
                          ``DatasetBuilder.slice_training_data()``).
            signals_path: Destination path for the ``.pkl`` file.

        Returns:
            Resolved :class:`~pathlib.Path` of the saved pickle file.
        """
        if full_data.index.tz is not None:
            full_data = full_data.copy()
            full_data.index = full_data.index.tz_localize(None)
        if train_data.index.tz is not None:
            train_data = train_data.copy()
            train_data.index = train_data.index.tz_localize(None)

        test_starts = pd.date_range(
            start=self._start, end=self._end, freq="MS"
        )
        logger.info(
            "fit_all: %d windows | model=%s",
            len(test_starts),
            type(self._model).__name__,
        )

        window_results = Parallel(n_jobs=self._n_jobs)(
            delayed(self._fit_window)(test_start, train_data, full_data)
            for test_start in test_starts
        )

        signals_path = Path(signals_path)
        signals_path.parent.mkdir(parents=True, exist_ok=True)
        with open(signals_path, "wb") as fh:
            pickle.dump(window_results, fh)

        n_ok = sum(1 for r in window_results if r is not None)
        logger.info("fit_all: saved %d/%d windows → %s", n_ok, len(test_starts), signals_path)

        return signals_path

    def _fit_window(
        self,
        test_start: pd.Timestamp,
        train_data: pd.DataFrame,
        full_data: pd.DataFrame,
        ) -> WindowResult | None:
        """Run fit() + predict() only — no backtest.

        Same as :meth:`_process_window` but skips ``run_backtest()``.
        Used by :meth:`fit_all` to persist signals for later reuse.

        Args:
            test_start: First timestamp of the out-of-sample period.
            train_data: Full training-eligible dataset (pre-filtered).
            full_data:  Full dataset for the out-of-sample slice.

        Returns:
            :class:`WindowResult` with empty ``trades`` on success,
            ``None`` on irrecoverable error.
        """
        label = test_start.strftime("%Y-%m-%d")

        train_end   = test_start - pd.Timedelta(seconds=1)
        train_start = train_end  - relativedelta(months=self._train_months)
        test_end    = min(
            test_start + relativedelta(months=self._test_months)
            - pd.Timedelta(seconds=1),
            self._end,
        )

        train_slice = train_data.loc[train_start:train_end].copy()
        test_slice  = full_data.loc[test_start:test_end].copy()

        if len(train_slice) < 80:
            logger.warning(
                "Window %s skipped — insufficient training rows (%d).",
                label,
                len(train_slice),
            )
            return None

        # Fit
        try:
            training_results = self._model.fit(train_slice)
            param_summary = training_results['summary']
            mean_params = training_results['estimates']
        except Exception as exc:  # noqa: BLE001
            logger.error("fit() failed for window %s: %s", label, exc)
            return None

        if param_summary.empty:
            logger.warning("Window %s: fit() returned empty params.", label)
            return None

        # Predict
        try:
            signal_df = self._model.predict(test_slice, mean_params)
        except Exception as exc:  # noqa: BLE001
            logger.error("predict() failed for window %s: %s", label, exc)
            return None

        return WindowResult(
            window_label=label,
            trades=pd.DataFrame(),   # intentionally empty — backtest not run
            params=param_summary,
            signal_df=signal_df,
        )

    @staticmethod
    def _aggregate(
        results: list[WindowResult | None],
        ) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
        """Concatenate per-window results into analysis-ready outputs.

        Args:
            results: List of :class:`WindowResult` (``None`` entries are
                     silently skipped).

        Returns:
            ``(all_trades_df, param_summary_dict)``
        """
        trade_frames: list[pd.DataFrame] = []
        param_summary: dict[str, pd.DataFrame] = {}

        for res in results:
            if res is None:
                continue
            if not res.trades.empty:
                trade_frames.append(res.trades)
            param_summary[res.window_label] = res.params

        if not trade_frames:
            return pd.DataFrame(), param_summary

        all_trades = (
            pd.concat(trade_frames)
            .sort_values("entry_time")
            .reset_index(drop=True)
        )

        return all_trades, param_summary
