"""
backtesting.engines.walk_forward
=================================
Walk-forward analysis (WFA) orchestrator.

Slices the full dataset into rolling train / test windows, calls
``model.fit`` → ``model.predict`` → ``engine.run_backtest`` for each
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

from src.backtesting.core.base_model import BaseModel
from src.backtesting.engines.engine import GenericBacktestEngine

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
                       ``backtest_settings.toml``.  Required keys:
                       ``start_date``, ``end_date``, ``training_months``,
                       ``testing_months``, ``parallel_jobs``.

    Example::

        from backtesting.core.config_loader import BacktestConfigLoader
        from backtesting.engines import GenericBacktestEngine, WalkForwardRunner
        from backtesting.models.registry import ModelRegistry

        loader  = BacktestConfigLoader()
        bt_cfg  = loader.get_backtest_settings()
        model   = ModelRegistry.get("mdrs_sde_btc", loader)
        engine  = GenericBacktestEngine(bt_cfg)
        runner  = WalkForwardRunner(model, engine, bt_cfg["walk_forward_settings"])

        result  = runner.run(preprocessed_data, train_data)
    """
    def __init__(
        self,
        model: BaseModel,
        engine: GenericBacktestEngine,
        wfa_config: dict[str, Any],
    ) -> None:
        self._model = model
        self._engine = engine
        self._start = pd.Timestamp(wfa_config["start_date"])
        self._end = pd.Timestamp(wfa_config["end_date"])
        self._train_months = wfa_config.get("training_months", 3)
        self._test_months = wfa_config.get("testing_months", 1)
        self._n_jobs = wfa_config.get("parallel_jobs", 1)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(
        self,
        full_data: pd.DataFrame,
        train_data: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
        """Execute the full walk-forward analysis.

        Args:
            full_data:  Complete preprocessed ``DataFrame`` used for the
                        testing windows.
            train_data: Optional separate training-eligible subset (e.g.
                        in-zone rows only for the SDE model).  Falls back to
                        *full_data* when ``None``.

        Returns:
            A 2-tuple:

            * ``all_trades``   — concatenated trades ``DataFrame`` sorted by
              ``entry_time``.
            * ``param_summary`` — dict mapping window labels to their
              estimated model parameter dicts.
        """
        if train_data is None or train_data.empty:
            train_data = full_data

        if train_data.index.tz is not None:
            train_data.index = train_data.index.tz_localize(None)

        test_starts = pd.date_range(
            start=self._start, end=self._end, freq="MS"
        )
        total = len(test_starts)
        logger.info(
            "Starting walk-forward analysis: %d windows | model=%s",
            total,
            type(self._model).__name__,
        )

        window_results = Parallel(
            n_jobs=self._n_jobs
        )(
            delayed(self._process_window)(ts, train_data, full_data)
            for ts in test_starts
        )

        return self._aggregate(window_results)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
    def _process_window(
        self,
        test_start: pd.Timestamp,
        train_data: pd.DataFrame,
        full_data: pd.DataFrame,
    ) -> WindowResult | None:
        """Execute one walk-forward window.

        Args:
            test_start: First timestamp of the out-of-sample period.
            train_data: Full training-eligible dataset (pre-filtered).
            full_data:  Full dataset for the out-of-sample slice.

        Returns:
            :class:`WindowResult` on success, ``None`` on irrecoverable
            error.
        """
        label = test_start.strftime("%Y-%m-%d")

        # Slice train window
        train_end = test_start - pd.Timedelta(seconds=1)
        train_start = train_end - relativedelta(months=self._train_months)
        test_end = min(
            test_start + relativedelta(months=self._test_months)
            - pd.Timedelta(seconds=1),
            self._end,
        )

        train_slice = train_data.loc[train_start:train_end].copy()
        test_slice = full_data.loc[test_start:test_end].copy()

        if len(train_slice) < 80:
            logger.warning(
                "Window %s skipped — insufficient training rows (%d).",
                label,
                len(train_slice),
            )
            return None

        # Fit → predict → run backtest
        try:
            params = self._model.fit(train_slice)
        except Exception as exc:  # noqa: BLE001
            logger.error("fit() failed for window %s: %s", label, exc)
            return None

        if not params:
            logger.warning("Window %s: fit() returned empty params.", label)
            return None

        try:
            signal_df = self._model.predict(test_slice, params)
        except Exception as exc:  # noqa: BLE001
            logger.error("predict() failed for window %s: %s", label, exc)
            return None

        dynamic_params = self._engine.build_dynamic_params(params)

        try:
            trades = self._engine.run_backtest(signal_df, dynamic_params)
        except Exception as exc:  # noqa: BLE001
            logger.error("run_backtest() failed for window %s: %s", label, exc)
            return None

        signal_cols = [c for c in ("signal", "confidence", "Close") if c in signal_df]
        return WindowResult(
            window_label=label,
            trades=trades,
            params=params,
            signal_df=signal_df[signal_cols],
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
        param_summary: dict[str, dict[str, Any]] = {}

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
