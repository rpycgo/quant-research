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
import pickle
from dataclasses import dataclass, field
from pathlib import Path
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
        signal_df:    Full test-window DataFrame including ``signal``,
                      ``confidence``, OHLCV and all feature columns.
                      Stored in full so that ``backtest_from_signals()``
                      can re-run ``run_backtest()`` without re-fitting.
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
        signals_path = runner.fit_all(preprocessed_data, train_data, "results/signals.pkl")
        trades, params = runner.backtest_from_signals(signals_path, bt_cfg)
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
        self._reference_window_months = wfa_config.get("reference_window_months", 12)

        filters = filter_config or {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fit_all(
        self,
        full_data: pd.DataFrame,
        train_data: pd.DataFrame | None,
        signals_path: str | Path,
        ) -> Path:
        """Run fit() + predict() for all windows and persist WindowResults.

        Executes MCMC fit and signal generation for every walk-forward
        window, then saves the resulting list of :class:`WindowResult`
        to *signals_path* as a pickle file. The saved file can be passed
        to :meth:`backtest_from_signals` to re-run the backtest step in
        isolation — without repeating fit/predict.

        This is the first step of the standard two-phase workflow:

        1. ``fit_all()``               — fit + predict, persist signals
        2. ``backtest_from_signals()`` — backtest only, repeat freely

        Args:
            full_data:    Full OHLCV + feature DataFrame (out-of-sample slicing).
            train_data:   Training-eligible rows. Falls back to *full_data*
                          when ``None``.
            signals_path: Destination path for the ``.pkl`` file.

        Returns:
            Resolved :class:`~pathlib.Path` of the saved pickle file.
        """
        if train_data is None or train_data.empty:
            train_data = full_data

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

        window_results: list[WindowResult | None] = Parallel(n_jobs=self._n_jobs)(
            delayed(self._fit_window)(test_start, train_data, full_data)
            for test_start in test_starts
        )

        signals_path = Path(signals_path)
        signals_path.parent.mkdir(parents=True, exist_ok=True)
        with open(signals_path, "wb") as fh:
            pickle.dump(window_results, fh)

        n_ok = sum(1 for r in window_results if r is not None)
        logger.info(
            "fit_all: saved %d/%d windows → %s",
            n_ok, len(test_starts), signals_path,
        )
        return signals_path

    def backtest_from_signals(
        self,
        signals_path: str | Path,
        bt_cfg: dict[str, Any],
        ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Re-run backtest from persisted WindowResults (no re-fitting).

        Loads the pickle file produced by :meth:`fit_all`, calls
        ``get_fixed_params()`` and ``run_backtest()`` for every window
        within the configured date range, and aggregates trades. MCMC
        sampling and signal generation are skipped entirely.

        Args:
            signals_path: Path to the ``.pkl`` file produced by
                          :meth:`fit_all`.
            bt_cfg:       Full backtest settings dict. Used to filter
                          windows by ``start_date`` / ``end_date`` from
                          ``bt_cfg["walk_forward_settings"]``.

        Returns:
            ``(all_trades, param_summary)``
        """
        wf_settings = bt_cfg["walk_forward_settings"]

        signals_path = Path(signals_path)
        with open(signals_path, "rb") as fh:
            window_results: list[WindowResult | None] = pickle.load(fh)

        logger.info(
            "backtest_from_signals: loaded %d windows from %s",
            len(window_results),
            signals_path,
        )
        logger.info(
            "backtesting dates: %s ~ %s",
            wf_settings["start_date"],
            wf_settings["end_date"],
        )

        exec_params = self._engine.get_fixed_params()

        updated: list[WindowResult | None] = []
        for res in window_results:
            if res is None:
                updated.append(None)
                continue
            if not (wf_settings["start_date"] <= res.window_label <= wf_settings["end_date"]):
                continue
            # Re-generate signals with current filter settings (supports ablation).
            # predict() re-applies sticky/ADX filters using self._model._filters
            # which reflects CLI --no-sticky / --no-adx overrides.
            try:
                mean_params = res.params if isinstance(res.params, dict) else {}
                signal_df   = self._model.predict(res.signal_df, mean_params)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "predict() failed for window %s: %s",
                    res.window_label, exc,
                )
                signal_df = res.signal_df

            try:
                trades = self._engine.run_backtest(signal_df, exec_params)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "run_backtest() failed for window %s: %s",
                    res.window_label, exc,
                )
                trades = pd.DataFrame()
            from dataclasses import replace
            updated.append(replace(res, trades=trades))

        return self._aggregate(updated)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fit_window(
        self,
        test_start: pd.Timestamp,
        train_data: pd.DataFrame,
        full_data: pd.DataFrame,
        ) -> WindowResult | None:
        """Run fit() + predict() only — no backtest.

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
        except Exception as exc:  # noqa: BLE001
            logger.error("fit() failed for window %s: %s", label, exc)
            return None

        if not training_results:
            logger.warning("Window %s: fit() returned empty params.", label)
            return None

        # MDRS-SDE: {'summary': DataFrame, 'estimates': dict, 'trace': ...}
        # All other models: wrap_fit_result → same structure
        param_summary = training_results.get("summary", pd.DataFrame())
        mean_params   = training_results.get("estimates", training_results)

        # MCMC models (MDRS-SDE): summary is a non-empty DataFrame
        # Non-MCMC models (DL, GARCH, etc.): summary is intentionally empty
        # Only fail if summary is empty AND estimates is also empty/missing
        if isinstance(param_summary, pd.DataFrame) and param_summary.empty:
            estimates = training_results.get("estimates", {})
            if not estimates:
                logger.warning("Window %s: fit() returned empty results.", label)
                return None

        # Predict
        try:
            signal_df = self._model.predict(test_slice, mean_params)
        except Exception as exc:  # noqa: BLE001
            logger.error("predict() failed for window %s: %s", label, exc)
            return None

        # Strip filter-dependent columns so backtest_from_signals()
        # can re-apply filters with different settings (ablation support).
        signal_df = signal_df.drop(columns=["signal", "confidence"], errors="ignore")

        return WindowResult(
            window_label=label,
            trades=pd.DataFrame(),   # intentionally empty — backtest not run yet
            params=param_summary,
            signal_df=signal_df,
        )

    @staticmethod
    def _aggregate(
        results: list[WindowResult | None],
        ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
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
