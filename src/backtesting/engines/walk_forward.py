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

PAITS-Event post-processing
---------------------------
When ``[filters.qpb].mode = "event_rate"`` is set, the aggregated
trades are filtered by :class:`WalkForwardEventRateGate`. This applies
a bar-level threshold on the rolling-max of OOS regime probability,
with the threshold recomputed each month by bisection so that the
cooldown-adjusted event rate matches ``target_events_per_month``. See
``_event_rate.py`` for details.

As of v2.0, ``"event_rate"`` is the only supported post-processing
mode. The legacy ``"static"`` and ``"walkforward"`` modes were removed
together with the underlying sticky / ADX / QPB filter chain.
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
from backtesting.models.adapters._event_rate import (
    WalkForwardEventRateConfig,
    WalkForwardEventRateGate,
)

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
    trades: pd.DataFrame = field(default_factory=pd.DataFrame)
    params: dict[str, Any] = field(default_factory=dict)
    signal_df: pd.DataFrame = field(default_factory=pd.DataFrame)


class WalkForwardRunner:
    """Rolling-window backtest orchestrator with pluggable models.

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
                       ``backtest_settings.toml``. Controls filter toggles
                       and the optional WF-QPB post-processing mode.
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

        self._filter_config = filter_config or {}

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
        within the configured date range, aggregates trades, and
        optionally applies WF-QPB post-processing if enabled via
        ``[filters.qpb].mode = "walkforward"``.

        Args:
            signals_path: Path to the ``.pkl`` file produced by
                          :meth:`fit_all`.
            bt_cfg:       Full backtest settings dict. Used to filter
                          windows by ``start_date`` / ``end_date`` from
                          ``bt_cfg["walk_forward_settings"]`` and to
                          configure WF-QPB post-processing.

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

        all_trades, param_summary = self._aggregate(updated)

        # Trade-level filter post-processing (PAITS-Event in v2.0+)
        all_trades = self._apply_filter_postprocessing(all_trades, updated)

        return all_trades, param_summary

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _apply_filter_postprocessing(
        self,
        trades: pd.DataFrame,
        window_results: list[WindowResult | None],
        ) -> pd.DataFrame:
        """Apply trade-level post-processing based on ``[filters.qpb].mode``.

        As of v2.0, the only supported mode is ``"event_rate"``
        (PAITS-Event); the legacy ``"static"`` and ``"walkforward"``
        modes were removed together with the sticky / ADX / QPB
        filter chain. Any other mode value (including the absence of
        the config section) results in a no-op pass-through.

        Args:
            trades: Aggregated completed-trades DataFrame.
            window_results: Per-window results carrying ``signal_df``.

        Returns:
            Filtered (or unchanged) trades DataFrame.
        """
        qpb_cfg = self._filter_config.get("qpb", {}) or {}
        mode = qpb_cfg.get("mode")

        if mode == "event_rate":
            return self._apply_event_rate(trades, window_results)
        if mode is not None and mode != "event_rate":
            logger.warning(
                "Unsupported [filters.qpb].mode=%r in v2.0; "
                "use \"event_rate\" or remove the section. Pass-through.",
                mode,
            )

        return trades

    def _apply_event_rate(
        self,
        trades: pd.DataFrame,
        window_results: list[WindowResult | None],
        ) -> pd.DataFrame:
        """Apply PAITS-Event walk-forward event-rate gating.

        Active when ``[filters.qpb].mode = "event_rate"`` is set in the
        config. Applies a bar-level rolling-max regime probability
        threshold whose value is set by bisection over the previous
        lookback window so that the cooldown-adjusted event rate
        matches ``target_events_per_month``.

        The event-rate gate is applied at the *bar* level (via
        :meth:`WalkForwardEventRateGate.compute_mask`) and then
        intersected with the existing aggregated trades by entry
        timestamp. Because the bar-level mask already enforces cooldown
        spacing, the downstream single-position engine sees a sparse
        stream of admissible entries.

        Args:
            trades: Aggregated trades DataFrame from upstream windows.
            window_results: Per-window results carrying ``signal_df``
                with ``regime_prob`` and ``direction_indicator``.

        Returns:
            Filtered trades DataFrame containing only those whose entry
            bars are admitted by the event-rate gate.
        """
        if trades.empty:
            return trades

        qpb_cfg = self._filter_config.get("qpb", {}) or {}
        er_section = qpb_cfg.get("event_rate", {}) or {}
        cfg = WalkForwardEventRateConfig(
            target_events_per_month=float(
                er_section.get("target_events_per_month", 5.0)
            ),
            cooldown_days=float(er_section.get("cooldown_days", 5.0)),
            lookback_days=int(er_section.get("lookback_days", 90)),
            score_window_bars=int(er_section.get("score_window_bars", 144)),
            refit_freq=str(er_section.get("refit_freq", "ME")),
            fallback_threshold=float(
                er_section.get("fallback_threshold", 0.45)
            ),
            min_lookback_bars=int(er_section.get("min_lookback_bars", 100)),
        )

        logger.info(
            "PAITS-Event gate ACTIVE: target=%.1f/mo cooldown=%.2fd lookback=%dd",
            cfg.target_events_per_month,
            cfg.cooldown_days,
            cfg.lookback_days,
        )

        # Reconstruct full signal frame from window results
        signal_dfs = [
            r.signal_df for r in window_results
            if r is not None and not r.signal_df.empty
        ]
        if not signal_dfs:
            logger.warning("PAITS-Event disabled — no signal DataFrames available")
            return trades

        full_signal = pd.concat(signal_dfs).sort_index()
        full_signal = full_signal[~full_signal.index.duplicated(keep="first")]

        required_cols = {"regime_prob", "direction_indicator"}
        missing = required_cols - set(full_signal.columns)
        if missing:
            logger.error(
                "PAITS-Event disabled — signal frame missing columns: %s",
                missing,
            )
            return trades

        gate = WalkForwardEventRateGate(cfg)
        mask = gate.compute_mask(full_signal)
        admitted_timestamps = full_signal.index[mask]

        if "entry_time" not in trades.columns:
            logger.error(
                "PAITS-Event disabled — trades frame lacks 'entry_time' column"
            )
            return trades

        admitted_set = set(admitted_timestamps)
        kept_mask = trades["entry_time"].isin(admitted_set)
        filtered = trades[kept_mask].reset_index(drop=True)

        logger.info(
            "PAITS-Event: %d / %d trades admitted (%.1f%%)",
            len(filtered),
            len(trades),
            100.0 * len(filtered) / max(len(trades), 1),
        )

        return filtered

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

        train_start = test_start - relativedelta(months=self._train_months)
        test_end    = test_start + relativedelta(months=self._test_months)

        train_slice = train_data[(train_data.index >= train_start) & (train_data.index < test_start)]
        test_slice  = full_data[(full_data.index >= test_start) & (full_data.index < test_end)]

        if train_slice.empty or test_slice.empty:
            logger.warning("Window %s: empty train or test slice.", label)
            return None

        try:
            fit_result = self._model.fit(train_slice)
        except Exception as exc:  # noqa: BLE001
            logger.error("fit() failed for window %s: %s", label, exc)
            return None

        mean_params = (
            fit_result.get("estimates", fit_result)
            if isinstance(fit_result, dict) else {}
        )
        try:
            signal_df = self._model.predict(test_slice, mean_params)
        except Exception as exc:  # noqa: BLE001
            logger.error("predict() failed for window %s: %s", label, exc)
            return None

        # Strip filter-dependent columns so backtest_from_signals() can
        # re-apply filters without stale cached state.
        return WindowResult(
            window_label=label,
            trades=pd.DataFrame(),   # intentionally empty — backtest not run yet
            params=mean_params,
            signal_df=signal_df,
        )

    def _aggregate(
        self,
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
