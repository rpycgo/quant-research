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

PAITS-Event pre-execution signal gate
---------------------------------------
When ``[filters.qpb].mode = "event_rate"`` is set,
:class:`WalkForwardEventRateGate` is applied **before**
``engine.run_backtest``. The gate computes admissible entry bars from
the rolling-max of OOS regime probability, rewrites the test-window
``signal`` / ``confidence`` columns to a sparse event-entry stream, and
then lets the existing execution engine manage TP / SL / break-even /
funding / costs.

This is intentionally not a trade-level post-filter. Trade-level
filtering is path-dependent and mismatches the engine's next-bar entry
semantics. As of v2.0.1, ``"event_rate"`` is the only supported PAITS
entry-control mode. The legacy ``"static"`` and ``"walkforward"`` modes
remain removed together with sticky / ADX / QPB.
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
        within the configured date range, and aggregates trades.

        In ``[filters.qpb].mode = "event_rate"`` mode, PAITS-Event is
        applied as a pre-execution signal gate: the persisted OOS
        ``regime_prob`` / ``direction_indicator`` columns are converted
        into sparse ``signal`` / ``confidence`` entries before the
        engine is called. ``model.predict`` is deliberately not invoked
        in this mode, because doing so would rebuild raw breakout
        signals and reintroduce the old post-filter failure mode.

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

        qpb_cfg = self._filter_config.get("qpb", {}) or {}
        mode = qpb_cfg.get("mode")
        use_event_rate = mode == "event_rate"

        if mode is not None and not use_event_rate:
            logger.warning(
                "Unsupported [filters.qpb].mode=%r in v2.0.1; "
                "use \"event_rate\" or remove the section. Pass-through.",
                mode,
            )

        event_mask = (
            self._build_event_rate_mask(window_results)
            if use_event_rate else None
        )

        updated: list[WindowResult | None] = []
        for res in window_results:
            if res is None:
                updated.append(None)
                continue
            if not (wf_settings["start_date"] <= res.window_label <= wf_settings["end_date"]):
                continue

            if use_event_rate:
                # PAITS-Event direct mode: do not call model.predict().
                # The pickle already contains OOS regime_prob and
                # direction_indicator for this test window. Rebuild only
                # the executable signal stream before the engine sees it.
                try:
                    signal_df = self._apply_event_rate_to_signal_df(
                        res.signal_df, event_mask
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "PAITS-Event signal gating failed for window %s: %s",
                        res.window_label, exc,
                    )
                    signal_df = res.signal_df.copy()
                    signal_df["signal"] = 0
                    signal_df["confidence"] = 0.0
            else:
                # Default/backward-compatible mode: re-generate raw model
                # signals with current settings.
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
            updated.append(replace(res, trades=trades, signal_df=signal_df))

        all_trades, param_summary = self._aggregate(updated)

        # No trade-level filter here. PAITS-Event must be applied before
        # run_backtest because the engine is single-position and enters on
        # the next bar. Post-filtering trades is path-dependent and causes
        # timestamp mismatches (signal bar t vs entry bar t+1).
        return all_trades, param_summary

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _event_rate_config(self) -> WalkForwardEventRateConfig:
        """Parse ``[filters.qpb.event_rate]`` into a config object."""
        qpb_cfg = self._filter_config.get("qpb", {}) or {}
        er_section = qpb_cfg.get("event_rate", {}) or {}
        return WalkForwardEventRateConfig(
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

    def _build_event_rate_mask(
        self,
        window_results: list[WindowResult | None],
        ) -> pd.Series:
        """Build the PAITS-Event admissible-bar mask over all OOS windows.

        The returned index contains signal-bar timestamps. The engine will
        consume the corresponding sparse ``signal`` values and enter at the
        next bar according to its normal semantics.
        """
        cfg = self._event_rate_config()

        logger.info(
            "PAITS-Event pre-execution gate ACTIVE: "
            "target=%.1f/mo cooldown=%.2fd lookback=%dd",
            cfg.target_events_per_month,
            cfg.cooldown_days,
            cfg.lookback_days,
        )

        signal_dfs = [
            r.signal_df for r in window_results
            if r is not None and not r.signal_df.empty
        ]
        if not signal_dfs:
            logger.warning("PAITS-Event disabled — no signal DataFrames available")
            return pd.Series(dtype=bool)

        full_signal = pd.concat(signal_dfs).sort_index()
        full_signal = full_signal[~full_signal.index.duplicated(keep="first")]

        required_cols = {"regime_prob", "direction_indicator"}
        missing = required_cols - set(full_signal.columns)
        if missing:
            raise KeyError(
                f"PAITS-Event requires signal columns {sorted(missing)}"
            )

        gate = WalkForwardEventRateGate(cfg)
        mask = gate.compute_mask(full_signal).astype(bool)
        n_events = int(mask.sum())
        logger.info(
            "PAITS-Event pre-execution events: %d / %d bars admitted (%.4f%%)",
            n_events,
            len(mask),
            100.0 * n_events / max(len(mask), 1),
        )
        return mask

    def _apply_event_rate_to_signal_df(
        self,
        signal_df: pd.DataFrame,
        event_mask: pd.Series | None,
        ) -> pd.DataFrame:
        """Rewrite a test-window signal frame as sparse PAITS-Event entries.

        ``signal`` and ``confidence`` are reset to flat everywhere. At
        event-mask bars, ``signal`` is set from ``direction_indicator`` and
        ``confidence`` from ``regime_prob``. No sticky / ADX / QPB chain and
        no raw model breakout signal are consulted.
        """
        if event_mask is None or event_mask.empty:
            out = signal_df.copy()
            out["signal"] = 0
            out["confidence"] = 0.0
            return out

        missing = {"regime_prob", "direction_indicator"} - set(signal_df.columns)
        if missing:
            raise KeyError(
                f"PAITS-Event requires signal columns {sorted(missing)}"
            )

        out = signal_df.copy()
        if "raw_signal_before_event_rate" not in out.columns and "signal" in out.columns:
            out["raw_signal_before_event_rate"] = out["signal"]
        if "raw_confidence_before_event_rate" not in out.columns and "confidence" in out.columns:
            out["raw_confidence_before_event_rate"] = out["confidence"]

        allowed = event_mask.reindex(out.index).fillna(False).astype(bool)

        out["signal"] = 0
        out["confidence"] = 0.0

        direction = pd.to_numeric(
            out.loc[allowed, "direction_indicator"], errors="coerce"
        ).fillna(0).astype(int)
        confidence = pd.to_numeric(
            out.loc[allowed, "regime_prob"], errors="coerce"
        ).fillna(0.0).clip(0.0, 1.0)

        out.loc[allowed, "signal"] = direction
        out.loc[allowed, "confidence"] = confidence
        out["event_rate_allowed"] = allowed.astype(bool)

        return out

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
