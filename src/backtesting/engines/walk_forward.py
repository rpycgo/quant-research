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

EMA sigma reference
--------------------
Before parallel execution, ``_precompute_sigma_reference()`` computes a
per-window ``reference_sigma_1`` from the log_return rolling std of the
preceding training slice, smoothed with an EMA. This replaces the fixed
config value so that each window's TP / SL scaling adapts to the recent
volatility regime. The first window always uses the config default.

Controlled by ``use_ema_sigma`` in ``[filters]`` and ``ema_sigma_span``
in ``[walk_forward_settings]`` of ``backtest_settings.toml``.
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
                       Optional: ``ema_sigma_span`` (default: 3).
        filter_config: Parsed ``[filters]`` section from
                       ``backtest_settings.toml``. Controls ``use_ema_sigma``
                       and other filter toggles. Defaults to empty dict.
        use_dynamic_params: When ``True``, uses ``build_dynamic_params()``
                       with SNR scaling and EMA sigma reference (MDRS-SDE only).
                       Default ``False``.
        use_ema_sigma:  When ``True``, precomputes EMA sigma reference for
                       all models and passes ref_sigma to get_fixed_params()
                       for vol_quality SL adjustment. Independent of
                       use_dynamic_params. Default ``True``.

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
        use_dynamic_params: bool = False,
        use_ema_sigma: bool = True,
        ) -> None:
        self._model                   = model
        self._engine                  = engine
        self._start                   = pd.Timestamp(wfa_config["start_date"])
        self._end                     = pd.Timestamp(wfa_config["end_date"])
        self._train_months            = wfa_config.get("training_months", 3)
        self._test_months             = wfa_config.get("testing_months", 1)
        self._n_jobs                  = wfa_config.get("parallel_jobs", 1)
        self._ema_sigma_span          = wfa_config.get("ema_sigma_span", 3)
        self._reference_window_months = wfa_config.get("reference_window_months", 12)
        self._use_dynamic_params      = use_dynamic_params
        self._use_ema_sigma           = use_ema_sigma

        filters = filter_config or {}
        self._filter_use_ema_sigma = filters.get("use_ema_sigma", True)

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
                        in-zone rows only for the SDE model). Falls back to
                        *full_data* when ``None``.

        Returns:
            A 2-tuple:

            * ``all_trades``    — concatenated trades ``DataFrame`` sorted by
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

        # Fallback: first window uses train_data log_return std
        if "log_return" in train_data.columns and len(train_data) > 10:
            fallback_sigma = float(train_data["log_return"].std() * 100)
        else:
            fallback_sigma = self._engine.risk_parameters.get("reference_sigma_1", 14.665)

        # EMA sigma is independent of use_dynamic_params
        # Both flags (model-level use_ema_sigma AND filter use_ema_sigma) must be True
        if self._use_ema_sigma and self._filter_use_ema_sigma:
            ref_sigma_map = self._precompute_reference_sigma(
                train_data=train_data,
                test_starts=test_starts,
                fallback=fallback_sigma,
                reference_window_months=self._reference_window_months,
            )
            logger.info(
                "EMA sigma reference enabled — expanding up to %d months then rolling.",
                self._reference_window_months,
            )
        else:
            ref_sigma_map = {ts: fallback_sigma for ts in test_starts}
            logger.info("EMA sigma reference disabled — vol_quality=1.0 for all windows.")

        window_results = Parallel(n_jobs=self._n_jobs)(
            delayed(self._process_window)(ts, train_data, full_data, ref_sigma_map[ts])
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
        ref_sigma: float,
        ) -> WindowResult | None:
        """Execute one walk-forward window.

        Args:
            test_start: First timestamp of the out-of-sample period.
            train_data: Full training-eligible dataset (pre-filtered).
            full_data:  Full dataset for the out-of-sample slice.
            ref_sigma:  EMA-smoothed reference_sigma_1 for this window.

        Returns:
            :class:`WindowResult` on success, ``None`` on irrecoverable error.
        """
        label = test_start.strftime("%Y-%m-%d")

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

        # Fit
        try:
            params = self._model.fit(train_slice)
        except Exception as exc:  # noqa: BLE001
            logger.error("fit() failed for window %s: %s", label, exc)
            return None

        if not params:
            logger.warning("Window %s: fit() returned empty params.", label)
            return None

        # Predict
        try:
            signal_df = self._model.predict(test_slice, params)
        except Exception as exc:  # noqa: BLE001
            logger.error("predict() failed for window %s: %s", label, exc)
            return None

        # Build execution params
        if self._use_dynamic_params:
            dynamic_params = self._engine.build_dynamic_params(params, ref_sigma=ref_sigma)
        else:
            # Compute vol_quality as ratio of current window sigma to reference sigma
            # Both are log_return.std() * 100 — same scale, no reference_sigma_1 needed
            train_end_   = test_start - pd.Timedelta(seconds=1)
            train_start_ = train_end_ - relativedelta(months=self._train_months)
            train_slice_ = train_data.loc[train_start_:train_end_]

            if len(train_slice_) > 10 and "log_return" in train_slice_.columns:
                window_sigma = float(train_slice_["log_return"].std() * 100)
                vol_quality  = (window_sigma / ref_sigma) if ref_sigma > 0 else 1.0
            else:
                vol_quality = 1.0

            dynamic_params = self._engine.get_fixed_params(vol_quality=vol_quality)

        # Run backtest
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

    def _precompute_reference_sigma(
        self,
        train_data: pd.DataFrame,
        test_starts: pd.DatetimeIndex,
        fallback: float,
        reference_window_months: int = 12,
        ) -> dict[pd.Timestamp, float]:
        """Compute per-window reference sigma using expanding then rolling window.

        For each test window, the reference sigma is computed from all data
        preceding the test start, up to a maximum of ``reference_window_months``.

        * Expanding phase: when available data < reference_window_months,
          use all available data (e.g. months 1-3, 1-6, 1-9, 1-12).
        * Rolling phase: when available data >= reference_window_months,
          use the most recent reference_window_months only.

        This ensures:
        - No look-ahead bias (always uses data before test_start)
        - Stable reference as data accumulates
        - Adapts to long-term volatility regime shifts

        Both reference sigma and window sigma use the same scale
        (log_return.std() * 100), so vol_quality is dimensionally consistent.

        Args:
            train_data:              Full training-eligible dataset.
            test_starts:             Ordered sequence of test window start timestamps.
            fallback:                Fallback sigma when insufficient data.
            reference_window_months: Max lookback for reference sigma (default: 12).

        Returns:
            Dict mapping each test_start timestamp to its reference sigma.
        """
        ref_sigmas: dict[pd.Timestamp, float] = {}

        for ts in test_starts:
            ref_end      = ts - pd.Timedelta(seconds=1)
            rolling_start = ref_end - relativedelta(months=reference_window_months)

            # Try rolling window first (most recent reference_window_months)
            # If not enough data (expanding phase), fall back to all available data
            slice_ = train_data.loc[rolling_start:ref_end]

            if len(slice_) > 10 and "log_return" in slice_.columns:
                # Rolling phase: sufficient data for full reference window
                sigma = float(slice_["log_return"].std() * 100)
            else:
                # Expanding phase: use all data available before test_start
                slice_ = train_data.loc[:ref_end]
                if len(slice_) > 10 and "log_return" in slice_.columns:
                    sigma = float(slice_["log_return"].std() * 100)
                else:
                    sigma = fallback

            ref_sigmas[ts] = sigma if sigma > 0 else fallback

        logger.debug(
            "Reference sigma — first: %.4f, last: %.4f, window: %d months",
            list(ref_sigmas.values())[0],
            list(ref_sigmas.values())[-1],
            reference_window_months,
        )

        return ref_sigmas

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
