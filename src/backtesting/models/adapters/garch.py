"""
backtesting.models.adapters.garch
==================================
Adapter that wraps a GARCH(1,1) volatility model (via the ``arch`` package)
and exposes it through the :class:`~backtesting.core.base_model.BaseModel`
interface.

Design notes
------------
* The GARCH model does not produce directional signals on its own.  The
  adapter maps GARCH-forecasted volatility onto a sigmoid regime-probability
  using the same functional form as the MDRS SDE model, allowing apples-
  to-apples comparison in walk-forward analysis.
* ``k_avg`` and ``gamma_avg`` (the sigmoid calibration constants) are read
  from ``configs/model_parameters/garch_btc.toml → [regime_signal]`` so
  they are no longer hard-coded in the source (see original ``garch.py``).
* Asymmetric alpha estimation (separate long / short drift) mirrors the
  approach used in the SDE benchmark pipeline.

External dependency
-------------------
``arch`` must be installed (already declared in the SDE repo's
``pyproject.toml``).  If absent, :meth:`fit` raises ``ImportError``.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from backtesting.core.base_model import BaseModel

logger = logging.getLogger(__name__)

_DEFAULT_K_AVG = 0.45
_DEFAULT_GAMMA_AVG = 2.70
_MIN_TRAINING_ROWS = 100


class GarchCryptoAdapter(BaseModel):
    """Adapter for the GARCH(1,1) benchmark model.

    Args:
        model_config: Merged config dict from
            ``BacktestConfigLoader.get_model_config("garch_*")``.
            Expected sections: ``[garch_settings]``, ``[regime_signal]``.
        backtest_config: Parsed ``[risk_management]`` and ``[filters]``
            sections used for entry-threshold and filter settings.

    Example::

        from backtesting.core.config_loader import BacktestConfigLoader
        from backtesting.models.adapters.garch import GarchCryptoAdapter

        loader  = BacktestConfigLoader()
        adapter = GarchCryptoAdapter(
            model_config=loader.get_model_config("garch_btc"),
            backtest_config=loader.get_backtest_settings(),
        )
        params  = adapter.fit(train_df)
        signals = adapter.predict(test_df, params)
    """
    def __init__(
        self,
        model_config: dict[str, Any],
        backtest_config: dict[str, Any],
        ) -> None:
        garch_cfg = model_config.get("garch_settings", {})
        regime_cfg = model_config.get("regime_signal", {})

        self._p = int(garch_cfg.get("p", 1))
        self._q = int(garch_cfg.get("q", 1))
        self._dist = garch_cfg.get("dist", "normal")

        self._k_avg = float(regime_cfg.get("k_avg", _DEFAULT_K_AVG))
        self._gamma_avg = float(
            regime_cfg.get("gamma_avg", _DEFAULT_GAMMA_AVG)
        )

        self._risk = backtest_config.get("risk_management", {})
        self._filters = backtest_config.get("filters", {})

    # ------------------------------------------------------------------
    # BaseModel interface
    # ------------------------------------------------------------------

    def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """Estimate GARCH(p, q) parameters on in-sample log-returns.

        Computes asymmetric alpha estimates (separate long / short drift
        proxies) and the final conditional volatility as ``sigma_1`` so
        that :class:`~backtesting.engines.engine.GenericBacktestEngine`
        can apply the same SNR-based TP/SL scaling as the SDE model.

        Args:
            train_data: In-sample ``DataFrame`` containing ``log_return``.
                Must have at least :data:`_MIN_TRAINING_ROWS` rows.

        Returns:
            Dictionary with keys:

            * ``alpha_long``  (float) — mean positive log-return × 100.
            * ``alpha_short`` (float) — mean absolute negative log-return × 100.
            * ``sigma_1``     (float) — last estimated conditional volatility.
            * ``arch_result`` (object) — fitted ``ARCHModelResult`` for
              forecasting in :meth:`predict`.
            * ``internal_scale`` (float) — rescaling factor applied by
              ``arch`` during fitting (may be 1.0).

        Returns an empty ``dict`` on estimation failure.

        Raises:
            ImportError: If the ``arch`` package is not installed.
        """
        self._check_arch_installed()

        if len(train_data) < _MIN_TRAINING_ROWS:
            logger.warning(
                "GarchCryptoAdapter.fit(): only %d training rows "
                "(minimum %d required) — returning empty params.",
                len(train_data),
                _MIN_TRAINING_ROWS,
            )
            return {}

        from arch import arch_model  # type: ignore[import]

        train_returns = train_data["log_return"] * 100.0
        model = arch_model(
            train_returns,
            vol="Garch",
            p=self._p,
            q=self._q,
            dist=self._dist,
            rescale=True,
        )

        try:
            result = model.fit(disp="off", show_warning=False)
        except Exception as exc:  # noqa: BLE001
            logger.error("GARCH fit failed: %s", exc)
            return {}

        internal_scale: float = float(
            result.scale if hasattr(result, "scale") else 1.0
        )

        # Asymmetric drift proxies
        pos_ret = train_data.loc[train_data["log_return"] > 0, "log_return"]
        neg_ret = train_data.loc[train_data["log_return"] < 0, "log_return"]
        alpha_long = float(pos_ret.mean() * 100) if not pos_ret.empty else 0.5
        alpha_short = (
            float(neg_ret.abs().mean() * 100) if not neg_ret.empty else 0.5
        )

        sigma_1 = float(
            result.conditional_volatility.iloc[-1] / internal_scale / 100.0
        )

        return {
            "alpha_long": alpha_long,
            "alpha_short": alpha_short,
            "sigma_1": sigma_1,
            "arch_result": result,
            "internal_scale": internal_scale,
        }

    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
        ) -> pd.DataFrame:
        """Generate signals by mapping GARCH volatility forecasts to regime
        probabilities.

        Pipeline:

        1. Forecast conditional volatility for the test horizon.
        2. Compute a per-bar Z-score using ``(Close - Open) / (Open × GARCH_Vol)``.
        3. Map Z-score to sigmoid regime probability using calibrated
           ``k_avg`` / ``gamma_avg`` constants.
        4. Apply sticky-breakout filter.
        5. Determine Long / Short direction from Donchian-channel S/R.
        6. Apply ADX gate.

        Args:
            test_data: Out-of-sample ``DataFrame`` containing ``Open``,
                ``Close``, ``High``, ``Low``, ``ADX``,
                ``dynamic_resistance``, ``dynamic_support``.
            params: Dict returned by :meth:`fit`.  Must contain
                ``arch_result`` and ``internal_scale``.

        Returns:
            *test_data* with ``GARCH_Vol``, ``regime_prob``, ``confidence``,
            and ``signal`` columns added.
        """
        if not params:
            logger.warning(
                "GarchCryptoAdapter.predict(): received empty params "
                "— all signals set to 0."
            )
            test_data = test_data.copy()
            test_data["signal"] = 0
            test_data["confidence"] = 0.0
            return test_data

        df = test_data.copy()
        result = params["arch_result"]
        internal_scale: float = float(params.get("internal_scale", 1.0))

        # Step 1 — volatility forecast
        horizon = len(df)
        forecasts = result.forecast(horizon=horizon, reindex=False)
        forecast_vol = (
            np.sqrt(forecasts.variance.values.flatten())
            / (internal_scale * 100.0)
        )
        df["GARCH_Vol"] = forecast_vol

        # Step 2 — bar-level Z-score
        safe_vol = df["GARCH_Vol"].replace(0, np.nan)
        z_score = (df["Close"] - df["Open"]) / (df["Open"] * safe_vol)

        # Step 3 — sigmoid regime probability
        df["regime_prob"] = 1.0 / (
            1.0 + np.exp(-self._k_avg * (z_score.abs() - self._gamma_avg))
        )
        df["confidence"] = df["regime_prob"]

        # Step 4 — sticky filter
        entry_threshold = self._risk.get("entry_probability_threshold", 0.5)
        min_duration = self._risk.get("minimum_signal_duration", 5)
        use_sticky = self._filters.get("use_sticky", True)
        use_adx = self._filters.get("use_adx", True)
        adx_threshold = 30

        binary_entry = (df["regime_prob"] > entry_threshold).astype(int)
        if use_sticky:
            sticky = (
                binary_entry.rolling(window=min_duration).sum() == min_duration
            ).astype(int)
        else:
            sticky = binary_entry

        # Step 5 & 6 — direction + ADX gate
        long_cond = df["Close"] > df["dynamic_resistance"]
        short_cond = df["Close"] < df["dynamic_support"]
        adx_pass = (df["ADX"] > adx_threshold) if use_adx else pd.Series(
            True, index=df.index
        )

        df["signal"] = 0
        df.loc[sticky.astype(bool) & long_cond & adx_pass, "signal"] = 1
        df.loc[sticky.astype(bool) & short_cond & adx_pass, "signal"] = -1

        return df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _check_arch_installed() -> None:
        try:
            import arch  # noqa: F401  # type: ignore[import]
        except ModuleNotFoundError as exc:
            raise ImportError(
                "The 'arch' package is required for GarchCryptoAdapter. "
                "Install it with:\n  pip install arch"
            ) from exc
