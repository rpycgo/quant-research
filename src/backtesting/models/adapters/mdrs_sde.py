"""
backtesting.models.adapters.mdrs_sde
=======================================
Adapter that wraps the ``mdrs-sde-btc`` external package and exposes it
through the :class:`~backtesting.core.base_model.BaseModel` interface.

Responsibilities
----------------
* **fit()** — delegates Bayesian MCMC parameter estimation to
  ``mdrs_sde.SdeModeler`` and returns the posterior means as a plain
  ``dict``.
* **predict()** — runs the sigmoid regime-probability calculation, applies
  the sticky-breakout persistence filter, and maps the results to
  ``signal`` / ``confidence`` columns.  All model-specific filtering
  (sticky duration, ADX gate, zone gate) lives here, not in the engine.

External dependency
-------------------
``mdrs-sde-btc`` must be installed before this adapter is used::

    pip install git+https://github.com/rpycgo/mdrs-sde-btc.git

If the package is absent, :meth:`fit` raises ``ImportError`` with a
helpful installation message rather than a bare ``ModuleNotFoundError``.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from backtesting.core.base_model import BaseModel

logger = logging.getLogger(__name__)

# Minimum consecutive bars a signal must persist before entry (sticky filter)
_DEFAULT_MIN_DURATION = 5


class MdrsSdeCryptoAdapter(BaseModel):
    """Adapter for the MDRS Regime-Switching SDE model.

    Wraps ``mdrs_sde.SdeModeler`` from the external ``mdrs-sde-btc``
    package.  The adapter is asset-agnostic: the same class handles BTC,
    ETH, or any other crypto pair whose preprocessed ``DataFrame`` contains
    the required feature columns (``hybrid_z_score``, ``log_return``,
    ``direction_indicator``).

    Args:
        model_config: Merged config dict from
            ``BacktestConfigLoader.get_model_config("mdrs_sde_*")``.
            Must contain ``[sde_priors]`` and ``[mcmc_settings]`` sections.
        backtest_config: Parsed ``[risk_management]`` section used for
            entry-threshold and sticky-filter settings.

    Example::

        from backtesting.core.config_loader import BacktestConfigLoader
        from backtesting.models.adapters.mdrs_sde import MdrsSdeCryptoAdapter

        loader = BacktestConfigLoader()
        adapter = MdrsSdeCryptoAdapter(
            model_config=loader.get_model_config("mdrs_sde_btc"),
            backtest_config=loader.get_backtest_settings(),
        )
        params = adapter.fit(train_df)
        signals = adapter.predict(test_df, params)
    """
    def __init__(
        self,
        model_config: dict[str, Any],
        backtest_config: dict[str, Any],
    ) -> None:
        self._model_config = model_config
        self._risk = backtest_config.get("risk_management", {})
        self._filters = backtest_config.get("filters", {})

    # ------------------------------------------------------------------
    # BaseModel interface
    # ------------------------------------------------------------------

    def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """Estimate SDE parameters via Bayesian MCMC (NUTS sampler).

        Delegates to ``mdrs_sde.SdeModeler.estimate_parameters``.

        Args:
            train_data: In-sample ``DataFrame`` containing ``hybrid_z_score``,
                ``log_return``, and ``direction_indicator`` columns.

        Returns:
            Dictionary of posterior mean estimates keyed by parameter name
            (``alpha_long``, ``alpha_short``, ``kappa``, ``gamma``, ``k``,
            ``sigma_0``, ``sigma_1``).  Returns an empty ``dict`` when MCMC
            sampling fails so the walk-forward runner can skip the window
            without raising.

        Raises:
            ImportError: If ``mdrs_sde`` is not installed.
            KeyError: If required columns are missing from *train_data*.
        """
        df = train_data.copy()

        conditions = [
            df["Close"] > df["manual_resistance"],
            df["Close"] < df["manual_support"],
        ]
        df["direction_indicator"] = np.select(conditions, [1, -1], default=0)

        modeler = self._get_modeler()

        required = {"hybrid_z_score", "log_return", "direction_indicator"}
        missing = required - set(train_data.columns)
        if missing:
            raise KeyError(
                f"MdrsSdeCryptoAdapter.fit(): train_data is missing columns "
                f"{missing}.  Run CryptoPreprocessor.run_full_pipeline() first."
            )

        _trace, _summary, estimates = modeler.estimate_parameters(
            z_values=train_data["hybrid_z_score"].values,
            returns_scaled=train_data["log_return"].values * 100,
            direction=train_data["direction_indicator"].values,
        )

        if estimates is None:
            logger.warning(
                "MdrsSdeCryptoAdapter: MCMC sampling failed — "
                "returning empty params dict."
            )
            return {}

        return estimates

    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
        ) -> pd.DataFrame:
        """Generate Long / Short / Flat signals using estimated SDE parameters.

        Pipeline (all model-specific; engine sees only ``signal``):

        1. Compute sigmoid regime probability from ``hybrid_z_score``.
        2. Apply sticky-breakout persistence filter.
        3. Determine entry direction from ``dynamic_resistance`` /
           ``dynamic_support`` relative to ``Close``.
        4. Gate signals by ADX threshold (if ``use_adx`` is ``True``).
        5. Map to ``signal`` (1 / -1 / 0) and ``confidence``.

        Args:
            test_data: Out-of-sample ``DataFrame`` containing at minimum
                ``hybrid_z_score``, ``Close``, ``ADX``,
                ``dynamic_resistance``, ``dynamic_support``.
            params: Posterior mean dict from :meth:`fit`.

        Returns:
            *test_data* with ``signal`` (``int``) and ``confidence``
            (``float``) columns added.
        """
        df = test_data.copy()
        k = float(params.get("k", 1.0))
        gamma = float(params.get("gamma", 2.0))

        entry_threshold = self._risk.get("entry_probability_threshold", 0.5)
        min_duration = self._risk.get("minimum_signal_duration", _DEFAULT_MIN_DURATION)
        use_sticky = self._filters.get("use_sticky", True)
        use_adx = self._filters.get("use_adx", True)
        adx_threshold = 30  # kept consistent with backtest_settings default

        # Step 1 — sigmoid regime probability
        df["regime_prob"] = 1.0 / (
            1.0 + np.exp(-k * (df["hybrid_z_score"] - gamma))
        )
        df["confidence"] = df["regime_prob"]

        # Step 2 — sticky breakout filter
        binary_entry = (df["regime_prob"] > entry_threshold).astype(int)
        if use_sticky:
            sticky = (
                binary_entry.rolling(window=min_duration).sum() == min_duration
            ).astype(int)
        else:
            sticky = binary_entry

        # Step 3 — direction gate
        long_cond = df["Close"] > df["dynamic_resistance"]
        short_cond = df["Close"] < df["dynamic_support"]

        # Step 4 — ADX gate
        adx_pass = (df["ADX"] > adx_threshold) if use_adx else pd.Series(
            True, index=df.index
        )

        # Step 5 — assemble signal column
        df["signal"] = 0
        df.loc[sticky.astype(bool) & long_cond & adx_pass, "signal"] = 1
        df.loc[sticky.astype(bool) & short_cond & adx_pass, "signal"] = -1

        return df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_modeler(self) -> Any:
        """Lazily import and construct ``SdeModeler`` from the external package.

        Raises:
            ImportError: If ``mdrs_sde`` is not installed.
        """
        try:
            from mdrs_sde.models import MdrsModeler  # type: ignore[import]
        except ModuleNotFoundError as exc:
            raise ImportError(
                "The 'mdrs-sde-btc' package is required for MdrsSdeCryptoAdapter. "
                "Install it with:\n"
                "  pip install git+https://github.com/rpycgo-research/mdrs-sde.git"
            ) from exc

        return MdrsModeler(config=self._model_config)
