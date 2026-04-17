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
* **predict()** — runs the sigmoid regime-probability calculation and
  applies the shared sticky / ADX / QPB pipeline from
  ``_regime_gates.assemble_signal`` before emitting ``signal`` and
  ``confidence`` columns.

External dependency
-------------------
``mdrs-sde-btc`` must be installed before this adapter is used::

    pip install git+https://github.com/rpycgo-research/mdrs-sde.git

If the package is absent, :meth:`fit` raises ``ImportError`` with a
helpful installation message rather than a bare ``ModuleNotFoundError``.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from backtesting.core.base_model import BaseModel
from backtesting.models.adapters._regime_gates import assemble_signal

logger = logging.getLogger(__name__)


class MdrsSdeCryptoAdapter(BaseModel):
    """Adapter for the MDRS Regime-Switching SDE model.

    Wraps ``mdrs_sde.SdeModeler`` from the external ``mdrs-sde-btc``
    package.  The adapter is asset-agnostic: the same class handles BTC,
    ETH, or any other crypto pair whose preprocessed ``DataFrame`` contains
    the required feature columns (``hybrid_z_score``, ``log_return``,
    ``direction_indicator``, and — when QPB is enabled — ``past_vol_48b``,
    ``past_ret_48b``, ``d_rv_90d_proxy``).

    Args:
        model_config: Merged config dict from
            ``BacktestConfigLoader.get_model_config("mdrs_sde_*")``.
            Must contain ``[sde_priors]`` and ``[mcmc_settings]`` sections.
        backtest_config: Parsed backtest settings dict (consumed sections:
            ``[risk_management]``, ``[filters]`` including
            ``[filters.qpb]``, and ``[trading_parameters]``).

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
        self._trade = backtest_config.get("trading_parameters", {})

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
            Dictionary containing the MCMC trace, summary, and posterior
            mean estimates.  Returns an empty ``dict`` when MCMC sampling
            fails so the walk-forward runner can skip the window without
            raising.

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

        return {
            'trace': _trace,
            'summary': _summary,
            'estimates': estimates,
        }

    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
        ) -> pd.DataFrame:
        """Generate Long / Short / Flat signals using estimated SDE parameters.

        Computes the sigmoid regime probability from ``hybrid_z_score``
        and delegates the downstream filtering pipeline (sticky / ADX /
        QPB) to :func:`_regime_gates.assemble_signal` to guarantee
        parity with the other regime-probability adapters (DL-regime,
        HMM).

        Args:
            test_data: Out-of-sample ``DataFrame`` containing at minimum
                ``hybrid_z_score``, ``Close``, ``ADX``,
                ``dynamic_resistance``, ``dynamic_support``, and — when
                QPB is enabled — ``past_vol_48b``, ``past_ret_48b``,
                ``d_rv_90d_proxy``.
            params: Posterior estimates dict from :meth:`fit`.

        Returns:
            *test_data* with ``regime_prob``, ``confidence``, and
            ``signal`` columns added.
        """
        df = test_data.copy()
        k = float(params.get("k", 1.0))
        gamma = float(params.get("gamma", 2.0))

        regime_prob = 1.0 / (
            1.0 + np.exp(-k * (df["hybrid_z_score"] - gamma))
        )

        return assemble_signal(
            df,
            regime_prob=regime_prob,
            risk_cfg=self._risk,
            filters_cfg=self._filters,
            trade_cfg=self._trade,
        )

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
