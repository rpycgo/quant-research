"""
backtesting.models.adapters.hmm_regime
========================================
2-state Gaussian Hidden Markov Model (HMM) regime-switching system.

Fits a Gaussian HMM on log-returns to identify two latent states.
The state with higher mean return is labeled bullish (Long signal),
the other bearish (Short signal).

Used as a direct regime-switching baseline for MDRS-SDE. Both models
detect market regimes but differ fundamentally: HMM uses discrete
hidden states while MDRS-SDE uses a continuous sigmoid-weighted blend
of mean-reversion and trend-following dynamics.

HMM generates a continuous regime probability (the posterior
probability of the bullish state) and uses the same breakout signal
assembly as MDRS-SDE, DL-regime, and LGBM. This ensures a fair
comparison of regime-signal quality across all four regime-probability
based models.

Requires: hmmlearn (uv add hmmlearn)
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM

from backtesting.core.base_model import BaseModel
from backtesting.models.adapters._regime_gates import assemble_signal

logger = logging.getLogger(__name__)

_DEFAULT_N_STATES   = 2
_DEFAULT_N_ITER     = 100
_DEFAULT_COVARIANCE = "full"


class HMMRegimeAdapter(BaseModel):
    """2-state Gaussian HMM regime-switching adapter.

    Args:
        model_config:    Config dict. Optional keys under ``[hmm_settings]``:
                         ``n_states`` (default 2), ``n_iter`` (default 100),
                         ``covariance_type`` (default "full").
        backtest_config: Parsed backtest settings dict (consumed sections:
                         ``[risk_management]`` and ``[trading_parameters]``).

    Raises:
        ImportError: If ``hmmlearn`` is not installed.
    """
    def __init__(
        self,
        model_config: dict[str, Any],
        backtest_config: dict[str, Any],
        ) -> None:
        hmm_cfg = model_config.get("hmm_settings", {})
        self._n_states   = int(hmm_cfg.get("n_states",        _DEFAULT_N_STATES))
        self._n_iter     = int(hmm_cfg.get("n_iter",          _DEFAULT_N_ITER))
        self._covariance = hmm_cfg.get("covariance_type", _DEFAULT_COVARIANCE)

        self._risk = backtest_config.get("risk_management", {})
        self._filters = backtest_config.get("filters", {})
        self._trade = backtest_config.get("trading_parameters", {})

    # ------------------------------------------------------------------
    # BaseModel interface
    # ------------------------------------------------------------------

    def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """Fit Gaussian HMM on training log-returns.

        Args:
            train_data: In-sample ``DataFrame`` with ``log_return`` column.

        Returns:
            Dict with ``model`` and ``bullish_state``.  Returns empty dict
            on fitting failure.

        Raises:
            ImportError: If ``hmmlearn`` is not installed.
        """
        self._check_hmmlearn()

        returns = (train_data["log_return"].dropna() * 100).values.reshape(-1, 1)

        if len(returns) < self._n_states * 10:
            logger.warning(
                "HMMRegimeAdapter.fit(): insufficient training rows (%d).",
                len(returns),
            )
            return {}

        try:
            model = GaussianHMM(
                n_components=self._n_states,
                covariance_type=self._covariance,
                n_iter=self._n_iter,
                random_state=42,
            )
            model.fit(returns)

            means = model.means_.flatten()
            bullish_state = int(np.argmax(means))

            logger.info(
                "HMM fitted — state means: %s | bullish state: %d",
                [f"{m:.6f}" for m in means],
                bullish_state,
            )

            return BaseModel.wrap_fit_result({
                "model":         model,
                "bullish_state": bullish_state,
            })

        except Exception as exc:  # noqa: BLE001
            logger.error("HMM fitting failed: %s", exc)
            return {}

    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
        ) -> pd.DataFrame:
        """Generate regime signals from HMM state predictions.

        Computes the posterior probability of the bullish state and
        delegates breakout signal assembly to
        :func:`_regime_gates.assemble_signal` for parity with
        MDRS-SDE, DL-regime, and LGBM.

        Args:
            test_data: Out-of-sample ``DataFrame`` with ``log_return``,
                       ``Close``, ``dynamic_resistance``, and
                       ``dynamic_support`` columns.
            params:    Dict from :meth:`fit` containing ``model`` and
                       ``bullish_state``.

        Returns:
            ``test_data`` with ``signal``, ``confidence``, ``hmm_state``,
            ``regime_prob`` columns added.
        """
        df = test_data.copy()
        model = params.get("model")

        if model is None:
            df["signal"]      = 0
            df["confidence"]  = 0.0
            df["hmm_state"]   = -1
            df["regime_prob"] = 0.5
            return df

        bullish_state = params.get("bullish_state", 0)
        returns = (df["log_return"].fillna(0) * 100).values.reshape(-1, 1)

        try:
            states = model.predict(returns)
            probs  = model.predict_proba(returns)

            df["hmm_state"]   = states
            regime_prob = pd.Series(probs[:, bullish_state], index=df.index)

        except Exception as exc:  # noqa: BLE001
            logger.error("HMM prediction failed: %s", exc)
            df["signal"]      = 0
            df["confidence"]  = 0.0
            df["hmm_state"]   = -1
            df["regime_prob"] = 0.5
            return df

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

    @staticmethod
    def _check_hmmlearn() -> None:
        try:
            import hmmlearn  # noqa: F401  # type: ignore[import]
        except ModuleNotFoundError as exc:
            raise ImportError(
                "hmmlearn is required for HMMRegimeAdapter. "
                "Install with: uv add hmmlearn"
            ) from exc
