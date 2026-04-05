"""
backtesting.models.adapters.rsi
========================================
RSI-based mean-reversion trading system.

Entry logic:
- Long  : RSI < oversold threshold (default 30)
- Short : RSI > overbought threshold (default 70)

Fully independent momentum/oscillator system. Serves as a baseline
to represent pure mean-reversion strategies, contrasting with
MDRS-SDE's regime-blended drift model.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from backtesting.core.base_model import BaseModel

logger = logging.getLogger(__name__)

_DEFAULT_PERIOD     = 14
_DEFAULT_OVERSOLD   = 30.0
_DEFAULT_OVERBOUGHT = 70.0


class RSIAdapter(BaseModel):
    """RSI mean-reversion system adapter.

    Args:
        model_config:    Config dict. Optional keys under ``[rsi_settings]``:
                         ``period`` (default 14), ``oversold`` (default 30),
                         ``overbought`` (default 70).
        backtest_config: Parsed backtest settings dict (unused).
    """

    def __init__(
        self,
        model_config: dict[str, Any],
        backtest_config: dict[str, Any],
        ) -> None:
        rsi_cfg = model_config.get("rsi_settings", {})
        self._period     = int(rsi_cfg.get("period",     _DEFAULT_PERIOD))
        self._oversold   = float(rsi_cfg.get("oversold",   _DEFAULT_OVERSOLD))
        self._overbought = float(rsi_cfg.get("overbought", _DEFAULT_OVERBOUGHT))

    # ------------------------------------------------------------------
    # BaseModel interface
    # ------------------------------------------------------------------

    def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """No training required.

        Args:
            train_data: In-sample ``DataFrame`` (not used).

        Returns:
            Dict with ``period``, ``oversold``, ``overbought`` keys.
        """
        return {
            "period":     self._period,
            "oversold":   self._oversold,
            "overbought": self._overbought,
        }

    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
        ) -> pd.DataFrame:
        """Generate RSI signals on test data.

        Args:
            test_data: Out-of-sample ``DataFrame`` with ``Close`` column.
            params:    Dict from :meth:`fit`.

        Returns:
            ``test_data`` with ``signal``, ``confidence``, ``rsi`` added.
        """
        df = test_data.copy()
        period     = int(params.get("period",     self._period))
        oversold   = float(params.get("oversold",   self._oversold))
        overbought = float(params.get("overbought", self._overbought))

        delta    = df["Close"].diff()
        gain     = delta.clip(lower=0)
        loss     = (-delta).clip(lower=0)
        avg_gain = gain.ewm(span=period, adjust=False).mean()
        avg_loss = loss.ewm(span=period, adjust=False).mean()
        rs       = avg_gain / avg_loss.replace(0, np.nan)
        df["rsi"] = 100 - (100 / (1 + rs))

        df["signal"] = 0
        df.loc[df["rsi"] < oversold,   "signal"] =  1
        df.loc[df["rsi"] > overbought, "signal"] = -1

        # Confidence: distance from threshold normalized to [0, 1]
        df["confidence"] = 0.5
        long_mask  = df["signal"] == 1
        short_mask = df["signal"] == -1
        df.loc[long_mask, "confidence"] = (
            (oversold - df.loc[long_mask, "rsi"]) / oversold
        ).clip(0, 1)
        df.loc[short_mask, "confidence"] = (
            (df.loc[short_mask, "rsi"] - overbought) / (100 - overbought)
        ).clip(0, 1)

        return df
