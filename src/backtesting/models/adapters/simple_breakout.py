"""
backtesting.models.adapters.simple_breakout
============================================
Simple Breakout rule-based trading system.

Entry logic:
- Long  : Close breaks above 288-period rolling high (shifted by 1 to avoid look-ahead)
- Short : Close breaks below 288-period rolling low  (shifted by 1 to avoid look-ahead)

Fully independent rule-based system with no statistical model.
Serves as a baseline to verify that regime-based approaches
outperform naive breakout systems.
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from backtesting.core.base_model import BaseModel

logger = logging.getLogger(__name__)

_DEFAULT_WINDOW = 288


class SimpleBreakoutAdapter(BaseModel):
    """Rule-based breakout system adapter.

    Args:
        model_config:    Config dict. Optional key under
                         ``[breakout_settings]``: ``window`` (int, default 20).
        backtest_config: Parsed backtest settings dict (unused).
    """
    def __init__(
        self,
        model_config: dict[str, Any],
        backtest_config: dict[str, Any],
        ) -> None:
        self._window = int(
            model_config.get("breakout_settings", {}).get("window", _DEFAULT_WINDOW)
        )

    # ------------------------------------------------------------------
    # BaseModel interface
    # ------------------------------------------------------------------

    def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """No training required.

        Args:
            train_data: In-sample ``DataFrame`` (not used).

        Returns:
            Dict with ``window`` key.
        """
        return {"window": self._window}

    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
        ) -> pd.DataFrame:
        """Generate breakout signals on test data.

        Args:
            test_data: Out-of-sample ``DataFrame`` with ``High``, ``Low``,
                       ``Close`` columns.
            params:    Dict from :meth:`fit`.

        Returns:
            ``test_data`` with ``signal`` (1 / -1 / 0) and
            ``confidence`` (float) columns added.
        """
        df = test_data.copy()
        window = params.get("window", self._window)

        rolling_high = df["High"].rolling(window).max().shift(1)
        rolling_low  = df["Low"].rolling(window).min().shift(1)

        df["signal"] = 0
        df.loc[df["Close"] > rolling_high, "signal"] =  1
        df.loc[df["Close"] < rolling_low,  "signal"] = -1

        # Confidence: normalized distance from breakout level
        df["confidence"] = 0.5
        long_mask  = df["signal"] == 1
        short_mask = df["signal"] == -1
        df.loc[long_mask, "confidence"] = (
            (df.loc[long_mask, "Close"] - rolling_high[long_mask])
            / rolling_high[long_mask]
        ).clip(0, 0.5) + 0.5
        df.loc[short_mask, "confidence"] = (
            (rolling_low[short_mask] - df.loc[short_mask, "Close"])
            / rolling_low[short_mask]
        ).clip(0, 0.5) + 0.5
        df["confidence"] = df["confidence"].clip(0, 1).fillna(0.5)

        return df
