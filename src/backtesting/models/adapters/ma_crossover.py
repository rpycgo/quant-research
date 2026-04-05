"""
backtesting.models.adapters.ma_crossover
=========================================
Moving Average Crossover trend-following system.

Entry logic:
- Long  : EMA(fast) > EMA(slow)
- Short : EMA(fast) < EMA(slow)

Fully independent trend-following system. Serves as a baseline
to verify that MDRS-SDE outperforms naive MA-based trend detection.
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from backtesting.core.base_model import BaseModel

logger = logging.getLogger(__name__)

_DEFAULT_FAST = 12
_DEFAULT_SLOW = 26


class MACrossoverAdapter(BaseModel):
    """EMA crossover trend-following adapter.

    Args:
        model_config:    Config dict. Optional keys under ``[ma_settings]``:
                         ``fast_period`` (default 12), ``slow_period`` (default 26).
        backtest_config: Parsed backtest settings dict (unused).
    """
    def __init__(
        self,
        model_config: dict[str, Any],
        backtest_config: dict[str, Any],
        ) -> None:
        ma_cfg = model_config.get("ma_settings", {})
        self._fast = int(ma_cfg.get("fast_period", _DEFAULT_FAST))
        self._slow = int(ma_cfg.get("slow_period", _DEFAULT_SLOW))

    # ------------------------------------------------------------------
    # BaseModel interface
    # ------------------------------------------------------------------

    def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """No training required.

        Args:
            train_data: In-sample ``DataFrame`` (not used).

        Returns:
            Dict with ``fast`` and ``slow`` keys.
        """
        return {"fast": self._fast, "slow": self._slow}

    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
        ) -> pd.DataFrame:
        """Generate MA crossover signals on test data.

        Args:
            test_data: Out-of-sample ``DataFrame`` with ``Close`` column.
            params:    Dict from :meth:`fit`.

        Returns:
            ``test_data`` with ``signal``, ``confidence``,
            ``ema_fast``, ``ema_slow`` columns added.
        """
        df = test_data.copy()
        fast = params.get("fast", self._fast)
        slow = params.get("slow", self._slow)

        df["ema_fast"] = df["Close"].ewm(span=fast, adjust=False).mean()
        df["ema_slow"] = df["Close"].ewm(span=slow, adjust=False).mean()

        df["signal"] = 0
        df.loc[df["ema_fast"] > df["ema_slow"], "signal"] =  1
        df.loc[df["ema_fast"] < df["ema_slow"], "signal"] = -1

        # Confidence: normalized EMA spread
        spread = (df["ema_fast"] - df["ema_slow"]).abs() / df["ema_slow"]
        rolling_max = spread.rolling(slow).max().replace(0, float("nan"))
        df["confidence"] = (spread / rolling_max).clip(0, 1).fillna(0.5)

        return df
