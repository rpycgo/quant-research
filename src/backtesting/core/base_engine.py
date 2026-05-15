"""
backtesting.core.base_engine
============================
Abstract base class for all backtest execution engines.

An engine receives a price ``DataFrame`` that already contains ``signal``
and ``confidence`` columns produced by a model adapter, and simulates
trade execution against historical prices.  It knows nothing about the
model that generated the signals.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
import pandas as pd


class BaseEngine(ABC):
    """Abstract interface for trade-simulation engines.

    The engine is the only layer that may access ``Open``, ``High``,
    ``Low``, ``Close`` columns directly for execution purposes (entry
    price, stop triggers, trailing stops, time-outs).  It must **not**
    read any model-specific columns beyond ``signal`` and ``confidence``.

    Example::

        class MyEngine(BaseEngine):
            def run_backtest(
                self,
                price_data: pd.DataFrame,
                dynamic_params: dict,
            ) -> pd.DataFrame:
                trades = []
                ...
                return pd.DataFrame(trades)
    """
    @abstractmethod
    def run_backtest(
        self,
        price_data: pd.DataFrame,
        dynamic_params: dict,
    ) -> pd.DataFrame:
        """Simulate trade execution against historical prices.

        Args:
            price_data: ``DataFrame`` with a ``DatetimeIndex`` containing
                at minimum the columns ``Open``, ``High``, ``Low``,
                ``Close``, ``signal`` (``int``: 1 / -1 / 0), and
                ``confidence`` (``float``: 0–1).
            dynamic_params: Execution parameters dict.  Expected keys:

                * ``tp_long``              (float) – take-profit ratio for longs.
                * ``sl_long``              (float) – stop-loss ratio for longs.
                * ``tp_short``             (float) – take-profit ratio for shorts.
                * ``sl_short``             (float) – stop-loss ratio for shorts.
                * ``max_hold``             (float) – maximum holding period in hours.
                * ``trailing_start_long``  (float) – trailing-stop activation ratio.
                * ``trailing_start_short`` (float) – trailing-stop activation ratio.

        Returns:
            ``DataFrame`` where each row represents a completed trade.
            Must contain at minimum:

            * ``entry_time`` (``Timestamp``)
            * ``exit_time``  (``Timestamp``)
            * ``type``       (``str``)   – ``"Long"`` or ``"Short"``
            * ``result``     (``str``)   – exit reason: ``"TakeProfit"``, ``"StopLoss"``, ``"BreakEven"``, ``"TrailingStop"``, or ``"TimeOut"``
            * ``exit_reason`` (``str``)  – same as ``result`` for explicit downstream use
            * ``outcome``    (``str``)   – ``"Win"``, ``"Loss"``, or ``"Flat"`` derived from net PnL
            * ``PnL``        (``float``) – net return after execution costs
            * ``equity``     (``float``) – cumulative equity curve
            * ``drawdown``   (``float``) – drawdown from running maximum
        """
        pass
