"""
backtesting.engines.engine
==========================
Generic trade-execution simulator.

Reads ``signal`` (1 = Long / -1 = Short / 0 = Flat) and ``confidence``
from the model-adapter output and simulates order execution against
historical OHLCV prices.  The engine implements:

* Asymmetric take-profit and stop-loss thresholds.
* Break-even stop migration once a configurable profit ratio is reached.
* Trailing stop activation above a configurable profit threshold.
* Maximum holding-period time-out.
* Round-trip execution cost deduction (commission + slippage).

All model-specific logic — regime detection, ADX filtering, zone selection,
— must be handled by the model adapter *before* calling this engine.
The engine is intentionally agnostic to the source of ``signal``.
"""
from __future__ import annotations

import logging
from typing import Any
import pandas as pd

from backtesting.assets.crypto.funding_rate import FundingRateManager
from backtesting.core.base_engine import BaseEngine

logger = logging.getLogger(__name__)


class GenericBacktestEngine(BaseEngine):
    """Model-agnostic trade-execution simulator.

    Args:
        config: Parsed backtest-settings dictionary. Must contain the
            sections ``trading_parameters``, ``risk_management``,
            and ``execution_costs`` as defined in ``configs/backtest_settings.toml``.

    Example::

        engine = GenericBacktestEngine(config=loader.get_backtest_settings())
        trades = engine.run_backtest(signal_df, dynamic_params)
    """
    def __init__(
        self,
        config: dict[str, Any],
        funding_rate_manager: FundingRateManager | None = None,
        symbol: str | None = None,
        ) -> None:
        self.trading_parameters  = config["trading_parameters"]
        self.risk_parameters     = config["risk_management"]
        self.execution_costs     = config["execution_costs"]
        self._funding_mgr        = funding_rate_manager
        self._symbol             = symbol

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_backtest(
        self,
        price_data: pd.DataFrame,
        dynamic_params: dict[str, Any],
        ) -> pd.DataFrame:
        """Simulate trade execution over *price_data*.

        Iterates bar-by-bar. An entry is triggered when ``signal != 0``
        and there is no open position. Exits are evaluated on the *next*
        bar's ``High`` / ``Low`` / ``Close`` to avoid look-ahead bias.

        Args:
            price_data:     ``DataFrame`` with a ``DatetimeIndex`` and at
                            minimum ``Open``, ``High``, ``Low``, ``Close``,
                            ``signal`` (int: 1 / -1 / 0), ``confidence``
                            (float: 0-1).
            dynamic_params: Execution parameters. Expected keys:
                            ``tp_long``, ``sl_long``, ``tp_short``,
                            ``sl_short``, ``max_hold``,
                            ``trailing_start_long``, ``trailing_start_short``.

        Returns:
            ``DataFrame`` of completed trades. Empty if no trades were
            triggered. Contains: ``entry_time``, ``exit_time``, ``type``,
            ``result``, ``exit_reason``, ``outcome``, ``PnL``, ``equity``, ``drawdown``.
        """
        round_trip_cost = (
            self.execution_costs["commission_rate"]
            + self.execution_costs["slippage_rate"]
        ) * 2

        trades: list[dict[str, Any]] = []
        is_in_position = False
        active_position: dict[str, Any] = {}

        for i in range(len(price_data) - 1):
            curr = price_data.iloc[i]
            nxt = price_data.iloc[i + 1]
            nxt_time = price_data.index[i + 1]

            if is_in_position:
                exit_result = self._evaluate_exit(
                    active_position, nxt, nxt_time, round_trip_cost
                )
                if exit_result is not None:
                    trades.append(exit_result)
                    is_in_position = False

            else:
                signal = int(curr.get("signal", 0))
                if signal == 0:
                    continue

                entry_price = float(nxt["Open"])

                if signal == 1:
                    sl_price = entry_price * (1.0 - dynamic_params["sl_long"])
                    active_position = {
                        "position_type": "Long",
                        "entry_price": entry_price,
                        "entry_time": nxt_time,
                        "tp_price": entry_price * (1.0 + dynamic_params["tp_long"]),
                        "sl_price": sl_price,
                        "stop_state": "StopLoss",
                        "hwm": entry_price,
                        "tp_target": dynamic_params["tp_long"],
                        "sl_target": dynamic_params["sl_long"],
                        "trail_start": dynamic_params["trailing_start_long"],
                        "max_hold": self.trading_parameters.get("max_hold_hours", 720),
                    }
                    is_in_position = True

                elif signal == -1:
                    sl_price = entry_price * (1.0 + dynamic_params["sl_short"])
                    active_position = {
                        "position_type": "Short",
                        "entry_price": entry_price,
                        "entry_time": nxt_time,
                        "tp_price": entry_price * (1.0 - dynamic_params["tp_short"]),
                        "sl_price": sl_price,
                        "stop_state": "StopLoss",
                        "lwm": entry_price,
                        "tp_target": dynamic_params["tp_short"],
                        "sl_target": dynamic_params["sl_short"],
                        "trail_start": dynamic_params["trailing_start_short"],
                        "max_hold": self.trading_parameters.get("max_hold_hours", 720),
                    }
                    is_in_position = True

        return self._build_results(trades)

    def get_fixed_params(self) -> dict[str, Any]:
        """Return fixed execution parameters from config.

        Used by all models. Returns raw config values without any
        vol_quality or SNR adjustment — scaling is handled entirely
        by the model adapter before signals reach the engine.

        Returns:
            Dictionary of execution parameters compatible with
            :meth:`run_backtest`.
        """
        tp    = self.trading_parameters

        return {
            "tp_long":              tp["tp_long"],
            "sl_long":              tp["sl_long"],
            "tp_short":             tp["tp_short"],
            "sl_short":             tp["sl_short"],
            "max_hold":             tp["max_hold_hours"],
            "trailing_start_long":  tp["trailing_stop_start_ratio"],
            "trailing_start_short": tp["trailing_stop_start_ratio"],
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _evaluate_exit(
        self,
        pos: dict[str, Any],
        nxt: pd.Series,
        nxt_time: pd.Timestamp,
        cost: float,
        ) -> dict[str, Any] | None:
        """Check all exit conditions for the current open position.

        Args:
            pos:      Active position dict (mutated in-place for stop adjustments).
            nxt:      Next bar's OHLCV ``Series``.
            nxt_time: Timestamp of the next bar.
            cost:     Round-trip execution cost ratio.

        Returns:
            Completed trade ``dict`` if an exit was triggered, else ``None``.
        """
        pos_type = pos["position_type"]
        entry = pos["entry_price"]
        entry_time = pos["entry_time"]
        risk = self.risk_parameters

        if pos_type == "Long":
            pos["hwm"] = max(pos["hwm"], float(nxt["High"]))

            be_ratio = risk["break_even_trigger_ratio_long"]
            if float(nxt["High"]) >= entry * (1.0 + pos["tp_target"] * be_ratio):
                be_stop = entry * 1.0005
                if be_stop > pos["sl_price"]:
                    pos["sl_price"] = be_stop
                    pos["stop_state"] = "BreakEven"

            if pos["hwm"] >= entry * (1.0 + pos["trail_start"]):
                trailing_stop = pos["hwm"] * (1.0 - pos["sl_target"])
                if trailing_stop > pos["sl_price"]:
                    pos["sl_price"] = trailing_stop
                    pos["stop_state"] = "TrailingStop"

            if float(nxt["Low"]) <= pos["sl_price"]:
                return self._trade_record(
                    (pos["sl_price"] - entry) / entry - cost,
                    entry_time, nxt_time, "Long", pos.get("stop_state", "StopLoss"),
                )
            if float(nxt["High"]) >= pos["tp_price"]:
                return self._trade_record(
                    pos["tp_target"] - cost,
                    entry_time, nxt_time, "Long", "TakeProfit",
                )

        elif pos_type == "Short":
            pos["lwm"] = min(pos["lwm"], float(nxt["Low"]))

            be_ratio = risk["break_even_trigger_ratio_short"]
            if float(nxt["Low"]) <= entry * (1.0 - pos["tp_target"] * be_ratio):
                be_stop = entry * 0.9995
                if be_stop < pos["sl_price"]:
                    pos["sl_price"] = be_stop
                    pos["stop_state"] = "BreakEven"

            if pos["lwm"] <= entry * (1.0 - pos["trail_start"]):
                trailing_stop = pos["lwm"] * (1.0 + pos["sl_target"])
                if trailing_stop < pos["sl_price"]:
                    pos["sl_price"] = trailing_stop
                    pos["stop_state"] = "TrailingStop"

            if float(nxt["High"]) >= pos["sl_price"]:
                return self._trade_record(
                    (entry - pos["sl_price"]) / entry - cost,
                    entry_time, nxt_time, "Short", pos.get("stop_state", "StopLoss"),
                )
            if float(nxt["Low"]) <= pos["tp_price"]:
                return self._trade_record(
                    pos["tp_target"] - cost,
                    entry_time, nxt_time, "Short", "TakeProfit",
                )

        elapsed_hours = (nxt_time - entry_time).total_seconds() / 3600
        if elapsed_hours > pos["max_hold"]:
            pnl_raw = (
                (float(nxt["Close"]) - entry) / entry
                if pos_type == "Long"
                else (entry - float(nxt["Close"])) / entry
            )
            return self._trade_record(
                pnl_raw - cost, entry_time, nxt_time, pos_type, "TimeOut"
            )

        return None

    def _trade_record(
        self,
        pnl: float,
        entry_time: pd.Timestamp,
        exit_time: pd.Timestamp,
        trade_type: str,
        exit_reason: str,
        ) -> dict[str, Any]:
        """Create a completed-trade record with explicit exit labels.

        ``exit_reason`` records the mechanical reason the engine closed
        the position: ``TakeProfit``, ``StopLoss``, ``BreakEven``,
        ``TrailingStop``, or ``TimeOut``. ``outcome`` is derived from
        final net PnL after funding and execution costs, so a trailing
        stop may still be a profitable outcome.
        """
        funding_cost = 0.0
        if self._funding_mgr is not None and self._symbol is not None:
            funding_cost = self._funding_mgr.get_funding_cost(
                symbol        = self._symbol,
                entry_time    = entry_time,
                exit_time     = exit_time,
                position_type = trade_type,
            )

        net_pnl = pnl - funding_cost
        if net_pnl > 0:
            outcome = "Win"
        elif net_pnl < 0:
            outcome = "Loss"
        else:
            outcome = "Flat"

        return {
            "PnL":          net_pnl,
            "entry_time":   entry_time,
            "exit_time":    exit_time,
            "type":         trade_type,
            "result":       exit_reason,
            "exit_reason":  exit_reason,
            "outcome":      outcome,
            "funding_cost": round(funding_cost, 8),
        }

    @staticmethod
    def _build_results(trades: list[dict[str, Any]]) -> pd.DataFrame:
        """Construct the final trades ``DataFrame`` with equity curve.

        Args:
            trades: List of completed trade dicts.

        Returns:
            Sorted ``DataFrame`` with ``equity`` and ``drawdown`` columns.
            Returns an empty ``DataFrame`` when no trades fired.
        """
        if not trades:
            return pd.DataFrame()

        df = (
            pd.DataFrame(trades)
            .sort_values("entry_time")
            .reset_index(drop=True)
        )
        df["equity"] = (1.0 + df["PnL"]).cumprod()
        df["drawdown"] = (
            (df["equity"] - df["equity"].cummax()) / df["equity"].cummax()
        )

        return df
