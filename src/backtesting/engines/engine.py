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
SNR scaling — must be handled by the model adapter *before* calling this
engine.  The engine is intentionally agnostic to the source of ``signal``.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from backtesting.core.base_engine import BaseEngine

logger = logging.getLogger(__name__)


class GenericBacktestEngine(BaseEngine):
    """Model-agnostic trade-execution simulator.

    Args:
        config: Parsed backtest-settings dictionary. Must contain the
            sections ``trading_parameters``, ``risk_management``,
            ``parameter_scaling``, and ``execution_costs`` as defined in
            ``configs/backtest_settings.toml``.

    Example::

        engine = GenericBacktestEngine(config=loader.get_backtest_settings())
        trades = engine.run_backtest(signal_df, dynamic_params)
    """
    def __init__(self, config: dict[str, Any]) -> None:
        self.trading_parameters = config["trading_parameters"]
        self.risk_parameters = config["risk_management"]
        self.scaling_parameters = config["parameter_scaling"]
        self.execution_costs = config["execution_costs"]

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
            ``result``, ``PnL``, ``equity``, ``drawdown``.
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

                adx_val = float(curr.get("ADX", 0))
                adx_thr = self.trading_parameters.get("adx_threshold", 30)
                adx_boost_mult = self.scaling_parameters.get("adx_boost_threshold_multiplier", 1.2)
                hold_boost_ratio = self.scaling_parameters.get("max_hold_boost_ratio", 1.5)

                current_max_hold = dynamic_params["max_hold"]
                if adx_val > adx_thr * adx_boost_mult:
                    current_max_hold *= hold_boost_ratio

                if signal == 1:
                    sl_price = min(
                        entry_price * (1.0 - dynamic_params["sl_long"]),
                        float(curr["dynamic_resistance"]),
                    )
                    active_position = {
                        "position_type": "Long",
                        "entry_price": entry_price,
                        "entry_time": nxt_time,
                        "tp_price": entry_price * (1.0 + dynamic_params["tp_long"]),
                        "sl_price": sl_price,
                        "hwm": entry_price,
                        "tp_target": dynamic_params["tp_long"],
                        "sl_target": dynamic_params["sl_long"],
                        "trail_start": dynamic_params["trailing_start_long"],
                        "max_hold": current_max_hold,
                    }
                    is_in_position = True

                elif signal == -1:
                    sl_price = max(
                        entry_price * (1.0 + dynamic_params["sl_short"]),
                        float(curr["dynamic_support"]),
                    )
                    active_position = {
                        "position_type": "Short",
                        "entry_price": entry_price,
                        "entry_time": nxt_time,
                        "tp_price": entry_price * (1.0 - dynamic_params["tp_short"]),
                        "sl_price": sl_price,
                        "lwm": entry_price,
                        "tp_target": dynamic_params["tp_short"],
                        "sl_target": dynamic_params["sl_short"],
                        "trail_start": dynamic_params["trailing_start_short"],
                        "max_hold": current_max_hold,
                    }
                    is_in_position = True

        return self._build_results(trades)

    def build_dynamic_params(
        self,
        estimated_params: dict[str, Any],
        ref_sigma: float | None = None,
        ) -> dict[str, Any]:
        """Scale execution parameters by the model's SNR and volatility.

        Derives trade-specific TP / SL / trailing / max-hold values from
        the model's estimated signal-to-noise ratio (SNR) so that each
        walk-forward window adapts its risk profile to current market
        conditions.

        Args:
            estimated_params: Model parameter dict from ``BaseModel.fit``.
                Expected keys: ``alpha_long``, ``alpha_short``, ``sigma_1``.
                Falls back to config defaults when keys are absent.
            ref_sigma: Per-window reference sigma computed from EMA of
                preceding training slice volatility. When provided, overrides
                the fixed ``reference_sigma_1`` value in config. Falls back
                to config default when ``None``.

        Returns:
            Dictionary of scaled execution parameters compatible with
            :meth:`run_backtest`.
        """
        ref_sigma = ref_sigma or self.risk_parameters.get("reference_sigma_1", 14.665)
        sigma_1 = estimated_params.get("sigma_1", ref_sigma)
        vol_quality = sigma_1 / ref_sigma

        snr_long = estimated_params.get("alpha_long", 30.0) / sigma_1
        snr_short = estimated_params.get("alpha_short", 20.0) / sigma_1

        scale = self.scaling_parameters
        snr_div = scale["snr_divisor"]
        tp = self.trading_parameters

        return {
            "tp_long": tp["tp_long"] * float(
                np.clip(snr_long / snr_div, *scale["tp_long_clip"])
            ),
            "sl_long": tp["sl_long"] * vol_quality * float(
                np.clip(1.0 / (snr_long / snr_div), *scale["sl_long_clip"])
            ),
            "tp_short": tp["tp_short"] * float(
                np.clip(snr_short / snr_div, *scale["tp_short_clip"])
            ),
            "sl_short": tp["sl_short"] * vol_quality * float(
                np.clip(
                    scale["sl_short_numerator"] / snr_short,
                    *scale["sl_short_clip"],
                )
            ),
            "max_hold": max(
                scale["min_hold_hours"],
                tp["max_hold_hours"] * vol_quality,
            ),
            "trailing_start_long": (
                tp["trailing_stop_start_ratio"] * vol_quality
            ),
            "trailing_start_short": (
                tp["trailing_stop_start_ratio"]
                * scale["short_trailing_multiplier"]
                * vol_quality
            ),
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
                pos["sl_price"] = max(pos["sl_price"], entry * 1.0005)

            if pos["hwm"] >= entry * (1.0 + pos["trail_start"]):
                pos["sl_price"] = max(
                    pos["sl_price"],
                    pos["hwm"] * (1.0 - pos["sl_target"]),
                )

            if float(nxt["Low"]) <= pos["sl_price"]:
                return self._trade_record(
                    (pos["sl_price"] - entry) / entry - cost,
                    entry_time, nxt_time, "Long", "StopLoss",
                )
            if float(nxt["High"]) >= pos["tp_price"]:
                return self._trade_record(
                    pos["tp_target"] - cost,
                    entry_time, nxt_time, "Long", "Win",
                )

        elif pos_type == "Short":
            pos["lwm"] = min(pos["lwm"], float(nxt["Low"]))

            be_ratio = risk["break_even_trigger_ratio_short"]
            if float(nxt["Low"]) <= entry * (1.0 - pos["tp_target"] * be_ratio):
                pos["sl_price"] = min(pos["sl_price"], entry * 0.9995)

            if pos["lwm"] <= entry * (1.0 - pos["trail_start"]):
                pos["sl_price"] = min(
                    pos["sl_price"],
                    pos["lwm"] * (1.0 + pos["sl_target"]),
                )

            if float(nxt["High"]) >= pos["sl_price"]:
                return self._trade_record(
                    (entry - pos["sl_price"]) / entry - cost,
                    entry_time, nxt_time, "Short", "StopLoss",
                )
            if float(nxt["Low"]) <= pos["tp_price"]:
                return self._trade_record(
                    pos["tp_target"] - cost,
                    entry_time, nxt_time, "Short", "Win",
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

    @staticmethod
    def _trade_record(
        pnl: float,
        entry_time: pd.Timestamp,
        exit_time: pd.Timestamp,
        trade_type: str,
        result: str,
        ) -> dict[str, Any]:
        return {
            "PnL": pnl,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "type": trade_type,
            "result": result,
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
