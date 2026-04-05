"""
backtesting.benchmarks.buy_and_hold
=====================================
Passive buy-and-hold baseline benchmark.

Computes daily-resampled performance metrics (Sharpe, Sortino, MDD,
CAGR, T-stat) over the same WFA period used by the strategy models,
ensuring a fair comparison.

Ported from mdrs-sde to quant-research so all benchmarks are managed
in a single execution platform.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class BuyAndHoldBenchmark:
    """Passive buy-and-hold performance calculator.

    Args:
        start: WFA start date (ISO-8601 string).
        end:   WFA end date   (ISO-8601 string).

    Example::

        bnh = BuyAndHoldBenchmark(start="2020-04-01", end="2026-01-31")
        metrics = bnh.run(price_df)
        bnh.print_report(metrics)
    """
    def __init__(self, start: str, end: str) -> None:
        self._start = start
        self._end = end

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, df: pd.DataFrame) -> dict:
        """Compute buy-and-hold performance metrics.

        Args:
            df: OHLCV ``DataFrame`` with at minimum a ``Close`` column
                and a ``DatetimeIndex``.

        Returns:
            Performance metrics dictionary with the same schema as
            ``PerformanceAnalyzer.calculate_metrics()`` for direct comparison.
        """
        data = df.loc[self._start:self._end].copy()
        if data.empty:
            logger.error("No data for period %s - %s", self._start, self._end)
            return {}

        daily_price = data["Close"].resample("D").last().ffill()
        daily_ret   = daily_price.pct_change().dropna()

        total_days   = max((daily_price.index[-1] - daily_price.index[0]).days, 1)
        total_return = daily_price.iloc[-1] / daily_price.iloc[0] - 1.0
        annual_return = (1.0 + total_return) ** (365.0 / total_days) - 1.0

        sharpe  = (daily_ret.mean() / daily_ret.std()) * np.sqrt(365)
        down_ret = daily_ret[daily_ret < 0]
        sortino = (daily_ret.mean() / down_ret.std()) * np.sqrt(365)

        rolling_max = daily_price.cummax()
        drawdown    = (daily_price - rolling_max) / rolling_max
        mdd         = drawdown.min()

        t_stat, p_value = stats.ttest_1samp(daily_ret, 0)

        in_dd     = drawdown < 0
        groups    = (in_dd != in_dd.shift()).cumsum()
        durations = in_dd.groupby(groups).sum()
        durations = durations[durations > 0]
        avg_recovery = float(durations.mean()) if not durations.empty else 0.0

        return {
            "total_trades":          len(daily_ret),
            "total_return_pct":      total_return * 100.0,
            "annualised_return_pct": annual_return * 100.0,
            "max_drawdown_pct":      mdd * 100.0,
            "sharpe_ratio":          sharpe,
            "sortino_ratio":         sortino,
            "win_rate_pct":          float((daily_ret > 0).mean() * 100),
            "avg_pnl_pct":           float(daily_ret.mean() * 100),
            "avg_recovery_days":     avg_recovery,
            "t_stat":                t_stat,
            "p_value":               p_value,
        }

    @staticmethod
    def print_report(metrics: dict, symbol: str = "") -> None:
        """Print a formatted buy-and-hold performance report.

        Args:
            metrics: Dict returned by :meth:`run`.
            symbol:  Optional asset symbol for the report header.
        """
        if not metrics:
            print("No metrics available.")
            return

        _LABELS = [
            ("total_trades",          "Total Trading Days"),
            ("total_return_pct",      "Total Return (%)"),
            ("annualised_return_pct", "Annualised Return (%)"),
            ("max_drawdown_pct",      "Max Drawdown (%)"),
            ("sharpe_ratio",          "Sharpe Ratio"),
            ("sortino_ratio",         "Sortino Ratio"),
            ("win_rate_pct",          "Win Rate (%)"),
            ("avg_pnl_pct",           "Avg PnL per Day (%)"),
            ("avg_recovery_days",     "Avg Recovery Time (days)"),
            ("t_stat",                "T-statistic"),
        ]

        title = f"BENCHMARK: BUY-AND-HOLD{' — ' + symbol if symbol else ''}"
        p = metrics.get("p_value", 1.0)
        print("\n" + "=" * 70)
        print(f"{title:^70}")
        print("=" * 70)
        for key, label in _LABELS:
            if key not in metrics:
                continue
            val = metrics[key]
            suffix = ""
            if key == "t_stat":
                suffix = "**" if p < 0.01 else ("*" if p < 0.05 else "")
            if isinstance(val, (float, np.floating)):
                print(f"{label:<35}: {val:>10.2f}{suffix}")
            else:
                print(f"{label:<35}: {val:>10}{suffix}")
        print("=" * 70 + "\n")
