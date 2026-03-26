"""
backtesting.engines.performance
================================
Quantitative performance-metric calculator for completed backtest trades.

Decouples statistical analysis from visualisation.  All methods are static
so the class acts as a pure function namespace — no state is held.

Metrics computed
----------------
* Total return and CAGR
* Maximum drawdown and average recovery time
* Sharpe ratio (annualised, trade-frequency-adjusted)
* Sortino ratio (downside deviation)
* Win rate
* Statistical significance (one-sample t-test against zero mean)
* Rank Information Coefficient (Spearman IC) across multiple horizons
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import spearmanr

logger = logging.getLogger(__name__)


class PerformanceAnalyzer:
    """Static utility class for backtest performance metrics.

    All public methods accept a trades ``DataFrame`` (as returned by
    :class:`~backtesting.engines.engine.GenericBacktestEngine`) and return
    structured results suitable for logging or further processing.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_metrics(
        trades_df: pd.DataFrame,
        ) -> tuple[dict[str, Any] | None, pd.Series | None, pd.Series | None]:
        """Compute comprehensive performance statistics from trade-level data.

        Args:
            trades_df: ``DataFrame`` with at minimum ``PnL`` (``float``)
                and ``exit_time`` (``Timestamp``) columns.

        Returns:
            A 3-tuple ``(metrics, equity_curve, drawdown)``.  All three
            elements are ``None`` when *trades_df* is empty.
        """
        if trades_df is None or trades_df.empty:
            logger.warning("PerformanceAnalyzer received an empty trades DataFrame.")
            return None, None, None

        df = trades_df.copy()
        df["exit_time"] = pd.to_datetime(df["exit_time"])
        df = df.sort_values("exit_time").reset_index(drop=True)

        equity: pd.Series = (1.0 + df["PnL"]).cumprod()
        running_max = equity.cummax()
        drawdown: pd.Series = (equity - running_max) / running_max

        total_trades = len(df)
        total_return = (equity.iloc[-1] - 1.0) * 100.0

        duration_days = max(
            (df["exit_time"].max() - df["exit_time"].min()).days, 1
        )
        annualised_return = (
            (1.0 + total_return / 100.0) ** (365.0 / duration_days) - 1.0
        ) * 100.0

        max_drawdown = drawdown.min() * 100.0
        avg_recovery_days = PerformanceAnalyzer._average_recovery_days(
            drawdown, duration_days, total_trades
        )

        mean_pnl = df["PnL"].mean()
        std_pnl = df["PnL"].std()
        ann_factor = np.sqrt(total_trades / max(duration_days / 365.0, 1e-9))

        sharpe = (mean_pnl / std_pnl) * ann_factor if std_pnl > 0 else 0.0
        downside_std = df.loc[df["PnL"] < 0, "PnL"].std()
        sortino = (
            (mean_pnl / downside_std) * ann_factor if downside_std > 0 else 0.0
        )

        t_stat, p_value = stats.ttest_1samp(df["PnL"], 0)
        win_rate = (df["PnL"] > 0).mean() * 100.0
        avg_pnl = mean_pnl * 100.0

        metrics = {
            "total_trades": total_trades,
            "total_return_pct": total_return,
            "annualised_return_pct": annualised_return,
            "max_drawdown_pct": max_drawdown,
            "sharpe_ratio": sharpe,
            "sortino_ratio": sortino,
            "t_stat": t_stat,
            "p_value": p_value,
            "avg_recovery_days": avg_recovery_days,
            "win_rate_pct": win_rate,
            "avg_pnl_pct": avg_pnl,
        }

        return metrics, equity, drawdown

    @staticmethod
    def print_report(metrics: dict[str, Any] | None) -> None:
        """Print a formatted ASCII performance report to stdout.

        Args:
            metrics: Dictionary returned by :meth:`calculate_metrics`.
                     Prints a warning and returns early when ``None``.
        """
        if not metrics:
            print("⚠️  No metrics available — trades DataFrame was empty.")
            return

        _LABEL_MAP = [
            ("total_trades",          "Total Executed Trades"),
            ("total_return_pct",      "Total Return (%)"),
            ("annualised_return_pct", "Annualised Return (%)"),
            ("max_drawdown_pct",      "Max Drawdown (%)"),
            ("sharpe_ratio",          "Sharpe Ratio"),
            ("sortino_ratio",         "Sortino Ratio"),
            ("win_rate_pct",          "Win Rate (%)"),
            ("avg_pnl_pct",           "Avg PnL per Trade (%)"),
            ("avg_recovery_days",     "Avg Recovery Time (days)"),
            ("t_stat",                "T-statistic"),
        ]

        print("\n" + "=" * 70)
        print(f"{'STRATEGY PERFORMANCE REPORT':^70}")
        print("=" * 70)

        for key, label in _LABEL_MAP:
            if key not in metrics:
                continue
            val = metrics[key]
            if key == "t_stat":
                p_val = metrics.get("p_value", 1.0)
                stars = "**" if p_val < 0.01 else ("*" if p_val < 0.05 else "")
                print(f"{label:<35}: {val:>8.2f}{stars}")
            elif isinstance(val, float | np.floating):
                print(f"{label:<35}: {val:>8.2f}")
            else:
                print(f"{label:<35}: {val:>8}")

        print("=" * 70)
        print("(* p<0.05  ** p<0.01)")
        print("=" * 70 + "\n")

    @staticmethod
    def calculate_ic(
        signal_df: pd.DataFrame,
        signal_col: str,
        horizons: list[int] | None = None,
        ) -> pd.DataFrame:
        """Compute Spearman rank IC across multiple forward horizons.

        Measures the predictive relationship between *signal_col* and
        realised absolute log-returns *n* bars ahead.

        Args:
            signal_df:  ``DataFrame`` with ``Close`` and *signal_col* columns.
            signal_col: Name of the signal / confidence column to evaluate.
            horizons:   List of forward-bar counts.  Defaults to
                        ``[1, 5, 10, 20, 50]``.

        Returns:
            ``DataFrame`` with columns ``horizon``, ``ic``, ``p_value``,
            ``significant`` indexed by horizon.
        """
        if horizons is None:
            horizons = [1, 5, 10, 20, 50]

        records = []
        for h in horizons:
            fwd_vol = (
                np.log(signal_df["Close"].shift(-h) / signal_df["Close"]).abs()
            )
            combined = pd.concat(
                [signal_df[signal_col], fwd_vol], axis=1
            ).dropna()

            if len(combined) < 30:  # noqa: PLR2004
                records.append(
                    {
                        "horizon": h,
                        "ic": np.nan,
                        "p_value": np.nan,
                        "significant": False,
                    }
                )
                continue

            ic_val, p_val = spearmanr(combined.iloc[:, 0], combined.iloc[:, 1])
            records.append(
                {
                    "horizon": h,
                    "ic": round(float(ic_val), 4),
                    "p_value": round(float(p_val), 4),
                    "significant": bool(p_val < 0.05),
                }
            )

        result = pd.DataFrame(records).set_index("horizon")
        logger.info("IC analysis complete for signal '%s':\n%s", signal_col, result)

        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _average_recovery_days(
        drawdown: pd.Series,
        duration_days: int,
        total_trades: int,
        ) -> float:
        """Estimate average calendar days to recover from drawdown periods.

        Args:
            drawdown:     Drawdown series (values ≤ 0).
            duration_days: Total backtest duration in calendar days.
            total_trades:  Total number of trades (used to convert trade
                           counts to approximate days).

        Returns:
            Average recovery time in calendar days.
        """
        in_dd = drawdown < 0.0
        recovery_lengths: list[int] = []
        current: int = 0

        for flag in in_dd:
            if flag:
                current += 1
            else:
                if current > 0:
                    recovery_lengths.append(current)
                current = 0

        if not recovery_lengths:
            return 0.0

        trades_per_day = max(total_trades / duration_days, 1e-9)

        return float(np.mean(recovery_lengths) / trades_per_day)
