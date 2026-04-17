"""
backtesting.benchmarks.statistical_validator
=============================================
Statistical validation tools for walk-forward backtest results.

Addresses the low-trade-count concern by providing three complementary
validation methods:

1. **Bootstrap CI** — resamples trade-level PnL to construct confidence
   intervals for Sharpe, Total Return, and MDD. Demonstrates that
   performance is not driven by a small number of outlier trades.

2. **Permutation test** — shuffles trade PnL to build a null distribution
   and computes a p-value against the observed Sharpe. Demonstrates that
   the signal-return relationship is not due to chance.

3. **Subperiod analysis** — splits the OOS period into three market regime
   sub-periods and computes key metrics for each. Demonstrates robustness
   across different market conditions.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class StatisticalValidator:
    """Statistical validation tools for walk-forward backtest results.

    All methods are static — no state is held.

    Example::

        trades = pd.read_csv("results/mdrs_sde_btc_trades.csv")
        trades["entry_time"] = pd.to_datetime(trades["entry_time"])
        trades["exit_time"]  = pd.to_datetime(trades["exit_time"])

        ci     = StatisticalValidator.bootstrap_ci(trades)
        pval   = StatisticalValidator.permutation_test(trades)
        sub    = StatisticalValidator.subperiod_analysis(trades)

        StatisticalValidator.print_report(ci, pval, sub)
    """

    # ------------------------------------------------------------------
    # Bootstrap CI
    # ------------------------------------------------------------------

    @staticmethod
    def bootstrap_ci(
        trades_df: pd.DataFrame,
        n_bootstrap: int = 10_000,
        confidence: float = 0.95,
        random_state: int = 42,
        ) -> dict[str, Any]:
        """Compute bootstrap confidence intervals for key metrics.

        Resamples trade-level PnL with replacement *n_bootstrap* times and
        computes Sharpe, Total Return, and MDD for each sample. Reports the
        observed value and the bootstrap CI.

        Args:
            trades_df:    Trades ``DataFrame`` with ``PnL`` column.
            n_bootstrap:  Number of bootstrap resamples (default 10,000).
            confidence:   Confidence level (default 0.95 → 95% CI).
            random_state: Random seed for reproducibility.

        Returns:
            Dict with keys ``sharpe``, ``total_return``, ``mdd``.
            Each value is a dict with ``observed``, ``ci_lower``, ``ci_upper``.
        """
        rng     = np.random.default_rng(random_state)
        pnl     = trades_df["PnL"].values
        n       = len(pnl)
        alpha   = 1.0 - confidence

        sharpes      : list[float] = []
        total_returns: list[float] = []
        mdds         : list[float] = []

        # Annualisation factor: consistent with PerformanceAnalyzer
        # ann_factor = sqrt(n_trades / (duration_years))
        if "entry_time" in trades_df.columns:
            entry = pd.to_datetime(trades_df["entry_time"])
            duration_days = (entry.max() - entry.min()).days
        else:
            duration_days = n * 1  # fallback: assume 1 day per trade
        duration_years = max(duration_days / 365.0, 1e-9)
        ann_factor = np.sqrt(n / duration_years)

        for _ in range(n_bootstrap):
            sample  = rng.choice(pnl, size=n, replace=True)
            equity  = np.cumprod(1.0 + sample)
            ret     = equity[-1] - 1.0
            mu, sigma = sample.mean(), sample.std()
            sh      = (mu / sigma * ann_factor) if sigma > 0 else 0.0
            peak    = np.maximum.accumulate(equity)
            dd      = ((equity - peak) / peak).min()

            sharpes.append(sh)
            total_returns.append(ret * 100.0)
            mdds.append(dd * 100.0)

        def _ci(arr: list[float], obs: float) -> dict[str, float]:
            lo = float(np.percentile(arr, alpha / 2 * 100))
            hi = float(np.percentile(arr, (1 - alpha / 2) * 100))

            return {"observed": obs, "ci_lower": lo, "ci_upper": hi}

        equity_obs  = np.cumprod(1.0 + pnl)
        peak_obs    = np.maximum.accumulate(equity_obs)
        mdd_obs     = ((equity_obs - peak_obs) / peak_obs).min() * 100.0
        ret_obs     = (equity_obs[-1] - 1.0) * 100.0
        mu_obs      = pnl.mean()
        sig_obs     = pnl.std()
        sharpe_obs  = (mu_obs / sig_obs * ann_factor) if sig_obs > 0 else 0.0

        logger.info(
            "Bootstrap CI (%d resamples, %.0f%% CI) complete.",
            n_bootstrap, confidence * 100,
        )

        return {
            "n_trades":     n,
            "n_bootstrap":  n_bootstrap,
            "confidence":   confidence,
            "sharpe":       _ci(sharpes,       sharpe_obs),
            "total_return": _ci(total_returns, ret_obs),
            "mdd":          _ci(mdds,          mdd_obs),
        }

    # ------------------------------------------------------------------
    # Permutation test
    # ------------------------------------------------------------------

    @staticmethod
    def permutation_test(
        trades_df: pd.DataFrame,
        n_permutations: int = 10_000,
        random_state: int = 42,
        ) -> dict[str, Any]:
        """Test whether observed Sharpe is significantly above chance.

        Shuffles trade PnL *n_permutations* times to build a null
        distribution and computes a one-sided p-value:
        P(null Sharpe >= observed Sharpe).

        Args:
            trades_df:      Trades ``DataFrame`` with ``PnL`` column.
            n_permutations: Number of permutations (default 10,000).
            random_state:   Random seed for reproducibility.

        Returns:
            Dict with ``observed_sharpe``, ``p_value``, ``null_mean``,
            ``null_std``, ``n_permutations``.
        """
        rng  = np.random.default_rng(random_state)
        pnl  = trades_df["PnL"].values
        n    = len(pnl)
        if "entry_time" in trades_df.columns:
            entry = pd.to_datetime(trades_df["entry_time"])
            duration_days = (entry.max() - entry.min()).days
        else:
            duration_days = n
        ann_factor = np.sqrt(n / max(duration_days / 365.0, 1e-9))
        mu   = pnl.mean()
        sig  = pnl.std()
        observed_sharpe = (mu / sig * ann_factor) if sig > 0 else 0.0

        null_sharpes: list[float] = []
        for _ in range(n_permutations):
            # Randomly flip signs of PnL to build null distribution.
            # Permuting order alone does not change mean/std/Sharpe.
            signs    = rng.choice([-1.0, 1.0], size=n)
            shuffled = pnl * signs
            s        = shuffled.std()
            sh       = (shuffled.mean() / s * ann_factor) if s > 0 else 0.0
            null_sharpes.append(sh)

        null_arr = np.array(null_sharpes)
        p_value  = float((null_arr >= observed_sharpe).mean())

        logger.info(
            "Permutation test (%d permutations): observed Sharpe=%.3f, p=%.4f",
            n_permutations, observed_sharpe, p_value,
        )

        return {
            "observed_sharpe": observed_sharpe,
            "p_value":         p_value,
            "null_mean":       float(null_arr.mean()),
            "null_std":        float(null_arr.std()),
            "n_permutations":  n_permutations,
        }

    # ------------------------------------------------------------------
    # Subperiod analysis
    # ------------------------------------------------------------------

    @staticmethod
    def subperiod_analysis(
        trades_df: pd.DataFrame,
        subperiods: list[tuple[str, str, str]] | None = None,
        ) -> pd.DataFrame:
        """Compute key metrics for each market-regime sub-period.

        Splits the OOS period into sub-periods representing distinct market
        conditions (bull, bear, recovery) and computes metrics for each.
        Demonstrates that performance is robust across different regimes.

        Args:
            trades_df:  Trades ``DataFrame`` with ``exit_time`` and
                        ``PnL`` columns.
            subperiods: List of (label, start, end) tuples (ISO-8601 dates).
                        Defaults to three crypto market regime periods.

        Returns:
            ``DataFrame`` with one row per sub-period and columns:
            ``trades``, ``total_return_pct``, ``sharpe``, ``mdd_pct``,
            ``win_rate_pct``, ``t_stat``, ``p_value``.
        """
        if subperiods is None:
            subperiods = [
                ("Bull + COVID Recovery", "2020-04-01", "2021-12-31"),
                ("Crypto Winter",         "2022-01-01", "2023-12-31"),
                ("Bull Run",              "2024-01-01", "2026-01-31"),
            ]

        df = trades_df.copy()
        df["exit_time"] = pd.to_datetime(df["exit_time"])

        records = []
        for label, start, end in subperiods:
            mask   = (df["exit_time"] >= start) & (df["exit_time"] <= end)
            subset = df[mask]

            if len(subset) < 2:
                records.append({
                    "period":           label,
                    "start":            start,
                    "end":              end,
                    "trades":           len(subset),
                    "total_return_pct": np.nan,
                    "sharpe":           np.nan,
                    "mdd_pct":          np.nan,
                    "win_rate_pct":     np.nan,
                    "t_stat":           np.nan,
                    "p_value":          np.nan,
                })
                continue

            pnl    = subset["PnL"].values
            equity = np.cumprod(1.0 + pnl)
            peak   = np.maximum.accumulate(equity)
            mdd    = ((equity - peak) / peak).min() * 100.0
            ret    = (equity[-1] - 1.0) * 100.0
            n      = len(pnl)
            mu, sigma = pnl.mean(), pnl.std()
            # Duration-based annualisation — consistent with PerformanceAnalyzer
            sub_entry = pd.to_datetime(subset["entry_time"])
            sub_days  = max((sub_entry.max() - sub_entry.min()).days, 1)
            sub_ann   = np.sqrt(n / max(sub_days / 365.0, 1e-9))
            sharpe = (mu / sigma * sub_ann) if sigma > 0 else 0.0
            win_rate = (pnl > 0).mean() * 100.0
            t_stat, p_value = stats.ttest_1samp(pnl, 0)

            records.append({
                "period":           label,
                "start":            start,
                "end":              end,
                "trades":           n,
                "total_return_pct": round(ret, 2),
                "sharpe":           round(sharpe, 3),
                "mdd_pct":          round(mdd, 2),
                "win_rate_pct":     round(win_rate, 2),
                "t_stat":           round(float(t_stat), 3),
                "p_value":          round(float(p_value), 4),
            })

        result = pd.DataFrame(records).set_index("period")
        logger.info("Subperiod analysis complete:\n%s", result)

        return result

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------

    @staticmethod
    def print_report(
        bootstrap: dict[str, Any],
        permutation: dict[str, Any],
        subperiods: pd.DataFrame,
        ) -> None:
        """Print formatted statistical validation report.

        Args:
            bootstrap:   Result from :meth:`bootstrap_ci`.
            permutation: Result from :meth:`permutation_test`.
            subperiods:  Result from :meth:`subperiod_analysis`.
        """
        ci_pct = int(bootstrap.get("confidence", 0.95) * 100)
        n_boot = bootstrap.get("n_bootstrap", 10_000)
        n_perm = permutation.get("n_permutations", 10_000)
        n_tr   = bootstrap.get("n_trades", "?")

        # Bootstrap CI
        print("\n" + "=" * 70)
        print(f"{'STATISTICAL VALIDATION REPORT':^70}")
        print("=" * 70)
        print(f"\n[1] Bootstrap CI  (n_trades={n_tr}, resamples={n_boot:,}, {ci_pct}% CI)")
        print("-" * 70)

        for key, label in [
            ("sharpe",       "Sharpe Ratio"),
            ("total_return", "Total Return (%)"),
            ("mdd",          "Max Drawdown (%)"),
        ]:
            v = bootstrap.get(key, {})
            obs = v.get("observed", float("nan"))
            lo  = v.get("ci_lower", float("nan"))
            hi  = v.get("ci_upper", float("nan"))
            print(f"  {label:<22}: {obs:>8.3f}   [{ci_pct}% CI: {lo:.3f}, {hi:.3f}]")

        # Permutation test
        obs_sh = permutation.get("observed_sharpe", float("nan"))
        p_val  = permutation.get("p_value", float("nan"))
        stars  = "**" if p_val < 0.01 else ("*" if p_val < 0.05 else "")
        print(f"\n[2] Permutation Test  (permutations={n_perm:,})")
        print("-" * 70)
        print(f"  Observed Sharpe    : {obs_sh:>8.3f}")
        print(f"  p-value            : {p_val:>8.4f}{stars}")
        print(f"  Null mean (Sharpe) : {permutation.get('null_mean', float('nan')):>8.3f}")
        print(f"  Null std  (Sharpe) : {permutation.get('null_std',  float('nan')):>8.3f}")

        # Subperiod analysis
        print(f"\n[3] Subperiod Analysis")
        print("-" * 70)
        cols = ["trades", "total_return_pct", "sharpe", "mdd_pct",
                "win_rate_pct", "t_stat", "p_value"]
        print(subperiods[cols].to_string())

        print("\n" + "=" * 70)
        print("(* p<0.05  ** p<0.01)")
        print("=" * 70 + "\n")
