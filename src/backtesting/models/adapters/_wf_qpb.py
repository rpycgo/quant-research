"""
backtesting.models.adapters._wf_qpb
====================================
Walk-forward QPB threshold estimator.

Computes QPB gate thresholds dynamically from recent completed trade
history rather than using fixed config-driven values. At each
re-estimation point the estimator performs a grid search over the
configured threshold space and selects the combination that maximises
a chosen objective on the lookback window.

The estimator runs as a *trade-level* post-processing layer inside
``WalkForwardRunner``. For this to be meaningful, the adapter-level
signal pipeline must bypass its static QPB gate when WF mode is
active — see ``_regime_gates.compute_qpb_mask`` for that logic.

Warm-up
-------
When fewer than ``min_lookback_trades`` have accumulated in the
lookback window, the estimator falls back to the static thresholds
recorded in ``[filters.qpb]`` (``past_vol_48b_max``,
``aligned_pret_48b_max``, ``d_rv_90d_max``). This preserves coverage
for the earliest WFA windows where threshold estimation would
otherwise be noisy.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from itertools import product
from typing import Callable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


_DEFAULT_VOL_GRID = [0.002, 0.0025, 0.003, 0.0035, 0.004, 0.005]
_DEFAULT_PRET_GRID = [0.015, 0.020, 0.025, 0.030, 0.040, 0.050]
_DEFAULT_RV_GRID = [0.45, 0.50, 0.55, 0.60, 0.70, 0.85]


@dataclass(frozen=True)
class ThresholdTriple:
    """Concrete threshold values for one re-estimation step."""
    vol_max: float
    pret_max: float
    rv_max: float


@dataclass
class WalkForwardQpbConfig:
    """Parsed config for WF-QPB estimator (mirrors [filters.qpb] toml)."""
    lookback_months: int = 9
    refit_freq_months: int = 3
    min_lookback_trades: int = 15
    min_filter_trades: int = 5
    criterion: str = "sharpe_stable"
    trades_per_year_estimate: float = 10.0

    vol_grid: list[float] = field(default_factory=lambda: list(_DEFAULT_VOL_GRID))
    pret_grid: list[float] = field(default_factory=lambda: list(_DEFAULT_PRET_GRID))
    rv_grid: list[float] = field(default_factory=lambda: list(_DEFAULT_RV_GRID))

    # Static-threshold fallback during the warm-up period
    fallback_vol_max: float = 0.003
    fallback_pret_max: float = 0.02
    fallback_rv_max: float = 0.55


class WalkForwardQpbGate:
    """Trade-level WF-QPB post-processing gate.

    Iterates over trades in chronological order. At each trade, decides
    whether the current thresholds need to be re-estimated (based on
    ``refit_freq_months``). If so, runs a grid search over the trade
    history within the lookback window and selects the optimal threshold
    triple according to the configured criterion. When the lookback
    window contains fewer than ``min_lookback_trades`` trades, the
    estimator falls back to the static thresholds until history catches
    up. The current trade is then evaluated against the active
    thresholds and either retained or dropped.

    Args:
        cfg: WF-QPB configuration.
    """
    def __init__(self, cfg: WalkForwardQpbConfig) -> None:
        self._cfg = cfg
        self._combos: list[ThresholdTriple] = [
            ThresholdTriple(v, p, r)
            for v, p, r in product(cfg.vol_grid, cfg.pret_grid, cfg.rv_grid)
        ]
        self._fallback = ThresholdTriple(
            vol_max=cfg.fallback_vol_max,
            pret_max=cfg.fallback_pret_max,
            rv_max=cfg.fallback_rv_max,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def apply(
        self,
        trades: pd.DataFrame,
        feature_lookup: Callable[[pd.Timestamp, int], tuple[float, float, float]],
        ) -> pd.DataFrame:
        """Filter *trades* using walk-forward QPB thresholds.

        Args:
            trades: Completed trades with at least ``entry_time`` (datetime),
                ``type`` ("Long"/"Short"), and ``PnL`` columns.
            feature_lookup: Function that returns
                ``(past_vol_48b, aligned_pret_48b, d_rv_90d_proxy)`` for
                a given entry time and direction (+1 long, -1 short).

        Returns:
            Filtered trades DataFrame with an extra ``wf_threshold`` column
            recording the active threshold triple at each trade's entry
            time.
        """
        if trades.empty:
            return trades

        sorted_trades = trades.sort_values("entry_time").reset_index(drop=True).copy()

        # Pre-compute features for every trade
        feats = []
        for _, row in sorted_trades.iterrows():
            direction = 1 if row["type"] == "Long" else -1
            try:
                f = feature_lookup(pd.Timestamp(row["entry_time"]), direction)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "WF-QPB feature lookup failed for %s: %s",
                    row["entry_time"], exc,
                )
                f = (np.nan, np.nan, np.nan)
            feats.append(f)

        sorted_trades[["past_vol_48b", "aligned_pret_48b", "d_rv_90d_proxy"]] = feats

        # Walk forward
        kept_mask: list[bool] = []
        threshold_log: list[ThresholdTriple] = []
        source_log: list[str] = []        # "wf" or "fallback"
        last_refit_ts: pd.Timestamp | None = None
        active_threshold: ThresholdTriple = self._fallback
        active_source: str = "fallback"

        n_fallback = 0
        n_wf = 0

        for _, row in sorted_trades.iterrows():
            now = pd.Timestamp(row["entry_time"])

            if self._need_refit(now, last_refit_ts):
                estimated = self._estimate_threshold(sorted_trades, now)
                if estimated is not None:
                    active_threshold = estimated
                    active_source = "wf"
                else:
                    active_threshold = self._fallback
                    active_source = "fallback"
                last_refit_ts = now

            threshold_log.append(active_threshold)
            source_log.append(active_source)
            if active_source == "wf":
                n_wf += 1
            else:
                n_fallback += 1

            kept_mask.append(self._satisfies(row, active_threshold))

        sorted_trades["wf_threshold"] = threshold_log
        sorted_trades["wf_threshold_source"] = source_log
        filtered = sorted_trades[kept_mask].copy().reset_index(drop=True)

        logger.info(
            "WF-QPB: kept %d / %d trades | threshold source: %d WF, %d fallback | "
            "criterion=%s, lookback=%dmo, refit=%dmo",
            len(filtered), len(sorted_trades), n_wf, n_fallback,
            self._cfg.criterion, self._cfg.lookback_months, self._cfg.refit_freq_months,
        )

        return filtered

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _need_refit(
        self,
        now: pd.Timestamp,
        last_refit_ts: pd.Timestamp | None,
        ) -> bool:
        if last_refit_ts is None:
            return True
        elapsed_days = (now - last_refit_ts).days
        return elapsed_days >= self._cfg.refit_freq_months * 30

    def _estimate_threshold(
        self,
        trades: pd.DataFrame,
        now: pd.Timestamp,
        ) -> ThresholdTriple | None:
        """Grid-search the threshold space on the lookback window.

        Returns ``None`` when the lookback window contains fewer than
        ``min_lookback_trades`` trades or when no candidate triple passes
        ``min_filter_trades``. The caller is responsible for falling back
        to the static thresholds in this case.
        """
        lookback_start = now - pd.DateOffset(months=self._cfg.lookback_months)
        past = trades[
            (trades["entry_time"] >= lookback_start)
            & (trades["entry_time"] < now)
        ]
        # Drop rows where any QPB feature is NaN
        past = past.dropna(subset=["past_vol_48b", "aligned_pret_48b", "d_rv_90d_proxy"])

        if len(past) < self._cfg.min_lookback_trades:
            return None

        best_score = -np.inf
        best_triple: ThresholdTriple | None = None
        for triple in self._combos:
            mask = (
                (past["past_vol_48b"] < triple.vol_max)
                & (past["aligned_pret_48b"] < triple.pret_max)
                & (past["d_rv_90d_proxy"] < triple.rv_max)
            )
            filtered_pnls = past.loc[mask, "PnL"].values
            if len(filtered_pnls) < self._cfg.min_filter_trades:
                continue
            if filtered_pnls.std() == 0:
                continue

            score = self._score(filtered_pnls)
            if score > best_score:
                best_score = score
                best_triple = triple

        return best_triple

    def _score(self, pnls: np.ndarray) -> float:
        """Objective function applied to a candidate threshold's filtered PnLs."""
        tpy = self._cfg.trades_per_year_estimate
        mean, std = pnls.mean(), pnls.std()

        if self._cfg.criterion == "sharpe":
            return (mean / std) * np.sqrt(tpy)
        if self._cfg.criterion == "mean":
            return mean
        if self._cfg.criterion == "sortino":
            neg = pnls[pnls < 0]
            if len(neg) == 0 or neg.std() == 0:
                return -np.inf
            return (mean / neg.std()) * np.sqrt(tpy)
        if self._cfg.criterion == "sharpe_stable":
            sh = (mean / std) * np.sqrt(tpy)
            return sh * (1.0 - 5.0 / max(len(pnls), 5))

        raise ValueError(f"unknown criterion: {self._cfg.criterion}")

    @staticmethod
    def _satisfies(row: pd.Series, t: ThresholdTriple) -> bool:
        v, p, r = row["past_vol_48b"], row["aligned_pret_48b"], row["d_rv_90d_proxy"]
        if pd.isna(v) or pd.isna(p) or pd.isna(r):
            return False

        return v < t.vol_max and p < t.pret_max and r < t.rv_max


def build_feature_lookup(
    signal_dfs: list[pd.DataFrame],
    ) -> Callable[[pd.Timestamp, int], tuple[float, float, float]]:
    """Build a feature-lookup function from per-window signal DataFrames.

    Args:
        signal_dfs: List of per-window signal DataFrames. Each must
            contain ``past_vol_48b``, ``past_ret_48b``, ``d_rv_90d_proxy``.

    Returns:
        A callable taking ``(timestamp, direction)`` and returning the
        three feature values at the signal bar immediately preceding
        the given timestamp.
    """
    combined = pd.concat([df for df in signal_dfs if df is not None])
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()

    required = {"past_vol_48b", "past_ret_48b", "d_rv_90d_proxy"}
    missing = required - set(combined.columns)
    if missing:
        raise KeyError(
            f"build_feature_lookup(): signal DataFrames missing columns {missing}."
        )

    def lookup(ts: pd.Timestamp, direction: int) -> tuple[float, float, float]:
        # Entry is at bar i+1 (open of next bar); features should be
        # evaluated at signal bar i. We take the preceding bar.
        pos = combined.index.searchsorted(ts)
        sig_pos = max(0, pos - 1)
        row = combined.iloc[sig_pos]
        vol = float(row["past_vol_48b"])
        pret_raw = float(row["past_ret_48b"])
        rv = float(row["d_rv_90d_proxy"])
        return (vol, direction * pret_raw, rv)

    return lookup
