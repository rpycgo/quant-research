"""
backtesting.models.adapters._event_rate
========================================
Walk-forward event-rate threshold estimator.

This module implements a bar-level entry gate that selects entries by
thresholding the rolling-max of out-of-sample regime probabilities. The
threshold itself is *not* a fixed hyperparameter; instead, it is
recomputed at the start of each test month by **bisection** over the
previous *lookback* window so that the *cooldown-adjusted* number of
entry events matches a target rate (``target_events_per_month``).

Design rationale
----------------
The legacy ``_wf_qpb.py`` estimator relies on a grid of QPB feature
thresholds (vol / pret / rv) and a hand-crafted product grid of
candidate values. PAITS-Event removes that grid entirely:

* Score:      ``score_t = rolling_max(regime_prob, score_window_bars)``
              (default 144 bars = 12 hours).
* Threshold:  Found analytically via bisection. ``count_events`` is
              monotonically non-increasing in the threshold, so the
              exact threshold producing the desired monthly event rate
              is well-defined and can be located in O(log N).
* Cooldown:   ``cooldown_days`` since the previous entry (default
              5 days, justifiable as one trading week).
* Lookback:   3 months by default, matching the Bayesian detector's
              training window for paper-consistent walk-forward.

The only sample-size-derived hyperparameters are
``target_events_per_month`` and ``cooldown_days``; the threshold range
[lookback_min, lookback_max] is data-driven, eliminating the
threshold-grid hyperparameter set entirely.

Single-position semantics are preserved: ``cooldown_bars`` constrains
the minimum spacing between consecutive entries, and the existing
single-position engine handles overlap-free execution naturally.

Warm-up
-------
When fewer than ``min_lookback_bars`` valid ``rp_max_window`` bars are
available in the lookback window, the estimator falls back to the
``fallback_threshold`` configured in ``[filters.qpb.event_rate]``. This
preserves coverage for the earliest WFA windows where bisection would
otherwise be unstable.

Integration
-----------
This module is invoked from ``WalkForwardRunner`` when
``[filters.qpb].mode = "event_rate"`` is set. The legacy ``"static"``
and ``"walkforward"`` modes (driven by ``_wf_qpb.py``) remain available
and unchanged.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import NamedTuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# Module-level constants. The "1 trading week" cooldown and "12-hour
# rolling max" score are not hyperparameters tuned to BTC; they are
# natural time units that transfer across assets without retuning.
_BARS_PER_DAY: int = 24 * 12  # 5-minute bars
_DEFAULT_SCORE_WINDOW_BARS: int = 144  # 12 hours = 1 trading session
_DEFAULT_COOLDOWN_DAYS: float = 5.0  # 1 trading week
_DEFAULT_TARGET_EVENTS_PER_MONTH: float = 5.0
_DEFAULT_LOOKBACK_DAYS: int = 90  # 3 months (paper-consistent)
_DEFAULT_FALLBACK_THRESHOLD: float = 0.45
_DEFAULT_MIN_LOOKBACK_BARS: int = 100
_BISECTION_MAX_ITERS: int = 40
_BISECTION_PRECISION: float = 1e-6


@dataclass(frozen=True)
class EventRateThreshold:
    """Resolved threshold for one re-estimation step."""
    threshold: float
    cooldown_bars: int
    refit_date: pd.Timestamp
    n_lookback_bars: int
    used_fallback: bool


@dataclass
class WalkForwardEventRateConfig:
    """Parsed config for PAITS-Event estimator.

    Mirrors ``[filters.qpb.event_rate]`` in TOML.
    """
    target_events_per_month: float = _DEFAULT_TARGET_EVENTS_PER_MONTH
    cooldown_days: float = _DEFAULT_COOLDOWN_DAYS
    lookback_days: int = _DEFAULT_LOOKBACK_DAYS
    score_window_bars: int = _DEFAULT_SCORE_WINDOW_BARS
    refit_freq: str = "ME"  # pandas offset alias for month-end
    fallback_threshold: float = _DEFAULT_FALLBACK_THRESHOLD
    min_lookback_bars: int = _DEFAULT_MIN_LOOKBACK_BARS

    @property
    def cooldown_bars(self) -> int:
        """Cooldown expressed in 5-minute bars."""
        return int(self.cooldown_days * _BARS_PER_DAY)

    @property
    def target_total(self) -> float:
        """Expected total events over the full lookback window."""
        return self.target_events_per_month * (self.lookback_days / 30.0)


def compute_score(
    regime_prob: pd.Series,
    window_bars: int = _DEFAULT_SCORE_WINDOW_BARS,
    ) -> pd.Series:
    """Compute the rolling-max score over a window of bars.

    Parameters
    ----------
    regime_prob : pd.Series
        Out-of-sample regime probability sequence indexed by timestamp.
    window_bars : int
        Rolling window length in bars (default 144 = 12 hours).

    Returns
    -------
    pd.Series
        Rolling maximum of ``regime_prob`` over ``window_bars``. The
        first ``window_bars - 1`` entries are ``NaN``.
    """
    if window_bars <= 0:
        raise ValueError(f"window_bars must be positive, got {window_bars}")

    return regime_prob.rolling(window_bars).max()


def count_events(
    score_values: np.ndarray,
    direction_values: np.ndarray,
    threshold: float,
    cooldown_bars: int,
    ) -> int:
    """Count entry events under a threshold and cooldown constraint.

    An "event" is a bar position where ``score > threshold``, the
    directional signal is non-zero, and the position is at least
    ``cooldown_bars`` away from the previous event.

    Parameters
    ----------
    score_values : np.ndarray
        Score values (typically rolling-max of regime_prob).
    direction_values : np.ndarray
        Direction indicator values; non-zero means a signal is present.
    threshold : float
        Score threshold to test.
    cooldown_bars : int
        Minimum spacing in bars between consecutive events.

    Returns
    -------
    int
        Number of cooldown-respecting events in the input sequence.

    Notes
    -----
    This function is monotonically non-increasing in ``threshold``,
    which is the foundation of the bisection search in
    :func:`bisection_threshold`.
    """
    candidates = np.where(
        (score_values > threshold)
        & (direction_values != 0)
        & ~np.isnan(score_values)
    )[0]
    last_event = -(10 ** 9)
    n_events = 0
    for cand in candidates:
        if cand - last_event >= cooldown_bars:
            n_events += 1
            last_event = cand

    return n_events


def bisection_threshold(
    score_values: np.ndarray,
    direction_values: np.ndarray,
    target_total: float,
    cooldown_bars: int,
    fallback: float = _DEFAULT_FALLBACK_THRESHOLD,
    ) -> float:
    """Bisect over thresholds to find one yielding ~target_total events.

    Because ``count_events`` is monotonically non-increasing in the
    threshold, the function ``f(t) = count_events(t) - target_total``
    has at most one sign change over ``[score_min, score_max]``. We
    bracket and bisect to ``_BISECTION_PRECISION`` or
    ``_BISECTION_MAX_ITERS`` iterations.

    Parameters
    ----------
    score_values : np.ndarray
        Score values from the lookback window.
    direction_values : np.ndarray
        Direction indicator values aligned with ``score_values``.
    target_total : float
        Desired event count over the lookback window. Typically
        ``target_events_per_month * (lookback_days / 30)``.
    cooldown_bars : int
        Cooldown constraint to apply during counting.
    fallback : float
        Threshold to return when the score sample is empty or all NaN.

    Returns
    -------
    float
        Threshold value.
    """
    valid_mask = ~np.isnan(score_values)
    if not valid_mask.any():
        return fallback

    valid_scores = score_values[valid_mask]
    lo, hi = float(valid_scores.min()), float(valid_scores.max())

    # Boundary checks: if even the loosest threshold under-shoots the
    # target, return the minimum (cannot produce more events). If even
    # the tightest threshold over-shoots, return the maximum.
    if count_events(score_values, direction_values, lo, cooldown_bars) <= target_total:
        return lo
    if count_events(score_values, direction_values, hi, cooldown_bars) >= target_total:
        return hi

    for _ in range(_BISECTION_MAX_ITERS):
        mid = (lo + hi) / 2.0
        cnt = count_events(score_values, direction_values, mid, cooldown_bars)
        if cnt > target_total:
            lo = mid  # too many events, raise threshold
        elif cnt < target_total:
            hi = mid  # too few events, lower threshold
        else:
            return mid
        if hi - lo < _BISECTION_PRECISION:
            break

    return (lo + hi) / 2.0


class _PlanEntry(NamedTuple):
    """One test window in the walk-forward plan."""
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    threshold: EventRateThreshold


class WalkForwardEventRateGate:
    """Bar-level walk-forward event-rate gate (PAITS-Event v1.20.0).

    This gate is intended to be invoked in ``[filters.qpb].mode =
    "event_rate"`` mode. It produces a boolean mask over the input
    signal frame indicating which bars are admissible entry candidates,
    given the dynamic threshold and cooldown.

    Usage
    -----
    >>> gate = WalkForwardEventRateGate(WalkForwardEventRateConfig())
    >>> mask = gate.compute_mask(signal_df)
    >>> entries = signal_df[mask]
    """

    def __init__(self, config=None):
        self.config = config or WalkForwardEventRateConfig()
        self._last_plan = None  # list of _PlanEntry, set by _build_plan

    def _build_plan(self, signal_df):
        """Construct the (refit_date, threshold) plan over the full timeline."""
        if signal_df.empty:
            return []
        if "regime_prob" not in signal_df.columns:
            raise KeyError("signal_df must contain 'regime_prob' column")
        if "direction_indicator" not in signal_df.columns:
            raise KeyError("signal_df must contain 'direction_indicator' column")

        score = compute_score(
            signal_df["regime_prob"], window_bars=self.config.score_window_bars
        )
        direction = signal_df["direction_indicator"]

        start = signal_df.index[0]
        end = signal_df.index[-1]
        first_refit = start + pd.Timedelta(days=self.config.lookback_days)
        refit_dates = pd.date_range(first_refit, end, freq=self.config.refit_freq)

        plan = []  # list of _PlanEntry
        for i, refit_date in enumerate(refit_dates):
            lookback_start = refit_date - pd.Timedelta(days=self.config.lookback_days)
            lb_score = score.loc[lookback_start:refit_date]
            lb_dir = direction.loc[lookback_start:refit_date]

            # Align and drop NaN scores
            valid = lb_score.notna()
            lb_score_v = lb_score[valid].values
            lb_dir_v = lb_dir[valid].values

            if len(lb_score_v) < self.config.min_lookback_bars:
                thr_val = self.config.fallback_threshold
                used_fallback = True
            else:
                thr_val = bisection_threshold(
                    lb_score_v, lb_dir_v,
                    target_total=self.config.target_total,
                    cooldown_bars=self.config.cooldown_bars,
                    fallback=self.config.fallback_threshold,
                )
                used_fallback = False

            test_end = (
                refit_dates[i + 1] if i + 1 < len(refit_dates)
                else end + pd.Timedelta(days=31)
            )
            plan.append(_PlanEntry(
                test_start=refit_date,
                test_end=test_end,
                threshold=EventRateThreshold(
                    threshold=thr_val,
                    cooldown_bars=self.config.cooldown_bars,
                    refit_date=refit_date,
                    n_lookback_bars=int(len(lb_score_v)),
                    used_fallback=used_fallback,
                ),
            ))

        logger.info(
            "PAITS-Event plan built: %d refit windows, threshold range [%.3f, %.3f]",
            len(plan),
            min(p.threshold.threshold for p in plan) if plan else 0.0,
            max(p.threshold.threshold for p in plan) if plan else 0.0,
        )
        self._last_plan = plan

        return plan

    def compute_mask(self, signal_df):
        """Compute the boolean entry mask over the signal frame.

        The mask is ``True`` at bars where:

        * The rolling-max score over the past ``score_window_bars``
          exceeds the current test window's threshold.
        * The direction indicator is non-zero.
        * The minimum cooldown spacing is respected.

        Single-position execution downstream handles overlap-free entry
        enforcement; the cooldown here primarily ensures stable event
        density and prevents same-burst re-entry.
        """
        plan = self._build_plan(signal_df)
        if not plan:
            return pd.Series(False, index=signal_df.index)

        score = compute_score(
            signal_df["regime_prob"], window_bars=self.config.score_window_bars
        )
        direction = signal_df["direction_indicator"]

        mask = pd.Series(False, index=signal_df.index)
        last_event_pos = -(10 ** 9)
        cooldown_bars = self.config.cooldown_bars
        score_v = score.values
        direction_v = direction.values

        positions = np.arange(len(signal_df))
        for entry in plan:
            in_window = (signal_df.index >= entry.test_start) & (signal_df.index < entry.test_end)
            candidate_positions = positions[
                in_window
                & (score_v > entry.threshold.threshold)
                & (direction_v != 0)
                & ~np.isnan(score_v)
            ]
            for cand in candidate_positions:
                if cand - last_event_pos >= cooldown_bars:
                    mask.iloc[cand] = True
                    last_event_pos = cand

        return mask

    @property
    def last_plan(self):
        """Return the most recently constructed plan (for inspection / logging)."""
        return self._last_plan
