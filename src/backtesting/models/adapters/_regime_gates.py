"""
backtesting.models.adapters._regime_gates
==========================================
Shared signal-assembly helper for all regime-probability based adapters
(MDRS-SDE, DL-regime, HMM, LGBM).

As of v2.0, this module contains only the breakout signal-assembly
logic. The legacy ``compute_sticky_mask`` / ``compute_adx_mask`` /
``compute_qpb_mask`` gates were removed because PAITS-Event
(``[filters.qpb].mode = "event_rate"``) supersedes them at the trade
post-processing layer; see ``_event_rate.py`` and
``backtesting.engines.walk_forward``.

Rule-based baseline adapters (simple_breakout, ma_crossover, rsi) are
evaluated as self-contained technical rules and do not use this module.

Signal convention
-----------------
``assemble_signal`` returns the input DataFrame with three columns
added:

* ``regime_prob``  — copy of the probability series produced by the
  detector.
* ``confidence``   — alias of ``regime_prob`` for engine consumption.
* ``signal``       — ``+1`` for long-breakout bars,  ``-1`` for
  short-breakout bars, ``0`` otherwise. A long-breakout bar is one
  where ``regime_prob > entry_threshold`` AND
  ``Close > dynamic_resistance``; the short side is symmetric using
  ``dynamic_support``.

Entry filtering is delegated to the trade-level post-processing layer
(``WalkForwardRunner._apply_event_rate``).
"""
from __future__ import annotations

from typing import Any

import pandas as pd


def assemble_signal(
    df: pd.DataFrame,
    regime_prob: pd.Series,
    risk_cfg: dict[str, Any],
    filters_cfg: dict[str, Any],  # retained for signature stability (v1.x callers)
    trade_cfg: dict[str, Any],  # retained for signature stability (v1.x callers)
    ) -> pd.DataFrame:
    """Assemble the breakout direction signal from regime probability.

    The function preserves its v1.x signature so that adapters can call
    it unchanged. The ``filters_cfg`` and ``trade_cfg`` arguments are
    accepted but no longer consulted; signal-level sticky / ADX / QPB
    gating was removed in v2.0. Entry-rate control is now performed
    downstream by the PAITS-Event gate in
    :class:`backtesting.engines.walk_forward.WalkForwardRunner`.

    Args:
        df:          Test-window OHLCV+features DataFrame. Must
            contain ``Close``, ``dynamic_resistance``, and
            ``dynamic_support`` columns (produced by the crypto
            preprocessor's Donchian channel computation).
        regime_prob: Out-of-sample regime probability series aligned to
            ``df.index``.
        risk_cfg:    ``[risk_management]`` settings. Only
            ``entry_probability_threshold`` is consulted.
        filters_cfg: Accepted for signature compatibility; ignored.
        trade_cfg:   Accepted for signature compatibility; ignored.

    Returns:
        ``df`` with ``regime_prob``, ``confidence``, and ``signal``
        columns added or overwritten.
    """
    del filters_cfg, trade_cfg  # accepted for signature stability; intentionally unused

    df = df.copy()
    df["regime_prob"] = regime_prob
    df["confidence"] = regime_prob

    entry_threshold = risk_cfg.get(
        "entry_probability_threshold", 0.5
    )

    above_threshold = regime_prob > entry_threshold
    long_cond = df["Close"] > df["dynamic_resistance"]
    short_cond = df["Close"] < df["dynamic_support"]

    df["signal"] = 0
    df.loc[above_threshold & long_cond, "signal"] = 1
    df.loc[above_threshold & short_cond, "signal"] = -1

    return df
