"""
backtesting.models.adapters._regime_gates
==========================================
Shared sticky-filter / ADX-gate / QPB-gate logic used by all
regime-probability based adapters (MDRS-SDE, DL-regime, HMM).

Rule-based baseline adapters (simple_breakout, ma_crossover, rsi) are
evaluated as self-contained technical rules and do not use this module.

Why a shared helper?
--------------------
Fair comparison of regime-probability models requires that they share
the downstream filtering pipeline. Duplicating the logic across three
adapters introduces drift risk. This module centralises the three
filtering layers so that every regime-probability adapter applies
them identically.
"""
from __future__ import annotations

from typing import Any

import pandas as pd


# Defaults mirror configs/backtest_settings.toml
_DEFAULT_MIN_DURATION = 5
_DEFAULT_ADX_THRESHOLD = 30
_DEFAULT_ENTRY_THRESHOLD = 0.5
_DEFAULT_QPB_VOL_MAX = 0.003
_DEFAULT_QPB_PRET_MAX = 0.02
_DEFAULT_QPB_RV90_MAX = 0.55


def compute_sticky_mask(
    regime_prob: pd.Series,
    entry_threshold: float = _DEFAULT_ENTRY_THRESHOLD,
    min_duration: int = _DEFAULT_MIN_DURATION,
    enabled: bool = True,
    ) -> pd.Series:
    """Return a boolean mask that is True only when the regime probability
    has exceeded *entry_threshold* for *min_duration* consecutive bars.

    When *enabled* is False, the mask reduces to ``regime_prob > entry_threshold``.
    """
    binary = (regime_prob > entry_threshold).astype(int)
    if not enabled:
        return binary.astype(bool)

    sticky = (
        binary.rolling(window=min_duration).sum() == min_duration
    ).astype(bool)
    return sticky


def compute_adx_mask(
    adx: pd.Series,
    threshold: float = _DEFAULT_ADX_THRESHOLD,
    enabled: bool = True,
    ) -> pd.Series:
    """Return a boolean mask that is True when ADX exceeds *threshold*.

    When *enabled* is False, the mask is all-True.
    """
    if not enabled:
        return pd.Series(True, index=adx.index)
    return adx > threshold


def compute_qpb_mask(
    df: pd.DataFrame,
    long_cond: pd.Series,
    short_cond: pd.Series,
    qpb_cfg: dict[str, Any],
    ) -> pd.Series:
    """Return a boolean mask satisfying the three QPB conditions.

    The Quiet Pre-Breakout (QPB) gate restricts entries to bars where:

    * ``past_vol_48b < past_vol_48b_max``         (local calm)
    * ``sign * past_ret_48b < aligned_pret_48b_max`` (no overextension)
    * ``d_rv_90d_proxy < d_rv_90d_max``           (non-extreme long vol)

    where ``sign`` is ``+1`` on long-condition bars, ``-1`` on short-
    condition bars, and ``0`` elsewhere.

    When ``qpb_cfg['enabled']`` is False or missing, the mask is all-True.

    Raises:
        KeyError: If any of the required feature columns are missing.
    """
    if not qpb_cfg.get("enabled", False):
        return pd.Series(True, index=df.index)

    required = {"past_vol_48b", "past_ret_48b", "d_rv_90d_proxy"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(
            f"compute_qpb_mask(): DataFrame is missing columns {missing}. "
            f"Ensure the preprocessor ran calculate_qpb_features()."
        )

    vol_max = float(qpb_cfg.get("past_vol_48b_max", _DEFAULT_QPB_VOL_MAX))
    pret_max = float(qpb_cfg.get("aligned_pret_48b_max", _DEFAULT_QPB_PRET_MAX))
    rv90_max = float(qpb_cfg.get("d_rv_90d_max", _DEFAULT_QPB_RV90_MAX))

    sign = pd.Series(0, index=df.index, dtype=int)
    sign.loc[long_cond] = 1
    sign.loc[short_cond] = -1
    aligned_pret_48b = sign * df["past_ret_48b"]

    mask = (
        (df["past_vol_48b"] < vol_max)
        & (aligned_pret_48b < pret_max)
        & (df["d_rv_90d_proxy"] < rv90_max)
    ).fillna(False)

    return mask


def assemble_signal(
    df: pd.DataFrame,
    regime_prob: pd.Series,
    risk_cfg: dict[str, Any],
    filters_cfg: dict[str, Any],
    trade_cfg: dict[str, Any],
    ) -> pd.DataFrame:
    """Apply the full sticky + ADX + QPB pipeline and assemble the signal.

    The pipeline is:
        1. Sticky persistence filter on *regime_prob*.
        2. Direction gate from ``Close`` vs ``dynamic_resistance`` /
           ``dynamic_support``.
        3. ADX gate.
        4. QPB gate.
        5. Final signal assembly (+1 / -1 / 0).

    Args:
        df: DataFrame containing ``Close``, ``ADX``,
            ``dynamic_resistance``, ``dynamic_support`` and — if QPB is
            enabled — ``past_vol_48b``, ``past_ret_48b``,
            ``d_rv_90d_proxy``.
        regime_prob: Series of regime probabilities aligned to ``df``.
        risk_cfg: Parsed ``[risk_management]`` section.
        filters_cfg: Parsed ``[filters]`` section (with optional nested
            ``[filters.qpb]`` sub-section).
        trade_cfg: Parsed ``[trading_parameters]`` section (used for
            ``adx_threshold``).

    Returns:
        *df* with ``regime_prob``, ``confidence``, and ``signal`` columns
        added / overwritten.
    """
    df = df.copy()
    df["regime_prob"] = regime_prob
    df["confidence"] = regime_prob

    entry_threshold = risk_cfg.get("entry_probability_threshold", _DEFAULT_ENTRY_THRESHOLD)
    min_duration = risk_cfg.get("minimum_signal_duration", _DEFAULT_MIN_DURATION)
    use_sticky = filters_cfg.get("use_sticky", True)
    use_adx = filters_cfg.get("use_adx", True)
    adx_threshold = trade_cfg.get("adx_threshold", _DEFAULT_ADX_THRESHOLD)
    qpb_cfg = filters_cfg.get("qpb", {})

    sticky_mask = compute_sticky_mask(
        regime_prob, entry_threshold, min_duration, enabled=use_sticky,
    )

    long_cond = df["Close"] > df["dynamic_resistance"]
    short_cond = df["Close"] < df["dynamic_support"]

    adx_mask = compute_adx_mask(df["ADX"], adx_threshold, enabled=use_adx)

    qpb_mask = compute_qpb_mask(df, long_cond, short_cond, qpb_cfg)

    df["signal"] = 0
    df.loc[sticky_mask & long_cond & adx_mask & qpb_mask, "signal"] = 1
    df.loc[sticky_mask & short_cond & adx_mask & qpb_mask, "signal"] = -1

    return df
