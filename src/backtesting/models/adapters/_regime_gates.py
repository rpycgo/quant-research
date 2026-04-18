"""
backtesting.models.adapters._regime_gates
==========================================
Shared sticky-filter / ADX-gate / QPB-gate logic used by all
regime-probability based adapters (MDRS-SDE, DL-regime, HMM).

Rule-based baseline adapters (simple_breakout, ma_crossover, rsi) are
evaluated as self-contained technical rules and do not use this module.

QPB mode interaction
--------------------
The static QPB gate here only operates when
``filters.qpb.mode == "static"`` (the default for backward compatibility).
In ``walkforward`` mode, this signal-level gate is intentionally
bypassed so that ``WalkForwardRunner._apply_wf_qpb`` can perform
trade-level threshold estimation on the raw baseline trade set rather
than on a statically pre-filtered subset. Applying both gates would
otherwise produce a degenerate double-filtering effect.
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
    """Return a boolean mask satisfying the three QPB conditions at signal bars.

    The static QPB gate is applied only when
    ``qpb_cfg['enabled'] is True`` AND ``qpb_cfg['mode'] == 'static'``.
    In ``walkforward`` mode the mask is all-True; threshold filtering
    is performed later by ``WalkForwardRunner._apply_wf_qpb`` at trade
    level.

    Raises:
        KeyError: If static mode is active and any of the required
            feature columns are missing.
    """
    if not qpb_cfg.get("enabled", False):
        return pd.Series(True, index=df.index)

    # In walk-forward mode, skip the signal-level gate entirely.
    mode = qpb_cfg.get("mode", "static")
    if mode == "walkforward":
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
    """Apply the sticky + ADX + (static) QPB pipeline and assemble the signal.

    When ``filters_cfg['qpb']['mode'] == 'walkforward'``, the QPB step
    is a no-op at signal level; the walk-forward layer in
    ``WalkForwardRunner`` handles threshold filtering at trade level
    instead.
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
