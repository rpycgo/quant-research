"""
backtesting.assets.crypto.preprocessor
=======================================
Generic feature engineering for cryptocurrency OHLCV data.

Computes the feature set shared by **all** model adapters that target
crypto assets.  Model-specific features (e.g. SDE-specific event tagging,
MCMC posterior inputs) are computed inside the adapter's ``fit`` / ``predict``
methods and must **not** live here.

Computed columns
----------------
* ``log_return``                — natural log of the price ratio
* ``absolute_return``           — absolute value of ``log_return``
* ``volume_z_score``            — rolling volume Z-score (denominator-defended)
* ``absolute_return_z_score``   — rolling return Z-score (noise-floored)
* ``hybrid_z_score``            — smoothed max of volume and return Z-scores
* ``is_quiet_regime``           — boolean flag where hybrid Z < quiet threshold
* ``manual_resistance``         — rolling-max high during quiet regimes
* ``manual_support``            — rolling-min low during quiet regimes
* ``dynamic_resistance``        — Donchian-channel upper band (lag-1)
* ``dynamic_support``           — Donchian-channel lower band (lag-1)
* ``ADX``                       — Average Directional Index (14-period)
* ``direction_indicator``       — 1 / -1 / 0 relative to quiet S/R levels
"""
from __future__ import annotations

import numpy as np
import pandas as pd


class CryptoPreprocessor:
    """Stateless feature-engineering pipeline for crypto OHLCV data.

    All methods accept and return a ``DataFrame`` so they can be chained
    or called individually.  The pipeline order expected by model adapters
    is documented in :meth:`run_full_pipeline`.

    Args:
        settings: Parsed ``[event_detection]`` section from
            ``data_settings.toml``.  All window sizes and thresholds are
            read from this dict so the preprocessor has no hard-coded
            constants.

    Example::

        settings = config_loader.get_data_settings()["event_detection"]
        pre = CryptoPreprocessor(settings)
        data = pre.run_full_pipeline(raw_df)
    """
    def __init__(self, settings: dict) -> None:
        self._window: int = settings.get("global_window_size", 288)
        self._noise: float = settings.get("noise_floor", 0.001)
        self._clip_upper: float = settings.get("clipping_upper_limit", 6.0)
        self._min_periods: int = settings.get("minimum_periods_standard", 20)
        self._smoothing: int = settings.get("hybrid_smoothing_window", 3)
        self._quiet_threshold: float = settings.get("quiet_regime_threshold", 1.3)
        self._sr_lookback: int = settings.get("sr_lookback_window", 288)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_full_pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the complete generic feature-engineering pipeline.

        Calls each step in the order required for column dependencies to be
        satisfied.  Pass the returned ``DataFrame`` directly to the model
        adapter.

        Args:
            df: Raw OHLCV ``DataFrame`` from the loader.

        Returns:
            ``df`` augmented with all computed columns listed in the module
            docstring.
        """
        df = self.calculate_base_features(df)
        df = self.identify_quiet_sr_levels(df)
        df = self.calculate_strategy_indicators(df)

        return df

    def calculate_base_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute log-returns and hybrid Z-score.

        Implements denominator defence (10 % of rolling mean) on volume and
        a noise floor on returns so that Z-scores remain numerically stable
        in low-volatility regimes.

        Args:
            df: OHLCV ``DataFrame``.

        Returns:
            ``df`` with ``log_return``, ``absolute_return``,
            ``volume_z_score``, ``absolute_return_z_score``, and
            ``hybrid_z_score`` added.
        """
        # Log-return and absolute return
        df["log_return"] = np.log(df["Close"] / df["Close"].shift(1))
        df["absolute_return"] = df["log_return"].abs()

        # Volume Z-score — denominator defended at 10 % of rolling mean
        vol = df["Volume"]
        roll_vol_mean = vol.rolling(self._window, min_periods=self._min_periods).mean()
        roll_vol_std = vol.rolling(self._window, min_periods=self._min_periods).std()
        roll_vol_std = np.maximum(roll_vol_std, roll_vol_mean * 0.1)
        df["volume_z_score"] = ((vol - roll_vol_mean) / roll_vol_std).clip(
            upper=self._clip_upper
        )

        # Absolute-return Z-score — noise floor applied to denominator
        ret = df["absolute_return"]
        roll_ret_mean = ret.rolling(self._window, min_periods=self._min_periods).mean()
        roll_ret_std = ret.rolling(self._window, min_periods=self._min_periods).std()
        roll_ret_std = np.maximum(roll_ret_std, self._noise)
        df["absolute_return_z_score"] = (
            (ret - roll_ret_mean) / roll_ret_std
        ).clip(upper=self._clip_upper)

        # Hybrid Z-score: max of both signals, then smoothed
        raw_hybrid = np.maximum(
            df["volume_z_score"], df["absolute_return_z_score"]
        )
        df["hybrid_z_score"] = (
            raw_hybrid.rolling(self._smoothing, min_periods=1).mean()
        )

        return df

    def identify_quiet_sr_levels(self, df: pd.DataFrame) -> pd.DataFrame:
        """Derive support / resistance levels from quiet-regime price action.

        Rows where ``hybrid_z_score < quiet_threshold`` are classified as
        quiet; highs and lows recorded during those periods form the S/R
        levels used to identify breakout direction.  Forward-fill ensures
        levels persist through non-quiet bars.

        Args:
            df: ``DataFrame`` containing ``hybrid_z_score``, ``High``,
                ``Low``.

        Returns:
            ``df`` with ``is_quiet_regime``, ``manual_resistance``, and
            ``manual_support`` added.
        """
        df["is_quiet_regime"] = df["hybrid_z_score"] < self._quiet_threshold

        raw_res = (
            df["High"]
            .where(df["is_quiet_regime"])
            .rolling(self._sr_lookback, min_periods=self._min_periods)
            .max()
        )
        raw_sup = (
            df["Low"]
            .where(df["is_quiet_regime"])
            .rolling(self._sr_lookback, min_periods=self._min_periods)
            .min()
        )

        # Forward-fill so S/R levels persist beyond quiet windows
        df["manual_resistance"] = raw_res.ffill().fillna(
            df["High"].rolling(self._sr_lookback, min_periods=1).max()
        )
        df["manual_support"] = raw_sup.ffill().fillna(
            df["Low"].rolling(self._sr_lookback, min_periods=1).min()
        )

        return df

    def calculate_directional_indicator(self, df: pd.DataFrame) -> pd.DataFrame:
        """Assign directional bias relative to quiet-regime S/R levels.

        * ``direction_indicator = 1``  when ``Close > manual_resistance``
        * ``direction_indicator = -1`` when ``Close < manual_support``
        * ``direction_indicator = 0``  otherwise

        Args:
            df: ``DataFrame`` containing ``Close``, ``manual_resistance``,
                ``manual_support``.

        Returns:
            ``df`` with ``direction_indicator`` added.
        """
        conditions = [
            df["Close"] > df["manual_resistance"],
            df["Close"] < df["manual_support"],
        ]
        df["direction_indicator"] = np.select(conditions, [1, -1], default=0)

        return df

    def calculate_strategy_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute Donchian-channel S/R and ADX for entry filtering.

        Donchian channels (lag-1) define dynamic resistance and support used
        by model adapters to determine Long / Short entry direction.  ADX
        filters out low-trending environments.

        Args:
            df: OHLCV ``DataFrame``.

        Returns:
            ``df`` with ``dynamic_resistance``, ``dynamic_support``, and
            ``ADX`` added.
        """
        df["dynamic_resistance"] = (
            df["High"].rolling(self._window).max().shift(1)
        )
        df["dynamic_support"] = (
            df["Low"].rolling(self._window).min().shift(1)
        )
        df["ADX"] = self._calculate_adx(df)

        return df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Compute the Average Directional Index (ADX).

        Uses a vectorised approach with Wilder's exponential smoothing
        (``ewm(alpha=1/period)``) for +DI and -DI, followed by a rolling
        mean for ADX.  Index alignment is guaranteed by operating on the
        full ``DataFrame`` series.

        Args:
            df:     OHLCV ``DataFrame`` containing ``High``, ``Low``,
                    ``Close``.
            period: Smoothing period (default: 14).

        Returns:
            ``Series`` of ADX values aligned to ``df``'s index.
        """
        plus_dm = df["High"].diff().clip(lower=0)
        minus_dm = df["Low"].diff().clip(upper=0).abs()

        true_range = pd.concat(
            [
                df["High"] - df["Low"],
                (df["High"] - df["Close"].shift(1)).abs(),
                (df["Low"] - df["Close"].shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)

        atr = true_range.rolling(period).mean()
        plus_di = 100 * (plus_dm.ewm(alpha=1 / period).mean() / atr)
        minus_di = 100 * (minus_dm.ewm(alpha=1 / period).mean() / atr)

        dx = (
            (plus_di - minus_di).abs() / (plus_di + minus_di).abs()
        ) * 100

        return dx.rolling(period).mean()
