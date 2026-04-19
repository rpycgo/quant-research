"""
backtesting.models.adapters.lgbm_regime
========================================
LightGBM-based regime detection adapter.

Design rationale
----------------
LightGBM is evaluated as a **general-purpose supervised classifier** that
learns from the full in-sample price history without domain-specific
filtering. This contrasts with MDRS-SDE which restricts its training
corpus to detected breakout event windows.

* **Training corpus**: Full in-sample train_slice (identical to GARCH and
  HMM baselines).

* **Label definition**: Direction-aligned cumulative log-return over the
  next ``label_horizon`` bars::

      future_ret = log_return.shift(-1).rolling(label_horizon).sum()
      y = (future_ret * direction_indicator > 0).astype(int)

  shift(-1) ensures no look-ahead bias on the current bar.

* **Features**: All preprocessed microstructure features are available
  to the model. If ``hybrid_z_score`` is informative, LightGBM will
  discover this via feature importance rather than being told explicitly
  via a hard threshold.

* **Inference**: Applied to the full out-of-sample test window to produce
  ``regime_prob``. The downstream sticky / ADX / QPB pipeline (shared
  with MDRS-SDE, DL, and HMM) then filters entry signals.

Look-ahead bias audit
---------------------
* train_slice strictly precedes test_slice (WalkForwardRunner).
* dynamic_resistance / dynamic_support use .shift(1) in preprocessor.
* Label uses shift(-1) within train_slice only — never on test data.
* Last ``label_horizon`` rows of train_slice dropped (NaN labels).
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from backtesting.core.base_model import BaseModel
from backtesting.models.adapters._regime_gates import assemble_signal

logger = logging.getLogger(__name__)

_DEFAULT_FEATURES: list[str] = [
    "hybrid_z_score",
    "volume_z_score",
    "absolute_return_z_score",
    "log_return",
    "direction_indicator",
    "ADX",
]

_MIN_TRAINING_ROWS = 100


class LGBMRegimeAdapter(BaseModel):
    """LightGBM binary classifier for regime detection.

    Trains one LightGBM model per walk-forward window on the full
    in-sample train_slice and produces ``regime_prob`` for the
    out-of-sample test window.

    The downstream signal pipeline (sticky / ADX / QPB) is identical to
    MDRS-SDE, DL, and HMM adapters via :func:`assemble_signal`.

    Args:
        model_config:    Merged config from ``BacktestConfigLoader``.
        backtest_config: Parsed backtest settings dict.
    """

    def __init__(
        self,
        model_config: dict[str, Any],
        backtest_config: dict[str, Any],
        ) -> None:
        self._model_config  = model_config
        self._risk          = backtest_config.get("risk_management", {})
        self._filters       = backtest_config.get("filters", {})
        self._trade         = backtest_config.get("trading_parameters", {})

        lgbm_cfg = model_config.get("lgbm_settings", {})

        self._features: list[str]    = lgbm_cfg.get("input_features", _DEFAULT_FEATURES)
        self._label_horizon: int     = int(lgbm_cfg.get("label_horizon", 12))
        self._n_estimators: int      = int(lgbm_cfg.get("n_estimators", 100))
        self._learning_rate: float   = float(lgbm_cfg.get("learning_rate", 0.1))
        self._max_depth: int         = int(lgbm_cfg.get("max_depth", -1))
        self._num_leaves: int        = int(lgbm_cfg.get("num_leaves", 31))
        self._min_child_samples: int = int(lgbm_cfg.get("min_child_samples", 20))
        self._random_seed: int       = int(lgbm_cfg.get("random_seed", 42))

    # ------------------------------------------------------------------
    # BaseModel interface
    # ------------------------------------------------------------------

    def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """Train LightGBM on full in-sample data.

        Steps:
        1. Validate required columns exist
        2. Label: direction-aligned future return over label_horizon bars
        3. Drop NaN rows (last label_horizon bars + any feature NaNs)
        4. Fit LightGBM binary classifier

        Args:
            train_data: Full in-sample DataFrame.

        Returns:
            wrap_fit_result with trained model. Empty dict on failure.
        """
        self._check_lgbm_installed()

        required = list(dict.fromkeys(
            self._features + ["log_return", "direction_indicator"]
        ))
        missing = [c for c in required if c not in train_data.columns]
        if missing:
            logger.warning(
                "LGBMRegimeAdapter.fit(): missing columns %s — skipping.",
                missing,
            )
            return {}

        df = train_data.copy()

        # Direction-aligned future return label
        # shift(-1): label window starts from next bar — no look-ahead
        future_ret = (
            df["log_return"]
            .shift(-1)
            .rolling(self._label_horizon)
            .sum()
        )
        df["_label"] = (
            (future_ret * df["direction_indicator"]) > 0
        ).astype(int)

        # Drop NaN rows (feature NaNs + last label_horizon rows)
        df = df.dropna(subset=self._features + ["_label"])

        if len(df) < _MIN_TRAINING_ROWS:
            logger.warning(
                "LGBMRegimeAdapter.fit(): insufficient rows after dropna "
                "(%d < %d).", len(df), _MIN_TRAINING_ROWS,
            )
            return {}

        X = df[self._features]  # keep DataFrame for feature name tracking
        y = df["_label"].values

        # Class imbalance guard
        pos_ratio = float(y.mean())
        if pos_ratio == 0.0 or pos_ratio == 1.0:
            logger.warning(
                "LGBMRegimeAdapter.fit(): degenerate labels "
                "(pos_ratio=%.3f) — skipping.", pos_ratio,
            )
            return {}

        import lightgbm as lgb  # type: ignore[import]

        model = lgb.LGBMClassifier(
            n_estimators=self._n_estimators,
            learning_rate=self._learning_rate,
            max_depth=self._max_depth,
            num_leaves=self._num_leaves,
            min_child_samples=self._min_child_samples,
            random_state=self._random_seed,
            n_jobs=-1,
            verbose=-1,
        )

        try:
            model.fit(X, y)
        except Exception as exc:  # noqa: BLE001
            logger.error("LGBMRegimeAdapter.fit(): training failed: %s", exc)
            return {}

        logger.info(
            "LGBMRegimeAdapter: trained on %d rows "
            "(pos_ratio=%.3f, horizon=%d)",
            len(df), pos_ratio, self._label_horizon,
        )

        return BaseModel.wrap_fit_result({
            "model":         model,
            "features":      self._features,
            "label_horizon": self._label_horizon,
        })

    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
        ) -> pd.DataFrame:
        """Generate signals from LightGBM regime probability.

        Runs inference on the full test window and delegates to
        assemble_signal for sticky / ADX / QPB filtering.

        Args:
            test_data: Out-of-sample DataFrame.
            params:    Dict from fit() containing the trained model.

        Returns:
            test_data with signal, confidence, regime_prob columns.
        """
        df    = test_data.copy()
        model = params.get("model")

        if model is None:
            df["signal"]      = 0
            df["confidence"]  = 0.0
            df["regime_prob"] = 0.5
            return df

        features: list[str] = params.get("features", self._features)
        missing = [c for c in features if c not in df.columns]
        if missing:
            logger.warning(
                "LGBMRegimeAdapter.predict(): missing columns %s — all flat.",
                missing,
            )
            df["signal"]      = 0
            df["confidence"]  = 0.0
            df["regime_prob"] = 0.5
            return df

        # Inference — NaN rows default to 0.5
        valid_mask  = df[features].notna().all(axis=1)
        regime_prob = np.full(len(df), 0.5, dtype=np.float32)

        if valid_mask.any():
            X_valid = df.loc[valid_mask, features]  # DataFrame preserves feature names
            proba   = model.predict_proba(X_valid)
            # Class 1 = profitable breakout direction
            regime_prob[valid_mask.values] = proba[:, 1].astype(np.float32)

        return assemble_signal(
            df,
            regime_prob=pd.Series(regime_prob, index=df.index),
            risk_cfg=self._risk,
            filters_cfg=self._filters,
            trade_cfg=self._trade,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _check_lgbm_installed() -> None:
        try:
            import lightgbm  # noqa: F401  # type: ignore[import]
        except ModuleNotFoundError as exc:
            raise ImportError(
                "The 'lightgbm' package is required for LGBMRegimeAdapter. "
                "Install it with:\n  pip install lightgbm"
            ) from exc
