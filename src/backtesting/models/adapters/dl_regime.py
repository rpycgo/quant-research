"""
backtesting.models.adapters.dl_regime
=======================================
Adapter that wraps pre-trained DL regime models (LSTM, TCN, Transformer)
from the ``dl-regime`` package and exposes them through the
:class:`~backtesting.core.base_model.BaseModel` interface.

Responsibilities
----------------
* **fit()** — loads the pre-trained checkpoint for the current WFA window.
  No actual training happens here; training is performed offline by
  ``dl-regime/run_train.py``.
* **predict()** — runs inference to produce ``regime_prob``, applies the
  same sticky filter + ADX gate used by MDRS-SDE, and maps results to
  ``signal`` / ``confidence`` columns.

This adapter enables direct comparison of DL baselines against MDRS-SDE
within the same WalkForwardRunner and GenericBacktestEngine, ensuring
identical evaluation conditions for ESWA submission.

External dependency
-------------------
``dl-regime`` must be installed::

    pip install git+https://github.com/rpycgo/dl-regime.git
"""
from __future__ import annotations

import logging
import pathlib
from typing import Any

import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import StandardScaler

from backtesting.core.base_model import BaseModel

logger = logging.getLogger(__name__)

_DEFAULT_MIN_DURATION = 5


class DlRegimeCryptoAdapter(BaseModel):
    """Adapter for pre-trained DL regime detection models.

    Loads per-window checkpoints produced by ``dl-regime`` WFA training
    and generates trading signals using the same filtering logic as
    MDRS-SDE (sticky filter + ADX gate + direction gate).

    DL models use **fixed** execution params (no SNR scaling) since
    they lack MCMC posterior estimates.  The engine's
    ``build_dynamic_params`` receives fallback values so that TP/SL/
    trailing stop ratios come from the config defaults.

    Args:
        model_config: Merged config dict from
            ``BacktestConfigLoader.get_model_config("dl_regime_*")``.
        backtest_config: Parsed backtest settings dict.

    Example::

        python run_backtest.py --model dl_regime_lstm_btc --symbol BTCUSDT
    """
    def __init__(
        self,
        model_config: dict[str, Any],
        backtest_config: dict[str, Any],
        ) -> None:
        self._model_config = model_config
        self._risk = backtest_config.get("risk_management", {})
        self._filters = backtest_config.get("filters", {})
        self._trade = backtest_config.get("trading_parameters", {})

        # DL model settings
        model_cfg = model_config.get("model", {})
        self._architecture: str = model_cfg.get("architecture", "lstm")
        self._checkpoint_dir = pathlib.Path(
            model_cfg.get("checkpoint_dir", "checkpoints")
        ) / self._architecture

        # Training settings (needed for inference)
        train_cfg = model_config.get("training", {})
        self._seq_len: int = train_cfg.get("seq_len", 60)
        self._batch_size: int = train_cfg.get("batch_size", 256)

        self._features: list[str] = model_cfg.get(
            "input_features",
            ["hybrid_z_score", "log_return", "direction_indicator",
             "volume_z_score", "absolute_return_z_score"],
        )

    # ------------------------------------------------------------------
    # BaseModel interface
    # ------------------------------------------------------------------

    def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """Load the pre-trained checkpoint for this WFA window.

        The window label is derived from the training data's end date:
        test_start = first day of the month after train_data ends.

        Args:
            train_data: In-sample ``DataFrame``.  Used only to determine
                which checkpoint to load.

        Returns:
            Dict with ``"model"`` (loaded Lightning module) and metadata.
            Empty dict if checkpoint not found.
        """
        train_end = train_data.index.max()
        test_start = (train_end + pd.DateOffset(months=1)).replace(day=1)
        window_label = test_start.strftime("%Y-%m-%d")

        ckpt_path = self._checkpoint_dir / window_label / "model.ckpt"
        if not ckpt_path.exists():
            logger.warning(
                "DlRegimeAdapter: checkpoint not found: %s", ckpt_path
            )
            return {}

        model = self._load_model(ckpt_path)
        logger.info(
            "DlRegimeAdapter: loaded %s checkpoint for window %s",
            self._architecture, window_label,
        )

        # Return fallback SDE-like params so engine.build_dynamic_params
        # produces reasonable fixed values
        return {
            "model": model,
            "checkpoint_path": str(ckpt_path),
            "window_label": window_label,
            "architecture": self._architecture,
            # Fallback values for build_dynamic_params (no SNR scaling)
            "alpha_long": 30.0,
            "alpha_short": 20.0,
            "sigma_1": self._risk.get("reference_sigma_1", 14.665),
        }

    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
        ) -> pd.DataFrame:
        """Generate trading signals from the DL model's regime probability.

        Pipeline (mirrors MDRS-SDE for fair comparison):

        1. DL inference → ``regime_prob`` in [0, 1].
        2. Sticky-breakout persistence filter.
        3. Direction gate (Close vs dynamic_resistance / dynamic_support).
        4. ADX gate.
        5. Map to ``signal`` (1 / -1 / 0) and ``confidence``.

        Args:
            test_data: Out-of-sample ``DataFrame``.
            params:    Dict from :meth:`fit` containing the loaded model.

        Returns:
            ``test_data`` with ``signal``, ``confidence``, ``regime_prob``.
        """
        df = test_data.copy()
        model = params.get("model")

        if model is None:
            df["signal"]      = 0
            df["confidence"]  = 0.0
            df["regime_prob"] = 0.5
            return df

        # Step 1 — DL inference → (n,) sigmoid regime_prob
        regime_prob = self._run_inference(model, df)
        df["regime_prob"] = regime_prob
        df["confidence"]  = regime_prob

        # Step 2 — sticky filter (mirrors MDRS-SDE for fair comparison)
        entry_thr  = self._risk.get("entry_probability_threshold", 0.5)
        min_dur    = self._risk.get("minimum_signal_duration", _DEFAULT_MIN_DURATION)
        use_sticky = self._filters.get("use_sticky", True)
        use_adx    = self._filters.get("use_adx", True)
        adx_thr    = self._trade.get("adx_threshold", 30)

        binary = (df["regime_prob"] > entry_thr).astype(int)
        if use_sticky:
            sticky = (
                binary.rolling(window=min_dur).sum() == min_dur
            ).astype(int)
        else:
            sticky = binary

        # Step 3 — direction gate (identical to MDRS-SDE for fair comparison)
        long_cond  = df["Close"] > df["dynamic_resistance"]
        short_cond = df["Close"] < df["dynamic_support"]

        # Step 4 — ADX gate
        adx_pass = (
            df["ADX"] > adx_thr
            if use_adx
            else pd.Series(True, index=df.index)
        )

        # Step 5 — signal assembly
        df["signal"] = 0
        df.loc[sticky.astype(bool) & long_cond  & adx_pass, "signal"] =  1
        df.loc[sticky.astype(bool) & short_cond & adx_pass, "signal"] = -1

        return df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_model(self, ckpt_path: pathlib.Path) -> Any:
        """Load a Lightning model from checkpoint."""
        try:
            from dl_regime.models import (
                LSTMRegimeModel,
                TCNRegimeModel,
                TransformerRegimeModel,
            )
        except ModuleNotFoundError as exc:
            raise ImportError(
                "The 'dl-regime' package is required for DlRegimeCryptoAdapter. "
                "Install it with:\n"
                "  pip install git+https://github.com/rpycgo/dl-regime.git"
            ) from exc

        model_cls = {
            "lstm": LSTMRegimeModel,
            "tcn": TCNRegimeModel,
            "transformer": TransformerRegimeModel,
        }[self._architecture]

        model = model_cls.load_from_checkpoint(str(ckpt_path), map_location="cpu")
        model.eval()

        return model

    def _run_inference(
        self,
        model: Any,
        test_df: pd.DataFrame,
        ) -> np.ndarray:
        """Run batched sliding-window inference, return class probs aligned to test_df.

        Builds all sliding windows at once as a single tensor and runs
        inference in batches. Returns a (n,) array of sigmoid probabilities
        for [flat, long, short].
        """
        valid_mask = test_df[self._features].notna().all(axis=1)
        valid_df   = test_df.loc[valid_mask, self._features]

        if len(valid_df) < self._seq_len:
            logger.warning(
                "DlRegimeAdapter: insufficient valid rows (%d < seq_len=%d).",
                len(valid_df), self._seq_len,
            )
            flat = np.zeros((len(test_df), 3), dtype=np.float32)
            flat[:, 0] = 1.0  # all flat

            return flat

        # Scale features
        scaler = StandardScaler()
        X      = scaler.fit_transform(valid_df.values.astype(np.float32))

        # Build all sliding windows — shape: (n_windows, seq_len, n_features)
        n_windows = len(X) - self._seq_len
        sequences = np.stack([X[i: i + self._seq_len] for i in range(n_windows)])

        # Batched inference on CPU — returns (n_windows,) sigmoid probs
        probs: list[float] = []
        model.eval()
        with torch.no_grad():
            for i in range(0, n_windows, self._batch_size):
                batch = torch.from_numpy(sequences[i: i + self._batch_size])
                out   = model(batch)
                probs.extend(out["regime_prob"].cpu().numpy().tolist())

        prob_array = np.array(probs, dtype=np.float32)  # (n_windows,)

        # Map back to original index — default 0.5
        full_prob     = np.full(len(test_df), 0.5, dtype=np.float32)
        valid_indices = np.where(valid_mask.values)[0]
        for i, prob in enumerate(prob_array):
            original_idx = valid_indices[i + self._seq_len]
            full_prob[original_idx] = prob

        return full_prob
