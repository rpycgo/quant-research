"""
backtesting.models.registry
============================
Central registry that maps model-key strings to fully configured
:class:`~backtesting.core.base_model.BaseModel` instances.

Adding a new model
------------------
1. Implement an adapter in ``backtesting/models/adapters/<name>.py``.
2. Add a :class:`ModelEntry` to :data:`_REGISTRY` with the adapter class
   and any required metadata flags.
3. Add the key → external package mapping to
   :data:`~backtesting.core.config_loader._MODEL_PACKAGE_MAP` if the model
   depends on an external package.

Metadata flags
--------------
``requires_event_tagging`` (bool, default ``False``)
    When ``True``, ``backtest.py`` will run event detection, apply event
    tagging to ``full_data``, and pass zone-filtered rows as ``train_data``
    to ``WalkForwardRunner``. Required by MDRS-SDE which trains MCMC only
    on breakout-zone rows.

    When ``False``, ``train_data`` is set to ``full_data`` directly,
    skipping the DatasetBuilder pipeline entirely.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from backtesting.core.base_model import BaseModel
from backtesting.core.config_loader import BacktestConfigLoader
from backtesting.models.adapters.dl_regime import DlRegimeCryptoAdapter
from backtesting.models.adapters.garch import GarchCryptoAdapter
from backtesting.models.adapters.hmm_regime import HMMRegimeAdapter
from backtesting.models.adapters.ma_crossover import MACrossoverAdapter
from backtesting.models.adapters.mdrs_sde import MdrsSdeCryptoAdapter
from backtesting.models.adapters.rsi import RSIAdapter
from backtesting.models.adapters.simple_breakout import SimpleBreakoutAdapter

logger = logging.getLogger(__name__)


@dataclass
class ModelEntry:
    """Registry entry for a single model key.

    Attributes:
        adapter_cls:            Adapter class to instantiate.
        requires_event_tagging: When ``True``, backtest.py runs the full
                                DatasetBuilder pipeline (event tagging +
                                zone-filtered train_data). Default ``False``.
    """
    adapter_cls:            type[BaseModel]
    requires_event_tagging: bool = field(default=False)


# ---------------------------------------------------------------------------
# model_key → ModelEntry
# ---------------------------------------------------------------------------
_REGISTRY: dict[str, ModelEntry] = {
    # MDRS-SDE — requires event tagging for zone-based MCMC training
    #            uses dynamic params (SNR scaling + EMA sigma reference)
    "mdrs_sde_btc":              ModelEntry(MdrsSdeCryptoAdapter,
                                            requires_event_tagging=True),
    "mdrs_sde_eth":              ModelEntry(MdrsSdeCryptoAdapter,
                                            requires_event_tagging=True),
    "mdrs_sde_sol":              ModelEntry(MdrsSdeCryptoAdapter,
                                            requires_event_tagging=True),
    "mdrs_sde_xrp":              ModelEntry(MdrsSdeCryptoAdapter,
                                            requires_event_tagging=True),
    # GARCH
    "garch_btc":                 ModelEntry(GarchCryptoAdapter),
    "garch_eth":                 ModelEntry(GarchCryptoAdapter),
    "garch_sol":                 ModelEntry(GarchCryptoAdapter),
    "garch_xrp":                 ModelEntry(GarchCryptoAdapter),
    # Simple Breakout
    "simple_breakout_btc":       ModelEntry(SimpleBreakoutAdapter),
    "simple_breakout_eth":       ModelEntry(SimpleBreakoutAdapter),
    "simple_breakout_sol":       ModelEntry(SimpleBreakoutAdapter),
    "simple_breakout_xrp":       ModelEntry(SimpleBreakoutAdapter),
    # MA Crossover
    "ma_crossover_btc":          ModelEntry(MACrossoverAdapter),
    "ma_crossover_eth":          ModelEntry(MACrossoverAdapter),
    "ma_crossover_sol":          ModelEntry(MACrossoverAdapter),
    "ma_crossover_xrp":          ModelEntry(MACrossoverAdapter),
    # RSI
    "rsi_btc":                   ModelEntry(RSIAdapter),
    "rsi_eth":                   ModelEntry(RSIAdapter),
    "rsi_sol":                   ModelEntry(RSIAdapter),
    "rsi_xrp":                   ModelEntry(RSIAdapter),
    # HMM
    "hmm_btc":                   ModelEntry(HMMRegimeAdapter),
    "hmm_eth":                   ModelEntry(HMMRegimeAdapter),
    "hmm_sol":                   ModelEntry(HMMRegimeAdapter),
    "hmm_xrp":                   ModelEntry(HMMRegimeAdapter),
    # DL Regime
    ## LSTM
    "dl_regime_lstm_btc":        ModelEntry(DlRegimeCryptoAdapter),
    "dl_regime_lstm_eth":        ModelEntry(DlRegimeCryptoAdapter),
    "dl_regime_lstm_sol":        ModelEntry(DlRegimeCryptoAdapter),
    "dl_regime_lstm_xrp":        ModelEntry(DlRegimeCryptoAdapter),
    ## TCN
    "dl_regime_tcn_btc":         ModelEntry(DlRegimeCryptoAdapter),
    "dl_regime_tcn_eth":         ModelEntry(DlRegimeCryptoAdapter),
    "dl_regime_tcn_sol":         ModelEntry(DlRegimeCryptoAdapter),
    "dl_regime_tcn_xrp":         ModelEntry(DlRegimeCryptoAdapter),
    ## Transformer
    "dl_regime_transformer_btc": ModelEntry(DlRegimeCryptoAdapter),
    "dl_regime_transformer_eth": ModelEntry(DlRegimeCryptoAdapter),
    "dl_regime_transformer_sol": ModelEntry(DlRegimeCryptoAdapter),
    "dl_regime_transformer_xrp": ModelEntry(DlRegimeCryptoAdapter),
}


class ModelRegistry:
    """Factory that constructs adapter instances from a string key.

    All public methods are static so callers do not need to instantiate
    the registry class itself.

    Example::

        loader = BacktestConfigLoader()
        model  = ModelRegistry.get("mdrs_sde_btc", loader)

        # Check metadata before running pipeline
        if ModelRegistry.requires_event_tagging("mdrs_sde_btc"):
            ...
    """
    @staticmethod
    def get(
        model_key: str,
        config_loader: BacktestConfigLoader,
        ) -> BaseModel:
        """Construct and return the adapter for *model_key*.

        Args:
            model_key:     Registered model identifier.
            config_loader: Initialised config loader.

        Returns:
            Configured :class:`~backtesting.core.base_model.BaseModel`
            instance ready for ``fit()`` / ``predict()``.

        Raises:
            KeyError: If *model_key* is not in :data:`_REGISTRY`.
        """
        entry = _REGISTRY.get(model_key)
        if entry is None:
            raise KeyError(
                f"Unknown model key '{model_key}'. "
                f"Register it in backtesting/models/registry.py. "
                f"Available keys: {ModelRegistry.available()}"
            )

        model_config   = config_loader.get_model_config(model_key)
        backtest_config = config_loader.get_backtest_settings()

        logger.info(
            "ModelRegistry: constructing '%s' -> %s",
            model_key,
            entry.adapter_cls.__name__,
        )

        return entry.adapter_cls(
            model_config=model_config,
            backtest_config=backtest_config,
        )

    @staticmethod
    def requires_event_tagging(model_key: str) -> bool:
        """Return whether this model requires event tagging.

        Args:
            model_key: Registered model identifier.

        Returns:
            ``True`` if the model requires DatasetBuilder event tagging
            and zone-filtered train_data. ``False`` otherwise.

        Raises:
            KeyError: If *model_key* is not in :data:`_REGISTRY`.
        """
        entry = _REGISTRY.get(model_key)
        if entry is None:
            raise KeyError(
                f"Unknown model key '{model_key}'. "
                f"Available keys: {ModelRegistry.available()}"
            )

        return entry.requires_event_tagging

    @staticmethod
    def available() -> list[str]:
        """Return a sorted list of all registered model keys.

        Returns:
            Sorted list of key strings.
        """
        return sorted(_REGISTRY.keys())

    @staticmethod
    def register(
        model_key: str,
        adapter_cls: type[BaseModel],
        *,
        requires_event_tagging: bool = False,
        overwrite: bool = False,
        ) -> None:
        """Programmatically register a new adapter at runtime.

        Args:
            model_key:              Unique string key for this model.
            adapter_cls:            Adapter class subclassing BaseModel.
            requires_event_tagging: Whether this model needs event tagging.
            overwrite:              When ``True``, replace existing key silently.

        Raises:
            ValueError: If *model_key* already exists and *overwrite* is ``False``.
        """
        if model_key in _REGISTRY and not overwrite:
            raise ValueError(
                f"Model key '{model_key}' is already registered. "
                f"Pass overwrite=True to replace it."
            )

        _REGISTRY[model_key] = ModelEntry(
            adapter_cls=adapter_cls,
            requires_event_tagging=requires_event_tagging,
        )
        logger.info(
            "ModelRegistry: registered '%s' -> %s",
            model_key,
            adapter_cls.__name__,
        )

    @staticmethod
    def _raw() -> dict[str, ModelEntry]:
        """Return the raw registry dict.

        Intended for testing and introspection only.
        """
        return _REGISTRY
