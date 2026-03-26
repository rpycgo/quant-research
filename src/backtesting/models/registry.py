"""
backtesting.models.registry
============================
Central registry that maps model-key strings to fully configured
:class:`~backtesting.core.base_model.BaseModel` instances.

Adding a new model
------------------
1. Implement an adapter in ``backtesting/models/adapters/<name>.py``.
2. Add the key → adapter class mapping to :data:`_REGISTRY`.
3. Add the key → external package mapping to
   :data:`~backtesting.core.config_loader._MODEL_PACKAGE_MAP`.

The registry is intentionally simple: it is a plain ``dict`` rather than
a plugin system.  This makes the codebase easy to navigate and the full
set of supported models visible at a glance.
"""
from __future__ import annotations

import logging

from src.backtesting.core.base_model import BaseModel
from src.backtesting.core.config_loader import BacktestConfigLoader
from src.backtesting.models.adapters.garch import GarchCryptoAdapter
from src.backtesting.models.adapters.mdrs_sde import MdrsSdeCryptoAdapter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# model_key → adapter class
# ---------------------------------------------------------------------------
# Keys follow the convention  <algorithm>_<asset>  so that the config-loader
# can resolve the correct ``configs/model_parameters/<key>.toml`` override.
# ---------------------------------------------------------------------------
_REGISTRY: dict[str, type[BaseModel]] = {
    "mdrs_sde_btc": MdrsSdeCryptoAdapter,
    "mdrs_sde_eth": MdrsSdeCryptoAdapter,
    "garch_btc": GarchCryptoAdapter,
    "garch_eth": GarchCryptoAdapter,
}


class ModelRegistry:
    """Factory that constructs adapter instances from a string key.

    All public methods are static so callers do not need to instantiate
    the registry class itself.

    Example::

        from backtesting.models.registry import ModelRegistry
        from backtesting.core.config_loader import BacktestConfigLoader

        loader = BacktestConfigLoader()
        model  = ModelRegistry.get("mdrs_sde_btc", loader)
    """
    @staticmethod
    def get(
        model_key: str,
        config_loader: BacktestConfigLoader,
        ) -> BaseModel:
        """Construct and return the adapter for *model_key*.

        Loads the merged model config (package default + local override)
        and the shared backtest config, then passes both to the adapter
        constructor.

        Args:
            model_key: Registered model identifier, e.g.
                ``"mdrs_sde_btc"`` or ``"garch_eth"``.
            config_loader: Initialised
                :class:`~backtesting.core.config_loader.BacktestConfigLoader`
                used to resolve both model-specific and shared configs.

        Returns:
            Configured :class:`~backtesting.core.base_model.BaseModel`
            instance ready for ``fit()`` / ``predict()``.

        Raises:
            KeyError: If *model_key* is not in :data:`_REGISTRY`.
        """
        if model_key not in _REGISTRY:
            raise KeyError(
                f"Unknown model key '{model_key}'. "
                f"Register it in backtesting/models/registry.py. "
                f"Available keys: {ModelRegistry.available()}"
            )

        adapter_cls = _REGISTRY[model_key]
        model_config = config_loader.get_model_config(model_key)
        backtest_config = config_loader.get_backtest_settings()

        logger.info(
            "ModelRegistry: constructing '%s' → %s",
            model_key,
            adapter_cls.__name__,
        )

        return adapter_cls(
            model_config=model_config,
            backtest_config=backtest_config,
        )

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
        overwrite: bool = False,
        ) -> None:
        """Programmatically register a new adapter at runtime.

        Useful for notebooks and ad-hoc experiments where creating a new
        adapter file is impractical.

        Args:
            model_key:   Unique string key for this model.
            adapter_cls: A class that subclasses
                :class:`~backtesting.core.base_model.BaseModel` and accepts
                ``model_config`` and ``backtest_config`` keyword arguments.
            overwrite:   When ``True``, silently replace an existing key.
                Default ``False`` raises ``ValueError`` on collision.

        Raises:
            ValueError: If *model_key* already exists and *overwrite* is
                ``False``.
        """
        if model_key in _REGISTRY and not overwrite:
            raise ValueError(
                f"Model key '{model_key}' is already registered. "
                f"Pass overwrite=True to replace it."
            )

        _REGISTRY[model_key] = adapter_cls
        logger.info(
            "ModelRegistry: registered '%s' → %s",
            model_key,
            adapter_cls.__name__,
        )

    @staticmethod
    def _raw() -> dict[str, type[BaseModel]]:
        """Return the raw registry dict.

        Intended for testing and introspection only.  Do not mutate the
        returned dict directly; use :meth:`register` instead.

        Returns:
            The internal ``_REGISTRY`` dictionary.
        """
        return _REGISTRY
