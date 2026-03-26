"""
backtesting
===========
Model-agnostic backtesting framework for the quant-research platform.

Supports multiple asset classes (crypto, equities, FX) and pluggable model
architectures through a clean interface contract.  External model packages
register themselves via the :mod:`backtesting.models.registry` and expose a
:class:`~backtesting.core.base_model.BaseModel` implementation; the engine
layer remains entirely unaware of their internals.

Typical usage::

    from backtesting.models.registry import ModelRegistry
    from backtesting.engines.walk_forward import WalkForwardRunner
    from backtesting.core.config_loader import BacktestConfigLoader

    loader = BacktestConfigLoader()
    model  = ModelRegistry.get("mdrs_sde_btc")
    runner = WalkForwardRunner(model=model, symbol="BTCUSDT", config_loader=loader)
    result = runner.run()
"""

__version__ = "0.1.0"
__all__ = ["core", "assets", "engines", "models"]
