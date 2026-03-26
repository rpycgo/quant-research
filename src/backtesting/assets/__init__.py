"""
backtesting.assets
==================
Asset-class-specific data loaders and preprocessors.

Each sub-package (``crypto``, ``equities``, ``fx``) exposes a concrete
:class:`~backtesting.core.base_loader.BaseLoader` implementation and a
matching preprocessor that computes the generic feature columns required
by the engine layer.

Currently implemented
---------------------
* :mod:`backtesting.assets.crypto` — Binance-compatible OHLCV loader and
  crypto feature preprocessor.
"""

__all__ = ["crypto"]
