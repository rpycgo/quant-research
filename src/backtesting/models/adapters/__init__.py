"""
backtesting.models.adapters
============================
Concrete :class:`~backtesting.core.base_model.BaseModel` adapters.

Each adapter wraps one external model package and:

* Translates the package's native API into ``fit()`` / ``predict()``.
* Ensures ``predict()`` always returns a ``DataFrame`` containing at
  minimum ``signal`` (``int``: 1 / -1 / 0) and ``confidence`` (``float``).
* Keeps all model-specific filtering (sticky breakout, ADX, zone selection,
  SNR scaling) **inside** the adapter so the engine layer stays clean.

Currently implemented
---------------------
* :class:`~backtesting.models.adapters.mdrs_sde.MdrsSdeCryptoAdapter`
  — wraps the ``mdrs-sde-btc`` package.
* :class:`~backtesting.models.adapters.garch.GarchCryptoAdapter`
  — wraps the ``arch`` GARCH(1,1) benchmark.
"""

__all__ = ["MdrsSdeCryptoAdapter", "GarchCryptoAdapter"]