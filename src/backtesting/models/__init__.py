"""
backtesting.models
==================
Model registry and adapter layer.

The registry maps string keys (e.g. ``"mdrs_sde_btc"``) to fully
configured :class:`~backtesting.core.base_model.BaseModel` instances.
Each concrete adapter in the ``adapters/`` sub-package wraps an external
model package and translates its output into the ``signal`` / ``confidence``
contract required by the engine layer.

Registering a new model
-----------------------
1. Install the external package via ``pyproject.toml``::

       [tool.poetry.dependencies]
       my-new-model = {git = "https://github.com/rpycgo/my-new-model.git"}

2. Add a local-override config under
   ``configs/model_parameters/my_new_model_btc.toml`` (keys that differ
   from the package default only).

3. Create an adapter in ``adapters/my_new_model.py`` that subclasses
   :class:`~backtesting.core.base_model.BaseModel`.

4. Register the key→class mapping in
   :data:`~backtesting.models.registry.ModelRegistry._REGISTRY`.
"""
from backtesting.models.registry import ModelRegistry


__all__ = ["ModelRegistry"]
