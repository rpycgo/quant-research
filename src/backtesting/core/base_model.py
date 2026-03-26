"""
backtesting.core.base_model
===========================
Abstract base class that every trading model must implement.

Any model — regardless of asset class, statistical approach, or inference
backend — must expose :meth:`fit` and :meth:`predict` so the engine layer
can consume it without knowing the model's internals.

Contract
--------
* ``fit``    accepts a training DataFrame and returns a plain ``dict`` of
             estimated parameters.  The dict is opaque to the engine; only
             the model's own ``predict`` method interprets it.
* ``predict`` accepts a test DataFrame **and** the parameter dict returned
             by ``fit``, and returns that same DataFrame augmented with at
             least two columns:

             - ``signal``     (``int``)   – 1 = Long, -1 = Short, 0 = Flat
             - ``confidence`` (``float``) – model confidence in ``[0, 1]``
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class BaseModel(ABC):
    """Abstract interface for all backtesting models.

    Subclass this and implement :meth:`fit` and :meth:`predict` to plug any
    model into the :class:`~backtesting.engines.walk_forward.WalkForwardRunner`.

    Example::

        class MyModel(BaseModel):
            def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
                ...
                return {"mu": mu_hat, "sigma": sigma_hat}

            def predict(
                self,
                test_data: pd.DataFrame,
                params: dict[str, Any],
            ) -> pd.DataFrame:
                test_data["signal"]     = ...   # int  : 1 / -1 / 0
                test_data["confidence"] = ...   # float: 0.0 – 1.0
                return test_data
    """
    @abstractmethod
    def fit(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """Estimate model parameters from in-sample data.

        Args:
            train_data: OHLCV ``DataFrame`` (with a ``DatetimeIndex``) plus
                any feature columns the concrete model requires.  The caller
                guarantees the DataFrame has been preprocessed and contains
                no leading ``NaN`` rows in the essential feature columns.

        Returns:
            A plain dictionary of estimated parameters.  The schema is
            model-specific; the engine layer never reads these values
            directly.  Returns an **empty dict** (not ``None``) when
            estimation fails so that the caller can detect failure without
            an exception.
        """
        pass

    @abstractmethod
    def predict(
        self,
        test_data: pd.DataFrame,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        """Generate trading signals for the out-of-sample period.

        This method is responsible for **all** model-specific logic:
        feature computation, filter application (sticky, ADX, zone), and
        dynamic parameter scaling.  The engine receives only the returned
        DataFrame and treats ``signal`` / ``confidence`` as the sole entry
        criteria.

        Args:
            test_data: OHLCV ``DataFrame`` covering the test window.  May
                contain pre-computed generic features from the preprocessor.
            params: The parameter dictionary produced by :meth:`fit`.

        Returns:
            ``test_data`` augmented with at minimum:

            * ``signal``     (``int``)   – 1 = Long, -1 = Short, 0 = Flat.
            * ``confidence`` (``float``) – model confidence in ``[0.0, 1.0]``.

            Any additional columns (e.g. ``regime_prob``, ``sticky_signal``)
            are preserved and available for downstream analysis.

        Raises:
            KeyError: If ``test_data`` is missing columns required by this
                model.
        """
        pass
