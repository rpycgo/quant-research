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
            A dictionary with the following structure:

            * ``"summary"``   — parameter summary (e.g. ``pd.DataFrame``
                                from arviz for MCMC models, or an empty
                                ``pd.DataFrame`` for non-probabilistic models).
            * ``"estimates"`` — plain ``dict`` of point estimates passed to
                                :meth:`predict`.

            Returns ``{"summary": pd.DataFrame(), "estimates": {}}`` when
            estimation fails so that the caller can detect failure without
            an exception.

            Non-MCMC adapters should wrap their return value using
            :meth:`wrap_fit_result`.
        """
        pass

    @staticmethod
    def wrap_fit_result(params: dict[str, Any]) -> dict[str, Any]:
        """Wrap a plain params dict into the standard fit() return structure.

        Non-probabilistic models (GARCH, DL, Simple Breakout, etc.) that
        return a plain ``dict`` from ``fit()`` should call this helper to
        conform to the standard contract expected by
        :class:`~backtesting.engines.walk_forward.WalkForwardRunner`.

        Args:
            params: Plain parameter dict returned by the concrete adapter.

        Returns:
            ``{"summary": pd.DataFrame(), "estimates": params}``

        Example::

            def fit(self, train_data):
                ...
                return BaseModel.wrap_fit_result({"window": self._window})
        """
        return {"summary": pd.DataFrame(), "estimates": params}

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
