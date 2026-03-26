"""
backtesting.core.base_loader
============================
Abstract base class for all asset-class data loaders.

Each concrete loader is responsible for fetching raw OHLCV data from its
source (exchange API, local CSV, database) and returning a consistently
shaped ``DataFrame`` so the rest of the pipeline does not need to know
where the data came from.

Output contract
---------------
The ``DataFrame`` returned by :meth:`load` **must** satisfy:

* ``DatetimeIndex`` (UTC-aware is preferred).
* Columns: ``Open``, ``High``, ``Low``, ``Close``, ``Volume`` — all
  ``float64``.
* No duplicate index entries.
* Rows sorted in ascending chronological order.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
import pandas as pd


class BaseLoader(ABC):
    """Abstract interface for OHLCV data loading.

    Subclass this to add support for a new asset class or data source.
    The walk-forward runner calls :meth:`load` once per backtest run and
    passes the result to the preprocessor, so all cleaning and resampling
    should happen here.

    Example::

        class MyLoader(BaseLoader):
            def load(
                self,
                symbol: str,
                start: str,
                end: str,
            ) -> pd.DataFrame:
                df = fetch_from_my_source(symbol, start, end)
                return self._normalise(df)
    """

    @abstractmethod
    def load(
        self,
        symbol: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """Fetch OHLCV data for *symbol* over ``[start, end]``.

        Args:
            symbol: Ticker or trading pair, e.g. ``"BTCUSDT"`` or
                ``"AAPL"``.  Concrete loaders define which symbols they
                support.
            start:  ISO-8601 date string, e.g. ``"2024-01-01"``.
            end:    ISO-8601 date string, e.g. ``"2026-01-31"``.

        Returns:
            ``DataFrame`` with a ``DatetimeIndex`` and at least the columns
            ``Open``, ``High``, ``Low``, ``Close``, ``Volume``, all cast to
            ``float64``.

        Raises:
            ValueError: If ``symbol`` is not supported by this loader.
            RuntimeError: If the data source is unreachable or returns an
                unexpected format.
        """
        pass
