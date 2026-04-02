"""
backtesting.assets.crypto.loader
=================================
OHLCV data loader for cryptocurrency spot and futures markets.

Primary source: Binance via ``ccxt``.
Fallback source: local CSV file (for offline / CI usage).

The loader validates the requested symbol against the list declared in
``configs/data_settings.toml → [binance_collection] supported_symbols``
and casts all price / volume columns to ``float64`` before returning.

Output ``DataFrame`` schema
---------------------------
* Index  : ``DatetimeIndex`` (UTC)
* Columns: ``Open``, ``High``, ``Low``, ``Close``, ``Volume`` — ``float64``
"""
from __future__ import annotations

import logging
import pathlib
from typing import Any
import pandas as pd

from backtesting.core.base_loader import BaseLoader


logger = logging.getLogger(__name__)

_OHLCV_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]
_CCXT_OHLCV_COLUMNS = ["timestamp", "Open", "High", "Low", "Close", "Volume"]


class CryptoLoader(BaseLoader):
    """Loads OHLCV data for cryptocurrency pairs.

    Attempts to fetch from Binance via ``ccxt`` first; if ``ccxt`` is not
    installed or the request fails, it falls back to a local CSV at
    ``data/<symbol_lower>.csv``.

    Args:
        config: Parsed ``[binance_collection]`` section from
            ``data_settings.toml``.  Must contain:

            * ``supported_symbols`` (list[str])
            * ``interval``          (str)  — e.g. ``"5m"``
            * ``output_directory``  (str)  — relative to project root

    Example::

        loader = CryptoLoader(config=data_cfg["binance_collection"])
        df = loader.load("BTCUSDT", "2024-01-01", "2026-01-31")
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._supported: list[str] = config.get("supported_symbols", [])
        self._interval: str = config.get("interval", "5m")
        self._output_dir: pathlib.Path = (
            pathlib.Path(__file__).resolve().parent.parent.parent.parent.parent
            / config.get("output_directory", "data")
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self,
        symbol: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """Fetch OHLCV data for *symbol* over ``[start, end]``.

        Args:
            symbol: Trading pair, e.g. ``"BTCUSDT"``.
            start:  ISO-8601 date string, e.g. ``"2024-01-01"``.
            end:    ISO-8601 date string, e.g. ``"2026-01-31"``.

        Returns:
            Cleaned ``DataFrame`` with a UTC ``DatetimeIndex`` and columns
            ``Open``, ``High``, ``Low``, ``Close``, ``Volume``.

        Raises:
            ValueError: If *symbol* is not in the supported list.
        """
        self._validate_symbol(symbol)

        df = self._fetch_from_exchange(symbol, start, end)
        if df is None or df.empty:
            logger.warning(
                "Exchange fetch failed for %s — falling back to local CSV.",
                symbol,
            )
            df = self._load_from_csv(symbol, self._interval)

        return self._clean(df, start, end)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_symbol(self, symbol: str) -> None:
        if self._supported and symbol not in self._supported:
            raise ValueError(
                f"Symbol '{symbol}' is not supported. "
                f"Add it to configs/data_settings.toml → supported_symbols "
                f"or implement a dedicated loader. "
                f"Currently supported: {self._supported}"
            )

    def _fetch_from_exchange(
        self,
        symbol: str,
        start: str,
        end: str,
    ) -> pd.DataFrame | None:
        """Attempt to fetch OHLCV candles from Binance via ccxt.

        Returns ``None`` on any error so the caller can fall back to CSV.
        """
        try:
            import ccxt  # optional dependency

            exchange = ccxt.binance({"options": {"defaultType": "future"}})
            since_ms = exchange.parse8601(f"{start}T00:00:00Z")
            end_ms = exchange.parse8601(f"{end}T23:59:59Z")

            all_candles: list[list] = []
            while since_ms < end_ms:
                candles = exchange.fetch_ohlcv(
                    symbol, self._interval, since=since_ms, limit=1000
                )
                if not candles:
                    break
                all_candles.extend(candles)
                since_ms = candles[-1][0] + 1

            if not all_candles:
                return None

            df = pd.DataFrame(all_candles, columns=_CCXT_OHLCV_COLUMNS)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df = df.set_index("timestamp")
            df.index.name = None

            return df

        except Exception as exc:  # noqa: BLE001
            logger.debug("ccxt fetch error for %s: %s", symbol, exc)
            return None

    def _load_from_csv(self, symbol: str, interval: str) -> pd.DataFrame:
        """Load OHLCV data from a local CSV file.

        Expects a file named ``<symbol_lower>.csv`` (e.g. ``btcusdt.csv``)
        in the directory configured by ``output_directory``.

        Args:
            symbol: Trading pair symbol, e.g. ``"BTCUSDT"``.

        Returns:
            Parsed ``DataFrame``.

        Raises:
            FileNotFoundError: If the expected CSV file does not exist.
        """
        ## To do
        ## hard coding the path for now, but ideally this should be configurable or discovered
        csv_path = self._output_dir / 'crypto/binance/futures' / f"{symbol.lower()}_{interval}.csv"

        # Also try the naming convention used in the SDE repo (btcusdt_future.csv)
        if not csv_path.exists():
            csv_path = self._output_dir / f"{symbol.lower()}_future_{interval}.csv"

        if not csv_path.exists():
            raise FileNotFoundError(
                f"No local CSV found for '{symbol}'. "
                f"Expected path: {csv_path}. "
                f"Run the data collector or provide a CSV file."
            )

        logger.info("Loading %s from local CSV: %s", symbol, csv_path)
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)

        # Ensure the index is timezone-aware UTC
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")

        return df

    @staticmethod
    def _clean(
        df: pd.DataFrame,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """Normalise, slice, and cast the raw ``DataFrame``.

        Args:
            df:    Raw OHLCV ``DataFrame``.
            start: Slice start (ISO-8601 date string).
            end:   Slice end   (ISO-8601 date string).

        Returns:
            Cleaned ``DataFrame`` ready for the preprocessor.
        """
        # Keep only canonical OHLCV columns
        df = df[[c for c in _OHLCV_COLUMNS if c in df.columns]].copy()
        df = df.astype(float)

        # Drop duplicates and sort
        df = df[~df.index.duplicated(keep="first")]
        df = df.sort_index()

        # Slice to the requested window
        df = df.loc[start:end]

        return df
