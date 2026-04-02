"""
backtesting.assets.crypto
=========================
Data loading and feature engineering for cryptocurrency markets.

Exports
-------
CryptoLoader
    Fetches OHLCV data from Binance via ``ccxt`` or falls back to a local
    CSV file (useful for offline testing).
CryptoPreprocessor
    Computes generic crypto features (log-returns, Z-scores, ADX, Donchian
    channels) shared by every model adapter that targets crypto assets.
"""

from backtesting.assets.crypto.loader import CryptoLoader
from backtesting.assets.crypto.preprocessor import CryptoPreprocessor

__all__ = ["CryptoLoader", "CryptoPreprocessor"]
