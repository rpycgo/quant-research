"""
backtesting.assets.crypto.funding_rate
========================================
Funding rate manager for trade-level PnL adjustment.

Loads per-symbol funding rate CSV and computes net funding cost
for a given holding period. Used by GenericBacktestEngine to
deduct funding fees from realized PnL.

Funding cost convention
-----------------------
* Long  position: pay when rate > 0, receive when rate < 0
                  → funding_cost = +Σ(rate)  (positive = cost)
* Short position: receive when rate > 0, pay when rate < 0
                  → funding_cost = -Σ(rate)  (positive = cost)

In both cases a positive funding_cost is subtracted from PnL.
"""
from __future__ import annotations

import logging
import pathlib

import pandas as pd

logger = logging.getLogger(__name__)


class FundingRateManager:
    """Computes trade-level funding costs from pre-collected CSV data.

    Args:
        project_root: Absolute path to the repository root.

    Example::

        mgr  = FundingRateManager(project_root=Path("."))
        cost = mgr.get_funding_cost(
            symbol        = "BTCUSDT",
            entry_time    = pd.Timestamp("2024-01-15 08:10:00"),
            exit_time     = pd.Timestamp("2024-01-17 14:00:00"),
            position_type = "Long",
        )
    """

    _FUNDING_DIR = pathlib.Path("data") / "crypto" / "binance" / "funding_rate"

    def __init__(self, project_root: pathlib.Path) -> None:
        self._root    = project_root
        self._cache: dict[str, pd.DataFrame | None] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_funding_cost(
        self,
        symbol: str,
        entry_time: pd.Timestamp,
        exit_time: pd.Timestamp,
        position_type: str,
        ) -> float:
        """Return net funding cost ratio for the holding period.

        Funding payments that occur strictly after entry_time and at or
        before exit_time are included (entry_time < t <= exit_time).

        Args:
            symbol:        Trading pair, e.g. ``"BTCUSDT"``.
            entry_time:    Position entry timestamp (UTC, tz-naive).
            exit_time:     Position exit timestamp  (UTC, tz-naive).
            position_type: ``"Long"`` or ``"Short"``.

        Returns:
            Net funding cost as a ratio (positive = cost to the trader).
            Returns 0.0 when funding data is unavailable.
        """
        df = self._load(symbol)
        if df is None:
            return 0.0

        # Normalize entry/exit to UTC-aware for comparison with funding datetime
        if entry_time.tzinfo is None:
            entry_time = entry_time.tz_localize("UTC")
        if exit_time.tzinfo is None:
            exit_time = exit_time.tz_localize("UTC")

        mask     = (df["datetime"] > entry_time) & (df["datetime"] <= exit_time)
        rate_sum = float(df.loc[mask, "last_funding_rate"].sum())

        # Long: pay when rate > 0 → cost = +rate_sum
        # Short: pay when rate < 0 → cost = -rate_sum
        return rate_sum if position_type == "Long" else -rate_sum

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load(self, symbol: str) -> pd.DataFrame | None:
        """Load and cache funding rate CSV for *symbol*."""
        if symbol in self._cache:
            return self._cache[symbol]

        path = self._root / self._FUNDING_DIR / f"{symbol.lower()}.csv"

        if not path.exists():
            logger.warning(
                "Funding rate data not found for %s (%s). "
                "PnL will not include funding costs. "
                "Run `qr-collect-funding --symbol %s` to collect data.",
                symbol, path, symbol,
            )
            self._cache[symbol] = None
            return None

        df = pd.read_csv(path)
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        df = df.sort_values("datetime").reset_index(drop=True)

        logger.debug("Loaded funding rate data for %s: %d rows.", symbol, len(df))
        self._cache[symbol] = df

        return df
