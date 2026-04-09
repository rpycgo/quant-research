"""
data.funding_rate.loader
=========================
Funding rate data loader for Binance perpetual futures.

Downloads monthly ZIP archives from Binance Vision, extracts and merges
them into a single CSV per symbol, then removes all intermediate files.

Output schema
-------------
* ``datetime``              — UTC timestamp (parsed from calc_time)
* ``calc_time``             — Unix timestamp in milliseconds
* ``funding_interval_hours``— Funding interval (typically 8)
* ``last_funding_rate``     — Funding rate at settlement time

Output path: ``data/crypto/binance/funding_rate/<symbol_lower>.csv``
"""
from __future__ import annotations

import logging
import pathlib
import shutil
import urllib.error
import urllib.request
import zipfile
from datetime import datetime
from typing import Any

import pandas as pd
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

_BASE_URL      = "https://data.binance.vision/data/futures/um/monthly/fundingRate"
_OUTPUT_COLS   = ["datetime", "calc_time", "funding_interval_hours", "last_funding_rate"]


class FundingRateLoader:
    """Downloads, merges and stores Binance funding rate data per symbol.

    Args:
        config:       Parsed ``[binance_collection]`` section from
                      ``data_settings.toml``.
        project_root: Absolute path to the repository root.

    Example::

        loader = FundingRateLoader(config=collection_cfg, project_root=root)
        loader.collect("BTCUSDT", start=datetime(2020, 1, 1), end=datetime(2025, 12, 31))
    """
    def __init__(
        self,
        config: dict[str, Any],
        project_root: pathlib.Path,
        ) -> None:
        self._supported: list[str] = config.get("supported_symbols", [])
        self._output_dir: pathlib.Path = (
            project_root / "data" / "crypto" / "binance" / "funding_rate"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        ) -> pathlib.Path | None:
        """Download, merge and save funding rate data for one symbol.

        Args:
            symbol: Trading pair, e.g. ``"BTCUSDT"``.
            start:  Collection start (year/month only — day is ignored).
            end:    Collection end   (year/month only — day is ignored).

        Returns:
            Path of the saved CSV, or ``None`` if no data was collected.
        """
        self._validate_symbol(symbol)
        self._output_dir.mkdir(parents=True, exist_ok=True)

        tmp_dir = self._output_dir / f"_tmp_{symbol.lower()}"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        try:
            urls = self._generate_urls(symbol, start, end)
            logger.info(
                "Collecting funding rate: %s | %s → %s | %d monthly files",
                symbol, start.strftime("%Y-%m"), end.strftime("%Y-%m"), len(urls),
            )

            zip_paths  = self._download_all(urls, tmp_dir)
            csv_paths  = self._extract_all(zip_paths, tmp_dir)
            merged     = self._merge(csv_paths)

            if merged is None or merged.empty:
                logger.warning("%s: merge produced no data.", symbol)
                return None

            output_path = self._output_dir / f"{symbol.lower()}.csv"
            merged.to_csv(output_path, index=False)
            logger.info("%s: %d rows → %s", symbol, len(merged), output_path)

            return output_path

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            logger.debug("%s: cleaned up temporary directory.", symbol)

    def load(self, symbol: str) -> pd.DataFrame | None:
        """Load the merged funding rate CSV for *symbol*.

        Args:
            symbol: Trading pair, e.g. ``"BTCUSDT"``.

        Returns:
            DataFrame with UTC datetime index, or ``None`` if file not found.
        """
        path = self._output_dir / f"{symbol.lower()}.csv"
        if not path.exists():
            logger.warning(
                "Funding rate file not found for %s. "
                "Run `qr-collect-funding --symbol %s` first.",
                symbol, symbol,
            )
            return None

        df = pd.read_csv(path)
        df["datetime"] = pd.to_datetime(df["datetime"])

        return df.sort_values("datetime").reset_index(drop=True)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_symbol(self, symbol: str) -> None:
        if self._supported and symbol not in self._supported:
            raise ValueError(
                f"Symbol '{symbol}' is not supported. "
                f"Add it to data_settings.toml → supported_symbols. "
                f"Currently supported: {self._supported}"
            )

    @staticmethod
    def _generate_urls(symbol: str, start: datetime, end: datetime) -> list[str]:
        """Generate monthly Binance Vision funding rate download URLs."""
        urls: list[str] = []
        current = start.replace(day=1)
        end_    = end.replace(day=1)

        while current <= end_:
            filename = f"{symbol}-fundingRate-{current:%Y-%m}.zip"
            urls.append(f"{_BASE_URL}/{symbol}/{filename}")
            current += relativedelta(months=1)

        return urls

    @staticmethod
    def _download_all(
        urls: list[str],
        dest_dir: pathlib.Path,
        ) -> list[pathlib.Path]:
        """Download all ZIP files, skipping 404s silently."""
        downloaded: list[pathlib.Path] = []

        for url in urls:
            filename = url.split("/")[-1]
            dest     = dest_dir / filename

            try:
                urllib.request.urlretrieve(url, dest)
                downloaded.append(dest)
                logger.debug("Downloaded: %s", filename)
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    logger.debug("Not found (404) — skipping: %s", filename)
                else:
                    logger.warning("HTTP %s for %s", exc.code, url)
            except urllib.error.URLError as exc:
                logger.warning("URL error for %s: %s", url, exc.reason)

        logger.info("Downloaded %d / %d files.", len(downloaded), len(urls))

        return downloaded

    @staticmethod
    def _extract_all(
        zip_paths: list[pathlib.Path],
        dest_dir: pathlib.Path,
        ) -> list[pathlib.Path]:
        """Extract all ZIP files and return list of extracted CSV paths."""
        csv_paths: list[pathlib.Path] = []

        for zip_path in zip_paths:
            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    for name in zf.namelist():
                        if name.endswith(".csv"):
                            zf.extract(name, dest_dir)
                            csv_paths.append(dest_dir / name)
            except zipfile.BadZipFile as exc:
                logger.warning("Bad ZIP %s: %s", zip_path.name, exc)

        return csv_paths

    @staticmethod
    def _merge(csv_paths: list[pathlib.Path]) -> pd.DataFrame | None:
        """Merge multiple funding rate CSVs into one sorted DataFrame."""
        frames: list[pd.DataFrame] = []

        for path in csv_paths:
            try:
                df = pd.read_csv(path)
                frames.append(df)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to read %s: %s", path.name, exc)

        if not frames:
            return None

        merged = pd.concat(frames, ignore_index=True)
        merged["calc_time"] = pd.to_numeric(merged["calc_time"], errors="coerce")
        merged["datetime"]  = pd.to_datetime(merged["calc_time"], unit="ms")

        merged = (
            merged
            .sort_values("datetime")
            .drop_duplicates(subset=["calc_time"])
            .reset_index(drop=True)
        )

        available = [column for column in _OUTPUT_COLS if column in merged.columns]

        return merged[available]
