"""
strategies.mdrs_sde.cli.event_detect
======================================
CLI entry point for MDRS-SDE breakout event detection.

Registered as ``qr-mdrs-detect`` in ``[project.scripts]``.

Detects breakout events from preprocessed OHLCV data and saves to
``data/events/<symbol_lower>_<interval>.toml``.

Range expansion detection
--------------------------
Each events file stores a ``[meta]`` section with ``detected_start``
and ``detected_end``. If the requested range exceeds the previously
detected range, detection is re-run automatically.

Usage
-----
::

    qr-mdrs-detect --symbol BTCUSDT --start 2023-10-01 --end 2025-12-31
    qr-mdrs-detect --symbol BTCUSDT --start 2023-01-01 --end 2026-06-30 --force
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys
import tomllib
from datetime import datetime
from typing import Any

import tomli_w

from backtesting.core.config_loader import BacktestConfigLoader
from backtesting.assets.crypto import CryptoLoader, CryptoPreprocessor

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qr-mdrs-detect",
        description="MDRS-SDE breakout event detector",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbol", type=str, required=True,
                        help="Trading pair symbol, e.g. BTCUSDT.")
    parser.add_argument("--start", type=str, required=True,
                        help="Detection start date (ISO-8601).")
    parser.add_argument("--end", type=str, required=True,
                        help="Detection end date (ISO-8601).")
    parser.add_argument("--interval", type=str, default=None,
                        help="Candlestick interval. Overrides config.")
    parser.add_argument("--force", action="store_true",
                        help="Force re-detection even if range is already covered.")
    parser.add_argument("--config-dir", type=str,
                        default=str(_REPO_ROOT / "src" / "configs"),
                        help="Path to configs/ directory.")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    return parser


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, level),
    )


def _get_events_path(
    events_dir: pathlib.Path,
    symbol: str,
    interval: str,
    ) -> pathlib.Path:
    return events_dir / f"{symbol.lower()}_{interval}.toml"


def _check_range_covered(
    events_path: pathlib.Path,
    requested_start: str,
    requested_end: str,
    ) -> bool:
    """Return True if the existing file already covers the requested range."""
    if not events_path.exists():
        return False

    with open(events_path, "rb") as fh:
        content = tomllib.load(fh)

    meta = content.get("meta", {})
    detected_start = meta.get("detected_start")
    detected_end = meta.get("detected_end")

    if not detected_start or not detected_end:
        return False

    return detected_start <= requested_start and detected_end >= requested_end


def _save_events(
    events: list[dict[str, Any]],
    events_path: pathlib.Path,
    detected_start: str,
    detected_end: str,
    ) -> None:
    events_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {
            "detected_start": detected_start,
            "detected_end": detected_end,
            "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "total_events": len(events),
        },
        "detected_events": events,
    }
    with open(events_path, "wb") as fh:
        tomli_w.dump(payload, fh)


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    _setup_logging(args.log_level)
    log = logging.getLogger(__name__)

    config_loader = BacktestConfigLoader(config_dir=args.config_dir)
    ds_cfg = config_loader.get_data_settings()
    collection_cfg = ds_cfg["binance_collection"]
    detection_cfg = ds_cfg["event_detection"]

    interval = args.interval or collection_cfg["interval"]
    collection_cfg["interval"] = interval

    events_dir = _REPO_ROOT / detection_cfg.get("events_directory", "data/events")
    events_path = _get_events_path(events_dir, args.symbol, interval)

    # ------------------------------------------------------------------
    # Range expansion check
    # ------------------------------------------------------------------
    if not args.force and _check_range_covered(events_path, args.start, args.end):
        log.info(
            "Events file already covers %s to %s. Skipping. "
            "Use --force to re-run.",
            args.start, args.end,
        )
        return 0

    log.info(
        "Detecting events for %s %s from %s to %s ...",
        args.symbol, interval, args.start, args.end,
    )

    # ------------------------------------------------------------------
    # Load and preprocess
    # ------------------------------------------------------------------
    loader = CryptoLoader(config=collection_cfg, project_root=_REPO_ROOT)

    try:
        raw = loader.load(symbol=args.symbol, start=args.start, end=args.end)
    except FileNotFoundError as exc:
        log.error(
            "Data not found: %s\n"
            "Run `qr-collect --symbol %s --start %s --end %s` first.",
            exc, args.symbol, args.start, args.end,
        )
        return 1

    if raw.index.tz is not None:
        raw.index = raw.index.tz_convert("UTC").tz_localize(None)

    preprocessor = CryptoPreprocessor(settings=detection_cfg)
    data = preprocessor.run_full_pipeline(raw)

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------
    try:
        from mdrs_sde.data.event_detector import EventDetector
    except ImportError as exc:
        raise ImportError(
            "mdrs-sde is required. "
            "Install: uv add 'mdrs-sde @ git+https://github.com/rpycgo-research/mdrs-sde.git'"
        ) from exc

    detector = EventDetector(settings=detection_cfg)
    events = detector.detect(data, start_date=args.start, end_date=args.end)

    # ------------------------------------------------------------------
    # Save with [meta]
    # ------------------------------------------------------------------
    _save_events(events, events_path, args.start, args.end)
    log.info("%d events detected -> %s", len(events), events_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
