"""
strategies.dl_regime.cli.train
================================
CLI entry point for DL regime model walk-forward training.

Registered as ``qr-dl-train`` in ``[project.scripts]``.

Trains LSTM / TCN / Transformer via rolling-window WFA and saves
per-window checkpoints to ``checkpoints/<model>/<window>/model.ckpt``.
All experiments are tracked in MLflow (``mlruns/``).

Data is loaded from ``data/crypto/binance/futures/<symbol>_<interval>.csv``
via ``CryptoLoader``, then preprocessed using ``CryptoPreprocessor``.

Usage
-----
::

    qr-dl-train --model lstm --symbol BTCUSDT
    qr-dl-train --model tcn --symbol ETHUSDT --start 2020-01-01 --end 2025-12-31
    qr-dl-train --model transformer --symbol SOLUSDT --horizon 12 --threshold 0.003
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys
import tomllib

from backtesting.assets.crypto import CryptoLoader, CryptoPreprocessor
from backtesting.core.config_loader import BacktestConfigLoader

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qr-dl-train",
        description="DL regime model walk-forward trainer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--model", type=str, required=True,
        choices=["lstm", "tcn", "transformer"],
        help="Model architecture to train.",
    )
    parser.add_argument(
        "--symbol", type=str, required=True,
        help="Trading pair symbol, e.g. BTCUSDT.",
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="WFA start date override (ISO-8601).",
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="WFA end date override (ISO-8601).",
    )
    parser.add_argument(
        "--horizon", type=int, default=None,
        help="Label horizon override (bars ahead for future return).",
    )
    parser.add_argument(
        "--threshold", type=float, default=None,
        help="Label threshold override (min absolute return for positive label).",
    )
    parser.add_argument(
        "--dl-config-dir", type=str,
        default=None,
        help="Path to dl-regime configs/ directory containing model yaml files. "
             "Defaults to dl-regime package configs.",
    )
    parser.add_argument(
        "--config-dir", type=str,
        default=str(_REPO_ROOT / "src" / "configs"),
        help="Path to quant-research configs/ directory.",
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    return parser


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, level),
    )


def _load_dl_config(model: str, dl_config_dir: str | None) -> dict:
    """Merge dl-regime default_config.toml with model-specific yaml."""
    try:
        import yaml
        from dl_regime import get_default_config_path
    except ImportError as exc:
        raise ImportError(
            "dl-regime is required. "
            "Install: uv add 'dl-regime @ git+https://github.com/rpycgo/dl-regime.git'"
        ) from exc

    with open(get_default_config_path(), "rb") as fh:
        base = tomllib.load(fh)

    # Resolve yaml config path
    if dl_config_dir:
        yaml_path = pathlib.Path(dl_config_dir) / f"{model}.yaml"
    else:
        # Fall back to dl-regime package directory
        import dl_regime
        yaml_path = pathlib.Path(dl_regime.__file__).parent.parent.parent / "configs" / f"{model}.yaml"

    if yaml_path.exists():
        with open(yaml_path) as fh:
            override = yaml.safe_load(fh)
        for k, v in override.items():
            if isinstance(v, dict) and k in base:
                base[k].update(v)
            else:
                base[k] = v

    return base


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    _setup_logging(args.log_level)
    log = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    qr_loader = BacktestConfigLoader(config_dir=args.config_dir)
    ds_cfg = qr_loader.get_data_settings()
    collection_cfg = ds_cfg["binance_collection"]
    preprocessor_cfg = ds_cfg["event_detection"]

    dl_cfg = _load_dl_config(args.model, args.dl_config_dir)

    # WFA date overrides
    wfa = dl_cfg.setdefault("walk_forward_settings", {})
    if args.start:
        wfa["start_date"] = args.start
    if args.end:
        wfa["end_date"] = args.end

    # Label overrides
    label_cfg = dl_cfg.setdefault("label", {})
    if args.horizon is not None:
        label_cfg["horizon"] = args.horizon
    if args.threshold is not None:
        label_cfg["threshold"] = args.threshold

    log.info(
        "Training: model=%s | symbol=%s | %s -> %s | horizon=%s | threshold=%s",
        args.model,
        args.symbol,
        wfa.get("start_date"),
        wfa.get("end_date"),
        label_cfg.get("horizon", 12),
        label_cfg.get("threshold", 0.003),
    )

    # ------------------------------------------------------------------
    # Data loading and preprocessing
    # ------------------------------------------------------------------
    data_loader  = CryptoLoader(config=collection_cfg, project_root=_REPO_ROOT)
    preprocessor = CryptoPreprocessor(settings=preprocessor_cfg)

    log.info("Loading data for %s ...", args.symbol)
    raw = data_loader.load(
        symbol=args.symbol,
        start=preprocessor_cfg["analysis_start_date"],
        end=wfa.get("end_date", preprocessor_cfg["analysis_end_date"]),
    )

    if raw.index.tz is not None:
        raw.index = raw.index.tz_convert("UTC").tz_localize(None)

    log.info("Preprocessing ...")
    full_data = preprocessor.run_full_pipeline(raw)
    full_data = preprocessor.calculate_directional_indicator(full_data)
    full_data = full_data.dropna(subset=dl_cfg["model"]["input_features"])
    log.info("Preprocessor applied: %d rows.", len(full_data))

    # ------------------------------------------------------------------
    # WFA Training
    # ------------------------------------------------------------------
    try:
        from dl_regime.trainer.wfa_trainer import WfaTrainer
    except ImportError as exc:
        raise ImportError(
            "dl-regime is required. "
            "Install: uv add 'dl-regime @ git+https://github.com/rpycgo/dl-regime.git'"
        ) from exc

    trainer = WfaTrainer(model_name=args.model, config=dl_cfg)
    predictions, _ = trainer.run(full_data)

    if predictions.empty:
        log.warning("No predictions generated.")
        return 1

    # ------------------------------------------------------------------
    # Save predictions
    # ------------------------------------------------------------------
    out_dir = _REPO_ROOT / "results" / "dl_predictions"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{args.model}_{args.symbol.lower()}_predictions.csv"
    predictions.to_csv(out_path)
    log.info("Predictions saved -> %s (%d rows)", out_path, len(predictions))

    return 0


if __name__ == "__main__":
    sys.exit(main())
