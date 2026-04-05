# strategies

Model-specific CLI tools for the quant-research platform.

Each sub-module corresponds to a model family and provides CLI entry points
for model-specific operations (event detection, training, analysis) that
fall outside the generic `qr-backtest` pipeline.

---

## Structure

```
strategies/
├── mdrs_sde/
│   └── cli/
│       └── event_detect.py     qr-mdrs-event-detect
└── dl_regime/
    └── cli/
        └── train.py            qr-dl-train
```

---

## MDRS-SDE

### qr-mdrs-event-detect

Detects breakout events from preprocessed OHLCV data and saves results
to `data/events/<symbol>_<interval>.toml`.

Each events file stores a `[meta]` section with `detected_start` and
`detected_end`. Detection is automatically skipped if the requested
range is already covered.

```bash
# Basic detection
qr-mdrs-event-detect --symbol BTCUSDT --start 2020-01-01 --end 2025-12-31

# Force re-detection
qr-mdrs-event-detect --symbol BTCUSDT --start 2020-01-01 --end 2025-12-31 --force

# Custom interval
qr-mdrs-event-detect --symbol BTCUSDT --start 2020-01-01 --end 2025-12-31 --interval 1m
```

**Output:** `data/events/btcusdt_5m.toml`

**Prerequisites:** OHLCV data must be collected first via `qr-data-collect`.

---

## DL Regime

### qr-dl-train

Trains LSTM / TCN / Transformer regime detection models via rolling-window
WFA and saves per-window checkpoints to `checkpoints/<model>/<window>/model.ckpt`.

Uses `CryptoLoader` and `CryptoPreprocessor` from quant-research for data
loading and feature engineering, then delegates WFA training to the
`dl-regime` `WfaTrainer`.

All experiments are tracked in MLflow (`mlruns/`).

```bash
# Train LSTM on BTC
qr-dl-train --model lstm --symbol BTCUSDT

# Train TCN on ETH with date override
qr-dl-train --model tcn --symbol ETHUSDT --start 2020-01-01 --end 2025-12-31

# Train Transformer with label overrides
qr-dl-train --model transformer --symbol SOLUSDT --horizon 12 --threshold 0.003

# All assets
qr-dl-train --model lstm --symbol BTCUSDT
qr-dl-train --model lstm --symbol ETHUSDT
qr-dl-train --model lstm --symbol SOLUSDT
qr-dl-train --model lstm --symbol XRPUSDT
```

**Output:** `results/dl_predictions/<model>_<symbol>_predictions.csv`

**Checkpoints:** `checkpoints/<model>/<window>/model.ckpt`

**Prerequisites:** OHLCV data must be collected first via `qr-data-collect`.

### Arguments

| Argument | Description | Default |
|---|---|---|
| `--model` | Architecture: `lstm`, `tcn`, `transformer` | required |
| `--symbol` | Trading pair symbol | required |
| `--start` | WFA start date (ISO-8601) | from config |
| `--end` | WFA end date (ISO-8601) | from config |
| `--horizon` | Label horizon (bars ahead) | 12 |
| `--threshold` | Min absolute return for positive label | 0.003 |

---

## Workflow

The typical research workflow combining both CLIs:

```bash
# Step 1 — Collect data
qr-data-collect --symbol BTCUSDT --start 2020-01-01 --end 2026-01-31

# Step 2a — MDRS-SDE: detect events
qr-mdrs-event-detect --symbol BTCUSDT --start 2020-01-01 --end 2025-12-31

# Step 2b — DL: train models
qr-dl-train --model lstm        --symbol BTCUSDT
qr-dl-train --model tcn         --symbol BTCUSDT
qr-dl-train --model transformer --symbol BTCUSDT

# Step 3 — Backtest
qr-backtest --model mdrs_sde_btc       --symbol BTCUSDT
qr-backtest --model dl_regime_lstm_btc --symbol BTCUSDT
```