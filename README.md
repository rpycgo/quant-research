# quant-research

> A comprehensive platform for quantitative trading research and strategy development

## Overview

**quant-research** is the single execution platform for quantitative trading research — from real-time market data collection to systematic backtesting and performance analysis.

Model libraries ([mdrs-sde](https://github.com/rpycgo-research/mdrs-sde), [dl-regime](https://github.com/rpycgo/dl-regime)) are maintained as separate pure libraries. All execution entry points live here.

## Modules

| Module | Description | Status |
|---|---|---|
| [backtesting](src/backtesting/README.md) | Walk-forward backtesting framework | ✅ Available |
| [collector](src/collector/README.md) | Real-time market data collection via Binance WebSocket | ✅ Available |
| [ingestor](src/ingestor/README.md) | Kafka consumer → TimescaleDB ingestion pipeline | ✅ Available |
| dashboards | Grafana monitoring and visualization | ✅ Available |

## Project Structure

```
quant-research/
├── src/
│   ├── backtesting/              Walk-forward backtesting framework
│   │   ├── cli/                  Installable CLI entry points
│   │   ├── core/                 Base classes and config loader
│   │   ├── engines/              Backtest and walk-forward engines
│   │   ├── models/               Model adapters and registry
│   │   │   └── adapters/         mdrs_sde, dl_regime, garch
│   │   ├── assets/               Asset-specific data loaders
│   │   └── visualization/        Performance plotters
│   ├── collector/                Binance WebSocket → Kafka producer
│   ├── ingestor/                 Kafka consumer → TimescaleDB
│   ├── configs/                  Backtesting and data configuration
│   │   └── model_parameters/     Per-model override configs
│   └── utils/                    Shared config loader
├── events/                       Detected breakout event files (DVC)
├── dashboards/                   Grafana dashboard definitions
├── run_collector.py              Collector entry-point
├── run_ingestor.py               Ingestor entry-point
├── run_backfill.py               Gap repair entry-point
├── docker-compose.yml            Infrastructure services
└── pyproject.toml
```

## Getting Started

### Prerequisites

- Python 3.12+
- uv
- Docker & Docker Compose

### Installation

```bash
git clone https://github.com/rpycgo/quant-research.git
cd quant-research
uv sync
```

### Infrastructure (TimescaleDB · Kafka · Grafana)

```bash
cp .env.example .env   # set DB_PASSWORD, DB_NAME, DB_USER
docker-compose up -d
```

## Quick Start

### Real-time data pipeline

```bash
# Terminal 1 — collector (Binance WebSocket → Kafka)
python run_collector.py

# Terminal 2 — ingestor (Kafka → TimescaleDB)
python run_ingestor.py

# Gap repair (run separately as needed)
python run_backfill.py
```

### Backtesting

```bash
# List all registered models
qr-backtest --list-models

# Run backtest
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT

# With date override
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT \
    --start 2024-01-01 --end 2026-01-31

# Other registered models
qr-backtest --model garch_btc --symbol BTCUSDT
qr-backtest --model dl_regime_lstm_btc --symbol BTCUSDT
qr-backtest --model dl_regime_tcn_btc --symbol BTCUSDT
qr-backtest --model dl_regime_transformer_btc --symbol BTCUSDT
```

## Registered Models

| Model key | Adapter | Source |
|---|---|---|
| `mdrs_sde_btc` | `MdrsSdeCryptoAdapter` | [mdrs-sde](https://github.com/rpycgo-research/mdrs-sde) |
| `mdrs_sde_eth` | `MdrsSdeCryptoAdapter` | [mdrs-sde](https://github.com/rpycgo-research/mdrs-sde) |
| `garch_btc` | `GarchCryptoAdapter` | arch package |
| `garch_eth` | `GarchCryptoAdapter` | arch package |
| `dl_regime_lstm_btc` | `DlRegimeCryptoAdapter` | [dl-regime](https://github.com/rpycgo/dl-regime) |
| `dl_regime_tcn_btc` | `DlRegimeCryptoAdapter` | [dl-regime](https://github.com/rpycgo/dl-regime) |
| `dl_regime_transformer_btc` | `DlRegimeCryptoAdapter` | [dl-regime](https://github.com/rpycgo/dl-regime) |

## Adding a New Model

1. Implement an adapter in `src/backtesting/models/adapters/<name>.py` inheriting `BaseModel`
2. Register it in `src/backtesting/models/registry.py`
3. Add `configs/model_parameters/<model_key>.toml` for local overrides
4. If the model ships a `default_config.toml`, add it to `_MODEL_PACKAGE_MAP` in `config_loader.py`

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12+ |
| Package management | uv |
| Message broker | Apache Kafka (KRaft mode) |
| Time-series DB | TimescaleDB (PostgreSQL 18) |
| Monitoring | Grafana |
| Containerization | Docker & Docker Compose |

## Related Repositories

- [mdrs-sde](https://github.com/rpycgo-research/mdrs-sde) — MDRS-SDE model library
- [dl-regime](https://github.com/rpycgo/dl-regime) — Deep learning regime detection library
- [mdrs-sde-theory](https://github.com/rpycgo/mdrs-sde-theory) — Theoretical foundations for MDRS-SDE