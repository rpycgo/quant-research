# quant-research

> A comprehensive platform for quantitative trading research and strategy development

## Overview

**quant-research** is the single execution platform for quantitative trading research — from real-time market data collection to systematic backtesting and performance analysis.

Model libraries ([mdrs-sde](https://github.com/rpycgo-research/mdrs-sde), [dl-regime](https://github.com/rpycgo/dl-regime)) are maintained as separate pure libraries. All execution entry points live here.

## Modules

| Module | Description | Status |
|---|---|---|
| [backtesting](src/backtesting/README.md) | Walk-forward backtesting framework | ✅ Available |
| [data](src/data/README.md) | Historical OHLCV and funding rate data collection | ✅ Available |
| [strategies](src/strategies/README.md) | Model-specific CLI tools | ✅ Available |
| [collector](src/collector/README.md) | Real-time market data collection via Binance WebSocket | ✅ Available |
| [ingestor](src/ingestor/README.md) | Kafka consumer → TimescaleDB ingestion pipeline | ✅ Available |
| dashboards | Grafana monitoring and visualization | ✅ Available |

## Project Structure

```
quant-research/
├── src/
│   ├── backtesting/              Walk-forward backtesting framework
│   │   ├── benchmarks/           BuyAndHoldBenchmark, StatisticalValidator
│   │   ├── cli/                  qr-backtest, qr-buy-and-hold, qr-validate
│   │   ├── core/                 Base classes and config loader
│   │   ├── engines/              GenericBacktestEngine, WalkForwardRunner
│   │   ├── models/
│   │   │   ├── adapters/         mdrs_sde, garch, simple_breakout, ma_crossover,
│   │   │   │                     rsi, hmm_regime, dl_regime
│   │   │   └── registry.py       ModelRegistry with ModelEntry
│   │   └── assets/crypto/        CryptoLoader, CryptoPreprocessor
│   ├── data/
│   │   ├── cli/                  qr-data-collect, qr-collect-funding
│   │   └── funding_rate/         FundingRateLoader
│   ├── strategies/
│   │   ├── mdrs_sde/cli/         qr-mdrs-event-detect
│   │   └── dl_regime/cli/        qr-dl-train
│   ├── collector/                Binance WebSocket → Kafka producer
│   ├── ingestor/                 Kafka consumer → TimescaleDB
│   ├── configs/
│   │   ├── backtest_settings.toml
│   │   ├── data_settings.toml
│   │   └── model_parameters/     Per-model override configs
│   └── utils/                    Shared config loader
├── data/
│   ├── crypto/binance/futures/   OHLCV CSV files
│   ├── crypto/binance/funding_rate/  Funding rate CSV files
│   └── events/                   Detected breakout event files (DVC)
├── results/                      Backtest trade results and params
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
- GPU recommended for DL model training (8GB VRAM minimum)

### Installation

```bash
git clone https://github.com/rpycgo-research/quant-research.git
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

# Gap repair
python run_backfill.py
```

### Research pipeline

```bash
# 1. Collect historical OHLCV
qr-data-collect --symbol BTCUSDT --start 2020-01-01 --end 2026-01-31

# 2. Collect funding rate data
qr-collect-funding-rate --symbol BTCUSDT

# 3. Detect breakout events (MDRS-SDE only)
qr-mdrs-event-detect --symbol BTCUSDT --start 2020-01-01 --end 2025-12-31

# 4. Train DL models (DL benchmarks only)
qr-dl-train --model lstm --symbol BTCUSDT

# 5. Run backtest (funding costs automatically applied)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT

# 6. Statistical validation
qr-validate --result results/mdrs_sde_btc_btcusdt_<timestamp>_trades.csv
```

## CLI Reference

| Command | Description |
|---|---|
| `qr-data-collect` | Fetch historical OHLCV from Binance Futures |
| `qr-collect-funding-rate` | Collect funding rate data from Binance Vision |
| `qr-mdrs-event-detect` | Detect breakout events for MDRS-SDE training |
| `qr-dl-train` | Train DL regime models (LSTM / TCN / Transformer) |
| `qr-backtest` | Run walk-forward backtest for any registered model |
| `qr-buy-and-hold` | Compute passive buy-and-hold benchmark metrics |
| `qr-validate` | Bootstrap CI, Permutation test, Subperiod analysis |

## Registered Models

### Proposed model

| Model key | Asset | Description |
|---|---|---|
| `mdrs_sde_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | MDRS-SDE with MCMC, SNR scaling, EMA sigma |

### Benchmark models

| Model key | Asset | Description |
|---|---|---|
| `garch_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | GARCH(1,1) volatility-based regime |
| `simple_breakout_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | 288-period rolling high/low breakout |
| `ma_crossover_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | EMA(12)/EMA(26) crossover |
| `rsi_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | RSI(14) mean-reversion |
| `hmm_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | 2-state Gaussian HMM |
| `dl_regime_lstm_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | LSTM regime detection |
| `dl_regime_tcn_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | TCN regime detection |
| `dl_regime_transformer_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | Transformer regime detection |

## Adding a New Model

1. Implement an adapter in `src/backtesting/models/adapters/<name>.py` inheriting `BaseModel`
2. Add a `ModelEntry` to `src/backtesting/models/registry.py` with appropriate metadata flags
3. Add `configs/model_parameters/<model_key>.toml` for local overrides
4. If the model ships a `default_config.toml`, add it to `_MODEL_PACKAGE_MAP` in `config_loader.py`

See [backtesting README](src/backtesting/README.md) for detailed instructions.

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