# quant-research

> A comprehensive platform for quantitative trading research and strategy development

## Overview

**quant-research** is a monorepo covering the full quantitative trading lifecycle — from real-time market data collection to systematic backtesting and performance analysis.

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
│   ├── backtesting/         Walk-forward backtesting framework
│   ├── collector/           Binance WebSocket → Kafka producer
│   ├── ingestor/            Kafka consumer → TimescaleDB
│   └── utils/               Shared config loader
├── configs/                 Backtesting configuration files
├── dashboards/              Grafana dashboard definitions
├── run_backtest.py          Backtesting entry-point
├── run_collector.py         Collector entry-point
├── run_ingestor.py          Ingestor entry-point
├── docker-compose.yml       Infrastructure services
└── pyproject.toml
```

## Getting Started

### Prerequisites

- Python 3.11+
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
cp .env.example .env   # DB_PASSWORD, DB_NAME, DB_USER 설정
docker-compose up -d
```

## Quick Start

### Real-time data pipeline

```bash
# Terminal 1 — collector (Binance WebSocket → Kafka)
python run_collector.py

# Terminal 2 — ingestor (Kafka → TimescaleDB)
python run_ingestor.py
```

### Backtesting

```bash
python run_backtest.py --model mdrs_sde_btc --symbol BTCUSDT
python run_backtest.py --list-models
```

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Package management | uv |
| Message broker | Apache Kafka (KRaft mode) |
| Time-series DB | TimescaleDB (PostgreSQL 18) |
| Monitoring | Grafana |
| Containerization | Docker & Docker Compose |

## Related Repositories

- [mdrs-sde](https://github.com/rpycgo-research/mdrs-sde) — PAITS for Bitcoin perpetual futures (MDRS-SDE model)
- [mdrs-sde-theory](https://github.com/rpycgo/mdrs-sde-theory) — Theoretical foundations for MDRS-SDE
