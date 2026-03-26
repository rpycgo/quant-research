# collector

Real-time cryptocurrency market data collector.

Connects to Binance Futures WebSocket streams and publishes aggregated trade ticks to a Kafka topic. Includes automatic gap detection and recovery via the REST API.

---

## Architecture

```
Binance Futures WebSocket (aggTrade)
        ↓
  BinanceCollector
  ├── Gap detection   — tracks trade IDs per symbol
  └── Gap recovery    — REST API backfill for small gaps (<1000 trades)
        ↓
  AsyncKafkaProducer
        ↓
  Kafka topic: crypto.futures.ticks
```

---

## Components

### `binance_stream.py` — `BinanceCollector`

- Subscribes to `<symbol>@aggTrade` streams for configured symbols
- Detects missing trade IDs between consecutive messages
- Small gaps (≤ 1000 trades): auto-recovered via REST API in a non-blocking task
- Large gaps (> 1000 trades): logged for manual intervention or backfill worker

### `kafka_producer.py` — `AsyncKafkaProducer`

- Thin async wrapper around `AIOKafkaProducer`
- JSON serialisation with `send_and_wait` for delivery guarantee

### `backfill.py` — `BackfillService`

- Standalone gap-repair service (entry-point: `run_backfill.py` at project root)
- Queries TimescaleDB for time gaps in the last N hours
- Fetches missing trades via Binance REST API and ingests directly to DB
- Supports source IP binding for network interface control

---

## Configuration

`src/configs/pipeline_settings.yaml`

```yaml
target_symbols:
  - BTCUSDT
  - ETHUSDT

kafka:
  bootstrap_servers: "localhost:9092"
  topic: "crypto.futures.ticks"
  batch_size: 50
```

Environment variables (`.env`)

```
SOURCE_IP=  # optional — bind outgoing requests to a specific network interface
```

---

## Usage

```bash
# Real-time collection
python run_collector.py

# Manual gap repair
python run_backfill.py
```

---

## Data schema

Each message published to Kafka:

```json
{
  "symbol": "BTCUSDT",
  "price": 95000.0,
  "volume": 0.012,
  "event_time": 1769799311651
}
```
