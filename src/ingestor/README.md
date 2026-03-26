# ingestor

Kafka consumer → TimescaleDB ingestion pipeline.

Consumes aggregated trade ticks from a Kafka topic and persists them to TimescaleDB as a hypertable. Automatically creates continuous aggregate views for OHLCV candle data at multiple timeframes on startup.

---

## Architecture

```
Kafka topic: crypto.futures.ticks
        ↓
  DataIngestionService (AIOKafkaConsumer)
  └── batch accumulation (default: 50 messages)
        ↓
  TimescaleIngestor
  ├── future_ticks hypertable    — raw tick data
  └── continuous aggregates      — OHLCV candle views
        ↓
  TimescaleDB
```

---

## Components

### `database_handler.py` — `TimescaleIngestor`

**On startup**, automatically initialises:

- `future_ticks` hypertable — raw tick storage (`time`, `symbol`, `price`, `volume`)
- Continuous aggregate views defined in `candle_views` config

**Candle views** (hierarchical aggregation):

| View | Interval | Source |
|---|---|---|
| `candle_1min` | 1 minute | `future_ticks` |
| `candle_5min` | 5 minutes | `candle_1min` |
| `candle_15min` | 15 minutes | `candle_1min` |
| `candle_1hour` | 1 hour | `candle_1min` |
| `candle_4hour` | 4 hours | `candle_1hour` |
| `candle_1day` | 1 day | `candle_1hour` |

---

## Configuration

`src/configs/pipeline_settings.yaml`

```yaml
database:
  host: 
  port: 
  dbname: 
  user: 
  password:

kafka:
  bootstrap_servers: "localhost:9092"
  topic: "crypto.futures.ticks"
  batch_size: 50

candle_views:
  - { name: "candle_1min",  interval: "1 minute",   source: "future_ticks" }
  - { name: "candle_5min",  interval: "5 minutes",  source: "candle_1min"  }
  - { name: "candle_15min", interval: "15 minutes", source: "candle_1min"  }
  - { name: "candle_1hour", interval: "1 hour",     source: "candle_1min"  }
  - { name: "candle_4hour", interval: "4 hours",    source: "candle_1hour" }
  - { name: "candle_1day",  interval: "1 day",      source: "candle_1hour" }
```

Environment variables (`.env`)

```
DB_HOST=
DB_PORT=
DB_NAME=
DB_USER=
DB_PASSWORD=
```

---

## Usage

```bash
python run_ingestor.py
```

The service starts consuming from the configured Kafka topic and flushes to TimescaleDB in batches.

---

## Monitoring

Grafana is available at `http://localhost:3000` (default credentials: `admin / admin`).

Connect a TimescaleDB datasource and use the continuous aggregate views (`candle_1min`, `candle_5min`, ...) as data sources for dashboards.
