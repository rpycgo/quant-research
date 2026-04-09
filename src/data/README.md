# data

Historical market data collection pipeline for the quant-research platform.

Provides CLI tools for collecting OHLCV candles and funding rate data from
Binance Futures, with automatic fallback to local CSV for offline usage.

---

## Structure

```
data/
├── cli/
│   ├── data_collect.py           qr-data-collect
│   └── funding_rate_collect.py   qr-collect-funding-rate
└── funding_rate/
    ├── __init__.py
    └── loader.py                 FundingRateLoader
```

---

## OHLCV data

### qr-data-collect

Fetches historical OHLCV candles from Binance Futures via `ccxt`.
Falls back to local CSV if exchange is unavailable.

```bash
qr-data-collect --symbol BTCUSDT --start 2020-01-01 --end 2026-01-31
qr-data-collect --symbol ETHUSDT --interval 1m --start 2020-01-01
```

**Output:** `data/crypto/binance/futures/<symbol_lower>_<interval>.csv`

**Schema:**

| Column | Type | Description |
|---|---|---|
| `Datetime` | DatetimeIndex (UTC) | Bar open time |
| `Open` | float64 | Open price |
| `High` | float64 | High price |
| `Low` | float64 | Low price |
| `Close` | float64 | Close price |
| `Volume` | float64 | Volume |

---

## Funding rate data

### qr-collect-funding-rate

Downloads monthly funding rate ZIP archives from Binance Vision, merges
them into a single CSV per symbol, and removes all intermediate files.

```bash
# Collect all supported symbols (from data_settings.toml)
qr-collect-funding-rate

# Single symbol
qr-collect-funding-rate --symbol BTCUSDT

# Custom date range
qr-collect-funding-rate --symbol ETHUSDT --start 2021-01-01 --end 2025-12-31
```

**Output:** `data/crypto/binance/funding_rate/<symbol_lower>.csv`

**Schema:**

| Column | Type | Description |
|---|---|---|
| `datetime` | datetime64 (UTC) | Settlement timestamp |
| `calc_time` | int64 | Unix timestamp (ms) |
| `funding_interval_hours` | int64 | Funding interval (typically 8) |
| `last_funding_rate` | float64 | Funding rate at settlement |

**Note:** Binance settles funding every 8 hours at 00:00, 08:00, 16:00 UTC.
A position held at settlement time pays (Long) or receives (Short) the rate.

---

## Data directory structure

```
data/
└── crypto/
    └── binance/
        ├── futures/
        │   ├── btcusdt_5m.csv
        │   ├── ethusdt_5m.csv
        │   ├── solusdt_5m.csv
        │   └── xrpusdt_5m.csv
        └── funding_rate/
            ├── btcusdt.csv
            ├── ethusdt.csv
            ├── solusdt.csv
            └── xrpusdt.csv
```

---

## Configuration

Both CLIs read from `configs/data_settings.toml`:

```toml
[binance_collection]
interval          = "5m"
output_directory  = "data/crypto/binance/futures"
supported_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]

[event_detection]
analysis_start_date = "2020-01-01"
analysis_end_date   = "2025-12-31"
```

`qr-collect-funding-rate` uses `supported_symbols` for the default symbol list
and `analysis_start_date` / `analysis_end_date` for the default date range.

---

## FundingRateLoader

Low-level loader used internally by `qr-collect-funding-rate` and
`FundingRateManager` in the backtesting engine.

```python
from data.funding_rate.loader import FundingRateLoader

loader = FundingRateLoader(config=collection_cfg, project_root=root)

# Collect and save
loader.collect("BTCUSDT", start=datetime(2020, 1, 1), end=datetime(2025, 12, 31))

# Load saved data
df = loader.load("BTCUSDT")
```