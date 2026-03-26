# backtesting

Model-agnostic walk-forward backtesting framework for the quant-research platform.

Supports multiple asset classes and pluggable model architectures through a clean interface contract. External model packages register via `ModelRegistry` and expose a `BaseModel` implementation; the execution engine remains entirely unaware of model internals.

---

## Architecture

```
backtesting/
├── core/               Abstract interfaces (BaseModel, BaseLoader, BaseEngine)
│                       + 3-layer hierarchical config loader
├── assets/
│   └── crypto/         CryptoLoader (Binance / local CSV) + CryptoPreprocessor
├── engines/            GenericBacktestEngine · WalkForwardRunner · PerformanceAnalyzer
├── models/
│   ├── adapters/       HybridSdeCryptoAdapter · GarchCryptoAdapter
│   └── registry.py     ModelRegistry factory
└── visualization/      PerformancePlotter (equity curve, drawdown, comparison)
```

### Design principles

- **Model layer** owns all model-specific logic: signal generation, SNR scaling, MCMC estimation. The engine sees only `signal` (1 / -1 / 0) and `confidence` (0–1).
- **Engine layer** owns execution: TP / SL / trailing stop / ADX boost / time-out. It knows nothing about the model that generated the signals.
- **Config layer** uses a 3-layer merge: package default → local override → shared infra. Only the keys that differ from the package default need to appear in the override file.

---

## Supported models

| Model key | Asset | Package |
|---|---|---|
| `mdrs_sde_btc` | BTCUSDT | [mdrs-sde](https://github.com/rpycgo-research/mdrs-sde) |
| `mdrs_sde_eth` | ETHUSDT | [mdrs-sde](https://github.com/rpycgo-research/mdrs-sde) |
| `garch_btc` | BTCUSDT | built-in (`arch`) |
| `garch_eth` | ETHUSDT | built-in (`arch`) |

---

## Quick start

```bash
# Install dependencies
uv sync

# Run MDRS-SDE walk-forward backtest
python run_backtest.py --model mdrs_sde_btc --symbol BTCUSDT

# Run GARCH benchmark
python run_backtest.py --model garch_btc --symbol BTCUSDT

# Override WFA date range
python run_backtest.py --model mdrs_sde_btc --symbol BTCUSDT \
    --start 2024-01-01 --end 2026-01-31

# List all registered models
python run_backtest.py --list-models
```

---

## Configuration

```
configs/
├── backtest_settings.toml          Shared — execution costs, WFA schedule,
│                                   trading parameters, risk management
├── data_settings.toml              Shared — data collection, preprocessing
└── model_parameters/
    ├── mdrs_sde_btc.toml           Local override for MDRS-SDE on BTC
    └── garch_btc.toml              Local override for GARCH on BTC
```

### 3-layer config resolution

1. **Package default** — `default_config.toml` shipped inside the model package (e.g. `mdrs_sde/configs/default_config.toml`)
2. **Local override** — `configs/model_parameters/<model_key>.toml` — only keys that differ from the default
3. **Shared infra** — `configs/backtest_settings.toml` and `configs/data_settings.toml`

---

## Adding a new model

**Step 1** — Install the external model package.

```toml
# pyproject.toml
[tool.uv.sources]
my-model = { git = "https://github.com/rpycgo/my-model.git" }
```

**Step 2** — Create an adapter in `src/backtesting/models/adapters/my_model.py`.

```python
from backtesting.core.base_model import BaseModel

class MyModelAdapter(BaseModel):
    def fit(self, train_data: pd.DataFrame) -> dict:
        ...

    def predict(self, test_data: pd.DataFrame, params: dict) -> pd.DataFrame:
        # Must return DataFrame with signal (int) and confidence (float) columns
        ...
```

**Step 3** — Register in `src/backtesting/models/registry.py`.

```python
_REGISTRY: dict[str, type[BaseModel]] = {
    ...
    "my_model_btc": MyModelAdapter,
}
```

**Step 4** — Add the model package mapping in `src/backtesting/core/config_loader.py`.

```python
_MODEL_PACKAGE_MAP: dict[str, str] = {
    ...
    "my_model_btc": "my_model.configs",
}
```

**Step 5** — Add a local override config (optional).

```toml
# configs/model_parameters/my_model_btc.toml
# Only keys that differ from the package default
[some_section]
some_param = value
```

---

## Adding a new asset class

Implement `BaseLoader` and `BasePreprocessor` in `src/backtesting/assets/<asset_class>/`.

```python
from backtesting.core.base_loader import BaseLoader

class EquityLoader(BaseLoader):
    def load(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        ...
```

Add the symbol to `configs/data_settings.toml`.

```toml
[binance_collection]
supported_symbols = ["BTCUSDT", "ETHUSDT", "AAPL"]  # extend here
```

---

## Performance metrics

`PerformanceAnalyzer` computes the following from a completed-trades DataFrame.

| Metric | Description |
|---|---|
| Total Return (%) | Cumulative return over the WFA period |
| Annualised Return (%) | CAGR |
| Max Drawdown (%) | Peak-to-trough decline |
| Sharpe Ratio | Annualised, trade-frequency-adjusted |
| Sortino Ratio | Downside deviation adjusted |
| Win Rate (%) | Fraction of profitable trades |
| T-statistic | One-sample t-test against zero mean PnL |
| IC | Spearman rank IC across forward horizons |

---

## Related repositories

- [mdrs-sde](https://github.com/rpycgo-research/mdrs-sde) — MDRS-SDE model implementation
- [mdrs-sde-theory](https://github.com/rpycgo/mdrs-sde-theory) — Theoretical foundations