# backtesting

Model-agnostic walk-forward backtesting framework for the quant-research platform.

Supports multiple asset classes and pluggable model architectures through a clean interface contract. External model packages register via `ModelRegistry` and expose a `BaseModel` implementation; the execution engine remains entirely unaware of model internals.

---

## Architecture

```
backtesting/
├── benchmarks/         BuyAndHoldBenchmark, StatisticalValidator
├── cli/                qr-backtest, qr-buy-and-hold, qr-validate
├── core/               Abstract interfaces (BaseModel, BaseLoader, BaseEngine)
│                       + 3-layer hierarchical config loader
├── assets/
│   └── crypto/         CryptoLoader (Binance / local CSV) + CryptoPreprocessor
├── engines/            GenericBacktestEngine · WalkForwardRunner · PerformanceAnalyzer
├── models/
│   ├── adapters/       mdrs_sde, dl_regime, hmm_regime (share _regime_gates);
│   │                   garch, simple_breakout, ma_crossover, rsi (pure rules)
│   └── registry.py     ModelRegistry with ModelEntry
└── visualization/      PerformancePlotter (equity curve, drawdown, comparison)
```

### Design principles

- **Model layer** owns all model-specific logic: signal generation, MCMC estimation. The engine sees only `signal` (1 / -1 / 0) and `confidence` (0–1).
- **Engine layer** owns execution: TP / SL / trailing stop / time-out. It knows nothing about the model that generated the signals.
- **Config layer** uses a 3-layer merge: package default → local override → shared infra.
- **Regime-probability adapters** (`mdrs_sde`, `dl_regime`, `hmm_regime`) share a common downstream pipeline (sticky filter → ADX gate → QPB gate) via `_regime_gates.assemble_signal`, ensuring fair comparison under identical execution conditions.
- **Rule-based adapters** (`simple_breakout`, `ma_crossover`, `rsi`) are evaluated as self-contained technical trading rules and do not share the regime-probability pipeline, preserving their original literature semantics.
- **ModelEntry** metadata flags control pipeline branching per model:
  - `requires_event_tagging` — whether DatasetBuilder pipeline is needed (MDRS-SDE only)

---

## Supported models

### Proposed model

| Model key | Asset | Adapter | Notes |
|---|---|---|---|
| `mdrs_sde_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `MdrsSdeCryptoAdapter` | MCMC, event-zone training, sticky/ADX/QPB |

### Benchmark models

| Model key | Asset | Adapter | Pipeline | Notes |
|---|---|---|---|---|
| `garch_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `GarchCryptoAdapter` | pure rule | GARCH(1,1), fixed TP/SL |
| `simple_breakout_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `SimpleBreakoutAdapter` | pure rule | 288-period rolling high/low |
| `ma_crossover_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `MACrossoverAdapter` | pure rule | EMA(12)/EMA(26) |
| `rsi_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `RSIAdapter` | pure rule | RSI(14), 30/70 thresholds |
| `hmm_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `HMMRegimeAdapter` | sticky/ADX/QPB | 2-state Gaussian HMM |
| `dl_regime_lstm_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `DlRegimeCryptoAdapter` | sticky/ADX/QPB | LSTM |
| `dl_regime_tcn_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `DlRegimeCryptoAdapter` | sticky/ADX/QPB | TCN |
| `dl_regime_transformer_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `DlRegimeCryptoAdapter` | sticky/ADX/QPB | Transformer |

---

## Quick start

```bash
# Run MDRS-SDE walk-forward backtest
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT

# Run benchmark models
qr-backtest --model garch_btc --symbol BTCUSDT
qr-backtest --model simple_breakout_btc --symbol BTCUSDT
qr-backtest --model ma_crossover_btc --symbol BTCUSDT
qr-backtest --model rsi_btc --symbol BTCUSDT
qr-backtest --model hmm_btc --symbol BTCUSDT
qr-backtest --model dl_regime_lstm_btc --symbol BTCUSDT

# Buy-and-Hold
qr-buy-and-hold --symbol BTCUSDT --start 2024-01-01 --end 2025-12-31

# List all registered models
qr-backtest --list-models
```

### Two-phase backtest (fit once, backtest many times)

MCMC sampling is the bottleneck. Use the two-phase API to run fitting
once and replay the backtest instantly whenever config changes.

```bash
# Phase 1 — fit + signal generation (MCMC, runs once, takes time)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --fit-only
# → results/mdrs_sde_btc_btcusdt_<timestamp>_signals.pkl

# Phase 2 — backtest only (seconds, repeat freely)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT \
    --from-signals results/mdrs_sde_btc_btcusdt_<timestamp>_signals.pkl
```

The `.pkl` file contains per-window `WindowResult` objects with the full
signal DataFrame (OHLCV + signal + confidence + all feature columns).
Changing `tp_long`, `sl_long`, or any other execution parameter in
`backtest_settings.toml` and re-running Phase 2 is sufficient — no
re-fitting required.

### Ablation study

```bash
# Full model (sticky + ADX + QPB, v1.17 reference)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT

# w/o QPB gate (v1.16-equivalent behaviour)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT \
    --override filters.qpb.enabled=false

# w/o sticky filter
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-sticky

# w/o ADX gate
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-adx

# Base model (all filters off)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-sticky --no-adx \
    --override filters.qpb.enabled=false
```

### Statistical validation

```bash
qr-validate --result results/mdrs_sde_btc_btcusdt_<timestamp>_trades.csv

# Custom subperiods
qr-validate --result results/mdrs_sde_btc_btcusdt_<timestamp>_trades.csv \
  --subperiod "Bull 2024,2024-01-01,2024-12-31" \
  --subperiod "Bull 2025,2025-01-01,2025-12-31"
```

---

## Configuration

```
configs/
├── backtest_settings.toml          Shared — execution costs, WFA schedule,
│                                   trading parameters, risk management, filters
├── data_settings.toml              Shared — data collection, preprocessing
└── model_parameters/
    ├── mdrs_sde_{btc,eth,sol,xrp}.toml
    ├── garch_{btc,eth,sol,xrp}.toml
    ├── simple_breakout_{btc,eth,sol,xrp}.toml
    ├── ma_crossover_{btc,eth,sol,xrp}.toml
    ├── rsi_{btc,eth,sol,xrp}.toml
    ├── hmm_{btc,eth,sol,xrp}.toml
    └── dl_regime_{lstm,tcn,transformer}_{btc,eth,sol,xrp}.toml
```

### 3-layer config resolution

1. **Package default** — `default_config.toml` shipped inside the model package
2. **Local override** — `configs/model_parameters/<model_key>.toml` — only differing keys
3. **Shared infra** — `configs/backtest_settings.toml` and `configs/data_settings.toml`

### Key settings

```toml
# backtest_settings.toml

[filters]
use_sticky         = true   # sticky breakout persistence filter
use_adx            = true   # ADX gate
only_selected_zone = false

[filters.qpb]
enabled              = true   # Quiet Pre-Breakout microstructure gate
past_vol_48b_max     = 0.003  # 4h log-return std cap
aligned_pret_48b_max = 0.02   # direction-aligned 4h return cap
d_rv_90d_max         = 0.55   # 90d annualised realised vol cap

[walk_forward_settings]
training_months = 3
testing_months  = 1
parallel_jobs   = 10
start_date      = "2020-04-01"
end_date        = "2025-12-31"

[trading_parameters]
tp_long                   = 0.06
sl_long                   = 0.03
tp_short                  = 0.05
sl_short                  = 0.025
trailing_stop_start_ratio = 0.02
max_hold_hours            = 720
```

The `[filters.*]` settings apply only to **regime-probability based
adapters** (`mdrs_sde`, `dl_regime`, `hmm_regime`). Rule-based adapters
(`simple_breakout`, `ma_crossover`, `rsi`) are evaluated as
self-contained technical rules and ignore these settings.

---

## Cross-asset generalization

The QPB gate thresholds in the default configuration are calibrated on
BTC/USDT 2020-2025. Applying identical absolute thresholds to ETH, SOL,
or XRP does not reproduce BTC-level risk-adjusted performance, because
each asset has a distinct volatility distribution (e.g. ETH median 90d
realised vol ≈ 0.77 versus BTC ≈ 0.52), so a BTC-fit threshold occupies
a different informational quantile on a more volatile asset.

For non-BTC assets, users should either:

- disable the gate (`filters.qpb.enabled = false`) and run baseline PAITS, or
- recalibrate `past_vol_48b_max`, `aligned_pret_48b_max`, and
  `d_rv_90d_max` on the target asset via in-sample grid search.

Cross-asset results under BTC-calibrated thresholds are reported in the
companion paper's appendix.

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

For regime-probability models, delegate the downstream filtering to
`backtesting.models.adapters._regime_gates.assemble_signal` to share
the canonical sticky / ADX / QPB pipeline.

**Step 3** — Register in `src/backtesting/models/registry.py`.

```python
_REGISTRY: dict[str, ModelEntry] = {
    ...
    "my_model_btc": ModelEntry(
        MyModelAdapter,
        requires_event_tagging=False,
    ),
}
```

**Step 4** — Add a local override config (optional).

```toml
# configs/model_parameters/my_model_btc.toml
[some_section]
some_param = value
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

`StatisticalValidator` provides additional validation:

| Method | Description |
|---|---|
| Bootstrap CI | 95% CI for Sharpe, Total Return, MDD via 10,000 resamples |
| Permutation test | One-sided p-value against null Sharpe |
| Subperiod analysis | Per-regime metrics across user-defined periods |

---

## Related repositories

- [mdrs-sde](https://github.com/rpycgo-research/mdrs-sde) — MDRS-SDE model implementation
- [dl-regime](https://github.com/rpycgo/dl-regime) — Deep learning regime detection
- [mdrs-sde-theory](https://github.com/rpycgo/mdrs-sde-theory) — Theoretical foundations