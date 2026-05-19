# backtesting

Model-agnostic walk-forward backtesting framework for the quant-research platform.

Supports multiple asset classes and pluggable model architectures through a clean interface contract. External model packages register via `ModelRegistry` and expose a `BaseModel` implementation; the execution engine remains entirely unaware of model internals.

---

## Architecture

```
backtesting/
├── benchmarks/         BuyAndHoldBenchmark, StatisticalValidator
├── cli/                qr-backtest, qr-buy-and-hold, qr-validate,
│                       qr-frozen-eval, qr-plot-reliability
├── core/               Abstract interfaces (BaseModel, BaseLoader, BaseEngine)
│                       + 3-layer hierarchical config loader
├── assets/
│   └── crypto/         CryptoLoader (Binance / local CSV) + CryptoPreprocessor
├── engines/            GenericBacktestEngine · WalkForwardRunner · PerformanceAnalyzer
├── models/
│   ├── adapters/       mdrs_sde, garch, dl_regime, hmm_regime, lgbm_regime
│   │                   (share _regime_gates for raw breakout assembly);
│   │                   _event_rate (pre-execution event-rate gate);
│   │                   simple_breakout, ma_crossover, rsi (pure rules)
│   └── registry.py     ModelRegistry with ModelEntry
└── visualization/      PerformancePlotter (equity curve, drawdown, comparison)
```

### Design principles

- **Model layer** owns all model-specific logic: signal generation, MCMC estimation. The engine sees only `signal` (1 / -1 / 0) and `confidence` (0–1).
- **Engine layer** owns execution: TP / SL / trailing stop / time-out. It knows nothing about the model that generated the signals.
- **Config layer** uses a 3-layer merge: package default → local override → shared infra.
- **Regime-probability adapters** (`mdrs_sde`, `garch`, `dl_regime`, `hmm_regime`, `lgbm_regime`) share a common raw breakout signal assembly via `_regime_gates.assemble_signal`: `signal = +1` when `regime_prob > entry_threshold` and `Close > dynamic_resistance`, `-1` for the short-symmetric case, else `0`. This guarantees fair comparison under identical execution conditions.
- **PAITS-Event / event-rate control** is applied by `WalkForwardRunner` before backtest execution. It thresholds `rolling_max(regime_prob, 12h)` with a threshold recomputed each month by bisection so that the cooldown-adjusted event budget matches `target_events_per_month`, then rewrites `signal` only on admitted event bars. See the *PAITS-Event gate* section below.
- **Rule-based adapters** (`simple_breakout`, `ma_crossover`, `rsi`) are evaluated as self-contained technical trading rules and do not share the regime-probability pipeline.
- **ModelEntry** metadata flags control pipeline branching per model:
  - `requires_event_tagging` — whether DatasetBuilder pipeline is needed (MDRS-SDE only)

---

## Supported models

### Proposed model

| Model key | Asset | Adapter | Notes |
|---|---|---|---|
| `mdrs_sde_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `MdrsSdeCryptoAdapter` | MCMC, event-zone training, breakout + PAITS-Event |

### Benchmark models

| Model key | Asset | Adapter | Pipeline | Notes |
|---|---|---|---|---|
| `garch_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `GarchCryptoAdapter` | breakout + PAITS-Event | GARCH(1,1) volatility mapped to regime probability |
| `simple_breakout_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `SimpleBreakoutAdapter` | pure rule | 288-period rolling high/low |
| `ma_crossover_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `MACrossoverAdapter` | pure rule | EMA(12)/EMA(26) |
| `rsi_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `RSIAdapter` | pure rule | RSI(14), 30/70 thresholds |
| `hmm_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `HMMRegimeAdapter` | breakout + PAITS-Event | 2-state Gaussian HMM |
| `dl_regime_lstm_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `DlRegimeCryptoAdapter` | breakout + PAITS-Event | LSTM |
| `dl_regime_tcn_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `DlRegimeCryptoAdapter` | breakout + PAITS-Event | TCN |
| `dl_regime_transformer_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `DlRegimeCryptoAdapter` | breakout + PAITS-Event | Transformer |
| `lgbm_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `LGBMRegimeAdapter` | breakout + PAITS-Event | LightGBM binary classifier, full train_slice |

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
qr-backtest --model lgbm_btc --symbol BTCUSDT

# List all registered models
qr-backtest --list-models
```

### PAITS-Event gate

PAITS-Event is the pre-execution event-rate gate that controls
entry density. It computes
`score_t = rolling_max(regime_prob, score_window_bars)`, then at each
`refit_freq` boundary bisects over the previous `lookback_days` of
scores to find the threshold giving approximately the configured event
budget, subject to directional availability and cooldown constraints. No
threshold grid is hardcoded; the
bracket `[score_min, score_max]` is data-driven, so the gate
transfers across assets without retuning absolute `regime_prob`
values.

Enable by setting `[filters.qpb].mode = "event_rate"` (this is the
default in `configs/backtest_settings.toml`) and configuring
`[filters.qpb.event_rate]`:

```toml
[filters.qpb.event_rate]
target_events_per_month = 5.0       # primary tuning hyperparameter
cooldown_days           = 5.0       # 1 trading week
lookback_days           = 90        # = Bayesian detector training window
score_window_bars       = 144       # 12h rolling max
refit_freq              = "ME"      # month-end
fallback_threshold      = 0.45      # warm-up fallback
min_lookback_bars       = 100
```

To disable event-rate gating entirely, omit `[filters.qpb]` from the
config file.

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

### Sensitivity studies

To explore PAITS-Event behaviour, edit
`configs/backtest_settings.toml::[filters.qpb.event_rate]` and re-run.
Recommended sweeps:

```toml
target_events_per_month   # 3, 4, 5, 6, 7      (entry density)
cooldown_days             # 3.0, 5.0, 7.0      (re-entry spacing)
lookback_days             # 60, 90, 120, 180   (threshold-fitting window)
```

The legacy ablation flags (`--no-sticky`, `--no-adx`, `--no-qpb`,
`--qpb-mode`) were removed in v2.0 together with the sticky / ADX /
static QPB filter chain. To reproduce v1.x baselines, check out the
`v1.20.0` tag.

### Statistical validation

```bash
qr-validate --result results/mdrs_sde_btc_btcusdt_<timestamp>_trades.csv

# Custom subperiods
qr-validate --result results/mdrs_sde_btc_btcusdt_<timestamp>_trades.csv \
  --subperiod "Bull 2024,2024-01-01,2024-12-31"
```

### Frozen-parameter robustness evaluation

`qr-frozen-eval` trains the MCMC regime detector on a single fixed
training block, freezes all estimated parameters, then evaluates the
system on the subsequent test period without any re-estimation. The
performance gap to the rolling walk-forward baseline empirically
quantifies the value of periodic Bayesian recalibration.

```bash
qr-frozen-eval \
    --model       mdrs_sde_btc \
    --symbol      BTCUSDT \
    --train-start 2020-01-01 \
    --train-end   2020-12-31 \
    --test-start  2021-01-01 \
    --test-end    2025-12-31 \
    --config-dir  src/configs \
    --out         results/frozen_eval.csv
```

Pipeline: load full OHLCV → slice training block, apply event
tagging and fit MCMC into a frozen params dict → roll the test block
month by month, predicting with the frozen params and running the
standard backtest engine → emit per-month trade CSVs plus a combined
summary for direct comparison against rolling WFA.

### Figure 2 — Reliability diagram for w(Z_t)

`qr-plot-reliability` produces the paper's Figure 2, the
reliability diagram for the Bayesian regime weight `w(Z_t)`. It
checks whether the posterior probability is *calibrated* — i.e.,
whether predicted probabilities match empirical frequencies of
direction-aligned breakout continuation under a profitability
hurdle.

```bash
qr-plot-reliability \
    --signals results/mdrs_sde_btc_btcusdt_<ts>_signals.pkl \
    --price   data/crypto/binance/futures/btcusdt_5m.csv \
    --horizon 280 \
    --cost    0.003 \
    --out     results/figure2_reliability.png
```

* `--horizon` — forward horizon `H` in bars (default 280 ≈ 23h at 5min)
* `--cost`    — round-trip cost hurdle `c` (default 0.003 = 0.30%)
* `--n-bins`  — number of probability bins (default 10)
* `--subperiod` — optional start date filter (e.g. `2024-01-01`)

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
    ├── dl_regime_{lstm,tcn,transformer}_{btc,eth,sol,xrp}.toml
    └── lgbm_{btc,eth,sol,xrp}.toml
```

### Key settings

```toml
# backtest_settings.toml

[filters]
only_selected_zone = false

[filters.qpb]
mode = "event_rate"

[filters.qpb.event_rate]
target_events_per_month = 5.0
cooldown_days           = 5.0
lookback_days           = 90
score_window_bars       = 144
refit_freq              = "ME"
fallback_threshold      = 0.45
min_lookback_bars       = 100
```

The `[filters.qpb.event_rate]` settings apply only to
**regime-probability based adapters** (`mdrs_sde`, `dl_regime`,
`hmm_regime`, `lgbm_regime`). Rule-based adapters (`simple_breakout`,
`ma_crossover`, `rsi`) are evaluated as self-contained technical rules
and ignore these settings.

---

## Cross-asset generalization

Unlike the legacy static QPB thresholds (BTC-calibrated absolute
values for `past_vol_48b_max`, `aligned_pret_48b_max`, and
`d_rv_90d_max`), the PAITS-Event gate is *self-calibrating* per
asset:

- The threshold bracket is `[min, max]` of the asset's own
  `rolling_max(regime_prob, 12h)` over the previous 3 months.
- Bisection finds the value giving exactly
  `target_events_per_month` cooldown-respecting events for *that
  asset's* probability distribution.

Therefore the *same* `[filters.qpb.event_rate]` block can be applied
to ETH, SOL, or XRP without rescaling. The cross-asset comparison
becomes meaningful: differences in performance reflect detector
quality on each asset's regime structure, not gate-calibration
mismatch. Cross-asset results are reported in the companion paper's
appendix.

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

For regime-probability models, delegate breakout signal assembly to
`backtesting.models.adapters._regime_gates.assemble_signal` to share
the canonical breakout-direction logic with `mdrs_sde`, `dl_regime`,
`hmm_regime`, and `lgbm_regime`.

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