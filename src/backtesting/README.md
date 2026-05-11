# backtesting

Model-agnostic walk-forward backtesting framework for the quant-research platform.

Supports multiple asset classes and pluggable model architectures through a clean interface contract. External model packages register via `ModelRegistry` and expose a `BaseModel` implementation; the execution engine remains entirely unaware of model internals.

---

## Architecture

```
backtesting/
├── benchmarks/         BuyAndHoldBenchmark, StatisticalValidator
├── cli/                qr-backtest, qr-buy-and-hold, qr-validate,
│                       qr-frozen-eval, qr-plot-sticky-sweep,
│                       qr-plot-reliability
├── core/               Abstract interfaces (BaseModel, BaseLoader, BaseEngine)
│                       + 3-layer hierarchical config loader
├── assets/
│   └── crypto/         CryptoLoader (Binance / local CSV) + CryptoPreprocessor
├── engines/            GenericBacktestEngine · WalkForwardRunner · PerformanceAnalyzer
├── models/
│   ├── adapters/       mdrs_sde, dl_regime, hmm_regime (share _regime_gates);
│   │                   _wf_qpb (walk-forward QPB estimator);
│   │                   _event_rate (PAITS-Event gate, v1.20+);
│   │                   garch, simple_breakout, ma_crossover, rsi (pure rules)
│   └── registry.py     ModelRegistry with ModelEntry
└── visualization/      PerformancePlotter (equity curve, drawdown, comparison)
```

### Design principles

- **Model layer** owns all model-specific logic: signal generation, MCMC estimation. The engine sees only `signal` (1 / -1 / 0) and `confidence` (0–1).
- **Engine layer** owns execution: TP / SL / trailing stop / time-out. It knows nothing about the model that generated the signals.
- **Config layer** uses a 3-layer merge: package default → local override → shared infra.
- **Regime-probability adapters** (`mdrs_sde`, `dl_regime`, `hmm_regime`) share a common downstream signal-level pipeline (sticky filter → ADX gate → static QPB gate) via `_regime_gates.assemble_signal`, ensuring fair comparison under identical execution conditions.
- **Trade-level WF-QPB post-processing** (optional) is applied by `WalkForwardRunner` after aggregating per-window trades. It re-estimates the three QPB thresholds from recent completed-trade history rather than using fixed values. See the *QPB modes* section below.
- **PAITS-Event gate** is an alternative post-processing mode that replaces the QPB envelope with a bar-level threshold on `rolling_max(regime_prob, 12h)`, recomputed each month by bisection so that the cooldown-adjusted event rate matches a target rate. See the *QPB modes* section below.
- **Rule-based adapters** (`simple_breakout`, `ma_crossover`, `rsi`) are evaluated as self-contained technical trading rules and do not share the regime-probability pipeline.
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
| `lgbm_{btc,eth,sol,xrp}` | BTC/ETH/SOL/XRP | `LGBMRegimeAdapter` | sticky/ADX/QPB | LightGBM binary classifier, full train_slice |

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

### QPB modes

Three QPB modes are supported:

| Mode | Description | When to use |
|---|---|---|
| `static` | Fixed thresholds from `[filters.qpb]` | Reproducing v1.17 results, sensitivity analysis, single-asset calibrated runs |
| `walkforward` | Thresholds re-estimated from trade history (grid search) | OOS-valid evaluation, default for v1.18–v1.19 |
| `event_rate` | PAITS-Event gate (v1.20+); bisection threshold on rolling-max regime_prob, target events/month | Cross-asset robust evaluation, paper-consistent walk-forward (lookback = Bayesian train window) |

```bash
# Explicit mode selection via CLI
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --qpb-mode static
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --qpb-mode walkforward
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --qpb-mode event_rate

# Disable QPB entirely (v1.16 equivalent)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-qpb
```

#### PAITS-Event mode (v1.20+)

Set `[filters.qpb].mode = "event_rate"` and configure
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

The gate computes `score_t = rolling_max(regime_prob, score_window_bars)`,
then at each `refit_freq` boundary bisects over the previous
`lookback_days` of scores to find the threshold giving exactly
`target_events_per_month` cooldown-respecting events. No threshold grid
is hardcoded; the bracket `[score_min, score_max]` is data-driven, so
the gate transfers across assets without retuning absolute
`regime_prob` values.

### Two-phase backtest (fit once, backtest many times)

MCMC sampling is the bottleneck. Use the two-phase API to run fitting
once and replay the backtest instantly whenever config changes. Both
QPB modes are compatible with `--from-signals`.

```bash
# Phase 1 — fit + signal generation (MCMC, runs once, takes time)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --fit-only
# → results/mdrs_sde_btc_btcusdt_<timestamp>_signals.pkl

# Phase 2 — backtest only (seconds, repeat freely)
# Same signals pickle works for any QPB mode
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT \
    --from-signals results/mdrs_sde_btc_btcusdt_<timestamp>_signals.pkl \
    --qpb-mode walkforward
```

### Ablation study

```bash
# Full model (sticky + ADX + static QPB, v1.17 reference)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --qpb-mode static

# Walk-forward QPB
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --qpb-mode walkforward

# w/o QPB gate
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-qpb

# w/o sticky filter
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-sticky

# w/o ADX gate
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-adx

# All filters off (pure baseline)
qr-backtest --model mdrs_sde_btc --symbol BTCUSDT --no-sticky --no-adx --no-qpb
```

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

### Figure 1 — Sticky filter threshold sweep

`qr-plot-sticky-sweep` produces the paper's Figure 1, which
characterises the decision-latency vs. noise-suppression trade-off
of the Persistence-Aware Stabilization Module.

* Panel A: `d_min` vs Sharpe ratio + total return (%)
* Panel B: `d_min` vs median entry delay (minutes) + false flip rate

It consumes a directory of trades CSVs produced by running
`qr-backtest` at different sticky `d_min` values, together with a
signals pickle from `qr-backtest --fit-only`.

```bash
qr-plot-sticky-sweep \
    --trades-dir results/sticky_sweep/ \
    --signals    results/mdrs_sde_btc_btcusdt_<ts>_signals.pkl \
    --baseline   5 \
    --out        results/figure1_sticky_sweep.png

# trades-dir should contain files matching: *duration={N}*.csv
```

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
use_sticky         = true
use_adx            = true
only_selected_zone = false

[filters.qpb]
mode                 = "walkforward"  # "static" | "walkforward"
enabled              = true           # gate on/off (static mode)
past_vol_48b_max     = 0.003          # static thresholds / WF fallback
aligned_pret_48b_max = 0.02
d_rv_90d_max         = 0.55

[filters.qpb.walkforward]
lookback_months           = 9
refit_freq_months         = 3
min_lookback_trades       = 20
min_filter_trades         = 5
criterion                 = "sharpe_stable"  # sharpe | mean | sortino | sharpe_stable
trades_per_year_estimate  = 10.0
vol_grid  = [0.002, 0.0025, 0.003, 0.0035, 0.004, 0.005]
pret_grid = [0.015, 0.020, 0.025, 0.030, 0.040, 0.050]
rv_grid   = [0.45, 0.50, 0.55, 0.60, 0.70, 0.85]
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