"""
backtesting.cli.plot_reliability
================================
Registered as ``qr-plot-reliability`` in ``[project.scripts]``.
============================
Reliability diagram for w(Z_t) — Bayesian regime weight calibration.

Evaluates whether the posterior regime probability w(Z_t) constitutes
a calibrated probabilistic knowledge source by comparing predicted
probabilities to empirical frequencies of direction-aligned breakout
continuation.

Usage
-----
::
    python backtesting.cli.plot_reliability \
        --signals results/mdrs_sde_btc_btcusdt_<ts>_signals.pkl \
        --price   data/crypto/binance/futures/btcusdt_5m.csv \
        --horizon 280 \
        --cost    0.003 \
        --out     results/figure2_reliability.png

Arguments
---------
--signals   Path to signals .pkl from qr-backtest --fit-only
--price     Path to 5-minute OHLCV CSV (index = timestamp)
--horizon   Forward horizon H in bars (default: 280 = ~23h at 5min)
--cost      Round-trip cost hurdle c (default: 0.003 = 0.3%)
--out       Output PNG path
--n-bins    Number of probability bins (default: 10)
--subperiod Start date filter e.g. 2024-01-01 (optional)
"""
from __future__ import annotations

import argparse
import logging
import pickle
import sys
import types
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ── colour palette ────────────────────────────────────────────────────
C_BLUE  = "#1a6faf"
C_RED   = "#c0392b"
C_GREEN = "#27ae60"
C_GRAY  = "#7f8c8d"
C_BASE  = "#2c3e50"
C_AMBER = "#e67e22"


def _mock_modules() -> None:
    """Mock backtesting modules so signals.pkl loads without the package."""
    mock = types.ModuleType("backtesting")
    mock.engines = types.ModuleType("backtesting.engines")
    mock.engines.walk_forward = types.ModuleType("backtesting.engines.walk_forward")

    class _WindowResult:
        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    mock.engines.walk_forward.WindowResult = _WindowResult
    sys.modules.setdefault("backtesting", mock)
    sys.modules.setdefault("backtesting.engines", mock.engines)
    sys.modules.setdefault("backtesting.engines.walk_forward", mock.engines.walk_forward)


def load_signals(path: str) -> pd.DataFrame:
    """Load signals.pkl and concatenate all window signal_dfs."""
    _mock_modules()
    with open(path, "rb") as fh:
        results = pickle.load(fh)

    frames = []
    for r in results:
        if r is not None and hasattr(r, "signal_df"):
            frames.append(r.signal_df)

    if not frames:
        raise ValueError("No valid windows found in signals pkl.")

    full = pd.concat(frames).sort_index()
    log.info("Loaded %d bars from %d windows", len(full), len(frames))

    return full


def compute_labels(
    signal_df: pd.DataFrame,
    price_df: pd.DataFrame,
    horizon: int,
    cost: float,
) -> pd.DataFrame:
    """Compute direction-aligned future return labels.

    Y_t = 1 if direction_indicator_t * cumulative_log_return_{t+1..t+H} > cost

    shift(-1): label window starts from next bar — no look-ahead bias.
    """
    # join price log_return if not in signal_df
    if "log_return" not in signal_df.columns:
        lr = np.log(price_df["Close"] / price_df["Close"].shift(1))
        signal_df = signal_df.join(lr.rename("log_return"), how="left")

    df = signal_df.copy()

    # future cumulative log-return starting from next bar
    future_ret = (
        df["log_return"]
        .shift(-1)
        .rolling(horizon)
        .sum()
        .shift(-(horizon - 1))
    )

    direction = df["direction_indicator"]

    # label: direction-aligned continuation net of cost hurdle
    df["future_ret"] = future_ret
    df["Y"] = ((future_ret * direction) > cost).astype(int)

    # keep only rows with valid direction and label
    df = df.loc[direction.isin([1, -1]) & df["Y"].notna() & df["regime_prob"].notna()]

    log.info(
        "Label computation: %d valid bars, Y=1 rate=%.3f",
        len(df), df["Y"].mean(),
    )

    return df


def compute_reliability(
    df: pd.DataFrame,
    n_bins: int = 10,
) -> pd.DataFrame:
    """Bin regime_prob and compute empirical event frequency per bin."""
    bins = np.linspace(0, 1, n_bins + 1)
    records = []

    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        mask = (df["regime_prob"] >= lo) & (df["regime_prob"] < hi)
        n = mask.sum()
        if n == 0:
            continue
        mean_p = float(df.loc[mask, "regime_prob"].mean())
        emp_f  = float(df.loc[mask, "Y"].mean())
        records.append({
            "bin_lo":    lo,
            "bin_hi":    hi,
            "bin_center": (lo + hi) / 2,
            "mean_prob": mean_p,
            "emp_freq":  emp_f,
            "count":     n,
            "calib_err": emp_f - mean_p,
        })

    result = pd.DataFrame(records)

    # ECE
    total = result["count"].sum()
    ece = float((result["calib_err"].abs() * result["count"] / total).sum())
    result.attrs["ece"] = ece

    log.info("ECE = %.4f", ece)

    return result


def plot_reliability(
    rel: pd.DataFrame,
    horizon: int,
    cost: float,
    out_path: str,
    ) -> None:
    """Render reliability diagram with sample count bar chart."""
    ece = rel.attrs.get("ece", float("nan"))

    plt.rcParams.update({
        "font.family":       "DejaVu Sans",
        "font.size":         10,
        "axes.linewidth":    0.8,
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "grid.alpha":        0.25,
        "grid.linestyle":    "--",
        "grid.linewidth":    0.6,
        "figure.facecolor":  "white",
        "axes.facecolor":    "#fafafa",
    })

    fig = plt.figure(figsize=(12, 5.5))
    fig.subplots_adjust(wspace=0.35, left=0.09, right=0.96,
                        top=0.92, bottom=0.12)

    ax_main = fig.add_axes([0.09, 0.12, 0.60, 0.80])
    ax_bar  = fig.add_axes([0.76, 0.12, 0.20, 0.80])

    # ── main reliability plot ─────────────────────────────────────────
    ax_main.set_facecolor("#fafafa")
    ax_main.plot([0, 1], [0, 1], color=C_GRAY, linewidth=1.2,
                 linestyle="--", label="Perfect calibration", zorder=2)

    # ±1/√n CI
    ci_lo = np.clip(rel["emp_freq"] - 1 / np.sqrt(rel["count"]), 0, 1)
    ci_hi = np.clip(rel["emp_freq"] + 1 / np.sqrt(rel["count"]), 0, 1)
    ax_main.fill_between(rel["mean_prob"], ci_lo, ci_hi,
                         alpha=0.18, color=C_BLUE, label=r"$\pm 1/\sqrt{n}$ CI")

    # scatter — size ∝ count
    sizes = np.clip(rel["count"] / 10, 40, 400)
    colors = [C_GREEN if e > m else C_RED
              for e, m in zip(rel["emp_freq"], rel["mean_prob"])]
    sc = ax_main.scatter(
        rel["mean_prob"], rel["emp_freq"],
        s=sizes, c=rel["emp_freq"],
        cmap="RdYlGn", vmin=0.3, vmax=0.7,
        zorder=5, edgecolors=C_BASE, linewidths=0.6, alpha=0.9,
    )

    for _, row in rel.iterrows():
        ax_main.annotate(
            f"n={int(row['count']):,}",
            xy=(row["mean_prob"], row["emp_freq"]),
            xytext=(5, 4), textcoords="offset points",
            fontsize=7, color=C_GRAY,
        )

    # shaded regions
    ax_main.fill_between([0, 0.5], [0, 0], [0, 0.5],
                         alpha=0.04, color=C_RED)
    ax_main.fill_between([0.5, 1], [0.5, 1], [1, 1],
                         alpha=0.04, color=C_RED)
    ax_main.fill_between([0, 0.5], [0, 0.5], [0.5, 0.5],
                         alpha=0.04, color=C_GREEN)
    ax_main.fill_between([0.5, 1], [0.5, 0.5], [0.5, 1],
                         alpha=0.04, color=C_GREEN)

    ax_main.text(0.04, 0.93, "Underconfident", fontsize=8,
                 color=C_GREEN, transform=ax_main.transAxes, alpha=0.8)
    ax_main.text(0.55, 0.07, "Overconfident",  fontsize=8,
                 color=C_RED,   transform=ax_main.transAxes, alpha=0.8)

    ax_main.set_xlim(0, 1); ax_main.set_ylim(0, 1)
    ax_main.set_xlabel(
        r"Mean Predicted Regime Probability $\bar{w}(Z_t)$", fontsize=10)
    ax_main.set_ylabel(
        "Empirical Directional Continuation Frequency", fontsize=10)
    ax_main.legend(loc="lower right", fontsize=8.5, framealpha=0.85)
    ax_main.grid(True, zorder=0)
    ax_main.set_title('(A) Reliability diagram')
    plt.colorbar(sc, ax=ax_main, label="Empirical frequency", shrink=0.8)

    # ── bar chart: count per bin ──────────────────────────────────────
    ax_bar.set_facecolor("#fafafa")
    bar_colors = [C_GREEN if e > m else C_RED
                  for e, m in zip(rel["emp_freq"], rel["mean_prob"])]
    ax_bar.barh(rel["mean_prob"], rel["count"], height=0.08,
                color=bar_colors, alpha=0.7, edgecolor="white")
    ax_bar.set_xlabel("Count per bin", fontsize=9)
    ax_bar.set_ylabel(r"$\bar{w}(Z_t)$ bin center", fontsize=9)
    ax_bar.set_ylim(0, 1)
    ax_bar.grid(axis="x", zorder=0)
    ax_bar.set_title('(B) Sample distribution')

    from matplotlib.patches import Patch
    ax_bar.legend(
        handles=[
            Patch(color=C_GREEN, alpha=0.7, label="Underconfident"),
            Patch(color=C_RED,   alpha=0.7, label="Overconfident"),
        ],
        fontsize=8, loc="lower right",
    )

    fig.savefig(out_path, dpi=180, bbox_inches="tight", facecolor="white")
    log.info("Saved → %s", out_path)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="qr-plot-reliability",
        description="Reliability diagram for w(Z_t) Bayesian regime weight.",
    )
    parser.add_argument("--signals",  required=True, help="signals .pkl path")
    parser.add_argument("--price",    required=True, help="5-minute OHLCV CSV path")
    parser.add_argument("--horizon",  type=int,   default=280,
                        help="Forward horizon H in bars (default: 280)")
    parser.add_argument("--cost",     type=float, default=0.003,
                        help="Cost hurdle c (default: 0.003)")
    parser.add_argument("--n-bins",   type=int,   default=10,
                        help="Number of probability bins (default: 10)")
    parser.add_argument("--subperiod", type=str,  default=None,
                        help="Filter from date e.g. 2024-01-01 (optional)")
    parser.add_argument("--out",      default="results/plot/reliability.png",
                        help="Output PNG path")
    args = parser.parse_args()

    # load
    signal_df = load_signals(args.signals)
    price_df  = pd.read_csv(args.price, index_col=0, parse_dates=True)
    price_df.index = pd.to_datetime(price_df.index, utc=True).tz_localize(None)

    if args.subperiod:
        signal_df = signal_df.loc[args.subperiod:]
        log.info("Filtered from %s: %d bars", args.subperiod, len(signal_df))

    # labels
    df = compute_labels(signal_df, price_df, args.horizon, args.cost)

    # reliability
    rel = compute_reliability(df, n_bins=args.n_bins)
    print(rel[["bin_center", "mean_prob", "emp_freq", "count", "calib_err"]]
          .to_string(index=False, float_format="{:.4f}".format))
    print(f"\nECE = {rel.attrs['ece']:.4f}")

    # plot
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    plot_reliability(rel, args.horizon, args.cost, args.out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
