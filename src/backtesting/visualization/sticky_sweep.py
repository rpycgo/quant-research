"""
backtesting.visualization.sticky_sweep
=============================
Figure 1 — Sticky Filter Threshold Sweep.

Combines per-threshold trade performance (from trades CSVs) with
false flip rate and entry delay (computed from signals.pkl) to
characterise the decision latency vs. noise suppression trade-off
of the Persistence-Aware Stabilization Module.

Panel A: d_min vs Sharpe Ratio + Total Return (%)
Panel B: d_min vs Median Entry Delay (min) + False Flip Rate (%)

Usage
-----
::
    python backtesting/visualization/sticky_sweep.py \
        --trades-dir results/sticky_sweep/ \
        --signals    results/mdrs_sde_btc_btcusdt_<ts>_signals.pkl \
        --baseline   5 \
        --out        results/figure1_sticky_sweep.png

    # trades-dir should contain files matching: *duration={N}*.csv
    # e.g. mdrs_sde_btc_btcusdt_20260421_234100_duration=5.csv
"""
from __future__ import annotations

import argparse
import logging
import pickle
import re
import sys
import types
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

C_BLUE  = "#1a6faf"
C_RED   = "#c0392b"
C_GREEN = "#27ae60"
C_GRAY  = "#7f8c8d"
C_BASE  = "#2c3e50"
C_AMBER = "#e67e22"


# ─────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────

def load_trades_dir(trades_dir: str) -> pd.DataFrame:
    """Load all duration_N trades CSVs from a directory.

    Parses d_min from filenames matching ``*duration={N}*.csv``.

    Returns a DataFrame with columns:
        d_min, trades, ret, sharpe, mdd, wr
    """
    pattern = re.compile(r"duration=(\d+)")
    records = []

    for path in sorted(Path(trades_dir).glob("*.csv")):
        m = pattern.search(path.stem)
        if not m:
            continue
        d_min = int(m.group(1))

        df = pd.read_csv(path)
        if df.empty or "PnL" not in df.columns:
            log.warning("Skipping %s — empty or missing PnL column.", path.name)
            continue

        records.append(_compute_performance(df, d_min))

    if not records:
        raise FileNotFoundError(
            f"No *duration=N*.csv files found in {trades_dir}"
        )

    result = pd.DataFrame(records).sort_values("d_min").reset_index(drop=True)
    log.info("Loaded %d duration levels: %s",
             len(result), list(result["d_min"]))

    return result


def _compute_performance(df: pd.DataFrame, d_min: int) -> dict:
    """Compute key metrics from a trades DataFrame."""
    df["entry_time"] = pd.to_datetime(df["entry_time"])
    equity = (1 + df["PnL"]).cumprod()
    mdd    = ((equity / equity.cummax()) - 1).min() * 100
    ret    = (equity.iloc[-1] - 1) * 100
    n      = len(df)
    dur    = (df["entry_time"].max() - df["entry_time"].min()).days
    ann    = np.sqrt(n / max(dur / 365, 1e-9))
    sharpe = (df["PnL"].mean() / df["PnL"].std() * ann
              if df["PnL"].std() > 0 else 0.0)
    wr     = (df["PnL"] > 0).mean() * 100

    return dict(d_min=d_min, trades=n,
                ret=round(ret, 2), sharpe=round(sharpe, 3),
                mdd=round(mdd, 2), wr=round(wr, 2))


def _mock_modules() -> None:
    mock = types.ModuleType("backtesting")
    mock.engines = types.ModuleType("backtesting.engines")
    mock.engines.walk_forward = types.ModuleType("backtesting.engines.walk_forward")

    class _WR:
        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    mock.engines.walk_forward.WindowResult = _WR
    sys.modules.setdefault("backtesting", mock)
    sys.modules.setdefault("backtesting.engines", mock.engines)
    sys.modules.setdefault(
        "backtesting.engines.walk_forward", mock.engines.walk_forward)


def load_full_regime_prob(signals_path: str) -> pd.Series:
    """Load and concatenate regime_prob from all windows in signals.pkl."""
    _mock_modules()
    with open(signals_path, "rb") as fh:
        results = pickle.load(fh)

    frames = []
    for r in results:
        if r is not None and hasattr(r, "signal_df"):
            if "regime_prob" in r.signal_df.columns:
                frames.append(r.signal_df["regime_prob"])

    if not frames:
        raise ValueError("No regime_prob found in signals pkl.")

    series = pd.concat(frames).sort_index()
    log.info("Loaded regime_prob: %d bars", len(series))

    return series


# ─────────────────────────────────────────────────────────────────────
# False flip & entry delay computation
# ─────────────────────────────────────────────────────────────────────

def compute_sweep_stats(
    regime_prob: pd.Series,
    d_min_values: list[int],
    entry_threshold: float = 0.5,
) -> pd.DataFrame:
    """Compute false flip rate and median entry delay for each d_min.

    Definitions
    -----------
    Raw activation:
        A new run of regime_prob > entry_threshold starting at bar t
        (i.e. prob[t] > threshold and prob[t-1] <= threshold).

    False flip:
        A raw activation that does NOT sustain for d_min consecutive
        bars — i.e. the signal collapses before being confirmed.

    False flip rate:
        false_flips / total_raw_activations × 100 (%)

    Entry delay:
        For confirmed activations only: number of bars between the raw
        activation start (t0) and sticky confirmation (ts = t0 + d_min - 1).
        Always equals d_min - 1 bars = (d_min - 1) × 5 minutes.
        Reported in minutes for interpretability.
    """
    raw_vals = (regime_prob > entry_threshold).astype(int).values
    n = len(raw_vals)

    records = []
    for d in d_min_values:
        false_flips      = 0
        true_activations = 0

        for idx in range(1, n):
            # detect new activation start
            if raw_vals[idx] == 1 and raw_vals[idx - 1] == 0:
                end    = min(idx + d, n)
                window = raw_vals[idx:end]
                if len(window) == d and window.sum() == d:
                    true_activations += 1
                else:
                    false_flips += 1

        total         = false_flips + true_activations
        ff_rate       = (false_flips / total * 100) if total > 0 else 0.0
        # confirmed entries always delayed by (d-1) bars × 5 min
        delay_bars    = max(d - 1, 0)
        delay_minutes = delay_bars * 5

        records.append(dict(
            d_min          = d,
            raw_activations= total,
            false_flips    = false_flips,
            confirmed      = true_activations,
            ff_rate        = round(ff_rate, 1),
            delay_bars     = delay_bars,
            delay_minutes  = delay_minutes,
        ))

        log.info("d_min=%d: raw=%d false_flips=%d ff_rate=%.1f%% delay=%dmin",
                 d, total, false_flips, ff_rate, delay_minutes)

    return pd.DataFrame(records).sort_values("d_min").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────
# Plotting
# ─────────────────────────────────────────────────────────────────────

def plot_figure1(
    perf_df: pd.DataFrame,
    sweep_df: pd.DataFrame,
    baseline: int,
    out_path: str,
) -> None:
    """Render Figure 1: two-panel sticky threshold sweep."""
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

    # merge on d_min
    df = perf_df.merge(sweep_df, on="d_min")
    d  = df["d_min"].tolist()

    fig = plt.figure(figsize=(12, 9))
    fig.suptitle(
        "Figure 1 — Sticky Filter Threshold Sweep",
        fontsize=13, fontweight="bold", color=C_BASE, y=0.98,
    )
    gs = gridspec.GridSpec(2, 1, hspace=0.45,
                           top=0.93, bottom=0.10,
                           left=0.10, right=0.88)

    # ── Panel A: Sharpe + Return ──────────────────────────────────────
    ax_a  = fig.add_subplot(gs[0])
    ax_ar = ax_a.twinx()

    bar_colors = [C_BLUE if s > 0 else C_RED for s in df["sharpe"]]
    ax_a.bar(d, df["sharpe"], color=bar_colors, alpha=0.75,
             width=0.55, zorder=3, label="Sharpe Ratio")
    ax_a.axhline(0, color=C_BASE, linewidth=0.8)
    ax_a.axvline(baseline, color=C_AMBER, linewidth=1.5,
                 linestyle="--", alpha=0.85, zorder=4)
    ax_a.text(baseline + 0.15, df["sharpe"].max() * 0.92,
              f"baseline\n($d_{{min}}={baseline}$)",
              color=C_AMBER, fontsize=8.5, va="top")

    ax_ar.plot(d, df["ret"], color=C_GREEN, marker="o",
               markersize=5, linewidth=1.6, label="Total Return (%)", zorder=5)
    ax_ar.axhline(0, color=C_GREEN, linewidth=0.5, linestyle=":", alpha=0.5)

    ax_a.set_xlabel(
        r"Minimum Persistence Length $d_{\min}$ (bars)", fontsize=10)
    ax_a.set_ylabel("Sharpe Ratio", fontsize=10, color=C_BLUE)
    ax_ar.set_ylabel("Total Return (%)", fontsize=10, color=C_GREEN)
    ax_a.set_xticks(d)
    ax_a.tick_params(axis="y", labelcolor=C_BLUE)
    ax_ar.tick_params(axis="y", labelcolor=C_GREEN)
    ax_a.set_title("Panel A — Economic Performance",
                   fontsize=10.5, fontweight="bold", color=C_BASE, pad=6)
    ax_a.grid(axis="y", zorder=0)
    ax_a.set_facecolor("#fafafa")

    # trade count annotation
    y_bot = df["sharpe"].min() - abs(df["sharpe"].min()) * 0.15
    for di, ti in zip(d, df["trades"]):
        ax_a.text(di, y_bot, f"n={ti}",
                  ha="center", va="top", fontsize=7.2, color=C_GRAY)

    lines1, lbl1 = ax_a.get_legend_handles_labels()
    lines2, lbl2 = ax_ar.get_legend_handles_labels()
    ax_a.legend(lines1 + lines2, lbl1 + lbl2,
                loc="upper left", fontsize=8.5, framealpha=0.85)

    # ── Panel B: Entry Delay + False Flip Rate ────────────────────────
    ax_b  = fig.add_subplot(gs[1])
    ax_br = ax_b.twinx()

    ax_b.plot(d, df["delay_minutes"], color=C_RED, marker="s",
              markersize=5.5, linewidth=1.8,
              label="Median Entry Delay (min)", zorder=5)
    ax_b.fill_between(d, df["delay_minutes"],
                      alpha=0.10, color=C_RED)

    ax_br.bar(d, df["ff_rate"], color=C_GRAY, alpha=0.35,
              width=0.55, zorder=2, label="False Flip Rate (%)")
    ax_br.plot(d, df["ff_rate"], color=C_GRAY, marker="o",
               markersize=4, linewidth=1.2, linestyle="--", zorder=4)

    ax_b.axvline(baseline, color=C_AMBER, linewidth=1.5,
                 linestyle="--", alpha=0.85, zorder=6)

    ax_b.set_xlabel(
        r"Minimum Persistence Length $d_{\min}$ (bars)", fontsize=10)
    ax_b.set_ylabel("Entry Delay (minutes)", fontsize=10, color=C_RED)
    ax_br.set_ylabel("False Flip Rate (%)", fontsize=10, color=C_GRAY)
    ax_b.set_xticks(d)
    ax_b.tick_params(axis="y", labelcolor=C_RED)
    ax_br.tick_params(axis="y", labelcolor=C_GRAY)
    ax_br.set_ylim(0, 105)
    ax_b.set_title(
        "Panel B — Decision Latency vs. Noise Suppression",
        fontsize=10.5, fontweight="bold", color=C_BASE, pad=6)
    ax_b.grid(axis="y", zorder=0)
    ax_b.set_facecolor("#fafafa")

    # false flip rate labels
    for di, ff in zip(d, df["ff_rate"]):
        ax_br.text(di, ff + 1.5, f"{ff:.0f}%",
                   ha="center", va="bottom", fontsize=7, color=C_GRAY)

    lines3, lbl3 = ax_b.get_legend_handles_labels()
    lines4, lbl4 = ax_br.get_legend_handles_labels()
    ax_b.legend(lines3 + lines4, lbl3 + lbl4,
                loc="upper left", fontsize=8.5, framealpha=0.85)

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=180, bbox_inches="tight", facecolor="white")
    log.info("Saved → %s", out_path)


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="plot_sticky_sweep",
        description="Figure 1 — Sticky Filter Threshold Sweep.",
    )
    parser.add_argument(
        "--trades-dir", required=True,
        help="Directory containing *duration={N}*.csv files.",
    )
    parser.add_argument(
        "--signals", required=True,
        help="signals .pkl path (for false flip / entry delay computation).",
    )
    parser.add_argument(
        "--baseline", type=int, default=5,
        help="d_min value to highlight as baseline (default: 5).",
    )
    parser.add_argument(
        "--entry-threshold", type=float, default=0.5,
        help="regime_prob threshold for raw activation (default: 0.5).",
    )
    parser.add_argument(
        "--out", default="results/figure1_sticky_sweep.png",
        help="Output PNG path.",
    )
    args = parser.parse_args()

    # load performance data
    perf_df = load_trades_dir(args.trades_dir)
    d_min_values = perf_df["d_min"].tolist()

    # load regime_prob and compute sweep stats
    regime_prob = load_full_regime_prob(args.signals)
    sweep_df    = compute_sweep_stats(
        regime_prob, d_min_values, args.entry_threshold)

    # print summary table
    merged = perf_df.merge(sweep_df, on="d_min")
    print("\n" + "=" * 85)
    print("STICKY THRESHOLD SWEEP SUMMARY")
    print("=" * 85)
    cols = ["d_min", "trades", "ret", "sharpe", "mdd", "wr",
            "ff_rate", "delay_minutes"]
    print(merged[cols].to_string(index=False))
    print("=" * 85)

    # plot
    plot_figure1(perf_df, sweep_df, args.baseline, args.out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
