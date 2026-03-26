"""
backtesting.visualization.performance_plotter
==============================================
Model-agnostic performance charts for the quant-research backtesting
module.

Accepts any ``pandas.Series`` produced by
:class:`~backtesting.engines.performance.PerformanceAnalyzer` so it
works identically for MDRS-SDE, GARCH, or any future model.

Intentionally kept separate from model-specific diagnostic plots
(e.g. MCMC pair / trace / rank plots) which live inside each model's
own repository.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd

logger = logging.getLogger(__name__)

_MATPLOTLIB_DEFAULTS: dict[str, Any] = {
    "font.size":       14,
    "axes.labelsize":  16,
    "axes.titlesize":  18,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12,
    "legend.fontsize": 13,
    "figure.dpi":      300,
}


class PerformancePlotter:
    """Model-agnostic equity curve and drawdown visualiser.

    Args:
        save_path: Default output directory for figures.  Can be
                   overridden per method call.

    Example::

        from backtesting.visualization import PerformancePlotter

        plotter = PerformancePlotter(save_path="figures")
        plotter.plot(equity, drawdown, label="MDRS-SDE")
    """

    def __init__(self, save_path: str = "figures") -> None:
        self._save_path = save_path
        plt.rcParams.update(_MATPLOTLIB_DEFAULTS)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def plot(
        self,
        equity: pd.Series,
        drawdown: pd.Series,
        label: str = "Strategy",
        save_path: str | None = None,
        ) -> None:
        """Save equity curve and drawdown profile as separate PNG files.

        Args:
            equity:    Cumulative equity series from
                       ``PerformanceAnalyzer.calculate_metrics``.
            drawdown:  Drawdown series from the same source.
            label:     Strategy label used in plot titles and filenames,
                       e.g. ``"MDRS-SDE"`` or ``"GARCH"``.
            save_path: Output directory.  Falls back to the instance
                       default when ``None``.
        """
        out = save_path or self._save_path
        os.makedirs(out, exist_ok=True)

        self._plot_equity(equity, label, out)
        self._plot_drawdown(drawdown, label, out)

    def plot_comparison(
        self,
        series_map: dict[str, tuple[pd.Series, pd.Series]],
        save_path: str | None = None,
        ) -> None:
        """Overlay multiple equity curves and drawdown profiles on shared axes.

        Useful for comparing MDRS-SDE vs GARCH vs Buy-and-Hold on the
        same chart.

        Args:
            series_map: Mapping of label → ``(equity, drawdown)`` pairs.
                        Example::

                            {
                                "MDRS-SDE": (equity_sde, dd_sde),
                                "GARCH":    (equity_garch, dd_garch),
                            }

            save_path:  Output directory.  Falls back to instance default.
        """
        out = save_path or self._save_path
        os.makedirs(out, exist_ok=True)

        # Equity comparison
        fig, ax = plt.subplots(figsize=(14, 7))
        for label, (equity, _) in series_map.items():
            ax.plot(equity, linewidth=1.8, label=label)
        ax.set_title("Equity curve comparison")
        ax.set_ylabel("Cumulative equity")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(out, "equity_comparison.png"))
        plt.close(fig)

        # Drawdown comparison
        fig, ax = plt.subplots(figsize=(14, 6))
        for label, (_, drawdown) in series_map.items():
            ax.fill_between(
                drawdown.index, drawdown, 0, alpha=0.25, label=label
            )
            ax.plot(drawdown, linewidth=0.8)
        ax.set_title("Drawdown comparison")
        ax.set_ylabel("Drawdown")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(out, "drawdown_comparison.png"))
        plt.close(fig)

        logger.info("Comparison figures saved → %s/", out)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _plot_equity(
        equity: pd.Series,
        label: str,
        out: str,
        ) -> None:
        fig, ax = plt.subplots(figsize=(12, 7))
        ax.plot(equity, color="steelblue", linewidth=2, label=label)
        ax.set_title(f"{label} — equity curve")
        ax.set_ylabel("Cumulative equity")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        stem = label.lower().replace(" ", "_").replace("-", "_")
        fig.savefig(os.path.join(out, f"equity_{stem}.png"))
        plt.close(fig)
        logger.info("Saved equity_%s.png → %s", stem, out)

    @staticmethod
    def _plot_drawdown(
        drawdown: pd.Series,
        label: str,
        out: str,
        ) -> None:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.fill_between(
            drawdown.index, drawdown, 0,
            color="crimson", alpha=0.3, label="Drawdown",
        )
        ax.set_title(f"{label} — drawdown profile")
        ax.set_ylabel("Drawdown")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        stem = label.lower().replace(" ", "_").replace("-", "_")
        fig.savefig(os.path.join(out, f"drawdown_{stem}.png"))
        plt.close(fig)
        logger.info("Saved drawdown_%s.png → %s", stem, out)
