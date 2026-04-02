"""
backtesting.engines
===================
Trade-simulation and analysis engines.

Components
----------
GenericBacktestEngine
    Simulates trade execution (TP / SL / trailing stop / time-out) against
    historical OHLCV prices.  Reads only ``signal`` (1 / -1 / 0) and
    ``confidence`` from the model layer; all model-specific logic is
    delegated to the adapter.

WalkForwardRunner
    Orchestrates the walk-forward analysis (WFA) loop: slices train/test
    windows, calls ``model.fit`` → ``model.predict`` → ``engine.run_backtest``
    in parallel, and aggregates the results.

PerformanceAnalyzer
    Computes Sharpe, Sortino, MDD, win-rate, IC, and statistical
    significance metrics from a completed-trades ``DataFrame``.
"""
from backtesting.engines.engine import GenericBacktestEngine
from backtesting.engines.performance import PerformanceAnalyzer
from backtesting.engines.walk_forward import WalkForwardRunner


__all__ = ["GenericBacktestEngine", "WalkForwardRunner", "PerformanceAnalyzer"]
