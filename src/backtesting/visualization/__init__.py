"""
backtesting.visualization
==========================
General-purpose performance visualisation for the backtesting module.

Exports
-------
PerformancePlotter
    Equity curve and drawdown profile charts.  Model-agnostic — accepts
    any ``pandas.Series`` produced by
    :class:`~backtesting.engines.performance.PerformanceAnalyzer`.
"""

from backtesting.visualization.performance_plotter import PerformancePlotter

__all__ = ["PerformancePlotter"]
