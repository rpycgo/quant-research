"""
backtesting.core
================
Abstract interface layer shared by every model, loader, and engine.

All concrete implementations must subclass these ABCs so that the
walk-forward runner can orchestrate them without coupling to any
specific model or data source.
"""
from src.backtesting.core.base_engine import BaseEngine
from src.backtesting.core.base_loader import BaseLoader
from src.backtesting.core.base_model import BaseModel


__all__ = ["BaseModel", "BaseLoader", "BaseEngine"]
