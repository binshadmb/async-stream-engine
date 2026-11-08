"""Asynchronous, dependency-light stream evaluation tools."""

from .core.engine import iter_signals
from .core.strategy import Action, MovingAverageCross, Signal
from .models import Candle

__all__ = ["Action", "Candle", "MovingAverageCross", "Signal", "iter_signals"]
