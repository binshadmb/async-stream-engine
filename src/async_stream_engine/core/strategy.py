from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum

from async_stream_engine.models import Candle


class Action(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True, slots=True)
class Signal:
    index: int
    action: Action
    price: Decimal
    timestamp: datetime


class MovingAverageCross:
    """Incremental simple-moving-average crossover using bounded window state."""

    def __init__(self, fast_window: int, slow_window: int) -> None:
        if fast_window < 1 or slow_window < 2:
            raise ValueError("windows must be positive and slow_window must be >= 2")
        if fast_window >= slow_window:
            raise ValueError("fast_window must be smaller than slow_window")
        self.fast_window = fast_window
        self.slow_window = slow_window
        self._fast: deque[Decimal] = deque()
        self._slow: deque[Decimal] = deque()
        self._fast_sum = Decimal(0)
        self._slow_sum = Decimal(0)
        self._previous_relation: int | None = None

    @staticmethod
    def _push(window: deque[Decimal], value: Decimal, limit: int) -> Decimal:
        window.append(value)
        removed = Decimal(0)
        if len(window) > limit:
            removed = window.popleft()
        return removed

    def update(self, candle: Candle, index: int) -> Signal | None:
        self._fast_sum += candle.close
        self._fast_sum -= self._push(self._fast, candle.close, self.fast_window)
        self._slow_sum += candle.close
        self._slow_sum -= self._push(self._slow, candle.close, self.slow_window)

        if len(self._slow) < self.slow_window:
            return None

        # Compare averages by cross multiplication, avoiding division/rounding.
        left = self._fast_sum * len(self._slow)
        right = self._slow_sum * len(self._fast)
        relation = (left > right) - (left < right)

        signal: Signal | None = None
        if self._previous_relation is not None:
            if self._previous_relation <= 0 and relation > 0:
                signal = Signal(index, Action.BUY, candle.close, candle.timestamp)
            elif self._previous_relation >= 0 and relation < 0:
                signal = Signal(index, Action.SELL, candle.close, candle.timestamp)
        self._previous_relation = relation
        return signal
