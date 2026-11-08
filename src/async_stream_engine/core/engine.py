from __future__ import annotations

from typing import AsyncIterable, AsyncIterator

from async_stream_engine.core.strategy import MovingAverageCross, Signal
from async_stream_engine.models import Candle


async def iter_signals(
    candles: AsyncIterable[Candle], strategy: MovingAverageCross
) -> AsyncIterator[Signal]:
    """Evaluate bars as they arrive and yield signals without storing the stream."""
    index = 0
    async for candle in candles:
        signal = strategy.update(candle, index)
        if signal is not None:
            yield signal
        index += 1
