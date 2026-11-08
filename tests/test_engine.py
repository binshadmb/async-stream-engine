from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from async_stream_engine.core.engine import iter_signals
from async_stream_engine.core.strategy import Action, MovingAverageCross
from async_stream_engine.models import Candle
from async_stream_engine.pipeline.stream import stream_csv


def candle(price: str, minute: int = 0) -> Candle:
    value = Decimal(price)
    return Candle(
        timestamp=datetime(2026, 1, 1, 9, minute, tzinfo=timezone.utc),
        open=value,
        high=value,
        low=value,
        close=value,
        volume=100,
    )


class CandleTests(unittest.TestCase):
    def test_rejects_timezone_free_timestamp(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone"):
            Candle(datetime(2026, 1, 1), *(Decimal("1") for _ in range(4)), 0)

    def test_rejects_inconsistent_ohlc(self) -> None:
        with self.assertRaisesRegex(ValueError, "high"):
            Candle(datetime.now(timezone.utc), Decimal("5"), Decimal("4"),
                   Decimal("3"), Decimal("5"), 1)

    def test_parses_exact_decimal_values(self) -> None:
        row = {
            "timestamp": "2026-01-01T09:00:00+00:00",
            "open": "1.10", "high": "1.20", "low": "1.00",
            "close": "1.15", "volume": "12",
        }
        bar = Candle.from_csv_row(row)
        self.assertEqual(bar.close, Decimal("1.15"))
        self.assertEqual(bar.volume, 12)


class StrategyTests(unittest.TestCase):
    def test_emits_crossover_signals(self) -> None:
        prices = ["3", "2", "1", "2", "3", "4", "3", "2", "1"]
        strategy = MovingAverageCross(fast_window=2, slow_window=3)
        signals = [
            result for i, price in enumerate(prices)
            if (result := strategy.update(candle(price, i), i)) is not None
        ]
        self.assertEqual([signal.action for signal in signals], [Action.BUY, Action.SELL])
        self.assertEqual([signal.index for signal in signals], [4, 7])

    def test_rejects_bad_window_sizes(self) -> None:
        with self.assertRaises(ValueError):
            MovingAverageCross(3, 3)


class PipelineTests(unittest.TestCase):
    def test_csv_stream_is_incremental_and_async(self) -> None:
        data = (
            "timestamp,open,high,low,close,volume\n"
            "2026-01-01T09:00:00+00:00,1,1,1,1,10\n"
            "2026-01-01T09:01:00+00:00,2,2,2,2,20\n"
        )
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "bars.csv"
            path.write_text(data, encoding="utf-8")

            async def collect() -> list[Candle]:
                return [bar async for bar in stream_csv(path, batch_size=1)]

            bars = asyncio.run(collect())
        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[1].close, Decimal("2"))

    def test_async_engine_yields_signals(self) -> None:
        prices = ["3", "2", "1", "2", "3", "4"]

        async def source():
            for i, price in enumerate(prices):
                yield candle(price, i)

        async def collect():
            return [
                signal async for signal in iter_signals(
                    source(), MovingAverageCross(2, 3)
                )
            ]

        signals = asyncio.run(collect())
        self.assertEqual([signal.action for signal in signals], [Action.BUY])


if __name__ == "__main__":
    unittest.main()
