# async-stream-engine

A dependency-light Python 3.11+ example that asynchronously reads OHLCV CSV data and evaluates an incremental moving-average crossover. The strategy uses `Decimal` prices and bounded window state; it does not connect to a broker or place orders.

## Architecture

- `pipeline/stream.py` reads CSV rows in bounded batches using `asyncio.to_thread` so file I/O does not block the event loop.
- `models.py` validates timestamps, OHLC relationships, decimal prices, and volume.
- `core/strategy.py` maintains two rolling sums and windows, and compares averages by cross multiplication to avoid division rounding.
- `core/engine.py` yields signals as an async iterator instead of retaining the entire input stream.

## Complexity

For `N` bars and window sizes `F` and `S`, processing takes **O(N)** time. The strategy's additional state is **O(F + S)**; the CSV reader holds at most one configured batch plus the strategy state. The caller controls any memory used to collect yielded signals.

## Setup and tests

Requires Python 3.11+.

```sh
python -m venv .venv
# Activate the environment, then:
python -m pip install -e ".[dev]"
pytest -q
```

## Run the example

```python
import asyncio
from async_stream_engine import MovingAverageCross, iter_signals
from async_stream_engine.pipeline.stream import stream_csv

async def main():
    bars = stream_csv("examples/sample_ohlcv.csv", batch_size=2)
    strategy = MovingAverageCross(fast_window=2, slow_window=3)
    async for signal in iter_signals(bars, strategy):
        print(signal)

asyncio.run(main())
```

Signals are generated after a bar is read and validated. This is an educational evaluator, not a backtester or investment recommendation.
