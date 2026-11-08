from __future__ import annotations

import asyncio
import csv
from itertools import islice
from pathlib import Path
from typing import AsyncIterator

from async_stream_engine.models import Candle


def _read_batch(reader: csv.DictReader, batch_size: int) -> list[dict[str, str]]:
    return list(islice(reader, batch_size))


async def stream_csv(path: str | Path, batch_size: int = 256) -> AsyncIterator[Candle]:
    """Read a CSV incrementally, moving blocking disk reads off the event loop."""
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero")

    with Path(path).open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"timestamp", "open", "high", "low", "close", "volume"}
        headers = set(reader.fieldnames or ())
        missing = sorted(required - headers)
        if missing:
            raise ValueError(f"CSV missing required columns: {', '.join(missing)}")

        while True:
            rows = await asyncio.to_thread(_read_batch, reader, batch_size)
            if not rows:
                break
            for row in rows:
                yield Candle.from_csv_row(row)
