from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping


@dataclass(frozen=True, slots=True)
class Candle:
    """One validated OHLCV bar. Prices use Decimal to avoid binary-float drift."""

    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        prices = (self.open, self.high, self.low, self.close)
        if any(not value.is_finite() or value <= 0 for value in prices):
            raise ValueError("OHLC prices must be finite and greater than zero")
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("high must be at least open, close, and low")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError("low must be at most open, close, and high")
        if isinstance(self.volume, bool) or self.volume < 0:
            raise ValueError("volume must be a non-negative integer")

    @classmethod
    def from_csv_row(cls, row: Mapping[str, str]) -> Candle:
        """Parse a row with timestamp, open, high, low, close, and volume fields."""
        required = ("timestamp", "open", "high", "low", "close", "volume")
        missing = [name for name in required if name not in row or row[name] == ""]
        if missing:
            raise ValueError(f"missing CSV fields: {', '.join(missing)}")
        try:
            timestamp = datetime.fromisoformat(row["timestamp"])
            prices = {name: Decimal(row[name]) for name in ("open", "high", "low", "close")}
            volume = int(row["volume"])
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError(f"invalid OHLCV row: {exc}") from exc
        return cls(timestamp=timestamp, volume=volume, **prices)
