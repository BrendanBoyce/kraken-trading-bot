"""Shared immutable records. All timestamps are timezone-aware UTC."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class Pair:
    symbol: str
    rest_id: str
    altname: str
    status: str


@dataclass(frozen=True)
class Price:
    symbol: str
    last: Decimal
    bid: Decimal
    ask: Decimal
    volume_24h: Decimal
    received_at: datetime
    exchange_timestamp: datetime | None = None


@dataclass(frozen=True)
class Candle:
    symbol: str
    timestamp: datetime
    interval_minutes: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    vwap: Decimal
    volume: Decimal
    trades: int
    complete: bool


@dataclass(frozen=True)
class CandleBatch:
    candles: tuple[Candle, ...]
    next_since: int
