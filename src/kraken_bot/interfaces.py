"""Boundary for future storage, scanning and simulation consumers."""

from collections.abc import AsyncIterator, Sequence
from typing import Protocol

from .models import CandleBatch, Pair, Price


class MarketData(Protocol):
    async def pairs(self) -> dict[str, Pair]: ...
    async def prices(self, symbols: Sequence[str]) -> list[Price]: ...
    async def candles(self, symbol: str, interval: int = 1,
                      since: int | None = None) -> CandleBatch: ...
    def stream(self, symbols: Sequence[str]) -> AsyncIterator[Price]: ...
