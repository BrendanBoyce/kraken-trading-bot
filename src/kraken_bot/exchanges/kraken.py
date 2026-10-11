"""Unauthenticated Kraken Spot REST and WebSocket v2 market data."""

import asyncio
import json
import logging
import random
import time
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp

from ..config import normalize_symbol
from ..models import Candle, CandleBatch, Pair, Price

LOG = logging.getLogger(__name__)
INTERVALS = {1, 5, 15, 30, 60, 240, 1440, 10080, 21600}


class KrakenError(RuntimeError):
    """Non-retryable API or subscription error."""


class TransientError(KrakenError):
    """An exhausted retryable API error."""


def parse_ticker(message: dict) -> list[Price]:
    if message.get("channel") != "ticker":
        return []
    received = datetime.now(UTC)
    return [Price(
        normalize_symbol(row["symbol"]), Decimal(str(row["last"])),
        Decimal(str(row["bid"])), Decimal(str(row["ask"])),
        Decimal(str(row["volume"])), received,
        datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
        if row.get("timestamp") else None,
    ) for row in message["data"]]


class KrakenClient:
    def __init__(self, *, rest_url: str = "https://api.kraken.com/0/public",
                 ws_url: str = "wss://ws.kraken.com/v2", retries: int = 4) -> None:
        if retries < 0:
            raise ValueError("retries must be nonnegative")
        self.rest_url = rest_url
        self.ws_url = ws_url
        self.retries = retries
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()
        self._last_request = 0.0
        self._pairs: dict[str, Pair] | None = None

    async def __aenter__(self) -> "KrakenClient":
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self

    async def __aexit__(self, *args) -> None:
        if self._session:
            await self._session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("Use KrakenClient as an async context manager")
        return self._session

    async def _request(self, endpoint: str, params: dict | None = None) -> dict:
        if endpoint not in {"AssetPairs", "Ticker", "OHLC"}:
            raise ValueError("Unsupported public market-data endpoint")
        for attempt in range(self.retries + 1):
            delay = min(2 ** attempt, 30) + random.uniform(0, 1)
            try:
                # Serialize starts across endpoints, including retry attempts.
                async with self._lock:
                    await asyncio.sleep(max(0, 1 - (time.monotonic() - self._last_request)))
                    self._last_request = time.monotonic()
                    async with self.session.get(f"{self.rest_url}/{endpoint}", params=params) as response:
                        if response.status == 429 or response.status >= 500:
                            retry_after = response.headers.get("Retry-After", "")
                            try:
                                delay = max(delay, float(retry_after))
                            except ValueError:
                                pass
                            raise TransientError(f"HTTP {response.status}")
                        if response.status >= 400:
                            raise KrakenError(f"HTTP {response.status} from {endpoint}")
                        payload = json.loads(await response.text(), parse_float=Decimal)
                errors = payload.get("error", [])
                if errors:
                    detail = "; ".join(errors)
                    if any(token in detail for token in (
                        "Rate limit", "Throttled", "Unavailable", "Busy", "Internal error"
                    )):
                        if "Throttled:" in detail:
                            try:
                                delay = max(delay, float(detail.rsplit(":", 1)[1]) - time.time())
                            except ValueError:
                                pass
                        raise TransientError(detail)
                    raise KrakenError(detail)
                return payload["result"]
            except (aiohttp.ClientError, asyncio.TimeoutError, TransientError) as exc:
                if attempt == self.retries:
                    raise TransientError(f"{endpoint} failed after {attempt + 1} attempts: {exc}") from exc
                LOG.warning("%s failed: %s; retrying in %.1fs", endpoint, exc, delay)
                await asyncio.sleep(delay)
        raise AssertionError("unreachable")

    async def pairs(self) -> dict[str, Pair]:
        if self._pairs is None:
            result = await self._request("AssetPairs")
            self._pairs = {}
            for key, row in result.items():
                if row.get("wsname") and not key.endswith(".d"):
                    symbol = normalize_symbol(row["wsname"])
                    self._pairs[symbol] = Pair(symbol, key, row["altname"], row.get("status", "unknown"))
        return dict(self._pairs)

    async def _resolve(self, symbols: Sequence[str]) -> list[Pair]:
        available = await self.pairs()
        resolved = []
        for value in symbols:
            symbol = normalize_symbol(value)
            if symbol not in available:
                raise KrakenError(f"Unknown Kraken Spot pair: {symbol}")
            resolved.append(available[symbol])
        if not resolved:
            raise ValueError("At least one symbol is required")
        return resolved

    async def prices(self, symbols: Sequence[str]) -> list[Price]:
        pairs = await self._resolve(symbols)
        result = await self._request("Ticker", {"pair": ",".join(p.altname for p in pairs)})
        received = datetime.now(UTC)
        prices = []
        for pair in pairs:
            row = result.get(pair.rest_id, result.get(pair.altname))
            if row is None:
                raise KrakenError(f"Ticker response omitted {pair.symbol}")
            prices.append(Price(pair.symbol, Decimal(row["c"][0]), Decimal(row["b"][0]),
                                Decimal(row["a"][0]), Decimal(row["v"][1]), received))
        return prices

    async def candles(self, symbol: str, interval: int = 1,
                      since: int | None = None) -> CandleBatch:
        if interval not in INTERVALS:
            raise ValueError(f"Supported minute intervals: {sorted(INTERVALS)}")
        pair = (await self._resolve([symbol]))[0]
        params = {"pair": pair.altname, "interval": interval}
        if since is not None:
            if since < 0:
                raise ValueError("since must be a nonnegative Unix timestamp")
            params["since"] = since
        result = await self._request("OHLC", params)
        rows = result.get(pair.rest_id, result.get(pair.altname))
        if rows is None:
            raise KrakenError(f"OHLC response omitted {pair.symbol}")
        candles = tuple(Candle(
            pair.symbol, datetime.fromtimestamp(row[0], UTC), interval,
            *(Decimal(str(value)) for value in row[1:7]), int(row[7]), i < len(rows) - 1,
        ) for i, row in enumerate(rows))
        return CandleBatch(candles, int(result["last"]))

    async def stream(self, symbols: Sequence[str]) -> AsyncIterator[Price]:
        pairs = await self._resolve(symbols)
        requested = {pair.symbol for pair in pairs}
        failures = 0
        while True:
            started = time.monotonic()
            try:
                async with self.session.ws_connect(
                    self.ws_url, heartbeat=20, max_msg_size=2 ** 20,
                ) as ws:
                    await ws.send_json({"method": "subscribe", "params": {
                        "channel": "ticker", "symbol": sorted(requested), "snapshot": True,
                    }})
                    pending = set(requested)
                    deadline = time.monotonic() + 20
                    while True:
                        timeout = min(30, max(0.01, deadline - time.monotonic())) if pending else 30
                        message = await asyncio.wait_for(ws.receive(), timeout=timeout)
                        if message.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(message.data, parse_float=Decimal)
                            if data.get("method") == "subscribe":
                                if not data.get("success"):
                                    raise KrakenError(f"Subscription rejected: {data.get('error', data)}")
                                pending.discard(data["result"]["symbol"])
                            for price in parse_ticker(data):
                                if price.symbol in requested:
                                    yield price
                            if pending and time.monotonic() >= deadline:
                                raise asyncio.TimeoutError("Subscription acknowledgements timed out")
                        elif message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED,
                                             aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSING}:
                            raise ConnectionError("WebSocket disconnected")
            except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as exc:
                if time.monotonic() - started > 60:
                    failures = 0
                # Minimum five seconds avoids aggressive reconnects during outages.
                delay = min(5 * 2 ** min(failures, 4), 60) + random.uniform(0, 1)
                failures += 1
                LOG.warning("Live feed interrupted: %s; reconnecting in %.1fs", exc, delay)
                await asyncio.sleep(delay)
