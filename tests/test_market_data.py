import asyncio
import json
from datetime import UTC
from decimal import Decimal

import pytest
from aiohttp import web

from kraken_bot.config import load_assets, normalize_symbol
from kraken_bot.exchanges.kraken import KrakenClient, KrakenError, TransientError, parse_ticker
from kraken_bot.models import Pair

PAIR = {"XXBTZUSD": {"wsname": "XBT/USD", "altname": "XBTUSD", "status": "online"}}
TICK = {"symbol": "BTC/USD", "last": "123.456789123", "bid": "123", "ask": "124",
        "volume": "10", "timestamp": "2026-10-11T12:00:00Z"}


def test_config(tmp_path):
    path = tmp_path / "assets.json"
    path.write_text(json.dumps({"assets": [" xbt/usd ", "ETH/USD"]}))
    assert load_assets(path) == ("BTC/USD", "ETH/USD")
    path.write_text(json.dumps({"assets": ["BTC/USD", "XBT/USD"]}))
    with pytest.raises(ValueError, match="Duplicate"):
        load_assets(path)
    with pytest.raises(ValueError):
        normalize_symbol("BTCUSD")
    assert normalize_symbol("xdg/usd") == "DOGE/USD"
    path.write_text(json.dumps({"assets": ["DOGE/USD", "XDG/USD"]}))
    with pytest.raises(ValueError, match="Duplicate"):
        load_assets(path)


def test_ticker_precision_and_time():
    price = parse_ticker({"channel": "ticker", "data": [TICK]})[0]
    assert price.last == Decimal("123.456789123")
    assert price.exchange_timestamp.tzinfo == UTC
    assert price.received_at.tzinfo == UTC
    assert parse_ticker({"channel": "heartbeat"}) == []
    without_time = dict(TICK)
    del without_time["timestamp"]
    assert parse_ticker({"channel": "ticker", "data": [without_time]})[0].exchange_timestamp is None


def test_rest_normalization_and_incomplete_candle(monkeypatch):
    async def scenario():
        async with KrakenClient() as client:
            calls = []

            async def request(endpoint, params=None):
                calls.append((endpoint, params))
                return {
                    "AssetPairs": PAIR,
                    "Ticker": {"XXBTZUSD": {"c": ["123.45"], "b": ["123"], "a": ["124"], "v": ["1", "10"]}},
                    "OHLC": {"XXBTZUSD": [[100, "1", "3", "1", "2", "2", "10", 2],
                                            [160, "2", "4", "2", "3", "3", "20", 3]], "last": 160},
                }[endpoint]

            monkeypatch.setattr(client, "_request", request)
            prices = await client.prices(["BTC/USD"])
            assert prices[0].last == Decimal("123.45")
            assert prices[0].exchange_timestamp is None
            batch = await client.candles("BTC/USD", since=100)
            assert [c.complete for c in batch.candles] == [True, False]
            assert batch.next_since == 160
            assert batch.candles[0].volume == Decimal("10")
            assert batch.candles[0].trades == 2
            assert calls[-1][1]["since"] == 100
            assert sum(e == "AssetPairs" for e, _ in calls) == 1
            with pytest.raises(KrakenError, match="Unknown"):
                await client.prices(["NOPE/USD"])
            with pytest.raises(ValueError):
                await client.candles("BTC/USD", interval=2)

    asyncio.run(scenario())


def test_retry_rate_limit_and_permanent_errors(monkeypatch):
    async def scenario():
        sleeps = []
        async def sleep(delay):
            sleeps.append(delay)

        class Response:
            headers = {"Retry-After": "9"}
            def __init__(self, status, payload):
                self.status, self.payload = status, payload
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            async def text(self): return json.dumps(self.payload)

        class Session:
            def __init__(self, responses): self.responses = iter(responses)
            def get(self, *args, **kwargs): return next(self.responses)

        monkeypatch.setattr(asyncio, "sleep", sleep)
        client = KrakenClient()
        client._session = Session([Response(429, {}), Response(200, {"error": [], "result": PAIR})])
        assert await client._request("AssetPairs") == PAIR
        assert 9 in sleeps
        client._session = Session([Response(200, {"error": ["EQuery:Unknown asset pair"]})])
        with pytest.raises(KrakenError, match="Unknown"):
            await client._request("Ticker")
        client.retries = 0
        client._session = Session([Response(503, {})])
        with pytest.raises(TransientError, match="1 attempts"):
            await client._request("Ticker")
        with pytest.raises(ValueError):
            await client._request("AddOrder")

    asyncio.run(scenario())


def test_websocket_reconnect_and_rejection(monkeypatch):
    async def scenario():
        connections = 0
        reject = False

        async def handler(request):
            nonlocal connections
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            subscription = await ws.receive_json()
            assert subscription["params"]["symbol"] == ["BTC/USD"]
            connections += 1
            if reject:
                await ws.send_json({"method": "subscribe", "success": False, "error": "invalid symbol"})
            elif connections == 1:
                await ws.close()
            else:
                await ws.send_json({"method": "subscribe", "success": True, "result": {"symbol": "BTC/USD"}})
                await ws.send_json({"channel": "heartbeat"})
                await ws.send_json({"channel": "ticker", "data": [TICK]})
            async for _ in ws:
                pass
            return ws

        app = web.Application()
        app.router.add_get("/ws", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        original_sleep = asyncio.sleep
        delays = []
        async def fast_sleep(delay):
            delays.append(delay)
            await original_sleep(0)
        monkeypatch.setattr(asyncio, "sleep", fast_sleep)
        try:
            async with KrakenClient(ws_url=f"http://127.0.0.1:{port}/ws") as client:
                client._pairs = {"BTC/USD": Pair("BTC/USD", "XXBTZUSD", "XBTUSD", "online")}
                stream = client.stream(["BTC/USD"])
                price = await asyncio.wait_for(anext(stream), 5)
                assert price.symbol == "BTC/USD"
                assert connections == 2
                assert delays[0] >= 5
                await stream.aclose()
                reject = True
                stream = client.stream(["BTC/USD"])
                with pytest.raises(KrakenError, match="Subscription rejected"):
                    await asyncio.wait_for(anext(stream), 5)
        finally:
            await runner.cleanup()

    asyncio.run(scenario())
