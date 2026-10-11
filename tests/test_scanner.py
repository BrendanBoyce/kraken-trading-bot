import asyncio
import json
import math
import statistics
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kraken_bot import indicators
from kraken_bot.ingestion import collect_candles
from kraken_bot.models import Candle, CandleBatch
from kraken_bot.scanner import ScannerConfig, ScanReporter, calculate, load_scanner_config, scan
from kraken_bot.storage import CandleStore

START = datetime(2026, 10, 11, tzinfo=UTC)
SETTINGS = ScannerConfig()


def rows(count=100):
    return [{"timestamp": START + timedelta(minutes=5 * i), "close": 100 + i,
             "volume": 10} for i in range(count)]


def test_known_sma_ema_macd_and_warmup():
    assert indicators.sma([1, 2, 3, 4], 3) == 3
    assert indicators.sma([1], 3) is None
    assert indicators.ema_series([1, 2, 3, 4, 5], 3) == [None, None, 2, 3, 4]
    assert indicators.macd(list(range(1, 41)), 12, 26, 9) == pytest.approx((7, 7, 0))
    assert indicators.macd([1] * 25, 12, 26, 9) == (None, None, None)
    line, signal, histogram = indicators.macd([1] * 26, 12, 26, 9)
    assert line == 0 and signal is histogram is None


def test_wilder_rsi_published_example_and_edge_cases():
    # Wilder-style example: first RSI after 14 price changes is about 70.4641.
    prices = [44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42,
              45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28, 46.00]
    assert indicators.rsi(prices[:15], 14) == pytest.approx(70.464135, abs=1e-6)
    assert indicators.rsi(prices, 14) == pytest.approx(66.249619, abs=1e-6)
    assert indicators.rsi([1] * 15, 14) == 50
    assert indicators.rsi(list(range(1, 16)), 14) == 100
    assert indicators.rsi(list(range(16, 1, -1)), 14) == 0
    assert indicators.rsi([1] * 14, 14) is None


def test_volatility_and_volume_baseline():
    closes = [100, 110, 99]
    expected = statistics.stdev([math.log(1.1), math.log(0.9)]) * 100
    assert indicators.volatility(closes, 2) == pytest.approx(expected)
    assert indicators.volatility([100], 2) is None
    assert indicators.volume_change([10, 10, 20], 2) == 100
    assert indicators.volume_change([0, 0, 20], 2) is None


def test_calculation_uses_contiguous_suffix_and_reports_quality():
    data = rows(100)
    result = calculate("BTC/USD", data, SETTINGS, now=START + timedelta(minutes=500))
    assert result.flags == ()
    assert result.sma_slow == statistics.fmean(range(150, 200))
    assert result.rsi == 100
    result = calculate("BTC/USD", data, SETTINGS, now=START + timedelta(minutes=511))
    assert "stale" in result.flags
    del data[90]
    result = calculate("BTC/USD", data, SETTINGS, now=START + timedelta(minutes=500))
    assert result.contiguous_candles == 9
    assert result.sma_fast is None and result.macd is None
    assert {"gap_in_lookback", "insufficient_history"} <= set(result.flags)


def test_no_data_invalid_future_and_zero_volume():
    now = START + timedelta(minutes=500)
    assert calculate("BTC/USD", [], SETTINGS, now=now).flags == ("no_data",)
    data = rows()
    data[-1]["close"] = 0
    assert "invalid_data" in calculate("BTC/USD", data, SETTINGS, now=now).flags
    data = rows()
    data.append({"timestamp": now, "close": 999999, "volume": 10})
    result = calculate("BTC/USD", data, SETTINGS, now=now)
    assert result.close == 199
    assert "future_data_excluded" in result.flags
    data = rows()
    for row in data:
        row["volume"] = 0
    result = calculate("BTC/USD", data, SETTINGS, now=now)
    assert result.volume_change_pct is None
    assert result.flags == ("zero_volume_baseline",)


def test_config_validation(tmp_path):
    path = tmp_path / "settings.json"
    path.write_text(json.dumps({"rsi_period": 7}))
    assert load_scanner_config(path).rsi_period == 7
    for changes in ({"rsi_period": 0}, {"rsi_period": True}, {"macd_fast": 26},
                    {"volatility_period": 1}, {"lookback": 10}):
        with pytest.raises(ValueError):
            ScannerConfig(**changes)
    path.write_text(json.dumps({"typo": 7}))
    with pytest.raises(ValueError, match="Invalid scanner"):
        load_scanner_config(path)


def test_database_scanning_allowlist_and_gap(tmp_path):
    with CandleStore(tmp_path / "test.duckdb") as store:
        candles = [Candle("BTC/USD", START + timedelta(minutes=i), 1,
                          Decimal(10), Decimal(12), Decimal(9), Decimal(11),
                          Decimal(10), Decimal(2), 1, True) for i in range(500)]
        store.append(candles)
        now = START + timedelta(minutes=500)
        result = scan(store, ["BTC/USD", "ETH/USD"], SETTINGS, now=now)
        assert result[0].contiguous_candles == 100 and result[0].flags == ()
        assert result[1].flags == ("no_data",)
        assert result[0].macd == result[0].macd_histogram == 0
        assert result[0].volatility_pct == 0
        # Removing one minute excludes its entire 5m bucket; indicators cannot bridge it.
        store.connection.execute("DELETE FROM candles WHERE timestamp=?", [START + timedelta(minutes=490)])
        result = scan(store, ["BTC/USD"], SETTINGS, now=now)[0]
        assert result.contiguous_candles == 1
        assert "gap_in_lookback" in result.flags


def test_reporter_emits_only_candle_or_quality_changes(capsys):
    reporter = ScanReporter(SETTINGS, json_output=True)
    result = calculate("BTC/USD", rows(), SETTINGS, now=START + timedelta(minutes=500))
    reporter.report([result])
    assert json.loads(capsys.readouterr().out)["symbol"] == "BTC/USD"
    reporter.report([result])
    assert capsys.readouterr().out == ""
    reporter.report([replace(result, flags=("stale",))])
    assert json.loads(capsys.readouterr().out)["flags"] == ["stale"]


def test_live_scanner_callback_uses_same_connection():
    async def scenario():
        now = datetime.now(UTC).replace(second=0, microsecond=0)
        class Client:
            async def candles(self, symbol, interval):
                return CandleBatch(tuple(Candle(symbol, now - timedelta(minutes=i), 1,
                    Decimal(10), Decimal(12), Decimal(9), Decimal(11), Decimal(10),
                    Decimal(2), 1, True) for i in range(1, 301)), 0)
        with CandleStore(":memory:") as store:
            results = []
            await collect_candles(Client(), store, ["BTC/USD"], once=True,
                                  on_pass=lambda: results.extend(scan(store, ["BTC/USD"], SETTINGS)))
            assert len(results) == 1
            assert results[0].sma_slow == 11 and not results[0].flags
    asyncio.run(scenario())
