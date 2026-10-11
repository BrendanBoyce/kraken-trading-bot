import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import duckdb
import pytest

from kraken_bot.exchanges.kraken import TransientError
from kraken_bot.ingestion import collect_candles
from kraken_bot.models import Candle, CandleBatch
from kraken_bot.storage import CandleStore, MINUTE, missing_ranges

START = datetime(2026, 10, 11, 0, 0, tzinfo=UTC)


def candle(minute: int, **changes) -> Candle:
    return replace(Candle("BTC/USD", START + minute * MINUTE, 1,
                          Decimal("10.123456789123"), Decimal("12"), Decimal("9"),
                          Decimal("11"), Decimal("10"), Decimal("2"), 3, True), **changes)


def test_ingestion_deduplication_restart_and_utc(tmp_path):
    path = tmp_path / "market.duckdb"
    offset = timezone(timedelta(hours=1))
    rows = [candle(0, timestamp=START.astimezone(offset)), candle(1), candle(2, complete=False)]
    with CandleStore(path) as store:
        assert store.append(rows) == 2
        assert store.append(rows) == 0
        assert store.recent("BTC/USD", limit=2)[-1]["open"] == Decimal("10.123456789123")
        assert store.latest("BTC/USD") == START + MINUTE
        assert store.latest("BTC/USD").utcoffset() == timedelta(0)
    with CandleStore(path) as store:
        assert store.append([candle(1), candle(2), candle(2)]) == 1
        assert store.summary()[0]["candles"] == 3


@pytest.mark.parametrize("changes", [
    {"timestamp": START.replace(tzinfo=None)},
    {"timestamp": START + timedelta(seconds=1)},
    {"interval_minutes": 5},
    {"volume": Decimal("-1")},
    {"open": Decimal("NaN")},
    {"open": Decimal("0.0000000000000000001")},
    {"low": Decimal("20")},
])
def test_invalid_batch_does_not_partially_write(changes):
    with CandleStore(":memory:") as store:
        with pytest.raises(ValueError):
            store.append([candle(0), candle(1, **changes)])
        assert store.summary() == []


def test_complete_aligned_aggregation_and_parquet(tmp_path):
    with CandleStore(":memory:") as store:
        rows = [candle(i, open=Decimal("10"), close=Decimal("11"),
                       vwap=Decimal("11"), volume=Decimal(i + 1)) for i in range(60)]
        rows[0] = replace(rows[0], open=Decimal("9"), low=Decimal("8"))
        rows[-1] = replace(rows[-1], close=Decimal("13"), high=Decimal("14"))
        assert store.append(rows + [candle(60), candle(61)]) == 62
        hour = store.recent("BTC/USD", 60)[0]
        assert hour["timestamp"] == START
        assert (hour["open"], hour["high"], hour["low"], hour["close"]) == (
            Decimal("9"), Decimal("14"), Decimal("8"), Decimal("13"))
        assert hour["volume"] == Decimal(1830)
        assert hour["trades"] == 180
        assert hour["vwap"] == pytest.approx(11)
        assert len(store.recent("BTC/USD", 5, 100)) == 12
        path = tmp_path / "hour.parquet"
        assert store.export(path, 60) == 1
        with duckdb.connect() as connection:
            assert connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0] == 1
            row = connection.execute("SELECT timestamp, open FROM read_parquet(?)", [str(path)]).fetchone()
            assert row[0] == START
            assert row[1] == Decimal("9")
        with pytest.raises(FileExistsError):
            store.export(path, 60)


def test_aggregation_excludes_internal_holes_and_partial_buckets():
    with CandleStore(":memory:") as store:
        store.append([candle(i) for i in range(60) if i != 2])
        assert store.recent("BTC/USD", 60) == []
        assert len(store.recent("BTC/USD", 5, 100)) == 11
        store.append([candle(2)])
        assert len(store.recent("BTC/USD", 60)) == 1
        assert len(store.recent("BTC/USD", 5, 100)) == 12


def test_vwap_is_volume_weighted():
    with CandleStore(":memory:") as store:
        store.append([candle(i, vwap=Decimal(10 if i == 0 else 11),
                             volume=Decimal(4 if i == 0 else 1)) for i in range(5)])
        assert store.recent("BTC/USD", 5)[0]["vwap"] == pytest.approx(10.5)


def test_gap_detection_repair_and_retention_expiry(tmp_path):
    path = tmp_path / "market.duckdb"
    with CandleStore(path) as store:
        inserted, gaps = store.reconcile("BTC/USD", [candle(i) for i in (0, 1, 4)], now=START + 6 * MINUTE)
        assert inserted == 3
        assert len(gaps) == 1
        assert gaps[0].start == START + 2 * MINUTE
        assert gaps[0].end == START + 3 * MINUTE
        assert gaps[0].missing_minutes == 2
        assert gaps[0].status == "missing_from_response"
        _, gaps = store.reconcile("BTC/USD", [candle(i) for i in range(5)], now=START + 6 * MINUTE)
        assert gaps == []
    with CandleStore(path) as store:
        # Restart beyond the available source window: missing minutes 5..999.
        _, gaps = store.reconcile("BTC/USD", [candle(1000)], now=START + 1002 * MINUTE)
        assert len(gaps) == 1
        assert gaps[0].missing_minutes == 995
        assert gaps[0].status == "outside_kraken_window"
        # Subsequent reconciliation retains the old unfillable gap.
        _, gaps = store.reconcile("BTC/USD", [candle(1000), candle(1001)], now=START + 1003 * MINUTE)
        assert gaps == store.gap_records()
        assert gaps[0].missing_minutes == 995


def test_pending_tail_can_be_recovered_and_empty_responses():
    with CandleStore(":memory:") as store:
        assert store.reconcile("BTC/USD", [], now=START)[1] == []
        _, gaps = store.reconcile("BTC/USD", [candle(0)], now=START + 5 * MINUTE)
        assert gaps[0].status == "awaiting_source"
        assert gaps[0].missing_minutes == 3
        _, gaps = store.reconcile("BTC/USD", [], now=START + 6 * MINUTE)
        assert gaps[0].missing_minutes == 4
        _, gaps = store.reconcile("BTC/USD", [candle(i) for i in range(5)], now=START + 6 * MINUTE)
        assert gaps == []


def test_gap_start_before_first_observation_is_unknown():
    with CandleStore(":memory:") as store:
        _, gaps = store.reconcile("BTC/USD", [candle(100), candle(101)], now=START + 103 * MINUTE)
        assert gaps == []


def test_gap_expiry_keeps_old_prefix_and_repairs_new_suffix():
    with CandleStore(":memory:") as store:
        store.reconcile("BTC/USD", [candle(0), candle(10)], now=START + 12 * MINUTE)
        _, gaps = store.reconcile("BTC/USD", [candle(i) for i in range(5, 11)], now=START + 12 * MINUTE)
        assert len(gaps) == 1
        assert gaps[0].start == START + MINUTE
        assert gaps[0].end == START + 4 * MINUTE
        assert gaps[0].status == "outside_kraken_window"


def test_reconcile_only_inserts_new_rows(monkeypatch):
    with CandleStore(":memory:") as store:
        store.reconcile("BTC/USD", [candle(i) for i in range(10)], now=START + 11 * MINUTE)
        original_insert = store._insert_rows
        sizes = []
        def insert(rows):
            sizes.append(len(rows))
            return original_insert(rows)
        monkeypatch.setattr(store, "_insert_rows", insert)
        store.reconcile("BTC/USD", [candle(i) for i in range(11)], now=START + 12 * MINUTE)
        assert sizes == [1]


def test_large_gap_is_reported_as_one_range():
    assert missing_ranges(START, START + 1_000_000 * MINUTE, [START]) == [
        (START + MINUTE, START + 1_000_000 * MINUTE)]


def test_reconcile_rolls_back_candles_and_gaps_together(monkeypatch):
    with CandleStore(":memory:") as store:
        original_insert = store._insert_rows
        def broken_insert(rows):
            original_insert(rows)
            raise RuntimeError("simulated interrupted transaction")
        monkeypatch.setattr(store, "_insert_rows", broken_insert)
        with pytest.raises(RuntimeError):
            store.reconcile("BTC/USD", [candle(0)], now=START + 2 * MINUTE)
        assert store.summary() == []
        assert store.gap_records() == []


def test_collector_continues_after_disconnect_and_reports_partial_once(monkeypatch, caplog):
    async def scenario():
        class Client:
            attempts = 0
            async def candles(self, symbol, interval):
                self.attempts += 1
                if self.attempts == 1:
                    raise TransientError("connection lost")
                now = datetime.now(UTC).replace(second=0, microsecond=0)
                return CandleBatch((candle(0, timestamp=now - 2 * MINUTE),), 0)
        passes = 0
        async def sleep(_):
            nonlocal passes
            passes += 1
            if passes == 2:
                raise asyncio.CancelledError
        monkeypatch.setattr(asyncio, "sleep", sleep)
        with CandleStore(":memory:") as store:
            client = Client()
            with pytest.raises(asyncio.CancelledError):
                await collect_candles(client, store, ["BTC/USD"])
            assert client.attempts == 2
            assert store.summary()[0]["candles"] == 1
            assert "unavailable" in caplog.text
            with pytest.raises(TransientError, match="Ingestion incomplete"):
                await collect_candles(Client(), store, ["BTC/USD"], once=True)
    asyncio.run(scenario())
