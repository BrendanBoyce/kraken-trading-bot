"""Single-writer DuckDB candle store with UTC timestamps and gap-aware views."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb

from .config import normalize_symbol
from .models import Candle

MINUTE = timedelta(minutes=1)


@dataclass(frozen=True)
class Gap:
    symbol: str
    start: datetime
    end: datetime  # Inclusive timestamp of the last missing candle.
    status: str

    @property
    def missing_minutes(self) -> int:
        return int((self.end - self.start).total_seconds() // 60) + 1


def missing_ranges(start: datetime, end: datetime,
                   timestamps: Sequence[datetime]) -> list[tuple[datetime, datetime]]:
    """Find absent minutes without materializing a minute for each gap."""
    cursor = start
    ranges = []
    for stamp in sorted(set(timestamps)):
        if stamp < cursor or stamp > end:
            continue
        if stamp > cursor:
            ranges.append((cursor, stamp - MINUTE))
        cursor = stamp + MINUTE
    if cursor <= end:
        ranges.append((cursor, end))
    return ranges


class CandleStore:
    """One connection per collector. Stop it before opening another process."""

    def __init__(self, path: Path | str = "data/market.duckdb", *, read_only: bool = False):
        if str(path) != ":memory:" and not read_only:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(str(path), read_only=read_only)
        self.connection.execute("SET TimeZone='UTC'")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='512MB'")
        if not read_only:
            self._schema()

    def __enter__(self) -> "CandleStore":
        return self

    def __exit__(self, *args) -> None:
        self.connection.close()

    def _schema(self) -> None:
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                symbol VARCHAR NOT NULL,
                interval_minutes INTEGER NOT NULL CHECK(interval_minutes = 1),
                timestamp TIMESTAMPTZ NOT NULL,
                open DECIMAL(38,18) NOT NULL, high DECIMAL(38,18) NOT NULL,
                low DECIMAL(38,18) NOT NULL, close DECIMAL(38,18) NOT NULL,
                vwap DECIMAL(38,18), volume DECIMAL(38,18) NOT NULL,
                trades BIGINT NOT NULL,
                PRIMARY KEY(symbol, interval_minutes, timestamp)
            );
            CREATE TABLE IF NOT EXISTS gaps (
                symbol VARCHAR NOT NULL, start TIMESTAMPTZ NOT NULL,
                "end" TIMESTAMPTZ NOT NULL, status VARCHAR NOT NULL,
                PRIMARY KEY(symbol, start)
            );
        """)
        # Older databases required VWAP; official historical archives omit it.
        self.connection.execute("DROP VIEW IF EXISTS candles_5m")
        self.connection.execute("DROP VIEW IF EXISTS candles_60m")
        self.connection.execute("ALTER TABLE candles ALTER COLUMN vwap DROP NOT NULL")
        for minutes, duration in ((5, "5 minutes"), (60, "1 hour")):
            # A count of N aligned, unique minute timestamps proves full coverage.
            self.connection.execute(f"""
                CREATE OR REPLACE VIEW candles_{minutes}m AS
                SELECT symbol, {minutes} AS interval_minutes,
                    time_bucket(INTERVAL '{duration}', timestamp) AS timestamp,
                    arg_min(open, timestamp) AS open, max(high) AS high,
                    min(low) AS low, arg_max(close, timestamp) AS close,
                    CASE WHEN count(*) FILTER (WHERE vwap IS NULL AND volume > 0) > 0 THEN NULL
                    WHEN sum(volume) = 0 THEN 0 ELSE
                        sum(CAST(vwap AS DOUBLE) * CAST(volume AS DOUBLE)) /
                        sum(CAST(volume AS DOUBLE)) END AS vwap,
                    sum(volume) AS volume, sum(trades) AS trades
                FROM candles
                GROUP BY symbol, time_bucket(INTERVAL '{duration}', timestamp)
                HAVING count(*) = {minutes}
            """)

    @staticmethod
    def _row(candle: Candle) -> tuple:
        if candle.interval_minutes != 1:
            raise ValueError("Only one-minute source candles may be ingested")
        stamp = candle.timestamp
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise ValueError("Candle timestamps must be timezone-aware")
        stamp = stamp.astimezone(UTC)
        if stamp.second or stamp.microsecond:
            raise ValueError("Candle timestamps must align to minute boundaries")
        values = (candle.open, candle.high, candle.low, candle.close, candle.vwap, candle.volume)
        for index, value in enumerate(values):
            if index == 4 and value is None:
                continue
            if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
                raise ValueError("Candle prices and volumes must be finite nonnegative Decimals")
            digits = list(value.as_tuple().digits)
            exponent = value.as_tuple().exponent
            while digits and digits[-1] == 0:
                digits.pop()
                exponent += 1
            if value != 0 and (exponent < -18 or value.adjusted() >= 20):
                raise ValueError("Candle value exceeds exact DECIMAL(38,18) storage")
        if candle.low > min(candle.open, candle.close) or candle.high < max(candle.open, candle.close):
            raise ValueError("Invalid OHLC price bounds")
        if candle.trades < 0:
            raise ValueError("Trade count cannot be negative")
        return (normalize_symbol(candle.symbol), 1, stamp, *values, candle.trades)

    def append(self, candles: Sequence[Candle]) -> int:
        """Atomically insert completed records; repeated keys are ignored."""
        rows = [self._row(candle) for candle in candles if candle.complete]
        if not rows:
            return 0
        self.connection.execute("BEGIN")
        try:
            inserted = self._insert_rows(rows)
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        return inserted

    def _insert_rows(self, rows: list[tuple]) -> int:
        # A bounded batch avoids slow per-row transactions and optional dataframe deps.
        inserted = 0
        for offset in range(0, len(rows), 1000):
            chunk = rows[offset:offset + 1000]
            placeholders = ",".join("(" + ",".join("?" for _ in row) + ")" for row in chunk)
            result = self.connection.execute(
                f"INSERT INTO candles VALUES {placeholders} ON CONFLICT DO NOTHING RETURNING symbol",
                [value for row in chunk for value in row],
            ).fetchall()
            inserted += len(result)
        return inserted

    def latest(self, symbol: str) -> datetime | None:
        return self.connection.execute(
            "SELECT max(timestamp) FROM candles WHERE symbol=?", [normalize_symbol(symbol)],
        ).fetchone()[0]

    def gap_records(self, symbol: str | None = None) -> list[Gap]:
        query = 'SELECT symbol, start, "end", status FROM gaps'
        params = []
        if symbol is not None:
            query += " WHERE symbol=?"
            params = [normalize_symbol(symbol)]
        return [Gap(*row) for row in self.connection.execute(
            query + " ORDER BY symbol, start", params,
        ).fetchall()]

    def reconcile(self, symbol: str, candles: Sequence[Candle], *, now: datetime) -> tuple[int, list[Gap]]:
        """Reconcile a full REST window and track absent candles, including outages.

        Before the initial oldest returned candle, coverage is unknown. Missing
        candles older than the full response's oldest row cannot be recovered by
        Kraken's OHLC endpoint. Within-window absences may also be no-trade minutes;
        no synthetic candles are inserted.
        """
        symbol = normalize_symbol(symbol)
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        rows = [c for c in candles if c.complete]
        if any(normalize_symbol(c.symbol) != symbol for c in candles):
            raise ValueError("Mixed trading pairs in ingestion batch")
        previous = self.latest(symbol)
        # Validate everything before writes and normalize incoming timezone offsets.
        validated = [self._row(c) for c in rows]
        stamps = [row[2] for row in validated]
        first = min(stamps) if stamps else None
        old_gaps = self.gap_records(symbol)
        if first is None and previous is None:
            return 0, old_gaps
        start = min(value for value in (first, previous + MINUTE if previous else None) if value is not None)
        # Allow one additional minute for exchange publication / clock skew.
        expected_end = now.astimezone(UTC).replace(second=0, microsecond=0) - 2 * MINUTE
        end = max([expected_end, *stamps, previous or start])
        stored = [row[0] for row in self.connection.execute(
            "SELECT timestamp FROM candles WHERE symbol=? AND timestamp BETWEEN ? AND ? ORDER BY timestamp",
            [symbol, start, end],
        ).fetchall()]
        existing_stamps = set(stored)
        # Most of a rolling history window is already stored. Avoid preparing an
        # INSERT for hundreds of overlapping rows on every minute's poll.
        validated = [row for row in validated if row[2] not in existing_stamps]
        stored.extend(stamps)
        gaps = []
        # Preserve the part of old gaps outside the reconciled range, including
        # long outages, without expanding them into individual missing minutes.
        for gap in old_gaps:
            if gap.start < start:
                gaps.append(Gap(symbol, gap.start, min(gap.end, start - MINUTE), gap.status))
            if gap.end > end:
                gaps.append(Gap(symbol, max(gap.start, end + MINUTE), gap.end, gap.status))
        last_source = max(stamps) if stamps else None
        for left, right in missing_ranges(start, end, stored):
            cursor = left
            if first is not None and cursor < first:
                stop = min(right, first - MINUTE)
                gaps.append(Gap(symbol, cursor, stop, "outside_kraken_window"))
                cursor = stop + MINUTE
            if last_source is not None and cursor <= min(right, last_source):
                stop = min(right, last_source)
                gaps.append(Gap(symbol, cursor, stop, "missing_from_response"))
                cursor = stop + MINUTE
            if cursor <= right:
                gaps.append(Gap(symbol, cursor, right, "awaiting_source"))
        # Old pending ranges expire as the source window moves forward.
        classified = []
        for gap in gaps:
            if first is not None and gap.start < first:
                stop = min(gap.end, first - MINUTE)
                classified.append(Gap(symbol, gap.start, stop, "outside_kraken_window"))
                if gap.end > stop:
                    classified.append(Gap(symbol, stop + MINUTE, gap.end, gap.status))
            else:
                classified.append(gap)
        merged: list[Gap] = []
        for gap in sorted(classified, key=lambda item: item.start):
            if merged and merged[-1].end + MINUTE == gap.start and merged[-1].status == gap.status:
                previous_gap = merged.pop()
                gap = Gap(symbol, previous_gap.start, gap.end, gap.status)
            merged.append(gap)
        self.connection.execute("BEGIN")
        try:
            inserted = self._insert_rows(validated)
            self.connection.execute("DELETE FROM gaps WHERE symbol=?", [symbol])
            if merged:
                self.connection.executemany('INSERT INTO gaps VALUES (?, ?, ?, ?)',
                                            [(g.symbol, g.start, g.end, g.status) for g in merged])
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        return inserted, merged

    @staticmethod
    def table(interval: int) -> str:
        if interval not in (1, 5, 60):
            raise ValueError("Stored/derived intervals are 1, 5 or 60 minutes")
        return "candles" if interval == 1 else f"candles_{interval}m"

    def summary(self) -> list[dict]:
        rows = self.connection.execute("""
            SELECT symbol, count(*), min(timestamp), max(timestamp)
            FROM candles GROUP BY symbol ORDER BY symbol
        """).fetchall()
        return [{"symbol": symbol, "candles": count, "first_utc": first, "last_utc": last,
                 "stored_minutes": count, "span_minutes": int((last - first).total_seconds() // 60) + 1}
                for symbol, count, first, last in rows]

    def recent(self, symbol: str, interval: int = 1, limit: int = 10) -> list[dict]:
        if limit < 1:
            raise ValueError("limit must be positive")
        cursor = self.connection.execute(
            f"SELECT * FROM {self.table(interval)} WHERE symbol=? ORDER BY timestamp DESC LIMIT ?",
            [normalize_symbol(symbol), limit],
        )
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    def export(self, path: Path, interval: int = 1, symbol: str | None = None) -> int:
        """Export a consistent snapshot; never overwrite an existing artifact."""
        table = self.table(interval)
        if path.exists():
            raise FileExistsError(f"Export already exists: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        query = f"SELECT * FROM {table}"
        params = []
        if symbol:
            query += " WHERE symbol=?"
            params.append(normalize_symbol(symbol))
        relation = self.connection.sql(query + " ORDER BY symbol, timestamp", params=params)
        count = relation.count("*").fetchone()[0]
        relation.write_parquet(str(path), compression="zstd")
        return count
