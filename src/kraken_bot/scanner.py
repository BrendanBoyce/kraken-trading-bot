"""Market conditions only; no strategy, signal generation or order execution."""

import json
import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import indicators
from .config import normalize_symbol
from .storage import CandleStore


@dataclass(frozen=True)
class ScannerConfig:
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    sma_fast: int = 20
    sma_slow: int = 50
    ema_period: int = 20
    volatility_period: int = 20
    volume_period: int = 20
    lookback: int = 300
    stale_minutes: int = 10

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.volatility_period < 2:
            raise ValueError("volatility_period must be at least 2")
        if self.macd_fast >= self.macd_slow or self.sma_fast >= self.sma_slow:
            raise ValueError("Fast moving-average periods must be smaller than slow periods")
        minimum = max(self.rsi_period + 1, self.macd_slow + self.macd_signal - 1,
                      self.sma_slow, self.ema_period, self.volatility_period + 1,
                      self.volume_period + 1)
        if not minimum <= self.lookback <= 10000:
            raise ValueError(f"lookback must be between {minimum} and 10000")


def load_scanner_config(path: Path | None) -> ScannerConfig:
    if path is None:
        return ScannerConfig()
    try:
        return ScannerConfig(**json.loads(path.read_text()))
    except TypeError as exc:
        raise ValueError(f"Invalid scanner settings: {exc}") from exc


@dataclass(frozen=True)
class ScanRow:
    symbol: str
    candle_start_utc: datetime | None
    contiguous_candles: int
    close: float | None
    rsi: float | None
    macd: float | None
    macd_signal: float | None
    macd_histogram: float | None
    sma_fast: float | None
    sma_slow: float | None
    ema: float | None
    volatility_pct: float | None
    volume_change_pct: float | None
    flags: tuple[str, ...]


def calculate(symbol: str, rows: Sequence[dict], config: ScannerConfig,
              *, now: datetime, known_gap: bool = False) -> ScanRow:
    """Use only the latest contiguous 5m suffix; never bridge missing buckets."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Scanner time must be timezone-aware")
    now = now.astimezone(UTC)
    flags = []
    ordered = sorted(rows, key=lambda row: row["timestamp"])
    closed = [row for row in ordered if row["timestamp"] + timedelta(minutes=5) <= now]
    if len(closed) < len(ordered):
        flags.append("future_data_excluded")
    start = 0
    for index in range(1, len(closed)):
        if closed[index]["timestamp"] - closed[index - 1]["timestamp"] != timedelta(minutes=5):
            flags.append("gap_in_lookback")
            start = index
    if known_gap and "gap_in_lookback" not in flags:
        flags.append("gap_in_lookback")
    rows = closed[start:]
    if not rows:
        return ScanRow(normalize_symbol(symbol), None, 0, *(None for _ in range(10)),
                       tuple([*flags, "no_data"]))
    latest = rows[-1]["timestamp"]
    if now - (latest + timedelta(minutes=5)) > timedelta(minutes=config.stale_minutes):
        flags.append("stale")
    closes = [float(row["close"]) for row in rows]
    volumes = [float(row["volume"]) for row in rows]
    if any(not math.isfinite(value) or value <= 0 for value in closes) or any(
        not math.isfinite(value) or value < 0 for value in volumes
    ):
        return ScanRow(normalize_symbol(symbol), latest, len(rows), *(None for _ in range(10)),
                       tuple([*flags, "invalid_data"]))
    line, signal, histogram = indicators.macd(closes, config.macd_fast, config.macd_slow, config.macd_signal)
    values = (
        closes[-1], indicators.rsi(closes, config.rsi_period), line, signal, histogram,
        indicators.sma(closes, config.sma_fast), indicators.sma(closes, config.sma_slow),
        indicators.ema_series(closes, config.ema_period)[-1],
        indicators.volatility(closes, config.volatility_period),
        indicators.volume_change(volumes, config.volume_period),
    )
    if any(value is None for value in values[1:-1]) or len(rows) <= config.volume_period:
        flags.append("insufficient_history")
    if len(rows) > config.volume_period and all(volume == 0 for volume in volumes[-config.volume_period - 1:-1]):
        flags.append("zero_volume_baseline")
    return ScanRow(normalize_symbol(symbol), latest, len(rows), *values, tuple(flags))


def scan(store: CandleStore, symbols: Sequence[str], config: ScannerConfig,
         *, now: datetime | None = None) -> list[ScanRow]:
    now = now or datetime.now(UTC)
    result = []
    for symbol in symbols:
        rows = store.recent(symbol, interval=5, limit=config.lookback)
        first = min((row["timestamp"] for row in rows), default=now)
        gap = any(record.end >= first and record.start < now for record in store.gap_records(symbol))
        result.append(calculate(symbol, rows, config, now=now, known_gap=gap))
    return result


class ScanReporter:
    """Emit once, then when a candle or quality status changes, not every poll."""

    def __init__(self, config: ScannerConfig, *, json_output: bool = False):
        self.config = config
        self.json_output = json_output
        self._previous: tuple | None = None

    def report(self, rows: Sequence[ScanRow]) -> None:
        signature = tuple((row.symbol, row.candle_start_utc, row.contiguous_candles, row.flags) for row in rows)
        if signature == self._previous:
            return
        self._previous = signature
        if self.json_output:
            for row in rows:
                print(json.dumps(asdict(row), default=str, allow_nan=False), flush=True)
            return
        print(f"\nScan {datetime.now(UTC).isoformat()} | completed 5m candles", flush=True)
        headers = ["PAIR", "CANDLE UTC", "CLOSE", f"RSI{self.config.rsi_period}", "MACD", "HIST",
                   f"SMA{self.config.sma_fast}", f"SMA{self.config.sma_slow}",
                   f"EMA{self.config.ema_period}", "VOL%", "VOLUMEΔ%", "STATUS"]
        print(" | ".join(headers), flush=True)
        for row in rows:
            stamp = row.candle_start_utc.strftime("%m-%d %H:%M") if row.candle_start_utc else "—"
            values = (row.close, row.rsi, row.macd, row.macd_histogram, row.sma_fast, row.sma_slow,
                      row.ema, row.volatility_pct, row.volume_change_pct)
            numbers = [f"{value:.6g}" if value is not None else "—" for value in values]
            print(" | ".join([row.symbol, stamp, *numbers, ",".join(row.flags) or "ok"]), flush=True)
