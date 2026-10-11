"""Deterministic indicators over chronological, contiguous close prices."""

import math
import statistics
from collections.abc import Sequence


def sma(values: Sequence[float], period: int) -> float | None:
    return statistics.fmean(values[-period:]) if len(values) >= period else None


def ema_series(values: Sequence[float], period: int) -> list[float | None]:
    """Seed with the first period's SMA; alpha = 2 / (period + 1)."""
    result: list[float | None] = [None] * len(values)
    if len(values) < period:
        return result
    average = statistics.fmean(values[:period])
    result[period - 1] = average
    alpha = 2 / (period + 1)
    for index in range(period, len(values)):
        average += alpha * (values[index] - average)
        result[index] = average
    return result


def rsi(values: Sequence[float], period: int) -> float | None:
    """Wilder RSI: SMA seed followed by alpha = 1 / period smoothing."""
    if len(values) <= period:
        return None
    changes = [b - a for a, b in zip(values, values[1:])]
    gain = statistics.fmean(max(change, 0) for change in changes[:period])
    loss = statistics.fmean(max(-change, 0) for change in changes[:period])
    for change in changes[period:]:
        gain = (gain * (period - 1) + max(change, 0)) / period
        loss = (loss * (period - 1) + max(-change, 0)) / period
    if gain == loss == 0:
        return 50.0
    if loss == 0:
        return 100.0
    return 100 - 100 / (1 + gain / loss)


def macd(values: Sequence[float], fast: int, slow: int,
         signal: int) -> tuple[float | None, float | None, float | None]:
    fast_ema, slow_ema = ema_series(values, fast), ema_series(values, slow)
    line = [a - b for a, b in zip(fast_ema, slow_ema) if a is not None and b is not None]
    if not line:
        return None, None, None
    signal_value = ema_series(line, signal)[-1]
    return line[-1], signal_value, line[-1] - signal_value if signal_value is not None else None


def volatility(values: Sequence[float], period: int) -> float | None:
    """Sample standard deviation of period log returns, percent per candle."""
    if len(values) <= period:
        return None
    tail = values[-period - 1:]
    return statistics.stdev(math.log(b / a) for a, b in zip(tail, tail[1:])) * 100


def volume_change(values: Sequence[float], period: int) -> float | None:
    """Latest volume vs mean of the preceding period candles, in percent."""
    if len(values) <= period:
        return None
    baseline = statistics.fmean(values[-period - 1:-1])
    return (values[-1] / baseline - 1) * 100 if baseline else None
