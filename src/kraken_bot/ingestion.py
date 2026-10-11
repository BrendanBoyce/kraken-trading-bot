"""Rate-limited REST reconciliation, independent of transient price ticks."""

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from .exchanges.kraken import TransientError
from .interfaces import MarketData
from .storage import CandleStore

LOG = logging.getLogger(__name__)


async def collect_candles(client: MarketData, store: CandleStore, symbols: Sequence[str],
                          *, once: bool = False, poll_seconds: float = 60,
                          on_pass: Callable[[], None] | None = None) -> None:
    if poll_seconds < 60:
        raise ValueError("Candle polling period must be at least 60 seconds")
    failed: set[str] = set()
    while True:
        started = time.monotonic()
        failures_this_pass = []
        for symbol in symbols:
            try:
                # Re-fetch the bounded recent window to repair holes even before
                # the newest stored candle. Never advance a cursor past a gap.
                batch = await client.candles(symbol, interval=1)
            except TransientError as exc:
                failed.add(symbol)
                failures_this_pass.append(symbol)
                LOG.error("%s candle feed unavailable: %s; retrying next pass", symbol, exc)
                continue
            before = store.gap_records(symbol)
            inserted, gaps = store.reconcile(symbol, batch.candles, now=datetime.now(UTC))
            if symbol in failed:
                LOG.info("%s candle feed recovered; available history reconciled", symbol)
                failed.remove(symbol)
            LOG.info("%s appended %d completed 1m candles", symbol, inserted)
            if gaps != before:
                for gap in gaps:
                    LOG.warning("%s gap %s through %s UTC (%d minutes): %s", symbol,
                                gap.start.isoformat(), gap.end.isoformat(),
                                gap.missing_minutes, gap.status)
            if not batch.candles:
                LOG.warning("%s returned no candles; historical coverage remains unknown", symbol)
        if on_pass is not None:
            on_pass()
        if once:
            if failures_this_pass:
                raise TransientError(f"Ingestion incomplete for: {', '.join(failures_this_pass)}")
            return
        elapsed = time.monotonic() - started
        if elapsed > poll_seconds:
            LOG.warning("Candle collection pass took %.1fs; exceeding %.1fs poll period", elapsed, poll_seconds)
        # Failed long passes must not turn into a tight retry loop.
        await asyncio.sleep(max(1, poll_seconds - elapsed))
