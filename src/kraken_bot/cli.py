"""Terminal commands for public market data; no permanent tick storage."""

import argparse
import asyncio
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any

import duckdb

from .config import load_assets, normalize_symbol
from .exchanges.kraken import KrakenClient, KrakenError
from .ingestion import collect_candles
from .storage import CandleStore


def emit(value: Any) -> None:
    print(json.dumps(asdict(value), default=str), flush=True)


async def run(args: argparse.Namespace) -> None:
    assets = load_assets(args.config)
    if args.command in {"db-summary", "db-gaps", "db-show", "export"}:
        with CandleStore(args.db, read_only=True) as store:
            if args.command == "db-summary":
                for row in store.summary():
                    print(json.dumps(row, default=str))
            elif args.command == "db-gaps":
                gaps = store.gap_records()
                for gap in gaps:
                    print(json.dumps({**asdict(gap), "missing_minutes": gap.missing_minutes}, default=str))
                if not gaps:
                    print(json.dumps({"gaps": 0}))
            elif args.command == "db-show":
                for row in store.recent(args.symbol, args.interval, args.limit):
                    print(json.dumps(row, default=str))
            else:
                count = store.export(args.output, args.interval, args.symbol)
                print(json.dumps({"exported_rows": count, "path": str(args.output)}))
        return
    async with KrakenClient() as client:
        if args.command == "ingest":
            with CandleStore(args.db) as store:
                await collect_candles(client, store, assets, once=args.once, poll_seconds=args.poll_seconds)
        elif args.command == "pairs":
            for pair in (await client.pairs()).values():
                emit(pair)
        elif args.command == "prices":
            for price in await client.prices(assets):
                emit(price)
        elif args.command == "ohlc":
            symbol = normalize_symbol(args.symbol)
            if symbol not in assets:
                raise ValueError(f"{symbol} is not in the configured allowlist")
            batch = await client.candles(symbol, args.interval, args.since)
            for candle in batch.candles:
                emit(candle)
            print(json.dumps({"next_since": batch.next_since}), flush=True)
        else:
            count = 0
            async for price in client.stream(assets):
                emit(price)
                count += 1
                if args.max_updates and count >= args.max_updates:
                    break


def main() -> None:
    parser = argparse.ArgumentParser(description="Public Kraken market-data collector")
    parser.add_argument("--config", type=Path, help="JSON asset allowlist; defaults to BTC, ETH, SOL / USD")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("pairs", help="Discover Kraken Spot pairs (not Funded eligibility)")
    commands.add_parser("prices", help="Fetch current prices for the allowlist")
    live = commands.add_parser("live", help="Stream timestamped prices; Ctrl-C stops")
    live.add_argument("--max-updates", type=int, default=0, help="Stop after N records; 0 means unlimited")
    ohlc = commands.add_parser("ohlc", help="Retrieve Kraken's limited recent candle window")
    ohlc.add_argument("symbol")
    ohlc.add_argument("--interval", type=int, default=1)
    ohlc.add_argument("--since", type=int)
    database = argparse.ArgumentParser(add_help=False)
    database.add_argument("--db", type=Path, default=Path("data/market.duckdb"))
    ingest = commands.add_parser("ingest", parents=[database], help="Store completed 1m candles continuously")
    ingest.add_argument("--once", action="store_true", help="Reconcile available history and stop")
    ingest.add_argument("--poll-seconds", type=float, default=60)
    commands.add_parser("db-summary", parents=[database], help="Show stored 1m history coverage")
    commands.add_parser("db-gaps", parents=[database], help="Show detected missing candle ranges")
    show = commands.add_parser("db-show", parents=[database], help="Inspect stored or aggregated candles")
    show.add_argument("symbol")
    show.add_argument("--interval", type=int, choices=[1, 5, 60], default=1)
    show.add_argument("--limit", type=int, default=10)
    export = commands.add_parser("export", parents=[database], help="Export candles to compressed Parquet")
    export.add_argument("output", type=Path)
    export.add_argument("--interval", type=int, choices=[1, 5, 60], default=1)
    export.add_argument("--symbol")
    args = parser.parse_args()
    if getattr(args, "max_updates", 0) < 0:
        parser.error("--max-updates must be nonnegative")
    if getattr(args, "poll_seconds", 60) < 60:
        parser.error("--poll-seconds must be at least 60")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except (KrakenError, ValueError, KeyError, OSError, duckdb.Error) as exc:
        parser.exit(1, f"Error: {exc}\n")


if __name__ == "__main__":
    main()
