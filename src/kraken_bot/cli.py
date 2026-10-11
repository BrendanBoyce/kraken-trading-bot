"""Terminal commands for public market data; no permanent tick storage."""

import argparse
import asyncio
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .config import load_assets, normalize_symbol
from .exchanges.kraken import KrakenClient, KrakenError


def emit(value: Any) -> None:
    print(json.dumps(asdict(value), default=str), flush=True)


async def run(args: argparse.Namespace) -> None:
    assets = load_assets(args.config)
    async with KrakenClient() as client:
        if args.command == "pairs":
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
    args = parser.parse_args()
    if getattr(args, "max_updates", 0) < 0:
        parser.error("--max-updates must be nonnegative")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except (KrakenError, ValueError, KeyError, OSError) as exc:
        parser.exit(1, f"Error: {exc}\n")


if __name__ == "__main__":
    main()
