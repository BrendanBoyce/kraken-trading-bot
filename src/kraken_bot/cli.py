"""Terminal commands for public market data; no permanent tick storage."""

import argparse
import asyncio
import json
import logging
import zipfile
from datetime import UTC, datetime
from dataclasses import asdict
from pathlib import Path
from typing import Any

import duckdb

from .config import load_assets, normalize_symbol
from .exchanges.kraken import KrakenClient, KrakenError
from .ingestion import collect_candles
from .storage import CandleStore
from .scanner import ScanReporter, load_scanner_config, scan


def emit(value: Any) -> None:
    print(json.dumps(asdict(value), default=str), flush=True)


async def run(args: argparse.Namespace) -> None:
    assets = load_assets(args.config)
    if args.command == "research-rallies":
        from .rally_study import rally_study
        with CandleStore(":memory:") as store:
            report = rally_study(store, assets, args.archive, args.output,
                datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC),
                datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC),
                tuple(args.horizons), tuple(args.cost_bps))
        print(json.dumps({"report": str(args.output), "experiments": [
            {"horizon_minutes": e["horizon_minutes"], "test_baseline": e["chronological"]["test_baseline"],
             "selected_test": e["chronological"]["selected_test"]} for e in report["experiments"]]}, indent=2))
        return
    if args.command == "research-walk-forward":
        from .walk_forward import walk_forward
        # Archive research is independent of the live collector's database lock.
        with CandleStore(":memory:") as store:
            report = walk_forward(store, assets, args.archive, args.output,
                datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC),
                datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC),
                args.horizon, args.train_days, args.test_days)
        print(json.dumps({"report": str(args.output), "aggregate": report["aggregate"]}, indent=2))
        return
    if args.command == "research":
        from .research import research
        with CandleStore(args.db, read_only=True) as store:
            report = research(store, assets, args.output,
                datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC),
                datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC), args.horizon)
        print(json.dumps({"report": str(args.output), "train_observations": report["train_observations"], "test": report["test"]}, indent=2))
        return
    if args.command == "import-history":
        from .history import import_archive
        with CandleStore(args.db) as store:
            import_archive(store, args.archive, assets, args.report)
        return
    if args.command == "scan":
        settings = load_scanner_config(args.scanner_config)
        reporter = ScanReporter(settings, json_output=args.json)
        with CandleStore(args.db, read_only=not args.live) as store:
            if args.live:
                async with KrakenClient() as client:
                    await collect_candles(client, store, assets,
                                          on_pass=lambda: reporter.report(scan(store, assets, settings)))
            else:
                reporter.report(scan(store, assets, settings))
        return
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
    rallies = commands.add_parser("research-rallies", help="Study rally events, controls, downside and chronological hypotheses")
    rallies.add_argument("--archive", type=Path, default=Path("data/archives/Kraken_OHLCVT_2026Q2.zip"))
    rallies.add_argument("--start", default="2026-04-01")
    rallies.add_argument("--end", default="2026-07-01")
    rallies.add_argument("--horizons", type=int, nargs="+", default=[240,720,1440], help="Minutes; first horizon is primary")
    rallies.add_argument("--cost-bps", type=float, nargs="+", default=[0,50,100], help="Assumed total cost haircuts in basis points")
    rallies.add_argument("--output", type=Path, default=Path("data/rally-study-report.json"))
    walk = commands.add_parser("research-walk-forward", help="Evaluate archive baseline over chronological periods")
    walk.add_argument("--archive", type=Path, default=Path("data/archives/Kraken_OHLCVT_2026Q2.zip"))
    walk.add_argument("--start", default="2026-04-01")
    walk.add_argument("--end", default="2026-07-01")
    walk.add_argument("--horizon", type=int, default=60)
    walk.add_argument("--train-days", type=int, default=28)
    walk.add_argument("--test-days", type=int, default=7)
    walk.add_argument("--output", type=Path, default=Path("data/walk-forward-report.json"))
    analysis = commands.add_parser("research", parents=[database], help="Offline historical regression baseline")
    analysis.add_argument("--start", default="2026-04-01", help="UTC date, inclusive")
    analysis.add_argument("--end", default="2026-07-01", help="UTC date, exclusive")
    analysis.add_argument("--horizon", type=int, default=60)
    analysis.add_argument("--output", type=Path, default=Path("data/research-report.json"))
    history = commands.add_parser("import-history", parents=[database], help="Import allowlisted 1m candles from a Kraken ZIP archive")
    history.add_argument("archive", type=Path)
    history.add_argument("--report", type=Path, default=Path("data/history-import-report.json"))
    ingest = commands.add_parser("ingest", parents=[database], help="Store completed 1m candles continuously")
    ingest.add_argument("--once", action="store_true", help="Reconcile available history and stop")
    ingest.add_argument("--poll-seconds", type=float, default=60)
    scanner = commands.add_parser("scan", parents=[database], help="Report indicators from completed 5m candles")
    scanner.add_argument("--live", action="store_true", help="Collect 1m candles and scan in one process")
    scanner.add_argument("--scanner-config", type=Path, help="JSON indicator periods and freshness settings")
    scanner.add_argument("--json", action="store_true", help="Emit structured scanner rows")
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
    except (KrakenError, ValueError, KeyError, OSError, duckdb.Error, zipfile.BadZipFile) as exc:
        parser.exit(1, f"Error: {exc}\n")


if __name__ == "__main__":
    main()
