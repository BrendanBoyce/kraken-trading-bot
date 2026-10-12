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
    if args.command == "research-exits":
        from .exit_study import run_exit_study
        report = run_exit_study(assets, args.plan, args.filter_plan, args.input_report,
            args.filter_report, args.training_archive, args.training_db,
            args.test_archive, args.test_db, args.output)
        print(json.dumps({"report": str(args.output), "selected_settings": report['selection']['settings_by_stream'],
            "Q3_selected_results": report['Q3_validation_selected_results'],
            "activity": report['Q3_entry_activity']}, indent=2))
        return
    if args.command == "research-filters":
        from .filter_study import run_study
        report = run_study(assets, args.plan, args.input_report, args.training_archive,
            args.training_db, args.test_archive, args.test_db, args.output)
        print(json.dumps({"report": str(args.output), "selected_filter": report['selection']['chosen_filter'],
            "holdout_results": report['holdout_results']}, indent=2))
        return
    if args.command=="research-thresholds":
        from .threshold_study import threshold_study
        report=threshold_study(args.input_report,args.output,args.minimum_validation_selections)
        print(json.dumps({"report":str(args.output),"results":[{"model":p["model"],"policy":p["policy"],**p["summary"]} for p in report["policies"]]},indent=2))
        return
    if args.command=="research-enriched":
        from .enriched_model import compare_enrichment
        with CandleStore(":memory:") as store:
            report=compare_enrichment(store,assets,args.archive,args.feature_db,args.output,
                datetime.strptime(args.start,"%Y-%m-%d").replace(tzinfo=UTC),
                datetime.strptime(args.end,"%Y-%m-%d").replace(tzinfo=UTC),args.horizon)
        print(json.dumps({"report":str(args.output),"aggregate":report["evaluation"]["aggregate"],"observed_change":report["observed_change"]},indent=2))
        return
    if args.command in {"trade-summary","trade-show"}:
        with CandleStore(args.feature_db,read_only=True) as store:
            if args.command=="trade-summary":
                cursor=store.connection.execute("SELECT symbol,sum(trades) AS trades,count(*) AS feature_minutes,min(timestamp) AS first_utc,max(timestamp) AS last_utc FROM trade_features GROUP BY symbol ORDER BY symbol")
            else:
                if args.limit<1:
                    raise ValueError("limit must be positive")
                cursor=store.connection.execute("SELECT * FROM trade_features WHERE symbol=? ORDER BY timestamp DESC LIMIT ?",[normalize_symbol(args.symbol),args.limit])
            names=[field[0] for field in cursor.description]
            for row in cursor.fetchall(): print(json.dumps(dict(zip(names,row)),default=str))
        return
    if args.command == "import-trades":
        from .trade_history import import_trades
        report=import_trades(args.archive,assets,args.output_dir,args.feature_db)
        print(json.dumps({key:report[key] for key in ("complete","total_trades","feature_minutes","feature_db")},indent=2))
        return
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
    exits = commands.add_parser("research-exits", help="Explore smaller profit targets with gap-aware stop and timeout proxies")
    exits.add_argument("--plan", type=Path, default=Path("config/exit-study-plan.json"))
    exits.add_argument("--filter-plan", type=Path, default=Path("config/filter-study-plan.json"))
    exits.add_argument("--input-report", type=Path, default=Path("data/threshold-model-input.json"))
    exits.add_argument("--filter-report", type=Path, default=Path("data/filter-study/report.json"))
    exits.add_argument("--training-archive", type=Path, default=Path("data/archives/Kraken_OHLCVT_2026Q2.zip"))
    exits.add_argument("--training-db", type=Path, default=Path("data/trade-features.duckdb"))
    exits.add_argument("--test-archive", type=Path, default=Path("data/archives/Kraken_OHLCVT_2026Q3.zip"))
    exits.add_argument("--test-db", type=Path, default=Path("data/trade-features-Q3.duckdb"))
    exits.add_argument("--output", type=Path, default=Path("data/exit-study/report.json"))
    filters = commands.add_parser("research-filters", help="Frozen past-validation filters on a later archive quarter")
    filters.add_argument("--plan", type=Path, default=Path("config/filter-study-plan.json"))
    filters.add_argument("--input-report", type=Path, default=Path("data/threshold-model-input.json"))
    filters.add_argument("--training-archive", type=Path, default=Path("data/archives/Kraken_OHLCVT_2026Q2.zip"))
    filters.add_argument("--training-db", type=Path, default=Path("data/trade-features.duckdb"))
    filters.add_argument("--test-archive", type=Path, default=Path("data/archives/Kraken_OHLCVT_2026Q3.zip"))
    filters.add_argument("--test-db", type=Path, default=Path("data/trade-features-Q3.duckdb"))
    filters.add_argument("--output", type=Path, default=Path("data/filter-study/report.json"))
    thresholds=commands.add_parser("research-thresholds",help="Evaluate fixed stricter past-validation thresholds")
    thresholds.add_argument("--input-report",type=Path,default=Path("data/threshold-model-input.json"))
    thresholds.add_argument("--output",type=Path,default=Path("data/threshold-study-report.json"))
    thresholds.add_argument("--minimum-validation-selections",type=int,default=10)
    enhanced=commands.add_parser("research-enriched",help="Compare candle-only and trade-enriched rally classifiers")
    enhanced.add_argument("--archive",type=Path,default=Path("data/archives/Kraken_OHLCVT_2026Q2.zip"))
    enhanced.add_argument("--feature-db",type=Path,default=Path("data/trade-features.duckdb"))
    enhanced.add_argument("--start",default="2026-04-01")
    enhanced.add_argument("--end",default="2026-07-01")
    enhanced.add_argument("--horizon",type=int,default=240)
    enhanced.add_argument("--output",type=Path,default=Path("data/enriched-model-report.json"))
    trade_database=argparse.ArgumentParser(add_help=False)
    trade_database.add_argument("--feature-db",type=Path,default=Path("data/trade-features.duckdb"))
    commands.add_parser("trade-summary",parents=[trade_database],help="Inspect trade feature coverage")
    trade_show=commands.add_parser("trade-show",parents=[trade_database],help="Inspect trade-derived minute features")
    trade_show.add_argument("symbol")
    trade_show.add_argument("--limit",type=int,default=5)
    trades=commands.add_parser("import-trades",help="Import official trade archive and derive minute features")
    trades.add_argument("archive",type=Path)
    trades.add_argument("--output-dir",type=Path,default=Path("data/trades"))
    trades.add_argument("--feature-db",type=Path,default=Path("data/trade-features.duckdb"))
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
