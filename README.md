# Kraken market-data bot

Stages 1–3 of a modular Python cryptocurrency research project. Uses only
public Kraken Spot endpoints. No credentials, account access or order placement.

## Setup on macOS / Apple Silicon

Python 3.12 or later is required. From this project directory:

```sh
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
pytest
```

Runtime dependencies are aiohttp, DuckDB and pytz (needed for DuckDB's Python
timezone-aware timestamp conversion); pytest is a development dependency. No
`.env` file is needed. If your Python reports certificate errors, fix its trust
store (python.org macOS installers provide `Install Certificates.command`);
do not disable TLS certificate verification.

## Commands

```sh
kraken-bot live
kraken-bot live --max-updates 6
kraken-bot prices
kraken-bot pairs
kraken-bot ohlc BTC/USD --interval 60
kraken-bot ohlc BTC/USD --interval 60 --since 1791676800
kraken-bot --config config/assets.json live
```

Stop live collection with Ctrl-C. Commands emit JSON lines to the terminal;
price/volume decimals are strings to retain precision. Prices have a UTC
`received_at`, and an `exchange_timestamp` where Kraken supplies one. REST
ticker observations have no exchange timestamp; receipt time is not a trade
time. WebSocket ticker updates are trade-triggered and include initial snapshots.
Neither source guarantees a fresh last trade for inactive pairs.

Edit `config/assets.json` to select pairs, then use `--config` before the command.
Without this option the built-in defaults are BTC/USD, ETH/USD and SOL/USD.
The configured list controls price streaming and OHLC access; `pairs` is a
discovery command listing the broader Spot universe. XBT is normalized to BTC.
**The allowlist has not been verified for Kraken Funded eligibility.** Confirm
eligibility from an official Funded source before subsequent stages use it.

One WebSocket connection subscribes to the configured list, avoiding REST polling
per asset. This supports expanding toward about 50 assets without storing every
tick or introducing a database. Actual availability is validated against Spot
pairs, and WebSocket subscription failures are reported clearly.

## Reliability and data limits

REST request starts are serialized at no more than one per second per client.
Other processes sharing your IP also contribute to Kraken's limits. HTTP 429,
server errors, timeouts and transient Kraken errors get up to four retries with
exponential backoff and jitter; numeric Retry-After and Kraken throttle timestamps
are honored. Permanent errors fail immediately. Live disconnects and idle
connections reconnect with backoff (5–60 seconds plus jitter), re-subscribing and
receiving fresh snapshots. An outage can leave gaps: snapshots do not recover
every missed price update. Subscription errors stop collection instead of looping.

OHLC provides a limited recent window, not unlimited historical data. Kraken
documents up to 720 entries; live checks on 11 October 2026 returned 721 rows
for each initial asset: 720 completed candles plus the current candle. The
collector preserves all returned rows rather than enforcing a hard row count.
`since` is an incremental cursor and cannot retrieve older unavailable history.
The last candle is still forming (`complete: false`); all prior returned candles
are marked complete. The command prints `next_since` after the candle records.
Intervals in minutes: 1, 5, 15, 30, 60, 240, 1440, 10080, 21600.

Official references:
- [REST OHLC](https://docs.kraken.com/api-reference/market-data/get-ohlc-data)
- [WebSocket v2 ticker](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ticker)
- [Public rate limits](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-)

## Architecture and next stages

```text
config/assets.json          Explicit asset allowlist
src/kraken_bot/config.py    Configuration and canonical symbols
src/kraken_bot/models.py    Immutable Pair, Price and Candle records
src/kraken_bot/interfaces.py MarketData protocol for future consumers
src/kraken_bot/exchanges/kraken.py  Public REST/WebSocket adapter
src/kraken_bot/cli.py       Terminal collector and inspection commands
src/kraken_bot/storage.py   DuckDB schema, deduplication, gaps, aggregation, exports
src/kraken_bot/ingestion.py Continuous completed-candle reconciliation
tests/                     Parsing, errors, retry and reconnection tests
```

Exchange-specific parsing stays in the adapter. Future scanning,
strategies, risk, backtesting and simulation can consume the shared records and
MarketData protocol. Stages 4–8 are not implemented. NumPy and Polars will be
introduced when needed. Funded balance, targets and drawdown rules are not implemented
or independently verified at this stage.

Git is local; no GitHub repository, remote or push is created automatically.

## Stage 3: historical database

From the project folder with the virtual environment activated:

```sh
# Fetch available history, then append newly completed minutes continuously.
kraken-bot --config config/assets.json ingest

# Or reconcile once and exit.
kraken-bot --config config/assets.json ingest --once
```

The primary file is `data/market.duckdb`. Use `--db path/to/market.duckdb` after
the command to select another file. `live` still only prints prices; it does
not build the database. Run `ingest` for storage. Ctrl-C shuts ingestion down
and closes the database. It is not installed as a background service: it must
remain running to keep collecting, and Mac sleep or network outages interrupt it.
On restart it reconciles the available history with the existing database.

Each pass fetches the complete, bounded one-minute REST window for every configured
pair. Requests remain serialized at no more than one per second. The collector
reconciles overlapping history to repair recoverable holes, rather than assuming
the newest stored timestamp proves all earlier minutes exist. Passes start roughly
60 seconds apart; slow requests or retries can extend this. At 50 assets the API
budget alone needs about 50 seconds per pass, so logs warn if a pass exceeds its
period. Only newly seen completed candles are appended; the forming candle is
excluded. No individual WebSocket ticker updates are stored.

### Database design

- `candles`: one-minute source OHLCV, VWAP and trade count. The primary key is
  `(symbol, interval_minutes, timestamp)`, so overlapping fetches and restarts
  cannot duplicate rows. Completed records use first-write-wins; repeated
  responses do not silently revise previously saved values.
- Timestamps are `TIMESTAMPTZ`, with every database connection set to UTC.
  Naive or unaligned source timestamps are rejected. Prices and volumes use
  exact `DECIMAL(38,18)` values; values outside that precision are rejected.
- Inserts and updated gap records are committed together in a transaction.
  Inserts use bounded batches; DuckDB uses two threads and a 512 MB working-memory
  limit. Python and other process memory is additional to that limit.
- `candles_5m` and `candles_60m` are SQL views generated from the one-minute table.
  They use UTC-aligned five-minute and clock-hour buckets. Open/close are the
  first/last minute, high/low are extrema, volume/trades are sums, and VWAP is
  volume-weighted (calculated in floating point; zero total volume yields zero).
  A view emits a bucket only when all five or sixty distinct minutes exist.
  Edge buckets and buckets with missing minutes are excluded. No fabricated
  candle is inserted to fill a hole.

### Gaps and interruptions

`gaps` stores inclusive missing-minute ranges per pair, avoiding one row per
minute of a long outage. Gaps are rechecked on successful reconciliation and
removed when the source supplies the missing candles. Publication gets a
one-minute grace period before a trailing absence is reported.

Statuses printed in logs and by `db-gaps`:

- `awaiting_source`: trailing completed minutes have not yet appeared.
- `missing_from_response`: a minute is absent inside the returned window. This
  does not prove a network outage: no-trade intervals may also be absent.
- `outside_kraken_window`: the missing range is older than the earliest completed
  candle in the latest full REST response. The current Kraken OHLC endpoint
  cannot recover it; another historical source would be needed.

Transient API/network failures are logged immediately, retry within the REST
client, and are retried on later passes if exhausted. Other assets continue.
Successful recovery is logged and reconciles available history. A once-only pass
exits with an error if an asset remains unavailable. Unknown pairs and permanent
API errors stop the command so configuration errors are visible.

History before the first fetched candle is **unknown**, not assumed complete.
The initial one-minute seed is about 12 hours when 720 completed candles are
available; it is not the longer history that hourly API candles would cover.
The retained database grows while ingestion runs. Outages beyond the endpoint's
available window can leave permanent gaps. The exact current counts and UTC
start/end timestamps are shown by `db-summary`.

### Inspect and export

DuckDB uses a single writer process. **Stop ingestion with Ctrl-C before using
inspection/export commands in another process.** Multiple read-only inspectors
may open the stopped database. Do not run two ingestion processes on the same file.

```sh
kraken-bot db-summary
kraken-bot db-gaps
kraken-bot db-show BTC/USD --limit 5
kraken-bot db-show BTC/USD --interval 5 --limit 5
kraken-bot db-show BTC/USD --interval 60 --limit 5

# All configured/stored pairs, source resolution.
kraken-bot export data/exports/candles-1m.parquet

# Complete derived candles for one pair.
kraken-bot export data/exports/btc-5m.parquet --interval 5 --symbol BTC/USD
kraken-bot export data/exports/btc-1h.parquet --interval 60 --symbol BTC/USD
```

Parquet exports are compressed with Zstandard. They are snapshots, not the primary
store; re-export under a new filename to capture subsequent data. Existing files
are never overwritten. Source and derived exports include interval and UTC time.
OHLC exports do not include gap metadata: inspect `db-gaps` separately.

The database, write-ahead log and Parquet files are excluded from Git, as is the
virtual environment. Commit source/configuration changes; back up the data file
separately after stopping ingestion.

To query directly from Python after stopping the collector:

```python
import duckdb

with duckdb.connect("data/market.duckdb", read_only=True) as db:
    db.execute("SET TimeZone='UTC'")
    print(db.execute("SELECT symbol, count(*) FROM candles GROUP BY symbol").fetchall())
    print(db.execute("SELECT * FROM candles_60m ORDER BY timestamp DESC LIMIT 3").fetchall())
```

DuckDB references: [Python API](https://duckdb.org/docs/stable/clients/python/overview),
[conflict handling](https://duckdb.org/docs/current/sql/statements/insert),
[Parquet](https://duckdb.org/docs/stable/data/parquet/overview).

## Verification

`pytest` runs offline tests, including a local WebSocket server for disconnect,
re-subscription and rejected subscription behavior. To verify your network access,
run `kraken-bot prices`, `kraken-bot ohlc BTC/USD --interval 60`, and
`kraken-bot live --max-updates 6`. These access public data only. Passing offline
tests alone does not establish connectivity or live exchange availability.

Verified on 11 October 2026 with Python 3.12.8 on this Mac: five tests passed;
REST prices and hourly OHLC succeeded for BTC/USD, ETH/USD and SOL/USD; the
public WebSocket delivered three initial snapshots and three subsequent updates.
Dependencies passed `pip check`. Reconnection was verified with the local test
server, rather than by deliberately interrupting the real exchange connection.

Stage 3 verification on the same date: **24 tests passed** (source ingestion,
duplicate keys, restart persistence, UTC conversion, invalid batches, transaction
rollback, aggregation, gap detection/repair/expiry, failure recovery and Parquet).
Live REST ingestion fetched 720 completed one-minute candles per initial asset,
appended the next minute during continuous polling, then appended two more minutes
on a subsequent once-only run. The saved database at that check contained 723
candles per pair (2,169 total), from 08:36 through 20:38 UTC on 11 October 2026,
with no detected missing minutes within that observed coverage.

Initial Parquet snapshots were exported at the earlier 721-candle-per-pair state:
2,163 one-minute rows, 429 complete five-minute rows and 33 complete hourly rows.
All three exports matched their database table/view exactly when checked. They
are snapshots; their counts differ from later database updates by design.

A synthetic in-memory 50-asset storage check ingested 36,000 candles in 35.14
seconds. Reconciling overlapping windows with one new candle for each asset took
0.83 seconds after skipping inserts for existing timestamps. This measures local
storage work, not 50 real exchange subscriptions or network throughput. The
serialized one-request-per-second REST budget remains the main ongoing constraint.
