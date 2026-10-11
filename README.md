# Kraken market-data bot

Stages 1–4 of a modular Python cryptocurrency research project. Uses only
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
discovery command listing the broader Spot universe. XBT is normalized to BTC
and XDG to DOGE.

The configuration now contains **47 distinct cryptocurrencies observed in the
user's Kraken Funded tab on 11 October 2026**, including the original three assets.
It is a partial, user-observed list, not an official eligibility certification.
It has not been verified as the complete Funded universe or a list of low-cap
assets. The JSON records that provenance without saving screenshots, prices,
gains, favourites or account details. Kraken's public Spot pair list is checked
separately for market-data availability; it does not certify Funded eligibility.
Eligibility can change and must be rechecked in the Funded tab or an official
source before any future trading use.

After editing this file, stop a running collector with Ctrl-C and restart it
with `--config config/assets.json`; existing processes do not reload it.
For 47 assets the first collection pass takes roughly a minute or longer,
depending on network and storage time. New pairs begin with whatever completed
one-minute history Kraken currently supplies. Previously stored BTC/ETH/SOL
history is preserved. There is no need to delete the database.

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
src/kraken_bot/indicators.py Indicator calculations over contiguous prices
src/kraken_bot/scanner.py   Configurable five-minute market scanner
tests/                     Parsing, errors, retry and reconnection tests
```

Exchange-specific parsing stays in the adapter. Future
strategies, risk, backtesting and simulation can consume the shared records and
MarketData protocol. Stages 5–8 are not implemented. NumPy and Polars will be
introduced when needed. Funded balance, targets and drawdown rules are not implemented
or independently verified at this stage.

Git remote `origin` points to the private `BrendanBoyce/kraken-trading-bot`
repository. Author and committer metadata use the GitHub noreply identity.

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

## Stage 4: market scanner

The scanner reports market conditions for the asset allowlist using completed
five-minute candles derived from the stored one-minute data. It does not generate
BUY/SELL/HOLD decisions, simulate positions or place orders.

```sh
# Snapshot: stop any ingestion process first, then inspect the saved database.
kraken-bot --config config/assets.json scan
kraken-bot scan --json

# Continuous: ingestion and scanning share one writer process.
kraken-bot --config config/assets.json scan --live --scanner-config config/scanner.json
```

**Use `scan --live` instead of running `ingest` in another terminal on the same
database.** Stop the existing collector with Ctrl-C first. Live scanning polls
Kraken for completed minutes, reconciles them into DuckDB, then scans after each
pass. The table is printed at startup and when a completed five-minute candle,
available history count or quality flag changes. It stays quiet on unchanged
passes, although ingestion logs still appear each minute. Ctrl-C stops both
collection and scanning. Read-only snapshots, inspections and exports should be
run after stopping this writer.

Edit `config/scanner.json` and pass `--scanner-config` to change settings. Without
it, the same defaults are built in:

| Field | Definition |
| --- | --- |
| RSI14 | Wilder RSI on closing prices, initial 14-change SMA followed by Wilder smoothing. A flat series is defined as 50. |
| MACD / HIST | EMA12 minus EMA26, nine-period EMA signal, and line minus signal. JSON also includes the signal value. |
| SMA20 / SMA50 | Mean of the last 20 / 50 five-minute closes. |
| EMA20 | Exponential average, alpha 2/21, seeded from the first 20-close SMA. |
| VOL% | Sample standard deviation of the last 20 five-minute log returns, expressed as percent per candle; not annualized. |
| VOLUMEΔ% | Latest five-minute base-asset volume versus the mean volume of the preceding 20 candles, in percent. |

MACD EMA seeds use their respective initial-period SMAs; its signal seed is the
mean of the first nine available MACD values. Calculations use at most the latest
300 derived candles (25 hours) to bound CPU and memory. EMA/RSI values depend on
this starting point and can differ slightly from platforms using longer histories
or other seed conventions. No additional calculation dependency is required for
these short sequences. Stored prices retain decimal precision; indicator
calculations and JSON indicator values use floating point.

Missing values appear as `—` in the table or `null` in JSON. Default indicators
need up to 50 contiguous candles (4 hours 10 minutes). The scanner never bridges
a missing five-minute bucket: it resets calculations to the contiguous suffix
after the last hole. Buckets with missing source minutes are already excluded
by the aggregation views.

Status flags:

- `stale`: the last completed candle's end is more than 10 minutes old.
- `gap_in_lookback`: a bucket is missing or a tracked source gap overlaps the
  selected history through the current time.
- `insufficient_history`: at least one configured calculation lacks its minimum
  contiguous history. Individual ready calculations remain visible.
- `no_data`: no completed five-minute candles are available for this pair.
- `zero_volume_baseline`: prior comparison volumes are all zero; volume change
  is undefined rather than infinite.
- `invalid_data` or `future_data_excluded`: nonpositive/nonfinite prices or
  invalid volumes prevent calculations; candles ending in the future are excluded.

`ok` means these data checks passed; it is not a trading recommendation. Snapshot
mode accesses no network and leaves the database unchanged. Live mode retains
the Stage 3 REST limits, recovery and gap policies and can use the same configurable
allowlist for approximately 50 pairs.

References: [RSI definition](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/RSI),
[DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency).

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

Stage 4 verification: **33 tests passed**, including known RSI/EMA/MACD results,
volume and volatility formulas, warm-up limits, gaps, stale/future/invalid data,
configuration validation, deduplicated output and same-connection live scanning.
A snapshot correctly flagged the stopped collector's history as stale. Live
scanning backfilled 22 available minutes per asset, displayed fresh five-minute
conditions, and appended the next minute on the following pass without repeating
an unchanged table. The verification process was stopped cleanly with Ctrl-C.

### Historical archive import

Kraken publishes [official OHLCVT archives](https://support.kraken.com/articles/360047124832-downloadable-historical-ohlcvt-open-high-low-close-volume-trades-data).
The initial import uses the latest published quarter, April–June 2026, rather than
all years and all markets. Stop the scanner with Ctrl-C before importing or inspecting
DuckDB from another process.

```sh
kraken-bot --config config/assets.json import-history data/archives/Kraken_OHLCVT_2026Q2.zip
kraken-bot db-summary
cat data/history-import-report.json
kraken-bot export data/btc-history.parquet --symbol BTC/USD
kraken-bot --config config/assets.json scan --live
```

The importer selects only allowlisted one-minute USD CSVs, processes one file at a
time, validates rows, and commits each pair atomically. Repeating an import ignores
existing keys and preserves existing live prices. An interrupted run can be repeated.
The JSON report records the archive SHA-256, its manifest, per-pair dates and counts,
missing pairs, and coverage gaps before existing data. SHA-256 records provenance;
it is not a comparison against a Kraken-published checksum for this quarterly ZIP.
ZIP CRC validation checks extracted members. Archive and reports stay under ignored
`data/` and are not pushed to GitHub.

Historical files contain OHLC, volume and trade count, but no VWAP; historical VWAP
is NULL. Aggregated VWAP remains NULL when a contributing traded minute lacks VWAP.
Prices and volumes use DECIMAL(38,18), including rounding source values beyond 18
fractional places. UTC timestamps must align to complete minutes. Archive minutes
without trades are absent, and no candles are fabricated. Strict 5m/1h views require
all constituent minutes, so sparse historical markets yield fewer aggregates.

A broad first-to-last span in `db-summary` does **not** prove continuous coverage.
The April–June archive does not fill July–October; consult the import report separately
from `db-gaps`, which tracks live collector gaps. LIGHTER/USD is absent from the Q2
archive. Current scanner calculations still use the latest configured lookback; this
import provides older data for subsequent research/backtesting, not a prediction model.
