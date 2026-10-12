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

### Offline historical research

```sh
# Stop scan --live first so another process can read DuckDB.
kraken-bot --config config/assets.json research
# Optional UTC date range and future-return horizon:
kraken-bot --config config/assets.json research --start 2026-04-01 --end 2026-07-01 --horizon 60
```

This runs locally and writes `data/research-report.json` plus a readable Markdown
report. It predicts next-hour close returns with a pooled ridge linear regression
(default penalty 10), using past 1m/15m/60m returns, the past-hour price range and
relative volume. The default period is the imported April–June quarter. Dates are
UTC; `--end` is exclusive. There are no orders, trade signals or strategy changes.

The first 70% of the requested calendar period trains the model, and the later 30%
forms a holdout. Scaling uses training observations only. Training targets ending
after the split are excluded. Decisions occur once per horizon per asset, with no
overlapping target windows and no crossing missing-minute gaps. Features require
61 consecutive close observations, and targets require all future minutes. Sparse
assets therefore contribute very few or no samples; the report lists every pair.

Compare held-out RMSE against the training-mean baseline; negative R-squared or a
worse baseline comparison gives no evidence of predictive skill. The future-high
10% counts are descriptive event observations, not achievable profit or probability
predictions. Reported forward returns ignore fees, spread, slippage and fills.
Asset selection is today's observed list, not a historical market-cap universe;
survivorship bias and correlated assets limit conclusions. A single holdout is
preliminary research, not a validated trading backtest. Further experiments need
walk-forward evaluation and fresh unseen periods rather than repeatedly tuning
against this same holdout.

### Archive-aware walk-forward evaluation

```sh
kraken-bot --config config/assets.json research-walk-forward
```

This command works directly from the downloaded Q2 archive in a separate in-memory
DuckDB connection, so it can run while the live collector is using the main database.
It writes `data/walk-forward-report.json` and `.md` and leaves stored candles unchanged.
One asset's CSV and temporary research table are processed at a time, using the
existing two-thread/512 MB DuckDB limits. No additional numerical package is needed.

[Kraken documents absent archive minutes as no-trade periods](https://support.kraken.com/articles/360047124832-downloadable-historical-ohlcvt-open-high-low-close-volume-trades-data).
Only inside an actual archive CSV's first/last recorded timestamps, temporary research
marks carry the most recent known close forward, with zero volume and flat price bounds
for omitted minutes. They never look forward, infer prices before the first record,
fill collector outages, extend beyond manifest coverage, or become stored candles.
Both prediction-reference and target-reference minutes must contain recorded trades.
That future target eligibility is known retrospectively and can bias evaluation;
it is not an executable rule or proof that an exit could have filled.

The default starts with 28 days of training, then evaluates successive seven-day
periods with expanding training history. Each fold refits the same ridge baseline
and train-only scaler; training target end times cannot exceed that fold's cutoff.
Test labels must end within the test period. Target periods do not overlap per pair.
Use `--train-days`, `--test-days`, `--horizon`, `--start`, `--end`, `--archive` or
`--output` to change the experiment explicitly. Requested dates outside archive
coverage are rejected. Outputs use `.json` plus a sibling `.md` report.

Compare the model with zero-return, pooled training-average and per-asset
training-average baselines. The latter falls back to the pooled mean for an asset
without earlier training observations. Reports show each period, every asset's
coverage, carried-minute counts and future +10% high observations. The archive
SHA-256 is recorded for reproducibility, not checked against an official checksum.

These are exploratory results on Q2 data already examined in the initial holdout
study, not a fresh final validation set. Correlated assets, today's selected asset
list, missing market-cap information and unavailable execution costs remain limits.
A positive result here would still need later, genuinely untouched data.

### Rally-event study

```sh
kraken-bot --config config/assets.json research-rallies
```

The local command produces `data/rally-study-report.json` and a readable `.md` report
from the archive without opening the live database. Default primary outcome: a
recorded trade candle high touching +10% within four hours. Twelve- and 24-hour
horizons are separate exploratory comparisons, not combined independent evidence.
`--horizons 240 720 1440` specifies minutes; the first horizon is primary.

Four fixed research conditions examine past-hour volume, momentum with volume,
a four-hour breakout with volume, and compression with volume. Features require
four hours of archive coverage, a recorded trade at the decision minute, at least
25% past-hour traded minutes and positive previous volume. Samples use regular
UTC-aligned, non-overlapping future windows per horizon. A future endpoint need
not have traded: it is flagged and its closing mark remains an unexecutable proxy.
Neither prices nor observed-trade flags from the future determine past features.

After 28 training days, each seven-day test period uses a condition selected only
from completed earlier labels. Selection requires at least 30 training exposures,
three rally windows and three merged positive episodes, then chooses the largest
asset-adjusted rally-rate difference. These are exploratory support thresholds,
not statistical significance. Unsupported periods select no condition. Reports
include false positives, recall, within-asset control rates and weekly consistency.

A separate descriptive comparison matches each rally window to one nearest non-rally
window from the same asset and ISO week, without reusing controls. These outcome-based
matches never choose a forecasting condition. Adjacent positive windows are merged
per asset for episode counts; shared market shocks still create dependence.

Downside is the lowest future candle low relative to the decision reference close.
First target (+10%) and downside (-5%) crossing minutes establish order when possible;
both occurring in one candle is marked ambiguous. A target high is never booked as
realized profit. Reported fixed-horizon close proxies use an illustrative combined
cost haircut `(1 + gross_close_return) * (1 - cost_bps/10000) - 1`.
`--cost-bps 0 50 100` means assumed total costs of 0%, 0.5% and 1%, **not verified
Kraken fees**, spread or obtainable fills. No take-profit/stop-loss strategy is
executed or backtested as a fill simulation.

The study reuses previously inspected Q2 data. Multiple hypotheses/horizons, few
rally events, sparse trading, today's selected assets and correlated markets limit
inference. Positive associations require new data before reliability claims.

The current local environment uses a regular package installation after a macOS
hidden-file attribute prevented Python from processing its editable path file.
After changing source, update the command with `python -m pip install --no-deps .`.
Tests explicitly use `src/` through the pytest configuration; `pytest` therefore
checks working source, rather than an older installed copy.

### Trade-level enrichment for the same historical quarter

```sh
kraken-bot --config config/assets.json import-trades data/archives/Kraken_Trades_2026Q2.zip
kraken-bot trade-summary
kraken-bot trade-show BTC/USD --limit 5
```

[Kraken's official trade archive](https://support.kraken.com/articles/360047543791-downloadable-historical-market-data-time-and-sales-)
contains timestamp, price, volume, buy/sell classification, order type, misc and trade
ID. The initial download adds April–June 2026 trade detail for configured USD pairs.
It does not extend calendar coverage or provide an untouched validation period.

Raw trades are stored per pair as Zstandard Parquet under
`data/trades/Kraken_Trades_2026Q2/`. Trade IDs are deduplicated per pair; repeated IDs
with conflicting contents are rejected. Exact epoch seconds retain nine fractional
digits. The UTC timestamp convenience column rounds to microseconds; exact epoch
seconds remain authoritative for ordering and minute buckets. Prices/volumes use
DECIMAL(38,18); derived quote values and VWAP use double precision. Validation rejects
unsupported precision rather than silently truncating it.

`data/trade-features.duckdb` has a `trade_features` table keyed by pair and UTC minute.
It stores trade count, base/quote volume, VWAP, largest/mean trade quote size,
buy-classified and sell-classified volume, and signed base/quote volume imbalance.
A zero-volume minute is not fabricated. Buy/sell classification does not establish
capital inflows, order-book depth, spread or executable fills.

The importer processes one pair at a time with two DuckDB threads and a 512 MB memory
limit. It checks archive-member CRCs on extraction, records source and Parquet SHA-256
hashes, and verifies saved raw data before repeat imports. Repeats reuse raw Parquet
and rebuild the same source's features without duplicates. Differing sources with
overlapping minute features are rejected. An interrupted pair may leave an unpaired
raw artifact; the importer refuses to overwrite artifacts without matching metadata.
All archives, raw data and the provenance report remain under ignored `data/`.
The existing candle collector can continue because enrichment uses a separate DB.

Example raw-data inspection in Python:

```python
import duckdb
with duckdb.connect() as db:
    db.execute("SET threads=2")
    db.execute("SET memory_limit='512MB'")
    print(db.execute("SELECT side, count(*) FROM read_parquet(?) GROUP BY side",
                     ["data/trades/Kraken_Trades_2026Q2/BTC_USD.parquet"]).fetchall())
```

The current research commands still use candle features. Acquiring trade detail does
not automatically improve predictions; adding these inputs requires a subsequent
explicit experiment with chronological validation.

### Trade-enriched rally classifiers

```sh
python -m pip install '.[research]'
kraken-bot --config config/assets.json research-enriched
```

This command actually uses `data/trade-features.duckdb`, joining its past-minute
features to the candle archive. It compares identical four-hour rally labels and
sample times with candle-only and trade-enriched feature sets. New features include
15m/60m quote-volume imbalance, imbalance change, hourly trade count and its
acceleration, average/large trade size, price relative to past-hour VWAP and a fixed
momentum/imbalance interaction. Missing trade data for observed candle minutes is
an error; it is not silently treated as no trades. Future trade fields never enter
past-window features.

The primary comparison holds L2 logistic regression fixed for both feature sets.
Separately reported exploratory variants use the same fixed learner candidates: L2 logistic regression
(C=1) and a small histogram gradient-boosted classifier (100 iterations, learning
rate .05, depth 3, seven leaves, minimum leaf size 50, L2=10). No parameter search
or class weighting is performed. In the exploratory variants, each test week
selects the learner solely by
average precision on the preceding validation week; ties/no validation events
select logistic. The nonlinear candidate was added after inspecting the initial
logistic experiment, so the expanded comparison is explicitly exploratory.

Training expands chronologically. Labels ending after the validation boundary are
excluded from fitting; validation/test labels must end within their own periods.
Training-only 1st/99th percentile clipping and standardization bound outliers.
The fitted estimator is unchanged across its validation week and following test
week. A score threshold comes from the 95th percentile of validation scores, never
from test-period rankings or outcome labels. Both pipelines receive exactly the
same eligible observations. Early periods lacking class support are reported as
skipped. Rare-rally precision, recall, false positives, average precision and Brier
error are reported instead of misleading overall classification accuracy.

Outputs are `data/enriched-model-report.json` and `.md`. Per-period fitted pipeline
artifacts, including threshold, fit/validation cutoffs and horizon, are saved under
`data/enriched-model-report-models/` with hashes recorded in the report. The initial
logistic comparison was retained separately as `data/enriched-model-logistic-report.*`.
Source hashes, library version, feature sets, preprocessing parameters and model
parameters support reproducibility. Research uses two numerical threads and does
not open the collector's main database for writing.

This is additional experimentation on previously inspected Q2 data, not fresh
validation. Better ranking or fewer false positives alone does not establish a
profitable strategy, calibrated probabilities or executable fills. Cost/downside
figures retain the event study's fixed-horizon mark and assumed-cost conventions.
There is no automatic promotion to the live scanner or order execution.

### Stricter selection thresholds

```sh
# Capture prior-validation scores along with frozen fitted models/test scores.
kraken-bot --config config/assets.json research-enriched --output data/threshold-model-input.json
# Evaluate the predetermined threshold policies without fitting models again.
kraken-bot research-thresholds --input-report data/threshold-model-input.json
```

Outputs are `data/threshold-study-report.json` and `.md`. The primary candle-only
and trade-enriched logistic models are compared on paired test keys and labels.
Four fixed policies use the 95th, 97.5th, 99th and 99.5th percentiles of each preceding
validation week's scores. A 99.5th percentile cutoff is **not** a 99.5% probability
of a rally. Ties and distribution shifts can change the later selection rate.

Additional policies seek empirical validation precision of 80%, 90% or 100%, with
at least ten selected validation cases by default. Each picks the least restrictive
score threshold meeting its target and support requirement; equal-score groups stay
intact. If no threshold qualifies, the policy abstains for that period. Change the
support requirement explicitly with `--minimum-validation-selections`; it is a
support guardrail, not proof of reliability or a confidence bound.

Thresholds never use test labels. Those labels measure false positives, hits, missed
rallies, precision, recall, worst downside and assumed-cost close-return proxies.
Validation target end times are checked against test boundaries, and every test key
must belong to exactly one period. Empty selections have undefined precision and
are labelled as providing no performance estimate. Zero observed false positives
with nonempty selections are still not a guarantee of future performance.

This is an exploratory policy comparison on previously inspected Q2 data. No policy
is automatically chosen from the test table, promoted to the live scanner, or marked
ready for trading. More selective policies can miss most rallies and remain negative
after assumed costs. Fresh data and executable-price validation remain necessary.

### Frozen liquidity and market-context filters

The fixed plan in `config/filter-study-plan.json` records the model/report hashes,
June fitting and calibration boundaries, July–September holdout dates, score
quantile, filter thresholds, and validation support requirements. It was written
before processing Q3 outcomes. The named model artifacts and Q2 report must exist;
checksum mismatches fail rather than silently using different models.

```sh
kraken-bot --config config/assets.json import-trades data/archives/Kraken_Trades_2026Q3.zip --feature-db data/trade-features-Q3.duckdb
kraken-bot --config config/assets.json research-filters
```

The command rebuilds the original Q2 samples and verifies their scores and labels,
checks candle/trade count and volume agreement, then compares fixed activity,
buy-classification, relative-strength, market-context, overextension and
trade-concentration filters. Turnover thresholds require USD pairs. BTC/ETH context
uses the last completed minute and trailing returns, with missing context rejected.
Only Q2 validation selections choose a filter: at least ten cases, empirical
precision at least 50%, and improvement over baseline. Ties favor more support,
then alphabetic filter name. These are research guardrails, not statistical proof.
Unsupported policies abstain.

The selection and June score cutoff are saved to
`data/filter-study/report.selection.json` before Q3 processing. Q3 uses the frozen
June classifier, without retraining or calibrating on Q3. Candidate comparisons in
`data/filter-study/report.md` and `.json` are diagnostic; choosing the best Q3 filter
would consume that holdout. The asset list was chosen later in October, so this
experiment retains retrospective asset-selection bias. Q3 is now inspected and
cannot be claimed as untouched in another tuned experiment.

The collector database is not opened. Q3 trade features and raw Parquet live
separately from Q2. Archive imports use one DuckDB thread, a 512 MB memory budget
and external sorting without insertion-order preservation for larger pairs.
No filter activates live trading. A +10% high touch does not establish a realizable
exit; an abstaining model has undefined precision and is not trading-ready.

### Smaller profit targets, stops and time limits

```sh
kraken-bot --config config/assets.json research-exits
```

`config/exit-study-plan.json` fixes 3%, 4%, 5% and benchmark 10% targets,
1%, 2% and 3% stops, and 60/120/240-minute limits. Combined hypothetical fee,
spread and slippage haircuts are 0/50/100 bps. The preceding experiment's entry
classifier and score cutoff stay unchanged; both baseline and buy-confirmation
entry streams are diagnostic because neither was approved for live trading.

Entries use the opening trade price of the next minute, not the previous closing
price used as the older rally-label reference. An absent entry minute leaves the
signal unfilled rather than waiting for a convenient later price. Observed OHLC
bars provide path references. A gap through a stop uses the worse opening price;
a target crossed at an opening price uses the target price. When both levels are
touched later in one candle and order is unknown, stop-first is assumed. Timeout
marks use the last observed close within the time limit; absent final-minute trades
produce explicitly flagged stale marks. Targets/stop prices remain assumed fills,
not proven executable quotes, and costs are sensitivities rather than verified
fee rates. Stop sizes do not cap losses across gaps.

Settings are selected only from Q2 validation windows: at least twenty marked
trades, positive mean net return at 50 bps, and no stale timeout marks. The highest
validation mean wins; ties favor smaller target, smaller stop, shorter hold.
Unsupported settings abstain. The selection is written to
`data/exit-study/report.selection.json` before computing Q3 exits. Original Q2
validation scores and Q3 entry keys/scores must reproduce their saved reports;
source and model hash checks guard against mixing experiments.

`data/exit-study/report.md` and `.json` include all fixed candidates, net win rates,
mean/median/worst trade-return proxies, profit factors, target/stop/timeout counts,
ambiguous candles, unfilled entries, stale marks, and signal frequency. These
figures are per-trade price-path research, not portfolio returns: no capital,
position sizing, depth, queue position or partial fills are modelled. Same-pair
windows do not overlap, but positions in different assets can overlap.

Both Q2 and Q3 were previously inspected. The new exit experiment is exploratory,
and optimizing against the Q3 table would introduce further overfitting. A best
row or positive average cannot establish readiness; the current asset universe
was selected later, validation support is small and many settings are compared.
This command does not alter the live collector, enable model signals, or place
orders.

### Train for positive net exits rather than +10% high touches

```sh
kraken-bot --config config/assets.json research-net-model
```

`config/net-model-plan.json` fixes the experiment before evaluation: 3/4/5% targets,
a 2% stop, four-hour maximum hold, L2 logistic and shallow histogram-boosting learners,
and validation-score quantiles 95%/97.5%. No exit optimization is performed on Q3.
The new classifiers predict a positive return after an 8 bps combined cost proxy,
including profitable time-limit exits. Training uses April through June 24 with
label-end purging; June 24–July 1 is reserved for selection/calibration. Neither
preprocessing nor fitting sees that validation week or the later quarter.

The existing candle/trade features gain past-only BTC/ETH average return, relative
return and a missing-context flag. Six fitted pipelines and twelve fixed threshold
candidates are compared using earlier validation only. The selected candidate needs
at least twenty signals, twenty marked trades, five positive returns, positive mean
net return per signal and no stale timeout marks. Ties favor more signals, smaller
targets, logistic regression, then lower quantiles. Unfilled signals stay in the
sample as nonpositive labels and zero-return decisions instead of being filtered
using future trading activity. Stale timeout outcomes remain flagged and cannot
support validation model selection. All selected model scores, pipelines and source
hashes are retained locally under `data/net-model/`.

The 8 bps proxy approximates Kraken Funded's documented 0.04% spread each way;
0/8/20/50 bps sensitivities are reported. It is not a verified mapping of Spot archive
prices to Funded quotes or a promise of fills. Funded is a separate mobile product,
without advanced order types; bot access remains unconfirmed. Simulated stops
therefore remain research assumptions. Reference:
https://support.kraken.com/articles/funded

The selection and frozen cutoffs are written before evaluating Q3 model predictions.
Original +10% classifier baselines are replayed on the same eligible Q3 windows and
same smaller-target exit/cost assumptions, alongside an unconditional sample
baseline. Old selection keys and scores must reproduce the prior filter study.
Report files are `data/net-model/report.json` and `.md`; fitted models are in `models/`.

Q2 and Q3 have both been inspected in previous experiments, so this is retrospective
research rather than untouched confirmation. Candidate tables may not be used to
select another model after seeing Q3 results. The October asset universe, correlated
positions, small calibration week and multiple comparisons limit inference. Mean
returns per trade do not model capital allocation, account equity, the 3% funded
failure floor or the probability of passing a challenge. No live collector, strategy
or order execution changes are made.

### Stricter selections and fixed $100 trade accounting

```sh
kraken-bot --config config/assets.json research-precision-trades
```

The command reuses the six saved profit-outcome models, without fitting on later
data. `config/precision-trade-plan.json` fixes $100 notional per position, diagnostic
99th/99.5th validation-score percentiles, and empirical validation win-rate gates
of 95%/100% with at least twenty selected cases. A score percentile is not a win
probability. Whole score-tie groups stay together. Precision gates cannot select
below the original validation 95th-percentile threshold; unsupported gates abstain.

June validation is rebuilt with preceding past-data warmup. Its sample count,
original 95th-percentile cutoff and model artifact hashes must reproduce the prior
net-model experiment. Later scores/outcomes come from that experiment's saved
above-cutoff predictions. Every stricter threshold lies within this complete saved
subset, so missing below-cutoff predictions cannot affect the new comparisons.
Later P&L never determines a threshold or promotes a model.

Reports are `data/precision-trades/report.json`, `.md` and `.selection.json`.
Per-position P&L uses a fixed $100 notional and 8/20/50 bps cost sensitivities,
including all selected winners and losers. Stale timeout marks, unfilled signals,
known versus stale-mark P&L, simultaneous positions and estimated peak nominal
capital are explicit. Concurrent entries require $100 each; an exit uses its candle
end for overlap estimation, and a timeout stays open for its full intended hold.

These are sums of independent simulated position returns, without compounding,
account equity, available-cash limits or funded challenge pass/fail modelling.
No starting account balance was assumed. A zero-selection policy has undefined
performance, not proof of zero-error prediction. The quarters were already
inspected, and bot access/software-managed exits in Funded remain unconfirmed.
Even a perfect historical validation record cannot guarantee future winners.
No policy is automatically activated and no live orders are placed.

### Current established-coin pullback evidence

```sh
kraken-bot --config config/pullback-assets.json research-pullbacks
```

This is read-only research. The separate manual watchlist includes Polkadot (DOT),
Cardano, Avalanche and other established projects without changing the collector's
asset list. Reputation, Funded eligibility and safety are not inferred from that
list. Public Kraken Ticker and hourly OHLC requests record current prices, trailing
24-hour highs, quoted spreads and recent completed-bar changes. Kraken's final
forming OHLC candle is explicitly excluded. Live ticker values are sampled
sequentially, with a timestamp for each pair.

Pairs at least 5% below their trailing 24-hour highs enter the comparison. That is
not the same as a 5% rolling daily loss. Recovery means touching that prior high:
a 5% decline requires a 5.26% gain from the lower price. April–September 1-minute
archives are aggregated to observed hourly bars, one pair at a time. Each archive
CSV is parsed strictly and independently because quarterly newline conventions
can differ. Invalid/duplicate minutes fail instead of being silently padded.

Historical analogues have a prior 24-hour high, a drawdown within two percentage
points of the current drop (at least 3%), and the same sign of BTC's preceding
24-hour completed-close return. Eligible decisions are sampled every four hours
and subsequent same-asset cases are spaced at least 48 hours apart. Outcomes count
recovery within 24/48 hours and another 3% decline before recovery. If both barriers
are touched in the same hour, downside is counted first. Incomplete hourly windows
are skipped. Other-asset cases can remain correlated.

Outputs are `data/pullbacks/report.json`, `.md` and `.snapshot.json`. Counts,
historical rates, Wilson intervals, source URLs, archive hashes and individual
cases are retained. Intervals assume independent observations and may understate
uncertainty. Historical frequencies are not calibrated forecasts; a past high
can be a temporary spike, and today's macro regime may differ. No fitted recovery
model or buy/sell recommendation is claimed. Earlier trade-enriched profit models
answer a different question and cannot be scored honestly with missing current
trade inputs or used to guarantee a return to an arbitrary prior high.

The main collector database is not opened or changed. No orders are submitted,
and no costs, execution availability, account equity or Funded bot access are
assumed by these price-touch statistics.

### Live experimental model scores in the scanner

```sh
kraken-bot --config config/pullback-assets.json scan --live --paper-models
# One-pass verification instead of a continuous loop:
kraken-bot scan --live --paper-models --once --db data/paper-check.duckdb --prediction-log data/paper-check/predictions.jsonl
```

This explicitly opt-in paper mode connects the saved 3/4/5% logistic profit-outcome
models to real public Kraken Spot candles and trades. It never uses API credentials,
account balances, private endpoints or order placement. A normal scan remains an
indicator scan. Paper mode defaults to a separate `data/paper-market.duckdb`, avoiding
the existing collector's `data/market.duckdb` lock. If the paper scanner is already
running, inspect its output instead of starting another writer on the same file.
Ctrl-C stops the process started in your terminal.

Each prediction uses a common UTC decision minute based on exchange server time,
with a 15-second settlement margin. Public Trades history is paginated from the
start of the 241-minute feature window, then refreshed using incremental exchange
cursors. Trade IDs deduplicate overlapping responses. Only rolling minute
aggregates and IDs are retained in memory; individual trades/ticks are not written
permanently. Completed 1-minute candles are stored in the separate paper database.

Per-minute observed trade counts and volumes must match the candle endpoint before
features are computed. A known empty minute can carry the preceding close with
zero volume, as in training. Unknown or mismatched history blocks predictions.
The same training SQL builds candle and trade features, and BTC/ETH context is
aligned to the same completed minute. Current/incomplete candles and trades at or
after the decision are excluded from features. Liquidity eligibility is unchanged:
the latest minute must contain actual trades, with sufficient past activity and
finite features. More than five minutes of input age or unavailable reference
assets/exchange time also blocks scores. Clock/network failure emits blocked status
rather than guessed predictions, then retries. A page limit blocks partial history.

Three frozen models return scores and a `positive_net`/`nonpositive_net` class at
0.5. These are not calibrated win probabilities, buy/sell recommendations or
predictions of price direction alone. Positive labels in training included
profitable timeouts, not just target touches. Fixed 99.5th-percentile validation
cutoffs provide a **diagnostic paper-watch flag**; no model/policy passed the earlier
validation requirements. Every row retains `validated_buy_signal=false` and
`orders_enabled=false`. $100 is paper notional metadata, not a placed position.

Inference refreshes approximately once a minute on a small watchlist. Initial
four-hour trade bootstrap and larger asset lists can take longer under public API
rate limits; subsequent passes use incremental cursors. Freshness checks continue
to apply. Predictions made outside the original four-hour training sampling grid
or on assets outside the training universe (including DOT, ADA, XRP and ATOM in the
current watchlist) are explicitly flagged. These cases have additional uncertainty.

Live output is `data/live-paper/latest.md` and `.json`; decision snapshots and the
actual input feature vectors are appended to `predictions.jsonl`, with model/source
hashes and quality flags. `runtime.json` identifies the scanner process. The process
needs the Mac awake and network access; it is not a scheduled or hosted service.
Funded account access and software-managed stops remain unconfirmed, and no
Funded/Spot order execution is part of this command.
