# Kraken market-data bot

Stages 1 and 2 of a modular Python cryptocurrency research project. Uses only
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

Only aiohttp is required at runtime; pytest is a development dependency. No
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
tests/                     Parsing, errors, retry and reconnection tests
```

Exchange-specific parsing stays in the adapter. Future storage, scanning,
strategies, risk, backtesting and simulation can consume the shared records and
MarketData protocol. Those stages are not implemented. DuckDB, Parquet, NumPy and
Polars will be introduced when their functionality is needed. Stage 3 requires
explicit approval. Funded balance, targets and drawdown rules are not implemented
or independently verified at this stage.

Git is local; no GitHub repository, remote or push is created automatically.

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
