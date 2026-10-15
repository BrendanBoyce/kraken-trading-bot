# Kraken Trading Bot

A modular Python platform for cryptocurrency market-data engineering, historical research and live experimental model scoring using public Kraken Spot data.

**Research project:** no API credentials, account access or order execution. No strategy has passed the project's validation requirements; model scores are not calibrated winning probabilities.

## What it does

- **Collects public market data:** asynchronous REST and WebSocket clients with rate limiting, retries and reconnection handling; configurable watchlists, including a 47-asset example.
- **Builds a UTC candle historian:** DuckDB stores completed one-minute candles with deduplication, missing-data detection and strict five-minute/hourly aggregation. Exports compressed Parquet snapshots.
- **Imports historical archives:** candle and trade-level ingestion, source checksums, coverage reports and minute-level trade-flow features.
- **Tests predictive hypotheses:** regression, logistic regression and gradient-boosting experiments using chronological splits, past-only features, validation-only thresholds and transaction-cost sensitivity analysis.
- **Scores live observations:** frozen research models run against completed candles and reconciled trade features. Stale or incomplete inputs block predictions; model/source provenance is logged.
- **Checks reliability:** 116 automated tests cover ingestion, deduplication, gaps, aggregation, indicators, research evaluation and live feature construction.

## Stack

Python 3.12+ · asyncio / aiohttp · DuckDB / SQL · Parquet · NumPy · scikit-learn · pytest

## Architecture

```text
Public Kraken REST / WebSocket + historical archives
                         |
             parsing, validation, retries
                         |
              UTC one-minute candle store
                    /           \
        aggregation / scanner   research / trade-flow features
                                      |
                          chronological model evaluation
                                      |
                           experimental live scoring
```

## Run locally

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install '.[dev,research]'
pytest -q

# Fetch the bounded recent OHLC history, then inspect it.
kraken-bot --config config/assets.json ingest --once
kraken-bot db-summary
kraken-bot --config config/assets.json scan
```

Live price streaming is also available with `kraken-bot live`. Continuous storage uses `ingest`; run a single writer per DuckDB file.

Research commands need the corresponding historical archives. Live model scoring additionally requires locally generated model artifacts; these are deliberately not bundled. See the [technical guide](docs/technical-guide.md) for commands, data preparation, experiment design and limitations.

## Research integrity and limitations

This project investigates whether market and trade-flow features offer predictive value; it does not claim profitable automated trading. Historical observations can be correlated, evaluated periods can become contaminated by repeated inspection, and assumed fills/costs can differ from executable quotes. These limitations are documented in the research workflows.

Only public **Spot** endpoints are used. Kraken Funded eligibility, bot access and execution support are not inferred from Spot market-data availability. There is no funded-account integration.

Local databases, downloaded archives, fitted models, reports, logs, virtual environments and credentials are excluded from Git. The CV is maintained separately and is not included in this repository.
