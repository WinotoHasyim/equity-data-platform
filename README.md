# Alpaca Stock Data Pipeline

A data-engineering pipeline that collects historical and real-time
US stock market data from Alpaca's free IEX feed. Raw events land in
Google Cloud Storage, and BigQuery with dbt performs the transformations.

## Stock universe

- AAPL
- MSFT
- NVDA
- AMZN
- META

These highly traded technology companies generate enough market events
for testing batch and streaming pipelines. Edit the `symbols` list in
`config.toml` to change or expand the universe without changing Python code.

## Data collected

### Historical batch data

- One-minute bars
- Trades
- Quotes
- Corporate actions
- Stock metadata

### Real-time streaming data

- Quotes
- Trades
- Minute bars
- Updated bars
- Daily bars
- Trading-status events
- Trade corrections
- Trade cancellations

## Why each dataset matters

- Quotes measure bid/ask spread and available liquidity.
- Trades represent completed market transactions.
- Bars provide OHLCV, VWAP, and trade-count aggregates.
- Updated bars capture trades that arrive late.
- Trading statuses capture halts and resumptions.
- Corrections and cancellations keep trade data accurate.
- Corporate actions explain events such as splits and dividends.

## Data-source limitation

This project uses Alpaca's free IEX feed. It does not represent the
complete consolidated US stock market.

## First milestone

For the five stocks:

1. Download five years of daily bars.
2. Download one month of one-minute bars.
3. Download one trading day of historical quotes and trades.
4. Store the unmodified source records in Google Cloud Storage.
5. Load or expose the raw datasets in BigQuery staging tables.
6. Use dbt to validate, normalize, deduplicate, and reconcile the data.
7. Build canonical and analytical tables in BigQuery.
8. Increase the quote and trade date range after measuring volume.
