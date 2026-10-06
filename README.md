# Alpaca free-tier data extractor

A small, dependency-free Python CLI that downloads historical US stock bars from
Alpaca and writes tidy CSV or JSON. It defaults to Alpaca Basic's **IEX** feed,
so recent requests do not require a paid market-data subscription.

## 1. Create credentials

Create a free Alpaca account (a paper-trading account is enough), then copy a key
ID and secret from the Alpaca dashboard. Keep the secret out of source control.

```bash
cp .env.example .env
# Edit .env with your real key ID and secret, then load it into this shell:
set -a
source .env
set +a
```

## 2. Extract data

The default command fetches the last seven calendar days of daily AAPL bars:

```bash
python3 extract_bars.py
```

Choose symbols, dates, and granularity:

```bash
python3 extract_bars.py \
  --symbols AAPL,MSFT,NVDA \
  --start 2026-09-01 \
  --end 2026-10-01 \
  --timeframe 1Hour \
  --output data/tech-hourly.csv
```

Write JSON instead:

```bash
python3 extract_bars.py --symbols SPY --format json
```

Run `python3 extract_bars.py --help` for all options. Dates are interpreted by
Alpaca and may be `YYYY-MM-DD` or RFC-3339 timestamps. The output columns are
symbol, timestamp, OHLC, volume, trade count, and VWAP.

## Free-tier notes

- `iex` is the default and is the free real-time US equities feed.
- IEX is one exchange, so its bars have less volume than consolidated SIP bars.
- Basic accounts are currently limited to 200 historical API requests per minute.
- Alpaca documents historical equities availability from 2016 onward.
- `--feed sip` can be useful for eligible delayed/historical requests, but recent
  SIP data requires a paid entitlement. Use `iex` for a predictable free-tier test.

## Test

```bash
python3 -m unittest -v
```

The tests use fake API responses and never need or expose real credentials.

