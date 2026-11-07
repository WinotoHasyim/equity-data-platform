# Multi-Source Platform Plan

**Status:** Planned

**Last reviewed:** 2026-11-06

## Purpose

The current implementation provides one complete batch path for Alpaca daily
bars. The next objective is to evolve that vertical slice into a multi-source
equity surveillance platform.

The platform should help answer:

> Which stocks are behaving unusually, and what market, company, or economic
> events may explain that behaviour?

Sources are selected to support that question. The goal is not to collect as
many APIs as possible. A source belongs in the platform only when it contributes
to a defined analytical product or improves the interpretation and reliability
of another dataset.

## Target source systems

The first complete multi-source release should use four independent providers.
Nasdaq Trader can be added after the initial mart is working.

| Provider | Data supplied | Role in the platform |
| --- | --- | --- |
| Alpaca | Bars, trades, quotes, assets, market calendar, corporate actions, news, and live market events | Market activity and reference data |
| SEC EDGAR | Company identifiers, filing submissions, and XBRL company facts | Company events and financial context |
| FINRA | Reg SHO daily short-sale volume | Additional trading-activity context |
| FRED | Interest rates and economic observations | Macroeconomic context |
| Nasdaq Trader | Symbol directory and trading-halt history | Security-master enrichment and market-status context |

### Alpaca

The following Alpaca datasets are candidates for persistent ingestion:

| Dataset | Delivery | Purpose | Delivery phase |
| --- | --- | --- | --- |
| Daily bars | Historical REST | Long-term price and volume baseline | Implemented |
| Asset metadata | Trading REST | Symbol, name, exchange, status, and trading attributes | First expansion |
| Market calendar | Trading REST | Trading sessions and shortened market days | First expansion |
| Corporate actions | Historical REST | Splits, dividends, mergers, and symbol changes | First expansion |
| Historical news | Historical REST | Context for unusual market behaviour | First expansion |
| Minute bars | Historical REST | Intraday movement and volatility | Second batch expansion |
| Trades | Historical REST | Executed transactions and trade activity | Second batch expansion |
| Quotes | Historical REST | Spread, midpoint, and displayed liquidity | Second batch expansion |
| Live trades and quotes | WebSocket | Real-time transaction and liquidity monitoring | Streaming phase |
| Live and updated bars | WebSocket | Real-time aggregates and late bar updates | Streaming phase |
| Trading statuses | WebSocket | Halts and resumptions | Streaming phase |
| Corrections and cancellations | WebSocket | Changes to provisional live trades | Streaming phase |
| Live news | WebSocket | Real-time event context | Streaming phase |

The free Alpaca plan uses the IEX equity feed and supports a limited WebSocket
symbol count. The product must describe its observations as IEX-observed rather
than representative of the complete US market.

The latest-value, snapshot, screener, and market-clock endpoints are useful for
health checks and operational interfaces, but they should not initially become
canonical warehouse facts. They either combine several grains or duplicate
events available through the historical datasets.

References:

- [Alpaca Historical API](https://docs.alpaca.markets/us/docs/historical-api)
- [Alpaca real-time stock data](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data)
- [Alpaca market-data plans](https://docs.alpaca.markets/us/docs/about-market-data-api)
- [Alpaca corporate actions](https://docs.alpaca.markets/us/reference/corporateactions-1)

### SEC EDGAR

SEC EDGAR adds company information that does not come from the market-data
provider:

- ticker and CIK mappings
- submission history
- 10-K, 10-Q, and 8-K filing metadata
- XBRL company facts such as revenue, net income, assets, liabilities, equity,
  and diluted earnings per share

Raw company facts should be retained in long format. A curated model can later
map selected concepts into consistent analytical measures. The filing
acceptance timestamp must be preserved so that point-in-time models use only
information that was publicly available at the time.

The public data APIs do not require an API key. The extractor must identify
itself through an appropriate user agent and comply with SEC access limits.

References:

- [SEC EDGAR data APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
- [SEC programmatic-access guidance](https://www.sec.gov/about/webmaster-frequently-asked-questions)

### FINRA

The initial FINRA dataset is Reg SHO daily short-sale volume. Its grain is one
trade facility, symbol, and date. Relevant fields include:

```text
trade_facility
date
symbol
short_volume
short_exempt_volume
total_volume
market
```

Daily short-sale volume is not short interest. Documentation, model names, and
dashboard labels must preserve that distinction. The public API provides a
rolling historical window, so immutable raw ingestion is useful for retaining
the observations collected by the project.

References:

- [FINRA developer documentation](https://developer.finra.org/docs)
- [FINRA explanation of short-sale volume and short interest](https://syndication.finra.org/content/short-interest-what-it-what-it-not)

### FRED

The first FRED series should be:

| Series | Meaning |
| --- | --- |
| `DFF` | Effective federal funds rate |
| `DGS2` | Two-year Treasury yield |
| `DGS10` | Ten-year Treasury yield |
| `CPIAUCSL` | Consumer Price Index |
| `UNRATE` | Unemployment rate |

Both series metadata and observations should be stored. Release dates and
vintages matter because macroeconomic observations can be revised after their
initial publication. Point-in-time models must not substitute a later revision
for the value known on an earlier date without identifying it as such.

References:

- [FRED API](https://fred.stlouisfed.org/docs/api/fred/)
- [FRED series observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)

### Nasdaq Trader

Nasdaq Trader is a later enrichment source for:

- listed and other-exchange symbol directories
- listing and security attributes
- current and historical trading halts
- halt reason and resumption information

Usage terms must be reviewed before raw Nasdaq files are published or
redistributed. The project should use this source to enrich the security master
and event context rather than duplicate Alpaca prices.

References:

- [Nasdaq symbol-directory definitions](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs)
- [Nasdaq trading-halt history](https://nasdaqtrader.com/trader.aspx?id=TradingHaltHistory)

## Optional future sources

The following sources are possible but are not part of the first expansion:

| Source | Possible use | Current decision |
| --- | --- | --- |
| US Treasury | Direct official yield-curve feed | Can replace the equivalent FRED series if original-source provenance becomes important |
| Bureau of Labor Statistics | Direct CPI and employment releases | Defer because FRED provides a simpler initial interface |
| OpenFIGI | Cross-provider security identifiers | Defer until the security universe is much larger |
| GDELT | Broader global news and events | Defer because it adds considerable noise and modelling scope |
| Commercial fundamentals APIs | Earnings calendars, estimates, and normalized fundamentals | Defer because of subscription, rate-limit, and redistribution concerns |
| Alternative market-data vendor | A second price feed | Add only if cross-provider comparison becomes a product requirement |
| Social-media sentiment | Public attention and sentiment | Exclude initially because validation and interpretation are weak |

## Data modelling strategy

Real data modelling does not depend on a table having many columns. It comes
from representing several business processes with different grains, shared
dimensions, revision behaviour, and point-in-time relationships.

### Canonical dimensions

```text
dim_security
dim_company
dim_trading_session
dim_macro_series
dim_event_type
dim_source
```

`dim_security` will combine provider identifiers and descriptive attributes.
Changes to names, symbols, exchanges, and statuses may eventually require a
type-2 slowly changing dimension. Provider identifiers must remain available
even after a common surrogate `security_key` is assigned.

### Canonical facts

```text
fct_daily_bars
fct_minute_bars
fct_trades
fct_quotes
fct_corporate_actions
fct_news
fct_sec_filings
fct_company_facts
fct_short_sale_volume
fct_macro_observations
fct_trading_halts
```

These facts must remain separate because they have different grains. They are
combined only after aggregation to the grain required by a mart.

### Bridges

```text
bridge_news_security
bridge_filing_security
```

A news article can mention several securities, so a bridge is preferable to
duplicating the entire article for every symbol.

### Operational models

```text
ingestion_runs
source_freshness
data_quality_results
reconciliation_changes
```

Operational models make pipeline reliability visible instead of limiting the
project to business data alone.

## Analytical marts

### `mart_daily_surveillance`

This is the first target mart.

**Grain:** one security and trading date.

It should support the question:

> Which stocks behaved unusually today, and what available events may explain
> that behaviour?

Candidate fields include:

```text
security_key
symbol
trading_date

open
high
low
close
volume
vwap
trade_count

daily_return
overnight_gap
intraday_range_pct
volume_20d_average
volume_zscore
volatility_20d

short_volume
short_exempt_volume
total_reported_short_market_volume
short_volume_ratio

has_corporate_action
corporate_action_type
has_sec_filing
latest_filing_form
latest_filing_accepted_at
news_count
has_trading_halt

federal_funds_rate
treasury_2y_rate
treasury_10y_rate
yield_curve_spread

data_status
source_as_of
```

The first version does not need every field. It should grow as each source is
implemented and verified.

### `mart_intraday_liquidity`

**Grain:** one security and minute.

Sources include historical and live quotes, trades, minute bars, market
statuses, and halt events.

Candidate measures include quoted spread, spread in basis points, midpoint,
displayed sizes, trade volume, trade count, VWAP, realized volatility, and halt
state.

It should support:

> Which securities show abnormal trading or deteriorating liquidity?

### `mart_event_impact`

**Grain:** one event, security, and measurement horizon.

Events include SEC filings, corporate actions, news, and trading halts. Market
reaction can be measured over intervals such as five minutes, thirty minutes,
one trading day, and five trading days.

It should support:

> How did price, volume, and liquidity change around a known event?

### `mart_company_fundamentals`

**Grain:** one company, financial concept, fiscal period, and filing version.

This mart will be built from SEC submissions and company facts. It must retain
the filing acceptance time and source concept so that normalized values remain
traceable to the original filing.

It should support:

> What company information was publicly available at a given point in time?

### `mart_pipeline_health`

**Grain:** one dataset and ingestion run.

Candidate measures include expected and received rows, invalid and duplicate
rows, freshness, duration, retries, load status, transformation status, and
source-as-of time.

It should support:

> Is the data current, complete, and safe for analysts to use?

## Storage and warehouse layout

Keep the existing raw and analytics BigQuery datasets. Table names should
identify the source:

```text
equity_raw.alpaca_daily_bars
equity_raw.alpaca_minute_bars
equity_raw.alpaca_trades
equity_raw.alpaca_quotes
equity_raw.alpaca_corporate_actions
equity_raw.alpaca_assets
equity_raw.alpaca_calendar
equity_raw.alpaca_news

equity_raw.sec_company_mapping
equity_raw.sec_submissions
equity_raw.sec_company_facts

equity_raw.finra_short_sale_volume

equity_raw.fred_series
equity_raw.fred_observations

equity_raw.nasdaq_symbols
equity_raw.nasdaq_trading_halts
```

New GCS prefixes should make the source and dataset explicit:

```text
source=<provider>/dataset=<dataset>/event_date=<date>/run_id=<id>/
```

Each completed prefix contains Parquet parts and a manifest. Requested
intervals, provider parameters, schema version, checksums, row counts, and
source-as-of time belong in the manifest.

## Pipeline structure

Shared batch behaviour should be extracted from the existing daily-bars code
without removing dataset-specific validation:

```text
ingestion/src/equity_data_platform/
├── batch/
│   ├── manifest.py
│   ├── publication.py
│   └── validation.py
├── sources/
│   ├── alpaca/
│   │   ├── assets.py
│   │   ├── calendar.py
│   │   ├── corporate_actions.py
│   │   ├── minute_bars.py
│   │   ├── trades.py
│   │   ├── quotes.py
│   │   └── news.py
│   ├── sec/
│   │   ├── client.py
│   │   ├── submissions.py
│   │   └── company_facts.py
│   ├── finra/
│   │   └── short_sale_volume.py
│   ├── fred/
│   │   ├── client.py
│   │   └── observations.py
│   └── nasdaq/
│       ├── symbols.py
│       └── trading_halts.py
└── pipeline.py
```

Possible Kestra parent workflows are:

```text
market_reference_refresh
company_events_refresh
macro_observations_refresh
historical_market_backfill
daily_surveillance_build
live_market_ingestion
```

Each parent should retain independent extract, load, and transform boundaries.
One failure should not require repeating unrelated successful source loads.

## Controls required before high-volume expansion

1. Add an ingestion-run ledger containing provider, dataset, request identity,
   manifest checksum, status, row count, source URI, and BigQuery job ID.
2. Introduce source and dataset fields in every raw record.
3. Version schemas per dataset.
4. Add source-specific retry and rate-limit policies.
5. Implement date and symbol chunking.
6. Write multiple Parquet parts according to a target file size or row count.
7. Add dataset-specific freshness and completeness tests.
8. Preserve point-in-time fields such as `published_at`, `accepted_at`,
   `received_at`, and `source_as_of`.
9. Add `FRED_API_KEY` to secret configuration.
10. Configure an identifiable `SEC_USER_AGENT`.

Trades and quotes must not inherit the daily-bar coverage rule. A day can
legitimately contain no trade or quote for some securities, while a daily bar
for an eligible liquid stock is expected once per completed session.

## Delivery phases

### Phase 1: multi-source daily context

1. Add the ingestion-run ledger and shared batch contract.
2. Add Alpaca assets and market calendar.
3. Add Alpaca corporate actions and historical news.
4. Add SEC ticker mapping and filing submissions.
5. Add FINRA daily short-sale volume.
6. Add FRED series metadata and observations.
7. Build `dim_security`, `dim_trading_session`, and
   `mart_daily_surveillance`.

This phase changes the project from a daily-bars pipeline into a genuine
multi-source analytical platform.

### Phase 2: detailed company and market history

1. Add SEC company facts.
2. Add Nasdaq symbol data and halt history.
3. Add historical minute bars.
4. Add historical trades and quotes using bounded daily chunks.
5. Build `mart_company_fundamentals` and `mart_event_impact`.

Historical trades and quotes should initially cover the existing five symbols
and a small date interval. Their volume should be measured before increasing
the universe or retention period.

### Phase 3: live surveillance

1. Add Kafka topics for trades, quotes, bars, statuses, corrections,
   cancellations, and news.
2. Write provisional live events to BigQuery through stream consumers.
3. Reconcile live data against later historical responses.
4. Build `mart_intraday_liquidity` and alerting models.

The live and historical maturity policy remains defined in
[`STREAMING_INGESTION.md`](../STREAMING_INGESTION.md).

### Phase 4: product and operations

1. Build an analyst-facing surveillance dashboard.
2. Add alerts for abnormal price, volume, spread, and trading status.
3. Build `mart_pipeline_health` and freshness alerts.
4. Add retention policies, cost controls, and production identities.

## Learning and implementation sequence

The project should be developed alongside Data Engineering Zoomcamp rather
than postponed until the entire course is finished.

1. Complete the core dbt lessons covering sources, staging, facts and
   dimensions, tests, documentation, incremental models, and snapshots.
2. Apply each concept to the existing daily-bars path.
3. Implement the Phase 1 sources and use them to build the first real mart.
4. Finish the remaining analytics-engineering material using the multi-source
   models.
5. Continue to batch-processing material before deciding whether Spark is
   justified by measured volume.
6. Learn the streaming material before implementing Kafka and WebSocket
   consumers.

Daily bars are sufficient for learning dbt mechanics, but not for the final
analytical product. The first meaningful modelling milestone is
`mart_daily_surveillance`, not another isolated source table.

## Immediate next milestone

The next project milestone is:

> Build `mart_daily_surveillance` from Alpaca daily bars, assets, corporate
> actions and news, SEC filing metadata, FINRA short-sale volume, and FRED
> economic observations.

This milestone provides multiple providers, several grains, conformed
dimensions, point-in-time joins, revisions, source lineage, and a clear reason
for the pipeline to exist. Intraday quotes and trades should follow after this
daily model is reliable.
