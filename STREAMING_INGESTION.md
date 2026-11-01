# Streaming Ingestion Design

**Status:** Accepted for future implementation  
**Revised:** 2026-11-01  
**Current scope:** The live path is planned but not yet implemented.

## Purpose

The platform serves two distinct data needs:

- live market surveillance needs low-latency observations even though a trade
  may later be corrected or canceled and a bar may receive a late update;
- historical analysis needs the best version available from Alpaca when a
  reconciliation job runs.

Alpaca does not document one universal deadline after which every stock trade,
quote, and bar is guaranteed permanently final. Therefore, the platform must
not describe historical observations as absolutely final or error-free.

## Architecture

Keep separate live and historical ingestion paths.

### Live path

```text
Alpaca WebSocket
        -> Kafka topics
        -> consumers group events into small appends
        -> BigQuery Storage Write API gRPC
        -> append-only raw live tables
        -> provisional live metrics and alerts
```

The BigQuery writer will not make one Storage Write API REST
(`tabledata.insertAll`) request per event. Kafka supplies short-term buffering
and replay. Raw live table retention will be selected after measuring actual
volume and recovery needs.

Writing every live event to GCS is not part of the initial design. It remains
an optional future capability only if the product acquires a concrete need for
long-term event replay, independent disaster recovery, or exact arrival-order
forensics.

### Historical path

```text
Alpaca Historical API
        -> validated Parquet batch
        -> raw GCS
        -> BigQuery load job
        -> raw historical tables
        -> dbt reconciliation
        -> canonical historical models
```

Historical ingestion bootstraps history, fills gaps, and refreshes recent
periods after live collection. It does not overwrite immutable raw live rows.
dbt selects the best currently known version for canonical analysis.

## Data-maturity model

The product will expose two honest maturity states:

### `provisional`

Data received from the WebSocket is immediately useful for monitoring but can
still be affected by late trades, corrections, cancellations, out-of-order
delivery, or an incomplete bar interval.

The live interface must disclose:

> Live market data is provisional and may be updated, corrected, or canceled.

### `reconciled`

Data has been compared with a later historical API response. Reconciled means
"best available from Alpaca as of this reconciliation," not "guaranteed never
to change."

Canonical records should expose metadata such as:

```text
data_status
source_event_at
received_at
reconciled_at
source_as_of
source_event_id
kafka_topic
kafka_partition
kafka_offset
```

The historical interface must disclose:

> Historical data was reconciled against Alpaca as of the displayed timestamp
> and may change if the provider later revises its records or adjustments.

## Initial reconciliation policy

Reconciliation uses a configurable operational policy because the provider does
not publish a single finality window that applies to every dataset:

1. Treat all WebSocket observations as provisional.
2. Run the first historical reconciliation after the market session has
   completed.
3. Re-fetch a configurable recent lookback window on subsequent runs so later
   provider updates can replace the canonical version.
4. Record `reconciled_at` and `source_as_of` on every reconciliation.
5. Permit targeted backfills for older periods.
6. Never label a record `final` unless Alpaca later documents a suitable
   finality guarantee for that dataset.

The exact lookback length will be configured per dataset after measuring data
volume and observing provider behavior. Trades, quotes, bars, and corporate
action-adjusted series need not share one window.

## Dataset-specific interpretation

- **Trades:** live corrections and cancel/error events can revise an earlier
  report; historical trades can carry corrected, incorrect, or canceled update
  states.
- **Quotes:** a new quote normally supersedes the previous market state. Alpaca
  does not document a quote-correction channel equivalent to trade
  corrections, so quote maturity should not be described as correction
  finality.
- **Bars:** bars are derived from eligible trades. Live updated bars handle
  some late trades, while later historical reconciliation provides the best
  available aggregate at query time.
- **Adjusted history:** split-, dividend-, and corporate-action-adjusted values
  can change when adjustment information changes, independently of live trade
  correction timing.

## Delivery and duplicate handling

Kafka identifies an event by topic, partition, and offset. A correctly managed
BigQuery gRPC write stream can use ordered offsets for safe retries. The
consumer checkpoints progress only after BigQuery acknowledges a write.

Canonical dbt models will still use deterministic source identities and
payload fingerprints. That protects against Kafka replay, consumer resets,
historical overlap, repeated backfills, and mistakes outside one write stream.

## Consequences

Benefits:

- the dashboard receives low-latency provisional data;
- analysis uses later, historically reconciled data;
- the architecture avoids an unjustified duplicate GCS copy of the live feed;
- every claim about data correctness is timestamped and testable;
- the existing historical daily-bars pipeline remains valid.

Tradeoffs:

- historical data is best-known rather than provably permanent;
- BigQuery is initially the retained store for raw live observations;
- replay beyond Kafka and BigQuery retention is unavailable unless GCS stream
  archival is added later;
- reconciliation schedules, lookback windows, and canonical precedence require
  dataset-specific tests.

## Delivery plan

The historical daily-bars path establishes the batch contracts first. The live
path will be added as a separate Kafka-based ingestion service and will reuse
the same lineage, data-quality, and canonical reconciliation conventions.
