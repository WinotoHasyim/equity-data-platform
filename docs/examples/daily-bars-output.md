# Sample Daily-Bars Output

This example comes from a successful local run for one AAPL trading day.

## Run summary

The local pipeline requested one AAPL daily bar for the inclusive interval
2026-10-01 through 2026-10-01. Extraction and validation succeeded and produced
one Parquet part followed by a completion manifest.

```json
{
  "dataset": "daily_bars",
  "feed": "iex",
  "timeframe": "1_day",
  "adjustment": "raw",
  "requested_start_date": "2026-10-01",
  "requested_end_date": "2026-10-01",
  "requested_symbols": ["AAPL"],
  "returned_symbols": ["AAPL"],
  "expected_session_count": 1,
  "row_count": 1,
  "status": "succeeded",
  "validation_status": "passed",
  "schema_version": 1,
  "files": [
    {
      "path": "part-00000.parquet",
      "row_count": 1,
      "byte_size": 6778
    }
  ]
}
```

Run-specific hashes and identifiers are omitted here for readability. The
actual manifest records the run ID, deterministic batch key, file SHA-256 hash,
timestamps, and software version.

## Example canonical input record

The Parquet file used explicit decimal, integer, and UTC timestamp types rather
than pandas-inferred storage types.

| symbol | timestamp | open | high | low | close | volume | trade_count | vwap | feed | timeframe |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| AAPL | 2026-10-01 04:00:00 UTC | 329.550000000 | 332.470000000 | 325.810000000 | 330.440000000 | 988015 | 19383 | 329.366790000 | iex | 1_day |

The stored row also includes `source`, `dataset`, `adjustment`, `run_id`,
`batch_key`, `bar_key`, `payload_fingerprint`, `ingested_at`, and
`schema_version`.

## Reproduce the evidence

Follow the [development runbook](../../DEVELOPMENT.md) to execute a new backfill.
The runbook includes commands for listing GCS objects, reading a manifest, and
checking the raw and canonical BigQuery tables. The raw table may retain
overlapping observations, while the canonical model enforces one current row
per deterministic `bar_key`.
