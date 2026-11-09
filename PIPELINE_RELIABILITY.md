# Reliability and Recovery

This document defines the reliability model for the historical daily-bars
pipeline. It covers the implemented path:

```text
Kestra request
  -> containerized Python extraction
  -> Alpaca calendar and daily-bars APIs
  -> validation and typed Parquet
  -> Kestra internal artifacts
  -> GCS immutable run prefix and completion manifest
  -> BigQuery append-only raw table
  -> dbt staging and canonical table
```

The pipeline distinguishes three outcomes:

- **Prevented:** validation or an assertion stops bad data before the next layer.
- **Recoverable:** durable data remains intact and the failed phase can be rerun.
- **Residual risk:** the project needs a later control, usually because the current
  daily-bars volume does not justify the added machinery yet.

## Reliability contract

The current pipeline makes these guarantees:

1. `start_date` and `end_date` are required and inclusive.
2. Symbols supplied to Kestra override the configured universe. If symbols are
   omitted, the CLI reads `pipeline.symbols` from `ingestion/config.toml` in the
   pipeline image.
3. A Python batch is not published locally until calendar, schema, value,
   coverage, Parquet schema, and Parquet row-count checks pass.
4. A GCS run is complete only when its `manifest.json` exists. Parquet objects
   without a manifest are incomplete and must not be loaded.
5. Raw BigQuery is append-only. Overlapping runs and repeated observations are
   preserved intentionally.
6. Canonical BigQuery contains one latest accepted observation per `bar_key`.
7. A child-flow failure prevents later child flows from running automatically.

These are data-pipeline guarantees, not a guarantee that Alpaca's IEX values
represent the complete US consolidated market.

## Stage-by-stage failure analysis

### 1. Local and Kestra bootstrap

| Failure | Detection | Behavior and mitigation |
| --- | --- | --- |
| Docker is not running | `docker info` or task-runner error | No extraction begins. Start Docker and rerun. |
| Kestra or PostgreSQL is unavailable | Compose health/status and Kestra UI | No cloud data is changed. Run `docker compose --env-file .env -f orchestration/kestra/docker-compose.yaml up -d`, then inspect logs. |
| Pipeline image is absent | Docker runner reports image not found because `pullPolicy: NEVER` | Build with the Compose build profile. This prevents silently pulling an unintended image. |
| `PIPELINE_IMAGE` KV is missing or stale | Flow expression or Docker runner fails | Rebuild if needed, then run `orchestration/kestra/scripts/sync_kestra_kv.py`. |
| Checked-in source changed but mutable `:dev` image was not rebuilt | Code in execution differs from the worktree | The development runbook explicitly requires rebuilding after source, dependency, `ingestion/config.toml`, or `ingestion/Dockerfile` changes. A production design must use immutable image digests or release tags. |
| Alpaca Kestra secrets are missing/stale | Secret resolution or Alpaca authentication fails | Regenerate `.env.kestra`, recreate Kestra, and rerun. No batch is published. |
| Google ADC is expired/missing | GCS/BigQuery authentication fails | Refresh ADC. Completed local extraction may exist only in Kestra internal storage until its retention removes it. |
| Kestra local database is deleted | Flows reload from Git, but executions/KV/history disappear | Rerun KV synchronization. GCS and BigQuery are not deleted by resetting Compose volumes. |
| Kestra starter/tutorial flows conflict with the read-only Git flow mount | Kestra logs attempts to write tutorial YAML into `/app/flows` | Tutorial-flow creation is disabled in Compose. Existing tutorial flows from an older local database can be removed in the UI or cleared with a deliberate local-volume reset. Project flows still load read-only from Git. |

Residual risks:

- Local Kestra runs as root and mounts the Docker socket and ADC credentials.
  This is acceptable only for an isolated development machine, not production.
- `.env.kestra` is Base64 encoded, not encrypted. It is ignored by Git but must
  still be treated as a secret.

### Cloud infrastructure prerequisites

| Failure | Detection | Behavior and mitigation |
| --- | --- | --- |
| Wrong Google project or stale Kestra KV | Terraform output and KV inspection differ from the observed bucket/dataset | Inspect `terraform -chdir=infrastructure/terraform output`, then run `orchestration/kestra/scripts/sync_kestra_kv.py`; never infer the destination from a Console tab. |
| Storage or BigQuery API disabled | Google API error | Terraform manages the required services. Restore the API, then rerun the failed boundary. |
| Billing disabled, quota exhausted, or service outage | Google job/API error | No later phase runs. Resolve the account/quota incident and retry with backoff. |
| GCS and BigQuery locations differ | BigQuery load error or transfer cost | Terraform provisions both with the same `location`. Do not create ad-hoc cross-region datasets. |
| Insufficient or excessive IAM | 403 response or overly broad blast radius | Development uses ADC; production must separate Terraform, Kestra ingestion, and transformation identities with least privilege. |
| Bucket/dataset deletion attempted with data present | Terraform apply/destroy failure | `force_destroy=false` and `delete_contents_on_destroy=false` protect non-empty data resources by default. |
| Local Terraform state is lost or used concurrently | Terraform forgets resources or concurrent applies race | The current local state is a development limitation. Use a protected remote state backend before team/production use. |

### 2. Parent-flow inputs and scheduling

| Failure | Detection | Behavior and mitigation |
| --- | --- | --- |
| Missing dates | Kestra input validation | Execution cannot start. |
| Malformed dates | Kestra `DATE` input validation | Execution cannot start. |
| Start date is after end date | Python CLI validation | The container exits before creating clients or making an Alpaca request. |
| Symbols omitted | Deliberate behavior | The shell supplies no `--symbol` flags; the CLI uses `ingestion/config.toml`. |
| Empty or malformed override | Kestra array typing, JSON-array check, Python normalization | Fails before publication. |
| Duplicate symbols | Python request contract | Fails rather than producing ambiguous expected coverage. |
| Invalid ticker | Usually manifests as missing Alpaca coverage | Strict coverage validation fails the batch. |
| Future/incomplete session requested | Missing coverage | Batch fails closed instead of publishing partial data. Backfill only completed market sessions. |
| Very large range/universe | Timeout or worker memory exhaustion | A 30-minute extraction timeout prevents an unbounded task, but chunking is not implemented yet. Keep daily-bar batches bounded until a chunk policy exists. |
| Concurrent parent backfills | Parent `concurrency.limit: 1` | Only one parent backfill runs at a time in development. Direct execution of child flows can bypass this guard. |

The strict coverage rule is appropriate for the initial five liquid stocks. It
will need an explicit symbol-eligibility policy before supporting newly listed,
delisted, halted for long periods, or thinly traded securities. Silently
weakening coverage would hide real gaps.

### 3. Alpaca calendar request

| Failure | Detection | Behavior and mitigation |
| --- | --- | --- |
| HTTP 429, server error, timeout, or connection reset | Python exception classifier | Retries up to four attempts with exponential backoff. |
| Authentication/authorization error | Non-transient API error | Fails immediately; invalid credentials are not retried. |
| Malformed calendar response | Missing `date`, invalid type, unsorted, duplicate, or out-of-range dates | Raises `DataValidationError`. |
| False empty calendar for a long interval | Empty response for at least seven inclusive calendar days | Fails closed. |
| Genuine weekend/short holiday interval | No sessions in an interval shorter than seven days | Produces a manifest with `status=no_data_expected` and no Parquet. |

Residual risk: the seven-day false-empty threshold is a defensive US-equities
assumption. If the platform later supports other calendars, it should be
replaced with an exchange-specific calendar policy.

### 4. Alpaca daily-bars request

| Failure | Detection | Behavior and mitigation |
| --- | --- | --- |
| Transient API/network problem | Same retry classifier as the calendar | Retries up to four attempts. |
| Permanent 4xx error | Non-transient API error | Fails immediately. |
| Alpaca SDK returns malformed object/frame | Response conversion guard | Raises a deliberate contract error. |
| SDK response omits a field | Required-column validation | Fails with the missing field names instead of an incidental pandas `KeyError`. |
| API pagination | Handled inside `alpaca-py` | All pages are returned before validation. This still accumulates the batch in memory. |
| Valid-looking but incomplete response | Exact symbol/session coverage comparison | Fails with a preview and count of missing bars. |
| Unexpected symbol or date | Exact coverage comparison | Fails rather than contaminating the requested batch. |

Residual risks:

- The entire response and Arrow table are currently materialized in memory.
  Add date/symbol chunking and bounded multipart output before processing large
  universes or trade/quote volumes.
- Provider-side incorrect values that still satisfy every structural and
  financial invariant cannot be detected at ingestion. Repeated historical
  observations and reconciliation are the appropriate later control.

### 5. Data validation

The producer rejects:

- missing source columns or required values;
- invalid or timezone-naive timestamps;
- duplicate `(symbol, timestamp)` rows;
- non-numeric, NaN, or infinite numeric values;
- zero or negative OHLC/VWAP prices;
- a high that is not the row maximum or low that is not the row minimum;
- negative volume or trade count;
- fractional volume or trade count;
- VWAP outside low/high;
- missing or unexpected symbol/session coverage.

Failure occurs before any final local directory or GCS manifest exists. Tests
cover these classes, including cleanup after a simulated local write failure.

Residual risks:

- Statistical anomalies such as an implausible 90% price jump are not rejected
  because they may represent real splits or exceptional trading. They belong in
  anomaly monitoring, not destructive ingestion filtering.
- Corporate-action interpretation is not part of `adjustment=raw`; the raw bar
  remains faithful to the selected API contract.

### 6. Keys, lineage, and reconciliation fields

| Field | Purpose | Failure contained |
| --- | --- | --- |
| `run_id` | Delivery/execution identity | Separates every physical backfill attempt. |
| `batch_key` | Deterministic requested-work identity | Reveals repeated logical requests despite different executions. |
| `bar_key` | Stable identity of one feed/symbol/timeframe/adjustment/timestamp bar | Allows canonical deduplication across overlapping runs. |
| `payload_fingerprint` | Hash of mutable bar values | Reveals that the same logical bar changed between observations. |
| `ingested_at` | Observation arrival time | Selects the newest accepted observation. |

Hashes are deterministic identities and comparison aids, not security controls.
Hash collisions are theoretically possible, although SHA-256 collision risk is
negligible for this use.

### 7. Parquet and local atomic publication

| Failure | Detection | Behavior and mitigation |
| --- | --- | --- |
| Decimal conversion or Arrow schema mismatch | Explicit schema and conversion | Fails before publication. |
| Disk full, permission failure, interrupted write | Filesystem/Arrow exception | Staging is removed for catchable exceptions. No final directory is published. |
| Corrupt or incomplete Parquet | Immediate read-back, schema check, and row-count check | Fails before manifest creation. |
| Same `run_id` reused locally | Final/staging collision check | Refuses to overwrite. |
| Process receives `SIGKILL` or host crashes | Not catchable by Python | A stale `.staging/run_id=...` may remain. Delete only that verified staging directory or use a new run ID. |

The rename from staging to the final local directory is atomic on the same
filesystem. The manifest records file size, row count, and SHA-256 checksum.

### 8. Kestra artifact capture

After the CLI succeeds, the flow parses the manifest and verifies that:

- the manifest `run_id` equals the Kestra execution ID;
- every `part-*.parquet` file on disk appears in the manifest;
- every manifest part exists on disk;
- row count and effective symbols are exposed as child-flow outputs.

If Kestra fails while capturing `outputFiles`, no GCS publication begins. A
large future batch could exceed Kestra internal-storage or worker-disk limits;
direct-to-object-storage multipart publication will then be preferable.

### 9. GCS part uploads and completion manifest

| Failure | Detection | Behavior and mitigation |
| --- | --- | --- |
| Temporary GCS/network error | Task failure | Each upload has bounded exponential retry and a timeout. |
| One of several part uploads fails | `ForEach` failure | Manifest upload does not run, so consumers must ignore the incomplete prefix. Successful parts may remain as recoverable/orphaned objects. |
| Manifest upload fails | Task failure | Parts remain but the batch is not complete. Restart the failed task or execution. |
| Object path collision | Unique Kestra execution ID normally prevents it | GCS objects are immutable per generation and bucket versioning retains replaced/deleted generations. Production should additionally use create-only generation preconditions. |
| Worker dies after GCS accepted an upload but before Kestra recorded success | Retry may upload the same path again | Content is deterministic; versioning retains generations. The final live content should be identical, but an extra noncurrent version may cost storage. |

The manifest is deliberately uploaded last. It is the commit marker; listing a
Parquet object alone is not proof that a run completed.

Each GCS upload attempt has a 10-minute timeout. Its 45-minute total retry
window accommodates all four attempts plus exponential-backoff delays, so a
full-length attempt does not accidentally consume the entire retry budget.

### 10. BigQuery raw load

| Failure | Detection | Behavior and mitigation |
| --- | --- | --- |
| Missing Parquet object, schema incompatibility, IAM, region, quota, or service error | BigQuery load-job failure | BigQuery load jobs are atomic: all rows are inserted or none are. The task uses bounded retry for transient failures. |
| Wildcard unexpectedly matches zero files | Load failure or loaded-row assertion | Transform does not run. |
| Load reports fewer/more rows than the manifest | `assert_loaded_row_count` | Flow fails before dbt. |
| Overlapping date intervals | Expected in raw | Each physical observation is retained; canonical deduplication uses `bar_key`. |
| Same GCS run is manually loaded twice | Raw delivery duplicates | Canonical remains unique, but raw contains duplicate delivery rows. A durable ingestion ledger is still needed for strict raw exactly-once behavior. |
| Too many tiny load jobs | BigQuery quota/cost/operational pressure | Batch at useful sizes and avoid loading one file per row or symbol. |

The current row-count assertion validates the load job's result against the
manifest. It does not prove that a prior manual load of the same `run_id` never
happened. The next reliability feature should be an ingestion-control table
keyed by `run_id`, source URI, manifest checksum, state, and BigQuery job ID.

Each BigQuery load attempt has a 30-minute timeout. Its 1-hour-35-minute total
retry window accommodates all three attempts and their backoff delays.

### 11. dbt staging and canonical transformation

| Failure | Detection | Behavior and mitigation |
| --- | --- | --- |
| Raw table missing or schema drift | dbt compile/run error | Raw GCS and any successful raw BigQuery rows remain intact. Fix the model/schema and rerun only transformation. |
| Duplicate logical bars | `row_number()` by `bar_key` | Canonical selects the newest `ingested_at`, then `run_id`. |
| Null/duplicate canonical keys | dbt tests | Child flow fails. |
| Long-running query | 30-minute task timeout | Execution stops rather than hanging indefinitely. |
| Unbounded scan cost | Canonical is partitioned by daily `timestamp` and clustered by `symbol, feed` | Downstream filtered queries can prune partitions/clusters. |
| dbt tests fail after a table build | Child flow fails, but the built table may already exist | Consumers need a promoted/stable relation or view before production. The current development model does not provide atomic model-plus-test promotion. |
| Full-table rebuild becomes expensive | Visible in BigQuery job bytes/cost | Convert to an incremental, partition-aware reconciliation strategy as data volume grows. |

Current tests assert canonical `bar_key` uniqueness, required lineage/value
fields, OHLC/VWAP/volume rules, and the expected source contract. Source
freshness, reconciliation-change reporting, and promotion are residual work.

### 12. Observability and recovery

Implemented signals:

- structured Python start, failure, row count, output path, and batch key logs;
- Kestra error handlers identifying the failed boundary;
- manifest timestamps, status, requested/returned symbols, sessions, file
  checksums, row counts, schema version, and software version;
- independent extract, load, and transform executions;
- a manifest-to-BigQuery loaded-row assertion.

Residual work before production:

1. Send alerts to an external destination; a log line alone does not wake an
   operator.
2. Store an ingestion-run ledger with state transitions and job IDs.
3. Add freshness and expected-arrival SLAs after scheduled ingestion exists.
4. Export metrics for rows, duration, retries, missing coverage, lag, and cost.
5. Enable appropriate Cloud Audit Logs and define retention/access policy.
6. Use immutable container digests and record the digest in the manifest.
7. Move Terraform state to a protected remote backend with locking/versioning.

## Retry and replay policy

Use the narrowest retry boundary:

| Failed stage | Recommended action |
| --- | --- |
| Alpaca call inside Python | Let Python's bounded retry complete. If exhausted, rerun extraction as a new execution. |
| GCS part/manifest task | Restart the failed task/execution. Treat a prefix without a manifest as incomplete. |
| BigQuery load before success | Restart load; load jobs are atomic. Check whether the job actually succeeded before manually launching a new independent load. |
| BigQuery load after success but assertion failed | Investigate row counts; do not append again blindly. |
| dbt model/test | Rerun transform only after fixing the SQL, data test, or credentials. Do not repeat extraction. |
| Parent replay from the start | Creates a new raw observation and is allowed, but increases raw storage and must be canonicalized. |

Retries are intentionally bounded. Kestra documents `timeout` as the maximum
duration of each attempt and `retry.maxDuration` as the maximum total retry
window. Permanent validation, authentication, and SQL errors should be fixed,
not retried indefinitely.

## Storage layout and retention

Each extraction writes to a unique prefix containing its requested date range
and execution ID:

```text
daily_bars/batch_start=<date>/batch_end=<date>/run_id=<execution-id>/
```

Cloud Storage folders are name prefixes rather than physical directories.
Operational checks should therefore use recursive listings and the configured
bucket name rather than assume that one Console view contains every run:

```bash
export RAW_BUCKET="$(terraform -chdir=infrastructure/terraform output -raw raw_bucket_name)"

gcloud storage ls --recursive "gs://${RAW_BUCKET}/daily_bars/**"
gcloud storage ls --all-versions --recursive "gs://${RAW_BUCKET}/daily_bars/**"
gcloud storage buckets describe "gs://${RAW_BUCKET}"
```

Object versioning retains replaced or deleted generations of an object. The
configured lifecycle rule aborts incomplete multipart uploads after seven days
but does not delete completed Parquet files or manifests. Noncurrent versions
consume storage and should receive an explicit retention policy before
production.

When investigating a missing object, verify:

1. the exact bucket resolved by Terraform and Kestra KV;
2. noncurrent generations and soft-deleted objects;
3. Cloud Audit Logs, if Data Access logging was enabled at deletion time;
4. manual cleanup commands, lifecycle changes, and Terraform applies;
5. whether a manifest ever existed—the upload may have failed before commit.

## Prioritized remaining work

### Before calling the batch path production-ready

1. Add an ingestion-run ledger and make the raw load idempotent by `run_id`.
2. Use immutable container image digests and include the digest in lineage.
3. Add external failure alerts and freshness/coverage SLAs.
4. Add a tested promotion boundary so a failed dbt test cannot expose a bad
   canonical replacement to downstream consumers.
5. Move Terraform state to a protected remote backend.
6. Replace broad local/development credentials with workload identity and
   least-privilege service accounts in production.

### Before expanding to large universes, trades, or quotes

1. Chunk API requests and bound in-memory frames.
2. Write Parquet parts by target compressed size, with an explicit maximum.
3. Validate each part and reconcile manifest totals across all parts.
4. Define symbol lifecycle/eligibility rules rather than expecting a bar for
   every symbol on every session.
5. Add rate-limit budgeting, backpressure, and dead-letter/replay policy.

## Verification coverage

The development test suite covers request validation, source-schema and value
checks, expected market-session coverage, deterministic keys, output
collisions, and cleanup after a simulated write failure. The end-to-end smoke
test verifies extraction, manifest publication, raw loading, row-count
reconciliation, canonical model construction, and dbt data tests.

Before release, the same checks should run in CI with flow-schema validation,
Terraform validation, and an isolated integration environment.

## Authoritative behavior references

- [Kestra task retries, timeouts, restart, and replay](https://kestra.io/docs/workflow-components/retries)
- [Google Cloud Storage object lifecycle actions](https://cloud.google.com/storage/docs/lifecycle)
- [Google Cloud Storage object versioning](https://cloud.google.com/storage/docs/object-versioning)
- [BigQuery batch loads and atomic load jobs](https://cloud.google.com/bigquery/docs/batch-loading-data)
- [dbt BigQuery partitioning and clustering](https://docs.getdbt.com/reference/resource-configs/bigquery-configs)
