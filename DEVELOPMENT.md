# Development Environment Runbook

This runbook reproduces the local development environment, provisions the
development cloud resources, runs the automated checks, executes a complete
Kestra daily-bars backfill, and verifies its GCS and BigQuery outputs.

The local Compose setup currently targets Linux or WSL because it mounts the
Docker socket and the Google Application Default Credentials file from the
host.

## Prerequisites

Install the following tools before continuing:

- Git
- Python 3.13 and `uv`
- Docker with Docker Compose v2
- Terraform 1.6 or newer
- Google Cloud CLI
- An Alpaca account with market-data API credentials
- A Google Cloud project with billing enabled

The Google identity used for development must be able to enable project APIs
and manage the project's GCS and BigQuery resources. Use narrowly scoped IAM
roles for shared environments; a personal sandbox project may use broader
permissions for initial setup.

Run all commands below from the repository root unless stated otherwise.

## One-time setup

### 1. Clone and enter the repository

```bash
git clone https://github.com/WinotoHasyim/equity-data-platform.git
cd equity-data-platform
```

If the repository is already cloned, open a terminal in its root directory.

### 2. Create the local environment file

```bash
cp .env.example .env
```

Edit `.env` and provide at least:

```dotenv
APCA_API_KEY_ID=your-alpaca-key
APCA_API_SECRET_KEY=your-alpaca-secret
KESTRA_POSTGRES_PASSWORD=choose-a-local-database-password
KESTRA_ADMIN_EMAIL=admin@localhost.dev
KESTRA_ADMIN_PASSWORD=ChooseALocalPassword1
```

The Kestra administrator password must contain uppercase and lowercase letters,
a number, and at least eight characters. Use letters, digits, and ordinary
punctuation that is safe inside a YAML double-quoted string.

Set the PostgreSQL password before its Docker volume is created. Changing only
the environment variable later does not change a password already stored in an
existing PostgreSQL volume.

Generate the ignored, Base64-encoded secret file consumed by Kestra:

```bash
uv run python scripts/generate_kestra_secrets.py
```

Regenerate `.env.kestra` whenever either Alpaca credential changes. Base64 is
not encryption; `.env` and `.env.kestra` must both remain outside Git.

### 3. Authenticate to Google Cloud

Set the development project ID for the current shell:

```bash
export GCP_PROJECT_ID="your-google-cloud-project-id"
```

Authenticate the Google Cloud CLI and local application libraries:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project "$GCP_PROJECT_ID"
gcloud auth application-default print-access-token >/dev/null \
  && echo "Google ADC works"
```

Terraform and Kestra use Application Default Credentials locally. Compose
mounts the resulting ADC file read-only into Kestra, so local development does
not require a downloaded service-account key.

### 4. Configure Terraform

Create the ignored development variable file:

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

Edit `terraform/terraform.tfvars`:

```hcl
project_id  = "your-google-cloud-project-id"
environment = "dev"
```

The project ID must match `GCP_PROJECT_ID` from the previous step.

### 5. Provision development infrastructure

```bash
terraform -chdir=terraform init
terraform -chdir=terraform validate
terraform -chdir=terraform plan -out=dev.tfplan
terraform -chdir=terraform apply dev.tfplan
terraform -chdir=terraform output
```

Terraform creates the raw GCS bucket and the raw and analytics BigQuery
datasets. Capture the generated bucket name for later commands:

```bash
export RAW_BUCKET="$(terraform -chdir=terraform output -raw raw_bucket_name)"
echo "$RAW_BUCKET"
```

## Start a development session

Use these steps after restarting the computer or returning to the project.

### 1. Verify local services and authentication

From the repository root:

```bash
docker info >/dev/null && echo "Docker works"
gcloud auth application-default print-access-token >/dev/null \
  && echo "Google ADC works"
```

If the ADC check fails, run:

```bash
gcloud auth application-default login
```

Recover the Terraform-managed bucket name in each new shell:

```bash
export RAW_BUCKET="$(terraform -chdir=terraform output -raw raw_bucket_name)"
```

### 2. Run project checks

```bash
uv sync --frozen
uv run ruff check src tests scripts
uv run pytest -q
terraform -chdir=terraform validate
terraform -chdir=terraform plan
docker compose config --quiet
```

The Terraform plan should report no changes unless the infrastructure
configuration was intentionally modified.

### 3. Build the pipeline image

```bash
docker compose --profile build build pipeline-image
docker compose --profile build images
```

Rebuild after changing Python source, `pyproject.toml`, `uv.lock`,
`config.toml`, or the Dockerfile. Building the image does not start the batch
pipeline.

### 4. Start Kestra

```bash
docker compose up -d
docker compose ps
docker compose logs -f kestra
```

Stop following the logs with `Ctrl+C` after Kestra reports that its server is
running. This stops log streaming, not the containers.

Open <http://localhost:8080> and sign in with `KESTRA_ADMIN_EMAIL` and
`KESTRA_ADMIN_PASSWORD` from `.env`.

Synchronize the Terraform-managed resource names and the locally selected
pipeline image into Kestra's `equity.dev` namespace:

```bash
uv run python scripts/sync_kestra_kv.py
```

Run this command after the first startup, after resetting Kestra's volumes, or
after changing Terraform outputs or `PIPELINE_IMAGE`. Terraform remains the
source of truth for the project, location, bucket, and dataset names; `.env`
remains the source of truth for the local image tag.

The image value reaches a flow through this sequence:

1. `.env` defines `PIPELINE_IMAGE`, such as `equity-data-platform:dev`.
2. Docker Compose uses that value when it builds and tags the local image.
3. `scripts/sync_kestra_kv.py` reads the same `.env` value.
4. The script sends it to Kestra's namespace KV API as
   `equity.dev/PIPELINE_IMAGE`.
5. A task resolves `{{ kv('PIPELINE_IMAGE') }}` when its execution starts.

Kestra stores the KV entry in its local persistent state; it does not read the
project's `.env` file directly. Resetting the Kestra Compose volumes therefore
deletes the KV entry, which is why the synchronization command must be rerun.
If the image tag changes, rebuild the image and synchronize the KV value:

```bash
docker compose --profile build build pipeline-image
uv run python scripts/sync_kestra_kv.py
```

A Kestra bootstrap flow containing `io.kestra.plugin.core.kv.Set` tasks could
also create these entries. That pattern is useful for a small tutorial with
manually chosen constants. This project instead synchronizes Terraform outputs
so bucket and dataset names are not copied into a second flow and allowed to
drift. The script is the bridge between the Terraform source of truth, the
local image configuration, and Kestra's KV API.

The `flows/` directory is mounted read-only and watched by Kestra. Four flows
should appear automatically:

```text
equity.dev.daily_bars_backfill
equity.dev.daily_bars_extract
equity.dev.daily_bars_load
equity.dev.daily_bars_transform
```

The backfill flow is the parent entry point. Its child flows provide independent
retry boundaries for Alpaca extraction and GCS publication, BigQuery loading,
and dbt transformation. Editing their YAML creates new Kestra revisions without
a manual import.

### 5. Execute a backfill

Through the Kestra UI:

1. Open **Flows**.
2. Select `equity.dev.daily_bars_backfill`.
3. Select **Execute**.
4. Enter an inclusive historical date range and one or more stock symbols.
5. Start the execution and inspect each task.

For a command-line smoke test, choose a known US trading day:

```bash
export TEST_START_DATE="2024-01-02"
export TEST_END_DATE="2024-01-02"

set -a
source .env
set +a

curl --fail-with-body \
  --user "$KESTRA_ADMIN_EMAIL:$KESTRA_ADMIN_PASSWORD" \
  --request POST \
  http://localhost:8080/api/v1/main/executions/equity.dev/daily_bars_backfill \
  --form "start_date=$TEST_START_DATE" \
  --form "end_date=$TEST_END_DATE" \
  --form 'symbols=["AAPL"]'
```

The response contains the Kestra execution ID and UI URL. Confirm that these
child executions succeed in order:

```text
daily_bars_extract
daily_bars_load
daily_bars_transform
```

The manifest uploads last and acts as the batch completion marker. A date range
with no trading sessions produces only a manifest.

### 6. Verify the raw GCS objects

Ensure `RAW_BUCKET`, `TEST_START_DATE`, and `TEST_END_DATE` are set, then run:

```bash
gcloud storage ls --recursive \
  "gs://${RAW_BUCKET}/daily_bars/batch_start=${TEST_START_DATE}/batch_end=${TEST_END_DATE}/"
```

A trading-day run currently contains these objects beneath its unique extract
execution ID:

```text
part-00000.parquet
manifest.json
```

The current Python producer writes one Arrow table to one Parquet file, so a
nonempty batch currently has exactly one part. Parquet does not automatically
create multiple files. A later producer can deliberately split a batch by a
maximum row count, target file size, symbol partition, or another policy, which
would produce `part-00001.parquet`, `part-00002.parquet`, and so forth.

The orchestration is already ready for that change: the extract flow discovers
and uploads every `part-*.parquet` file, while the load flow passes the GCS
`part-*.parquet` wildcard to BigQuery. `concurrencyLimit: 4` controls how many
uploads run simultaneously; it does not impose a four-file maximum.

There is no project-defined maximum number of parts yet because the producer
cannot create more than one. If multipart output is introduced, define a target
file size and an explicit per-batch safety limit instead of relying on the much
larger platform limit. BigQuery currently permits up to 10 million files and
15 TB in one load job:
<https://cloud.google.com/bigquery/quotas#load_jobs>.

Multipart writing is intentionally deferred for daily bars. At the initial
scale, splitting them would create many inefficient small files without solving
a real size problem. Introduce a reusable row-count or target-size splitter
when high-volume trades or quotes are added, then reuse it for bars only if bar
batches become large enough to justify it.

Copy the complete manifest URI from the listing and inspect it with:

```bash
gcloud storage cat "gs://<bucket>/<path>/manifest.json"
```

Identical retries and overlapping backfills are intentionally retained in raw
GCS and the append-only raw BigQuery table. The dbt canonical model uses the
bar key and ingestion metadata to retain the newest observation for each
logical bar.

### 7. Verify BigQuery and dbt outputs

Recover the Terraform-managed identifiers and query both layers:

```bash
export GCP_PROJECT_ID="$(terraform -chdir=terraform output -raw project_id)"
export RAW_DATASET="$(terraform -chdir=terraform output -json bigquery_dataset_ids \
  | uv run python -c 'import json,sys; print(json.load(sys.stdin)["raw"])')"
export ANALYTICS_DATASET="$(terraform -chdir=terraform output -json bigquery_dataset_ids \
  | uv run python -c 'import json,sys; print(json.load(sys.stdin)["analytics"])')"

bq query --use_legacy_sql=false \
  "SELECT COUNT(*) AS rows FROM \`${GCP_PROJECT_ID}.${RAW_DATASET}.daily_bars\`"

bq query --use_legacy_sql=false \
  "SELECT COUNT(*) AS rows FROM \`${GCP_PROJECT_ID}.${ANALYTICS_DATASET}.daily_bars\`"
```

The raw count may increase when the same interval is backfilled again. The
canonical count remains one row per `bar_key`, with the latest ingestion chosen
deterministically. The transform child also runs the dbt uniqueness and
not-null tests before it succeeds.

## Stop the development environment

```bash
docker compose down
```

This removes the local containers and network while preserving Kestra flows,
execution history, and internal storage in Docker named volumes.

Inspect the preserved Compose volumes with:

```bash
docker volume ls --filter label=com.docker.compose.project
```

## Reset all local Kestra state

The following command permanently deletes the local Kestra database, flow
revisions, execution history, and internal files stored in Compose volumes:

```bash
docker compose down --volumes --remove-orphans
```

Checked-in flows load again on the next startup. This command does not delete
GCS objects, BigQuery datasets, Terraform state, source files, or Docker images.

## Troubleshooting

```bash
docker compose ps
docker compose logs --tail=200 kestra
docker compose logs --tail=200 kestra-postgres
docker compose config --quiet
terraform -chdir=terraform state list
gcloud storage buckets describe "gs://${RAW_BUCKET}"
```

If Kestra cannot find the pipeline image, rebuild it:

```bash
docker compose --profile build build pipeline-image
uv run python scripts/sync_kestra_kv.py
```

If the API rejects the configured administrator credentials, ensure
`KESTRA_ADMIN_PASSWORD` contains uppercase and lowercase letters and a digit.
After changing credentials for an already initialized local instance, reset
the local volumes and start again:

```bash
docker compose down --volumes --remove-orphans
docker compose up -d
uv run python scripts/sync_kestra_kv.py
```

If Kestra cannot resolve the Alpaca secrets, regenerate `.env.kestra` and
recreate Kestra:

```bash
uv run python scripts/generate_kestra_secrets.py
docker compose up -d --force-recreate kestra
```

If Kestra cannot upload to GCS, verify ADC and access to the bucket:

```bash
gcloud auth application-default print-access-token >/dev/null
gcloud storage buckets describe "gs://${RAW_BUCKET}"
```
