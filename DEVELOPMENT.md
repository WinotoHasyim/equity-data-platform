# Development Runbook

This runbook describes how to reproduce the development environment, provision
its cloud resources, run the automated checks, and verify a complete daily-bars
backfill.

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

Use these steps for each development session.

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
docker image ls equity-data-platform:dev
```

Rebuild after changing Python source, `pyproject.toml`, `uv.lock`,
`config.toml`, or the Dockerfile. Building the image does not start the batch
pipeline. `docker compose images` lists images used by containers that Compose
has already created, so it can be empty immediately after a build-only command.
Use `docker image ls` to verify a locally built image.

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
after changing Terraform outputs or `PIPELINE_IMAGE`. Terraform supplies the
cloud resource identifiers, while `.env` supplies the local image tag. The
script publishes those values to the `equity.dev` namespace in Kestra.

If the image tag changes, rebuild the image and synchronize the namespace
configuration:

```bash
docker compose --profile build build pipeline-image
uv run python scripts/sync_kestra_kv.py
```

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
4. Enter an inclusive historical date range. Leave `symbols` empty to use the
   universe in `config.toml`, or supply an array to override it for this run.
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

A successful trading-day run contains a completion manifest and one or more
Parquet files beneath a unique execution ID:

```text
part-00000.parquet
manifest.json
```

The current daily-bars producer writes one Parquet part. The flow already uses
the `part-*.parquet` convention so multipart output can be introduced later
without changing the storage contract.

Copy the complete manifest URI from the listing and inspect it with:

```bash
gcloud storage cat "gs://<bucket>/<path>/manifest.json"
```

Identical retries and overlapping backfills are intentionally retained in raw
GCS and the append-only raw BigQuery table. The dbt canonical model uses the
bar key and ingestion metadata to retain the newest observation for each
logical bar.

To inspect all daily-bars runs or retained object generations, use:

```bash
gcloud storage ls --recursive "gs://${RAW_BUCKET}/daily_bars/**"
gcloud storage ls --all-versions --recursive "gs://${RAW_BUCKET}/daily_bars/**"
```

See [Reliability and recovery](PIPELINE_RELIABILITY.md) for the storage commit
protocol, retry boundaries, and recovery policy.

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
