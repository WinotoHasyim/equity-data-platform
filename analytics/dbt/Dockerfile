FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

ENV PATH="/app/analytics/dbt/.venv/bin:$PATH"

COPY analytics/dbt/pyproject.toml analytics/dbt/uv.lock ./analytics/dbt/

RUN uv sync \
    --project analytics/dbt \
    --frozen \
    --no-dev \
    --no-install-project

COPY analytics/dbt ./analytics/dbt

WORKDIR /app/analytics/dbt

CMD ["dbt", "--help"]
