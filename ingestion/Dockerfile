FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY ingestion/pyproject.toml ingestion/uv.lock ingestion/README.md ingestion/config.toml ./
COPY ingestion/src ./src
COPY analytics/dbt ./analytics/dbt

RUN uv sync --frozen --no-dev \
    && useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["uv", "run", "--no-sync", "equity-ingest"]
