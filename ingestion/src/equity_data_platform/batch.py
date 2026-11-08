"""Shared contracts for immutable source batches."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from alpaca.common.exceptions import APIError
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError as RequestsHTTPError
from requests.exceptions import Timeout as RequestsTimeout
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

DEFAULT_MAX_ROWS_PER_FILE = 500_000

LOGGER = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """Base error for an expected pipeline failure."""


class DataValidationError(PipelineError):
    """Raised when source data violates its declared contract."""


class OutputCollisionError(PipelineError):
    """Raised when a run would overwrite an existing output directory."""


@dataclass(frozen=True)
class PublishedBatch:
    """Files and status produced by a completed batch."""

    run_id: str
    batch_key: str
    status: str
    output_dir: Path
    row_count: int


def canonical_json(value: Any) -> str:
    """Serialize hash inputs deterministically."""

    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def hash_mapping(value: dict[str, Any]) -> str:
    """Create a stable SHA-256 identity for a mapping."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def utc_timestamp(value: Any) -> datetime:
    """Normalize a pandas or Python timestamp to an aware UTC datetime."""

    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()


def package_version() -> str:
    """Return the installed project version for batch lineage."""

    try:
        return version("equity-data-platform")
    except PackageNotFoundError:
        return "unknown"


def file_sha256(path: Path) -> str:
    """Hash one published data file without loading it entirely into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_parquet_parts(
    *,
    records: Sequence[Mapping[str, Any]],
    schema: pa.Schema,
    output_dir: Path,
    max_rows_per_file: int = DEFAULT_MAX_ROWS_PER_FILE,
) -> list[dict[str, Any]]:
    """Write validated Parquet parts and return their manifest metadata."""

    if max_rows_per_file <= 0:
        raise ValueError("max_rows_per_file must be greater than zero")
    if not records:
        return []

    files: list[dict[str, Any]] = []
    for part_number, offset in enumerate(range(0, len(records), max_rows_per_file)):
        part_path = output_dir / f"part-{part_number:05d}.parquet"
        table = pa.Table.from_pylist(
            list(records[offset : offset + max_rows_per_file]),
            schema=schema,
        )
        pq.write_table(table, part_path, compression="zstd")

        parquet_file = pq.ParquetFile(part_path)
        if not parquet_file.schema_arrow.equals(schema, check_metadata=False):
            raise DataValidationError(
                f"Parquet schema mismatch for {part_path.name}"
            )
        if parquet_file.metadata.num_rows != table.num_rows:
            raise DataValidationError(
                f"Parquet row-count mismatch for {part_path.name}"
            )

        files.append(
            {
                "byte_size": part_path.stat().st_size,
                "path": part_path.name,
                "row_count": table.num_rows,
                "sha256": file_sha256(part_path),
            }
        )
    return files


def write_json(path: Path, value: dict[str, Any]) -> None:
    """Write deterministic, human-readable JSON with a trailing newline."""

    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _is_transient_exception(exc: BaseException) -> bool:
    if isinstance(exc, APIError):
        return exc.status_code == 429 or (
            exc.status_code is not None and exc.status_code >= 500
        )
    if isinstance(exc, RequestsHTTPError):
        status_code = exc.response.status_code if exc.response is not None else None
        return status_code == 429 or (status_code is not None and status_code >= 500)
    return isinstance(
        exc,
        (ConnectionError, RequestsConnectionError, RequestsTimeout, TimeoutError),
    )


def call_with_retries(operation: Callable[[], Any]) -> Any:
    """Run an Alpaca operation with bounded retries for transient failures."""

    retryer = Retrying(
        retry=retry_if_exception(_is_transient_exception),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(4),
        reraise=True,
        before_sleep=lambda state: LOGGER.warning(
            "Transient Alpaca error; retrying attempt %s: %s",
            state.attempt_number + 1,
            state.outcome.exception() if state.outcome else "unknown error",
        ),
    )
    return retryer(operation)
