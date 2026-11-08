"""Shared contracts for immutable source batches."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import pandas as pd
from alpaca.common.exceptions import APIError
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError as RequestsHTTPError
from requests.exceptions import Timeout as RequestsTimeout
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

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
