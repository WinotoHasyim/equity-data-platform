"""SEC company mapping and filing-submission extraction."""

from __future__ import annotations

import re
import shutil
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import requests

from equity_data_platform.core.batch import (
    DataValidationError,
    OutputCollisionError,
    PublishedBatch,
    call_with_retries,
    hash_mapping,
    package_version,
    utc_timestamp,
    write_json,
    write_parquet_parts,
)

SOURCE = "sec_edgar"
DATASET = "filings"
SCHEMA_VERSION = 1
TICKER_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SUBMISSIONS_ARCHIVE_URL = "https://data.sec.gov/submissions/{name}"
MIN_REQUEST_INTERVAL_SECONDS = 0.11
ACCESSION_PATTERN = re.compile(r"^\d{10}-\d{2}-\d{6}$")
EMAIL_PATTERN = re.compile(r"[^\s@]+@[^\s@]+\.[^\s@]+")
SUBMISSION_FIELDS = (
    "accessionNumber",
    "filingDate",
    "reportDate",
    "acceptanceDateTime",
    "act",
    "form",
    "fileNumber",
    "filmNumber",
    "items",
    "size",
    "isXBRL",
    "isInlineXBRL",
    "primaryDocument",
    "primaryDocDescription",
)

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("cik", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("entity_name", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=True),
        pa.field("accession_number", pa.string(), nullable=False),
        pa.field("filing_date", pa.date32(), nullable=False),
        pa.field("report_date", pa.date32(), nullable=True),
        pa.field("acceptance_datetime", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("form", pa.string(), nullable=False),
        pa.field("act", pa.string(), nullable=True),
        pa.field("file_number", pa.string(), nullable=True),
        pa.field("film_number", pa.string(), nullable=True),
        pa.field("items", pa.string(), nullable=True),
        pa.field("document_size", pa.int64(), nullable=True),
        pa.field("is_xbrl", pa.bool_(), nullable=True),
        pa.field("is_inline_xbrl", pa.bool_(), nullable=True),
        pa.field("primary_document", pa.string(), nullable=True),
        pa.field("primary_document_description", pa.string(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("batch_key", pa.string(), nullable=False),
        pa.field("filing_key", pa.string(), nullable=False),
        pa.field("payload_fingerprint", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
    ]
)


@dataclass(frozen=True)
class SecFilingsBatchRequest:
    start_date: date
    end_date: date
    symbols: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be later than end_date")
        if not self.symbols or len(set(self.symbols)) != len(self.symbols):
            raise ValueError("A non-empty unique stock-symbol universe is required")
        if any(symbol != symbol.strip().upper() for symbol in self.symbols):
            raise ValueError("Stock symbols must be normalized uppercase values")

    @property
    def batch_key(self) -> str:
        return hash_mapping(
            {
                "dataset": DATASET,
                "end_date": self.end_date.isoformat(),
                "source": SOURCE,
                "start_date": self.start_date.isoformat(),
                "symbols": sorted(self.symbols),
            }
        )


@dataclass(frozen=True)
class SecFetchResult:
    filings: list[dict[str, Any]]
    api_request_count: int


def validate_sec_user_agent(user_agent: str) -> None:
    """Require the descriptive identity and contact requested by SEC."""

    normalized = user_agent.strip()
    if (
        normalized != user_agent
        or len(normalized) < 8
        or len(normalized) > 200
        or EMAIL_PATTERN.search(normalized) is None
    ):
        raise DataValidationError(
            "SEC_USER_AGENT must contain an application identity and contact email"
        )


def _get_json(session: requests.Session, url: str) -> dict[str, Any]:
    def operation() -> dict[str, Any]:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise DataValidationError(f"SEC returned a non-object response from {url}")
        return payload

    return call_with_retries(operation)


def _ticker_mapping(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    fields = payload.get("fields")
    rows = payload.get("data")
    if not isinstance(fields, list) or not isinstance(rows, list):
        raise DataValidationError("SEC ticker mapping has an unexpected structure")
    mapping: dict[str, dict[str, Any]] = {}
    for values in rows:
        row = dict(zip(fields, values, strict=True))
        symbol = str(row["ticker"]).upper()
        mapping[symbol] = row
    return mapping


def _submission_rows(
    arrays: Mapping[str, Any],
    *,
    cik: str,
    symbol: str,
    entity_name: str,
    exchange: Any,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    missing = [field for field in SUBMISSION_FIELDS if field not in arrays]
    if missing:
        raise DataValidationError(
            f"SEC submissions for {symbol} are missing arrays: {missing}"
        )

    lengths: dict[str, int] = {}
    for field in SUBMISSION_FIELDS:
        values = arrays[field]
        if not isinstance(values, list):
            raise DataValidationError(
                f"SEC submission field {field!r} for {symbol} is not an array"
            )
        lengths[field] = len(values)
    if len(set(lengths.values())) != 1:
        raise DataValidationError(
            f"SEC submission arrays for {symbol} have different lengths: {lengths}"
        )

    rows: list[dict[str, Any]] = []
    for index, accession in enumerate(arrays["accessionNumber"]):
        try:
            filing_date = date.fromisoformat(arrays["filingDate"][index])
        except (TypeError, ValueError) as exc:
            raise DataValidationError(
                f"SEC submission for {symbol} has an invalid filing date"
            ) from exc
        if not start_date <= filing_date <= end_date:
            continue
        rows.append(
            {
                "cik": cik,
                "symbol": symbol,
                "entity_name": entity_name,
                "exchange": exchange,
                "accession_number": accession,
                "filing_date": filing_date,
                "report_date": arrays["reportDate"][index],
                "acceptance_datetime": arrays["acceptanceDateTime"][index],
                "form": arrays["form"][index],
                "act": arrays["act"][index],
                "file_number": arrays["fileNumber"][index],
                "film_number": arrays["filmNumber"][index],
                "items": arrays["items"][index],
                "document_size": arrays["size"][index],
                "is_xbrl": arrays["isXBRL"][index],
                "is_inline_xbrl": arrays["isInlineXBRL"][index],
                "primary_document": arrays["primaryDocument"][index],
                "primary_document_description": arrays[
                    "primaryDocDescription"
                ][index],
            }
        )
    return rows


def _optional_non_negative_int(value: Any, field: str) -> int | None:
    if value in (None, ""):
        return None
    try:
        converted = int(value)
    except (TypeError, ValueError) as exc:
        raise DataValidationError(f"Invalid SEC {field}: {value!r}") from exc
    if converted < 0 or str(converted) != str(value):
        raise DataValidationError(f"Invalid SEC {field}: {value!r}")
    return converted


def _optional_bool(value: Any, field: str) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise DataValidationError(f"Invalid SEC {field}: {value!r}")


def validate_sec_filings(
    filings: list[dict[str, Any]], request: SecFilingsBatchRequest
) -> None:
    """Validate SEC filing identity, types, range, and uniqueness."""

    required_fields = {
        "cik",
        "symbol",
        "entity_name",
        "accession_number",
        "filing_date",
        "form",
    }
    identities: set[tuple[str, str]] = set()
    for filing in filings:
        missing = sorted(required_fields.difference(filing))
        if missing:
            raise DataValidationError(f"SEC filing is missing fields: {missing}")

        cik = filing["cik"]
        symbol = filing["symbol"]
        accession = filing["accession_number"]
        filing_date = filing["filing_date"]
        if not isinstance(cik, str) or len(cik) != 10 or not cik.isdigit():
            raise DataValidationError(f"Invalid SEC CIK: {cik!r}")
        if symbol not in request.symbols:
            raise DataValidationError(f"Unexpected SEC filing symbol: {symbol!r}")
        if not isinstance(accession, str) or not ACCESSION_PATTERN.fullmatch(accession):
            raise DataValidationError(f"Invalid SEC accession number: {accession!r}")
        if not isinstance(filing_date, date) or isinstance(filing_date, datetime):
            raise DataValidationError(f"Invalid SEC filing date: {filing_date!r}")
        if not request.start_date <= filing_date <= request.end_date:
            raise DataValidationError("SEC filing falls outside the requested interval")
        if not isinstance(filing["entity_name"], str) or not filing[
            "entity_name"
        ].strip():
            raise DataValidationError("SEC filing has an empty entity name")
        if not isinstance(filing["form"], str) or not filing["form"].strip():
            raise DataValidationError("SEC filing has an empty form")

        _optional_date(filing.get("report_date"))
        _optional_timestamp(filing.get("acceptance_datetime"))
        _optional_non_negative_int(filing.get("document_size"), "document size")
        _optional_bool(filing.get("is_xbrl"), "is_xbrl")
        _optional_bool(filing.get("is_inline_xbrl"), "is_inline_xbrl")

        identity = (cik, accession)
        if identity in identities:
            raise DataValidationError(f"Duplicate SEC filing identity: {identity}")
        identities.add(identity)


def _archive_overlaps(
    descriptor: Mapping[str, Any], start_date: date, end_date: date
) -> bool:
    try:
        archive_start = date.fromisoformat(str(descriptor["filingFrom"]))
        archive_end = date.fromisoformat(str(descriptor["filingTo"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise DataValidationError("SEC historical filing descriptor is malformed") from exc
    return archive_start <= end_date and archive_end >= start_date


def _fetch_sec_filings_with_metadata(
    request: SecFilingsBatchRequest,
    user_agent: str,
    session: requests.Session | None = None,
    *,
    min_request_interval: float = MIN_REQUEST_INTERVAL_SECONDS,
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> SecFetchResult:
    validate_sec_user_agent(user_agent)
    if min_request_interval < 0:
        raise ValueError("min_request_interval must not be negative")

    client = session or requests.Session()
    client.headers.update(
        {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
    )
    request_count = 0
    last_completed_at: float | None = None

    def get_json(url: str) -> dict[str, Any]:
        nonlocal last_completed_at, request_count
        if last_completed_at is not None:
            remaining = min_request_interval - (monotonic() - last_completed_at)
            if remaining > 0:
                sleeper(remaining)
        payload = _get_json(client, url)
        request_count += 1
        last_completed_at = monotonic()
        return payload

    mapping = _ticker_mapping(get_json(TICKER_URL))
    missing = sorted(set(request.symbols).difference(mapping))
    if missing:
        raise DataValidationError(
            f"SEC ticker mapping is missing requested symbols: {missing}"
        )

    records: list[dict[str, Any]] = []
    for symbol in request.symbols:
        company = mapping[symbol]
        cik = str(company["cik"]).zfill(10)
        payload = get_json(SUBMISSIONS_URL.format(cik=cik))
        entity_name = str(payload.get("name") or company["name"])
        filings = payload.get("filings")
        if not isinstance(filings, Mapping):
            raise DataValidationError(
                f"SEC submissions for {symbol} have no filing object"
            )
        recent = filings.get("recent")
        if not isinstance(recent, Mapping):
            raise DataValidationError(
                f"SEC submissions for {symbol} have no recent filing object"
            )
        records.extend(
            _submission_rows(
                recent,
                cik=cik,
                symbol=symbol,
                entity_name=entity_name,
                exchange=company.get("exchange"),
                start_date=request.start_date,
                end_date=request.end_date,
            )
        )

        archive_files = filings.get("files", [])
        if not isinstance(archive_files, list):
            raise DataValidationError(
                f"SEC historical filing descriptors for {symbol} are malformed"
            )
        for descriptor in archive_files:
            if not isinstance(descriptor, Mapping):
                raise DataValidationError(
                    f"SEC historical filing descriptor for {symbol} is malformed"
                )
            if not _archive_overlaps(
                descriptor, request.start_date, request.end_date
            ):
                continue
            name = descriptor.get("name")
            if not isinstance(name, str) or not name or "/" in name:
                raise DataValidationError(
                    f"SEC historical filing filename for {symbol} is invalid"
                )
            historical = get_json(SUBMISSIONS_ARCHIVE_URL.format(name=name))
            records.extend(
                _submission_rows(
                    historical,
                    cik=cik,
                    symbol=symbol,
                    entity_name=entity_name,
                    exchange=company.get("exchange"),
                    start_date=request.start_date,
                    end_date=request.end_date,
                )
            )

    validate_sec_filings(records, request)
    return SecFetchResult(filings=records, api_request_count=request_count)


def fetch_sec_filings(
    request: SecFilingsBatchRequest,
    user_agent: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    return _fetch_sec_filings_with_metadata(
        request,
        user_agent,
        session,
    ).filings


def _optional_date(value: Any) -> date | None:
    return date.fromisoformat(value) if value else None


def _optional_timestamp(value: Any) -> datetime | None:
    return utc_timestamp(value) if value else None


def build_sec_filing_records(
    filings: list[dict[str, Any]],
    request: SecFilingsBatchRequest,
    run_id: str,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for filing in filings:
        payload = filing | {
            "report_date": _optional_date(filing["report_date"]),
            "acceptance_datetime": _optional_timestamp(filing["acceptance_datetime"]),
            "document_size": _optional_non_negative_int(
                filing["document_size"], "document size"
            ),
            "is_xbrl": _optional_bool(filing["is_xbrl"], "is_xbrl"),
            "is_inline_xbrl": _optional_bool(
                filing["is_inline_xbrl"], "is_inline_xbrl"
            ),
        }
        fingerprint_payload = {
            key: value.isoformat() if isinstance(value, (date, datetime)) else value
            for key, value in payload.items()
        }
        output.append(
            payload
            | {
                "source": SOURCE,
                "dataset": DATASET,
                "run_id": run_id,
                "batch_key": request.batch_key,
                "filing_key": hash_mapping(
                    {"accession_number": filing["accession_number"], "source": SOURCE}
                ),
                "payload_fingerprint": hash_mapping(fingerprint_payload),
                "ingested_at": utc_timestamp(ingested_at),
                "schema_version": SCHEMA_VERSION,
            }
        )
    return output


def publish_sec_filings(
    *,
    output_root: Path,
    request: SecFilingsBatchRequest,
    run_id: str,
    records: list[dict[str, Any]],
    started_at: datetime,
    completed_at: datetime,
    api_request_count: int,
) -> PublishedBatch:
    final_dir = output_root / "sec_filings" / f"run_id={run_id}"
    staging_dir = output_root / ".staging" / "sec_filings" / f"run_id={run_id}"
    if final_dir.exists() or staging_dir.exists():
        raise OutputCollisionError(f"Output already exists for run_id={run_id}")
    staging_dir.mkdir(parents=True)
    files: list[dict[str, Any]] = []
    try:
        if records:
            files = write_parquet_parts(
                records=records,
                schema=PARQUET_SCHEMA,
                output_dir=staging_dir,
            )
        manifest = {
            "api_request_count": api_request_count,
            "batch_key": request.batch_key,
            "completed_at": utc_timestamp(completed_at).isoformat(),
            "dataset": DATASET,
            "files": files,
            "requested_end_date": request.end_date.isoformat(),
            "requested_start_date": request.start_date.isoformat(),
            "requested_symbols": list(request.symbols),
            "row_count": len(records),
            "run_id": run_id,
            "schema_version": SCHEMA_VERSION,
            "software_version": package_version(),
            "source": SOURCE,
            "started_at": utc_timestamp(started_at).isoformat(),
            "status": "succeeded" if records else "no_data_returned",
            "validation_status": "passed",
        }
        write_json(staging_dir / "manifest.json", manifest)
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        staging_dir.rename(final_dir)
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise
    return PublishedBatch(
        run_id, request.batch_key, manifest["status"], final_dir, len(records)
    )


def execute_sec_filings_batch(
    *,
    request: SecFilingsBatchRequest,
    run_id: str,
    output_root: Path,
    user_agent: str,
    session: requests.Session | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PublishedBatch:
    started_at = clock()
    fetch_result = _fetch_sec_filings_with_metadata(
        request,
        user_agent,
        session,
    )
    records = build_sec_filing_records(
        fetch_result.filings, request, run_id, clock()
    )
    return publish_sec_filings(
        output_root=output_root,
        request=request,
        run_id=run_id,
        records=records,
        started_at=started_at,
        completed_at=clock(),
        api_request_count=fetch_result.api_request_count,
    )
