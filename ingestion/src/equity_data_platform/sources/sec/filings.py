"""SEC company mapping and filing-submission extraction."""

from __future__ import annotations

import shutil
from collections.abc import Callable
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


def fetch_sec_filings(
    request: SecFilingsBatchRequest,
    user_agent: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    client = session or requests.Session()
    client.headers.update(
        {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
    )
    mapping = _ticker_mapping(_get_json(client, TICKER_URL))
    missing = sorted(set(request.symbols).difference(mapping))
    if missing:
        raise DataValidationError(
            f"SEC ticker mapping is missing requested symbols: {missing}"
        )

    records: list[dict[str, Any]] = []
    for symbol in request.symbols:
        company = mapping[symbol]
        cik = str(company["cik"]).zfill(10)
        payload = _get_json(client, SUBMISSIONS_URL.format(cik=cik))
        recent = payload.get("filings", {}).get("recent", {})
        accessions = recent.get("accessionNumber", [])
        if not isinstance(accessions, list):
            raise DataValidationError(
                f"SEC submissions for {symbol} have no recent filing array"
            )
        for index, accession in enumerate(accessions):
            filing_date = date.fromisoformat(recent["filingDate"][index])
            if request.start_date <= filing_date <= request.end_date:
                records.append(
                    {
                        "cik": cik,
                        "symbol": symbol,
                        "entity_name": str(payload.get("name") or company["name"]),
                        "exchange": company.get("exchange"),
                        "accession_number": accession,
                        "filing_date": filing_date,
                        "report_date": recent["reportDate"][index],
                        "acceptance_datetime": recent["acceptanceDateTime"][index],
                        "form": recent["form"][index],
                        "act": recent["act"][index],
                        "file_number": recent["fileNumber"][index],
                        "film_number": recent["filmNumber"][index],
                        "items": recent["items"][index],
                        "document_size": recent["size"][index],
                        "is_xbrl": recent["isXBRL"][index],
                        "is_inline_xbrl": recent["isInlineXBRL"][index],
                        "primary_document": recent["primaryDocument"][index],
                        "primary_document_description": recent["primaryDocDescription"][
                            index
                        ],
                    }
                )
    return records


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
            "document_size": int(filing["document_size"])
            if filing["document_size"]
            else None,
            "is_xbrl": bool(filing["is_xbrl"])
            if filing["is_xbrl"] is not None
            else None,
            "is_inline_xbrl": (
                bool(filing["is_inline_xbrl"])
                if filing["is_inline_xbrl"] is not None
                else None
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
            "api_request_count": 1 + len(request.symbols),
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
    filings = fetch_sec_filings(request, user_agent, session)
    records = build_sec_filing_records(filings, request, run_id, clock())
    return publish_sec_filings(
        output_root=output_root,
        request=request,
        run_id=run_id,
        records=records,
        started_at=started_at,
        completed_at=clock(),
    )
