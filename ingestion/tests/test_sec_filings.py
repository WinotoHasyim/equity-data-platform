import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from equity_data_platform.core.batch import DataValidationError, OutputCollisionError
from equity_data_platform.sources.sec.filings import (
    PARQUET_SCHEMA,
    SecFilingsBatchRequest,
    execute_sec_filings_batch,
    fetch_sec_filings,
    validate_sec_user_agent,
)


class FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.urls: list[str] = []

    def get(self, url: str, timeout: int) -> SimpleNamespace:
        assert timeout == 30
        self.urls.append(url)
        if "company_tickers" in url:
            payload = {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
            }
        else:
            payload = {
                "name": "Apple Inc.",
                "filings": {
                    "files": [],
                    "recent": {
                        "accessionNumber": ["0000320193-26-000001"],
                        "filingDate": ["2026-10-30"],
                        "reportDate": ["2026-09-26"],
                        "acceptanceDateTime": ["2026-10-30T18:01:26.000Z"],
                        "act": ["34"],
                        "form": ["10-K"],
                        "fileNumber": ["001-36743"],
                        "filmNumber": ["261234567"],
                        "items": [""],
                        "size": [1234567],
                        "isXBRL": [1],
                        "isInlineXBRL": [1],
                        "primaryDocument": ["aapl-20260926.htm"],
                        "primaryDocDescription": ["10-K"],
                    }
                },
            }
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: payload,
        )


def test_execute_sec_filings_batch(tmp_path: Path) -> None:
    request = SecFilingsBatchRequest(
        start_date=date(2026, 10, 1),
        end_date=date(2026, 10, 31),
        symbols=("AAPL",),
    )
    moments = iter(
        [
            datetime(2026, 11, 8, 1, tzinfo=UTC),
            datetime(2026, 11, 8, 1, 1, tzinfo=UTC),
            datetime(2026, 11, 8, 1, 2, tzinfo=UTC),
        ]
    )
    result = execute_sec_filings_batch(
        request=request,
        run_id="sec-run",
        output_root=tmp_path,
        user_agent="Portfolio Project developer@example.com",
        session=FakeSession(),
        clock=lambda: next(moments),
    )
    table = pq.read_table(result.output_dir / "part-00000.parquet")
    assert table.schema.equals(PARQUET_SCHEMA, check_metadata=False)
    assert table.num_rows == 1
    assert table.column("form").to_pylist() == ["10-K"]
    manifest = json.loads((result.output_dir / "manifest.json").read_text())
    assert manifest["api_request_count"] == 2

    with pytest.raises(OutputCollisionError):
        execute_sec_filings_batch(
            request=request,
            run_id="sec-run",
            output_root=tmp_path,
            user_agent="Portfolio Project developer@example.com",
            session=FakeSession(),
            clock=lambda: datetime(2026, 11, 8, 2, tzinfo=UTC),
        )


def test_request_rejects_non_normalized_symbols() -> None:
    with pytest.raises(ValueError, match="normalized uppercase"):
        SecFilingsBatchRequest(
            start_date=date(2026, 10, 1),
            end_date=date(2026, 10, 31),
            symbols=("aapl",),
        )


def test_user_agent_requires_contact_email() -> None:
    with pytest.raises(DataValidationError, match="contact email"):
        validate_sec_user_agent("anonymous-script")


class HistoricalSession(FakeSession):
    def get(self, url: str, timeout: int) -> SimpleNamespace:
        assert timeout == 30
        self.urls.append(url)
        if "company_tickers" in url:
            payload = {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
            }
        elif url.endswith("CIK0000320193.json"):
            payload = {
                "name": "Apple Inc.",
                "filings": {
                    "recent": {
                        "accessionNumber": [],
                        "filingDate": [],
                        "reportDate": [],
                        "acceptanceDateTime": [],
                        "act": [],
                        "form": [],
                        "fileNumber": [],
                        "filmNumber": [],
                        "items": [],
                        "size": [],
                        "isXBRL": [],
                        "isInlineXBRL": [],
                        "primaryDocument": [],
                        "primaryDocDescription": [],
                    },
                    "files": [
                        {
                            "name": "CIK0000320193-submissions-001.json",
                            "filingFrom": "2019-01-01",
                            "filingTo": "2021-12-31",
                        }
                    ],
                },
            }
        else:
            payload = {
                "accessionNumber": ["0000320193-20-000001"],
                "filingDate": ["2020-10-30"],
                "reportDate": ["2020-09-26"],
                "acceptanceDateTime": ["2020-10-30T18:01:26.000Z"],
                "act": ["34"],
                "form": ["10-K"],
                "fileNumber": ["001-36743"],
                "filmNumber": ["201234567"],
                "items": [""],
                "size": [1234567],
                "isXBRL": [1],
                "isInlineXBRL": [1],
                "primaryDocument": ["aapl-20200926.htm"],
                "primaryDocDescription": ["10-K"],
            }
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)


def test_fetch_includes_relevant_historical_submission_files() -> None:
    session = HistoricalSession()
    filings = fetch_sec_filings(
        SecFilingsBatchRequest(
            start_date=date(2020, 10, 1),
            end_date=date(2020, 10, 31),
            symbols=("AAPL",),
        ),
        "Portfolio Project developer@example.com",
        session,
    )
    assert [filing["accession_number"] for filing in filings] == [
        "0000320193-20-000001"
    ]
    assert any("submissions-001" in url for url in session.urls)


class MisalignedSession(FakeSession):
    def get(self, url: str, timeout: int) -> SimpleNamespace:
        response = super().get(url, timeout)
        payload = response.json()
        if "company_tickers" not in url:
            payload["filings"]["recent"]["form"] = []
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)


def test_fetch_rejects_misaligned_submission_arrays() -> None:
    with pytest.raises(DataValidationError, match="different lengths"):
        fetch_sec_filings(
            SecFilingsBatchRequest(
                start_date=date(2026, 10, 1),
                end_date=date(2026, 10, 31),
                symbols=("AAPL",),
            ),
            "Portfolio Project developer@example.com",
            MisalignedSession(),
        )


def test_empty_sec_interval_publishes_manifest_only(tmp_path: Path) -> None:
    result = execute_sec_filings_batch(
        request=SecFilingsBatchRequest(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
            symbols=("AAPL",),
        ),
        run_id="empty-sec-run",
        output_root=tmp_path,
        user_agent="Portfolio Project developer@example.com",
        session=FakeSession(),
        clock=lambda: datetime(2026, 11, 8, 2, tzinfo=UTC),
    )
    manifest = json.loads((result.output_dir / "manifest.json").read_text())
    assert result.status == "no_data_returned"
    assert manifest["files"] == []
    assert list(result.output_dir.glob("*.parquet")) == []


def test_sec_publish_failure_removes_staging_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_write(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated SEC write failure")

    monkeypatch.setattr(
        "equity_data_platform.core.batch.pq.write_table",
        fail_write,
    )
    with pytest.raises(OSError, match="simulated SEC write failure"):
        execute_sec_filings_batch(
            request=SecFilingsBatchRequest(
                start_date=date(2026, 10, 1),
                end_date=date(2026, 10, 31),
                symbols=("AAPL",),
            ),
            run_id="failed-sec-run",
            output_root=tmp_path,
            user_agent="Portfolio Project developer@example.com",
            session=FakeSession(),
            clock=lambda: datetime(2026, 11, 8, 2, tzinfo=UTC),
        )
    assert not (
        tmp_path / ".staging" / "sec_filings" / "run_id=failed-sec-run"
    ).exists()
    assert not (tmp_path / "sec_filings" / "run_id=failed-sec-run").exists()
