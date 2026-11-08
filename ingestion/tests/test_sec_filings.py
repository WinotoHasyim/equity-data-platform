from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq

from equity_data_platform.sources.sec.filings import (
    PARQUET_SCHEMA,
    SecFilingsBatchRequest,
    execute_sec_filings_batch,
)


class FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

    def get(self, url: str, timeout: int) -> SimpleNamespace:
        assert timeout == 30
        if "company_tickers" in url:
            payload = {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
            }
        else:
            payload = {
                "name": "Apple Inc.",
                "filings": {
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
