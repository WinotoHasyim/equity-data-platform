import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow.parquet as pq
import pytest

from equity_data_platform.daily_bars import (
    PARQUET_SCHEMA,
    DailyBarsBatchRequest,
    DataValidationError,
    OutputCollisionError,
    build_parquet_records,
    build_stock_bars_request,
    execute_daily_bars_batch,
    publish_batch,
    validate_daily_bars,
)


def request(symbols: tuple[str, ...] = ("AAPL", "MSFT")) -> DailyBarsBatchRequest:
    return DailyBarsBatchRequest(
        start_date=date(2026, 10, 1),
        end_date=date(2026, 10, 2),
        symbols=symbols,
        feed="iex",
    )


def bars_frame() -> pd.DataFrame:
    rows = []
    for symbol, offset in (("AAPL", 0.0), ("MSFT", 100.0)):
        for day in (1, 2):
            rows.append(
                {
                    "symbol": symbol,
                    "timestamp": pd.Timestamp(f"2026-10-{day:02d}T04:00:00Z"),
                    "open": 100.0 + offset + day,
                    "high": 103.0 + offset + day,
                    "low": 99.0 + offset + day,
                    "close": 102.0 + offset + day,
                    "volume": 1_000.0 + day,
                    "trade_count": 100.0 + day,
                    "vwap": 101.5 + offset + day,
                }
            )
    return pd.DataFrame(rows)


def test_request_uses_inclusive_end_date() -> None:
    alpaca_request = build_stock_bars_request(request())
    assert alpaca_request.start.date() == date(2026, 10, 1)
    assert alpaca_request.end.date() == date(2026, 10, 3)


def test_batch_key_is_independent_of_symbol_order() -> None:
    first = request(("AAPL", "MSFT"))
    second = request(("MSFT", "AAPL"))
    assert first.batch_key == second.batch_key


def test_validate_daily_bars_requires_complete_coverage() -> None:
    incomplete = bars_frame().iloc[:-1]
    with pytest.raises(DataValidationError, match="Missing 1 expected"):
        validate_daily_bars(
            incomplete,
            request(),
            (date(2026, 10, 1), date(2026, 10, 2)),
        )


def test_keys_are_stable_for_identical_records() -> None:
    frame = bars_frame().iloc[:1]
    ingested_at = datetime(2026, 10, 24, 12, tzinfo=UTC)
    first = build_parquet_records(frame, request(("AAPL",)), "run-one", ingested_at)
    second = build_parquet_records(frame, request(("AAPL",)), "run-two", ingested_at)

    assert first[0]["bar_key"] == second[0]["bar_key"]
    assert first[0]["payload_fingerprint"] == second[0]["payload_fingerprint"]
    assert first[0]["run_id"] != second[0]["run_id"]


def test_publish_batch_writes_typed_parquet_and_manifest(tmp_path: Path) -> None:
    batch_request = request()
    frame = bars_frame()
    validate_daily_bars(
        frame,
        batch_request,
        (date(2026, 10, 1), date(2026, 10, 2)),
    )
    now = datetime(2026, 10, 24, 12, tzinfo=UTC)
    records = build_parquet_records(frame, batch_request, "test-run", now)

    result = publish_batch(
        output_root=tmp_path,
        request=batch_request,
        run_id="test-run",
        expected_sessions=(date(2026, 10, 1), date(2026, 10, 2)),
        records=records,
        started_at=now,
        completed_at=now,
    )

    table = pq.read_table(result.output_dir / "part-00000.parquet")
    manifest = json.loads((result.output_dir / "manifest.json").read_text())
    assert table.schema.equals(PARQUET_SCHEMA, check_metadata=False)
    assert table.num_rows == 4
    assert manifest["status"] == "succeeded"
    assert manifest["row_count"] == 4
    assert manifest["files"][0]["sha256"]
    assert not (tmp_path / ".staging" / "run_id=test-run").exists()

    with pytest.raises(OutputCollisionError):
        publish_batch(
            output_root=tmp_path,
            request=batch_request,
            run_id="test-run",
            expected_sessions=(date(2026, 10, 1), date(2026, 10, 2)),
            records=records,
            started_at=now,
            completed_at=now,
        )


class FakeTradingClient:
    def __init__(self, sessions: tuple[date, ...]) -> None:
        self.sessions = sessions

    def get_calendar(self, _request: object) -> list[SimpleNamespace]:
        return [SimpleNamespace(date=session) for session in self.sessions]


class FakeHistoricalClient:
    def __init__(self, frame: pd.DataFrame | None = None) -> None:
        self.frame = frame
        self.called = False

    def get_stock_bars(self, _request: object) -> SimpleNamespace:
        self.called = True
        assert self.frame is not None
        indexed = self.frame.set_index(["symbol", "timestamp"])
        return SimpleNamespace(df=indexed)


def test_no_expected_sessions_publishes_manifest_without_parquet(
    tmp_path: Path,
) -> None:
    historical_client = FakeHistoricalClient()
    moments = iter(
        [
            datetime(2026, 10, 24, 10, tzinfo=UTC),
            datetime(2026, 10, 24, 10, 1, tzinfo=UTC),
        ]
    )
    result = execute_daily_bars_batch(
        request=request(),
        run_id="weekend-run",
        output_root=tmp_path,
        historical_client=historical_client,
        trading_client=FakeTradingClient(()),
        clock=lambda: next(moments),
    )

    manifest = json.loads((result.output_dir / "manifest.json").read_text())
    assert result.status == "no_data_expected"
    assert manifest["files"] == []
    assert not historical_client.called
    assert list(result.output_dir.glob("*.parquet")) == []
