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
    fetch_daily_bars,
    fetch_market_sessions,
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


def test_request_rejects_duplicate_symbols() -> None:
    with pytest.raises(ValueError, match="unique"):
        request(("AAPL", "AAPL"))


def test_validate_daily_bars_requires_complete_coverage() -> None:
    incomplete = bars_frame().iloc[:-1]
    with pytest.raises(DataValidationError, match="Missing 1 expected"):
        validate_daily_bars(
            incomplete,
            request(),
            (date(2026, 10, 1), date(2026, 10, 2)),
        )


def test_validate_daily_bars_rejects_duplicate_rows() -> None:
    duplicated = pd.concat([bars_frame(), bars_frame().iloc[[0]]], ignore_index=True)
    with pytest.raises(DataValidationError, match="duplicate symbol/timestamp"):
        validate_daily_bars(
            duplicated,
            request(),
            (date(2026, 10, 1), date(2026, 10, 2)),
        )


@pytest.mark.parametrize(
    ("column", "value", "error"),
    [
        ("open", float("inf"), "non-numeric or non-finite"),
        ("close", 0, "non-positive prices"),
        ("volume", -1, "negative volume"),
        ("trade_count", 1.5, "non-integral trade_count"),
        ("vwap", 1_000, "VWAP outside low/high"),
    ],
)
def test_validate_daily_bars_rejects_invalid_values(
    column: str,
    value: float,
    error: str,
) -> None:
    invalid = bars_frame()
    invalid.loc[0, column] = value
    with pytest.raises(DataValidationError, match=error):
        validate_daily_bars(
            invalid,
            request(),
            (date(2026, 10, 1), date(2026, 10, 2)),
        )


def test_validate_daily_bars_rejects_timezone_naive_timestamp() -> None:
    invalid = bars_frame()
    invalid["timestamp"] = invalid["timestamp"].astype("object")
    invalid.at[0, "timestamp"] = pd.Timestamp("2026-10-01T04:00:00")
    with pytest.raises(DataValidationError, match="timezone-naive"):
        validate_daily_bars(
            invalid,
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


def test_fetch_preserves_missing_columns_for_contract_validation() -> None:
    incomplete = bars_frame().drop(columns="vwap")
    frame = fetch_daily_bars(FakeHistoricalClient(incomplete), request())
    with pytest.raises(DataValidationError, match="Missing required columns.*vwap"):
        validate_daily_bars(
            frame,
            request(),
            (date(2026, 10, 1), date(2026, 10, 2)),
        )


def test_market_calendar_rejects_out_of_range_session() -> None:
    with pytest.raises(DataValidationError, match="out-of-range"):
        fetch_market_sessions(
            FakeTradingClient((date(2026, 10, 3),)),
            date(2026, 10, 1),
            date(2026, 10, 2),
        )


def test_market_calendar_rejects_datetime_instead_of_date() -> None:
    with pytest.raises(DataValidationError, match="invalid or out-of-range"):
        fetch_market_sessions(
            FakeTradingClient((datetime(2026, 10, 1, tzinfo=UTC),)),
            date(2026, 10, 1),
            date(2026, 10, 2),
        )


def test_market_calendar_rejects_suspicious_week_long_empty_response() -> None:
    with pytest.raises(DataValidationError, match="seven or more"):
        fetch_market_sessions(
            FakeTradingClient(()),
            date(2026, 10, 1),
            date(2026, 10, 7),
        )


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


def test_publish_failure_removes_staging_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch_request = request()
    now = datetime(2026, 10, 24, 12, tzinfo=UTC)
    records = build_parquet_records(
        bars_frame(),
        batch_request,
        "failed-run",
        now,
    )

    def fail_write(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated disk failure")

    monkeypatch.setattr(
        "equity_data_platform.daily_bars.pq.write_table",
        fail_write,
    )

    with pytest.raises(OSError, match="simulated disk failure"):
        publish_batch(
            output_root=tmp_path,
            request=batch_request,
            run_id="failed-run",
            expected_sessions=(date(2026, 10, 1), date(2026, 10, 2)),
            records=records,
            started_at=now,
            completed_at=now,
        )

    assert not (tmp_path / ".staging" / "run_id=failed-run").exists()
    assert not (tmp_path / "daily_bars" / "run_id=failed-run").exists()
