import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow.parquet as pq
import pytest

from equity_data_platform.core.batch import DataValidationError, OutputCollisionError
from equity_data_platform.sources.alpaca.market_events import (
    QUOTE_SCHEMA,
    TRADE_SCHEMA,
    MarketEventsBatchRequest,
    build_market_event_records,
    execute_market_events_batch,
    publish_market_events,
    validate_market_events,
)


def request(event_type: str = "trades") -> MarketEventsBatchRequest:
    return MarketEventsBatchRequest(
        event_type=event_type,
        start_date=date(2026, 10, 27),
        end_date=date(2026, 10, 27),
        symbols=("AAPL",),
    )


def trades_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "timestamp": pd.Timestamp("2026-10-27T13:10:48.220580Z"),
                "exchange": "V",
                "price": 342.55,
                "size": 7.0,
                "id": 1,
                "conditions": ["@", "T", "I"],
                "tape": "C",
            }
        ]
    )


def quotes_frame(
    timestamp: str = "2026-10-27T13:14:18.344023Z",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "timestamp": pd.Timestamp(timestamp),
                "bid_price": 325.28,
                "bid_size": 40.0,
                "bid_exchange": "V",
                "ask_price": 359.8,
                "ask_size": 40.0,
                "ask_exchange": "V",
                "conditions": ["R"],
                "tape": "C",
            }
        ]
    )


def test_request_limits_large_event_backfills() -> None:
    with pytest.raises(ValueError, match="limited to one date"):
        MarketEventsBatchRequest(
            event_type="quotes",
            start_date=date(2026, 10, 1),
            end_date=date(2026, 10, 8),
            symbols=("AAPL",),
        )


def test_request_requires_one_symbol_per_task() -> None:
    with pytest.raises(ValueError, match="exactly one symbol"):
        MarketEventsBatchRequest(
            event_type="quotes",
            start_date=date(2026, 10, 1),
            end_date=date(2026, 10, 1),
            symbols=("AAPL", "MSFT"),
        )


@pytest.mark.parametrize(
    ("event_type", "frame", "column", "value", "message"),
    [
        ("trades", trades_frame(), "price", 0, "non-positive"),
        ("trades", trades_frame(), "id", 1.5, "trade IDs"),
        ("quotes", quotes_frame(), "ask_size", -1, "negative sizes"),
        ("quotes", quotes_frame(), "symbol", "MSFT", "unrequested"),
    ],
)
def test_validation_rejects_bad_events(
    event_type: str,
    frame: pd.DataFrame,
    column: str,
    value: object,
    message: str,
) -> None:
    if column == "id":
        frame[column] = frame[column].astype(object)
    frame.loc[0, column] = value
    with pytest.raises(DataValidationError, match=message):
        validate_market_events(frame, request(event_type))


def test_trade_identity_is_stable_while_payload_detects_correction() -> None:
    now = datetime(2026, 11, 8, tzinfo=UTC)
    first = build_market_event_records(trades_frame(), request(), "one", now)[0]
    corrected = trades_frame()
    corrected.loc[0, "price"] = 343.0
    second = build_market_event_records(corrected, request(), "two", now)[0]
    assert first["event_key"] == second["event_key"]
    assert first["payload_fingerprint"] != second["payload_fingerprint"]


@pytest.mark.parametrize(
    ("timestamp", "expected_unit"),
    [
        ("2025-11-02T17:00:00Z", "round_lots"),
        ("2025-11-03T17:00:00Z", "shares"),
    ],
)
def test_quote_size_unit_follows_alpaca_cutover(
    timestamp: str,
    expected_unit: str,
) -> None:
    record = build_market_event_records(
        quotes_frame(timestamp),
        request("quotes"),
        "quote-unit-run",
        datetime(2026, 11, 8, tzinfo=UTC),
    )[0]
    assert record["quote_size_unit"] == expected_unit


@pytest.mark.parametrize(
    ("event_type", "frame", "schema"),
    [
        ("trades", trades_frame(), TRADE_SCHEMA),
        ("quotes", quotes_frame(), QUOTE_SCHEMA),
    ],
)
def test_publish_writes_expected_schema(
    tmp_path: Path,
    event_type: str,
    frame: pd.DataFrame,
    schema: object,
) -> None:
    batch_request = request(event_type)
    now = datetime(2026, 11, 8, tzinfo=UTC)
    records = build_market_event_records(frame, batch_request, "event-run", now)
    result = publish_market_events(
        output_root=tmp_path,
        request=batch_request,
        run_id="event-run",
        records=records,
        started_at=now,
        completed_at=now,
    )
    table = pq.read_table(result.output_dir / "part-00000.parquet")
    manifest = json.loads((result.output_dir / "manifest.json").read_text())
    assert table.schema.equals(schema, check_metadata=False)
    assert manifest["row_count"] == 1
    with pytest.raises(OutputCollisionError):
        publish_market_events(
            output_root=tmp_path,
            request=batch_request,
            run_id="event-run",
            records=records,
            started_at=now,
            completed_at=now,
        )


class FakeHistoricalClient:
    def get_stock_trades(self, _request: object) -> SimpleNamespace:
        return SimpleNamespace(df=trades_frame().set_index(["symbol", "timestamp"]))

    def get_stock_quotes(self, _request: object) -> SimpleNamespace:
        return SimpleNamespace(df=quotes_frame().set_index(["symbol", "timestamp"]))


@pytest.mark.parametrize("event_type", ["trades", "quotes"])
def test_execute_market_event_batch(tmp_path: Path, event_type: str) -> None:
    moments = iter(
        [
            datetime(2026, 11, 8, 1, tzinfo=UTC),
            datetime(2026, 11, 8, 1, 1, tzinfo=UTC),
            datetime(2026, 11, 8, 1, 2, tzinfo=UTC),
        ]
    )
    result = execute_market_events_batch(
        request=request(event_type),
        run_id=f"{event_type}-run",
        output_root=tmp_path,
        historical_client=FakeHistoricalClient(),
        clock=lambda: next(moments),
    )
    assert result.row_count == 1
