"""Historical Alpaca trade and quote extraction contracts."""

from __future__ import annotations

import shutil
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
from alpaca.data.enums import DataFeed
from alpaca.data.requests import StockQuotesRequest, StockTradesRequest

from equity_data_platform.core.batch import (
    DEFAULT_MAX_ROWS_PER_FILE,
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

EventType = Literal["trades", "quotes"]
SOURCE = "alpaca"
SCHEMA_VERSION = 1
MAX_DAYS_PER_RUN = 1
MAX_ROWS_PER_FILE = DEFAULT_MAX_ROWS_PER_FILE
PRICE_QUANTUM = Decimal("0.000000001")
SIZE_QUANTUM = Decimal("0.000000001")
NEW_YORK = ZoneInfo("America/New_York")
QUOTE_SIZE_SHARES_START_DATE = date(2025, 11, 3)

COMMON_FIELDS = [
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
]
LINEAGE_FIELDS = [
    pa.field("source", pa.string(), nullable=False),
    pa.field("dataset", pa.string(), nullable=False),
    pa.field("feed", pa.string(), nullable=False),
    pa.field("run_id", pa.string(), nullable=False),
    pa.field("batch_key", pa.string(), nullable=False),
    pa.field("event_key", pa.string(), nullable=False),
    pa.field("payload_fingerprint", pa.string(), nullable=False),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("schema_version", pa.int16(), nullable=False),
]

TRADE_SCHEMA = pa.schema(
    COMMON_FIELDS
    + [
        pa.field("provider_trade_id", pa.int64(), nullable=False),
        pa.field("exchange", pa.string(), nullable=True),
        pa.field("price", pa.decimal128(20, 9), nullable=False),
        pa.field("size", pa.decimal128(20, 9), nullable=False),
        pa.field("conditions", pa.list_(pa.string()), nullable=True),
        pa.field("tape", pa.string(), nullable=True),
    ]
    + LINEAGE_FIELDS
)

QUOTE_SCHEMA = pa.schema(
    COMMON_FIELDS
    + [
        pa.field("bid_price", pa.decimal128(20, 9), nullable=False),
        pa.field("bid_size", pa.decimal128(20, 9), nullable=False),
        pa.field("bid_exchange", pa.string(), nullable=True),
        pa.field("ask_price", pa.decimal128(20, 9), nullable=False),
        pa.field("ask_size", pa.decimal128(20, 9), nullable=False),
        pa.field("ask_exchange", pa.string(), nullable=True),
        pa.field("quote_size_unit", pa.string(), nullable=False),
        pa.field("conditions", pa.list_(pa.string()), nullable=True),
        pa.field("tape", pa.string(), nullable=True),
    ]
    + LINEAGE_FIELDS
)

SCHEMAS = {"trades": TRADE_SCHEMA, "quotes": QUOTE_SCHEMA}
REQUIRED_COLUMNS = {
    "trades": {
        "symbol",
        "timestamp",
        "exchange",
        "price",
        "size",
        "id",
        "conditions",
        "tape",
    },
    "quotes": {
        "symbol",
        "timestamp",
        "bid_price",
        "bid_size",
        "bid_exchange",
        "ask_price",
        "ask_size",
        "ask_exchange",
        "conditions",
        "tape",
    },
}


@dataclass(frozen=True)
class MarketEventsBatchRequest:
    event_type: EventType
    start_date: date
    end_date: date
    symbols: tuple[str, ...]
    feed: Literal["iex"] = "iex"

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be later than end_date")
        day_count = (self.end_date - self.start_date).days + 1
        if day_count > MAX_DAYS_PER_RUN:
            raise ValueError("Trade and quote extraction tasks are limited to one date")
        if not self.symbols:
            raise ValueError("At least one stock symbol is required")
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("Stock symbols must be unique")
        if len(self.symbols) != 1:
            raise ValueError(
                "Trade and quote extraction tasks require exactly one symbol"
            )
        if any(symbol != symbol.strip().upper() for symbol in self.symbols):
            raise ValueError("Stock symbols must be normalized uppercase values")

    @property
    def batch_key(self) -> str:
        return hash_mapping(
            {
                "dataset": self.event_type,
                "end_date": self.end_date.isoformat(),
                "feed": self.feed,
                "source": SOURCE,
                "start_date": self.start_date.isoformat(),
                "symbols": sorted(self.symbols),
            }
        )


def _dates(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, datetime.min.time(), tzinfo=NEW_YORK)
    return start, start + timedelta(days=1)


def fetch_market_events(client: Any, request: MarketEventsBatchRequest) -> pd.DataFrame:
    """Fetch bounded one-symbol, one-day chunks and combine their frames."""

    frames: list[pd.DataFrame] = []
    for day in _dates(request.start_date, request.end_date):
        start, end = _bounds(day)
        for symbol in request.symbols:
            request_type = (
                StockTradesRequest
                if request.event_type == "trades"
                else StockQuotesRequest
            )
            params = request_type(
                symbol_or_symbols=[symbol],
                start=start,
                end=end,
                feed=DataFeed.IEX,
            )
            method = (
                client.get_stock_trades
                if request.event_type == "trades"
                else client.get_stock_quotes
            )
            response = call_with_retries(
                lambda method=method, params=params: method(params)
            )
            frame = response.df.reset_index()
            if not frame.empty:
                frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=sorted(REQUIRED_COLUMNS[request.event_type]))
    return pd.concat(frames, ignore_index=True)


def _conditions(value: Any) -> list[str] | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return sorted(value)
    raise DataValidationError(f"Invalid market-event conditions: {value!r}")


def validate_market_events(
    frame: pd.DataFrame, request: MarketEventsBatchRequest
) -> None:
    missing = REQUIRED_COLUMNS[request.event_type].difference(frame.columns)
    if missing:
        raise DataValidationError(f"Missing required columns: {sorted(missing)}")
    if frame.empty:
        return
    if not set(frame["symbol"]).issubset(request.symbols):
        raise DataValidationError("Response contains an unrequested stock symbol")

    timestamps = pd.to_datetime(frame["timestamp"], errors="coerce", utc=False)
    if timestamps.isna().any() or timestamps.dt.tz is None:
        raise DataValidationError(
            "Market events contain invalid or timezone-naive timestamps"
        )
    numeric_columns = (
        ["price", "size"]
        if request.event_type == "trades"
        else ["bid_price", "bid_size", "ask_price", "ask_size"]
    )
    numeric = frame[numeric_columns].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any():
        raise DataValidationError("Market events contain invalid numeric values")
    price_columns = [column for column in numeric_columns if "price" in column]
    size_columns = [column for column in numeric_columns if "size" in column]
    if (numeric[price_columns] <= 0).any().any():
        raise DataValidationError("Market events contain non-positive prices")
    if (numeric[size_columns] < 0).any().any():
        raise DataValidationError("Market events contain negative sizes")
    if request.event_type == "trades":
        trade_ids = pd.to_numeric(frame["id"], errors="coerce")
        if (
            trade_ids.isna().any()
            or (trade_ids < 0).any()
            or (trade_ids % 1 != 0).any()
        ):
            raise DataValidationError("Trades contain invalid provider trade IDs")

    for value in frame["conditions"]:
        _conditions(value)


def _decimal(value: Any, quantum: Decimal) -> Decimal:
    return Decimal(str(value)).quantize(quantum)


def _string_or_none(value: Any) -> str | None:
    return None if pd.isna(value) else str(value)


def _quote_size_unit(timestamp: datetime) -> str:
    market_date = timestamp.astimezone(NEW_YORK).date()
    return (
        "shares"
        if market_date >= QUOTE_SIZE_SHARES_START_DATE
        else "round_lots"
    )


def _hashable_payload(
    record: Mapping[str, Any], fields: tuple[str, ...]
) -> dict[str, Any]:
    return {
        field: (
            format(record[field], "f")
            if isinstance(record[field], Decimal)
            else record[field].isoformat()
            if isinstance(record[field], datetime)
            else record[field]
        )
        for field in fields
    }


def build_market_event_records(
    frame: pd.DataFrame,
    request: MarketEventsBatchRequest,
    run_id: str,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    normalized_ingested_at = utc_timestamp(ingested_at)
    records: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        common: dict[str, Any] = {
            "symbol": row["symbol"],
            "timestamp": utc_timestamp(row["timestamp"]),
        }
        if request.event_type == "trades":
            payload = common | {
                "provider_trade_id": int(row["id"]),
                "exchange": _string_or_none(row["exchange"]),
                "price": _decimal(row["price"], PRICE_QUANTUM),
                "size": _decimal(row["size"], SIZE_QUANTUM),
                "conditions": _conditions(row["conditions"]),
                "tape": _string_or_none(row["tape"]),
            }
            identity = {
                "feed": request.feed,
                "provider_trade_id": payload["provider_trade_id"],
                "source": SOURCE,
                "symbol": payload["symbol"],
                "trade_date": payload["timestamp"].date().isoformat(),
            }
        else:
            payload = common | {
                "bid_price": _decimal(row["bid_price"], PRICE_QUANTUM),
                "bid_size": _decimal(row["bid_size"], SIZE_QUANTUM),
                "bid_exchange": _string_or_none(row["bid_exchange"]),
                "ask_price": _decimal(row["ask_price"], PRICE_QUANTUM),
                "ask_size": _decimal(row["ask_size"], SIZE_QUANTUM),
                "ask_exchange": _string_or_none(row["ask_exchange"]),
                "quote_size_unit": _quote_size_unit(common["timestamp"]),
                "conditions": _conditions(row["conditions"]),
                "tape": _string_or_none(row["tape"]),
            }
            identity = _hashable_payload(payload, tuple(payload)) | {
                "feed": request.feed,
                "source": SOURCE,
            }

        payload_fields = tuple(payload)
        record = payload | {
            "source": SOURCE,
            "dataset": request.event_type,
            "feed": request.feed,
            "run_id": run_id,
            "batch_key": request.batch_key,
            "event_key": hash_mapping(identity),
            "payload_fingerprint": hash_mapping(
                _hashable_payload(payload, payload_fields)
            ),
            "ingested_at": normalized_ingested_at,
            "schema_version": SCHEMA_VERSION,
        }
        records.append(record)
    return records


def publish_market_events(
    *,
    output_root: Path,
    request: MarketEventsBatchRequest,
    run_id: str,
    records: list[dict[str, Any]],
    started_at: datetime,
    completed_at: datetime,
) -> PublishedBatch:
    final_dir = output_root / request.event_type / f"run_id={run_id}"
    staging_dir = output_root / ".staging" / request.event_type / f"run_id={run_id}"
    if final_dir.exists() or staging_dir.exists():
        raise OutputCollisionError(f"Output already exists for run_id={run_id}")
    staging_dir.mkdir(parents=True)
    files: list[dict[str, Any]] = []
    try:
        files = write_parquet_parts(
            records=records,
            schema=SCHEMAS[request.event_type],
            output_dir=staging_dir,
            max_rows_per_file=MAX_ROWS_PER_FILE,
        )
        manifest = {
            "api_chunk_count": len(request.symbols)
            * len(_dates(request.start_date, request.end_date)),
            "batch_key": request.batch_key,
            "completed_at": utc_timestamp(completed_at).isoformat(),
            "dataset": request.event_type,
            "feed": request.feed,
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
        run_id=run_id,
        batch_key=request.batch_key,
        status=manifest["status"],
        output_dir=final_dir,
        row_count=len(records),
    )


def execute_market_events_batch(
    *,
    request: MarketEventsBatchRequest,
    run_id: str,
    output_root: Path,
    historical_client: Any,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PublishedBatch:
    started_at = clock()
    frame = fetch_market_events(historical_client, request)
    validate_market_events(frame, request)
    records = build_market_event_records(frame, request, run_id, clock())
    return publish_market_events(
        output_root=output_root,
        request=request,
        run_id=run_id,
        records=records,
        started_at=started_at,
        completed_at=clock(),
    )
