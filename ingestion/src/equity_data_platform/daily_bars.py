"""Historical daily-bar extraction, validation, lineage, and serialization."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from alpaca.common.exceptions import APIError
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.requests import GetCalendarRequest
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

LOGGER = logging.getLogger(__name__)

SOURCE = "alpaca"
DATASET = "daily_bars"
TIMEFRAME = "1_day"
ADJUSTMENT = "raw"
SCHEMA_VERSION = 1
PRICE_QUANTUM = Decimal("0.000000001")

SOURCE_COLUMNS = (
    "symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
)

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("open", pa.decimal128(20, 9), nullable=False),
        pa.field("high", pa.decimal128(20, 9), nullable=False),
        pa.field("low", pa.decimal128(20, 9), nullable=False),
        pa.field("close", pa.decimal128(20, 9), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
        pa.field("trade_count", pa.int64(), nullable=False),
        pa.field("vwap", pa.decimal128(20, 9), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("feed", pa.string(), nullable=False),
        pa.field("timeframe", pa.string(), nullable=False),
        pa.field("adjustment", pa.string(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("batch_key", pa.string(), nullable=False),
        pa.field("bar_key", pa.string(), nullable=False),
        pa.field("payload_fingerprint", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
    ]
)


class PipelineError(RuntimeError):
    """Base error for an expected pipeline failure."""


class DataValidationError(PipelineError):
    """Raised when source data violates the daily-bars contract."""


class OutputCollisionError(PipelineError):
    """Raised when a run would overwrite an existing output directory."""


@dataclass(frozen=True)
class DailyBarsBatchRequest:
    """Normalized logical request for one atomic daily-bars batch."""

    start_date: date
    end_date: date
    symbols: tuple[str, ...]
    feed: str

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be later than end_date")
        if not self.symbols:
            raise ValueError("At least one stock symbol is required")
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("Stock symbols must be unique within a batch request")
        non_normalized = [
            symbol
            for symbol in self.symbols
            if symbol != symbol.strip().upper()
        ]
        if non_normalized:
            raise ValueError(
                f"Stock symbols must be normalized uppercase values: {non_normalized}"
            )

    @property
    def batch_key(self) -> str:
        return hash_mapping(
            {
                "adjustment": ADJUSTMENT,
                "dataset": DATASET,
                "end_date": self.end_date.isoformat(),
                "feed": self.feed,
                "source": SOURCE,
                "start_date": self.start_date.isoformat(),
                "symbols": sorted(self.symbols),
                "timeframe": TIMEFRAME,
            }
        )


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
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def decimal_value(value: Any) -> Decimal:
    """Convert a source numeric value to the fixed financial scale."""

    try:
        return Decimal(str(value)).quantize(PRICE_QUANTUM)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise DataValidationError(f"Invalid decimal value: {value!r}") from exc


def utc_timestamp(value: Any) -> datetime:
    """Normalize a pandas or Python timestamp to an aware UTC datetime."""

    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()


def build_stock_bars_request(request: DailyBarsBatchRequest) -> StockBarsRequest:
    """Translate inclusive calendar dates to Alpaca's exclusive upper bound."""

    start_at = datetime.combine(request.start_date, time.min, tzinfo=UTC)
    end_at = datetime.combine(
        request.end_date + timedelta(days=1),
        time.min,
        tzinfo=UTC,
    )
    return StockBarsRequest(
        symbol_or_symbols=list(request.symbols),
        timeframe=TimeFrame.Day,
        start=start_at,
        end=end_at,
        adjustment=Adjustment.RAW,
        feed=DataFeed(request.feed),
    )


def _is_transient_exception(exc: BaseException) -> bool:
    if isinstance(exc, APIError):
        return exc.status_code == 429 or (
            exc.status_code is not None and exc.status_code >= 500
        )
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


def fetch_market_sessions(
    trading_client: Any,
    start_date: date,
    end_date: date,
) -> tuple[date, ...]:
    calendar_request = GetCalendarRequest(start=start_date, end=end_date)
    calendar = call_with_retries(lambda: trading_client.get_calendar(calendar_request))
    try:
        sessions = tuple(session.date for session in calendar)
    except (AttributeError, TypeError) as exc:
        raise DataValidationError("Alpaca returned a malformed market calendar") from exc

    invalid_sessions = [
        session
        for session in sessions
        if not isinstance(session, date)
        or isinstance(session, datetime)
        or not start_date <= session <= end_date
    ]
    if invalid_sessions:
        raise DataValidationError(
            f"Market calendar contains invalid or out-of-range dates: "
            f"{invalid_sessions[:10]}"
        )
    if tuple(sorted(set(sessions))) != sessions:
        raise DataValidationError(
            "Market calendar dates must be unique and in ascending order"
        )
    if not sessions and (end_date - start_date).days >= 6:
        raise DataValidationError(
            "Alpaca returned no market sessions for an interval of seven or more "
            "calendar days; refusing to publish a possibly false empty batch"
        )
    return sessions


def fetch_daily_bars(
    historical_client: Any,
    request: DailyBarsBatchRequest,
) -> pd.DataFrame:
    response = call_with_retries(
        lambda: historical_client.get_stock_bars(build_stock_bars_request(request))
    )
    try:
        frame = response.df.reset_index()
    except (AttributeError, TypeError, ValueError) as exc:
        raise DataValidationError("Alpaca returned a malformed bar response") from exc
    if frame.empty:
        return pd.DataFrame(columns=SOURCE_COLUMNS)
    # Preserve the response until validation so a missing source column produces
    # a deliberate contract error instead of an incidental pandas KeyError.
    return frame.copy()


def validate_daily_bars(
    frame: pd.DataFrame,
    request: DailyBarsBatchRequest,
    expected_sessions: tuple[date, ...],
) -> None:
    """Validate schema, values, and exact symbol/session coverage."""

    missing_columns = sorted(set(SOURCE_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise DataValidationError(f"Missing required columns: {missing_columns}")

    if not expected_sessions:
        if not frame.empty:
            raise DataValidationError(
                "Alpaca returned bars for an interval with no market sessions"
            )
        return

    if frame.empty:
        raise DataValidationError(
            "Alpaca returned no daily bars for an interval with market sessions"
        )

    null_rows = int(frame[list(SOURCE_COLUMNS)].isna().any(axis=1).sum())
    if null_rows:
        raise DataValidationError(
            f"Daily bars contain {null_rows} rows with missing required values"
        )

    timestamps = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    invalid_timestamps = int(timestamps.isna().sum())
    if invalid_timestamps:
        raise DataValidationError(
            f"Daily bars contain {invalid_timestamps} invalid timestamps"
        )

    naive_timestamp_count = int(
        frame["timestamp"].map(lambda value: pd.Timestamp(value).tzinfo is None).sum()
    )
    if naive_timestamp_count:
        raise DataValidationError(
            f"Daily bars contain {naive_timestamp_count} timezone-naive timestamps"
        )

    symbols = frame["symbol"].astype("string").str.upper()
    duplicate_count = int(
        pd.DataFrame({"symbol": symbols, "timestamp": timestamps})
        .duplicated(keep=False)
        .sum()
    )
    if duplicate_count:
        raise DataValidationError(
            f"Daily bars contain {duplicate_count} duplicate symbol/timestamp rows"
        )

    price_columns = ["open", "high", "low", "close"]
    numeric_columns = [*price_columns, "volume", "trade_count", "vwap"]
    numeric = frame[numeric_columns].apply(pd.to_numeric, errors="coerce")
    invalid_numeric = numeric.map(lambda value: not math.isfinite(float(value))).any(
        axis=1
    )
    if invalid_numeric.any():
        raise DataValidationError(
            f"Daily bars contain {int(invalid_numeric.sum())} non-numeric or "
            "non-finite rows"
        )

    non_positive_prices = (numeric[[*price_columns, "vwap"]] <= 0).any(axis=1)
    if non_positive_prices.any():
        raise DataValidationError(
            f"Daily bars contain {int(non_positive_prices.sum())} rows with "
            "non-positive prices"
        )

    invalid_ohlc = (numeric["high"] != numeric[price_columns].max(axis=1)) | (
        numeric["low"] != numeric[price_columns].min(axis=1)
    )
    if invalid_ohlc.any():
        raise DataValidationError(
            f"Daily bars contain {int(invalid_ohlc.sum())} invalid OHLC rows"
        )

    if (numeric["volume"] < 0).any():
        raise DataValidationError("Daily bars contain negative volume")
    if (numeric["trade_count"] < 0).any():
        raise DataValidationError("Daily bars contain negative trade_count")
    if (
        (numeric["vwap"] < numeric["low"])
        | (numeric["vwap"] > numeric["high"])
    ).any():
        raise DataValidationError("Daily bars contain VWAP outside low/high")

    for column in ("volume", "trade_count"):
        non_integral = numeric[column].map(lambda value: float(value).is_integer())
        if not non_integral.all():
            raise DataValidationError(f"Daily bars contain non-integral {column}")

    actual_coverage = set(zip(symbols, timestamps.dt.date, strict=True))
    expected_coverage = {
        (symbol, session) for symbol in request.symbols for session in expected_sessions
    }
    missing_coverage = sorted(expected_coverage - actual_coverage)
    unexpected_coverage = sorted(actual_coverage - expected_coverage)

    if missing_coverage:
        preview = missing_coverage[:10]
        raise DataValidationError(
            f"Missing {len(missing_coverage)} expected symbol/session bars: {preview}"
        )
    if unexpected_coverage:
        preview = unexpected_coverage[:10]
        raise DataValidationError(
            f"Received {len(unexpected_coverage)} unexpected symbol/session bars: "
            f"{preview}"
        )


def _bar_key(
    *,
    symbol: str,
    timestamp: datetime,
    feed: str,
) -> str:
    return hash_mapping(
        {
            "adjustment": ADJUSTMENT,
            "feed": feed,
            "symbol": symbol,
            "timeframe": TIMEFRAME,
            "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        }
    )


def _payload_fingerprint(row: dict[str, Any]) -> str:
    return hash_mapping(
        {
            "close": format(row["close"], "f"),
            "high": format(row["high"], "f"),
            "low": format(row["low"], "f"),
            "open": format(row["open"], "f"),
            "trade_count": row["trade_count"],
            "volume": row["volume"],
            "vwap": format(row["vwap"], "f"),
        }
    )


def build_parquet_records(
    frame: pd.DataFrame,
    request: DailyBarsBatchRequest,
    run_id: str,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    """Convert validated SDK values into the explicit raw-record contract."""

    normalized_ingested_at = utc_timestamp(ingested_at)
    records: list[dict[str, Any]] = []

    for source_row in frame.to_dict(orient="records"):
        timestamp = utc_timestamp(source_row["timestamp"])
        symbol = str(source_row["symbol"]).upper()
        record: dict[str, Any] = {
            "symbol": symbol,
            "timestamp": timestamp,
            "open": decimal_value(source_row["open"]),
            "high": decimal_value(source_row["high"]),
            "low": decimal_value(source_row["low"]),
            "close": decimal_value(source_row["close"]),
            "volume": int(source_row["volume"]),
            "trade_count": int(source_row["trade_count"]),
            "vwap": decimal_value(source_row["vwap"]),
            "source": SOURCE,
            "dataset": DATASET,
            "feed": request.feed,
            "timeframe": TIMEFRAME,
            "adjustment": ADJUSTMENT,
            "run_id": run_id,
            "batch_key": request.batch_key,
            "ingested_at": normalized_ingested_at,
            "schema_version": SCHEMA_VERSION,
        }
        record["bar_key"] = _bar_key(
            symbol=symbol,
            timestamp=timestamp,
            feed=request.feed,
        )
        record["payload_fingerprint"] = _payload_fingerprint(record)
        records.append(record)

    return records


def _package_version() -> str:
    try:
        return version("equity-data-platform")
    except PackageNotFoundError:
        return "unknown"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def publish_batch(
    *,
    output_root: Path,
    request: DailyBarsBatchRequest,
    run_id: str,
    expected_sessions: tuple[date, ...],
    records: list[dict[str, Any]],
    started_at: datetime,
    completed_at: datetime,
) -> PublishedBatch:
    """Write and atomically publish one complete batch directory."""

    final_dir = output_root / DATASET / f"run_id={run_id}"
    staging_dir = output_root / ".staging" / f"run_id={run_id}"

    if final_dir.exists() or staging_dir.exists():
        raise OutputCollisionError(f"Output already exists for run_id={run_id}")

    staging_dir.mkdir(parents=True, exist_ok=False)
    files: list[dict[str, Any]] = []
    status = "no_data_expected" if not expected_sessions else "succeeded"

    try:
        if records:
            table = pa.Table.from_pylist(records, schema=PARQUET_SCHEMA)
            part_path = staging_dir / "part-00000.parquet"
            pq.write_table(table, part_path, compression="zstd")

            round_trip = pq.read_table(part_path)
            if not round_trip.schema.equals(PARQUET_SCHEMA, check_metadata=False):
                raise DataValidationError("Parquet round-trip schema mismatch")
            if round_trip.num_rows != len(records):
                raise DataValidationError("Parquet round-trip row-count mismatch")

            files.append(
                {
                    "byte_size": part_path.stat().st_size,
                    "path": part_path.name,
                    "row_count": round_trip.num_rows,
                    "sha256": _file_sha256(part_path),
                }
            )

        manifest = {
            "adjustment": ADJUSTMENT,
            "api_chunk_count": 0 if not expected_sessions else 1,
            "batch_key": request.batch_key,
            "completed_at": utc_timestamp(completed_at).isoformat(),
            "dataset": DATASET,
            "expected_session_count": len(expected_sessions),
            "feed": request.feed,
            "files": files,
            "requested_end_date": request.end_date.isoformat(),
            "requested_start_date": request.start_date.isoformat(),
            "requested_symbols": list(request.symbols),
            "returned_symbols": sorted({record["symbol"] for record in records}),
            "row_count": len(records),
            "run_id": run_id,
            "schema_version": SCHEMA_VERSION,
            "software_version": _package_version(),
            "source": SOURCE,
            "started_at": utc_timestamp(started_at).isoformat(),
            "status": status,
            "timeframe": TIMEFRAME,
            "validation_status": "passed",
        }
        _write_json(staging_dir / "manifest.json", manifest)

        final_dir.parent.mkdir(parents=True, exist_ok=True)
        staging_dir.rename(final_dir)
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    return PublishedBatch(
        run_id=run_id,
        batch_key=request.batch_key,
        status=status,
        output_dir=final_dir,
        row_count=len(records),
    )


def execute_daily_bars_batch(
    *,
    request: DailyBarsBatchRequest,
    run_id: str,
    output_root: Path,
    historical_client: Any,
    trading_client: Any,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PublishedBatch:
    """Execute the complete extract, validate, serialize, and publish workflow."""

    started_at = clock()
    expected_sessions = fetch_market_sessions(
        trading_client,
        request.start_date,
        request.end_date,
    )

    if expected_sessions:
        frame = fetch_daily_bars(historical_client, request)
        validate_daily_bars(frame, request, expected_sessions)
        records = build_parquet_records(frame, request, run_id, clock())
    else:
        frame = pd.DataFrame(columns=SOURCE_COLUMNS)
        validate_daily_bars(frame, request, expected_sessions)
        records = []

    completed_at = clock()
    return publish_batch(
        output_root=output_root,
        request=request,
        run_id=run_id,
        expected_sessions=expected_sessions,
        records=records,
        started_at=started_at,
        completed_at=completed_at,
    )
