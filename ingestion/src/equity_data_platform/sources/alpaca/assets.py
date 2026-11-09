"""Alpaca security-master extraction, validation, and serialization."""

from __future__ import annotations

import shutil
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pyarrow as pa

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

SOURCE = "alpaca"
DATASET = "assets"
SCHEMA_VERSION = 1
MARGIN_QUANTUM = Decimal("0.0001")

REQUIRED_FIELDS = {
    "class",
    "exchange",
    "fractionable",
    "id",
    "marginable",
    "shortable",
    "status",
    "symbol",
    "tradable",
}

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("provider_asset_id", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("asset_class", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("tradable", pa.bool_(), nullable=False),
        pa.field("marginable", pa.bool_(), nullable=False),
        pa.field("shortable", pa.bool_(), nullable=False),
        pa.field("fractionable", pa.bool_(), nullable=False),
        pa.field("borrow_status", pa.string(), nullable=True),
        pa.field("easy_to_borrow", pa.bool_(), nullable=True),
        pa.field(
            "maintenance_margin_requirement",
            pa.decimal128(9, 4),
            nullable=True,
        ),
        pa.field("margin_requirement_long", pa.decimal128(9, 4), nullable=True),
        pa.field("margin_requirement_short", pa.decimal128(9, 4), nullable=True),
        pa.field("attributes", pa.list_(pa.string()), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("batch_key", pa.string(), nullable=False),
        pa.field("asset_key", pa.string(), nullable=False),
        pa.field("payload_fingerprint", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
    ]
)


@dataclass(frozen=True)
class AssetsBatchRequest:
    """Normalized logical request for one security-master snapshot."""

    symbols: tuple[str, ...]

    def __post_init__(self) -> None:
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
                "dataset": DATASET,
                "source": SOURCE,
                "symbols": sorted(self.symbols),
            }
        )


def fetch_assets(
    trading_client: Any,
    request: AssetsBatchRequest,
) -> list[dict[str, Any]]:
    """Fetch one raw asset object for every requested symbol."""

    assets: list[dict[str, Any]] = []
    for symbol in request.symbols:
        response = call_with_retries(lambda symbol=symbol: trading_client.get_asset(symbol))
        if not isinstance(response, Mapping):
            raise DataValidationError(
                "Alpaca asset extraction requires a raw-data TradingClient"
            )
        assets.append(dict(response))
    return assets


def _validate_optional_decimal(
    asset: Mapping[str, Any],
    field: str,
) -> None:
    value = asset.get(field)
    if value is None:
        return
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise DataValidationError(
            f"Asset {asset.get('symbol')!r} has invalid {field}: {value!r}"
        ) from exc
    if not decimal.is_finite() or decimal < 0:
        raise DataValidationError(
            f"Asset {asset.get('symbol')!r} has invalid {field}: {value!r}"
        )


def validate_assets(
    assets: list[dict[str, Any]],
    request: AssetsBatchRequest,
) -> None:
    """Validate source fields and exact requested-symbol coverage."""

    if len(assets) != len(request.symbols):
        raise DataValidationError(
            f"Expected {len(request.symbols)} asset responses, received {len(assets)}"
        )

    returned_symbols: list[str] = []
    provider_ids: list[str] = []
    for asset in assets:
        missing = sorted(REQUIRED_FIELDS.difference(asset))
        if missing:
            raise DataValidationError(f"Asset response is missing fields: {missing}")

        symbol = asset["symbol"]
        if not isinstance(symbol, str) or symbol != symbol.strip().upper():
            raise DataValidationError(f"Invalid asset symbol: {symbol!r}")
        returned_symbols.append(symbol)

        provider_id = asset["id"]
        try:
            normalized_id = str(uuid.UUID(str(provider_id)))
        except (ValueError, TypeError, AttributeError) as exc:
            raise DataValidationError(
                f"Asset {symbol!r} has invalid provider id: {provider_id!r}"
            ) from exc
        provider_ids.append(normalized_id)

        if asset["class"] != "us_equity":
            raise DataValidationError(
                f"Asset {symbol!r} has unsupported class: {asset['class']!r}"
            )
        for field in ("exchange", "status"):
            if not isinstance(asset[field], str) or not asset[field].strip():
                raise DataValidationError(
                    f"Asset {symbol!r} has invalid {field}: {asset[field]!r}"
                )
        for field in ("tradable", "marginable", "shortable", "fractionable"):
            if not isinstance(asset[field], bool):
                raise DataValidationError(
                    f"Asset {symbol!r} has non-boolean {field}: {asset[field]!r}"
                )
        if asset.get("easy_to_borrow") is not None and not isinstance(
            asset["easy_to_borrow"], bool
        ):
            raise DataValidationError(
                f"Asset {symbol!r} has non-boolean easy_to_borrow"
            )

        attributes = asset.get("attributes")
        if attributes is not None and (
            not isinstance(attributes, list)
            or any(not isinstance(value, str) for value in attributes)
        ):
            raise DataValidationError(
                f"Asset {symbol!r} has invalid attributes: {attributes!r}"
            )

        for field in (
            "maintenance_margin_requirement",
            "margin_requirement_long",
            "margin_requirement_short",
        ):
            _validate_optional_decimal(asset, field)

    if len(set(returned_symbols)) != len(returned_symbols):
        raise DataValidationError("Alpaca returned duplicate asset symbols")
    if len(set(provider_ids)) != len(provider_ids):
        raise DataValidationError("Alpaca returned duplicate provider asset IDs")
    if set(returned_symbols) != set(request.symbols):
        raise DataValidationError(
            "Returned asset symbols do not match the requested symbol universe"
        )


def _optional_decimal(value: Any, quantum: Decimal) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value)).quantize(quantum)


def _asset_key(provider_asset_id: str) -> str:
    return hash_mapping(
        {
            "provider_asset_id": provider_asset_id,
            "source": SOURCE,
        }
    )


def _payload_fingerprint(record: dict[str, Any]) -> str:
    payload_fields = (
        "asset_class",
        "attributes",
        "borrow_status",
        "easy_to_borrow",
        "exchange",
        "fractionable",
        "maintenance_margin_requirement",
        "margin_requirement_long",
        "margin_requirement_short",
        "marginable",
        "name",
        "shortable",
        "status",
        "symbol",
        "tradable",
    )
    payload = {
        field: (
            format(record[field], "f")
            if isinstance(record[field], Decimal)
            else record[field]
        )
        for field in payload_fields
    }
    return hash_mapping(payload)


def build_parquet_records(
    assets: list[dict[str, Any]],
    request: AssetsBatchRequest,
    run_id: str,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    """Convert validated API mappings into the explicit raw-record contract."""

    normalized_ingested_at = utc_timestamp(ingested_at)
    records: list[dict[str, Any]] = []
    for asset in sorted(assets, key=lambda value: value["symbol"]):
        provider_asset_id = str(uuid.UUID(str(asset["id"])))
        attributes = asset.get("attributes")
        record: dict[str, Any] = {
            "provider_asset_id": provider_asset_id,
            "symbol": asset["symbol"],
            "name": asset.get("name"),
            "asset_class": asset["class"],
            "exchange": asset["exchange"],
            "status": asset["status"],
            "tradable": asset["tradable"],
            "marginable": asset["marginable"],
            "shortable": asset["shortable"],
            "fractionable": asset["fractionable"],
            "borrow_status": asset.get("borrow_status"),
            "easy_to_borrow": asset.get("easy_to_borrow"),
            "maintenance_margin_requirement": _optional_decimal(
                asset.get("maintenance_margin_requirement"), MARGIN_QUANTUM
            ),
            "margin_requirement_long": _optional_decimal(
                asset.get("margin_requirement_long"), MARGIN_QUANTUM
            ),
            "margin_requirement_short": _optional_decimal(
                asset.get("margin_requirement_short"), MARGIN_QUANTUM
            ),
            "attributes": sorted(attributes) if attributes is not None else None,
            "source": SOURCE,
            "dataset": DATASET,
            "run_id": run_id,
            "batch_key": request.batch_key,
            "asset_key": _asset_key(provider_asset_id),
            "ingested_at": normalized_ingested_at,
            "schema_version": SCHEMA_VERSION,
        }
        record["payload_fingerprint"] = _payload_fingerprint(record)
        records.append(record)
    return records


def publish_assets_batch(
    *,
    output_root: Path,
    request: AssetsBatchRequest,
    run_id: str,
    records: list[dict[str, Any]],
    started_at: datetime,
    completed_at: datetime,
) -> PublishedBatch:
    """Write and atomically publish one complete asset snapshot."""

    final_dir = output_root / DATASET / f"run_id={run_id}"
    staging_dir = output_root / ".staging" / DATASET / f"run_id={run_id}"
    if final_dir.exists() or staging_dir.exists():
        raise OutputCollisionError(f"Output already exists for run_id={run_id}")

    staging_dir.mkdir(parents=True, exist_ok=False)
    try:
        files = write_parquet_parts(
            records=records,
            schema=PARQUET_SCHEMA,
            output_dir=staging_dir,
        )

        manifest = {
            "api_request_count": len(request.symbols),
            "batch_key": request.batch_key,
            "completed_at": utc_timestamp(completed_at).isoformat(),
            "dataset": DATASET,
            "files": files,
            "observation_type": "current_snapshot",
            "requested_symbols": list(request.symbols),
            "returned_symbols": sorted(record["symbol"] for record in records),
            "row_count": len(records),
            "run_id": run_id,
            "schema_version": SCHEMA_VERSION,
            "software_version": package_version(),
            "source": SOURCE,
            "started_at": utc_timestamp(started_at).isoformat(),
            "status": "succeeded",
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
        status="succeeded",
        output_dir=final_dir,
        row_count=len(records),
    )


def execute_assets_batch(
    *,
    request: AssetsBatchRequest,
    run_id: str,
    output_root: Path,
    trading_client: Any,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PublishedBatch:
    """Execute the complete asset extract, validate, and publish workflow."""

    started_at = clock()
    assets = fetch_assets(trading_client, request)
    validate_assets(assets, request)
    records = build_parquet_records(assets, request, run_id, clock())
    return publish_assets_batch(
        output_root=output_root,
        request=request,
        run_id=run_id,
        records=records,
        started_at=started_at,
        completed_at=clock(),
    )
