import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from equity_data_platform.core.batch import DataValidationError, OutputCollisionError
from equity_data_platform.sources.alpaca.assets import (
    PARQUET_SCHEMA,
    AssetsBatchRequest,
    build_parquet_records,
    execute_assets_batch,
    publish_assets_batch,
    validate_assets,
)

ASSET_IDS = {
    "AAPL": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
    "MSFT": "b6d1aa75-5c9c-4353-a305-9e2caa1925ab",
}


def asset(symbol: str) -> dict[str, object]:
    return {
        "id": ASSET_IDS[symbol],
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": symbol,
        "name": f"{symbol} Incorporated",
        "status": "active",
        "tradable": True,
        "marginable": True,
        "shortable": True,
        "fractionable": True,
        "borrow_status": "easy_to_borrow",
        "easy_to_borrow": True,
        "maintenance_margin_requirement": "30",
        "margin_requirement_long": "30",
        "margin_requirement_short": "30",
        "attributes": ["fractional_eh_enabled", "ptp_no_exception"],
    }


def request() -> AssetsBatchRequest:
    return AssetsBatchRequest(symbols=("AAPL", "MSFT"))


def test_batch_key_is_independent_of_symbol_order() -> None:
    assert request().batch_key == AssetsBatchRequest(symbols=("MSFT", "AAPL")).batch_key


def test_request_rejects_duplicate_or_non_normalized_symbols() -> None:
    with pytest.raises(ValueError, match="unique"):
        AssetsBatchRequest(symbols=("AAPL", "AAPL"))
    with pytest.raises(ValueError, match="normalized"):
        AssetsBatchRequest(symbols=("aapl",))


def test_validation_requires_exact_requested_coverage() -> None:
    unexpected = asset("AAPL") | {
        "id": "4ce9353c-66d1-46c2-898f-fce867ab0247",
        "symbol": "NVDA",
    }
    with pytest.raises(DataValidationError, match="do not match"):
        validate_assets([asset("AAPL"), unexpected], request())


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("class", "crypto", "unsupported class"),
        ("tradable", "true", "non-boolean"),
        ("margin_requirement_long", "not-a-number", "invalid"),
        ("attributes", ["valid", 1], "invalid attributes"),
    ],
)
def test_validation_rejects_invalid_source_values(
    field: str,
    value: object,
    message: str,
) -> None:
    invalid = asset("AAPL")
    invalid[field] = value
    with pytest.raises(DataValidationError, match=message):
        validate_assets([invalid], AssetsBatchRequest(symbols=("AAPL",)))


def test_keys_distinguish_identity_from_payload_changes() -> None:
    ingested_at = datetime(2026, 11, 8, 10, tzinfo=UTC)
    first = build_parquet_records(
        [asset("AAPL")],
        AssetsBatchRequest(symbols=("AAPL",)),
        "run-one",
        ingested_at,
    )[0]
    changed_asset = asset("AAPL")
    changed_asset["status"] = "inactive"
    second = build_parquet_records(
        [changed_asset],
        AssetsBatchRequest(symbols=("AAPL",)),
        "run-two",
        ingested_at,
    )[0]

    assert first["asset_key"] == second["asset_key"]
    assert first["payload_fingerprint"] != second["payload_fingerprint"]


def test_publish_writes_typed_parquet_and_manifest(tmp_path: Path) -> None:
    now = datetime(2026, 11, 8, 10, tzinfo=UTC)
    records = build_parquet_records(
        [asset("AAPL"), asset("MSFT")], request(), "asset-run", now
    )
    result = publish_assets_batch(
        output_root=tmp_path,
        request=request(),
        run_id="asset-run",
        records=records,
        started_at=now,
        completed_at=now,
    )

    table = pq.read_table(result.output_dir / "part-00000.parquet")
    manifest = json.loads((result.output_dir / "manifest.json").read_text())
    assert table.schema.equals(PARQUET_SCHEMA, check_metadata=False)
    assert table.num_rows == 2
    assert manifest["observation_type"] == "current_snapshot"
    assert manifest["row_count"] == 2
    assert manifest["files"][0]["sha256"]

    with pytest.raises(OutputCollisionError):
        publish_assets_batch(
            output_root=tmp_path,
            request=request(),
            run_id="asset-run",
            records=records,
            started_at=now,
            completed_at=now,
        )


class FakeTradingClient:
    def get_asset(self, symbol: str) -> dict[str, object]:
        return asset(symbol)


def test_execute_assets_batch(tmp_path: Path) -> None:
    moments = iter(
        [
            datetime(2026, 11, 8, 10, tzinfo=UTC),
            datetime(2026, 11, 8, 10, 1, tzinfo=UTC),
            datetime(2026, 11, 8, 10, 2, tzinfo=UTC),
        ]
    )
    result = execute_assets_batch(
        request=request(),
        run_id="complete-run",
        output_root=tmp_path,
        trading_client=FakeTradingClient(),
        clock=lambda: next(moments),
    )
    assert result.row_count == 2
    assert result.status == "succeeded"
