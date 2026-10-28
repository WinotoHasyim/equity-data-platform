import pytest

from equity_data_platform.config import normalize_symbols


def test_normalize_symbols_uppercases_deduplicates_and_preserves_order() -> None:
    assert normalize_symbols([" msft ", "AAPL", "MSFT"]) == ("MSFT", "AAPL")


def test_normalize_symbols_rejects_empty_collection() -> None:
    with pytest.raises(ValueError, match="At least one"):
        normalize_symbols([])


def test_normalize_symbols_rejects_malformed_symbol() -> None:
    with pytest.raises(ValueError, match="Invalid stock symbol"):
        normalize_symbols(["NOT A SYMBOL"])
