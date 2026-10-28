from datetime import date

import pytest

from equity_data_platform.pipeline import build_parser, parse_run_id


def test_cli_requires_both_dates() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_cli_parses_inclusive_dates_and_repeatable_symbols() -> None:
    args = build_parser().parse_args(
        [
            "--start-date",
            "2026-10-01",
            "--end-date",
            "2026-10-16",
            "--symbol",
            "AAPL",
            "--symbol",
            "MSFT",
        ]
    )
    assert args.start_date == date(2026, 10, 1)
    assert args.end_date == date(2026, 10, 16)
    assert args.symbols == ["AAPL", "MSFT"]


def test_run_id_rejects_path_characters() -> None:
    with pytest.raises(Exception, match="run ID"):
        parse_run_id("../../unsafe")
