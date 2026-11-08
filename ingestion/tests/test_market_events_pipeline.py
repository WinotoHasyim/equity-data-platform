from datetime import date

from equity_data_platform.commands.market_events import build_parser


def test_cli_requires_event_type_and_dates() -> None:
    args = build_parser().parse_args(
        [
            "quotes",
            "--start-date",
            "2026-10-27",
            "--end-date",
            "2026-10-27",
            "--symbol",
            "AAPL",
        ]
    )
    assert args.event_type == "quotes"
    assert args.start_date == date(2026, 10, 27)
    assert args.symbol == "AAPL"
