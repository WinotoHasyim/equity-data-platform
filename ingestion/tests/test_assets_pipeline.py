from equity_data_platform.commands.assets import build_parser


def test_asset_cli_uses_configured_symbols_by_default() -> None:
    assert build_parser().parse_args([]).symbols is None


def test_asset_cli_accepts_repeatable_symbol_override() -> None:
    args = build_parser().parse_args(
        ["--symbol", "AAPL", "--symbol", "MSFT", "--run-id", "asset-run"]
    )
    assert args.symbols == ["AAPL", "MSFT"]
    assert args.run_id == "asset-run"
