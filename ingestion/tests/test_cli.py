import pytest

from equity_data_platform import cli


def test_cli_dispatches_source_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    received: list[str] = []

    def handler(arguments: object) -> int:
        assert isinstance(arguments, list)
        received.extend(arguments)
        return 0

    monkeypatch.setitem(cli.COMMANDS, "assets", handler)
    result = cli.run(["assets", "--symbol", "AAPL"])

    assert result == 0
    assert received == ["--symbol", "AAPL"]


def test_cli_help_lists_sources(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.run(["--help"]) == 0
    output = capsys.readouterr().out
    assert "daily-bars" in output
    assert "assets" in output
    assert "market-events" in output
    assert "sec-filings" in output


def test_cli_rejects_unknown_source() -> None:
    with pytest.raises(SystemExit) as error:
        cli.run(["unknown-source"])
    assert error.value.code == 2
