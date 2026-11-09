"""Unified command-line dispatcher for ingestion datasets."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable, Sequence

from equity_data_platform.commands import assets, daily_bars, market_events, sec_filings

LOGGER = logging.getLogger(__name__)

CommandHandler = Callable[[Sequence[str] | None], int]
COMMANDS: dict[str, CommandHandler] = {
    "daily-bars": daily_bars.run,
    "assets": assets.run,
    "market-events": market_events.run,
    "sec-filings": sec_filings.run,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="equity-ingest",
        description="Extract and publish one Equity Data Platform source batch.",
        epilog=(
            "Run 'equity-ingest <source> --help' for source-specific arguments. "
            f"Available sources: {', '.join(COMMANDS)}"
        ),
    )
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    if not arguments or arguments[0] in {"-h", "--help"}:
        parser.print_help()
        return 0

    source, *source_arguments = arguments
    handler = COMMANDS.get(source)
    if handler is None:
        parser.error(
            f"unknown source {source!r}; choose from: {', '.join(COMMANDS)}"
        )
    return handler(source_arguments)


def main() -> None:
    daily_bars.configure_logging()
    try:
        raise SystemExit(run())
    except SystemExit:
        raise
    except Exception:
        LOGGER.exception("Ingestion command failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
